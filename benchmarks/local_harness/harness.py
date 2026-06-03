#!/usr/bin/env python3

from __future__ import annotations

import concurrent.futures
import csv
import json
import os
import signal
import shutil
import socket
import stat
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, TextIO

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

import measure_latency

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
RESULTS_ROOT = SCRIPT_DIR / "results"
DEFAULT_STATS_SOCKET = Path("/var/run/scx/root/stats")
DEFAULT_EVENT_BRIDGE_PORT = 9731
DEFAULT_PROFILE_CATALOG = REPO_ROOT / "benchmarks" / "configs" / "profile_catalog.json"
DEFAULT_CGROUP_POLICY_ROOT = Path("/sys/fs/cgroup/cosmos-policy")
DEFAULT_SLO_MIN_SLACK_US = 5_000
DEFAULT_WORKLOAD_DURATION_MS = 250
SCHEDULER_STATS_READY_TIMEOUT_S = 30.0
EVENT_BRIDGE_EVENT_TIMEOUT_S = 10.0
TIME_BIN = Path("/usr/bin/time")
_CARGO_ENV = os.environ.get("CARGO")
_CARGO_PATH = shutil.which("cargo")
CARGO_BIN = Path(_CARGO_ENV) if _CARGO_ENV else Path(_CARGO_PATH or "cargo")
BPFTOOL_BIN = Path("/usr/sbin/bpftool")
if not BPFTOOL_BIN.exists():
    BPFTOOL_BIN = Path("/usr/bin/bpftool")
BPF_HAS_INVOCATION_PATH = Path("/sys/fs/bpf/cosmos/has_invocation")
BENCHMARK_WORKLOAD_BIN = REPO_ROOT / "target" / "release" / "cosmos-benchmark-workload"
DEBUG_BPF_MAP = os.environ.get("COSMOS_BENCH_DEBUG_BPF_MAP") == "1"
GATE_SCRIPT = 'IFS= read -r _ <&"$COSMOS_START_FD"; exec "$@"'
STOP_SCRIPT = 'kill -STOP $$; exec "$@"'
INLINE_PROFILE_HINTS = os.environ.get("COSMOS_BENCH_INLINE_PROFILE_HINTS") == "1"
BENCH_CGROUPS_ENABLED = os.environ.get("COSMOS_BENCH_CGROUPS", "1") != "0"


def available_cpus() -> list[int]:
    try:
        return sorted(os.sched_getaffinity(0))
    except AttributeError:
        return list(range(os.cpu_count() or 1))


def parse_cpu_set(spec: str | None) -> list[int]:
    if spec is None or spec.strip() == "":
        return []
    cpus: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start_s, end_s = part.split("-", 1)
            start = int(start_s)
            end = int(end_s)
            if end < start:
                raise ValueError(f"invalid CPU range: {part}")
            cpus.update(range(start, end + 1))
        else:
            cpus.add(int(part))
    if any(cpu < 0 for cpu in cpus):
        raise ValueError(f"CPU set contains a negative CPU: {spec}")
    return sorted(cpus)


def format_cpu_set(cpus: Iterable[int] | None) -> str | None:
    ordered = sorted(dict.fromkeys(cpus or []))
    if not ordered:
        return None

    ranges: list[str] = []
    start = prev = ordered[0]
    for cpu in ordered[1:]:
        if cpu == prev + 1:
            prev = cpu
            continue
        ranges.append(str(start) if start == prev else f"{start}-{prev}")
        start = prev = cpu
    ranges.append(str(start) if start == prev else f"{start}-{prev}")
    return ",".join(ranges)


def resolve_cpu_partition(
    control_plane_cpu_spec: str | None,
    reserve_control_plane_cpus: int,
) -> tuple[list[int], list[int]]:
    if reserve_control_plane_cpus < 0:
        raise ValueError("--reserve-control-plane-cpus must be non-negative")

    allowed = available_cpus()
    allowed_set = set(allowed)
    control = parse_cpu_set(control_plane_cpu_spec)
    if control and reserve_control_plane_cpus:
        raise ValueError(
            "use either --control-plane-cpus or --reserve-control-plane-cpus, not both"
        )
    if control:
        unknown = sorted(set(control) - allowed_set)
        if unknown:
            raise ValueError(f"control-plane CPUs are outside allowed affinity: {unknown}")
    elif reserve_control_plane_cpus:
        if reserve_control_plane_cpus >= len(allowed):
            raise ValueError("cannot reserve all available CPUs for control-plane work")
        control = allowed[-reserve_control_plane_cpus:]

    control_set = set(control)
    workload = [cpu for cpu in allowed if cpu not in control_set]
    if control and not workload:
        raise ValueError("control-plane CPU reservation leaves no workload CPUs")
    return control, workload


def with_cpu_affinity(
    command: list[str],
    cpus: Iterable[int] | None,
) -> list[str]:
    cpu_set = format_cpu_set(cpus)
    if cpu_set is None:
        return command
    if command and command[0] == str(TIME_BIN):
        time_prefix = command[:3]
        payload = command[3:]
        return [*time_prefix, "taskset", "-c", cpu_set, *payload]
    return ["taskset", "-c", cpu_set, *command]


def workload_env(
    sched_ext: bool,
    base: dict[str, str] | None = None,
) -> dict[str, str] | None:
    if not sched_ext:
        return base
    env = dict(base) if base is not None else os.environ.copy()
    env["COSMOS_BENCH_SCHED_EXT"] = "1"
    return env


@dataclass(frozen=True)
class WorkloadSpec:
    name: str
    default_duration_ms: int
    default_deadline_us: int
    default_slo_class: int
    runner: Path
    inspired_by: tuple[str, ...]
    description: str


@dataclass(frozen=True)
class InvocationSpec:
    invocation_id: int
    workload: str
    actual_duration_ms: int
    deadline_us: int
    config: str
    expected_duration_ms: int | None = None
    action_name: str | None = None
    slo_class: int | None = None
    record_fields: dict[str, object] = field(default_factory=dict)


@dataclass
class StagedInvocation:
    output_json: Path
    stderr_file: TextIO
    process: subprocess.Popen[bytes]
    spec: InvocationSpec
    launch_start_ns: int
    metadata_ready_ns: int
    metadata_tgid: int
    metadata_tgids: list[int]
    metadata_key_visible: bool | None
    slo_class: int
    cgroup_path: Path | None


@dataclass(frozen=True)
class MixedWorkloadSpec:
    workload: str
    count: int
    duration_ms: int | None = None
    deadline_us: int | None = None
    slo_class: int | None = None


@dataclass(frozen=True)
class ReplayInvocationSpec:
    invocation_id: int
    at_ms: float
    function_hash: str
    workload: str
    duration_ms: int
    deadline_us: int
    slo_class: int


def default_deadline_us_for_duration(duration_ms: int) -> int:
    duration_us = duration_ms * 1_000
    return duration_us + max(duration_us, DEFAULT_SLO_MIN_SLACK_US)


def resolve_deadline_us(
    spec: WorkloadSpec,
    duration_ms: int,
    requested_deadline_us: int | None,
) -> int:
    if requested_deadline_us is not None:
        return requested_deadline_us
    if duration_ms == spec.default_duration_ms:
        return spec.default_deadline_us
    return default_deadline_us_for_duration(duration_ms)


WORKLOADS: dict[str, WorkloadSpec] = {
    "cpu_burst": WorkloadSpec(
        name="cpu_burst",
        default_duration_ms=DEFAULT_WORKLOAD_DURATION_MS,
        default_deadline_us=default_deadline_us_for_duration(
            DEFAULT_WORKLOAD_DURATION_MS
        ),
        default_slo_class=1,
        runner=BENCHMARK_WORKLOAD_BIN,
        inspired_by=("010.sleep",),
        description="calibrated dense matrix CPU burst for scheduler overhead and tail latency pressure",
    ),
    "sleep_short": WorkloadSpec(
        name="sleep_short",
        default_duration_ms=DEFAULT_WORKLOAD_DURATION_MS,
        default_deadline_us=default_deadline_us_for_duration(
            DEFAULT_WORKLOAD_DURATION_MS
        ),
        default_slo_class=1,
        runner=BENCHMARK_WORKLOAD_BIN,
        inspired_by=("010.sleep",),
        description="minimal baseline overhead calibration",
    ),
    "io_mixed": WorkloadSpec(
        name="io_mixed",
        default_duration_ms=DEFAULT_WORKLOAD_DURATION_MS,
        default_deadline_us=default_deadline_us_for_duration(
            DEFAULT_WORKLOAD_DURATION_MS
        ),
        default_slo_class=1,
        runner=BENCHMARK_WORKLOAD_BIN,
        inspired_by=("311.compression", "220.video-processing"),
        description="small CPU plus synchronous file IO mix",
    ),
    "memory_heavy": WorkloadSpec(
        name="memory_heavy",
        default_duration_ms=DEFAULT_WORKLOAD_DURATION_MS,
        default_deadline_us=default_deadline_us_for_duration(
            DEFAULT_WORKLOAD_DURATION_MS
        ),
        default_slo_class=1,
        runner=BENCHMARK_WORKLOAD_BIN,
        inspired_by=("411.image-recognition", "220.video-processing"),
        description="allocation and scanning workload that stresses memory bandwidth and cache locality",
    ),
    "network_heavy": WorkloadSpec(
        name="network_heavy",
        default_duration_ms=DEFAULT_WORKLOAD_DURATION_MS,
        default_deadline_us=default_deadline_us_for_duration(
            DEFAULT_WORKLOAD_DURATION_MS
        ),
        default_slo_class=1,
        runner=BENCHMARK_WORKLOAD_BIN,
        inspired_by=("120.uploader",),
        description="loopback TCP transfer workload for network wait and copy pressure",
    ),
    "pipeline": WorkloadSpec(
        name="pipeline",
        default_duration_ms=DEFAULT_WORKLOAD_DURATION_MS,
        default_deadline_us=default_deadline_us_for_duration(
            DEFAULT_WORKLOAD_DURATION_MS
        ),
        default_slo_class=1,
        runner=BENCHMARK_WORKLOAD_BIN,
        inspired_by=("120.uploader", "311.compression", "220.video-processing"),
        description="three-stage loopback fetch, CPU compute, and synchronous upload pipeline",
    ),
    "compression_mixed": WorkloadSpec(
        name="compression_mixed",
        default_duration_ms=DEFAULT_WORKLOAD_DURATION_MS,
        default_deadline_us=default_deadline_us_for_duration(
            DEFAULT_WORKLOAD_DURATION_MS
        ),
        default_slo_class=1,
        runner=BENCHMARK_WORKLOAD_BIN,
        inspired_by=("311.compression",),
        description="repeated compress/decompress cycles over moderately sized buffers",
    ),
    "graph_bfs": WorkloadSpec(
        name="graph_bfs",
        default_duration_ms=DEFAULT_WORKLOAD_DURATION_MS,
        default_deadline_us=default_deadline_us_for_duration(
            DEFAULT_WORKLOAD_DURATION_MS
        ),
        default_slo_class=1,
        runner=BENCHMARK_WORKLOAD_BIN,
        inspired_by=("503.graph-bfs", "501.graph-pagerank"),
        description="graph traversal workload with irregular memory access",
    ),
}


def workload_names() -> list[str]:
    return sorted(WORKLOADS)


def workload_spec(name: str) -> WorkloadSpec:
    try:
        return WORKLOADS[name]
    except KeyError as exc:
        raise KeyError(f"unknown workload: {name}") from exc


def workload_command(
    workload: str, duration_ms: int, warm_hold_ms: int = 0
) -> list[str]:
    spec = workload_spec(workload)
    command = [
        str(spec.runner),
        "--workload",
        workload,
        "--duration-ms",
        str(duration_ms),
    ]
    if warm_hold_ms > 0:
        command.extend(["--warm-hold-ms", str(warm_hold_ms)])
    if TIME_BIN.exists():
        return [
            str(TIME_BIN),
            "-f",
            "COSMOS_TIME real_s=%e user_s=%U sys_s=%S maxrss_kb=%M",
            *command,
        ]
    return command


def invocation_output_payload(
    spec: InvocationSpec,
    *,
    returncode: int,
    launch_start_ns: int,
    start_ns: int,
    end_ns: int,
    stderr_path: Path,
    metadata_ready_ns: int | None,
    metadata_tgid: int | None,
    metadata_tgids: list[int],
    metadata_key_visible: bool | None,
    time_stats: dict[str, float] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "invocation_id": spec.invocation_id,
        "status": "ok" if returncode == 0 else "failed",
        "exit_code": returncode,
        "launch_start_monotonic_ns": launch_start_ns,
        "start_monotonic_ns": start_ns,
        "end_monotonic_ns": end_ns,
        "duration_ms": (end_ns - start_ns) / 1_000_000.0,
        "metadata_ready_monotonic_ns": metadata_ready_ns,
        "metadata_setup_ms": (
            (metadata_ready_ns - launch_start_ns) / 1_000_000.0
            if metadata_ready_ns is not None
            else None
        ),
        "deadline_us": spec.deadline_us,
        "workload": spec.workload,
        "config": spec.config,
        "stderr_path": str(stderr_path),
        "metadata_tgid": metadata_tgid,
        "metadata_tgids": metadata_tgids,
        "metadata_key_visible": metadata_key_visible,
        "actual_duration_ms": spec.actual_duration_ms,
        "expected_duration_ms": spec.expected_duration_ms,
        "action_name": spec.action_name or spec.workload,
        "slo_class": spec.slo_class,
    }
    if time_stats is not None:
        payload.update(time_stats)
    payload.update(spec.record_fields)
    return payload


def parse_invocation_time_stats_file(path: Path) -> dict[str, float] | None:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None

    match = measure_latency.TIME_STATS_RE.search(text)
    if match is None:
        return None

    real_s = float(match.group("real_s"))
    user_s = float(match.group("user_s"))
    sys_s = float(match.group("sys_s"))
    return {
        "real_ms": real_s * 1_000.0,
        "user_ms": user_s * 1_000.0,
        "sys_ms": sys_s * 1_000.0,
        "cpu_ms": (user_s + sys_s) * 1_000.0,
        "maxrss_kb": float(match.group("maxrss_kb")),
    }


def gated_workload_command(command: list[str]) -> list[str]:
    if command and command[0] == str(TIME_BIN):
        time_prefix = command[:3]
        payload = command[3:]
        return [*time_prefix, "bash", "-c", GATE_SCRIPT, "cosmos-workload", *payload]
    return ["bash", "-c", GATE_SCRIPT, "cosmos-workload", *command]


def stopped_workload_command(command: list[str]) -> list[str]:
    """Wrap command so the payload process STOPs itself before exec.

    The harness detects the T (stopped) state, registers metadata with
    the event bridge, then sends SIGCONT.  After resuming, bash exec's
    the real payload which inherits the same PID/TGID — ensuring the
    BPF map key matches what the kernel sees at enqueue time.
    """
    if command and command[0] == str(TIME_BIN):
        time_prefix = command[:3]
        payload = command[3:]
        return [*time_prefix, "bash", "-c", STOP_SCRIPT, "cosmos-workload", *payload]
    return ["bash", "-c", STOP_SCRIPT, "cosmos-workload", *command]


WORKLOAD_PROFILE_HINTS: dict[str, dict[str, int | float]] = {
    "cpu_burst": {
        "cpu_intensity": 0.95,
        "io_weight": 400,
    },
    "sleep_short": {
        "cpu_intensity": 0.05,
        "io_weight": 100,
    },
    "io_mixed": {
        "cpu_intensity": 0.35,
        "io_weight": 700,
        "io_bandwidth_bytes_per_sec": 64 * 1024 * 1024,
    },
    "memory_heavy": {
        "cpu_intensity": 0.45,
        "memory_bytes": 256 * 1024 * 1024,
        "working_set_bytes": 128 * 1024 * 1024,
        "cold_load_penalty_ns": 2_000_000_000,
        "io_weight": 350,
    },
    "network_heavy": {
        "cpu_intensity": 0.30,
        "network_bandwidth_bytes_per_sec": 128 * 1024 * 1024,
        "io_weight": 300,
    },
    "pipeline": {
        "cpu_intensity": 0.50,
        "memory_bytes": 96 * 1024 * 1024,
        "working_set_bytes": 48 * 1024 * 1024,
        "cold_load_penalty_ns": 750_000_000,
        "io_weight": 650,
        "io_bandwidth_bytes_per_sec": 96 * 1024 * 1024,
        "network_bandwidth_bytes_per_sec": 96 * 1024 * 1024,
        "phase_sequence": [
            {"kind": "IoBound", "duration_pct": 33},
            {"kind": "CpuBound", "duration_pct": 34},
            {"kind": "IoBound", "duration_pct": 33},
        ],
    },
    "compression_mixed": {
        "cpu_intensity": 0.75,
        "memory_bytes": 128 * 1024 * 1024,
        "working_set_bytes": 64 * 1024 * 1024,
        "cold_load_penalty_ns": 1_000_000_000,
        "io_weight": 600,
        "phase_sequence": [
            {"kind": "MemoryBound", "duration_pct": 20},
            {"kind": "CpuBound", "duration_pct": 60},
            {"kind": "IoBound", "duration_pct": 20},
        ],
    },
    "graph_bfs": {
        "cpu_intensity": 0.55,
        "memory_bytes": 192 * 1024 * 1024,
        "working_set_bytes": 96 * 1024 * 1024,
        "cold_load_penalty_ns": 1_500_000_000,
        "io_weight": 300,
    },
}


def profile_id_for_workload(workload: str) -> str | None:
    if workload in WORKLOAD_PROFILE_HINTS:
        return workload
    return None


def profile_hints_for_workload(workload: str) -> dict:
    if not INLINE_PROFILE_HINTS:
        return {}
    return dict(WORKLOAD_PROFILE_HINTS.get(workload, {}))


def metadata_profile_fields_for_workload(workload: str) -> dict:
    fields: dict[str, object] = {}
    profile_id = profile_id_for_workload(workload)
    if profile_id is not None:
        fields["profile_id"] = profile_id
    profile_hints = profile_hints_for_workload(workload)
    if profile_hints:
        fields["profile_hints"] = profile_hints
    return fields


def send_event_bridge_event(port: int, event: dict) -> None:
    payload = json.dumps(event).encode("utf-8") + b"\n"
    with socket.create_connection(
        ("127.0.0.1", port), timeout=EVENT_BRIDGE_EVENT_TIMEOUT_S
    ) as conn:
        conn.sendall(payload)
        raw = b""
        while not raw.endswith(b"\n"):
            chunk = conn.recv(1024)
            if not chunk:
                break
            raw += chunk
        response = raw.decode("utf-8", errors="replace").strip()
        if response != "ok":
            raise RuntimeError(
                f"event bridge rejected event {event!r}: {response or 'no response'}"
            )


def try_send_event_bridge_event(port: int, event: dict) -> str | None:
    try:
        send_event_bridge_event(port, event)
        return None
    except Exception as exc:
        return str(exc)


def process_state(pid: int) -> str | None:
    status_path = Path("/proc") / str(pid) / "status"
    try:
        for line in status_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("State:"):
                return line
    except FileNotFoundError:
        return None
    return None


def child_pids(pid: int) -> list[int]:
    children_path = Path("/proc") / str(pid) / "task" / str(pid) / "children"
    try:
        raw = children_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return []
    if not raw:
        return []
    return [int(child) for child in raw.split()]


def descendant_pids(pid: int) -> list[int]:
    seen: set[int] = set()
    queue = [pid]
    ordered: list[int] = []
    while queue:
        current = queue.pop(0)
        if current in seen:
            continue
        seen.add(current)
        ordered.append(current)
        queue.extend(child_pids(current))
    return ordered


def benchmark_cgroup_root() -> Path:
    return Path(os.environ.get("COSMOS_CGROUP_POLICY_ROOT", DEFAULT_CGROUP_POLICY_ROOT))


def create_benchmark_cgroup(output_json: Path, invocation_id: int) -> Path | None:
    if not BENCH_CGROUPS_ENABLED:
        return None
    root = benchmark_cgroup_root()
    root.mkdir(parents=True, exist_ok=True)
    run_id = output_json.parent.parent.name
    cgroup = root / f"bench-{run_id}-{invocation_id}"
    cgroup.mkdir(exist_ok=True)
    return cgroup


def assign_pids_to_cgroup(cgroup: Path | None, pids: Iterable[int]) -> None:
    if cgroup is None:
        return
    procs = cgroup / "cgroup.procs"
    for pid in pids:
        try:
            procs.write_text(f"{pid}\n", encoding="utf-8")
        except ProcessLookupError:
            continue


def cleanup_benchmark_cgroups(run_dir: Path) -> None:
    root = benchmark_cgroup_root()
    invocations_dir = run_dir / "invocations"
    if not invocations_dir.exists():
        return
    for path in sorted(invocations_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        cgroup_path = payload.get("cgroup_path")
        if not cgroup_path:
            continue
        cgroup = Path(cgroup_path)
        try:
            resolved_root = root.resolve()
            resolved_cgroup = cgroup.resolve()
        except OSError:
            continue
        if resolved_cgroup != resolved_root and resolved_cgroup.is_relative_to(resolved_root):
            try:
                cgroup.rmdir()
            except OSError:
                pass


def wait_for_process_stopped(pid: int, timeout_s: float = 2.0) -> int:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if process_state(pid) is None:
            raise RuntimeError(
                f"workload pid {pid} exited before metadata registration"
            )
        for candidate in descendant_pids(pid):
            state = process_state(candidate)
            if state is not None and ("\tT" in state or "\tt" in state):
                return candidate
        time.sleep(0.005)
    raise TimeoutError(f"timed out waiting for workload pid {pid} to stop")


def wait_for_metadata_process(
    pid: int, expect_child: bool, timeout_s: float = 2.0
) -> int:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if process_state(pid) is None:
            raise RuntimeError(
                f"workload pid {pid} exited before metadata registration"
            )
        descendants = descendant_pids(pid)
        if expect_child:
            children = [candidate for candidate in descendants if candidate != pid]
            if children:
                return children[-1]
        else:
            return pid
        time.sleep(0.005)
    raise TimeoutError(f"timed out waiting for workload child of pid {pid}")


def dump_has_invocation_map(output_path: Path) -> None:
    if not BPFTOOL_BIN.exists():
        output_path.write_text("bpftool not installed\n", encoding="utf-8")
        return
    completed = subprocess.run(
        [
            str(BPFTOOL_BIN),
            "-j",
            "map",
            "dump",
            "pinned",
            str(BPF_HAS_INVOCATION_PATH),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
        text=True,
    )
    output_path.write_text(completed.stdout, encoding="utf-8")


def invocation_meta_key_visible(tgid: int) -> bool:
    if not BPFTOOL_BIN.exists():
        return True
    completed = subprocess.run(
        [
            str(BPFTOOL_BIN),
            "-j",
            "map",
            "dump",
            "pinned",
            str(BPF_HAS_INVOCATION_PATH),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        return False
    try:
        entries = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return False
    return any(entry.get("formatted", {}).get("key") == tgid for entry in entries)


def wait_for_invocation_meta_key(tgid: int, timeout_s: float = 0.2) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if invocation_meta_key_visible(tgid):
            return True
        time.sleep(0.005)
    return invocation_meta_key_visible(tgid)


def timestamped_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def ensure_release_build() -> None:
    scheduler = REPO_ROOT / "target" / "release" / "cosmos"
    sources = [
        REPO_ROOT / "src" / "main.rs",
        REPO_ROOT / "src" / "bpf.rs",
        REPO_ROOT / "src" / "actuator" / "mod.rs",
        REPO_ROOT / "src" / "coordinator" / "mod.rs",
        REPO_ROOT / "src" / "coordinator" / "phase_tracker.rs",
        REPO_ROOT / "src" / "coordinator" / "policy.rs",
        REPO_ROOT / "src" / "coordinator" / "slack.rs",
        REPO_ROOT / "src" / "metadata.rs",
        REPO_ROOT / "src" / "policy" / "cosmos.rs",
        REPO_ROOT / "src" / "policy" / "sfs.rs",
        REPO_ROOT / "src" / "policy" / "mod.rs",
        REPO_ROOT / "src" / "registry" / "store.rs",
        REPO_ROOT / "src" / "registry" / "types.rs",
        REPO_ROOT / "src" / "scheduler.rs",
        REPO_ROOT / "src" / "stats.rs",
        REPO_ROOT / "cosmos-metadata-model" / "src" / "lib.rs",
        REPO_ROOT / "rust" / "scx_rustland_core" / "assets" / "bpf.rs",
        REPO_ROOT / "rust" / "scx_rustland_core" / "assets" / "bpf" / "main.bpf.c",
        REPO_ROOT / "rust" / "scx_rustland_core" / "assets" / "bpf" / "intf.h",
        REPO_ROOT / "Cargo.toml",
    ]
    if scheduler.exists() and scheduler.stat().st_mtime >= max(
        path.stat().st_mtime for path in sources
    ):
        return
    subprocess.run(
        [
            str(CARGO_BIN),
            "build",
            "--release",
            "--manifest-path",
            str(REPO_ROOT / "Cargo.toml"),
        ],
        check=True,
        cwd=REPO_ROOT,
    )


def ensure_event_bridge_build() -> None:
    bridge = REPO_ROOT / "target" / "release" / "cosmos-event-bridge"
    sources = [
        REPO_ROOT / "cosmos-event-bridge" / "src" / "main.rs",
        REPO_ROOT / "cosmos-event-bridge" / "src" / "metadata_writer.rs",
        REPO_ROOT / "cosmos-event-bridge" / "Cargo.toml",
    ]
    if bridge.exists() and bridge.stat().st_mtime >= max(
        path.stat().st_mtime for path in sources
    ):
        return
    subprocess.run(
        [str(CARGO_BIN), "build", "--release", "-p", "cosmos-event-bridge"],
        check=True,
        cwd=REPO_ROOT,
    )


def ensure_benchmark_workload_build() -> None:
    sources = [
        REPO_ROOT / "benchmarks" / "workloads" / "runner" / "Cargo.toml",
        REPO_ROOT / "benchmarks" / "workloads" / "runner" / "src" / "main.rs",
    ]
    if BENCHMARK_WORKLOAD_BIN.exists() and BENCHMARK_WORKLOAD_BIN.stat().st_mtime >= max(
        path.stat().st_mtime for path in sources
    ):
        return
    subprocess.run(
        [str(CARGO_BIN), "build", "--release", "-p", "cosmos-benchmark-workload"],
        check=True,
        cwd=REPO_ROOT,
    )


def run_workload_invocation(
    output_json: Path,
    workload: str,
    duration_ms: int,
    deadline_us: int,
    invocation_id: int,
    use_metadata: bool,
    config: str,
    metadata_bridge_port: int | None = None,
    slo_class: int | None = None,
    workload_cpus: Iterable[int] | None = None,
    workload_sched_ext: bool = False,
) -> int:
    spec = InvocationSpec(
        invocation_id=invocation_id,
        workload=workload,
        actual_duration_ms=duration_ms,
        deadline_us=deadline_us,
        config=config,
        slo_class=slo_class,
    )
    return run_invocation_spec(
        output_json,
        spec,
        use_metadata,
        metadata_bridge_port,
        workload_cpus=workload_cpus,
        workload_sched_ext=workload_sched_ext,
    )


def run_invocation_spec(
    output_json: Path,
    spec: InvocationSpec,
    use_metadata: bool,
    metadata_bridge_port: int | None = None,
    workload_cpus: Iterable[int] | None = None,
    workload_sched_ext: bool = False,
) -> int:
    stderr_path = output_json.with_suffix(".stderr")
    command = with_cpu_affinity(
        workload_command(spec.workload, spec.actual_duration_ms),
        workload_cpus,
    )
    workload = workload_spec(spec.workload)
    launch_start_ns = time.monotonic_ns()
    start_ns = launch_start_ns
    metadata_ready_ns = None
    env = workload_env(workload_sched_ext)
    process: subprocess.Popen[bytes] | None = None
    metadata_tgid = None
    metadata_tgids: list[int] = []
    metadata_key_visible = None

    if use_metadata:
        if metadata_bridge_port is None:
            raise RuntimeError("metadata invocations require a running event bridge")
        env = workload_env(workload_sched_ext, os.environ.copy())
        command = stopped_workload_command(command)
    metadata_cleanup_errors: list[str] = []

    try:
        with stderr_path.open("w", encoding="utf-8") as stderr_file:
            if metadata_bridge_port is None:
                completed = subprocess.run(
                    command,
                    env=env,
                    stdout=subprocess.DEVNULL,
                    stderr=stderr_file,
                    check=False,
                )
                returncode = completed.returncode
            else:
                process = subprocess.Popen(
                    command,
                    env=env,
                    stdout=subprocess.DEVNULL,
                    stderr=stderr_file,
                )
                metadata_tgid = wait_for_process_stopped(process.pid)
                metadata_tgids = list(dict.fromkeys([process.pid, metadata_tgid]))
                for tgid in metadata_tgids:
                    send_event_bridge_event(
                        metadata_bridge_port,
                        {
                            "type": "local_start",
                            "activation_id": f"{spec.config}-{spec.action_name or spec.workload}-{spec.invocation_id}",
                            "tgid": tgid,
                            "timeout_ms": max(1, spec.deadline_us // 1_000),
                            "estimated_duration_ms": spec.expected_duration_ms
                            if spec.expected_duration_ms is not None
                            else spec.actual_duration_ms,
                            "slo_class": (
                                spec.slo_class
                                if spec.slo_class is not None
                                else workload.default_slo_class
                            ),
                            "action_name": spec.action_name or spec.workload,
                            "kind": "local-rust",
                            "cold_start": False,
                            **metadata_profile_fields_for_workload(spec.workload),
                        },
                    )
                if DEBUG_BPF_MAP:
                    metadata_key_visible = wait_for_invocation_meta_key(metadata_tgid)
                    dump_has_invocation_map(output_json.with_suffix(".bpfmap.json"))
                metadata_ready_ns = time.monotonic_ns()
                start_ns = metadata_ready_ns
                os.kill(metadata_tgid, signal.SIGCONT)
                returncode = process.wait()
                for tgid in reversed(metadata_tgids):
                    err = try_send_event_bridge_event(
                        metadata_bridge_port,
                        {
                            "type": "local_end",
                            "activation_id": f"{spec.config}-{spec.action_name or spec.workload}-{spec.invocation_id}",
                            "tgid": tgid,
                        },
                    )
                    if err is not None:
                        metadata_cleanup_errors.append(err)
    except Exception:
        if process is not None:
            stop_process(process, signal.SIGKILL)
        raise

    end_ns = time.monotonic_ns()
    payload = invocation_output_payload(
        spec,
        returncode=returncode,
        launch_start_ns=launch_start_ns,
        start_ns=start_ns,
        end_ns=end_ns,
        stderr_path=stderr_path,
        metadata_ready_ns=metadata_ready_ns,
        metadata_tgid=metadata_tgid,
        metadata_tgids=metadata_tgids,
        metadata_key_visible=metadata_key_visible,
        time_stats=parse_invocation_time_stats_file(stderr_path),
    )
    payload["cgroup_path"] = None
    if metadata_cleanup_errors:
        payload["metadata_cleanup_errors"] = metadata_cleanup_errors
    output_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return returncode


def stage_metadata_bridge_invocation(
    output_json: Path,
    workload: str,
    duration_ms: int,
    deadline_us: int,
    invocation_id: int,
    config: str,
    metadata_bridge_port: int,
    slo_class: int | None = None,
    warm_hold_ms: int = 0,
    workload_cpus: Iterable[int] | None = None,
    workload_sched_ext: bool = False,
) -> StagedInvocation:
    spec = InvocationSpec(
        invocation_id=invocation_id,
        workload=workload,
        actual_duration_ms=duration_ms,
        deadline_us=deadline_us,
        config=config,
        slo_class=slo_class,
    )
    return stage_invocation_spec(
        output_json,
        spec,
        metadata_bridge_port,
        workload_cpus=workload_cpus,
        workload_sched_ext=workload_sched_ext,
        warm_hold_ms=warm_hold_ms,
    )


def stage_invocation_spec(
    output_json: Path,
    spec: InvocationSpec,
    metadata_bridge_port: int,
    workload_cpus: Iterable[int] | None = None,
    workload_sched_ext: bool = False,
    warm_hold_ms: int = 0,
) -> StagedInvocation:
    stderr_path = output_json.with_suffix(".stderr")
    command = stopped_workload_command(
        with_cpu_affinity(
            workload_command(spec.workload, spec.actual_duration_ms, warm_hold_ms),
            workload_cpus,
        )
    )
    workload = workload_spec(spec.workload)
    launch_start_ns = time.monotonic_ns()
    stderr_file = stderr_path.open("w", encoding="utf-8")
    process: subprocess.Popen[bytes] | None = None
    effective_slo_class = (
        spec.slo_class if spec.slo_class is not None else workload.default_slo_class
    )
    try:
        process = subprocess.Popen(
            command,
            env=workload_env(workload_sched_ext, os.environ.copy()),
            stdout=subprocess.DEVNULL,
            stderr=stderr_file,
        )
        metadata_tgid = wait_for_process_stopped(process.pid)
        metadata_tgids = list(dict.fromkeys([process.pid, metadata_tgid]))
        cgroup_path = create_benchmark_cgroup(output_json, spec.invocation_id)
        assign_pids_to_cgroup(cgroup_path, metadata_tgids)
        for tgid in metadata_tgids:
            send_event_bridge_event(
                metadata_bridge_port,
                {
                    "type": "local_start",
                    "activation_id": f"{spec.config}-{spec.action_name or spec.workload}-{spec.invocation_id}",
                    "tgid": tgid,
                    "timeout_ms": max(1, spec.deadline_us // 1_000),
                    "estimated_duration_ms": (
                        spec.expected_duration_ms
                        if spec.expected_duration_ms is not None
                        else spec.actual_duration_ms
                    ),
                    "slo_class": (
                        spec.slo_class
                        if spec.slo_class is not None
                        else workload.default_slo_class
                    ),
                    "action_name": spec.action_name or spec.workload,
                    "kind": "local-rust",
                    "cold_start": False,
                    **metadata_profile_fields_for_workload(spec.workload),
                },
            )
        metadata_key_visible = None
        if DEBUG_BPF_MAP:
            metadata_key_visible = wait_for_invocation_meta_key(metadata_tgid)
            dump_has_invocation_map(output_json.with_suffix(".bpfmap.json"))
        metadata_ready_ns = time.monotonic_ns()
        return StagedInvocation(
            output_json=output_json,
            stderr_file=stderr_file,
            process=process,
            spec=spec,
            launch_start_ns=launch_start_ns,
            metadata_ready_ns=metadata_ready_ns,
            metadata_tgid=metadata_tgid,
            metadata_tgids=metadata_tgids,
            metadata_key_visible=metadata_key_visible,
            slo_class=effective_slo_class,
            cgroup_path=cgroup_path,
        )
    except Exception:
        stop_process(process, signal.SIGKILL)
        stderr_file.close()
        raise


def complete_metadata_bridge_invocation(
    staged: StagedInvocation,
    start_ns: int,
    metadata_bridge_port: int,
) -> int:
    returncode = staged.process.wait()
    end_ns = time.monotonic_ns()
    metadata_cleanup_errors = []
    for tgid in reversed(staged.metadata_tgids):
        err = try_send_event_bridge_event(
            metadata_bridge_port,
            {
                "type": "local_end",
                "activation_id": f"{staged.spec.config}-{staged.spec.action_name or staged.spec.workload}-{staged.spec.invocation_id}",
                "tgid": tgid,
            },
        )
        if err is not None:
            metadata_cleanup_errors.append(err)
    staged.stderr_file.close()

    payload = invocation_output_payload(
        staged.spec,
        returncode=returncode,
        launch_start_ns=staged.launch_start_ns,
        start_ns=start_ns,
        end_ns=end_ns,
        stderr_path=staged.output_json.with_suffix(".stderr"),
        metadata_ready_ns=staged.metadata_ready_ns,
        metadata_tgid=staged.metadata_tgid,
        metadata_tgids=staged.metadata_tgids,
        metadata_key_visible=staged.metadata_key_visible,
        time_stats=parse_invocation_time_stats_file(
            staged.output_json.with_suffix(".stderr")
        ),
    )
    payload["cgroup_path"] = (
        str(staged.cgroup_path) if staged.cgroup_path is not None else None
    )
    if metadata_cleanup_errors:
        payload["metadata_cleanup_errors"] = metadata_cleanup_errors
    staged.output_json.write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8"
    )
    return returncode


def cleanup_staged_invocations(staged: Iterable[StagedInvocation]) -> None:
    for invocation in staged:
        stop_process(invocation.process, signal.SIGKILL)
        invocation.stderr_file.close()


def run_metadata_bridge_invocations(
    run_dir: Path,
    workload: str,
    concurrency: int,
    duration_ms: int,
    deadline_us: int,
    config: str,
    metadata_bridge_port: int,
    slo_class: int | None = None,
    workload_cpus: Iterable[int] | None = None,
    workload_sched_ext: bool = False,
) -> int:
    staged_invocations: list[StagedInvocation] = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(
                    stage_metadata_bridge_invocation,
                    run_dir / "invocations" / f"{invocation_id}.json",
                    workload,
                    duration_ms,
                    deadline_us,
                    invocation_id,
                    config,
                    metadata_bridge_port,
                    slo_class=slo_class,
                    workload_cpus=workload_cpus,
                    workload_sched_ext=workload_sched_ext,
                )
                for invocation_id in range(1, concurrency + 1)
            ]
            for future in concurrent.futures.as_completed(futures):
                staged_invocations.append(future.result())

        staged_invocations.sort(key=lambda invocation: invocation.spec.invocation_id)
        start_ns = time.monotonic_ns()
        for invocation in staged_invocations:
            os.kill(invocation.metadata_tgid, signal.SIGCONT)

        failures = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(
                    complete_metadata_bridge_invocation,
                    invocation,
                    start_ns,
                    metadata_bridge_port,
                )
                for invocation in staged_invocations
            ]
            for future in concurrent.futures.as_completed(futures):
                if future.result() != 0:
                    failures += 1
        return failures
    except Exception:
        cleanup_staged_invocations(staged_invocations)
        raise


def parse_duration_us(value: str) -> int:
    normalized = value.strip().lower()
    if normalized in {"inf", "infinite", "infinity"}:
        return 24 * 60 * 60 * 1_000_000
    for suffix, multiplier in (
        ("us", 1),
        ("ms", 1_000),
        ("s", 1_000_000),
    ):
        if normalized.endswith(suffix):
            return int(float(normalized[: -len(suffix)]) * multiplier)
    return int(normalized)


def split_mix_parts(mix_str: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    start = 0
    for idx, char in enumerate(mix_str):
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            parts.append(mix_str[start:idx])
            start = idx + 1
    parts.append(mix_str[start:])
    return parts


def parse_mix_spec(mix_str: str) -> list[MixedWorkloadSpec]:
    """Parse 'cpu_burst:8(slo=0),cpu_burst:16(slo=2)' into workload specs."""
    specs: list[MixedWorkloadSpec] = []
    for part in split_mix_parts(mix_str):
        part = part.strip()
        if not part:
            continue
        options: dict[str, str] = {}
        if part.endswith(")") and "(" in part:
            part, raw_options = part[:-1].split("(", 1)
            for item in raw_options.split(";"):
                for option in item.split(","):
                    option = option.strip()
                    if not option:
                        continue
                    key, value = option.split("=", 1)
                    options[key.strip()] = value.strip()
        workload, count = part.rsplit(":", 1)
        duration_ms = options.get("duration_ms")
        deadline = (
            options.get("deadline_us")
            or options.get("deadline")
            or options.get("timeout_us")
        )
        deadline_ms = options.get("deadline_ms") or options.get("timeout_ms")
        if deadline is None and deadline_ms is not None:
            deadline = f"{deadline_ms}ms"
        slo_class = options.get("slo_class") or options.get("slo")
        specs.append(
            MixedWorkloadSpec(
                workload=workload.strip(),
                count=int(count.strip()),
                duration_ms=int(duration_ms) if duration_ms is not None else None,
                deadline_us=parse_duration_us(deadline) if deadline is not None else None,
                slo_class=int(slo_class) if slo_class is not None else None,
            )
        )
    return specs


def load_replay_plan(path: Path) -> list[ReplayInvocationSpec]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    invocations = raw.get("invocations")
    if not isinstance(invocations, list):
        raise ValueError(f"replay plan {path} must contain an invocations array")
    specs: list[ReplayInvocationSpec] = []
    for idx, item in enumerate(invocations, start=1):
        workload = str(item["workload"])
        workload_spec(workload)
        duration_ms = int(item.get("duration_ms") or DEFAULT_WORKLOAD_DURATION_MS)
        deadline_us = int(
            item.get("deadline_us") or default_deadline_us_for_duration(duration_ms)
        )
        specs.append(
            ReplayInvocationSpec(
                invocation_id=int(item.get("invocation_id") or idx),
                at_ms=float(item.get("at_ms") or 0.0),
                function_hash=str(item.get("function_hash") or workload),
                workload=workload,
                duration_ms=duration_ms,
                deadline_us=deadline_us,
                slo_class=int(item.get("slo_class", workload_spec(workload).default_slo_class)),
            )
        )
    specs.sort(key=lambda spec: (spec.at_ms, spec.invocation_id))
    return specs


def run_replay_invocations(
    run_dir: Path,
    replay_plan: Path,
    use_metadata: bool,
    config: str,
    metadata_bridge_port: int | None = None,
) -> int:
    run_dir.joinpath("invocations").mkdir(parents=True, exist_ok=True)
    ensure_benchmark_workload_build()
    if use_metadata and metadata_bridge_port is None:
        raise RuntimeError("metadata replay requires a running event bridge")

    specs = load_replay_plan(replay_plan)
    (run_dir / "replay_plan_used.json").write_text(
        replay_plan.read_text(encoding="utf-8"), encoding="utf-8"
    )
    if not specs:
        write_client_latency_csv(run_dir)
        return 0

    base_ns = time.monotonic_ns()

    def run_one(spec: ReplayInvocationSpec) -> int:
        target_ns = base_ns + int(max(0.0, spec.at_ms) * 1_000_000)
        sleep_ns = target_ns - time.monotonic_ns()
        if sleep_ns > 0:
            time.sleep(sleep_ns / 1_000_000_000)
        output_json = run_dir / "invocations" / f"{spec.invocation_id}.json"
        if use_metadata:
            staged = stage_metadata_bridge_invocation(
                output_json,
                spec.workload,
                spec.duration_ms,
                spec.deadline_us,
                spec.invocation_id,
                config,
                metadata_bridge_port,
                spec.slo_class,
            )
            start_ns = time.monotonic_ns()
            os.kill(staged.metadata_tgid, signal.SIGCONT)
            return complete_metadata_bridge_invocation(
                staged, start_ns, metadata_bridge_port
            )
        return run_workload_invocation(
            output_json,
            spec.workload,
            spec.duration_ms,
            spec.deadline_us,
            spec.invocation_id,
            False,
            config,
            None,
            spec.slo_class,
        )

    failures = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(specs)) as executor:
        futures = [executor.submit(run_one, spec) for spec in specs]
        for future in concurrent.futures.as_completed(futures):
            if future.result() != 0:
                failures += 1
    write_client_latency_csv(run_dir)
    return failures


def run_mixed_metadata_bridge_invocations(
    run_dir: Path,
    mix_specs: list[MixedWorkloadSpec],
    deadline_us: int,
    config: str,
    metadata_bridge_port: int,
    slo_class: int | None = None,
    workload_cpus: Iterable[int] | None = None,
    workload_sched_ext: bool = False,
) -> int:
    total = sum(spec.count for spec in mix_specs)
    staged_invocations: list[StagedInvocation] = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=total) as executor:
            futures: list[concurrent.futures.Future[StagedInvocation]] = []
            inv_id = 1
            for mix in mix_specs:
                spec = workload_spec(mix.workload)
                duration_ms = mix.duration_ms or spec.default_duration_ms
                invocation_deadline_us = mix.deadline_us or deadline_us
                invocation_slo_class = (
                    mix.slo_class if mix.slo_class is not None else slo_class
                )
                for _ in range(mix.count):
                    futures.append(
                        executor.submit(
                            stage_metadata_bridge_invocation,
                            run_dir / "invocations" / f"{inv_id}.json",
                            mix.workload,
                            duration_ms,
                            invocation_deadline_us,
                            inv_id,
                            config,
                            metadata_bridge_port,
                            slo_class=invocation_slo_class,
                            workload_cpus=workload_cpus,
                            workload_sched_ext=workload_sched_ext,
                        )
                    )
                    inv_id += 1
            for future in concurrent.futures.as_completed(futures):
                staged_invocations.append(future.result())

        staged_invocations.sort(key=lambda invocation: invocation.spec.invocation_id)
        start_ns = time.monotonic_ns()
        for invocation in staged_invocations:
            os.kill(invocation.metadata_tgid, signal.SIGCONT)

        failures = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=total) as executor:
            futures = [
                executor.submit(
                    complete_metadata_bridge_invocation,
                    invocation,
                    start_ns,
                    metadata_bridge_port,
                )
                for invocation in staged_invocations
            ]
            for future in concurrent.futures.as_completed(futures):
                if future.result() != 0:
                    failures += 1
        return failures
    except Exception:
        cleanup_staged_invocations(staged_invocations)
        raise


def run_mixed_direct_invocations(
    run_dir: Path,
    mix_specs: list[MixedWorkloadSpec],
    deadline_us: int,
    use_metadata: bool,
    config: str,
    metadata_bridge_port: int | None = None,
    slo_class: int | None = None,
    workload_cpus: Iterable[int] | None = None,
    workload_sched_ext: bool = False,
) -> int:
    total = sum(spec.count for spec in mix_specs)
    failures = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=total) as executor:
        futures: list[concurrent.futures.Future[int]] = []
        inv_id = 1
        for mix in mix_specs:
            spec = workload_spec(mix.workload)
            duration_ms = mix.duration_ms or spec.default_duration_ms
            invocation_deadline_us = mix.deadline_us or deadline_us
            invocation_slo_class = (
                mix.slo_class if mix.slo_class is not None else slo_class
            )
            for _ in range(mix.count):
                futures.append(
                    executor.submit(
                        run_workload_invocation,
                        run_dir / "invocations" / f"{inv_id}.json",
                        mix.workload,
                        duration_ms,
                        invocation_deadline_us,
                        inv_id,
                        use_metadata,
                        config,
                        metadata_bridge_port,
                        invocation_slo_class,
                        workload_cpus,
                        workload_sched_ext,
                    )
                )
                inv_id += 1
        for future in concurrent.futures.as_completed(futures):
            if future.result() != 0:
                failures += 1
    return failures


def write_manifest(
    run_dir: Path,
    config: str,
    workload: str,
    concurrency: int,
    duration_ms: int,
    deadline_us: int,
    metadata_mode: str,
    scheduler_flags: Iterable[str],
    slo_class: int | None = None,
) -> None:
    payload: dict = {
        "config": config,
        "workload": workload,
        "concurrency": concurrency,
        "duration_ms": duration_ms,
        "deadline_us": deadline_us,
        "cpu_cores": os.cpu_count() or 1,
        "metadata_mode": metadata_mode,
        "scheduler_flags": list(scheduler_flags),
        "slo_class_override": slo_class,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if "," not in workload and workload in WORKLOADS:
        spec = workload_spec(workload)
        payload["workload_description"] = spec.description
        payload["inspired_by_sebs"] = list(spec.inspired_by)
    (run_dir / "manifest.json").write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8"
    )


def write_client_latency_csv(run_dir: Path) -> None:
    rows = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((run_dir / "invocations").glob("*.json"))
        if path.stem.isdigit()
    ]
    fieldnames = [
        "invocation_id",
        "status",
        "exit_code",
        "launch_start_monotonic_ns",
        "start_monotonic_ns",
        "end_monotonic_ns",
        "duration_ms",
        "metadata_ready_monotonic_ns",
        "metadata_setup_ms",
        "deadline_us",
        "workload",
        "config",
        "stderr_path",
        "metadata_tgid",
        "metadata_tgids",
        "metadata_key_visible",
        "actual_duration_ms",
        "expected_duration_ms",
        "action_name",
        "slo_class",
    ]
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)

    with (run_dir / "client_latency.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=fieldnames,
        )
        writer.writeheader()
        writer.writerows(rows)


def refresh_latest_link(config_root: Path, run_dir: Path) -> None:
    config_root.mkdir(parents=True, exist_ok=True)
    latest = config_root / "latest"
    if latest.exists() or latest.is_symlink():
        latest.unlink()
    latest.symlink_to(run_dir.name)


def run_invocations(
    run_dir: Path,
    workload: str,
    concurrency: int,
    duration_ms: int,
    deadline_us: int,
    use_metadata: bool,
    config: str,
    metadata_bridge_port: int | None = None,
    slo_class: int | None = None,
    workload_cpus: Iterable[int] | None = None,
    workload_sched_ext: bool = False,
) -> int:
    run_dir.joinpath("invocations").mkdir(parents=True, exist_ok=True)
    ensure_benchmark_workload_build()
    if use_metadata and metadata_bridge_port is None:
        raise RuntimeError("metadata invocations require a running event bridge")

    # Heterogeneous: workload string contains commas → mixed workload types
    if "," in workload:
        mix_specs = parse_mix_spec(workload)
        if use_metadata and metadata_bridge_port is not None:
            failures = run_mixed_metadata_bridge_invocations(
                run_dir,
                mix_specs,
                deadline_us,
                config,
                metadata_bridge_port,
                slo_class,
                workload_cpus,
                workload_sched_ext,
            )
        else:
            failures = run_mixed_direct_invocations(
                run_dir,
                mix_specs,
                deadline_us,
                use_metadata,
                config,
                metadata_bridge_port,
                slo_class,
                workload_cpus,
                workload_sched_ext,
            )
        write_client_latency_csv(run_dir)
        return failures

    if use_metadata and metadata_bridge_port is not None:
        failures = run_metadata_bridge_invocations(
            run_dir,
            workload,
            concurrency,
            duration_ms,
            deadline_us,
            config,
            metadata_bridge_port,
            slo_class,
            workload_cpus,
            workload_sched_ext,
        )
        write_client_latency_csv(run_dir)
        return failures

    failures = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(
                run_workload_invocation,
                run_dir / "invocations" / f"{invocation_id}.json",
                workload,
                duration_ms,
                deadline_us,
                invocation_id,
                use_metadata,
                config,
                metadata_bridge_port,
                slo_class,
                workload_cpus,
                workload_sched_ext,
            )
            for invocation_id in range(1, concurrency + 1)
        ]
        for future in concurrent.futures.as_completed(futures):
            if future.result() != 0:
                failures += 1
    write_client_latency_csv(run_dir)
    return failures


def run_invocation_specs(
    run_dir: Path,
    invocations: list[InvocationSpec],
    use_metadata: bool,
    metadata_bridge_port: int | None = None,
    workload_cpus: Iterable[int] | None = None,
    workload_sched_ext: bool = False,
) -> int:
    run_dir.joinpath("invocations").mkdir(parents=True, exist_ok=True)
    ensure_benchmark_workload_build()
    if use_metadata and metadata_bridge_port is None:
        raise RuntimeError("metadata invocations require a running event bridge")

    failures = 0
    if use_metadata and metadata_bridge_port is not None:
        staged_invocations: list[StagedInvocation] = []
        try:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max(1, len(invocations))
            ) as executor:
                futures = [
                    executor.submit(
                        stage_invocation_spec,
                        run_dir / "invocations" / f"{spec.invocation_id}.json",
                        spec,
                        metadata_bridge_port,
                        workload_cpus,
                        workload_sched_ext,
                    )
                    for spec in invocations
                ]
                for future in concurrent.futures.as_completed(futures):
                    staged_invocations.append(future.result())

            staged_invocations.sort(key=lambda invocation: invocation.spec.invocation_id)
            start_ns = time.monotonic_ns()
            for invocation in staged_invocations:
                os.kill(invocation.metadata_tgid, signal.SIGCONT)

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max(1, len(staged_invocations))
            ) as executor:
                futures = [
                    executor.submit(
                        complete_metadata_bridge_invocation,
                        invocation,
                        start_ns,
                        metadata_bridge_port,
                    )
                    for invocation in staged_invocations
                ]
                for future in concurrent.futures.as_completed(futures):
                    if future.result() != 0:
                        failures += 1
        except Exception:
            cleanup_staged_invocations(staged_invocations)
            raise
    else:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max(1, len(invocations))
        ) as executor:
            futures = [
                executor.submit(
                    run_invocation_spec,
                    run_dir / "invocations" / f"{spec.invocation_id}.json",
                    spec,
                    use_metadata,
                    metadata_bridge_port,
                    workload_cpus=workload_cpus,
                    workload_sched_ext=workload_sched_ext,
                )
                for spec in invocations
            ]
            for future in concurrent.futures.as_completed(futures):
                if future.result() != 0:
                    failures += 1

    write_client_latency_csv(run_dir)
    return failures


def summarize_run(run_dir: Path) -> dict:
    return measure_latency.summarize_run(run_dir)


def wait_for_scheduler_stats(
    socket_path: Path, scheduler: subprocess.Popen[bytes], log_path: Path
) -> None:
    deadline = time.monotonic() + SCHEDULER_STATS_READY_TIMEOUT_S
    while time.monotonic() < deadline:
        if scheduler.poll() is not None:
            raise RuntimeError(log_path.read_text(encoding="utf-8", errors="replace"))
        try:
            measure_latency.request_scheduler_stats(socket_path)
            time.sleep(0.1)
            if scheduler.poll() is not None:
                raise RuntimeError(
                    log_path.read_text(encoding="utf-8", errors="replace")
                )
            return
        except Exception:
            time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for scheduler stats socket at {socket_path}")


def remove_stale_unix_socket(socket_path: Path) -> None:
    try:
        mode = socket_path.lstat().st_mode
    except FileNotFoundError:
        return
    if stat.S_ISSOCK(mode):
        try:
            socket_path.unlink()
        except PermissionError:
            subprocess.run(
                ["sudo", "-n", "rm", "-f", str(socket_path)],
                check=True,
                cwd=REPO_ROOT,
            )


def start_scheduler_stats_capture(
    output_path: Path,
    socket_path: Path,
    control_plane_cpus: Iterable[int] | None = None,
) -> subprocess.Popen[bytes]:
    command = with_cpu_affinity(
        [
            "python3",
            str(SCRIPT_DIR / "measure_latency.py"),
            "capture-stats",
            "--output",
            str(output_path),
            "--socket-path",
            str(socket_path),
            "--interval-ms",
            "100",
        ],
        control_plane_cpus,
    )
    return subprocess.Popen(
        command,
        cwd=REPO_ROOT,
    )


def wait_for_scheduler_stats_sample(
    output_path: Path,
    capture: subprocess.Popen[bytes],
    timeout_s: float = 1.0,
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if capture.poll() is not None:
            raise RuntimeError(
                "scheduler stats capture exited before producing a sample"
            )
        if output_path.exists() and output_path.stat().st_size > 0:
            return
        time.sleep(0.02)


def start_event_bridge(
    log_path: Path,
    port: int,
    control_plane_cpus: Iterable[int] | None = None,
) -> subprocess.Popen[bytes]:
    ensure_event_bridge_build()
    with log_path.open("w", encoding="utf-8") as log_file:
        command = with_cpu_affinity(
            [
                str(REPO_ROOT / "target" / "release" / "cosmos-event-bridge"),
                "--port",
                str(port),
            ],
            control_plane_cpus,
        )
        return subprocess.Popen(
            command,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=REPO_ROOT,
        )


def wait_for_event_bridge(
    port: int, bridge: subprocess.Popen[bytes], log_path: Path
) -> None:
    for _ in range(50):
        if bridge.poll() is not None:
            raise RuntimeError(log_path.read_text(encoding="utf-8", errors="replace"))
        try:
            send_event_bridge_event(
                port,
                {
                    "type": "local_end",
                    "activation_id": "cosmos-event-bridge-healthcheck",
                    "tgid": 0,
                },
            )
            return
        except Exception:
            time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for event bridge on 127.0.0.1:{port}")


def stop_process(
    process: subprocess.Popen[bytes] | None, sig: int = signal.SIGINT
) -> None:
    if process is None or process.poll() is not None:
        return
    pids = descendant_pids(process.pid)
    if process.pid not in pids:
        pids.append(process.pid)

    signal_name = signal.Signals(sig).name.removeprefix("SIG")

    for pid in reversed(pids):
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            continue
        except PermissionError:
            subprocess.run(
                ["sudo", "-n", "kill", f"-{signal_name}", str(pid)],
                check=False,
                cwd=REPO_ROOT,
            )
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        for pid in reversed(pids):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                continue
            except PermissionError:
                subprocess.run(
                    ["sudo", "-n", "kill", "-KILL", str(pid)],
                    check=False,
                    cwd=REPO_ROOT,
                )
        process.wait(timeout=5)


def finalize_run(run_dir: Path, config_root: Path) -> None:
    summarize_run(run_dir)
    refresh_latest_link(config_root, run_dir)


def default_results_dir(config: str) -> Path:
    return RESULTS_ROOT / config
