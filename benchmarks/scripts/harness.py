#!/usr/bin/env python3

from __future__ import annotations

import concurrent.futures
import csv
import json
import os
import signal
import socket
import stat
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

import measure_latency

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
RESULTS_ROOT = SCRIPT_DIR / "results"
DEFAULT_STATS_SOCKET = Path("/var/run/scx/root/stats")
DEFAULT_EVENT_BRIDGE_PORT = 9731
DEFAULT_SLO_MIN_SLACK_US = 5_000
DEFAULT_WORKLOAD_DURATION_MS = 250
SCHEDULER_STATS_READY_TIMEOUT_S = 30.0
TIME_BIN = Path("/usr/bin/time")
BPFTOOL_BIN = Path("/usr/sbin/bpftool")
if not BPFTOOL_BIN.exists():
    BPFTOOL_BIN = Path("/usr/bin/bpftool")
BPF_INVOCATION_META_PATH = Path("/sys/fs/bpf/cosmos/invocation_meta")
BENCHMARK_WORKLOAD_BIN = REPO_ROOT / "target" / "release" / "cosmos-benchmark-workload"
DEBUG_BPF_MAP = os.environ.get("COSMOS_BENCH_DEBUG_BPF_MAP") == "1"
GATE_SCRIPT = 'IFS= read -r _ <&"$COSMOS_START_FD"; exec "$@"'
STOP_SCRIPT = 'kill -STOP $$; exec "$@"'


@dataclass(frozen=True)
class WorkloadSpec:
    name: str
    default_duration_ms: int
    default_deadline_us: int
    default_slo_class: int
    runner: Path
    inspired_by: tuple[str, ...]
    description: str


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
        default_slo_class=0,
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
        default_slo_class=2,
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
    "compression_mixed": WorkloadSpec(
        name="compression_mixed",
        default_duration_ms=DEFAULT_WORKLOAD_DURATION_MS,
        default_deadline_us=default_deadline_us_for_duration(
            DEFAULT_WORKLOAD_DURATION_MS
        ),
        default_slo_class=2,
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
        default_slo_class=2,
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


def workload_command(workload: str, duration_ms: int) -> list[str]:
    spec = workload_spec(workload)
    command = [
        str(spec.runner),
        "--workload",
        workload,
        "--duration-ms",
        str(duration_ms),
    ]
    if TIME_BIN.exists():
        return [
            str(TIME_BIN),
            "-f",
            "COSMOS_TIME real_s=%e user_s=%U sys_s=%S maxrss_kb=%M",
            *command,
        ]
    return command


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


def workload_env_with_metadata(deadline_us: int, invocation_id: int) -> dict[str, str]:
    env = os.environ.copy()
    shim = REPO_ROOT / "shim" / "libcosmos_meta.so"
    existing_preload = env.get("LD_PRELOAD", "")
    env["LD_PRELOAD"] = f"{shim}:{existing_preload}" if existing_preload else str(shim)
    env["COSMOS_DEADLINE_NS"] = str(time.monotonic_ns() + deadline_us * 1_000)
    env["COSMOS_SLO_CLASS"] = "0"
    env["COSMOS_COLD_START"] = "0"
    env["COSMOS_INVOCATION_ID"] = str(invocation_id)
    return env


def send_event_bridge_event(port: int, event: dict) -> None:
    payload = json.dumps(event).encode("utf-8") + b"\n"
    with socket.create_connection(("127.0.0.1", port), timeout=2.0) as conn:
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


def dump_invocation_meta_map(output_path: Path) -> None:
    if not BPFTOOL_BIN.exists():
        return
    completed = subprocess.run(
        [
            str(BPFTOOL_BIN),
            "-j",
            "map",
            "dump",
            "pinned",
            str(BPF_INVOCATION_META_PATH),
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
            str(BPF_INVOCATION_META_PATH),
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
        REPO_ROOT / "src" / "pool.rs",
        REPO_ROOT / "src" / "stats.rs",
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
            "cargo",
            "build",
            "--release",
            "--manifest-path",
            str(REPO_ROOT / "Cargo.toml"),
        ],
        check=True,
        cwd=REPO_ROOT,
    )


def ensure_shim_build() -> None:
    shim = REPO_ROOT / "shim" / "libcosmos_meta.so"
    sources = [
        REPO_ROOT / "shim" / "cosmos_meta.c",
        REPO_ROOT / "shim" / "cosmos_preload.c",
        REPO_ROOT / "shim" / "cosmos_meta.h",
    ]
    if shim.exists() and shim.stat().st_mtime >= max(
        path.stat().st_mtime for path in sources
    ):
        return
    subprocess.run(["make"], check=True, cwd=REPO_ROOT / "shim")


def ensure_event_bridge_build() -> None:
    bridge = REPO_ROOT / "target" / "release" / "cosmos-event-bridge"
    sources = [
        REPO_ROOT / "cosmos-event-bridge" / "src" / "main.rs",
        REPO_ROOT / "cosmos-event-bridge" / "src" / "bpf_writer.rs",
        REPO_ROOT / "cosmos-event-bridge" / "Cargo.toml",
    ]
    if bridge.exists() and bridge.stat().st_mtime >= max(
        path.stat().st_mtime for path in sources
    ):
        return
    subprocess.run(
        ["cargo", "build", "--release", "-p", "cosmos-event-bridge"],
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
        ["cargo", "build", "--release", "-p", "cosmos-benchmark-workload"],
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
) -> int:
    stderr_path = output_json.with_suffix(".stderr")
    command = workload_command(workload, duration_ms)
    spec = workload_spec(workload)
    start_ns = time.monotonic_ns()
    env = None
    process: subprocess.Popen[bytes] | None = None
    metadata_tgid = None
    metadata_tgids: list[int] = []
    metadata_key_visible = None

    if use_metadata:
        if metadata_bridge_port is not None:
            env = os.environ.copy()
            command = stopped_workload_command(command)
        else:
            ensure_shim_build()
            env = workload_env_with_metadata(deadline_us, invocation_id)

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
                        "activation_id": f"{config}-{workload}-{invocation_id}",
                        "tgid": tgid,
                        "timeout_ms": max(1, deadline_us // 1_000),
                        "slo_class": spec.default_slo_class,
                        "action_name": workload,
                        "kind": "local-rust",
                        "cold_start": False,
                    },
                )
            if DEBUG_BPF_MAP:
                metadata_key_visible = wait_for_invocation_meta_key(metadata_tgid)
                dump_invocation_meta_map(output_json.with_suffix(".bpfmap.json"))
            os.kill(metadata_tgid, signal.SIGCONT)
            returncode = process.wait()
            for tgid in reversed(metadata_tgids):
                send_event_bridge_event(
                    metadata_bridge_port,
                    {
                        "type": "local_end",
                        "activation_id": f"{config}-{workload}-{invocation_id}",
                        "tgid": tgid,
                    },
                )

    end_ns = time.monotonic_ns()
    payload = {
        "invocation_id": invocation_id,
        "status": "ok" if returncode == 0 else "failed",
        "exit_code": returncode,
        "start_monotonic_ns": start_ns,
        "end_monotonic_ns": end_ns,
        "duration_ms": (end_ns - start_ns) / 1_000_000.0,
        "deadline_us": deadline_us,
        "workload": workload,
        "config": config,
        "stderr_path": str(stderr_path),
        "metadata_tgid": metadata_tgid,
        "metadata_tgids": metadata_tgids,
        "metadata_key_visible": metadata_key_visible,
    }
    output_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return returncode


def write_manifest(
    run_dir: Path,
    config: str,
    workload: str,
    concurrency: int,
    duration_ms: int,
    deadline_us: int,
    metadata_mode: str,
    scheduler_flags: Iterable[str],
) -> None:
    spec = workload_spec(workload)
    payload = {
        "config": config,
        "workload": workload,
        "workload_description": spec.description,
        "inspired_by_sebs": list(spec.inspired_by),
        "concurrency": concurrency,
        "duration_ms": duration_ms,
        "deadline_us": deadline_us,
        "cpu_cores": os.cpu_count() or 1,
        "metadata_mode": metadata_mode,
        "scheduler_flags": list(scheduler_flags),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (run_dir / "manifest.json").write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8"
    )


def write_client_latency_csv(run_dir: Path) -> None:
    rows = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((run_dir / "invocations").glob("*.json"))
        if path.stem.isdigit()
    ]
    with (run_dir / "client_latency.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "invocation_id",
                "status",
                "exit_code",
                "start_monotonic_ns",
                "end_monotonic_ns",
                "duration_ms",
                "deadline_us",
                "workload",
                "config",
                "stderr_path",
                "metadata_tgid",
                "metadata_tgids",
                "metadata_key_visible",
            ],
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
) -> int:
    run_dir.joinpath("invocations").mkdir(parents=True, exist_ok=True)
    ensure_benchmark_workload_build()
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
            )
            for invocation_id in range(1, concurrency + 1)
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
        socket_path.unlink()


def start_scheduler_stats_capture(
    output_path: Path, socket_path: Path
) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
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


def start_event_bridge(log_path: Path, port: int) -> subprocess.Popen[bytes]:
    ensure_event_bridge_build()
    with log_path.open("w", encoding="utf-8") as log_file:
        return subprocess.Popen(
            [
                str(REPO_ROOT / "target" / "release" / "cosmos-event-bridge"),
                "--port",
                str(port),
            ],
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
    process.send_signal(sig)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def finalize_run(run_dir: Path, config_root: Path) -> None:
    summarize_run(run_dir)
    refresh_latest_link(config_root, run_dir)


def default_results_dir(config: str) -> Path:
    return RESULTS_ROOT / config
