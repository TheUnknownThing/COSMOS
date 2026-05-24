#!/usr/bin/env python3

from __future__ import annotations

import concurrent.futures
import csv
import json
import os
import signal
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


@dataclass(frozen=True)
class WorkloadSpec:
    name: str
    default_duration_ms: int
    default_deadline_us: int
    entrypoint: Path
    inspired_by: tuple[str, ...]
    description: str


WORKLOADS: dict[str, WorkloadSpec] = {
    "cpu_burst": WorkloadSpec(
        name="cpu_burst",
        default_duration_ms=10,
        default_deadline_us=10_000,
        entrypoint=REPO_ROOT / "benchmarks" / "workloads" / "cpu_burst" / "workload.py",
        inspired_by=("010.sleep",),
        description="short CPU-only burst for scheduler overhead and tail latency pressure",
    ),
    "sleep_short": WorkloadSpec(
        name="sleep_short",
        default_duration_ms=2,
        default_deadline_us=5_000,
        entrypoint=REPO_ROOT / "benchmarks" / "workloads" / "sleep_short" / "workload.py",
        inspired_by=("010.sleep",),
        description="minimal baseline overhead calibration",
    ),
    "io_mixed": WorkloadSpec(
        name="io_mixed",
        default_duration_ms=20,
        default_deadline_us=20_000,
        entrypoint=REPO_ROOT / "benchmarks" / "workloads" / "io_mixed" / "workload.py",
        inspired_by=("311.compression", "220.video-processing"),
        description="small CPU plus synchronous file IO mix",
    ),
    "memory_heavy": WorkloadSpec(
        name="memory_heavy",
        default_duration_ms=25,
        default_deadline_us=25_000,
        entrypoint=REPO_ROOT / "benchmarks" / "workloads" / "memory_heavy" / "workload.py",
        inspired_by=("411.image-recognition", "220.video-processing"),
        description="allocation and scanning workload that stresses memory bandwidth and cache locality",
    ),
    "network_heavy": WorkloadSpec(
        name="network_heavy",
        default_duration_ms=25,
        default_deadline_us=25_000,
        entrypoint=REPO_ROOT / "benchmarks" / "workloads" / "network_heavy" / "workload.py",
        inspired_by=("120.uploader",),
        description="loopback TCP transfer workload for network wait and copy pressure",
    ),
    "compression_mixed": WorkloadSpec(
        name="compression_mixed",
        default_duration_ms=20,
        default_deadline_us=20_000,
        entrypoint=REPO_ROOT / "benchmarks" / "workloads" / "compression_mixed" / "workload.py",
        inspired_by=("311.compression",),
        description="repeated compress/decompress cycles over moderately sized buffers",
    ),
    "graph_bfs": WorkloadSpec(
        name="graph_bfs",
        default_duration_ms=20,
        default_deadline_us=20_000,
        entrypoint=REPO_ROOT / "benchmarks" / "workloads" / "graph_bfs" / "workload.py",
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
    return ["python3", str(spec.entrypoint), "--duration-ms", str(duration_ms)]


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


def timestamped_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def ensure_release_build() -> None:
    scheduler = REPO_ROOT / "target" / "release" / "cosmos"
    if scheduler.exists():
        return
    subprocess.run(
        ["cargo", "build", "--release", "--manifest-path", str(REPO_ROOT / "Cargo.toml")],
        check=True,
        cwd=REPO_ROOT,
    )


def ensure_shim_build() -> None:
    shim = REPO_ROOT / "shim" / "libcosmos_meta.so"
    if shim.exists():
        return
    subprocess.run(["make"], check=True, cwd=REPO_ROOT / "shim")


def run_workload_invocation(
    output_json: Path,
    workload: str,
    duration_ms: int,
    deadline_us: int,
    invocation_id: int,
    use_metadata: bool,
    config: str,
) -> int:
    stderr_path = output_json.with_suffix(".stderr")
    command = workload_command(workload, duration_ms)
    start_ns = time.monotonic_ns()
    env = None

    if use_metadata:
        ensure_shim_build()
        env = workload_env_with_metadata(deadline_us, invocation_id)

    with stderr_path.open("w", encoding="utf-8") as stderr_file:
        completed = subprocess.run(
            command,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=stderr_file,
            check=False,
        )

    end_ns = time.monotonic_ns()
    payload = {
        "invocation_id": invocation_id,
        "status": "ok" if completed.returncode == 0 else "failed",
        "exit_code": completed.returncode,
        "start_monotonic_ns": start_ns,
        "end_monotonic_ns": end_ns,
        "duration_ms": (end_ns - start_ns) / 1_000_000.0,
        "deadline_us": deadline_us,
        "workload": workload,
        "config": config,
        "stderr_path": str(stderr_path),
    }
    output_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return completed.returncode


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
        "metadata_mode": metadata_mode,
        "scheduler_flags": list(scheduler_flags),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (run_dir / "manifest.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_client_latency_csv(run_dir: Path) -> None:
    rows = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((run_dir / "invocations").glob("*.json"))
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
) -> int:
    run_dir.joinpath("invocations").mkdir(parents=True, exist_ok=True)
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


def wait_for_scheduler_stats(socket_path: Path, scheduler: subprocess.Popen[bytes], log_path: Path) -> None:
    for _ in range(50):
        if scheduler.poll() is not None:
            raise RuntimeError(log_path.read_text(encoding="utf-8", errors="replace"))
        try:
            measure_latency.request_scheduler_stats(socket_path)
            return
        except Exception:
            time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for scheduler stats socket at {socket_path}")


def start_scheduler_stats_capture(output_path: Path, socket_path: Path) -> subprocess.Popen[bytes]:
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


def stop_process(process: subprocess.Popen[bytes] | None, sig: int = signal.SIGINT) -> None:
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
