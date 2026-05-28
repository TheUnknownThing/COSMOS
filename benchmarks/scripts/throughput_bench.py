#!/usr/bin/env python3
"""Throughput benchmark: fire invocations as fast as possible for a duration.
Compares SFS vs COSMOS-full vs CFS on sustained throughput."""

from __future__ import annotations

import argparse
import json
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent

BENCHMARK_WORKLOAD_BIN = REPO_ROOT / "target" / "release" / "cosmos-benchmark-workload"
TIME_BIN = Path("/usr/bin/time")
DEFAULT_STATS_SOCKET = Path("/var/run/scx/root/stats")
DEFAULT_EVENT_BRIDGE_PORT = 9731
SCHEDULER_BIN = REPO_ROOT / "target" / "release" / "cosmos"
EVENT_BRIDGE_BIN = REPO_ROOT / "target" / "release" / "cosmos-event-bridge"


class ThroughputResult:
    def __init__(self):
        self.lock = threading.Lock()
        self.completed = 0
        self.failed = 0
        self.slo_violations = 0
        self.latencies: list[float] = []
        self.start_ns = 0
        self.end_ns = 0
        self.in_flight = 0
        self.max_in_flight = 0


def run_single(wl: str, dur_ms: int, deadline_us: int, result: ThroughputResult):
    cmd = [
        str(BENCHMARK_WORKLOAD_BIN),
        "--workload", wl,
        "--duration-ms", str(dur_ms),
    ]
    if TIME_BIN.exists():
        cmd = [str(TIME_BIN), "-f", "\\nCOSMOS_TIME real_s=%e user_s=%U sys_s=%S", *cmd]

    invoke_start = time.monotonic_ns()
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=max(30, dur_ms / 1000 * 5),
            cwd=str(REPO_ROOT),
        )
        elapsed_ns = time.monotonic_ns() - invoke_start
        elapsed_ms = elapsed_ns / 1_000_000

        with result.lock:
            result.completed += 1
            result.latencies.append(elapsed_ms)
            if elapsed_us := elapsed_ns / 1000 > deadline_us:
                result.slo_violations += 1
        return True
    except Exception:
        with result.lock:
            result.failed += 1
        return False


def fire_loop(
    wl: str, dur_ms: int, deadline_us: int,
    duration_s: int, max_concurrency: int,
    result: ThroughputResult, stop_event: threading.Event,
):
    with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
        futures: deque = deque()
        result.start_ns = time.monotonic_ns()
        deadline_ns = result.start_ns + duration_s * 1_000_000_000

        while not stop_event.is_set():
            now = time.monotonic_ns()
            if now >= deadline_ns:
                stop_event.set()
                break

            # Clean completed futures
            while futures and futures[0].done():
                futures.popleft()

            # Fire new if below max concurrency
            max_to_fire = max_concurrency - len(futures)
            for _ in range(max_to_fire):
                # Check if we have time before firing
                remaining_ns = deadline_ns - time.monotonic_ns()
                if remaining_ns < dur_ms * 1_000_000:
                    stop_event.set()
                    break
                f = executor.submit(run_single, wl, dur_ms, deadline_us, result)
                futures.append(f)

            if not stop_event.is_set():
                time.sleep(0.001)  # 1ms polling

        # Wait for remaining
        for f in futures:
            try:
                f.result(timeout=60)
            except Exception:
                pass

        result.end_ns = time.monotonic_ns()


def start_scheduler(config: str, deadline_us: int, log_path: Path) -> subprocess.Popen | None:
    if config == "cfs-default":
        return None

    if config == "sfs":
        flags = ["--policy", "sfs"]
    elif config == "cosmos-full":
        flags = ["--slo-target-us", str(deadline_us)]
    else:
        raise ValueError(f"Unknown config: {config}")

    log_file = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        [str(SCHEDULER_BIN), *flags],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        cwd=str(REPO_ROOT),
    )
    return proc


def start_event_bridge(port: int, log_path: Path) -> subprocess.Popen:
    log_file = log_path.open("w", encoding="utf-8")
    return subprocess.Popen(
        [str(EVENT_BRIDGE_BIN), "--listen-port", str(port)],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        cwd=str(REPO_ROOT),
    )


def wait_for_stats_socket(socket_path: Path, timeout_s: int = 30) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if socket_path.exists():
            return True
        time.sleep(0.1)
    return False


def stop_process(proc, sig=signal.SIGINT):
    if proc and proc.poll() is None:
        try:
            proc.send_signal(sig)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def main():
    parser = argparse.ArgumentParser(description="Throughput benchmark: SFS vs COSMOS vs CFS")
    parser.add_argument("--config", required=True, choices=["cfs-default", "sfs", "cosmos-full"])
    parser.add_argument("--workload", required=True)
    parser.add_argument("--duration-ms", type=int, default=250)
    parser.add_argument("--deadline-us", type=int, default=500000)
    parser.add_argument("--run-duration-s", type=int, default=30, help="How long to fire invocations")
    parser.add_argument("--max-concurrency", type=int, default=256, help="Max concurrent invocations")
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--skip-scheduler-check", action="store_true")
    args = parser.parse_args()

    # Resolve deadline
    deadline_us = args.deadline_us or (args.duration_ms * 2 * 1000)

    # Output directory
    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    out_dir = args.out_dir or SCRIPT_DIR / "results" / f"throughput_{args.config}"
    run_dir = out_dir / ts
    run_dir.mkdir(parents=True, exist_ok=True)

    # Write manifest
    manifest = {
        "config": args.config,
        "workload": args.workload,
        "duration_ms": args.duration_ms,
        "deadline_us": deadline_us,
        "run_duration_s": args.run_duration_s,
        "max_concurrency": args.max_concurrency,
        "nproc": len(list(filter(lambda x: x, Path("/sys/devices/system/cpu").glob("cpu[0-9]*")))) or 1,
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

    scheduler = None
    event_bridge = None

    try:
        if args.config != "cfs-default":
            # Start scheduler
            scheduler = start_scheduler(args.config, deadline_us, run_dir / "scheduler.log")
            print(f"Scheduler PID: {scheduler.pid}")
            if not wait_for_stats_socket(DEFAULT_STATS_SOCKET, 30):
                print("WARNING: stats socket did not appear, continuing anyway")
            else:
                print("Stats socket ready")

            # Start event bridge (needed for cosmos-full; sfs uses it too for metadata)
            if args.config in ("sfs", "cosmos-full"):
                event_bridge = start_event_bridge(DEFAULT_EVENT_BRIDGE_PORT, run_dir / "event_bridge.log")
                print(f"Event bridge PID: {event_bridge.pid}")
                time.sleep(1)

        # Run throughput test
        result = ThroughputResult()
        stop_event = threading.Event()

        print(f"Running throughput test: {args.config} | {args.workload} | {args.run_duration_s}s | max_concurrency={args.max_concurrency}")
        t0 = time.monotonic()
        fire_loop(
            args.workload, args.duration_ms, deadline_us,
            args.run_duration_s, args.max_concurrency,
            result, stop_event,
        )
        elapsed = time.monotonic() - t0

        # Compute stats
        total = result.completed
        wall_s = (result.end_ns - result.start_ns) / 1_000_000_000 if result.end_ns > result.start_ns else elapsed
        throughput = total / wall_s if wall_s > 0 else 0

        lats = sorted(result.latencies)
        p50 = lats[len(lats) // 2] if lats else 0
        p95_idx = int(len(lats) * 0.95)
        p99_idx = int(len(lats) * 0.99)
        p95 = lats[min(p95_idx, len(lats) - 1)] if lats else 0
        p99 = lats[min(p99_idx, len(lats) - 1)] if lats else 0
        mean_lat = sum(lats) / len(lats) if lats else 0

        summary = {
            "config": args.config,
            "workload": args.workload,
            "duration_ms": args.duration_ms,
            "deadline_us": deadline_us,
            "run_duration_s": args.run_duration_s,
            "wall_elapsed_s": wall_s,
            "total_completed": total,
            "total_failed": result.failed,
            "throughput_inv_per_sec": throughput,
            "slo_violations": result.slo_violations,
            "slo_violation_rate": result.slo_violations / total if total > 0 else 0,
            "p50_ms": p50,
            "p95_ms": p95,
            "p99_ms": p99,
            "mean_ms": mean_lat,
        }
        (run_dir / "summary.json").write_text(json.dumps(summary, indent=2))

        # Print results
        print(f"\n=== {args.config} | {args.workload} ===")
        print(f"  Duration: {wall_s:.1f}s")
        print(f"  Completed: {total} (failed: {result.failed})")
        print(f"  Throughput: {throughput:.1f} inv/s")
        print(f"  p50: {p50:.1f}ms  p95: {p95:.1f}ms  p99: {p99:.1f}ms  mean: {mean_lat:.1f}ms")
        print(f"  SLO violations: {result.slo_violations}/{total} ({result.slo_violations/total*100:.1f}%)" if total > 0 else "  SLO: N/A")
        print(f"  Result: {run_dir}")

    finally:
        stop_process(event_bridge)
        stop_process(scheduler)


if __name__ == "__main__":
    main()
