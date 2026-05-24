#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import math
import signal
import socket
import sys
import time
from pathlib import Path
from typing import Any


STOP = False


def _handle_stop(_signum: int, _frame: Any) -> None:
    global STOP
    STOP = True


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]

    ordered = sorted(values)
    index = (len(ordered) - 1) * quantile
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    fraction = index - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def load_client_rows(run_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with (run_dir / "client_latency.csv").open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            row["duration_ms"] = float(row["duration_ms"])
            row["deadline_us"] = int(row["deadline_us"])
            row["exit_code"] = int(row["exit_code"])
            row["invocation_id"] = int(row["invocation_id"])
            rows.append(row)
    return rows


def load_scheduler_samples(run_dir: Path) -> list[dict[str, Any]]:
    path = run_dir / "scheduler_stats.jsonl"
    if not path.exists():
        return []

    samples = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            samples.append(json.loads(line))
    return samples


def summarize_run(run_dir: Path) -> dict[str, Any]:
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    rows = load_client_rows(run_dir)
    scheduler_samples = load_scheduler_samples(run_dir)

    durations = [row["duration_ms"] for row in rows]
    ok_rows = [row for row in rows if row["status"] == "ok"]
    failures = len(rows) - len(ok_rows)
    deadline_ms = manifest["deadline_us"] / 1000.0 if manifest["deadline_us"] else 0.0
    client_slo_violations = sum(1 for row in ok_rows if deadline_ms and row["duration_ms"] > deadline_ms)

    scheduler_last = scheduler_samples[-1]["stats"] if scheduler_samples else {}
    scheduler_peak = {
        "nr_queued": max((sample["stats"].get("nr_queued", 0) for sample in scheduler_samples), default=0),
        "nr_scheduled": max((sample["stats"].get("nr_scheduled", 0) for sample in scheduler_samples), default=0),
    }

    latency = {
        "count": len(durations),
        "successes": len(ok_rows),
        "failures": failures,
        "min_ms": min(durations) if durations else 0.0,
        "max_ms": max(durations) if durations else 0.0,
        "mean_ms": sum(durations) / len(durations) if durations else 0.0,
        "p50_ms": percentile(durations, 0.50),
        "p95_ms": percentile(durations, 0.95),
        "p99_ms": percentile(durations, 0.99),
        "client_slo_violations": client_slo_violations,
    }

    summary = {
        "config": manifest["config"],
        "workload": manifest["workload"],
        "concurrency": manifest["concurrency"],
        "duration_ms": manifest["duration_ms"],
        "deadline_us": manifest["deadline_us"],
        "metadata_mode": manifest["metadata_mode"],
        "scheduler_flags": manifest["scheduler_flags"],
        "latency": latency,
        "scheduler": {
            "samples": len(scheduler_samples),
            "last": scheduler_last,
            "peak": scheduler_peak,
        },
    }

    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def request_scheduler_stats(socket_path: Path) -> dict[str, Any]:
    payload = json.dumps({"req": "stats", "args": {}}).encode("utf-8") + b"\n"
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.connect(str(socket_path))
        client.sendall(payload)
        raw = b""
        while not raw.endswith(b"\n"):
            chunk = client.recv(65536)
            if not chunk:
                break
            raw += chunk
    if not raw:
        raise RuntimeError("scheduler stats socket returned no data")

    response = json.loads(raw.decode("utf-8"))
    if response.get("errno", 0) != 0:
        raise RuntimeError(f"stats request failed: {response}")
    return response["args"]["resp"]


def capture_stats(output: Path, socket_path: Path, interval_ms: int) -> None:
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as fh:
        while not STOP:
            try:
                sample = {
                    "ts_monotonic_ns": time.monotonic_ns(),
                    "stats": request_scheduler_stats(socket_path),
                }
                fh.write(json.dumps(sample) + "\n")
                fh.flush()
            except (FileNotFoundError, ConnectionRefusedError, RuntimeError, OSError):
                pass
            time.sleep(interval_ms / 1000.0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize or capture COSMOS benchmark latency data.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    summarize = subparsers.add_parser("summarize", help="Build summary.json for a run directory.")
    summarize.add_argument("--run-dir", required=True, type=Path)

    capture = subparsers.add_parser("capture-stats", help="Poll the scheduler stats socket into JSONL.")
    capture.add_argument("--output", required=True, type=Path)
    capture.add_argument("--socket-path", default=Path("/var/run/scx/root/stats"), type=Path)
    capture.add_argument("--interval-ms", default=100, type=int)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "summarize":
        summary = summarize_run(args.run_dir)
        json.dump(summary, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0

    if args.command == "capture-stats":
        capture_stats(args.output, args.socket_path, args.interval_ms)
        return 0

    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
