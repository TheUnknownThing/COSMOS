#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import signal
import socket
import sys
import time
from pathlib import Path
from typing import Any, Callable, TextIO

stop_requested = False
FAIR_LOAD_LIMIT = 1.0
FULL_LOAD_RATIO = 0.80
SLIGHTLY_OVERLOADED_RATIO = 0.95
TIME_STATS_RE = re.compile(
    r"COSMOS_TIME real_s=(?P<real_s>[0-9.]+) "
    + r"user_s=(?P<user_s>[0-9.]+) "
    + r"sys_s=(?P<sys_s>[0-9.]+) "
    + r"maxrss_kb=(?P<maxrss_kb>[0-9]+)"
)

SCHEDULER_TOTAL_FIELDS = (
    "nr_background_tasks",
    "nr_bounce_dispatches",
    "nr_cancel_dispatches",
    "nr_cold_start_tasks",
    "nr_failed_dispatches",
    "nr_heuristic_classified",
    "nr_hot_invocation_tasks",
    "nr_kernel_dispatches",
    "nr_metadata_classified",
    "nr_metadata_refreshed",
    "nr_has_invocation_enqueues",
    "nr_pool_batch",
    "nr_pool_latency",
    "nr_pool_migrations",
    "nr_sched_congested",
    "nr_slo_boosted",
    "nr_slo_violations",
    "nr_tail_guard_dispatches",
    "nr_user_dispatches",
)


def _handle_stop(_signum: int, _frame: Any) -> None:
    global stop_requested
    stop_requested = True


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


def summarize_optional_ms(rows: list[dict[str, Any]], field: str) -> dict[str, float] | None:
    values = [
        float(row[field])
        for row in rows
        if row.get(field) not in (None, "")
    ]
    if not values:
        return None
    return {
        "count": len(values),
        "min_ms": min(values),
        "max_ms": max(values),
        "mean_ms": sum(values) / len(values),
        "p50_ms": percentile(values, 0.50),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
    }


def load_scheduler_samples(run_dir: Path) -> list[dict[str, Any]]:
    path = run_dir / "scheduler_stats.jsonl"
    if not path.exists():
        return []

    samples = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            samples.append(json.loads(line))
    return samples


def _invocation_stderr_path(run_dir: Path, row: dict[str, Any]) -> Path:
    sidecar = run_dir / "invocations" / f"{row['invocation_id']}.stderr"
    if sidecar.exists():
        return sidecar

    stderr_path = Path(str(row.get("stderr_path", "")))
    if stderr_path.is_absolute() or stderr_path.exists():
        return stderr_path
    return run_dir / stderr_path


def parse_invocation_time_stats(
    run_dir: Path, row: dict[str, Any]
) -> dict[str, float] | None:
    path = _invocation_stderr_path(run_dir, row)
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None

    match = TIME_STATS_RE.search(text)
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


def build_compute_summary(
    run_dir: Path,
    rows: list[dict[str, Any]],
    duration_ms: int,
    concurrency: int,
) -> dict[str, Any]:
    time_stats = [parse_invocation_time_stats(run_dir, row) for row in rows]
    measured = [stat for stat in time_stats if stat is not None]
    measured_cpu_ms = [stat["cpu_ms"] for stat in measured]

    missing_count = len(rows) - len(measured_cpu_ms)
    if measured_cpu_ms:
        total_cpu_ms = sum(measured_cpu_ms) + float(missing_count * duration_ms)
        source = (
            "usr_bin_time"
            if missing_count == 0
            else "partial_usr_bin_time_duration_ms_fallback"
        )
    else:
        total_cpu_ms = float(duration_ms * concurrency)
        source = "duration_ms_fallback"

    return {
        "source": source,
        "count": len(measured_cpu_ms),
        "missing_count": missing_count,
        "total_cpu_ms": total_cpu_ms,
        "mean_cpu_ms": total_cpu_ms / len(rows) if rows else float(duration_ms),
        "p50_cpu_ms": percentile(measured_cpu_ms, 0.50),
        "p95_cpu_ms": percentile(measured_cpu_ms, 0.95),
        "p99_cpu_ms": percentile(measured_cpu_ms, 0.99),
    }


def assess_load(
    *,
    concurrency: int,
    duration_ms: int,
    deadline_us: int,
    cpu_cores: int,
    total_compute_ms: float | None = None,
    compute_source: str | None = None,
) -> dict[str, Any]:
    deadline_ms = deadline_us / 1_000.0 if deadline_us else 0.0
    total_compute_ms = (
        float(total_compute_ms)
        if total_compute_ms is not None
        else float(duration_ms * concurrency)
    )
    capacity_ms = deadline_ms * max(cpu_cores, 1)
    load_ratio = total_compute_ms / capacity_ms if capacity_ms > 0.0 else math.inf
    fair = load_ratio < FAIR_LOAD_LIMIT

    if not fair:
        load_class = "unfair-overloaded"
    elif load_ratio >= SLIGHTLY_OVERLOADED_RATIO:
        load_class = "slightly-overloaded"
    elif load_ratio >= FULL_LOAD_RATIO:
        load_class = "full"
    else:
        load_class = "underloaded"

    return {
        "fair": fair,
        "class": load_class,
        "rule": "total_compute_ms < deadline_ms * cpu_cores",
        "load_ratio": load_ratio,
        "fair_load_limit": FAIR_LOAD_LIMIT,
        "total_compute_ms": total_compute_ms,
        "capacity_ms": capacity_ms,
        "deadline_ms": deadline_ms,
        "cpu_cores": cpu_cores,
        "compute_source": compute_source or "duration_ms_fallback",
    }


def assess_summary_load(summary: dict[str, Any]) -> dict[str, Any]:
    existing = summary.get("load")
    if isinstance(existing, dict):
        return existing

    scheduler_last = summary.get("scheduler", {}).get("last", {})
    cpu_cores = int(
        summary.get("cpu_cores") or scheduler_last.get("nr_cpus") or os.cpu_count() or 1
    )
    compute = summary.get("compute", {})
    return assess_load(
        concurrency=int(summary["concurrency"]),
        duration_ms=int(summary["duration_ms"]),
        deadline_us=int(summary["deadline_us"]),
        cpu_cores=cpu_cores,
        total_compute_ms=compute.get("total_cpu_ms"),
        compute_source=compute.get("source"),
    )


def summarize_run(run_dir: Path) -> dict[str, Any]:
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    rows = load_client_rows(run_dir)
    scheduler_samples = load_scheduler_samples(run_dir)

    durations = [row["duration_ms"] for row in rows]
    ok_rows = [row for row in rows if row["status"] == "ok"]
    failures = len(rows) - len(ok_rows)
    deadline_ms = manifest["deadline_us"] / 1000.0 if manifest["deadline_us"] else 0.0
    client_slo_violations = sum(
        1 for row in ok_rows if deadline_ms and row["duration_ms"] > deadline_ms
    )

    scheduler_last = scheduler_samples[-1]["stats"] if scheduler_samples else {}
    scheduler_peak = {
        "nr_queued": max(
            (sample["stats"].get("nr_queued", 0) for sample in scheduler_samples),
            default=0,
        ),
        "nr_scheduled": max(
            (sample["stats"].get("nr_scheduled", 0) for sample in scheduler_samples),
            default=0,
        ),
    }
    scheduler_total = {
        field: sum(sample["stats"].get(field, 0) for sample in scheduler_samples)
        for field in SCHEDULER_TOTAL_FIELDS
    }
    cpu_cores = int(
        manifest.get("cpu_cores")
        or scheduler_last.get("nr_cpus")
        or os.cpu_count()
        or 1
    )
    compute = build_compute_summary(
        run_dir,
        rows,
        int(manifest["duration_ms"]),
        int(manifest["concurrency"]),
    )
    load = assess_load(
        concurrency=int(manifest["concurrency"]),
        duration_ms=int(manifest["duration_ms"]),
        deadline_us=int(manifest["deadline_us"]),
        cpu_cores=cpu_cores,
        total_compute_ms=compute["total_cpu_ms"],
        compute_source=compute["source"],
    )

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
        "cpu_cores": cpu_cores,
        "metadata_mode": manifest["metadata_mode"],
        "scheduler_flags": manifest["scheduler_flags"],
        "compute": compute,
        "load": load,
        "latency": latency,
        "scheduler": {
            "samples": len(scheduler_samples),
            "last": scheduler_last,
            "peak": scheduler_peak,
            "total": scheduler_total,
        },
    }
    metadata_setup = summarize_optional_ms(rows, "metadata_setup_ms")
    if metadata_setup is not None:
        summary["metadata_setup"] = metadata_setup

    per_workload: dict[str, dict[str, Any]] = {}
    for row in rows:
        wl = row.get("workload", "unknown")
        if wl not in per_workload:
            per_workload[wl] = {"durations": [], "ok_rows": []}
        per_workload[wl]["durations"].append(row["duration_ms"])
        if row["status"] == "ok":
            per_workload[wl]["ok_rows"].append(row)
    if len(per_workload) > 1:
        summary["per_workload"] = {}
        for wl, data in sorted(per_workload.items()):
            durs = data["durations"]
            ok = data["ok_rows"]
            wl_violations = sum(
                1 for row in ok if deadline_ms and row["duration_ms"] > deadline_ms
            )
            summary["per_workload"][wl] = {
                "count": len(durs),
                "successes": len(ok),
                "failures": len(durs) - len(ok),
                "min_ms": min(durs) if durs else 0.0,
                "max_ms": max(durs) if durs else 0.0,
                "mean_ms": sum(durs) / len(durs) if durs else 0.0,
                "p50_ms": percentile(durs, 0.50),
                "p95_ms": percentile(durs, 0.95),
                "p99_ms": percentile(durs, 0.99),
                "client_slo_violations": wl_violations,
            }

    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    return summary


STATS_REQUEST_PAYLOAD = json.dumps({"req": "stats", "args": {}}).encode("utf-8") + b"\n"
STATS_SOCKET_TIMEOUT_S = 5.0


def _decode_stats_response(raw: bytes) -> dict[str, Any]:
    if not raw:
        raise RuntimeError("scheduler stats socket returned no data")

    response = json.loads(raw.decode("utf-8"))
    if response.get("errno", 0) != 0:
        raise RuntimeError(f"stats request failed: {response}")
    return response["args"]["resp"]


def request_scheduler_stats(socket_path: Path) -> dict[str, Any]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(STATS_SOCKET_TIMEOUT_S)
        client.connect(str(socket_path))
        client.sendall(STATS_REQUEST_PAYLOAD)
        raw = b""
        while not raw.endswith(b"\n"):
            chunk = client.recv(65536)
            if not chunk:
                break
            raw += chunk
    return _decode_stats_response(raw)


def _read_stats_on_connection(client: socket.socket) -> dict[str, Any]:
    """Send a stats request on an existing connection and return the response."""
    client.sendall(STATS_REQUEST_PAYLOAD)
    raw = b""
    while not raw.endswith(b"\n"):
        chunk = client.recv(65536)
        if not chunk:
            raise RuntimeError("stats connection closed by server")
        raw += chunk
    return _decode_stats_response(raw)


def _write_stats_sample(fh: TextIO, stats: dict[str, Any]) -> None:
    sample = {
        "ts_monotonic_ns": time.monotonic_ns(),
        "stats": stats,
    }
    fh.write(json.dumps(sample) + "\n")
    fh.flush()


def _sleep_until(deadline: float, should_stop: Callable[[], bool]) -> None:
    while not should_stop():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 0.05))


def capture_stats(
    output: Path,
    socket_path: Path,
    interval_ms: int,
    should_stop: Callable[[], bool] | None = None,
    install_signal_handlers: bool = True,
) -> None:
    global stop_requested
    stop_requested = False
    if install_signal_handlers:
        signal.signal(signal.SIGINT, _handle_stop)
        signal.signal(signal.SIGTERM, _handle_stop)
    if interval_ms <= 0:
        raise ValueError("interval_ms must be greater than zero")

    should_stop = should_stop or (lambda: stop_requested)
    interval_s = interval_ms / 1000.0

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as fh:
        client: socket.socket | None = None
        last_read_at: float | None = None
        try:
            while not should_stop():
                try:
                    if client is None:
                        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                        client.settimeout(STATS_SOCKET_TIMEOUT_S)
                        client.connect(str(socket_path))
                        # The scx_stats server captures the delta baseline when
                        # a stats target is opened on a connection. Discard this
                        # first read and keep the same socket for all benchmark
                        # samples so the next reads cover real elapsed intervals.
                        _read_stats_on_connection(client)
                        last_read_at = time.monotonic()

                    assert last_read_at is not None
                    _sleep_until(last_read_at + interval_s, should_stop)
                    if should_stop():
                        break

                    stats = _read_stats_on_connection(client)
                    last_read_at = time.monotonic()
                    _write_stats_sample(fh, stats)
                except (
                    FileNotFoundError,
                    ConnectionRefusedError,
                    RuntimeError,
                    OSError,
                ):
                    if client is not None:
                        client.close()
                        client = None
                        last_read_at = None
                    _sleep_until(time.monotonic() + interval_s, should_stop)
        finally:
            if client is not None:
                try:
                    stats = _read_stats_on_connection(client)
                    _write_stats_sample(fh, stats)
                except (RuntimeError, OSError):
                    pass
                finally:
                    client.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Summarize or capture COSMOS benchmark latency data."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    summarize = subparsers.add_parser(
        "summarize", help="Build summary.json for a run directory."
    )
    summarize.add_argument("--run-dir", required=True, type=Path)

    capture = subparsers.add_parser(
        "capture-stats", help="Poll the scheduler stats socket into JSONL."
    )
    capture.add_argument("--output", required=True, type=Path)
    capture.add_argument(
        "--socket-path", default=Path("/var/run/scx/root/stats"), type=Path
    )
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
