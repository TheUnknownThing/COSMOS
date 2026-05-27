#!/usr/bin/env python3
"""Analyze profiler trace data and produce a COSMOS-compatible summary.json.

Reads profiler run directories (produced by `cosmos-bench-profiler standalone`
or `cosmos-bench-profiler open-whisk`) and outputs a summary.json with the
fields expected by compare.py: config, workload, concurrency, duration_ms,
deadline_us, compute, load, latency, scheduler.

Usage:
    # Analyze a single profiler run directory
    python3 benchmarks/scripts/analyze_trace.py \
        --run-dir benchmarks/runs/<run-id> \
        --deadline-us 500000 \
        --config cosmos-full

    # Compare two profiler runs
    python3 benchmarks/scripts/analyze_trace.py \
        --baseline-dir benchmarks/runs/<cfs-run> \
        --candidate-dir benchmarks/runs/<cosmos-run> \
        --deadline-us 500000

    # The compare subcommand writes compat summary.json files and runs compare.py
    python3 benchmarks/scripts/analyze_trace.py compare \
        --baseline benchmarks/runs/<cfs-run> \
        --candidate benchmarks/runs/<cosmos-run> \
        --deadline-us 500000
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


def percentile(values: list[float], quantile: float) -> float:
    """Compute the quantile of a list of values."""
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


def read_csv(path: Path) -> list[list[str]]:
    """Read a CSV file into a list of row lists."""
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.reader(fh))


def read_cgroup_samples(run_dir: Path) -> list[dict[str, int]]:
    """Read cgroup_cpu.csv samples and return list of dicts with usage fields."""
    rows = read_csv(run_dir / "cgroup_cpu.csv")
    if len(rows) < 2:
        return []
    header = rows[0]
    samples = []
    for row in rows[1:]:
        if len(row) < len(header):
            continue
        sample = {}
        for i, key in enumerate(header):
            try:
                sample[key] = int(row[i])
            except (ValueError, IndexError):
                sample[key] = 0
        samples.append(sample)
    return samples


def read_client_rows(run_dir: Path) -> list[dict[str, Any]]:
    """Read client_latency.csv rows."""
    rows = read_csv(run_dir / "client_latency.csv")
    if len(rows) < 2:
        return []
    result = []
    for row in rows[1:]:
        if len(row) < 6:
            continue
        if row[0] == "run_id":
            continue
        send = int(row[2]) if row[2] else 0
        end = int(row[4]) if row[4] else 0
        if end < send:
            continue
        result.append({
            "run_id": row[0] if len(row) > 0 else "",
            "activation_id": row[1] if len(row) > 1 else "",
            "send_ns": send,
            "response_end_ns": end,
            "duration_ns": end - send,
            "duration_ms": (end - send) / 1_000_000.0,
            "status": row[5] if len(row) > 5 else "unknown",
            "timing_source": row[6] if len(row) > 6 else "",
            "error": row[7] if len(row) > 7 else "",
        })
    return result


def read_scheduler_samples(run_dir: Path) -> list[dict[str, Any]]:
    """Read scheduler_stats.csv and return list of sample dicts."""
    rows = read_csv(run_dir / "scheduler_stats.csv")
    if len(rows) < 2:
        return []
    field_names = rows[0][3:]  # skip timestamp_ns,available,error
    samples = []
    for row in rows[1:]:
        if len(row) < 3:
            continue
        available = row[1] if len(row) > 1 else "0"
        if available != "1":
            continue
        stats = {}
        for i, name in enumerate(field_names):
            idx = i + 3
            if idx < len(row):
                try:
                    stats[name] = int(row[idx])
                except ValueError:
                    stats[name] = 0
        if stats:
            samples.append({
                "ts_ns": int(row[0]) if row[0] else 0,
                "stats": stats,
            })
    return samples


def read_events(run_dir: Path) -> list[dict[str, Any]]:
    """Read events.jsonl and return parsed event list."""
    path = run_dir / "events.jsonl"
    if not path.exists():
        return []
    events = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return events


def compute_cpu_time(run_dir: Path) -> dict[str, Any]:
    """Compute CPU accounting from cgroup samples."""
    samples = read_cgroup_samples(run_dir)
    if len(samples) < 2:
        return {
            "source": "none",
            "count": 0,
            "missing_count": 0,
            "total_cpu_ms": 0.0,
            "mean_cpu_ms": 0.0,
            "p50_cpu_ms": 0.0,
            "p95_cpu_ms": 0.0,
            "p99_cpu_ms": 0.0,
        }

    first = samples[0]
    last = samples[-1]
    total_usec = last.get("usage_usec", 0) - first.get("usage_usec", 0)
    if total_usec < 0:
        total_usec = 0
    total_cpu_ms = total_usec / 1000.0

    return {
        "source": "cgroup_usage_usec",
        "count": 1,
        "missing_count": 0,
        "total_cpu_ms": total_cpu_ms,
        "mean_cpu_ms": total_cpu_ms,
        "p50_cpu_ms": total_cpu_ms,
        "p95_cpu_ms": total_cpu_ms,
        "p99_cpu_ms": total_cpu_ms,
    }


def assess_load(
    total_compute_ms: float,
    deadline_us: int,
    cpu_cores: int,
    concurrency: int,
    duration_ms: int,
    compute_source: str = "cgroup_usage_usec",
) -> dict[str, Any]:
    """Assess load ratio and fairness class."""
    deadline_ms = deadline_us / 1000.0 if deadline_us else 0.0
    if total_compute_ms <= 0.0:
        total_compute_ms = float(duration_ms * concurrency)
        compute_source = "duration_ms_fallback"
    capacity_ms = deadline_ms * max(cpu_cores, 1)
    load_ratio = total_compute_ms / capacity_ms if capacity_ms > 0.0 else math.inf

    FAIR_LOAD_LIMIT = 1.0
    FULL_LOAD_RATIO = 0.80
    SLIGHTLY_OVERLOADED_RATIO = 0.95

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
        "compute_source": compute_source,
    }


def summarize_scheduler(samples: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize scheduler stats from samples."""
    if not samples:
        return {"samples": 0, "last": {}, "peak": {}, "total": {}}

    last = samples[-1]["stats"]
    peak = {}
    for field in last:
        peak[field] = max((s["stats"].get(field, 0) for s in samples), default=0)

    total = {}
    for field in last:
        total[field] = sum(s["stats"].get(field, 0) for s in samples)

    return {
        "samples": len(samples),
        "last": last,
        "peak": peak,
        "total": total,
    }


def duration_from_events(events: list[dict[str, Any]]) -> int:
    """Estimate duration_ms from invocation_started/finished events."""
    start_ns = None
    end_ns = None
    for ev in events:
        if ev.get("event") == "invocation_started":
            ts = ev.get("timestamp_ns", 0)
            if isinstance(ts, (int, float)) and ts > 0:
                if start_ns is None or ts < start_ns:
                    start_ns = ts
        elif ev.get("event") == "invocation_finished":
            ts = ev.get("timestamp_ns", 0)
            if isinstance(ts, (int, float)) and ts > 0:
                if end_ns is None or ts > end_ns:
                    end_ns = ts
    if start_ns and end_ns and end_ns > start_ns:
        return int((end_ns - start_ns) / 1_000_000)
    return 0


def build_compat_summary(
    run_dir: Path,
    config: str = "profiler",
    deadline_us: int = 0,
    workload: str = "",
) -> dict[str, Any]:
    """Build a COSMOS-compatible summary.json from profiler trace data.

    Args:
        run_dir: Path to a profiler run directory containing run_meta.json,
                 client_latency.csv, cgroup_cpu.csv, scheduler_stats.csv, etc.
        config: Config label for the summary (e.g. "cfs-default", "cosmos-full").
        deadline_us: SLO deadline in microseconds.
        workload: Override workload name. If empty, read from run_meta.json.

    Returns:
        A dict compatible with compare.py's expected summary.json format.
    """
    # Load metadata
    meta_path = run_dir / "run_meta.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    else:
        meta = {}

    if not workload:
        workload = meta.get("workload", "unknown")
    if not config:
        config = meta.get("config", "profiler")

    # Read client latency rows
    client_rows = read_client_rows(run_dir)

    # Read events for duration estimation
    events = read_events(run_dir)

    duration_ms = meta.get("duration_ms", 0) or duration_from_events(events)
    cpu_count = meta.get("env", {}).get("cpu_count", 0) or os.cpu_count() or 1
    if isinstance(cpu_count, str):
        try:
            cpu_count = int(cpu_count)
        except ValueError:
            cpu_count = os.cpu_count() or 1

    # Latency statistics
    durations_ms = [row["duration_ms"] for row in client_rows]
    ok_rows = [row for row in client_rows if row["status"] in ("exit:0", "active", "success")]
    failures = len(client_rows) - len(ok_rows)
    concurrency = len(client_rows) or 1

    deadline_ms = deadline_us / 1000.0 if deadline_us else 0.0
    client_slo_violations = sum(
        1 for row in ok_rows
        if deadline_ms and row["duration_ms"] > deadline_ms
    )

    latency = {
        "count": len(durations_ms),
        "successes": len(ok_rows),
        "failures": failures,
        "min_ms": min(durations_ms) if durations_ms else 0.0,
        "max_ms": max(durations_ms) if durations_ms else 0.0,
        "mean_ms": sum(durations_ms) / len(durations_ms) if durations_ms else 0.0,
        "p50_ms": percentile(durations_ms, 0.50),
        "p95_ms": percentile(durations_ms, 0.95),
        "p99_ms": percentile(durations_ms, 0.99),
        "client_slo_violations": client_slo_violations,
    }

    # CPU compute summary
    compute = compute_cpu_time(run_dir)
    if compute["total_cpu_ms"] <= 0.0 and duration_ms and concurrency:
        compute["total_cpu_ms"] = float(duration_ms * concurrency)
        compute["source"] = "duration_ms_fallback"

    # Load assessment
    load = assess_load(
        total_compute_ms=compute["total_cpu_ms"],
        deadline_us=deadline_us,
        cpu_cores=cpu_count,
        concurrency=concurrency,
        duration_ms=duration_ms or 0,
        compute_source=compute["source"],
    )

    # Scheduler stats
    scheduler_samples = read_scheduler_samples(run_dir)
    scheduler = summarize_scheduler(scheduler_samples)

    # Per-workload stats (for multi-workload runs)
    per_workload: dict[str, dict[str, Any]] = {}
    workload_key = meta.get("workload_label", "")
    if workload_key and client_rows:
        wl_durs = [row["duration_ms"] for row in client_rows]
        wl_ok = [row for row in client_rows if row["status"] in ("exit:0", "active", "success")]
        wl_violations = sum(
            1 for row in wl_ok
            if deadline_ms and row["duration_ms"] > deadline_ms
        )
        per_workload[workload_key] = {
            "count": len(wl_durs),
            "successes": len(wl_ok),
            "failures": len(client_rows) - len(wl_ok),
            "min_ms": min(wl_durs) if wl_durs else 0.0,
            "max_ms": max(wl_durs) if wl_durs else 0.0,
            "mean_ms": sum(wl_durs) / len(wl_durs) if wl_durs else 0.0,
            "p50_ms": percentile(wl_durs, 0.50),
            "p95_ms": percentile(wl_durs, 0.95),
            "p99_ms": percentile(wl_durs, 0.99),
            "client_slo_violations": wl_violations,
        }

    summary = {
        "config": config,
        "workload": workload,
        "concurrency": concurrency,
        "duration_ms": duration_ms or 0,
        "deadline_us": deadline_us,
        "cpu_cores": cpu_count,
        "metadata_mode": meta.get("metadata_mode", "none"),
        "scheduler_flags": meta.get("scheduler_flags", []),
        "compute": compute,
        "load": load,
        "latency": latency,
        "scheduler": scheduler,
    }

    if per_workload:
        summary["per_workload"] = per_workload

    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze profiler trace data into COSMOS-compatible summary.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Single-run analysis
    analyze = subparsers.add_parser(
        "analyze", help="Generate compat summary.json for a profiler run directory."
    )
    analyze.add_argument("--run-dir", required=True, type=Path)
    analyze.add_argument("--config", default="profiler")
    analyze.add_argument("--deadline-us", type=int, default=0)
    analyze.add_argument("--workload", default="")
    analyze.add_argument("--out", type=Path, help="Output path. Default: <run-dir>/compat_summary.json")

    # Compare two profiler runs
    compare = subparsers.add_parser(
        "compare", help="Compare two profiler run directories."
    )
    compare.add_argument("--baseline", required=True, type=Path)
    compare.add_argument("--candidate", required=True, type=Path)
    compare.add_argument("--deadline-us", type=int, default=0)
    compare.add_argument("--baseline-config", default="cfs-default")
    compare.add_argument("--candidate-config", default="cosmos-full")
    compare.add_argument("--workload", default="")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "analyze":
        summary = build_compat_summary(
            args.run_dir,
            config=args.config,
            deadline_us=args.deadline_us,
            workload=args.workload,
        )
        out = args.out or (args.run_dir / "compat_summary.json")
        out.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {out}")
        return 0

    if args.command == "compare":
        baseline_summary = build_compat_summary(
            args.baseline,
            config=args.baseline_config,
            deadline_us=args.deadline_us,
            workload=args.workload,
        )
        candidate_summary = build_compat_summary(
            args.candidate,
            config=args.candidate_config,
            deadline_us=args.deadline_us,
            workload=args.workload,
        )

        baseline_out = args.baseline / "compat_summary.json"
        candidate_out = args.candidate / "compat_summary.json"
        baseline_out.write_text(json.dumps(baseline_summary, indent=2) + "\n", encoding="utf-8")
        candidate_out.write_text(json.dumps(candidate_summary, indent=2) + "\n", encoding="utf-8")

        # Run compare.py
        compare_script = Path(__file__).resolve().parent / "compare.py"
        result = subprocess.run(
            [sys.executable, str(compare_script), str(baseline_out), str(candidate_out)],
            check=False,
        )
        return result.returncode

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
