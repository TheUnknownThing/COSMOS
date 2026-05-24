#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def resolve_summary_path(path: Path) -> Path:
    if path.is_file():
        return path

    direct = path / "summary.json"
    if direct.exists():
        return direct

    latest = path / "latest"
    if latest.exists():
        latest_summary = latest.resolve() / "summary.json"
        if latest_summary.exists():
            return latest_summary

    candidates = sorted(
        (
            candidate / "summary.json"
            for candidate in path.iterdir()
            if candidate.is_dir() and (candidate / "summary.json").exists()
        ),
        key=lambda summary: summary.stat().st_mtime,
        reverse=True,
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(f"could not find summary.json under {path}")


def load_summary(path: Path) -> dict[str, Any]:
    return json.loads(resolve_summary_path(path).read_text(encoding="utf-8"))


def metric_row(name: str, baseline: float, candidate: float) -> tuple[str, float, float, float, float]:
    delta = candidate - baseline
    pct = 0.0 if baseline == 0 else (delta / baseline) * 100.0
    return (name, baseline, candidate, delta, pct)


def compare_summaries(baseline: dict[str, Any], candidate: dict[str, Any]) -> list[tuple[str, float, float, float, float]]:
    base_latency = baseline["latency"]
    cand_latency = candidate["latency"]
    return [
        metric_row("p50_ms", base_latency["p50_ms"], cand_latency["p50_ms"]),
        metric_row("p95_ms", base_latency["p95_ms"], cand_latency["p95_ms"]),
        metric_row("p99_ms", base_latency["p99_ms"], cand_latency["p99_ms"]),
        metric_row("mean_ms", base_latency["mean_ms"], cand_latency["mean_ms"]),
        metric_row(
            "client_slo_violations",
            float(base_latency["client_slo_violations"]),
            float(cand_latency["client_slo_violations"]),
        ),
        metric_row(
            "scheduler_slo_violations",
            float(baseline["scheduler"]["last"].get("nr_slo_violations", 0)),
            float(candidate["scheduler"]["last"].get("nr_slo_violations", 0)),
        ),
    ]


def print_report(baseline_path: Path, candidate_path: Path) -> int:
    baseline = load_summary(baseline_path)
    candidate = load_summary(candidate_path)
    rows = compare_summaries(baseline, candidate)

    print(f"baseline : {baseline['config']} ({resolve_summary_path(baseline_path).parent})")
    print(f"candidate: {candidate['config']} ({resolve_summary_path(candidate_path).parent})")
    print(f"workload : {baseline['workload']} @ concurrency={baseline['concurrency']}")
    print("")
    print(f"{'metric':<26} {'baseline':>12} {'candidate':>12} {'delta':>12} {'delta%':>10}")
    print("-" * 76)
    for name, base, cand, delta, pct in rows:
        print(f"{name:<26} {base:>12.3f} {cand:>12.3f} {delta:>12.3f} {pct:>9.1f}%")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare two COSMOS benchmark result directories.")
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    args = parser.parse_args(argv)
    return print_report(args.baseline, args.candidate)


if __name__ == "__main__":
    raise SystemExit(main())
