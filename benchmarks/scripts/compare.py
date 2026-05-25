#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import json
import math
from pathlib import Path
from types import ModuleType
from typing import Any


def load_measure_latency_module() -> ModuleType:
    module_path = Path(__file__).resolve().with_name("measure_latency.py")
    spec = importlib.util.spec_from_file_location("cosmos_measure_latency", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"could not load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


measure_latency = load_measure_latency_module()


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


def metric_row(
    name: str, baseline: float, candidate: float
) -> tuple[str, float, float, float, float]:
    delta = candidate - baseline
    pct = 0.0 if baseline == 0 else (delta / baseline) * 100.0
    return (name, baseline, candidate, delta, pct)


def slo_violation_rate(summary: dict[str, Any]) -> float:
    latency = summary["latency"]
    successes = float(latency.get("successes", 0))
    if successes <= 0.0:
        return math.inf
    return float(latency["client_slo_violations"]) / successes


def compare_summaries(
    baseline: dict[str, Any], candidate: dict[str, Any]
) -> list[tuple[str, float, float, float, float]]:
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
            "client_slo_violation_rate",
            slo_violation_rate(baseline),
            slo_violation_rate(candidate),
        ),
        metric_row(
            "scheduler_slo_violations",
            float(baseline["scheduler"]["last"].get("nr_slo_violations", 0)),
            float(candidate["scheduler"]["last"].get("nr_slo_violations", 0)),
        ),
    ]


def format_load_line(label: str, summary: dict[str, Any]) -> str:
    load = measure_latency.assess_summary_load(summary)
    return (
        f"{label:<9}: {load['class']} "
        f"fair={str(load['fair']).lower()} "
        f"ratio={load['load_ratio']:.3f} "
        f"demand_ms={load['total_compute_ms']:.3f} "
        f"capacity_ms={load['capacity_ms']:.3f} "
        f"cpus={load['cpu_cores']} "
        f"source={load['compute_source']}"
    )


def fair_case_verdict(baseline: dict[str, Any], candidate: dict[str, Any]) -> str:
    base_load = measure_latency.assess_summary_load(baseline)
    cand_load = measure_latency.assess_summary_load(candidate)
    if not base_load["fair"] or not cand_load["fair"]:
        return (
            "verdict  : unfair overloaded case; do not use this run to judge "
            "whether a scheduler can optimize both p99 and SLO hit rate."
        )

    eps = 1e-9
    base_latency = baseline["latency"]
    cand_latency = candidate["latency"]
    base_p99 = float(base_latency["p99_ms"])
    cand_p99 = float(cand_latency["p99_ms"])
    base_slo_rate = slo_violation_rate(baseline)
    cand_slo_rate = slo_violation_rate(candidate)

    p99_better = cand_p99 < base_p99 - eps
    p99_no_worse = cand_p99 <= base_p99 + eps
    slo_better = cand_slo_rate < base_slo_rate - eps
    slo_no_worse = cand_slo_rate <= base_slo_rate + eps

    if p99_no_worse and slo_no_worse and (p99_better or slo_better):
        return "verdict  : fair case; candidate improves or preserves both p99 and SLO hit rate."
    if p99_better and not slo_no_worse:
        return "verdict  : fair case; candidate improves p99 but hurts SLO hit rate."
    if slo_better and not p99_no_worse:
        return "verdict  : fair case; candidate improves SLO hit rate but hurts p99."
    if p99_no_worse and slo_no_worse:
        return "verdict  : fair case; candidate ties baseline on p99 and SLO hit rate."
    return "verdict  : fair case; candidate does not improve the p99/SLO objective."


def print_report(baseline_path: Path, candidate_path: Path) -> int:
    baseline = load_summary(baseline_path)
    candidate = load_summary(candidate_path)
    rows = compare_summaries(baseline, candidate)

    print(
        f"baseline : {baseline['config']} ({resolve_summary_path(baseline_path).parent})"
    )
    print(
        f"candidate: {candidate['config']} ({resolve_summary_path(candidate_path).parent})"
    )
    print(f"workload : {baseline['workload']} @ concurrency={baseline['concurrency']}")
    print(format_load_line("baseline", baseline))
    print(format_load_line("candidate", candidate))
    print(fair_case_verdict(baseline, candidate))
    print("")
    print(
        f"{'metric':<26} {'baseline':>12} {'candidate':>12} {'delta':>12} {'delta%':>10}"
    )
    print("-" * 76)
    for name, base, cand, delta, pct in rows:
        print(f"{name:<26} {base:>12.3f} {cand:>12.3f} {delta:>12.3f} {pct:>9.1f}%")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compare two COSMOS benchmark result directories."
    )
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    args = parser.parse_args(argv)
    return print_report(args.baseline, args.candidate)


if __name__ == "__main__":
    raise SystemExit(main())
