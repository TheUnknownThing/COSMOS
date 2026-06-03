#!/usr/bin/env python3
"""Generate mixed top-functions config from Azure distribution for benchmarking.

Picks functions from both frequent and CPU-time-heavy functions and writes a config with:
- Frequency (sampling weight)
- Expected time (fixed, configurable: p50/p75/p99)
- Time distribution (min, p25, p50, p75, p99, max)

The key insight: expected_time is what we tell the scheduler (fixed),
but actual_time is sampled from the distribution (varies per invocation).
This tests scheduler robustness to estimation error.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate mixed top-functions config from Azure distribution"
    )
    parser.add_argument(
        "--distribution-json",
        type=Path,
        required=True,
        help="Path to azure_2019_cpu_distribution.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=SCRIPT_DIR / "results" / "top_functions_config.json",
        help="Output config file",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Total selection budget; defaults to frequency half + total-time half",
    )
    parser.add_argument(
        "--frequency-top-functions",
        type=int,
        default=None,
        help="Functions to take by invocation count. Defaults to ceil(top-n / 2).",
    )
    parser.add_argument(
        "--time-top-functions",
        type=int,
        default=None,
        help="Functions to take by total CPU time. Defaults to floor(top-n / 2).",
    )
    parser.add_argument(
        "--expected-time-percentile",
        type=str,
        default="p50",
        choices=["p25", "p50", "p75", "p99", "mean"],
        help="Which percentile to use as expected_time (what we tell the scheduler)",
    )
    return parser.parse_args(argv)


def mean_cpu_time_ms(func: dict) -> float:
    value = func.get("expected_cpu_time_ms")
    if value is not None:
        return float(value)
    cpu_time = func.get("cpu_time_ms", {})
    if "mean" in cpu_time:
        return float(cpu_time["mean"])
    return float(cpu_time.get("p50", 1.0))


def total_cpu_time_ms(func: dict) -> float:
    return float(func["invocation_count"]) * mean_cpu_time_ms(func)


def function_key(func: dict) -> str:
    return str(func["function_id"])


def select_functions(
    candidates: list[dict],
    frequency_count: int,
    time_count: int,
    target_count: int,
) -> list[dict]:
    by_frequency = sorted(
        candidates,
        key=lambda item: (-int(item["invocation_count"]), function_key(item)),
    )
    by_time = sorted(
        candidates,
        key=lambda item: (-total_cpu_time_ms(item), function_key(item)),
    )

    frequency_ranks = {
        function_key(item): rank for rank, item in enumerate(by_frequency, start=1)
    }
    time_ranks = {function_key(item): rank for rank, item in enumerate(by_time, start=1)}

    selected_by_id: dict[str, dict] = {}
    reasons: dict[str, list[str]] = {}
    for item in by_frequency[:frequency_count]:
        key = function_key(item)
        selected_by_id[key] = item
        reasons.setdefault(key, []).append("frequency")
    for item in by_time[:time_count]:
        key = function_key(item)
        selected_by_id[key] = item
        reasons.setdefault(key, []).append("total_cpu_time")

    fill_from_frequency = True
    freq_idx = frequency_count
    time_idx = time_count
    while len(selected_by_id) < min(target_count, len(candidates)):
        source = "frequency_fill" if fill_from_frequency else "total_cpu_time_fill"
        ranking = by_frequency if fill_from_frequency else by_time
        idx = freq_idx if fill_from_frequency else time_idx

        while idx < len(ranking) and function_key(ranking[idx]) in selected_by_id:
            idx += 1
        if idx < len(ranking):
            item = ranking[idx]
            key = function_key(item)
            selected_by_id[key] = item
            reasons.setdefault(key, []).append(source)
            idx += 1

        if fill_from_frequency:
            freq_idx = idx
        else:
            time_idx = idx
        fill_from_frequency = not fill_from_frequency

        if freq_idx >= len(by_frequency) and time_idx >= len(by_time):
            break

    def selected_sort_key(item: dict) -> tuple[int, int, int, str]:
        key = function_key(item)
        freq_rank = frequency_ranks[key]
        time_rank = time_ranks[key]
        reason_priority = 0 if "frequency" in reasons[key] else 1
        return (reason_priority, min(freq_rank, time_rank), freq_rank, key)

    selected = sorted(selected_by_id.values(), key=selected_sort_key)
    for item in selected:
        key = function_key(item)
        item["_selection_reasons"] = reasons[key]
        item["_frequency_rank"] = frequency_ranks[key]
        item["_total_cpu_time_rank"] = time_ranks[key]
        item["_total_cpu_time_ms"] = total_cpu_time_ms(item)
    return selected


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.top_n < 1:
        raise SystemExit("--top-n must be at least 1")

    # Load distribution
    payload = json.loads(args.distribution_json.read_text(encoding="utf-8"))

    if payload.get("schema") != "cosmos.azure.2019.cpu-distribution":
        raise SystemExit(f"Unsupported schema: {payload.get('schema')}")

    # Prefer all profiles if present; older distribution files may only have top_functions.
    candidates = payload.get("profiles") or payload.get("top_functions", [])
    if not candidates:
        raise SystemExit("No profiles/top_functions found in distribution JSON")

    frequency_count = (
        args.frequency_top_functions
        if args.frequency_top_functions is not None
        else (args.top_n + 1) // 2
    )
    time_count = (
        args.time_top_functions
        if args.time_top_functions is not None
        else args.top_n // 2
    )
    if frequency_count < 0 or time_count < 0:
        raise SystemExit("selection counts must be non-negative")
    if frequency_count == 0 and time_count == 0:
        raise SystemExit("at least one selection count must be positive")

    selected = select_functions(candidates, frequency_count, time_count, args.top_n)
    total_invocations = sum(f["invocation_count"] for f in selected)
    total_cpu_ms = sum(f["_total_cpu_time_ms"] for f in selected)

    # Build config
    functions = []
    for i, func in enumerate(selected):
        # Determine expected time based on percentile choice
        if args.expected_time_percentile == "mean":
            expected_ms = func["expected_cpu_time_ms"]
        else:
            expected_ms = func["cpu_time_ms"][args.expected_time_percentile]

        functions.append({
            "function_id": func["function_id"],
            "function_index": i,
            "frequency": func["invocation_count"] / total_invocations,
            "expected_time_ms": expected_ms,  # Fixed - what we tell scheduler
            "time_distribution": {  # Variable - actual sampled time
                "min": func["cpu_time_ms"]["min"],
                "p25": func["cpu_time_ms"]["p25"],
                "p50": func["cpu_time_ms"]["p50"],
                "p75": func["cpu_time_ms"]["p75"],
                "p99": func["cpu_time_ms"]["p99"],
                "max": func["cpu_time_ms"]["max"],
            },
            "metadata": {
                "invocation_count": func["invocation_count"],
                "total_cpu_time_ms": func["_total_cpu_time_ms"],
                "frequency_rank": func["_frequency_rank"],
                "total_cpu_time_rank": func["_total_cpu_time_rank"],
                "selection_reasons": func["_selection_reasons"],
                "trigger": func.get("trigger", "unknown"),
            },
        })

    config = {
        "version": 1,
        "schema": "cosmos.azure.top-functions-config",
        "source": {
            "distribution_json": str(args.distribution_json),
            "top_n": args.top_n,
            "selection": "frequency_and_total_cpu_time",
            "frequency_top_functions": frequency_count,
            "time_top_functions": time_count,
            "expected_time_percentile": args.expected_time_percentile,
        },
        "summary": {
            "function_count": len(functions),
            "total_invocations": total_invocations,
            "total_cpu_time_ms": total_cpu_ms,
            "coverage": total_invocations / payload["cpu_time_distribution"]["total_invocations"],
        },
        "functions": functions,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")

    print(f"Generated config with {len(functions)} functions")
    print(f"  Selection: top {frequency_count} by frequency + top {time_count} by total CPU time")
    print(f"  Coverage: {config['summary']['coverage']*100:.1f}% of trace invocations")
    print(f"  Expected time: {args.expected_time_percentile}")
    print(f"  Output: {args.output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
