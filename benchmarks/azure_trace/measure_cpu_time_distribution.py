#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent

if str(SCRIPT_DIR) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(SCRIPT_DIR))

from cpu_trace_common import (
    csv_files,
    day_index_from_path,
    deadline_us_for_duration,
    duration_bucket,
    first_present,
    normalize_trigger,
    open_csv_rows,
    parse_float,
    percentile,
    require_2019_cpu_dataset,
    resolve_default_dataset_dir,
    slo_class_for_duration,
    weighted_mean,
    weighted_percentile,
)


DEFAULT_OUTPUT = SCRIPT_DIR / "results" / "azure_2019_cpu_distribution.json"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Measure a CPU service-time distribution from the Azure Functions 2019 trace. "
            "The trace exposes function execution duration rather than CPU cycles, so this "
            "script uses duration-derived service time as the synthetic CPU target."
        )
    )
    parser.add_argument(
        "--dataset-dir",
        type=Path,
        default=resolve_default_dataset_dir(),
        help="Path to azurefunctions-dataset2019",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--days", type=int, help="Use only the first N daily CSV files")
    parser.add_argument(
        "--invocation-row-limit",
        type=int,
        help="Use only the first N invocation rows across the selected day files",
    )
    parser.add_argument(
        "--top-functions",
        type=int,
        default=32,
        help="Number of heavy hitter functions to include as examples in the JSON output",
    )
    return parser.parse_args(argv)


def load_invocation_stats(
    paths: list[Path],
    row_limit: int | None,
) -> tuple[dict[str, dict[str, Any]], list[int], int]:
    functions: dict[str, dict[str, Any]] = {}
    arrival_per_minute: list[int] = []
    rows_seen = 0
    max_minutes_per_day = 0

    for fallback_day, path in enumerate(paths):
        day_index = day_index_from_path(path, fallback_day)
        for row in open_csv_rows(path):
            if row_limit is not None and rows_seen >= row_limit:
                return functions, arrival_per_minute, rows_seen
            rows_seen += 1

            owner = str(first_present(row, ("HashOwner", "owner")) or "")
            app = first_present(row, ("HashApp", "app"))
            func = first_present(row, ("HashFunction", "func"))
            if app is None or func is None:
                continue

            minute_fields = sorted((name for name in row if str(name).isdigit()), key=int)
            if not minute_fields:
                continue
            minutes_per_day = int(minute_fields[-1])
            max_minutes_per_day = max(max_minutes_per_day, minutes_per_day)

            function_id = f"{app}:{func}"
            trigger = normalize_trigger(str(row.get("Trigger") or "others"))
            stats = functions.setdefault(
                function_id,
                {
                    "function_id": function_id,
                    "owner": owner,
                    "app": str(app),
                    "func": str(func),
                    "trigger_counts": Counter(),
                    "invocation_count": 0,
                    "active_minutes": 0,
                },
            )

            total = 0
            active = 0
            for name in minute_fields:
                count = int(parse_float(row.get(name)) or 0)
                total += count
                if count > 0:
                    active += 1
                minute_index = day_index * minutes_per_day + int(name) - 1
                if minute_index >= len(arrival_per_minute):
                    arrival_per_minute.extend([0] * (minute_index + 1 - len(arrival_per_minute)))
                arrival_per_minute[minute_index] += count

            stats["trigger_counts"][trigger] += max(1, total)
            stats["invocation_count"] += total
            stats["active_minutes"] += active

    if max_minutes_per_day > 0 and arrival_per_minute:
        expected_len = ((len(arrival_per_minute) + max_minutes_per_day - 1) // max_minutes_per_day) * max_minutes_per_day
        if expected_len > len(arrival_per_minute):
            arrival_per_minute.extend([0] * (expected_len - len(arrival_per_minute)))
    return functions, arrival_per_minute, rows_seen


def load_duration_stats(
    paths: list[Path],
    allowed_functions: set[str],
) -> dict[str, dict[str, float]]:
    values: dict[str, dict[str, list[tuple[float, int]]]] = defaultdict(lambda: defaultdict(list))
    for path in paths:
        for row in open_csv_rows(path):
            app = first_present(row, ("HashApp", "app"))
            func = first_present(row, ("HashFunction", "func"))
            if app is None or func is None:
                continue
            function_id = f"{app}:{func}"
            if function_id not in allowed_functions:
                continue
            weight = max(1, int(parse_float(row.get("Count")) or 1))
            fields = {
                "min": ("Minimum", "percentile_Average_0", "percentile_Average_1"),
                "mean": ("Average", "percentile_Average_50"),
                "p25": ("percentile_Average_25", "percentile_Average_50", "Average"),
                "p50": ("percentile_Average_50", "Average", "percentile_Average_75"),
                "p75": ("percentile_Average_75", "percentile_Average_50", "Average"),
                "p99": ("percentile_Average_99", "percentile_Average_75", "Maximum", "Average"),
                "max": ("Maximum", "percentile_Average_100", "percentile_Average_99", "Average"),
            }
            for key, candidates in fields.items():
                value = parse_float(first_present(row, candidates))
                if value is not None and value >= 0:
                    values[function_id][key].append((value, weight))

    result: dict[str, dict[str, float]] = {}
    for function_id, series in values.items():
        result[function_id] = {
            "min_ms": weighted_percentile(series.get("min", series.get("p25", [])), 0.01),
            "mean_ms": weighted_mean(series.get("mean", series.get("p50", []))),
            "p25_ms": weighted_percentile(series.get("p25", series.get("p50", [])), 0.25),
            "p50_ms": weighted_percentile(series.get("p50", []), 0.50),
            "p75_ms": weighted_percentile(series.get("p75", series.get("p50", [])), 0.75),
            "p99_ms": weighted_percentile(series.get("p99", series.get("p75", [])), 0.99),
            "max_ms": weighted_percentile(series.get("max", series.get("p99", [])), 1.0),
        }
    return result


def summarize_arrivals(arrival_per_minute: list[int]) -> dict[str, Any]:
    active = [count for count in arrival_per_minute if count > 0]
    total_invocations = sum(arrival_per_minute)
    total_minutes = len(arrival_per_minute)
    return {
        "total_minutes": total_minutes,
        "active_minutes": len(active),
        "active_minute_ratio": (len(active) / total_minutes) if total_minutes else 0.0,
        "total_invocations": total_invocations,
        "mean_invocations_per_minute": (total_invocations / total_minutes) if total_minutes else 0.0,
        "mean_invocations_per_active_minute": (total_invocations / len(active)) if active else 0.0,
        "p50_invocations_per_minute": percentile(arrival_per_minute, 0.50),
        "p95_invocations_per_minute": percentile(arrival_per_minute, 0.95),
        "p99_invocations_per_minute": percentile(arrival_per_minute, 0.99),
        "max_invocations_per_minute": max(arrival_per_minute) if arrival_per_minute else 0,
        "per_minute_counts": arrival_per_minute,
    }


def build_profiles(
    invocation_stats: dict[str, dict[str, Any]],
    duration_stats: dict[str, dict[str, float]],
) -> tuple[list[dict[str, Any]], dict[str, float], list[dict[str, Any]]]:
    function_profiles: list[dict[str, Any]] = []
    weighted_means: list[tuple[float, int]] = []
    weighted_p50s: list[tuple[float, int]] = []
    weighted_p75s: list[tuple[float, int]] = []
    weighted_p99s: list[tuple[float, int]] = []
    total_invocations = 0

    for function_id, base in invocation_stats.items():
        duration = duration_stats.get(function_id)
        if duration is None:
            continue
        invocation_count = int(base["invocation_count"])
        if invocation_count <= 0:
            continue
        total_invocations += invocation_count
        trigger = base["trigger_counts"].most_common(1)[0][0] if base["trigger_counts"] else "others"
        expected_ms = max(1, int(round(duration["mean_ms"] or duration["p50_ms"] or 1)))
        function_profiles.append(
            {
                "function_id": function_id,
                "owner": base["owner"],
                "app": base["app"],
                "func": base["func"],
                "trigger": trigger,
                "invocation_count": invocation_count,
                "active_minutes": int(base["active_minutes"]),
                "expected_cpu_time_ms": float(duration["mean_ms"]),
                "cpu_time_ms": {
                    "min": float(duration["min_ms"]),
                    "p25": float(duration["p25_ms"]),
                    "p50": float(duration["p50_ms"]),
                    "p75": float(duration["p75_ms"]),
                    "p99": float(duration["p99_ms"]),
                    "max": float(duration["max_ms"]),
                },
                "suggested_deadline_us": deadline_us_for_duration(expected_ms),
                "suggested_slo_class": slo_class_for_duration(expected_ms),
            }
        )
        weighted_means.append((float(duration["mean_ms"]), invocation_count))
        weighted_p50s.append((float(duration["p50_ms"]), invocation_count))
        weighted_p75s.append((float(duration["p75_ms"]), invocation_count))
        weighted_p99s.append((float(duration["p99_ms"]), invocation_count))

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for profile in function_profiles:
        bucket = duration_bucket(profile["expected_cpu_time_ms"])
        grouped[(profile["trigger"], bucket)].append(profile)

    compact_profiles: list[dict[str, Any]] = []
    for (trigger, bucket), members in sorted(grouped.items()):
        weight = sum(int(member["invocation_count"]) for member in members)
        mean_series = [(float(member["expected_cpu_time_ms"]), int(member["invocation_count"])) for member in members]
        p25_series = [(float(member["cpu_time_ms"]["p25"]), int(member["invocation_count"])) for member in members]
        p50_series = [(float(member["cpu_time_ms"]["p50"]), int(member["invocation_count"])) for member in members]
        p75_series = [(float(member["cpu_time_ms"]["p75"]), int(member["invocation_count"])) for member in members]
        p99_series = [(float(member["cpu_time_ms"]["p99"]), int(member["invocation_count"])) for member in members]
        max_series = [(float(member["cpu_time_ms"]["max"]), int(member["invocation_count"])) for member in members]
        min_series = [(float(member["cpu_time_ms"]["min"]), int(member["invocation_count"])) for member in members]
        expected_ms = max(1, int(round(weighted_mean(mean_series))))
        compact_profiles.append(
            {
                "profile_id": f"{trigger}:{bucket}",
                "trigger": trigger,
                "cpu_time_bucket": bucket,
                "workload": "cpu_burst",
                "profile_hints": {"cpu_intensity": 0.95},
                "member_functions": len(members),
                "invocation_count": weight,
                "sampling_weight": 0.0,
                "active_minutes_mean": weighted_mean(
                    [(float(member["active_minutes"]), int(member["invocation_count"])) for member in members]
                ),
                "expected_cpu_time_ms": weighted_mean(mean_series),
                "cpu_time_ms": {
                    "min": weighted_percentile(min_series, 0.01),
                    "p25": weighted_percentile(p25_series, 0.25),
                    "p50": weighted_percentile(p50_series, 0.50),
                    "p75": weighted_percentile(p75_series, 0.75),
                    "p99": weighted_percentile(p99_series, 0.99),
                    "max": weighted_percentile(max_series, 1.0),
                },
                "suggested_deadline_us": deadline_us_for_duration(expected_ms),
                "suggested_slo_class": slo_class_for_duration(expected_ms),
            }
        )

    compact_profiles.sort(key=lambda item: (-int(item["invocation_count"]), item["profile_id"]))
    for profile in compact_profiles:
        profile["sampling_weight"] = (
            profile["invocation_count"] / total_invocations if total_invocations else 0.0
        )

    summary = {
        "total_functions": len(function_profiles),
        "total_profile_groups": len(compact_profiles),
        "total_invocations": total_invocations,
        "weighted_mean_cpu_time_ms": weighted_mean(weighted_means),
        "weighted_p50_cpu_time_ms": weighted_percentile(weighted_p50s, 0.50),
        "weighted_p75_cpu_time_ms": weighted_percentile(weighted_p75s, 0.75),
        "weighted_p95_cpu_time_ms": weighted_percentile(weighted_p99s, 0.95),
        "weighted_p99_cpu_time_ms": weighted_percentile(weighted_p99s, 0.99),
        "max_cpu_time_ms": max((profile["cpu_time_ms"]["max"] for profile in function_profiles), default=0.0),
        "trigger_mix": dict(
            sorted(
                (
                    trigger,
                    sum(
                        int(profile["invocation_count"])
                        for profile in function_profiles
                        if profile["trigger"] == trigger
                    )
                    / total_invocations,
                )
                for trigger in sorted({profile["trigger"] for profile in function_profiles})
            )
        )
        if total_invocations
        else {},
    }

    top_functions = sorted(
        function_profiles,
        key=lambda item: (-int(item["invocation_count"]), item["function_id"]),
    )[:]
    return compact_profiles, summary, top_functions


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.days is not None and args.days <= 0:
        raise SystemExit("--days must be positive")
    if args.invocation_row_limit is not None and args.invocation_row_limit <= 0:
        raise SystemExit("--invocation-row-limit must be positive")
    if args.top_functions <= 0:
        raise SystemExit("--top-functions must be positive")

    invocation_files, duration_files = require_2019_cpu_dataset(args.dataset_dir, args.days)
    invocation_stats, arrival_per_minute, rows_seen = load_invocation_stats(
        invocation_files,
        args.invocation_row_limit,
    )
    if not invocation_stats:
        raise SystemExit("no invocation rows were parsed from the dataset")
    duration_stats = load_duration_stats(duration_files, set(invocation_stats))
    profiles, cpu_summary, top_functions = build_profiles(invocation_stats, duration_stats)
    if not profiles:
        raise SystemExit("no functions had both invocation and duration data")

    created_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "version": 1,
        "schema": "cosmos.azure.2019.cpu-distribution",
        "created_at": created_at,
        "source": {
            "dataset_dir": str(args.dataset_dir),
            "invocation_files": [str(path) for path in invocation_files],
            "duration_files": [str(path) for path in duration_files],
            "days": args.days,
            "invocation_row_limit": args.invocation_row_limit,
            "rows_seen": rows_seen,
            "cpu_model_note": (
                "Azure Functions 2019 exposes execution duration rather than CPU cycles. "
                "This distribution treats duration-derived service time as the CPU target "
                "for synthetic cpu_burst workload generation."
            ),
        },
        "arrival_distribution": summarize_arrivals(arrival_per_minute),
        "cpu_time_distribution": cpu_summary,
        "profiles": profiles,
        "top_functions": top_functions[: args.top_functions],
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

