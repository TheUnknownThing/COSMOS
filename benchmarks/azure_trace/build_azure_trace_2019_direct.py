#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import azure_replay_common as replay_common
from build_azure_trace_2019_synthetic import (
    FAMILY_TO_SYNTHETIC_WORKLOAD,
    profile_hints,
)
from workload_classifier_2019 import (
    ClassifiedProfile,
    Missing2019DataError,
    duration_class,
    first_present,
    load_classified_profiles,
    open_csv_rows,
    parse_float,
    require_2019_dataset,
)


DEFAULT_OUTPUT_DIR = Path("benchmarks/azure_trace/results/azure-2019-direct-synthetic")

ARRIVAL_MODES = (
    "uniform-within-minute",
    "front-loaded-burst",
    "evenly-spaced",
    "clustered-bursty",
)

PROFILE_HINT_KEYS = {
    "cpu_intensity",
    "memory_bytes",
    "working_set_bytes",
    "io_weight",
    "io_bandwidth_bytes_per_sec",
    "network_bandwidth_bytes_per_sec",
    "phase_sequence",
    "cold_load_penalty_ns",
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build an Azure Functions 2019 direct-arrival replay. The trace gives "
            "per-minute counts, and this script expands them into deterministic "
            "sub-minute synthetic arrivals."
        )
    )
    parser.add_argument("--dataset-2019-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--arrival-mode", choices=ARRIVAL_MODES, default="uniform-within-minute")
    parser.add_argument(
        "--workload-mix-mode",
        choices=["trace", "balanced", "peak-stress"],
        default="trace",
        help=(
            "trace preserves the classified Azure mix, balanced equalizes emitted "
            "workload families, and peak-stress keeps the dominant family in the "
            "selected window."
        ),
    )
    parser.add_argument(
        "--deadline-mode",
        choices=["duration-headroom", "target-duration-aware"],
        default="duration-headroom",
        help=(
            "Deadline labeling mode. target-duration-aware keeps deadlines tied "
            "to each invocation target duration and marks the replay accordingly."
        ),
    )
    parser.add_argument("--window-start-ms", type=float, default=0.0)
    parser.add_argument("--window-ms", type=float)
    parser.add_argument("--scale", type=float, default=1.0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--min-slack-ms", type=int, default=5)
    parser.add_argument("--metrics-sample-limit", type=int, default=200_000)
    parser.add_argument(
        "--max-2019-profiles",
        type=int,
        help="Deterministically cap the number of classified source profiles.",
    )
    parser.add_argument(
        "--dataset-2019-days",
        type=int,
        help="Use only the first N daily 2019 CSV files from each required family.",
    )
    parser.add_argument(
        "--dataset-2019-invocation-row-limit",
        type=int,
        help="Use only the first N 2019 invocation rows before expansion.",
    )
    return parser.parse_args(argv)


def day_index(path: Path, fallback: int) -> int:
    match = re.search(r"\.d(\d+)\.", path.name)
    if not match:
        return fallback
    return max(0, int(match.group(1)) - 1)


def stable_fraction(key: str) -> float:
    return replay_common.stable_u64(key) / float(2**64)


def subminute_offset_ms(mode: str, count: int, index: int, key: str) -> float:
    count = max(1, count)
    if mode == "evenly-spaced":
        return ((index + 0.5) * 60_000.0) / count
    if mode == "front-loaded-burst":
        span_ms = min(10_000.0, 60_000.0)
        jitter = (stable_fraction(f"{key}:front-jitter") - 0.5) * min(500.0, span_ms / count)
        return max(0.0, min(59_999.999, ((index + 0.5) * span_ms) / count + jitter))
    if mode == "clustered-bursty":
        clusters = 1 + (replay_common.stable_u64(f"{key}:clusters") % 4)
        cluster = index % clusters
        center = ((cluster + 0.5) * 60_000.0) / clusters
        width = min(5_000.0, 30_000.0 / clusters)
        jitter = (stable_fraction(f"{key}:cluster-jitter") - 0.5) * width
        return max(0.0, min(59_999.999, center + jitter))
    return stable_fraction(f"{key}:uniform") * 60_000.0


def sampled_duration_ms(profile: ClassifiedProfile, key: str) -> int:
    features = profile.features
    draw = stable_fraction(f"{key}:duration")
    if draw < 0.25:
        value = features.duration_p25_ms
    elif draw < 0.50:
        value = features.median_duration_ms
    elif draw < 0.75:
        value = features.duration_p75_ms
    elif draw < 0.99:
        value = features.duration_p99_ms
    else:
        value = features.duration_max_ms
    return max(1, int(round(value)))


def in_window(start_ms: float, end_ms: float | None) -> bool:
    if start_ms < 0:
        return False
    if end_ms is not None and start_ms > end_ms:
        return False
    return True


def minute_overlaps_window(minute_start_ms: float, window_start_ms: float, end_ms: float | None) -> bool:
    minute_end_ms = minute_start_ms + 60_000.0
    if minute_end_ms < window_start_ms:
        return False
    if end_ms is not None and minute_start_ms > end_ms:
        return False
    return True


def iter_2019_direct_events(
    invocation_paths: list[Path],
    profiles_by_function: dict[str, ClassifiedProfile],
    args: argparse.Namespace,
) -> list[tuple[replay_common.TraceEvent, ClassifiedProfile]]:
    end_ms = args.window_start_ms + args.window_ms if args.window_ms is not None else None
    events: list[tuple[replay_common.TraceEvent, ClassifiedProfile]] = []
    rows_seen = 0
    for fallback_day, path in enumerate(invocation_paths):
        day = day_index(path, fallback_day)
        day_start_ms = day * 1440 * 60_000.0
        day_end_ms = day_start_ms + 1440 * 60_000.0
        if day_end_ms < args.window_start_ms:
            continue
        if end_ms is not None and day_start_ms > end_ms:
            break
        for row in open_csv_rows(path):
            if (
                args.dataset_2019_invocation_row_limit is not None
                and rows_seen >= args.dataset_2019_invocation_row_limit
            ):
                return sorted(events, key=lambda item: (item[0].source_start_ms, item[0].function_id))
            rows_seen += 1
            app = first_present(row, ("HashApp", "app"))
            func = first_present(row, ("HashFunction", "func"))
            if app is None or func is None:
                continue
            app = str(app)
            func = str(func)
            function_id = f"{app}:{func}"
            profile = profiles_by_function.get(function_id)
            if profile is None:
                continue
            for name, value in row.items():
                if not str(name).isdigit():
                    continue
                minute_index = day * 1440 + int(name) - 1
                minute_start_ms = minute_index * 60_000.0
                if not minute_overlaps_window(minute_start_ms, args.window_start_ms, end_ms):
                    continue
                count = int(parse_float(value) or 0)
                if count <= 0:
                    continue
                for index in range(count):
                    key = f"{path.name}:{function_id}:{name}:{index}"
                    source_start_ms = (
                        minute_start_ms + subminute_offset_ms(args.arrival_mode, count, index, key)
                    )
                    relative_ms = source_start_ms - args.window_start_ms
                    if not in_window(relative_ms, end_ms - args.window_start_ms if end_ms else None):
                        continue
                    duration_ms = sampled_duration_ms(profile, key)
                    events.append(
                        (
                            replay_common.TraceEvent(
                                source_start_ms=source_start_ms,
                                function_id=function_id,
                                app=app,
                                func=func,
                                duration_ms=duration_ms,
                                owner=profile.features.owner,
                            ),
                            profile,
                        )
                    )
                    if args.limit is not None and len(events) >= args.limit:
                        return sorted(
                            events, key=lambda item: (item[0].source_start_ms, item[0].function_id)
                        )
    return sorted(events, key=lambda item: (item[0].source_start_ms, item[0].function_id))


def collect_window_function_ids(invocation_paths: list[Path], args: argparse.Namespace) -> set[str]:
    end_ms = args.window_start_ms + args.window_ms if args.window_ms is not None else None
    function_ids: set[str] = set()
    rows_seen = 0
    for fallback_day, path in enumerate(invocation_paths):
        day = day_index(path, fallback_day)
        day_start_ms = day * 1440 * 60_000.0
        day_end_ms = day_start_ms + 1440 * 60_000.0
        if day_end_ms < args.window_start_ms:
            continue
        if end_ms is not None and day_start_ms > end_ms:
            break
        for row in open_csv_rows(path):
            if (
                args.dataset_2019_invocation_row_limit is not None
                and rows_seen >= args.dataset_2019_invocation_row_limit
            ):
                return function_ids
            rows_seen += 1
            app = first_present(row, ("HashApp", "app"))
            func = first_present(row, ("HashFunction", "func"))
            if app is None or func is None:
                continue
            has_count_in_window = False
            for name, value in row.items():
                if not str(name).isdigit():
                    continue
                minute_index = day * 1440 + int(name) - 1
                minute_start_ms = minute_index * 60_000.0
                if not minute_overlaps_window(minute_start_ms, args.window_start_ms, end_ms):
                    continue
                if int(parse_float(value) or 0) > 0:
                    has_count_in_window = True
                    break
            if has_count_in_window:
                function_ids.add(f"{app}:{func}")
    return function_ids


def build_metrics(events: list[replay_common.TraceEvent], sample_limit: int) -> replay_common.Metrics:
    metrics = replay_common.Metrics()
    for event in events:
        metrics.add(event, sample_limit)
    return metrics


def apply_workload_mix_mode(
    selected: list[tuple[replay_common.TraceEvent, ClassifiedProfile]],
    mode: str,
) -> list[tuple[replay_common.TraceEvent, ClassifiedProfile]]:
    if mode == "trace":
        return selected

    grouped: dict[str, list[tuple[replay_common.TraceEvent, ClassifiedProfile]]] = {}
    for event, profile in selected:
        workload = FAMILY_TO_SYNTHETIC_WORKLOAD[profile.family]
        grouped.setdefault(workload, []).append((event, profile))
    if not grouped:
        return selected

    if mode == "balanced":
        per_workload = min(len(items) for items in grouped.values())
        balanced = [
            item
            for workload in sorted(grouped)
            for item in grouped[workload][:per_workload]
        ]
        return sorted(balanced, key=lambda item: (item[0].source_start_ms, item[0].function_id))

    if mode == "peak-stress":
        dominant_workload = max(sorted(grouped), key=lambda workload: len(grouped[workload]))
        return grouped[dominant_workload]

    raise ValueError(f"unknown workload mix mode: {mode}")


def write_invocations_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "event_id",
        "invocation_id",
        "at_ms",
        "source_start_time_ms",
        "hash_owner",
        "app",
        "func",
        "function_id",
        "app_group",
        "app_function_count",
        "profile_id",
        "workload_family",
        "workload",
        "target_duration_class",
        "target_duration_ms",
        "duration_p25_ms",
        "duration_p50_ms",
        "duration_p75_ms",
        "duration_p99_ms",
        "duration_max_ms",
        "memory_p50_mb",
        "memory_p75_mb",
        "memory_p95_mb",
        "memory_p99_mb",
        "deadline_us",
        "slo_class",
        "classifier_confidence",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def classified_profile_payload(profile: ClassifiedProfile, workload: str) -> dict[str, Any]:
    return {
        "profile_id": profile.profile_id,
        "workload_family": profile.family,
        "workload": workload,
        "kernel": workload,
        "classifier_confidence": profile.confidence,
        "classifier_probabilities": profile.probabilities,
        "classifier_reason": profile.reason,
        "source_2019_features": asdict(profile.features),
        "profile_duration_class": duration_class(profile.features.median_duration_ms),
        "duration_class": duration_class(profile.features.median_duration_ms),
        **profile_hints(profile, workload),
    }


def row_from_event(
    invocation_id: int,
    event: replay_common.TraceEvent,
    profile: ClassifiedProfile,
    profile_id: str,
    workload: str,
    base_ms: float,
    scale: float,
    min_slack_ms: int,
) -> dict[str, Any]:
    features = profile.features
    return {
        "event_id": f"az2019d-{invocation_id:08d}",
        "invocation_id": invocation_id,
        "at_ms": round((event.source_start_ms - base_ms) / scale, 3),
        "source_start_time_ms": round(event.source_start_ms, 3),
        "hash_owner": features.owner,
        "app": event.app,
        "func": event.func,
        "function_id": event.function_id,
        "app_group": f"{features.owner}:{features.app}" if features.owner else features.app,
        "app_function_count": features.app_function_count,
        "profile_id": profile_id,
        "workload_family": profile.family,
        "workload": workload,
        "target_duration_class": replay_common.duration_class(event.duration_ms),
        "target_duration_ms": event.duration_ms,
        "duration_p25_ms": features.duration_p25_ms,
        "duration_p50_ms": features.median_duration_ms,
        "duration_p75_ms": features.duration_p75_ms,
        "duration_p99_ms": features.duration_p99_ms,
        "duration_max_ms": features.duration_max_ms,
        "memory_p50_mb": features.memory_mb,
        "memory_p75_mb": features.memory_p75_mb,
        "memory_p95_mb": features.memory_p95_mb,
        "memory_p99_mb": features.memory_p99_mb,
        "deadline_us": replay_common.deadline_us(event.duration_ms, min_slack_ms),
        "slo_class": replay_common.slo_for_duration(event.duration_ms),
        "classifier_confidence": round(profile.confidence, 6),
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.scale <= 0:
        raise SystemExit("--scale must be greater than zero")
    if args.limit is not None and args.limit <= 0:
        raise SystemExit("--limit must be positive")
    if args.max_2019_profiles is not None and args.max_2019_profiles <= 0:
        raise SystemExit("--max-2019-profiles must be positive")
    if args.dataset_2019_days is not None and args.dataset_2019_days <= 0:
        raise SystemExit("--dataset-2019-days must be positive")
    if (
        args.dataset_2019_invocation_row_limit is not None
        and args.dataset_2019_invocation_row_limit <= 0
    ):
        raise SystemExit("--dataset-2019-invocation-row-limit must be positive")

    try:
        invocation_files, _, _ = require_2019_dataset(args.dataset_2019_dir, args.dataset_2019_days)
        window_function_ids = collect_window_function_ids(invocation_files, args)
        if not window_function_ids:
            raise SystemExit("no 2019 functions had invocations in the requested window")
        classified_profiles = load_classified_profiles(
            args.dataset_2019_dir,
            args.max_2019_profiles,
            args.dataset_2019_days,
            args.dataset_2019_invocation_row_limit,
            window_function_ids,
        )
    except Missing2019DataError as exc:
        raise SystemExit(str(exc)) from exc

    profiles_by_function = {
        profile.features.function_id: profile for profile in classified_profiles
    }
    selected = apply_workload_mix_mode(
        iter_2019_direct_events(invocation_files, profiles_by_function, args),
        args.workload_mix_mode,
    )
    if not selected:
        raise SystemExit("no 2019 invocations matched the requested window")

    selected_events = [event for event, _profile in selected]
    base_ms = args.window_start_ms
    source_metrics = build_metrics(selected_events, args.metrics_sample_limit)
    profile_lookup: dict[str, dict[str, Any]] = {}
    function_profiles: dict[str, str] = {}
    app_profiles: dict[str, list[str]] = {}
    invocation_rows: list[dict[str, Any]] = []
    replay_invocations: list[dict[str, Any]] = []
    family_counts: Counter[str] = Counter()
    workload_counts: Counter[str] = Counter()

    for invocation_id, (event, profile) in enumerate(selected, start=1):
        workload = FAMILY_TO_SYNTHETIC_WORKLOAD[profile.family]
        profile_id = f"{profile.profile_id}_as_{workload}"
        if profile_id not in profile_lookup:
            profile_lookup[profile_id] = classified_profile_payload(profile, workload)
        function_profiles.setdefault(event.function_id, profile_id)
        app_group = f"{profile.features.owner}:{profile.features.app}" if profile.features.owner else profile.features.app
        app_profiles.setdefault(app_group, [])
        if profile_id not in app_profiles[app_group]:
            app_profiles[app_group].append(profile_id)
        family_counts[profile.family] += 1
        workload_counts[workload] += 1

        row = row_from_event(
            invocation_id,
            event,
            profile,
            profile_id,
            workload,
            base_ms,
            args.scale,
            args.min_slack_ms,
        )
        hints = {
            key: value
            for key, value in profile_lookup[profile_id].items()
            if key in PROFILE_HINT_KEYS
        }
        invocation_rows.append(row)
        replay_invocations.append(
            {
                **row,
                "function_hash": event.function_id,
                "duration_ms": event.duration_ms,
                "deadline_source": (
                    "target-duration-aware"
                    if args.deadline_mode == "target-duration-aware"
                    else "duration-headroom; override with OpenWhisk calibration for SLO metrics"
                ),
                "profile_hints": hints,
            }
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_invocations_csv(args.output_dir / "invocations.csv", invocation_rows)
    created_at = datetime.now(timezone.utc).isoformat()

    profiles_payload = {
        "version": 1,
        "schema": "cosmos.azure.2019-direct-synthetic-profiles",
        "created_at": created_at,
        "source": {
            "profile_truth": "Azure Functions 2019 trigger, duration, memory, and per-minute invocation CSVs",
            "arrival_truth": "Azure Functions 2019 per-minute invocation counts",
            "dataset_2019_dir": str(args.dataset_2019_dir),
            "dataset_2019_days": args.dataset_2019_days,
            "dataset_2019_invocation_row_limit": args.dataset_2019_invocation_row_limit,
        },
        "family_to_synthetic_workload": FAMILY_TO_SYNTHETIC_WORKLOAD,
        "profiles": profile_lookup,
        "function_profiles": function_profiles,
        "app_profiles": app_profiles,
    }
    (args.output_dir / "profiles.json").write_text(
        json.dumps(profiles_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    replay_payload = {
        "version": 1,
        "schema": "cosmos.azure.2019-direct-synthetic-replay",
        "created_at": created_at,
        "source": {
            "dataset_2019_dir": str(args.dataset_2019_dir),
            "arrival_truth": "Azure Functions 2019 per-minute invocation counts expanded deterministically",
            "profile_truth": "Azure Functions 2019 classified synthetic profiles",
        },
        "window": {
            "base_ms": base_ms,
            "window_start_ms": args.window_start_ms,
            "window_ms": args.window_ms,
            "scale": args.scale,
            "limit": args.limit,
            "arrival_mode": args.arrival_mode,
            "workload_mix_mode": args.workload_mix_mode,
            "deadline_mode": args.deadline_mode,
        },
        "classifier_summary": {
            "source_2019_profiles": len(classified_profiles),
            "family_counts": dict(sorted(family_counts.items())),
            "workload_counts": dict(sorted(workload_counts.items())),
        },
        "profiles_path": "profiles.json",
        "invocations_path": "invocations.csv",
        "invocations": replay_invocations,
    }
    (args.output_dir / "replay.json").write_text(
        json.dumps(replay_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    fidelity = replay_common.fidelity_payload(source_metrics, selected_events, base_ms, args.scale)
    fidelity.update(
        {
            "created_at": created_at,
            "arrival_mode": args.arrival_mode,
            "workload_mix_mode": args.workload_mix_mode,
            "deadline_mode": args.deadline_mode,
            "notes": [
                "2019 per-minute arrivals are used directly.",
                "Sub-minute placement is deterministic synthetic expansion because the public 2019 trace is minute-granular.",
                "Resource behavior remains classified synthetic, not measured Azure execution.",
            ],
        }
    )
    (args.output_dir / "fidelity.json").write_text(
        json.dumps(fidelity, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    print(args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
