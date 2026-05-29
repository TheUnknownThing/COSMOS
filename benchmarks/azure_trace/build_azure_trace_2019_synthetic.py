#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import azure_replay_common as replay_common
from workload_classifier_2019 import (
    ClassifiedProfile,
    Missing2019DataError,
    duration_class,
    load_classified_profiles,
    select_profile_for_function,
)


DEFAULT_OUTPUT_DIR = Path("benchmarks/azure_trace/results/azure-2019-classified-synthetic")

FAMILY_TO_SYNTHETIC_WORKLOAD = {
    "cpu_bound": "cpu_burst",
    "network_service": "network_heavy",
    "storage_io": "io_mixed",
    "event_pipeline": "pipeline",
    "memory_heavy": "memory_heavy",
    "orchestration": "pipeline",
    "timer_control": "cpu_burst",
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build an Azure 2021 replay using a required Azure Functions 2019 "
            "probabilistic workload classifier, mapped to synthetic kernels."
        )
    )
    parser.add_argument("--trace-2021", type=Path, default=replay_common.DEFAULT_2021_TRACE)
    parser.add_argument("--dataset-2019-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--window-start-ms", type=float, default=0.0)
    parser.add_argument("--window-ms", type=float)
    parser.add_argument("--scale", type=float, default=1.0)
    parser.add_argument("--limit", type=int)
    parser.add_argument(
        "--downsample-mode",
        choices=("top-apps", "stratified-apps", "hash-functions"),
        default="top-apps",
    )
    parser.add_argument("--min-slack-ms", type=int, default=5)
    parser.add_argument("--metrics-sample-limit", type=int, default=200_000)
    parser.add_argument(
        "--max-2019-profiles",
        type=int,
        help="Deterministically cap the number of classified 2019 source profiles.",
    )
    parser.add_argument(
        "--dataset-2019-days",
        type=int,
        help="Use only the first N daily 2019 CSV files from each required family.",
    )
    parser.add_argument(
        "--dataset-2019-invocation-row-limit",
        type=int,
        help="Use only the first N real 2019 invocation rows before joining duration and memory data.",
    )
    return parser.parse_args(argv)


def profile_hints(profile: ClassifiedProfile, workload: str) -> dict[str, Any]:
    features = profile.features
    memory_bytes = int(features.memory_mb * 1024 * 1024)
    if workload == "memory_heavy":
        return {
            "cpu_intensity": 0.45,
            "memory_bytes": memory_bytes,
            "working_set_bytes": max(64 * 1024 * 1024, memory_bytes // 2),
            "cold_load_penalty_ns": 2_000_000_000,
            "io_weight": 350,
        }
    if workload == "io_mixed":
        return {
            "cpu_intensity": 0.35,
            "memory_bytes": max(memory_bytes, 128 * 1024 * 1024),
            "working_set_bytes": max(64 * 1024 * 1024, min(memory_bytes, 256 * 1024 * 1024)),
            "io_weight": 700,
            "io_bandwidth_bytes_per_sec": 64 * 1024 * 1024,
            "cold_load_penalty_ns": 750_000_000,
        }
    if workload == "network_heavy":
        return {
            "cpu_intensity": 0.30,
            "memory_bytes": max(memory_bytes, 128 * 1024 * 1024),
            "working_set_bytes": max(64 * 1024 * 1024, min(memory_bytes, 256 * 1024 * 1024)),
            "network_bandwidth_bytes_per_sec": 128 * 1024 * 1024,
            "io_weight": 300,
        }
    if workload == "pipeline":
        return {
            "cpu_intensity": 0.50,
            "memory_bytes": max(memory_bytes, 96 * 1024 * 1024),
            "working_set_bytes": max(48 * 1024 * 1024, min(memory_bytes, 256 * 1024 * 1024)),
            "cold_load_penalty_ns": 1_500_000_000
            if features.median_duration_ms > 2000
            else 750_000_000,
            "io_weight": 650,
            "io_bandwidth_bytes_per_sec": 96 * 1024 * 1024,
            "network_bandwidth_bytes_per_sec": 96 * 1024 * 1024,
            "phase_sequence": [
                {"kind": "IoBound", "duration_pct": 33},
                {"kind": "CpuBound", "duration_pct": 34},
                {"kind": "IoBound", "duration_pct": 33},
            ],
        }
    return {"cpu_intensity": 0.95, "io_weight": 400}


def write_invocations_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "event_id",
        "invocation_id",
        "at_ms",
        "source_start_time_ms",
        "app",
        "func",
        "function_id",
        "profile_id",
        "workload_family",
        "workload",
        "target_duration_class",
        "inferred_duration_class",
        "target_duration_ms",
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
        classified_profiles = load_classified_profiles(
            args.dataset_2019_dir,
            args.max_2019_profiles,
            args.dataset_2019_days,
            args.dataset_2019_invocation_row_limit,
        )
    except Missing2019DataError as exc:
        raise SystemExit(str(exc)) from exc

    selected_events, source_metrics, base_ms = replay_common.select_trace_events(args)
    profile_lookup: dict[str, dict[str, Any]] = {}
    function_profiles: dict[str, str] = {}
    invocation_rows: list[dict[str, Any]] = []
    replay_invocations: list[dict[str, Any]] = []
    family_counts: Counter[str] = Counter()
    workload_counts: Counter[str] = Counter()

    for invocation_id, event in enumerate(
        sorted(selected_events, key=lambda item: (item.source_start_ms, item.function_id)),
        start=1,
    ):
        inferred = select_profile_for_function(
            classified_profiles, event.function_id, event.duration_ms
        )
        workload = FAMILY_TO_SYNTHETIC_WORKLOAD[inferred.family]
        profile_id = f"{inferred.profile_id}_as_{workload}"
        if profile_id not in profile_lookup:
            profile_lookup[profile_id] = classified_profile_payload(inferred, workload)
        function_profiles.setdefault(event.function_id, profile_id)
        family_counts[inferred.family] += 1
        workload_counts[workload] += 1

        at_ms = (event.source_start_ms - base_ms) / args.scale
        row = {
            "event_id": f"az2021c-{invocation_id:08d}",
            "invocation_id": invocation_id,
            "at_ms": round(at_ms, 3),
            "source_start_time_ms": round(event.source_start_ms, 3),
            "app": event.app,
            "func": event.func,
            "function_id": event.function_id,
            "profile_id": profile_id,
            "workload_family": inferred.family,
            "workload": workload,
            "target_duration_class": replay_common.duration_class(event.duration_ms),
            "inferred_duration_class": duration_class(inferred.features.median_duration_ms),
            "target_duration_ms": event.duration_ms,
            "deadline_us": replay_common.deadline_us(event.duration_ms, args.min_slack_ms),
            "slo_class": replay_common.slo_for_duration(event.duration_ms),
            "classifier_confidence": round(inferred.confidence, 6),
        }
        invocation_rows.append(row)
        replay_invocations.append(
            {
                **row,
                "function_hash": event.function_id,
                "duration_ms": event.duration_ms,
                "profile_hints": {
                    key: value
                    for key, value in profile_lookup[profile_id].items()
                    if key
                    in {
                        "cpu_intensity",
                        "memory_bytes",
                        "working_set_bytes",
                        "io_weight",
                        "io_bandwidth_bytes_per_sec",
                        "network_bandwidth_bytes_per_sec",
                        "phase_sequence",
                        "cold_load_penalty_ns",
                    }
                },
            }
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_invocations_csv(args.output_dir / "invocations.csv", invocation_rows)
    created_at = datetime.now(timezone.utc).isoformat()

    profiles_payload = {
        "version": 1,
        "schema": "cosmos.azure.2019-classified-synthetic-profiles",
        "created_at": created_at,
        "source": {
            "profile_truth": (
                "Azure Functions 2019 trigger, duration, memory, and invocation "
                "shape distributions classified probabilistically"
            ),
            "arrival_truth": "Azure Functions 2021 invocation trace",
            "dataset_2019_dir": str(args.dataset_2019_dir),
            "dataset_2019_days": args.dataset_2019_days,
            "dataset_2019_invocation_row_limit": args.dataset_2019_invocation_row_limit,
        },
        "family_to_synthetic_workload": FAMILY_TO_SYNTHETIC_WORKLOAD,
        "profiles": profile_lookup,
        "function_profiles": function_profiles,
    }
    (args.output_dir / "profiles.json").write_text(
        json.dumps(profiles_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    replay_payload = {
        "version": 1,
        "schema": "cosmos.azure.2019-classified-synthetic-replay",
        "created_at": created_at,
        "source": {
            "trace_2021": str(args.trace_2021),
            "dataset_2019_dir": str(args.dataset_2019_dir),
            "dataset_2019_days": args.dataset_2019_days,
            "dataset_2019_invocation_row_limit": args.dataset_2019_invocation_row_limit,
        },
        "window": {
            "base_ms": base_ms,
            "window_start_ms": args.window_start_ms,
            "window_ms": args.window_ms,
            "scale": args.scale,
            "downsample_mode": args.downsample_mode,
            "limit": args.limit,
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
            "notes": [
                "2021 trace is used for invocation arrival and target duration timing.",
                "2019 Azure Functions data is required and used for probabilistic workload-family classification.",
                "Workload families are mapped to synthetic kernels by this script, not by the classifier module.",
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
