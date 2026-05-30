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


DEFAULT_OUTPUT_DIR = Path("benchmarks/azure_trace/results/azure-2019-classified-sebs")

SEBS_BENCHMARKS = {
    "010.sleep": {
        "path": "000.microbenchmarks/010.sleep",
        "default_runtime": "python",
        "kind": "micro",
    },
    "110.dynamic-html": {
        "path": "100.webapps/110.dynamic-html",
        "default_runtime": "python",
        "kind": "web",
    },
    "120.uploader": {
        "path": "100.webapps/120.uploader",
        "default_runtime": "python",
        "kind": "web-storage",
    },
    "210.thumbnailer": {
        "path": "200.multimedia/210.thumbnailer",
        "default_runtime": "python",
        "kind": "multimedia-storage",
    },
    "220.video-processing": {
        "path": "200.multimedia/220.video-processing",
        "default_runtime": "python",
        "kind": "multimedia-pipeline",
    },
    "311.compression": {
        "path": "300.utilities/311.compression",
        "default_runtime": "python",
        "kind": "utility",
    },
    "411.image-recognition": {
        "path": "400.inference/411.image-recognition",
        "default_runtime": "python",
        "kind": "inference",
    },
    "503.graph-bfs": {
        "path": "500.scientific/503.graph-bfs",
        "default_runtime": "python",
        "kind": "scientific",
    },
}

FAMILY_TO_SEBS_CANDIDATES = {
    "cpu_bound": ("110.dynamic-html", "311.compression", "503.graph-bfs"),
    "network_service": ("120.uploader", "110.dynamic-html"),
    "storage_io": ("210.thumbnailer", "311.compression"),
    "event_pipeline": ("220.video-processing", "311.compression"),
    "memory_heavy": ("411.image-recognition",),
    "orchestration": ("220.video-processing",),
    "timer_control": ("010.sleep",),
}

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
            "Build an Azure 2021 replay using a required Azure Functions 2019 "
            "probabilistic workload classifier, mapped to SeBS benchmarks."
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
    parser.add_argument(
        "--runtime",
        default="python",
        help="Preferred SeBS runtime label to place in generated payloads.",
    )
    return parser.parse_args(argv)


def input_size_class(duration_ms: int, memory_mb: float) -> str:
    if duration_ms <= 50:
        return "tiny"
    if duration_ms <= 200 and memory_mb < 1024:
        return "small"
    if duration_ms <= 400:
        return "medium"
    if duration_ms <= 2000:
        return "large"
    return "xlarge"


def select_sebs_benchmark(profile: ClassifiedProfile, target_duration_ms: int) -> str:
    features = profile.features
    family = profile.family
    if family == "network_service":
        return "120.uploader" if target_duration_ms > 200 else "110.dynamic-html"
    if family == "storage_io":
        return "210.thumbnailer" if features.trigger == "storage" else "311.compression"
    if family == "event_pipeline":
        return "220.video-processing" if target_duration_ms > 400 else "311.compression"
    if family == "cpu_bound":
        if target_duration_ms > 400:
            return "503.graph-bfs"
        if target_duration_ms > 200:
            return "311.compression"
        return "110.dynamic-html"
    return FAMILY_TO_SEBS_CANDIDATES[family][0]


def sebs_profile_hints(profile: ClassifiedProfile, benchmark: str) -> dict[str, Any]:
    features = profile.features
    memory_bytes = int(features.memory_mb * 1024 * 1024)
    if benchmark == "411.image-recognition":
        return {
            "cpu_intensity": 0.55,
            "memory_bytes": max(memory_bytes, 1024 * 1024 * 1024),
            "working_set_bytes": max(512 * 1024 * 1024, memory_bytes // 2),
            "cold_load_penalty_ns": 2_000_000_000,
            "io_weight": 250,
        }
    if benchmark in {"210.thumbnailer", "220.video-processing"}:
        return {
            "cpu_intensity": 0.45,
            "memory_bytes": max(memory_bytes, 256 * 1024 * 1024),
            "working_set_bytes": max(128 * 1024 * 1024, min(memory_bytes, 512 * 1024 * 1024)),
            "io_weight": 750,
            "io_bandwidth_bytes_per_sec": 96 * 1024 * 1024,
            "network_bandwidth_bytes_per_sec": 64 * 1024 * 1024,
            "cold_load_penalty_ns": 1_000_000_000,
            "phase_sequence": [
                {"kind": "IoBound", "duration_pct": 35},
                {"kind": "CpuBound", "duration_pct": 35},
                {"kind": "IoBound", "duration_pct": 30},
            ],
        }
    if benchmark == "120.uploader":
        return {
            "cpu_intensity": 0.25,
            "memory_bytes": max(memory_bytes, 128 * 1024 * 1024),
            "working_set_bytes": max(64 * 1024 * 1024, min(memory_bytes, 256 * 1024 * 1024)),
            "network_bandwidth_bytes_per_sec": 128 * 1024 * 1024,
            "io_weight": 350,
        }
    if benchmark == "311.compression":
        return {
            "cpu_intensity": 0.70,
            "memory_bytes": max(memory_bytes, 128 * 1024 * 1024),
            "working_set_bytes": max(64 * 1024 * 1024, min(memory_bytes, 256 * 1024 * 1024)),
            "io_weight": 450,
        }
    if benchmark == "503.graph-bfs":
        return {
            "cpu_intensity": 0.75,
            "memory_bytes": max(memory_bytes, 192 * 1024 * 1024),
            "working_set_bytes": max(96 * 1024 * 1024, min(memory_bytes, 512 * 1024 * 1024)),
            "io_weight": 250,
        }
    if benchmark == "010.sleep":
        return {"cpu_intensity": 0.05, "io_weight": 100}
    return {"cpu_intensity": 0.80, "io_weight": 250}


def sebs_payload(
    profile: ClassifiedProfile,
    benchmark: str,
    target_duration_ms: int,
    runtime: str,
) -> dict[str, Any]:
    size = input_size_class(target_duration_ms, profile.features.memory_mb)
    return {
        "benchmark": benchmark,
        "benchmark_path": SEBS_BENCHMARKS[benchmark]["path"],
        "runtime": runtime or SEBS_BENCHMARKS[benchmark]["default_runtime"],
        "input_size": size,
        "target_duration_ms": target_duration_ms,
        "memory_mb": int(round(profile.features.memory_mb)),
        "trigger": profile.features.trigger,
        "workload_family": profile.family,
    }


def sebs_invoke_payload(
    benchmark: str,
    input_size: str,
    target_duration_ms: int,
) -> dict[str, Any]:
    if benchmark == "010.sleep":
        return {"sleep": max(1, int(round(target_duration_ms / 1000.0)))}
    if benchmark == "110.dynamic-html":
        sizes = {"tiny": 10, "small": 100, "medium": 1000, "large": 5000, "xlarge": 10000}
        return {"username": "azure-trace", "random_len": sizes[input_size]}
    if benchmark == "503.graph-bfs":
        sizes = {"tiny": 10, "small": 1000, "medium": 10000, "large": 10000, "xlarge": 100000}
        return {"size": sizes[input_size], "seed": 42}
    if benchmark == "120.uploader":
        sizes = {
            "tiny": "test",
            "small": "test",
            "medium": "small",
            "large": "small",
            "xlarge": "large",
        }
        return {"_sebs_input_size": sizes[input_size]}
    if benchmark == "210.thumbnailer":
        return {"_sebs_input_size": "test"}
    if benchmark == "220.video-processing":
        sizes = {
            "tiny": "test",
            "small": "test",
            "medium": "small",
            "large": "small",
            "xlarge": "large",
        }
        return {"_sebs_input_size": sizes[input_size]}
    if benchmark == "311.compression":
        return {"_sebs_input_size": "test"}
    if benchmark == "411.image-recognition":
        return {"_sebs_input_size": "test"}
    raise KeyError(f"unsupported SeBS benchmark: {benchmark}")


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
        "sebs_benchmark",
        "workload",
        "sebs_input_size",
        "target_duration_class",
        "inferred_duration_class",
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


def classified_profile_payload(
    profile: ClassifiedProfile,
    benchmark: str,
    target_duration_ms: int,
    runtime: str,
) -> dict[str, Any]:
    payload = sebs_payload(profile, benchmark, target_duration_ms, runtime)
    return {
        "profile_id": profile.profile_id,
        "workload_family": profile.family,
        "workload": benchmark,
        "kernel": benchmark,
        "sebs_benchmark": benchmark,
        "sebs": payload,
        "classifier_confidence": profile.confidence,
        "classifier_probabilities": profile.probabilities,
        "classifier_reason": profile.reason,
        "source_2019_features": asdict(profile.features),
        "profile_duration_class": duration_class(profile.features.median_duration_ms),
        "duration_class": duration_class(profile.features.median_duration_ms),
        **sebs_profile_hints(profile, benchmark),
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
    benchmark_counts: Counter[str] = Counter()

    for invocation_id, event in enumerate(
        sorted(selected_events, key=lambda item: (item.source_start_ms, item.function_id)),
        start=1,
    ):
        inferred = select_profile_for_function(
            classified_profiles, event.function_id, event.duration_ms
        )
        benchmark = select_sebs_benchmark(inferred, event.duration_ms)
        size = input_size_class(event.duration_ms, inferred.features.memory_mb)
        profile_id = f"{inferred.profile_id}_as_{benchmark}"
        if profile_id not in profile_lookup:
            profile_lookup[profile_id] = classified_profile_payload(
                inferred, benchmark, event.duration_ms, args.runtime
            )
        function_profiles.setdefault(event.function_id, profile_id)
        family_counts[inferred.family] += 1
        benchmark_counts[benchmark] += 1

        at_ms = (event.source_start_ms - base_ms) / args.scale
        row = {
            "event_id": f"az2021s-{invocation_id:08d}",
            "invocation_id": invocation_id,
            "at_ms": round(at_ms, 3),
            "source_start_time_ms": round(event.source_start_ms, 3),
            "hash_owner": inferred.features.owner,
            "app": event.app,
            "func": event.func,
            "function_id": event.function_id,
            "app_group": f"{inferred.features.owner}:{inferred.features.app}"
            if inferred.features.owner
            else inferred.features.app,
            "app_function_count": inferred.features.app_function_count,
            "profile_id": profile_id,
            "workload_family": inferred.family,
            "sebs_benchmark": benchmark,
            "workload": benchmark,
            "sebs_input_size": size,
            "target_duration_class": replay_common.duration_class(event.duration_ms),
            "inferred_duration_class": duration_class(inferred.features.median_duration_ms),
            "target_duration_ms": event.duration_ms,
            "duration_p25_ms": inferred.features.duration_p25_ms,
            "duration_p50_ms": inferred.features.median_duration_ms,
            "duration_p75_ms": inferred.features.duration_p75_ms,
            "duration_p99_ms": inferred.features.duration_p99_ms,
            "duration_max_ms": inferred.features.duration_max_ms,
            "memory_p50_mb": inferred.features.memory_mb,
            "memory_p75_mb": inferred.features.memory_p75_mb,
            "memory_p95_mb": inferred.features.memory_p95_mb,
            "memory_p99_mb": inferred.features.memory_p99_mb,
            "deadline_us": replay_common.deadline_us(event.duration_ms, args.min_slack_ms),
            "slo_class": replay_common.slo_for_duration(event.duration_ms),
            "classifier_confidence": round(inferred.confidence, 6),
        }
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
                "profile_hints": hints,
                "sebs": sebs_payload(inferred, benchmark, event.duration_ms, args.runtime),
                "sebs_payload": sebs_invoke_payload(benchmark, size, event.duration_ms),
            }
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_invocations_csv(args.output_dir / "invocations.csv", invocation_rows)
    created_at = datetime.now(timezone.utc).isoformat()

    profiles_payload = {
        "version": 1,
        "schema": "cosmos.azure.2019-classified-sebs-profiles",
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
        "family_to_sebs_candidates": FAMILY_TO_SEBS_CANDIDATES,
        "sebs_benchmarks": SEBS_BENCHMARKS,
        "profiles": profile_lookup,
        "function_profiles": function_profiles,
    }
    (args.output_dir / "profiles.json").write_text(
        json.dumps(profiles_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    replay_payload = {
        "version": 1,
        "schema": "cosmos.azure.2019-classified-sebs-replay",
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
            "sebs_benchmark_counts": dict(sorted(benchmark_counts.items())),
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
                "Workload families are mapped to SeBS benchmark IDs by this script.",
                "For storage-backed SeBS benchmarks, replay sebs_payload is a placeholder until prepared SeBS storage input is attached.",
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
