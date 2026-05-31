#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent

if str(SCRIPT_DIR) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(SCRIPT_DIR))

from cpu_trace_common import deadline_us_for_duration, slo_class_for_duration


DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "results" / "azure_2019_cpu_stream"
ARRIVAL_MODES = (
    "uniform-within-minute",
    "front-loaded-burst",
    "evenly-spaced",
    "clustered-bursty",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a synthetic CPU-only workload stream from an Azure 2019 CPU "
            "distribution JSON."
        )
    )
    parser.add_argument("--distribution-json", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--window-minutes",
        type=int,
        help="Number of minute buckets to emit; defaults to the source trace length",
    )
    parser.add_argument(
        "--start-minute",
        type=int,
        default=0,
        help="Start index into arrival_distribution.per_minute_counts",
    )
    parser.add_argument(
        "--count-scale",
        type=float,
        default=1.0,
        help="Multiply per-minute invocation counts by this factor",
    )
    parser.add_argument(
        "--time-scale",
        type=float,
        default=1.0,
        help="Compress or expand inter-arrival time by dividing minute offsets by this factor",
    )
    parser.add_argument("--limit", type=int, help="Hard cap on generated invocations")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--arrival-mode", choices=ARRIVAL_MODES, default="clustered-bursty")
    return parser.parse_args(argv)


def validate_distribution(payload: dict[str, Any]) -> None:
    if payload.get("schema") != "cosmos.azure.2019.cpu-distribution":
        raise SystemExit(
            f"unsupported distribution schema: {payload.get('schema')!r}"
        )
    if not isinstance(payload.get("profiles"), list) or not payload["profiles"]:
        raise SystemExit("distribution JSON does not contain any profiles")
    arrival = payload.get("arrival_distribution") or {}
    if not isinstance(arrival.get("per_minute_counts"), list) or not arrival["per_minute_counts"]:
        raise SystemExit("distribution JSON does not contain arrival_distribution.per_minute_counts")


def scaled_counts(counts: list[int], scale: float) -> list[int]:
    if scale <= 0:
        raise SystemExit("--count-scale must be greater than zero")
    result: list[int] = []
    carry = 0.0
    for count in counts:
        scaled = count * scale + carry
        rounded = int(scaled)
        carry = scaled - rounded
        result.append(max(0, rounded))
    return result


def subminute_offset_ms(
    rng: random.Random,
    mode: str,
    count: int,
    index: int,
) -> float:
    count = max(1, count)
    if mode == "evenly-spaced":
        return ((index + 0.5) * 60_000.0) / count
    if mode == "front-loaded-burst":
        span_ms = min(10_000.0, 60_000.0)
        jitter = (rng.random() - 0.5) * min(500.0, span_ms / count)
        return max(0.0, min(59_999.999, ((index + 0.5) * span_ms) / count + jitter))
    if mode == "clustered-bursty":
        clusters = 1 + rng.randrange(4)
        cluster = index % clusters
        center = ((cluster + 0.5) * 60_000.0) / clusters
        width = min(5_000.0, 30_000.0 / clusters)
        jitter = (rng.random() - 0.5) * width
        return max(0.0, min(59_999.999, center + jitter))
    return rng.random() * 60_000.0


def choose_profile(rng: random.Random, profiles: list[dict[str, Any]]) -> dict[str, Any]:
    draw = rng.random()
    running = 0.0
    for profile in profiles:
        running += float(profile["sampling_weight"])
        if draw <= running:
            return profile
    return profiles[-1]


def interpolate_quantiles(rng: random.Random, cpu_time_ms: dict[str, Any]) -> int:
    points = [
        (0.00, max(1.0, float(cpu_time_ms.get("min", 1.0)))),
        (0.25, max(1.0, float(cpu_time_ms.get("p25", 1.0)))),
        (0.50, max(1.0, float(cpu_time_ms.get("p50", 1.0)))),
        (0.75, max(1.0, float(cpu_time_ms.get("p75", 1.0)))),
        (0.99, max(1.0, float(cpu_time_ms.get("p99", 1.0)))),
        (1.00, max(1.0, float(cpu_time_ms.get("max", 1.0)))),
    ]
    draw = rng.random()
    for left, right in zip(points, points[1:]):
        (left_p, left_v), (right_p, right_v) = left, right
        if draw <= right_p:
            width = max(right_p - left_p, 1e-9)
            position = (draw - left_p) / width
            value = left_v + (right_v - left_v) * position
            return max(1, int(round(value)))
    return max(1, int(round(points[-1][1])))


def write_invocations_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "event_id",
        "invocation_id",
        "at_ms",
        "minute_index",
        "source_minute_index",
        "profile_id",
        "trigger",
        "cpu_time_bucket",
        "workload",
        "duration_ms",
        "target_duration_ms",
        "expected_duration_ms",
        "deadline_us",
        "slo_class",
        "deadline_source",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(
            {
                key: value
                for key, value in row.items()
                if key in fieldnames
            }
            for row in rows
        )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.time_scale <= 0:
        raise SystemExit("--time-scale must be greater than zero")
    if args.window_minutes is not None and args.window_minutes <= 0:
        raise SystemExit("--window-minutes must be positive")
    if args.limit is not None and args.limit <= 0:
        raise SystemExit("--limit must be positive")

    payload = json.loads(args.distribution_json.read_text(encoding="utf-8"))
    validate_distribution(payload)

    profiles = sorted(
        payload["profiles"],
        key=lambda item: (-float(item["sampling_weight"]), item["profile_id"]),
    )
    arrival_counts = list(payload["arrival_distribution"]["per_minute_counts"])
    total_source_minutes = len(arrival_counts)
    window_minutes = args.window_minutes or total_source_minutes
    selected_counts = [
        arrival_counts[(args.start_minute + minute) % total_source_minutes]
        for minute in range(window_minutes)
    ]
    generated_counts = scaled_counts(selected_counts, args.count_scale)

    rng = random.Random(args.seed)
    invocations: list[dict[str, Any]] = []
    for minute_index, count in enumerate(generated_counts):
        if count <= 0:
            continue
        for offset_index in range(count):
            if args.limit is not None and len(invocations) >= args.limit:
                break
            profile = choose_profile(rng, profiles)
            target_duration_ms = interpolate_quantiles(rng, profile["cpu_time_ms"])
            expected_duration_ms = max(1, int(round(float(profile["expected_cpu_time_ms"]))))
            at_ms = (
                minute_index * 60_000.0
                + subminute_offset_ms(rng, args.arrival_mode, count, offset_index)
            ) / args.time_scale
            deadline_duration_ms = max(target_duration_ms, expected_duration_ms)
            invocation_id = len(invocations) + 1
            invocations.append(
                {
                    "event_id": f"az2019cpu-{invocation_id:08d}",
                    "invocation_id": invocation_id,
                    "at_ms": round(at_ms, 3),
                    "minute_index": minute_index,
                    "source_minute_index": (args.start_minute + minute_index) % total_source_minutes,
                    "profile_id": profile["profile_id"],
                    "trigger": profile["trigger"],
                    "cpu_time_bucket": profile["cpu_time_bucket"],
                    "workload": "cpu_burst",
                    "duration_ms": target_duration_ms,
                    "target_duration_ms": target_duration_ms,
                    "expected_duration_ms": expected_duration_ms,
                    "deadline_us": deadline_us_for_duration(deadline_duration_ms),
                    "slo_class": slo_class_for_duration(deadline_duration_ms),
                    "profile_hints": profile.get("profile_hints", {"cpu_intensity": 0.95}),
                    "deadline_source": "max(expected_duration_ms,target_duration_ms) with default headroom",
                }
            )
        if args.limit is not None and len(invocations) >= args.limit:
            break

    created_at = datetime.now(timezone.utc).isoformat()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_invocations_csv(args.output_dir / "invocations.csv", invocations)

    replay_payload = {
        "version": 1,
        "schema": "cosmos.azure.2019.cpu-stream",
        "created_at": created_at,
        "source": {
            "distribution_json": str(args.distribution_json),
            "distribution_schema": payload["schema"],
        },
        "window": {
            "start_minute": args.start_minute,
            "window_minutes": window_minutes,
            "count_scale": args.count_scale,
            "time_scale": args.time_scale,
            "limit": args.limit,
            "arrival_mode": args.arrival_mode,
            "seed": args.seed,
        },
        "summary": {
            "source_minutes": total_source_minutes,
            "selected_minutes": window_minutes,
            "selected_invocations": sum(selected_counts),
            "generated_invocations": len(invocations),
            "generated_window_ms": (window_minutes * 60_000.0) / args.time_scale,
        },
        "profiles_path": None,
        "invocations_path": "invocations.csv",
        "invocations": invocations,
    }
    (args.output_dir / "replay.json").write_text(
        json.dumps(replay_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    print(args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
