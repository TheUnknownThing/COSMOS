#!/usr/bin/env python3
"""Replay Azure top functions as a CPU-only open-loop benchmark.

Generates invocations in real time (open-loop), measures steady-state goodput
under a chosen scheduler, and runs either a single offered-rate experiment or a
linear offered-load sweep.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import signal
import statistics
import subprocess
import sys
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import harness
import run_cosmos

SCRIPT_DIR = Path(__file__).resolve().parent
RESULTS_ROOT = SCRIPT_DIR / "results"


@dataclass(frozen=True)
class FunctionProfile:
    function_id: str
    frequency: float
    expected_time_ms: int  # p75, told to the scheduler
    p99_time_ms: int
    mean_time_ms: float
    time_distribution: dict[str, float]


@dataclass(frozen=True)
class ScheduledInvocation:
    invocation_id: int
    release_offset_s: float
    profile: FunctionProfile
    actual_duration_ms: int
    deadline_us: int
    generated_at_ns: int = 0


@dataclass(frozen=True)
class PoolInvocation:
    function_id: str
    expected_time_ms: int
    actual_duration_ms: int
    p99_time_ms: int
    deadline_us: int


@dataclass(frozen=True)
class InvocationPool:
    invocations: list[PoolInvocation]
    actual_mean_time_ms: float | None
    weighted_mean_time_ms: float
    load_mean_time_ms: float
    load_mean_source: str
    cpu_cores: int


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay Azure top functions as a CPU-only open-loop benchmark."
    )
    parser.add_argument("--config-json", type=Path, required=True)
    parser.add_argument(
        "--config",
        default="cosmos-full",
        choices=[
            "cfs-default",
            "cosmos-heuristic",
            "cosmos-metadata",
            "cosmos-pooled",
            "cosmos-full",
            "sfs",
        ],
    )
    parser.add_argument("--run-duration-s", type=float, default=300.0)
    parser.add_argument(
        "--warmup-duration-s",
        type=float,
        default=60.0,
        help="Initial seconds to exclude from steady-state metrics.",
    )
    parser.add_argument(
        "--arrival-mode",
        choices=["poisson", "evenly-spaced"],
        default="poisson",
    )
    parser.add_argument(
        "--load-min",
        type=float,
        default=0.5,
        help="Minimum offered load factor for the linear sweep.",
    )
    parser.add_argument(
        "--load-max",
        type=float,
        default=1.0,
        help="Maximum offered load factor for the linear sweep.",
    )
    parser.add_argument(
        "--load-steps",
        type=int,
        default=6,
        help="Number of evenly spaced offered load points to run.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Run one explicit offered rate in invocations/s instead of a load sweep.",
    )
    parser.add_argument(
        "--min-slack-us", type=int, default=harness.DEFAULT_SLO_MIN_SLACK_US
    )
    parser.add_argument(
        "--deadline-safety-factor",
        type=float,
        default=1.2,
        help="Multiplier on p99 before adding slack.",
    )
    parser.add_argument(
        "--deadline-floor-ms",
        type=float,
        default=100.0,
        help="Minimum replay deadline in ms; protects against launcher floor.",
    )
    parser.add_argument(
        "--slo-miss-threshold",
        type=float,
        default=0.05,
        help="Maximum tolerated steady-state SLO miss rate.",
    )
    parser.add_argument(
        "--tail-max-multiplier",
        type=float,
        default=10.0,
        help="(kept for compatibility; tail is now clamped to p99)",
    )
    parser.add_argument(
        "--duration-cap-ms",
        type=int,
        default=10_000,
        help="Clamp generated invocation durations and p99/deadlines; use 0 to disable.",
    )
    parser.add_argument(
        "--worker-safety-factor",
        type=float,
        default=2.0,
    )
    parser.add_argument(
        "--scheduler-settle-s",
        type=float,
        default=1.0,
    )
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--max-launch-workers", type=int, default=1024)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument(
        "--generate-pool-json",
        type=Path,
        default=None,
        help="Generate a reusable invocation pool JSON and exit.",
    )
    parser.add_argument(
        "--pool-json",
        type=Path,
        default=None,
        help="Replay invocations from a generated pool JSON.",
    )
    parser.add_argument(
        "--pool-size",
        type=int,
        default=100_000,
        help="Number of invocations to write with --generate-pool-json.",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=1,
        help="Repeat each offered-load point and aggregate metrics by median.",
    )
    parser.add_argument(
        "--scheduler-bin",
        type=Path,
        default=harness.REPO_ROOT / "target" / "release" / "cosmos",
    )
    parser.add_argument(
        "--stats-socket", type=Path, default=harness.DEFAULT_STATS_SOCKET
    )
    parser.add_argument(
        "--event-bridge-port", type=int, default=harness.DEFAULT_EVENT_BRIDGE_PORT
    )
    parser.add_argument("--scheduler-flag", action="append", default=[])
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Profile loading & workload helpers
# ---------------------------------------------------------------------------


def expected_time_ms(distribution: dict[str, float]) -> float:
    """Expected runtime from the sampled distribution used by the generator."""
    segments = [
        (distribution["min"], distribution["p25"]),
        (distribution["p25"], distribution["p50"]),
        (distribution["p50"], distribution["p75"]),
        (distribution["p75"], distribution["p99"]),
        (distribution["p99"], distribution["p99"]),
    ]
    weights = [0.25, 0.25, 0.25, 0.24, 0.01]
    return sum(
        ((low + high) / 2.0) * weight
        for (low, high), weight in zip(segments, weights)
    )


def weighted_mean_time_ms(profiles: list[FunctionProfile]) -> float:
    return sum(p.frequency * p.mean_time_ms for p in profiles)


def load_factor(
    rate_inv_per_sec: float, weighted_mean_ms: float, cpu_cores: int
) -> float:
    if weighted_mean_ms <= 0.0 or cpu_cores <= 0:
        return 0.0
    return rate_inv_per_sec * (weighted_mean_ms / 1000.0) / cpu_cores


def rate_from_load(load: float, weighted_mean_ms: float, cpu_cores: int) -> float:
    if weighted_mean_ms <= 0.0 or cpu_cores <= 0:
        raise SystemExit("Weighted mean execution time and CPU count must be positive")
    return load * cpu_cores * 1000.0 / weighted_mean_ms


def median_float(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(statistics.median(values))


def load_profiles(config_json: Path) -> list[FunctionProfile]:
    payload = json.loads(config_json.read_text(encoding="utf-8"))
    if payload.get("schema") != "cosmos.azure.top-functions-config":
        raise SystemExit(f"Unsupported schema: {payload.get('schema')}")

    functions = payload.get("functions", [])
    if not functions:
        raise SystemExit("Config contains no functions")

    profiles: list[FunctionProfile] = []
    total_weight = 0.0
    for item in functions:
        dist = {
            k: float(item["time_distribution"][k])
            for k in ("min", "p25", "p50", "p75", "p99", "max")
        }
        freq = float(item["frequency"])
        total_weight += freq
        profiles.append(
            FunctionProfile(
                function_id=str(item["function_id"]),
                frequency=freq,
                expected_time_ms=max(1, int(round(float(dist["p75"])))),
                p99_time_ms=max(1, int(round(float(dist["p99"])))),
                mean_time_ms=expected_time_ms(dist),
                time_distribution=dist,
            )
        )
    if total_weight <= 0.0:
        raise SystemExit("Function frequencies must sum to a positive value")

    # Normalise frequencies
    return [
        FunctionProfile(
            function_id=p.function_id,
            frequency=p.frequency / total_weight,
            expected_time_ms=p.expected_time_ms,
            p99_time_ms=p.p99_time_ms,
            mean_time_ms=p.mean_time_ms,
            time_distribution=p.time_distribution,
        )
        for p in profiles
    ]


def cap_profiles(
    profiles: list[FunctionProfile], duration_cap_ms: int | None
) -> list[FunctionProfile]:
    if duration_cap_ms is None:
        return profiles
    if duration_cap_ms < 1:
        raise SystemExit("--duration-cap-ms must be positive, or 0 to disable")

    cap = float(duration_cap_ms)
    capped: list[FunctionProfile] = []
    for profile in profiles:
        dist = {
            key: min(float(value), cap)
            for key, value in profile.time_distribution.items()
        }
        capped.append(
            FunctionProfile(
                function_id=profile.function_id,
                frequency=profile.frequency,
                expected_time_ms=max(1, int(round(dist["p75"]))),
                p99_time_ms=max(1, int(round(dist["p99"]))),
                mean_time_ms=expected_time_ms(dist),
                time_distribution=dist,
            )
        )
    return capped


def piecewise_sample_duration_ms(
    distribution: dict[str, float],
    rng: random.Random,
    tail_max_multiplier: float = 10.0,  # ignored, kept for compatibility
) -> int:
    """Sample actual duration. The tail segment (p99–max) always returns p99."""
    segments = [
        ("min", "p25", 0.25),
        ("p25", "p50", 0.25),
        ("p50", "p75", 0.25),
        ("p75", "p99", 0.24),
        ("p99", "max", 0.01),
    ]
    draw = rng.random()
    cumulative = 0.0
    selected = segments[-1]
    for seg in segments:
        cumulative += seg[2]
        if draw <= cumulative:
            selected = seg
            break

    low = float(distribution[selected[0]])
    high = float(distribution[selected[1]])

    # Tail clamped to p99 – guarantees actual ≤ p99 ≤ deadline
    if selected[0] == "p99":
        return max(1, int(round(float(distribution["p99"]))))

    sampled = low if math.isclose(low, high) else rng.uniform(low, high)
    return max(1, int(round(sampled)))


def deadline_us_from_p99(
    p99_ms: int,
    min_slack_us: int,
    *,
    safety_factor: float = 1.0,
    deadline_floor_ms: float = 0.0,
) -> int:
    base_ms = float(p99_ms) * max(0.001, safety_factor)
    p99_us = int(round(max(1.0, base_ms) * 1_000))
    deadline_us = p99_us + max(p99_us, min_slack_us)
    floor_us = int(round(max(0.0, deadline_floor_ms) * 1_000))
    return max(deadline_us, floor_us)


def slo_class_from_duration_ms(duration_ms: int) -> int:
    if duration_ms <= 250:
        return 0
    if duration_ms <= 1000:
        return 1
    return 2


def weighted_expected_p50_ms(profiles: list[FunctionProfile]) -> int:
    """Used for global SLO target configuration."""
    ordered = sorted(profiles, key=lambda p: p.expected_time_ms)
    running = 0.0
    for p in ordered:
        running += p.frequency
        if running >= 0.5:
            return p.expected_time_ms
    return ordered[-1].expected_time_ms


def estimate_worker_requirement(
    profiles: list[FunctionProfile],
    *,
    rate: float,
    min_slack_us: int,
    deadline_safety_factor: float,
    deadline_floor_ms: float,
    worker_safety_factor: float,
    max_launch_workers: int,
) -> int:
    if not profiles or rate <= 0.0:
        return 1
    max_dl = max(
        deadline_us_from_p99(
            p.p99_time_ms,
            min_slack_us,
            safety_factor=deadline_safety_factor,
            deadline_floor_ms=deadline_floor_ms,
        )
        for p in profiles
    )
    estimated = (
        int(math.ceil(rate * (max_dl / 1_000_000.0) * max(1.0, worker_safety_factor)))
        + 16
    )
    estimated = max(32, estimated)
    return max(1, min(max_launch_workers, estimated))


# ---------------------------------------------------------------------------
# Real‑time invocation generation
# ---------------------------------------------------------------------------


def build_invocation(
    profiles: list[FunctionProfile],
    weights: list[float],
    *,
    invocation_id: int,
    release_offset_s: float,
    generated_at_ns: int,
    min_slack_us: int,
    deadline_safety_factor: float,
    deadline_floor_ms: float,
    tail_max_multiplier: float,
    rng: random.Random,
) -> ScheduledInvocation:
    profile = rng.choices(profiles, weights=weights, k=1)[0]
    actual_duration_ms = piecewise_sample_duration_ms(
        profile.time_distribution,
        rng,
        tail_max_multiplier=tail_max_multiplier,
    )
    deadline_us = deadline_us_from_p99(
        profile.p99_time_ms,
        min_slack_us,
        safety_factor=deadline_safety_factor,
        deadline_floor_ms=deadline_floor_ms,
    )
    return ScheduledInvocation(
        invocation_id=invocation_id,
        release_offset_s=release_offset_s,
        profile=profile,
        actual_duration_ms=actual_duration_ms,
        deadline_us=deadline_us,
        generated_at_ns=generated_at_ns,
    )


def actual_mean_time_ms_from_pool(invocations: list[PoolInvocation]) -> float:
    if not invocations:
        return 0.0
    return sum(item.actual_duration_ms for item in invocations) / len(invocations)


def generate_invocation_pool(
    profiles: list[FunctionProfile],
    *,
    count: int,
    seed: int,
    min_slack_us: int,
    deadline_safety_factor: float,
    deadline_floor_ms: float,
    tail_max_multiplier: float,
) -> list[PoolInvocation]:
    if count < 1:
        raise SystemExit("--pool-size must be at least 1")

    rng = random.Random(seed)
    weights = [p.frequency for p in profiles]
    invocations: list[PoolInvocation] = []
    for invocation_id in range(1, count + 1):
        item = build_invocation(
            profiles,
            weights,
            invocation_id=invocation_id,
            release_offset_s=0.0,
            generated_at_ns=0,
            min_slack_us=min_slack_us,
            deadline_safety_factor=deadline_safety_factor,
            deadline_floor_ms=deadline_floor_ms,
            tail_max_multiplier=tail_max_multiplier,
            rng=rng,
        )
        invocations.append(
            PoolInvocation(
                function_id=item.profile.function_id,
                expected_time_ms=item.profile.expected_time_ms,
                actual_duration_ms=item.actual_duration_ms,
                p99_time_ms=item.profile.p99_time_ms,
                deadline_us=item.deadline_us,
            )
        )
    return invocations


def write_pool_json(
    pool_json: Path,
    *,
    config_json: Path,
    profiles: list[FunctionProfile],
    invocations: list[PoolInvocation],
    seed: int,
    duration_cap_ms: int | None = None,
) -> None:
    weighted_mean_ms = weighted_mean_time_ms(profiles)
    actual_mean_ms = actual_mean_time_ms_from_pool(invocations)
    payload = {
        "schema": "cosmos.azure.top-functions-pool",
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "config_json": str(config_json),
        "seed": seed,
        "pool_size": len(invocations),
        "cpu_cores": os.cpu_count() or 1,
        "duration_cap_ms": duration_cap_ms,
        "weighted_mean_time_ms": weighted_mean_ms,
        "weighted_mean_duration_ms": weighted_mean_ms,
        "actual_mean_time_ms": actual_mean_ms,
        "profiles": [
            {
                "function_id": p.function_id,
                "frequency": p.frequency,
                "expected_time_ms": p.expected_time_ms,
                "p99_time_ms": p.p99_time_ms,
                "mean_time_ms": p.mean_time_ms,
                "time_distribution": p.time_distribution,
            }
            for p in profiles
        ],
        "invocations": [
            {
                "invocation_id": idx,
                "function_id": item.function_id,
                "expected_time_ms": item.expected_time_ms,
                "actual_duration_ms": item.actual_duration_ms,
                "p99_time_ms": item.p99_time_ms,
                "deadline_us": item.deadline_us,
            }
            for idx, item in enumerate(invocations, start=1)
        ],
    }
    pool_json.parent.mkdir(parents=True, exist_ok=True)
    pool_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def load_profiles_from_pool_json(pool_json: Path) -> list[FunctionProfile]:
    payload = json.loads(pool_json.read_text(encoding="utf-8"))
    raw_profiles = payload.get("profiles") or payload.get("profiles_summary")
    if raw_profiles:
        profiles: list[FunctionProfile] = []
        total_weight = 0.0
        for raw in raw_profiles:
            dist = {
                k: float(raw["time_distribution"][k])
                for k in ("min", "p25", "p50", "p75", "p99", "max")
            }
            freq = float(raw["frequency"])
            total_weight += freq
            profiles.append(
                FunctionProfile(
                    function_id=str(raw["function_id"]),
                    frequency=freq,
                    expected_time_ms=max(
                        1, int(round(float(raw.get("expected_time_ms", dist["p75"]))))
                    ),
                    p99_time_ms=max(
                        1, int(round(float(raw.get("p99_time_ms", dist["p99"]))))
                    ),
                    mean_time_ms=float(raw.get("mean_time_ms", expected_time_ms(dist))),
                    time_distribution=dist,
                )
            )
        if total_weight <= 0.0:
            raise SystemExit(f"Pool profiles have non-positive frequency sum: {pool_json}")
        return [
            FunctionProfile(
                function_id=p.function_id,
                frequency=p.frequency / total_weight,
                expected_time_ms=p.expected_time_ms,
                p99_time_ms=p.p99_time_ms,
                mean_time_ms=p.mean_time_ms,
                time_distribution=p.time_distribution,
            )
            for p in profiles
        ]

    config_json_raw = payload.get("config_json")
    if config_json_raw:
        config_json = Path(str(config_json_raw))
        candidates = [config_json]
        if not config_json.is_absolute():
            candidates.append(pool_json.parent / config_json)
        for candidate in candidates:
            if candidate.exists():
                return load_profiles(candidate)
    raise SystemExit(
        f"{pool_json} does not contain profiles metadata; pass through a regenerated pool"
    )


def _warn_pool_mean_fallback(pool_json: Path) -> None:
    print(
        f"WARNING: {pool_json} missing actual_mean_time_ms; "
        "falling back to theoretical weighted_mean_time_ms for load normalization.",
        file=sys.stderr,
    )


def load_invocation_pool(
    pool_json: Path,
    profiles: list[FunctionProfile],
    *,
    weighted_mean_ms: float,
    min_slack_us: int,
    deadline_safety_factor: float,
    deadline_floor_ms: float,
) -> InvocationPool:
    payload = json.loads(pool_json.read_text(encoding="utf-8"))
    raw_invocations = payload.get("invocations", [])
    if not raw_invocations:
        raise SystemExit(f"Pool contains no invocations: {pool_json}")

    profiles_by_id = {p.function_id: p for p in profiles}
    invocations: list[PoolInvocation] = []
    for raw in raw_invocations:
        function_id = str(raw["function_id"])
        profile = profiles_by_id.get(function_id)
        if profile is None:
            raise SystemExit(
                f"Pool invocation references unknown function_id {function_id!r}"
            )

        p99_time_ms = int(raw.get("p99_time_ms", profile.p99_time_ms))
        invocations.append(
            PoolInvocation(
                function_id=function_id,
                expected_time_ms=int(
                    raw.get("expected_time_ms", profile.expected_time_ms)
                ),
                actual_duration_ms=int(
                    raw.get("actual_duration_ms", raw.get("duration_ms"))
                ),
                p99_time_ms=p99_time_ms,
                deadline_us=int(
                    raw.get(
                        "deadline_us",
                        deadline_us_from_p99(
                            p99_time_ms,
                            min_slack_us,
                            safety_factor=deadline_safety_factor,
                            deadline_floor_ms=deadline_floor_ms,
                        ),
                    )
                ),
            )
        )

    actual_mean_raw = payload.get("actual_mean_time_ms")
    actual_mean_ms = None
    if actual_mean_raw is not None:
        actual_mean_ms = float(actual_mean_raw)

    if actual_mean_ms is not None and actual_mean_ms > 0.0:
        load_mean_ms = actual_mean_ms
        load_mean_source = "actual_mean_time_ms"
    else:
        _warn_pool_mean_fallback(pool_json)
        load_mean_ms = weighted_mean_ms
        load_mean_source = "weighted_mean_time_ms"

    return InvocationPool(
        invocations=invocations,
        actual_mean_time_ms=actual_mean_ms,
        weighted_mean_time_ms=float(payload.get("weighted_mean_time_ms", weighted_mean_ms)),
        load_mean_time_ms=load_mean_ms,
        load_mean_source=load_mean_source,
        cpu_cores=max(1, int(payload.get("cpu_cores") or (os.cpu_count() or 1))),
    )


def invocation_from_pool_item(
    pool_item: PoolInvocation,
    profiles_by_id: dict[str, FunctionProfile],
    *,
    invocation_id: int,
    release_offset_s: float,
    generated_at_ns: int,
) -> ScheduledInvocation:
    profile = profiles_by_id[pool_item.function_id]
    return ScheduledInvocation(
        invocation_id=invocation_id,
        release_offset_s=release_offset_s,
        profile=profile,
        actual_duration_ms=pool_item.actual_duration_ms,
        deadline_us=pool_item.deadline_us,
        generated_at_ns=generated_at_ns,
    )


def invocation_generator(
    profiles: list[FunctionProfile],
    *,
    rate: float,
    duration_s: float,
    arrival_mode: str,
    min_slack_us: int,
    deadline_safety_factor: float,
    deadline_floor_ms: float,
    tail_max_multiplier: float,
    rng: random.Random,
    run_start_ns: int,
) -> Iterator[ScheduledInvocation]:
    if rate <= 0.0 or duration_s <= 0.0:
        return

    weights = [p.frequency for p in profiles]
    end_s = time.monotonic() + duration_s
    next_arrival_s = time.monotonic()
    invocation_id = 1

    while True:
        if arrival_mode == "poisson":
            next_arrival_s += rng.expovariate(rate)
        else:
            next_arrival_s += 1.0 / rate
        if next_arrival_s > end_s:
            break

        # Busy‑wait until arrival time
        while True:
            remaining = next_arrival_s - time.monotonic()
            if remaining <= 0.0:
                break
            time.sleep(min(remaining, 0.01))

        generated_ns = time.monotonic_ns()
        yield build_invocation(
            profiles,
            weights,
            invocation_id=invocation_id,
            release_offset_s=(generated_ns - run_start_ns) / 1_000_000_000.0,
            generated_at_ns=generated_ns,
            min_slack_us=min_slack_us,
            deadline_safety_factor=deadline_safety_factor,
            deadline_floor_ms=deadline_floor_ms,
            tail_max_multiplier=tail_max_multiplier,
            rng=rng,
        )
        invocation_id += 1


def pool_invocation_generator(
    pool: InvocationPool,
    profiles: list[FunctionProfile],
    *,
    rate: float,
    duration_s: float,
    arrival_mode: str,
    rng: random.Random,
    run_start_ns: int,
) -> Iterator[ScheduledInvocation]:
    if rate <= 0.0 or duration_s <= 0.0:
        return
    if not pool.invocations:
        return

    profiles_by_id = {p.function_id: p for p in profiles}
    end_s = time.monotonic() + duration_s
    next_arrival_s = time.monotonic()
    invocation_id = 1

    while True:
        if arrival_mode == "poisson":
            next_arrival_s += rng.expovariate(rate)
        else:
            next_arrival_s += 1.0 / rate
        if next_arrival_s > end_s:
            break

        while True:
            remaining = next_arrival_s - time.monotonic()
            if remaining <= 0.0:
                break
            time.sleep(min(remaining, 0.01))

        generated_ns = time.monotonic_ns()
        pool_item = pool.invocations[(invocation_id - 1) % len(pool.invocations)]
        yield invocation_from_pool_item(
            pool_item,
            profiles_by_id,
            invocation_id=invocation_id,
            release_offset_s=(generated_ns - run_start_ns) / 1_000_000_000.0,
            generated_at_ns=generated_ns,
        )
        invocation_id += 1


# ---------------------------------------------------------------------------
# Harness integration
# ---------------------------------------------------------------------------


def invocation_spec_from_schedule(
    config: str,
    item: ScheduledInvocation,
    release_monotonic_ns: int,
) -> harness.InvocationSpec:
    exp = item.profile.expected_time_ms
    return harness.InvocationSpec(
        invocation_id=item.invocation_id,
        workload="cpu_burst",
        actual_duration_ms=item.actual_duration_ms,
        deadline_us=item.deadline_us,
        config=config,
        expected_duration_ms=exp,
        action_name=item.profile.function_id,
        slo_class=slo_class_from_duration_ms(item.profile.p99_time_ms),
        record_fields={
            "function_id": item.profile.function_id,
            "p99_time_ms": item.profile.p99_time_ms,
            "scheduled_offset_ms": item.release_offset_s * 1000.0,
            "generated_at_ns": item.generated_at_ns,
            "scheduled_monotonic_ns": release_monotonic_ns,
            "estimation_error_ms": item.actual_duration_ms - exp,
        },
    )


def run_invocation_task(
    run_dir: Path,
    config: str,
    item: ScheduledInvocation,
    use_metadata: bool,
    metadata_bridge_port: int | None,
) -> int:
    submitted_ns = time.monotonic_ns()
    spec = invocation_spec_from_schedule(config, item, submitted_ns)
    return harness.run_invocation_spec(
        run_dir / "invocations" / f"{item.invocation_id}.json",
        spec,
        use_metadata,
        metadata_bridge_port,
    )


# ---------------------------------------------------------------------------
# Scheduler stack management
# ---------------------------------------------------------------------------


def start_scheduler_stack(
    *,
    config: str,
    scheduler_bin: Path,
    stats_socket: Path,
    event_bridge_port: int,
    scheduler_flags: list[str],
    use_metadata: bool,
    run_dir: Path,
) -> tuple[
    subprocess.Popen[bytes] | None,
    subprocess.Popen[bytes] | None,
    subprocess.Popen[bytes] | None,
]:
    if config == "cfs-default":
        (run_dir / "scheduler_stats.jsonl").write_text("", encoding="utf-8")
        return None, None, None

    harness.ensure_release_build()
    harness.remove_stale_unix_socket(stats_socket)
    scheduler_log = run_dir / "scheduler.log"
    with scheduler_log.open("w", encoding="utf-8") as f:
        scheduler = subprocess.Popen(
            [str(scheduler_bin), *scheduler_flags],
            stdout=f,
            stderr=subprocess.STDOUT,
            cwd=harness.REPO_ROOT,
        )
    harness.wait_for_scheduler_stats(stats_socket, scheduler, scheduler_log)

    event_bridge = None
    if use_metadata:
        eb_log = run_dir / "event_bridge.log"
        event_bridge = harness.start_event_bridge(eb_log, event_bridge_port)
        harness.wait_for_event_bridge(event_bridge_port, event_bridge, eb_log)

    stats_path = run_dir / "scheduler_stats.jsonl"
    stats_capture = harness.start_scheduler_stats_capture(stats_path, stats_socket)
    harness.wait_for_scheduler_stats_sample(stats_path, stats_capture)
    return scheduler, event_bridge, stats_capture


def stop_scheduler_stack(
    scheduler: subprocess.Popen[bytes] | None,
    event_bridge: subprocess.Popen[bytes] | None,
    stats_capture: subprocess.Popen[bytes] | None,
) -> None:
    harness.stop_process(stats_capture, signal.SIGINT)
    harness.stop_process(event_bridge, signal.SIGINT)
    harness.stop_process(scheduler, signal.SIGINT)


# ---------------------------------------------------------------------------
# CSV & summary helpers
# ---------------------------------------------------------------------------

SCHEDULE_CSV_FIELDS = [
    "invocation_id",
    "release_offset_s",
    "generated_at_ns",
    "function_id",
    "expected_time_ms",
    "actual_duration_ms",
    "p99_time_ms",
    "deadline_us",
]


def load_invocation_rows(run_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    path = run_dir / "client_latency.csv"
    if not path.exists():
        return rows
    with path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            parsed: dict[str, Any] = dict(row)
            for key in (
                "invocation_id",
                "exit_code",
                "launch_start_monotonic_ns",
                "start_monotonic_ns",
                "end_monotonic_ns",
                "metadata_ready_monotonic_ns",
                "deadline_us",
                "metadata_tgid",
                "scheduled_monotonic_ns",
                "generated_at_ns",
            ):
                if parsed.get(key) not in (None, ""):
                    parsed[key] = int(float(parsed[key]))
            for key in (
                "duration_ms",
                "metadata_setup_ms",
                "actual_duration_ms",
                "expected_duration_ms",
                "p99_time_ms",
                "scheduled_offset_ms",
                "estimation_error_ms",
                "real_ms",
                "user_ms",
                "sys_ms",
                "cpu_ms",
                "maxrss_kb",
            ):
                if parsed.get(key) not in (None, ""):
                    parsed[key] = float(parsed[key])
            rows.append(parsed)
    return rows


def write_invocations_csv(run_dir: Path, rows: list[dict[str, Any]]) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "invocations.csv"
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


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
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def scheduler_peak_stats(samples: list[dict[str, Any]]) -> dict[str, int | float]:
    peaks: dict[str, int | float] = {}
    for sample in samples:
        stats = sample.get("stats", {})
        if not isinstance(stats, dict):
            continue
        for key, value in stats.items():
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            current = peaks.get(key)
            if current is None or value > current:
                peaks[key] = value
    return peaks


def build_summary(
    run_dir: Path,
    *,
    generated_invocations: int,
    offered_rate: float,
    weighted_mean_ms: float,
    actual_mean_ms: float | None,
    load_mean_ms: float,
    load_mean_source: str,
    cpu_cores: int,
    run_duration_s: float,
    warmup_duration_s: float,
    arrival_mode: str,
    scheduler_stats_path: Path,
    launcher_started_ns: int,
    launcher_finished_ns: int,
) -> dict[str, Any]:
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    all_rows = load_invocation_rows(run_dir)
    write_invocations_csv(run_dir, all_rows)

    measurement_start = launcher_started_ns + int(warmup_duration_s * 1_000_000_000)
    measurement_end = launcher_started_ns + int(run_duration_s * 1_000_000_000)

    rows = [
        r
        for r in all_rows
        if measurement_start
        <= int(
            r.get("generated_at_ns")
            or r.get("scheduled_monotonic_ns")
            or r.get("launch_start_monotonic_ns")
            or 0
        )
        < measurement_end
    ]
    write_invocations_csv(run_dir / "steady_state", rows)

    durations = [float(r["duration_ms"]) for r in rows]
    ok = [r for r in rows if r["status"] == "ok"]
    good = [r for r in ok if float(r["duration_ms"]) * 1000.0 <= int(r["deadline_us"])]
    late = [r for r in ok if r not in good]

    deadline_ratios = [
        (float(r["duration_ms"]) * 1000.0) / int(r["deadline_us"])
        for r in ok
        if int(r["deadline_us"]) > 0
    ]
    start_delays_ms = [
        (
            int(r["start_monotonic_ns"])
            - int(r.get("generated_at_ns") or r.get("scheduled_monotonic_ns", 0))
        )
        / 1_000_000.0
        for r in rows
        if r.get("scheduled_monotonic_ns") not in (None, "")
    ]
    dispatch_delays_ms = [
        (int(r["scheduled_monotonic_ns"]) - int(r["generated_at_ns"])) / 1_000_000.0
        for r in rows
        if r.get("generated_at_ns") not in (None, "", 0)
        and r.get("scheduled_monotonic_ns") not in (None, "")
    ]
    estimation_errors = [
        float(r["estimation_error_ms"])
        for r in rows
        if r.get("estimation_error_ms") not in (None, "")
    ]

    scheduler_samples = []
    if scheduler_stats_path.exists():
        for line in scheduler_stats_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                scheduler_samples.append(json.loads(line))

    wall_elapsed = max(
        0.0, (launcher_finished_ns - launcher_started_ns) / 1_000_000_000.0
    )
    meas_dur = max(0.0, run_duration_s - warmup_duration_s)
    throughput = len(ok) / meas_dur if meas_dur > 0 else 0.0
    goodput = len(good) / meas_dur if meas_dur > 0 else 0.0
    cpu_rows = [r for r in ok if r.get("cpu_ms") not in (None, "")]
    good_cpu_rows = [r for r in good if r.get("cpu_ms") not in (None, "")]
    cpu_ms = [float(r["cpu_ms"]) for r in cpu_rows]
    good_cpu_ms = [float(r["cpu_ms"]) for r in good_cpu_rows]
    total_cpu_ms = sum(cpu_ms)
    good_cpu_total_ms = sum(good_cpu_ms)
    cpu_capacity_ms = meas_dur * max(cpu_cores, 1) * 1_000.0
    effective_utilization = (
        good_cpu_total_ms / cpu_capacity_ms
        if cpu_capacity_ms > 0.0 and good_cpu_ms
        else load_factor(goodput, load_mean_ms, cpu_cores)
    )
    effective_utilization_source = (
        "usr_bin_time_cpu_ms" if good_cpu_ms else "load_mean_time_ms_fallback"
    )

    summary = {
        "config": manifest["config"],
        "workload": manifest["workload"],
        "metadata_mode": manifest["metadata_mode"],
        "scheduler_flags": manifest["scheduler_flags"],
        "offered_rate_inv_per_sec": offered_rate,
        "weighted_mean_time_ms": weighted_mean_ms,
        "weighted_mean_duration_ms": weighted_mean_ms,
        "actual_mean_time_ms": actual_mean_ms,
        "load_mean_time_ms": load_mean_ms,
        "load_mean_source": load_mean_source,
        "cpu_cores": cpu_cores,
        "offered_load": load_factor(offered_rate, load_mean_ms, cpu_cores),
        "effective_utilization": effective_utilization,
        "effective_utilization_source": effective_utilization_source,
        "scheduled_invocations": generated_invocations,
        "completed_invocations": len(all_rows),
        "measurement_invocations": len(rows),
        "run_duration_s": run_duration_s,
        "warmup_duration_s": warmup_duration_s,
        "measurement_duration_s": meas_dur,
        "arrival_mode": arrival_mode,
        "wall_elapsed_s": wall_elapsed,
        "throughput_inv_per_sec": throughput,
        "goodput_inv_per_sec": goodput,
        "slo_miss_rate": len(late) / len(ok) if ok else 0.0,
        "failure_rate": (len(rows) - len(ok)) / len(rows) if rows else 0.0,
        "cpu_time": {
            "source": "usr_bin_time",
            "count": len(cpu_ms),
            "missing_count": len(ok) - len(cpu_ms),
            "total_ms": total_cpu_ms,
            "good_total_ms": good_cpu_total_ms,
            "capacity_ms": cpu_capacity_ms,
            "utilization": total_cpu_ms / cpu_capacity_ms
            if cpu_capacity_ms > 0.0 and cpu_ms
            else 0.0,
            "good_utilization": effective_utilization,
            "mean_ms": sum(cpu_ms) / len(cpu_ms) if cpu_ms else 0.0,
            "p50_ms": percentile(cpu_ms, 0.50),
            "p95_ms": percentile(cpu_ms, 0.95),
            "p99_ms": percentile(cpu_ms, 0.99),
        },
        "latency": {
            "count": len(rows),
            "successes": len(ok),
            "failures": len(rows) - len(ok),
            "min_ms": min(durations) if durations else 0.0,
            "max_ms": max(durations) if durations else 0.0,
            "mean_ms": sum(durations) / len(durations) if durations else 0.0,
            "p50_ms": percentile(durations, 0.50),
            "p95_ms": percentile(durations, 0.95),
            "p99_ms": percentile(durations, 0.99),
            "client_slo_violations": len(late),
        },
        "deadline_ratio": {
            "mean": sum(deadline_ratios) / len(deadline_ratios)
            if deadline_ratios
            else 0.0,
            "p95": percentile(deadline_ratios, 0.95),
            "p99": percentile(deadline_ratios, 0.99),
        },
        "start_delay_ms": {
            "mean": sum(start_delays_ms) / len(start_delays_ms)
            if start_delays_ms
            else 0.0,
            "p95": percentile(start_delays_ms, 0.95),
            "p99": percentile(start_delays_ms, 0.99),
        },
        "dispatch_delay_ms": {
            "mean": sum(dispatch_delays_ms) / len(dispatch_delays_ms)
            if dispatch_delays_ms
            else 0.0,
            "p95": percentile(dispatch_delays_ms, 0.95),
            "p99": percentile(dispatch_delays_ms, 0.99),
        },
        "estimation_error_ms": {
            "mean": sum(estimation_errors) / len(estimation_errors)
            if estimation_errors
            else 0.0,
            "p50": percentile(estimation_errors, 0.50),
            "p95": percentile(estimation_errors, 0.95),
            "p99": percentile(estimation_errors, 0.99),
        },
        "scheduler": {
            "samples": len(scheduler_samples),
            "last": scheduler_samples[-1]["stats"] if scheduler_samples else {},
            "peak": scheduler_peak_stats(scheduler_samples),
        },
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    return summary


# ---------------------------------------------------------------------------
# Single‑rate experiment
# ---------------------------------------------------------------------------


def write_manifest(
    run_dir: Path,
    *,
    config: str,
    scheduler_flags: list[str],
    metadata_mode: str,
    config_json: Path,
    offered_rate: float,
    weighted_mean_ms: float,
    actual_mean_ms: float | None,
    load_mean_ms: float,
    load_mean_source: str,
    cpu_cores: int,
    arrival_mode: str,
    run_duration_s: float,
    warmup_duration_s: float,
    profiles: list[FunctionProfile],
    min_slack_us: int,
    deadline_safety_factor: float,
    deadline_floor_ms: float,
    launcher_workers: int,
    pool_json: Path | None = None,
) -> None:
    max_dl = max(
        (
            deadline_us_from_p99(
                p.p99_time_ms,
                min_slack_us,
                safety_factor=deadline_safety_factor,
                deadline_floor_ms=deadline_floor_ms,
            )
            for p in profiles
        ),
        default=0,
    )
    payload = {
        "config": config,
        "workload": "cpu_burst",
        "concurrency": launcher_workers,
        "duration_ms": int(round(run_duration_s * 1000)),
        "warmup_duration_ms": int(round(warmup_duration_s * 1000)),
        "deadline_us": max_dl,
        "cpu_cores": cpu_cores,
        "metadata_mode": metadata_mode,
        "scheduler_flags": scheduler_flags,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "config_json": str(config_json),
        "offered_rate_inv_per_sec": offered_rate,
        "weighted_mean_time_ms": weighted_mean_ms,
        "weighted_mean_duration_ms": weighted_mean_ms,
        "actual_mean_time_ms": actual_mean_ms,
        "load_mean_time_ms": load_mean_ms,
        "load_mean_source": load_mean_source,
        "offered_load": load_factor(offered_rate, load_mean_ms, cpu_cores),
        "arrival_mode": arrival_mode,
        "scheduled_invocations": 0,
    }
    if pool_json is not None:
        payload["pool_json"] = str(pool_json)
    (run_dir / "manifest.json").write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8"
    )


def update_manifest_scheduled(run_dir: Path, count: int) -> None:
    path = run_dir / "manifest.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["scheduled_invocations"] = count
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def run_single_rate(
    *,
    args: argparse.Namespace,
    sweep_dir: Path,
    config_json: Path,
    profiles: list[FunctionProfile],
    rate: float,
    repetition: int,
    seed: int,
    pool: InvocationPool | None = None,
    pool_json: Path | None = None,
) -> dict[str, Any]:
    run_dir = sweep_dir / f"rate_{rate:.6f}_rep_{repetition}"
    run_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    cpu_cores = pool.cpu_cores if pool is not None else (os.cpu_count() or 1)
    weighted_mean_ms = (
        pool.weighted_mean_time_ms if pool is not None else weighted_mean_time_ms(profiles)
    )
    actual_mean_ms = pool.actual_mean_time_ms if pool is not None else None
    load_mean_ms = pool.load_mean_time_ms if pool is not None else weighted_mean_ms
    load_mean_source = pool.load_mean_source if pool is not None else "weighted_mean_time_ms"

    if args.config == "cfs-default":
        scheduler_flags: list[str] = []
        metadata_mode = "disabled"
        use_metadata = False
    else:
        global_slo = weighted_expected_p50_ms(profiles) * 1_000
        scheduler_flags, metadata_mode, use_metadata = run_cosmos.cosmos_config(
            args.config,
            global_slo,
            weighted_expected_p50_ms(profiles),
        )
        scheduler_flags = [*scheduler_flags, *args.scheduler_flag]

    workers = estimate_worker_requirement(
        profiles,
        rate=rate,
        min_slack_us=args.min_slack_us,
        deadline_safety_factor=args.deadline_safety_factor,
        deadline_floor_ms=args.deadline_floor_ms,
        worker_safety_factor=args.worker_safety_factor,
        max_launch_workers=args.max_launch_workers,
    )
    write_manifest(
        run_dir,
        config=args.config,
        scheduler_flags=scheduler_flags,
        metadata_mode=metadata_mode,
        config_json=config_json,
        offered_rate=rate,
        weighted_mean_ms=weighted_mean_ms,
        actual_mean_ms=actual_mean_ms,
        load_mean_ms=load_mean_ms,
        load_mean_source=load_mean_source,
        cpu_cores=cpu_cores,
        arrival_mode=args.arrival_mode,
        run_duration_s=args.run_duration_s,
        warmup_duration_s=args.warmup_duration_s,
        profiles=profiles,
        min_slack_us=args.min_slack_us,
        deadline_safety_factor=args.deadline_safety_factor,
        deadline_floor_ms=args.deadline_floor_ms,
        launcher_workers=workers,
        pool_json=pool_json,
    )

    harness.ensure_benchmark_workload_build()
    scheduler = event_bridge = stats_capture = None
    failures = 0
    generated = 0
    launcher_start = 0
    launcher_end = launcher_start

    try:
        scheduler, event_bridge, stats_capture = start_scheduler_stack(
            config=args.config,
            scheduler_bin=args.scheduler_bin,
            stats_socket=args.stats_socket,
            event_bridge_port=args.event_bridge_port,
            scheduler_flags=scheduler_flags,
            use_metadata=use_metadata,
            run_dir=run_dir,
        )
        if args.scheduler_settle_s > 0:
            time.sleep(args.scheduler_settle_s)

        (run_dir / "invocations").mkdir(parents=True, exist_ok=True)
        launcher_start = time.monotonic_ns()

        with (run_dir / "schedule.csv").open("w", encoding="utf-8", newline="") as fh:
            csv_writer = csv.DictWriter(fh, fieldnames=SCHEDULE_CSV_FIELDS)
            csv_writer.writeheader()

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures: list[Future[int]] = []
                generator: Iterator[ScheduledInvocation]
                if pool is None:
                    generator = invocation_generator(
                        profiles,
                        rate=rate,
                        duration_s=args.run_duration_s,
                        arrival_mode=args.arrival_mode,
                        min_slack_us=args.min_slack_us,
                        deadline_safety_factor=args.deadline_safety_factor,
                        deadline_floor_ms=args.deadline_floor_ms,
                        tail_max_multiplier=args.tail_max_multiplier,
                        rng=rng,
                        run_start_ns=launcher_start,
                    )
                else:
                    generator = pool_invocation_generator(
                        pool,
                        profiles,
                        rate=rate,
                        duration_s=args.run_duration_s,
                        arrival_mode=args.arrival_mode,
                        rng=rng,
                        run_start_ns=launcher_start,
                    )

                for item in generator:
                    generated += 1
                    csv_writer.writerow(
                        {
                            "invocation_id": item.invocation_id,
                            "release_offset_s": f"{item.release_offset_s:.6f}",
                            "generated_at_ns": item.generated_at_ns,
                            "function_id": item.profile.function_id,
                            "expected_time_ms": item.profile.expected_time_ms,
                            "actual_duration_ms": item.actual_duration_ms,
                            "p99_time_ms": item.profile.p99_time_ms,
                            "deadline_us": item.deadline_us,
                        }
                    )
                    futures.append(
                        executor.submit(
                            run_invocation_task,
                            run_dir,
                            args.config,
                            item,
                            use_metadata,
                            args.event_bridge_port if use_metadata else None,
                        )
                    )

                for fut in as_completed(futures):
                    if fut.result() != 0:
                        failures += 1

        launcher_end = time.monotonic_ns()
        update_manifest_scheduled(run_dir, generated)

    finally:
        stop_scheduler_stack(scheduler, event_bridge, stats_capture)

    harness.write_client_latency_csv(run_dir)
    summary = build_summary(
        run_dir,
        generated_invocations=generated,
        offered_rate=rate,
        weighted_mean_ms=weighted_mean_ms,
        actual_mean_ms=actual_mean_ms,
        load_mean_ms=load_mean_ms,
        load_mean_source=load_mean_source,
        cpu_cores=cpu_cores,
        run_duration_s=args.run_duration_s,
        warmup_duration_s=args.warmup_duration_s,
        arrival_mode=args.arrival_mode,
        scheduler_stats_path=run_dir / "scheduler_stats.jsonl",
        launcher_started_ns=launcher_start,
        launcher_finished_ns=launcher_end,
    )
    summary["total_failures"] = failures
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    return summary


# ---------------------------------------------------------------------------
# Load sweep
# ---------------------------------------------------------------------------


def run_load_sweep(
    args: argparse.Namespace,
    *,
    sweep_dir: Path,
    profiles: list[FunctionProfile],
    pool: InvocationPool | None = None,
    pool_json: Path | None = None,
) -> dict[str, Any]:
    cpu_cores = pool.cpu_cores if pool is not None else (os.cpu_count() or 1)
    weighted_mean_ms = (
        pool.weighted_mean_time_ms if pool is not None else weighted_mean_time_ms(profiles)
    )
    actual_mean_ms = pool.actual_mean_time_ms if pool is not None else None
    load_mean_ms = pool.load_mean_time_ms if pool is not None else weighted_mean_ms
    load_mean_source = pool.load_mean_source if pool is not None else "weighted_mean_time_ms"
    repeats = max(1, int(getattr(args, "repeats", 1)))
    config_json = getattr(args, "config_json", pool_json or Path("pool.json"))

    if args.rate is not None:  # explicit single run only
        if args.rate <= 0.0:
            raise SystemExit("--rate must be positive")
        rates = [args.rate]
    else:
        if args.load_steps < 1:
            raise SystemExit("--load-steps must be at least 1")
        if args.load_min <= 0.0 or args.load_max <= 0.0:
            raise SystemExit("--load-min and --load-max must be positive")
        if args.load_max < args.load_min:
            raise SystemExit("--load-max must be greater than or equal to --load-min")
        if args.load_steps == 1:
            loads = [args.load_min]
        else:
            loads = [
                args.load_min
                + (args.load_max - args.load_min) * i / (args.load_steps - 1)
                for i in range(args.load_steps)
            ]
        rates = [rate_from_load(load, load_mean_ms, cpu_cores) for load in loads]

    candidates: list[dict[str, Any]] = []
    for idx, rate in enumerate(rates, start=1):
        runs: list[dict[str, Any]] = []
        run_dirs: list[str] = []
        for repetition in range(1, repeats + 1):
            run = run_single_rate(
                args=args,
                sweep_dir=sweep_dir,
                config_json=config_json,
                profiles=profiles,
                rate=rate,
                repetition=repetition,
                seed=args.seed
                + idx * 37
                + repetition * 10_003
                + int(round(rate * 1000.0)),
                pool=pool,
                pool_json=pool_json,
            )
            runs.append(run)
            run_dirs.append(str(sweep_dir / f"rate_{rate:.6f}_rep_{repetition}"))

        median_goodput = median_float(
            [float(run["goodput_inv_per_sec"]) for run in runs]
        )
        median_throughput = median_float(
            [float(run["throughput_inv_per_sec"]) for run in runs]
        )
        median_miss = median_float([float(run["slo_miss_rate"]) for run in runs])
        median_effective_utilization = median_float(
            [float(run["effective_utilization"]) for run in runs]
        )
        candidates.append(
            {
                "rate_inv_per_sec": rate,
                "offered_load": load_factor(rate, load_mean_ms, cpu_cores),
                "goodput_inv_per_sec": median_goodput,
                "effective_utilization": median_effective_utilization,
                "throughput_inv_per_sec": median_throughput,
                "slo_miss_rate": median_miss,
                "meets_slo_threshold": median_miss <= args.slo_miss_threshold,
                "repeats": repeats,
                "run_dir": run_dirs[0],
                "run_dirs": run_dirs,
            }
        )

    ordered = candidates
    compliant = [c for c in ordered if c["slo_miss_rate"] <= args.slo_miss_threshold]
    best = (
        max(compliant or ordered, key=lambda c: c["goodput_inv_per_sec"])
        if ordered
        else None
    )
    best_rate = best["rate_inv_per_sec"] if best is not None else None
    best_goodput = best["goodput_inv_per_sec"] if best is not None else None
    best_effective_utilization = (
        load_factor(best_goodput, load_mean_ms, cpu_cores)
        if best_goodput is not None
        else None
    )
    meets_slo = best in compliant if best is not None else False

    single_rate = args.rate is not None

    sweep = {
        "config": args.config,
        "config_json": str(config_json),
        "pool_json": str(pool_json) if pool_json is not None else None,
        "arrival_mode": args.arrival_mode,
        "run_duration_s": args.run_duration_s,
        "warmup_duration_s": args.warmup_duration_s,
        "steady_state_duration_s": max(
            0.0, args.run_duration_s - args.warmup_duration_s
        ),
        "slo_threshold": args.slo_miss_threshold,
        "single_rate_mode": single_rate,
        "repeats": repeats,
        "cpu_cores": cpu_cores,
        "weighted_mean_time_ms": weighted_mean_ms,
        "weighted_mean_duration_ms": weighted_mean_ms,
        "actual_mean_time_ms": actual_mean_ms,
        "load_mean_time_ms": load_mean_ms,
        "load_mean_source": load_mean_source,
        "load_sweep": None
        if single_rate
        else {
            "load_min": args.load_min,
            "load_max": args.load_max,
            "load_steps": args.load_steps,
        },
        "offered_rate_inv_per_sec": args.rate,
        "optimal_rate_inv_per_sec": best_rate,
        "optimal_offered_load": (
            load_factor(best_rate, load_mean_ms, cpu_cores)
            if best_rate is not None
            else None
        ),
        "max_goodput_inv_per_sec": best_goodput,
        "max_effective_utilization": best_effective_utilization,
        "max_goodput_meets_slo": meets_slo,
        "candidates": ordered,
    }
    (sweep_dir / "sweep.json").write_text(
        json.dumps(sweep, indent=2) + "\n", encoding="utf-8"
    )
    return sweep


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.warmup_duration_s < 0:
        raise SystemExit("--warmup-duration-s must be non-negative")
    if args.run_duration_s <= args.warmup_duration_s:
        raise SystemExit("--run-duration-s must be greater than --warmup-duration-s")
    if args.repeats < 1:
        raise SystemExit("--repeats must be at least 1")

    profiles = load_profiles(args.config_json)
    duration_cap_ms = args.duration_cap_ms if args.duration_cap_ms > 0 else None
    profiles = cap_profiles(profiles, duration_cap_ms)
    if args.generate_pool_json is not None:
        invocations = generate_invocation_pool(
            profiles,
            count=args.pool_size,
            seed=args.seed,
            min_slack_us=args.min_slack_us,
            deadline_safety_factor=args.deadline_safety_factor,
            deadline_floor_ms=args.deadline_floor_ms,
            tail_max_multiplier=args.tail_max_multiplier,
        )
        write_pool_json(
            args.generate_pool_json,
            config_json=args.config_json,
            profiles=profiles,
            invocations=invocations,
            seed=args.seed,
            duration_cap_ms=duration_cap_ms,
        )
        print(args.generate_pool_json)
        return 0

    pool = None
    if args.pool_json is not None:
        pool = load_invocation_pool(
            args.pool_json,
            profiles,
            weighted_mean_ms=weighted_mean_time_ms(profiles),
            min_slack_us=args.min_slack_us,
            deadline_safety_factor=args.deadline_safety_factor,
            deadline_floor_ms=args.deadline_floor_ms,
        )

    config_root = args.out_dir or RESULTS_ROOT / "azure_top_functions" / args.config
    sweep_dir = config_root / harness.timestamped_run_id()
    sweep_dir.mkdir(parents=True, exist_ok=True)

    sweep = run_load_sweep(
        args,
        sweep_dir=sweep_dir,
        profiles=profiles,
        pool=pool,
        pool_json=args.pool_json,
    )
    harness.refresh_latest_link(config_root, sweep_dir)
    print(sweep_dir)
    return 0 if sweep["max_goodput_inv_per_sec"] is not None else 1


if __name__ == "__main__":
    raise SystemExit(main())
