#!/usr/bin/env python3
"""Generate a deterministic Azure top-functions invocation pool."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
LOCAL_HARNESS_DIR = SCRIPT_DIR.parent / "local_harness"
if str(LOCAL_HARNESS_DIR) not in sys.path:
    sys.path.insert(0, str(LOCAL_HARNESS_DIR))

import harness
import replay_top_functions as replay


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a reusable invocation pool JSON for Azure replay."
    )
    parser.add_argument("--config-json", type=Path, required=True)
    parser.add_argument("--pool-size", type=int, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("-o", "--output", type=Path, required=True)
    parser.add_argument(
        "--workload-mix",
        choices=replay.WORKLOAD_MIX_CHOICES,
        default=replay.CONFIG_WORKLOAD_MIX,
        help=(
            "Assign local benchmark workload types to Azure functions. "
            "'config' uses per-function workload fields when present and "
            "falls back to cpu_burst."
        ),
    )
    parser.add_argument(
        "--min-slack-us", type=int, default=harness.DEFAULT_SLO_MIN_SLACK_US
    )
    parser.add_argument("--deadline-safety-factor", type=float, default=1.2)
    parser.add_argument("--deadline-floor-ms", type=float, default=100.0)
    parser.add_argument(
        "--tail-max-multiplier",
        type=float,
        default=10.0,
        help="Kept for compatibility; replay tails are clamped to p99.",
    )
    parser.add_argument(
        "--duration-cap-ms",
        type=int,
        default=10_000,
        help="Clamp generated invocation durations and p99/deadlines; use 0 to disable.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    profiles = replay.load_profiles(args.config_json, workload_mix=args.workload_mix)
    duration_cap_ms = args.duration_cap_ms if args.duration_cap_ms > 0 else None
    profiles = replay.cap_profiles(profiles, duration_cap_ms)
    invocations = replay.generate_invocation_pool(
        profiles,
        count=args.pool_size,
        seed=args.seed,
        min_slack_us=args.min_slack_us,
        deadline_safety_factor=args.deadline_safety_factor,
        deadline_floor_ms=args.deadline_floor_ms,
        tail_max_multiplier=args.tail_max_multiplier,
    )
    replay.write_pool_json(
        args.output,
        config_json=args.config_json,
        profiles=profiles,
        invocations=invocations,
        seed=args.seed,
        duration_cap_ms=duration_cap_ms,
        workload_mix=args.workload_mix,
    )
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
