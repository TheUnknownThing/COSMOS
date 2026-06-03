#!/usr/bin/env python3
"""Run an offered-load sweep from a pre-generated invocation pool."""

from __future__ import annotations

import argparse
from pathlib import Path

import harness
import replay_top_functions as replay


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay a deterministic Azure top-functions invocation pool."
    )
    parser.add_argument("--pool-json", type=Path, required=True)
    parser.add_argument(
        "--config",
        default="cosmos-full",
        choices=[
            "cfs-default",
            "cosmos-heuristic",
            "cosmos-metadata",
            "cosmos-full",
            "sfs",
        ],
    )
    parser.add_argument("--load-min", type=float, default=0.8)
    parser.add_argument("--load-max", type=float, default=1.4)
    parser.add_argument("--load-steps", type=int, default=8)
    parser.add_argument("--rate", type=float, default=None)
    parser.add_argument("--run-duration-s", type=float, default=180.0)
    parser.add_argument("--warmup-duration-s", type=float, default=30.0)
    parser.add_argument(
        "--arrival-mode",
        choices=["evenly-spaced"],
        default="evenly-spaced",
        help="Pool replay intentionally fixes arrivals to evenly-spaced.",
    )
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument(
        "--slo-miss-threshold",
        type=float,
        default=0.05,
        help="Maximum tolerated steady-state SLO miss rate.",
    )
    parser.add_argument(
        "--min-slack-us", type=int, default=harness.DEFAULT_SLO_MIN_SLACK_US
    )
    parser.add_argument("--deadline-safety-factor", type=float, default=1.2)
    parser.add_argument("--deadline-floor-ms", type=float, default=100.0)
    parser.add_argument("--worker-safety-factor", type=float, default=2.0)
    parser.add_argument("--scheduler-settle-s", type=float, default=1.0)
    parser.add_argument("--max-launch-workers", type=int, default=1024)
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


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.warmup_duration_s < 0:
        raise SystemExit("--warmup-duration-s must be non-negative")
    if args.run_duration_s <= args.warmup_duration_s:
        raise SystemExit("--run-duration-s must be greater than --warmup-duration-s")
    if args.repeats < 1:
        raise SystemExit("--repeats must be at least 1")

    profiles = replay.load_profiles_from_pool_json(args.pool_json)
    pool = replay.load_invocation_pool(
        args.pool_json,
        profiles,
        weighted_mean_ms=replay.weighted_mean_time_ms(profiles),
        min_slack_us=args.min_slack_us,
        deadline_safety_factor=args.deadline_safety_factor,
        deadline_floor_ms=args.deadline_floor_ms,
    )
    args.config_json = args.pool_json
    args.tail_max_multiplier = 10.0

    config_root = (
        args.out_dir
        or replay.RESULTS_ROOT
        / "azure_top_functions_pool"
        / args.pool_json.stem
        / args.config
    )
    sweep_dir = config_root / harness.timestamped_run_id()
    sweep_dir.mkdir(parents=True, exist_ok=True)

    sweep = replay.run_load_sweep(
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
