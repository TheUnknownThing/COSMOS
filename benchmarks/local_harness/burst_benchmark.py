#!/usr/bin/env python3

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent


def parse_mix_concurrency(mix_str: str) -> int:
    total = 0
    depth = 0
    start = 0
    parts: list[str] = []
    for idx, char in enumerate(mix_str):
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            parts.append(mix_str[start:idx])
            start = idx + 1
    parts.append(mix_str[start:])
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if part.endswith(")") and "(" in part:
            part = part[: part.rfind("(")]
        _, count = part.rsplit(":", 1)
        total += int(count.strip())
    return total


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Dispatch a Phase 6 benchmark run.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--workload", default=None)
    parser.add_argument("--mix", default=None, help="Mixed workloads e.g. cpu_burst:50,sleep_short:50")
    parser.add_argument("--replay-plan", type=Path)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--duration-ms", type=int)
    parser.add_argument("--deadline-us", type=int)
    parser.add_argument(
        "--slo-class",
        type=int,
        choices=[0, 1, 2],
        help="Override synthetic metadata SLO class for COSMOS configs",
    )
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--scheduler-bin", type=Path)
    parser.add_argument("--stats-socket", type=Path)
    parser.add_argument("--event-bridge-port", type=int)
    parser.add_argument("--scheduler-flag", action="append", default=[])
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    specified = sum(value is not None for value in (args.mix, args.workload, args.replay_plan))
    if specified != 1:
        raise SystemExit("specify exactly one of --workload, --mix, or --replay-plan")

    workload = args.workload or args.mix
    concurrency = args.concurrency
    if args.mix:
        concurrency = parse_mix_concurrency(args.mix)

    if args.config == "cfs-default":
        command = ["python3", str(SCRIPT_DIR / "run_baseline.py")]
    elif args.config in {
        "cosmos-heuristic",
        "cosmos-metadata",
        "cosmos-pooled",
        "cosmos-full",
        "cosmos-slack-only",
        "cosmos-slack+xres",
        "cosmos-slack+xres+phase",
        "cosmos-no-phase-predict",
        "cosmos-phase-predict",
        "sfs",
    }:
        command = ["python3", str(SCRIPT_DIR / "run_cosmos.py")]
    else:
        raise SystemExit(f"unknown benchmark config: {args.config}")

    command.extend(["--config", args.config])
    if args.replay_plan is not None:
        command.extend(["--replay-plan", str(args.replay_plan)])
    else:
        command.extend(["--workload", workload, "--concurrency", str(concurrency)])
    if args.duration_ms is not None:
        command.extend(["--duration-ms", str(args.duration_ms)])
    if args.deadline_us is not None:
        command.extend(["--deadline-us", str(args.deadline_us)])
    if args.out_dir is not None:
        command.extend(["--out-dir", str(args.out_dir)])
    if args.scheduler_bin is not None:
        command.extend(["--scheduler-bin", str(args.scheduler_bin)])
    if args.stats_socket is not None:
        command.extend(["--stats-socket", str(args.stats_socket)])
    if args.event_bridge_port is not None:
        command.extend(["--event-bridge-port", str(args.event_bridge_port)])
    if args.slo_class is not None and args.config != "cfs-default":
        command.extend(["--slo-class", str(args.slo_class)])
    for flag in args.scheduler_flag:
        command.extend(["--scheduler-flag", flag])

    completed = subprocess.run(command, cwd=SCRIPT_DIR.parent.parent, check=False)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
