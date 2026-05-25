#!/usr/bin/env python3

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Dispatch a Phase 6 benchmark run.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--workload", required=True)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--duration-ms", type=int)
    parser.add_argument("--deadline-us", type=int)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--scheduler-bin", type=Path)
    parser.add_argument("--stats-socket", type=Path)
    parser.add_argument("--event-bridge-port", type=int)
    parser.add_argument("--scheduler-flag", action="append", default=[])
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    command = []
    if args.config == "cfs-default":
        command = ["python3", str(SCRIPT_DIR / "run_baseline.py")]
    elif args.config in {"cosmos-heuristic", "cosmos-metadata", "cosmos-pooled", "cosmos-full"}:
        command = ["python3", str(SCRIPT_DIR / "run_cosmos.py")]
    else:
        raise SystemExit(f"unknown benchmark config: {args.config}")

    command.extend(["--config", args.config, "--workload", args.workload, "--concurrency", str(args.concurrency)])
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
    for flag in args.scheduler_flag:
        command.extend(["--scheduler-flag", flag])

    completed = subprocess.run(command, cwd=SCRIPT_DIR.parent.parent, check=False)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
