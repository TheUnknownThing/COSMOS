#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a workload command with COSMOS invocation metadata.")
    parser.add_argument("--shim-lib", required=True, type=Path)
    parser.add_argument("--deadline-us", type=int, default=10_000)
    parser.add_argument("--slo-class", type=int, default=0)
    parser.add_argument("--cold-start", type=int, default=0)
    parser.add_argument("--invocation-id", type=int, default=1)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise SystemExit("a workload command is required")

    deadline_ns = time.monotonic_ns() + args.deadline_us * 1_000
    env = os.environ.copy()
    existing_preload = env.get("LD_PRELOAD", "")
    env["LD_PRELOAD"] = (
        f"{args.shim_lib}:{existing_preload}" if existing_preload else str(args.shim_lib)
    )
    env["COSMOS_DEADLINE_NS"] = str(deadline_ns)
    env["COSMOS_SLO_CLASS"] = str(args.slo_class)
    env["COSMOS_COLD_START"] = str(args.cold_start)
    env["COSMOS_INVOCATION_ID"] = str(args.invocation_id)

    completed = subprocess.run(command, env=env, check=False)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
