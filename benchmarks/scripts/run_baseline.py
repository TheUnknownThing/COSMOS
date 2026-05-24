#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import harness


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a lightweight CFS baseline benchmark.")
    parser.add_argument("--config", default="cfs-default")
    parser.add_argument("--workload", required=True, choices=harness.workload_names())
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--duration-ms", type=int)
    parser.add_argument("--deadline-us", type=int)
    parser.add_argument("--out-dir", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    spec = harness.workload_spec(args.workload)
    duration_ms = args.duration_ms or spec.default_duration_ms
    deadline_us = args.deadline_us or spec.default_deadline_us
    config_root = args.out_dir or harness.default_results_dir(args.config)
    run_dir = config_root / harness.timestamped_run_id()
    run_dir.mkdir(parents=True, exist_ok=True)

    harness.write_manifest(
        run_dir,
        args.config,
        args.workload,
        args.concurrency,
        duration_ms,
        deadline_us,
        "disabled",
        [],
    )
    (run_dir / "scheduler_stats.jsonl").write_text("", encoding="utf-8")
    failures = harness.run_invocations(
        run_dir,
        args.workload,
        args.concurrency,
        duration_ms,
        deadline_us,
        False,
        args.config,
    )
    harness.finalize_run(run_dir, config_root)
    print(run_dir)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
