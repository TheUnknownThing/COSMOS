#!/usr/bin/env python3

from __future__ import annotations

import argparse
import signal
import subprocess
import sys
from pathlib import Path

import harness


def cosmos_config(config: str, deadline_us: int) -> tuple[list[str], str, bool]:
    if config == "cosmos-heuristic":
        return (
            [
                "--slo-target-us",
                str(deadline_us),
                "--disable-pools",
                "--disable-deadline-scoring",
                "--tail-guard-cpus",
                "0",
                "--tail-guard-threshold-us",
                "0",
            ],
            "heuristic-fallback",
            False,
        )
    if config == "cosmos-metadata":
        return (
            [
                "--slo-target-us",
                str(deadline_us),
                "--disable-pools",
                "--disable-deadline-scoring",
                "--tail-guard-cpus",
                "0",
                "--tail-guard-threshold-us",
                "0",
            ],
            "metadata-only",
            True,
        )
    if config == "cosmos-pooled":
        return (
            [
                "--slo-target-us",
                str(deadline_us),
                "--disable-deadline-scoring",
                "--tail-guard-cpus",
                "0",
                "--tail-guard-threshold-us",
                "0",
            ],
            "metadata-with-pools",
            True,
        )
    if config == "cosmos-full":
        return (
            ["--slo-target-us", str(deadline_us)],
            "metadata-full",
            True,
        )
    if config == "sfs":
        return (
            ["--policy", "sfs"],
            "metadata-sfs",
            True,
        )
    raise KeyError(f"unknown COSMOS config: {config}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a lightweight COSMOS benchmark.")
    parser.add_argument(
        "--config",
        default="cosmos-full",
        choices=["cosmos-heuristic", "cosmos-metadata", "cosmos-pooled", "cosmos-full", "sfs"],
    )
    parser.add_argument("--workload", required=True)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--duration-ms", type=int)
    parser.add_argument("--deadline-us", type=int)
    parser.add_argument("--out-dir", type=Path)
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
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    is_mixed = "," in args.workload
    if is_mixed:
        duration_ms = args.duration_ms or harness.DEFAULT_WORKLOAD_DURATION_MS
        deadline_us = args.deadline_us or (duration_ms * 2 * 1000)
    else:
        spec = harness.workload_spec(args.workload)
        duration_ms = args.duration_ms or spec.default_duration_ms
        deadline_us = harness.resolve_deadline_us(spec, duration_ms, args.deadline_us)
    scheduler_flags, metadata_mode, use_metadata = cosmos_config(
        args.config, deadline_us
    )
    scheduler_flags.extend(args.scheduler_flag)

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
        metadata_mode,
        scheduler_flags,
    )

    harness.ensure_release_build()
    scheduler_log = run_dir / "scheduler.log"
    event_bridge_log = run_dir / "event_bridge.log"
    stats_capture = None
    scheduler = None
    event_bridge = None

    try:
        harness.remove_stale_unix_socket(args.stats_socket)
        with scheduler_log.open("w", encoding="utf-8") as log_file:
            scheduler = subprocess.Popen(
                [str(args.scheduler_bin), *scheduler_flags],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd=harness.REPO_ROOT,
            )

        harness.wait_for_scheduler_stats(args.stats_socket, scheduler, scheduler_log)
        metadata_bridge_port = None
        if use_metadata:
            event_bridge = harness.start_event_bridge(
                event_bridge_log, args.event_bridge_port
            )
            harness.wait_for_event_bridge(
                args.event_bridge_port, event_bridge, event_bridge_log
            )
            metadata_bridge_port = args.event_bridge_port

        scheduler_stats_path = run_dir / "scheduler_stats.jsonl"
        stats_capture = harness.start_scheduler_stats_capture(
            scheduler_stats_path, args.stats_socket
        )
        harness.wait_for_scheduler_stats_sample(scheduler_stats_path, stats_capture)
        failures = harness.run_invocations(
            run_dir,
            args.workload,
            args.concurrency,
            duration_ms,
            deadline_us,
            use_metadata,
            args.config,
            metadata_bridge_port,
        )
    finally:
        harness.stop_process(stats_capture, signal.SIGINT)
        harness.stop_process(event_bridge, signal.SIGINT)
        harness.stop_process(scheduler, signal.SIGINT)

    harness.finalize_run(run_dir, config_root)
    print(run_dir)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
