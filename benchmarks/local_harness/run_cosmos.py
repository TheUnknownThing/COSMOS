#!/usr/bin/env python3

from __future__ import annotations

import argparse
import signal
import subprocess
import sys
from pathlib import Path

import harness


def cosmos_config(config: str, deadline_us: int, duration_ms: int) -> tuple[list[str], str, bool]:
    slo_target_us = duration_ms * 1000
    if config == "cosmos-heuristic":
        return (
            [
                "--slo-target-us",
                str(slo_target_us),
                "--disable-deadline-scoring",
                "--disable-short-preemption",
            ],
            "heuristic-fallback",
            False,
        )
    if config == "cosmos-metadata":
        return (
            [
                "--slo-target-us",
                str(slo_target_us),
                "--disable-deadline-scoring",
                "--disable-short-preemption",
            ],
            "metadata-only",
            True,
        )
    if config == "cosmos-full":
        return (
            ["--slo-target-us", str(slo_target_us)],
            "metadata-deadline-preempt",
            True,
        )
    if config == "cosmos-slack-only":
        return (
            [
                "--slo-target-us",
                str(slo_target_us),
                "--disable-cgroup-actuator",
                "--disable-network-actuator",
                "--disable-phase-prediction",
                "--disable-warm-value",
            ],
            "metadata-slack-only",
            True,
        )
    if config == "cosmos-slack+xres":
        return (
            [
                "--slo-target-us",
                str(slo_target_us),
                "--disable-phase-prediction",
                "--disable-warm-value",
            ],
            "metadata-slack-xres",
            True,
        )
    if config == "cosmos-slack+xres+phase":
        return (
            [
                "--slo-target-us",
                str(slo_target_us),
                "--disable-warm-value",
            ],
            "metadata-slack-xres-phase",
            True,
        )
    if config == "cosmos-no-phase-predict":
        return (
            [
                "--slo-target-us",
                str(slo_target_us),
                "--disable-phase-prediction",
            ],
            "metadata-no-phase-predict",
            True,
        )
    if config == "cosmos-phase-predict":
        return (
            ["--slo-target-us", str(slo_target_us)],
            "metadata-phase-predict",
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
        choices=[
            "cosmos-heuristic",
            "cosmos-metadata",
            "cosmos-full",
            "cosmos-slack-only",
            "cosmos-slack+xres",
            "cosmos-slack+xres+phase",
            "cosmos-no-phase-predict",
            "cosmos-phase-predict",
            "sfs",
        ],
    )
    parser.add_argument("--workload")
    parser.add_argument("--replay-plan", type=Path)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--duration-ms", type=int)
    parser.add_argument("--deadline-us", type=int)
    parser.add_argument(
        "--slo-class",
        type=int,
        choices=[0, 1, 2],
        help="Override synthetic metadata SLO class: 0 latency-critical, 1 standard, 2 batch",
    )
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
    parser.add_argument(
        "--profile-catalog",
        type=Path,
        default=harness.DEFAULT_PROFILE_CATALOG,
    )
    parser.add_argument("--scheduler-flag", action="append", default=[])
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.replay_plan is None and not args.workload:
        raise SystemExit("--workload is required unless --replay-plan is provided")
    if args.replay_plan is not None:
        replay_specs = harness.load_replay_plan(args.replay_plan)
        concurrency = len(replay_specs)
        duration_ms = args.duration_ms or harness.DEFAULT_WORKLOAD_DURATION_MS
        deadline_us = args.deadline_us or harness.default_deadline_us_for_duration(duration_ms)
        workload_label = f"azure-replay:{args.replay_plan.name}"
    else:
        workload_label = args.workload

    is_mixed = args.replay_plan is None and "," in args.workload
    if is_mixed:
        mix_specs = harness.parse_mix_spec(args.workload)
        concurrency = sum(spec.count for spec in mix_specs)
        duration_ms = args.duration_ms or harness.DEFAULT_WORKLOAD_DURATION_MS
        deadline_us = args.deadline_us or (duration_ms * 2 * 1000)
    elif args.replay_plan is None:
        spec = harness.workload_spec(args.workload)
        concurrency = args.concurrency
        duration_ms = args.duration_ms or spec.default_duration_ms
        deadline_us = harness.resolve_deadline_us(spec, duration_ms, args.deadline_us)
    scheduler_flags, metadata_mode, use_metadata = cosmos_config(
        args.config, deadline_us, duration_ms
    )
    if use_metadata:
        scheduler_flags.extend(["--profile-catalog", str(args.profile_catalog)])
    scheduler_flags.extend(args.scheduler_flag)

    config_root = args.out_dir or harness.default_results_dir(args.config)
    run_dir = config_root / harness.timestamped_run_id()
    run_dir.mkdir(parents=True, exist_ok=True)
    harness.write_manifest(
        run_dir,
        args.config,
        workload_label,
        concurrency,
        duration_ms,
        deadline_us,
        metadata_mode,
        scheduler_flags,
        args.slo_class,
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
        if args.replay_plan is not None:
            failures = harness.run_replay_invocations(
                run_dir,
                args.replay_plan,
                use_metadata,
                args.config,
                metadata_bridge_port,
            )
        else:
            failures = harness.run_invocations(
                run_dir,
                args.workload,
                concurrency,
                duration_ms,
                deadline_us,
                use_metadata,
                args.config,
                metadata_bridge_port,
                args.slo_class,
            )
    finally:
        harness.stop_process(stats_capture, signal.SIGINT)
        harness.stop_process(event_bridge, signal.SIGINT)
        harness.stop_process(scheduler, signal.SIGINT)

    harness.finalize_run(run_dir, config_root)
    harness.cleanup_benchmark_cgroups(run_dir)
    print(run_dir)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
