#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import signal
import subprocess
import time
from pathlib import Path
from typing import Any


DEFAULT_RUN_ROOT = Path("/users/Hanning/cosmos-semantic-benchmark/run-20260601-semantic")
DEFAULT_REPO = Path("/users/Hanning/COSMOS")
EVENT_BRIDGE_PORT = 9731
METADATA_PORT = 9732
SLO_METRICS = ("client", "openwhisk-duration", "action-elapsed", "kernel-elapsed")


ACTION_MAP = [
    ("cpu-spin-controllable", "cpu-spin-controllable"),
    ("noop-dispatch-controllable", "noop-dispatch-controllable"),
    ("passive-wait-controllable", "passive-wait-controllable"),
    ("db-network-wait-controllable", "db-network-wait-controllable"),
    ("local-file-io-controllable", "local-file-io-controllable"),
    ("memory-touch-controllable", "memory-touch-controllable"),
    ("cpu-loop-controllable", "cpu-loop-controllable"),
    ("memory-scan-controllable", "memory-scan-controllable"),
    ("storage-io-controllable", "storage-io-controllable"),
    ("network-transfer-controllable", "network-transfer-controllable"),
    ("balanced-pipeline-controllable", "balanced-pipeline-controllable"),
    ("mixed-pipeline-controllable", "mixed-pipeline-controllable"),
    ("workflow-fanout-controllable", "workflow-fanout-controllable"),
    ("010.sleep", "sebs_sleep"),
    ("030.clock-synchronization", "sebs_clock_synchronization"),
    ("040.server-reply", "sebs_server_reply"),
    ("110.dynamic-html", "sebs_dynamic_html"),
    ("120.uploader", "sebs_uploader"),
    ("130.crud-api", "sebs_crud_api"),
    ("210.thumbnailer", "sebs_thumbnailer"),
    ("220.video-processing", "sebs_video_processing"),
    ("311.compression", "sebs_compression"),
    ("411.image-recognition", "sebs_image_recognition"),
    ("501.graph-pagerank", "sebs_graph_pagerank"),
    ("502.graph-mst", "sebs_graph_mst"),
    ("503.graph-bfs", "sebs_graph_bfs"),
    ("504.dna-visualisation", "sebs_dna_visualisation"),
]


RUN_CONFIGS: dict[str, list[str] | None] = {
    "cfs-default": None,
    "sfs": ["--policy", "sfs"],
    "cosmos-full": ["--slo-target-us", "10000"],
    "cosmos-slack-only": [
        "--slo-target-us",
        "10000",
        "--disable-cgroup-actuator",
        "--disable-network-actuator",
        "--disable-phase-prediction",
        "--disable-warm-value",
    ],
    "cosmos-slack+xres": [
        "--slo-target-us",
        "10000",
        "--disable-phase-prediction",
        "--disable-warm-value",
    ],
    "cosmos-slack+xres+phase": [
        "--slo-target-us",
        "10000",
        "--disable-warm-value",
    ],
    "cosmos-no-phase-predict": [
        "--slo-target-us",
        "10000",
        "--disable-phase-prediction",
    ],
}


def terminate(process: subprocess.Popen[Any] | None, sig: int = signal.SIGINT) -> None:
    if process is None or process.poll() is not None:
        return
    process.send_signal(sig)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def wait_for_port(port: int, deadline_s: float = 30.0) -> None:
    import socket

    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1.0):
                return
        except OSError:
            time.sleep(0.2)
    raise TimeoutError(f"timed out waiting for port {port}")


def profile_catalog_path(repo: Path, run_root: Path) -> Path:
    generated = run_root / "replay" / "profiles.json"
    if generated.exists():
        return generated
    return repo / "benchmarks/configs/profile_catalog.json"


def start_scheduler(
    repo: Path,
    config: str,
    flags: list[str],
    run_dir: Path,
    profile_catalog: Path,
) -> subprocess.Popen[Any]:
    log = (run_dir / "scheduler.log").open("w", encoding="utf-8")
    cmd = [
        "sudo",
        str(repo / "target/release/cosmos"),
        *flags,
        "--metadata-port",
        str(METADATA_PORT),
        "--profile-catalog",
        str(profile_catalog),
    ]
    process = subprocess.Popen(cmd, cwd=repo, stdout=log, stderr=subprocess.STDOUT, text=True)
    time.sleep(1.0)
    if process.poll() is not None:
        raise RuntimeError(f"{config} scheduler exited early with {process.returncode}")
    wait_for_port(METADATA_PORT)
    return process


def start_event_bridge(repo: Path, run_dir: Path) -> subprocess.Popen[Any]:
    log = (run_dir / "event_bridge.log").open("w", encoding="utf-8")
    cmd = [
        str(repo / "target/release/cosmos-event-bridge"),
        "--port",
        str(EVENT_BRIDGE_PORT),
        "--metadata-port",
        str(METADATA_PORT),
    ]
    process = subprocess.Popen(cmd, cwd=repo, stdout=log, stderr=subprocess.STDOUT, text=True)
    time.sleep(0.5)
    if process.poll() is not None:
        raise RuntimeError(f"event bridge exited early with {process.returncode}")
    wait_for_port(EVENT_BRIDGE_PORT)
    return process


def replay_command(
    repo: Path,
    run_root: Path,
    out_dir: Path,
    with_metadata: bool,
    warmup_repetitions: int,
    warmup_target_ms: int,
    slo_calibration: Path | None,
    slo_deadline_multiplier: float,
    require_target_calibrated_slo: bool,
    slo_metric: str,
) -> list[str]:
    cmd = [
        "python3",
        "benchmarks/semantic_benchmark/run_openwhisk_semantic_replay.py",
        "--replay",
        str(run_root / "replay/replay.json"),
        "--out-dir",
        str(out_dir),
        "--wsk",
        "wsk",
        "--warmup-repetitions",
        str(warmup_repetitions),
        "--warmup-target-ms",
        str(warmup_target_ms),
        "--slo-deadline-multiplier",
        str(slo_deadline_multiplier),
        "--slo-metric",
        slo_metric,
    ]
    if slo_calibration is not None:
        cmd.extend(["--slo-calibration", str(slo_calibration)])
    if require_target_calibrated_slo:
        cmd.append("--require-target-calibrated-slo")
    if with_metadata:
        cmd.extend(
            [
                "--event-bridge-port",
                str(EVENT_BRIDGE_PORT),
                "--metadata-target",
                "openwhisk-container",
            ]
        )
    for key, value in ACTION_MAP:
        cmd.extend(["--action-map", f"{key}={value}"])
    return cmd


def collect_summary(run_root: Path, configs: list[str]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for config in configs:
        config_dir = run_root / "runs" / config
        summaries = sorted(
            config_dir.glob("openwhisk-semantic-*/summary.json"),
            key=lambda path: path.stat().st_mtime,
        )
        direct_summary = config_dir / "summary.json"
        if direct_summary.exists():
            summaries.append(direct_summary)
        summary_path = summaries[-1] if summaries else direct_summary
        if not summary_path.exists():
            rows.append({"config": config, "ok": False, "error": "missing summary"})
            continue
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        slo = summary.get("slo", {})
        slo_latency = slo.get("slo_latency_ms") or {}
        post = slo.get("post_submit_latency_ms") or {}
        reported = slo_latency or post
        rows.append(
            {
                "config": config,
                "ok": summary.get("ok"),
                "count": summary.get("count"),
                "failures": summary.get("failures"),
                "successes": slo.get("successes"),
                "slo_metric": slo.get("slo_metric"),
                "slo_success_rate": slo.get("slo_success_rate"),
                "slo_goodput_per_s": slo.get("slo_goodput_per_s"),
                "p50_ms": reported.get("p50"),
                "p95_ms": reported.get("p95"),
                "p99_ms": reported.get("p99"),
                "post_submit_p95_ms": post.get("p95"),
                "run_dir": str(summary_path.parent),
            }
        )
    output = {"rows": rows}
    (run_root / "semantic_scheduler_comparison.json").write_text(
        json.dumps(output, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with (run_root / "semantic_scheduler_comparison.csv").open(
        "w", encoding="utf-8", newline=""
    ) as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()) if rows else ["config"])
        writer.writeheader()
        writer.writerows(rows)
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=DEFAULT_REPO)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--configs", nargs="+", default=list(RUN_CONFIGS))
    parser.add_argument(
        "--warmup-repetitions",
        type=int,
        default=1,
        help="Pre-warm each replayed OpenWhisk action before measured replay.",
    )
    parser.add_argument("--warmup-target-ms", type=int, default=25)
    parser.add_argument(
        "--slo-calibration",
        type=Path,
        help="OpenWhisk action calibration JSON. Deadlines become multiplier * calibrated warm p99.",
    )
    parser.add_argument("--slo-deadline-multiplier", type=float, default=2.0)
    parser.add_argument(
        "--slo-metric",
        choices=SLO_METRICS,
        default="openwhisk-duration",
        help=(
            "Metric used for calibration p99 and replay SLO checks. Defaults to "
            "OpenWhisk activation duration to exclude client/platform submit overhead."
        ),
    )
    parser.add_argument(
        "--allow-uncalibrated-slo",
        action="store_true",
        help=(
            "Allow replay rows without an action,target_duration_ms calibration "
            "to fall back to replay deadlines. Disabled by default for fair SLOs."
        ),
    )
    parser.add_argument(
        "--skip-prepare-cgroup-root",
        action="store_true",
        help="Skip prepare_cosmos_cgroup_root.sh when it has already been run.",
    )
    args = parser.parse_args()

    for config in args.configs:
        if config not in RUN_CONFIGS:
            raise SystemExit(f"unknown config: {config}")

    if args.warmup_repetitions < 0:
        raise SystemExit("--warmup-repetitions must be non-negative")
    if args.warmup_target_ms <= 0:
        raise SystemExit("--warmup-target-ms must be positive")
    if args.slo_deadline_multiplier <= 0:
        raise SystemExit("--slo-deadline-multiplier must be positive")
    if args.slo_calibration is not None and not args.slo_calibration.exists():
        raise SystemExit(f"--slo-calibration does not exist: {args.slo_calibration}")
    profile_catalog = profile_catalog_path(args.repo, args.run_root)
    if not profile_catalog.exists():
        raise SystemExit(f"profile catalog does not exist: {profile_catalog}")

    if not args.skip_prepare_cgroup_root:
        subprocess.run(
            ["bash", str(args.repo / "benchmarks/scripts/prepare_cosmos_cgroup_root.sh")],
            cwd=args.repo,
            check=True,
        )

    status: dict[str, Any] = {}
    for config in args.configs:
        out_dir = args.run_root / "runs" / config
        out_dir.mkdir(parents=True, exist_ok=True)
        flags = RUN_CONFIGS[config]
        scheduler = None
        bridge = None
        try:
            if flags is not None:
                scheduler = start_scheduler(args.repo, config, flags, out_dir, profile_catalog)
                bridge = start_event_bridge(args.repo, out_dir)
            cmd = replay_command(
                args.repo,
                args.run_root,
                out_dir,
                flags is not None,
                args.warmup_repetitions,
                args.warmup_target_ms,
                args.slo_calibration,
                args.slo_deadline_multiplier,
                not args.allow_uncalibrated_slo,
                args.slo_metric,
            )
            with (out_dir / "replay.log").open("w", encoding="utf-8") as log:
                rc = subprocess.call(cmd, cwd=args.repo, stdout=log, stderr=subprocess.STDOUT)
            status[config] = {"returncode": rc, "run_dir": str(out_dir)}
        except Exception as exc:
            status[config] = {"returncode": -1, "run_dir": str(out_dir), "error": str(exc)}
            print(f"ERROR {config}: {exc}", flush=True)
        finally:
            terminate(bridge)
            terminate(scheduler)
            time.sleep(1.0)
        (args.run_root / "semantic_scheduler_status.json").write_text(
            json.dumps(status, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    collect_summary(args.run_root, args.configs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
