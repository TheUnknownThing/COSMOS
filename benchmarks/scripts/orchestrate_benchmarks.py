#!/usr/bin/env python3
"""
Comprehensive COSMOS Benchmarking Orchestrator

This script orchestrates:
1. SLO calibration on remote OpenWhisk testbed
2. Azure 2019 Direct trace benchmark
3. Azure 2019 SeBS trace benchmark
4. Local harness benchmarks with CFS, SFS, and COSMOS
5. Ablation experiments
6. Result aggregation and comparison
"""

import argparse
import csv
import json
import math
import os
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
BENCHMARK_ROOT = REPO_ROOT / "benchmarks"
AZURE_TRACE_DIR = BENCHMARK_ROOT / "azure_trace"
LOCAL_HARNESS_DIR = BENCHMARK_ROOT / "local_harness"

# Remote testbed configuration
REMOTE_HOST = "Hanning@amd252.utah.cloudlab.us"
REMOTE_COSMOS_DIR = "/users/Hanning/COSMOS"

# Azure dataset paths
AZURE_2021_TRACE = BENCHMARK_ROOT / "third_party/AzurePublicDataset/data/AzureFunctionsInvocationTraceForTwoWeeksJan2021.rar"
AZURE_2019_DATASET = BENCHMARK_ROOT / "third_party/AzurePublicDataset/data/azurefunctions-dataset2019"

# Default benchmark parameters
DEFAULT_WINDOW_MS = 5400000  # 90 minutes
DEFAULT_SCALE = 45
DEFAULT_SLO_MULTIPLIER = 2.0
DEFAULT_CALIBRATION_REPS = 5
DEFAULT_OPENWHISK_LIMIT = 1000
DEFAULT_OPENWHISK_MAX_INFLIGHT = 0

# Workload configurations
WORKLOADS = [
    "cpu_burst",
    "sleep_short",
    "io_mixed",
    "memory_heavy",
    "network_heavy",
    "compression_mixed",
    "graph_bfs",
]

CONCURRENCY_LEVELS = {
    "cpu_burst": [64, 128],
    "sleep_short": [64, 128],
    "io_mixed": [32, 64],
    "memory_heavy": [32, 64],
    "network_heavy": [32, 64],
    "compression_mixed": [64, 128],
    "graph_bfs": [32, 64],
}

SCHEDULER_CONFIGS = ["cfs-default", "sfs", "cosmos-heuristic", "cosmos-full"]
COSCHEDULING_CONFIGS = ["cfs-default", "cosmos-full"]

COSCHEDULING_SCENARIOS = [
    (
        "lc-cpu_vs_batch-cpu_c96",
        "cpu_burst:24(slo=0;duration_ms=125;deadline_ms=250),"
        "cpu_burst:72(slo=2;duration_ms=500;deadline_ms=2000)",
    ),
    (
        "lc-cpu_vs_batch-memory_c96",
        "cpu_burst:24(slo=0;duration_ms=125;deadline_ms=250),"
        "memory_heavy:72(slo=2;duration_ms=500;deadline_ms=2000)",
    ),
    (
        "lc-network_vs_batch-cpu_c96",
        "network_heavy:24(slo=0;duration_ms=125;deadline_ms=250),"
        "cpu_burst:72(slo=2;duration_ms=500;deadline_ms=2000)",
    ),
    (
        "lc-io_vs_batch-cpu_c96",
        "io_mixed:24(slo=0;duration_ms=125;deadline_ms=300),"
        "cpu_burst:72(slo=2;duration_ms=500;deadline_ms=2000)",
    ),
    (
        "lc-sleep_vs_batch-memory_c96",
        "sleep_short:24(slo=0;duration_ms=50;deadline_ms=150),"
        "memory_heavy:72(slo=2;duration_ms=500;deadline_ms=2000)",
    ),
    (
        "lc-standard-batch_three-class_c96",
        "network_heavy:24(slo=0;duration_ms=125;deadline_ms=250),"
        "cpu_burst:24(slo=1;duration_ms=250;deadline_ms=750),"
        "compression_mixed:48(slo=2;duration_ms=500;deadline_ms=2000)",
    ),
]

# OpenWhisk action mappings
OPENWHISK_ACTION_MAP = {
    "cpu_burst": "ow_cpu_burst",
    "pipeline": "ow_pipeline",
    "memory_heavy": "ow_memory_heavy",
    "io_mixed": "ow_io_mixed",
    "network_heavy": "ow_network_heavy",
    "010.sleep": "sebs_sleep",
    "110.dynamic-html": "sebs_dynamic_html",
    "120.uploader": "sebs_uploader",
    "210.thumbnailer": "sebs_thumbnailer",
    "220.video-processing": "sebs_video_processing",
    "311.compression": "sebs_compression",
    "411.image-recognition": "sebs_image_recognition",
    "503.graph-bfs": "sebs_graph_bfs",
}


def pct_delta(candidate: float | None, baseline: float | None) -> float | None:
    if candidate is None or baseline is None or baseline == 0:
        return None
    return ((candidate - baseline) / baseline) * 100.0


def fmt_float(value: object, digits: int = 1) -> str:
    if value is None:
        return "n/a"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if not math.isfinite(numeric):
        return "n/a"
    return f"{numeric:.{digits}f}"


def fmt_pct(value: object, digits: int = 1) -> str:
    formatted = fmt_float(value, digits)
    return "n/a" if formatted == "n/a" else f"{formatted}%"


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


class BenchmarkOrchestrator:
    def __init__(
        self,
        results_dir: Path,
        skip_remote: bool = False,
        skip_local: bool = False,
        openwhisk_limit: int = DEFAULT_OPENWHISK_LIMIT,
        openwhisk_max_inflight: int = DEFAULT_OPENWHISK_MAX_INFLIGHT,
    ):
        self.results_dir = results_dir
        self.skip_remote = skip_remote
        self.skip_local = skip_local
        self.openwhisk_limit = openwhisk_limit
        self.openwhisk_max_inflight = openwhisk_max_inflight
        self.log_file = results_dir / "orchestrator.log"
        self.results_dir.mkdir(parents=True, exist_ok=True)

        # Open log file
        self.log_fp = open(self.log_file, "w", buffering=1)

    def log(self, message: str):
        """Log message to both console and file"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"[{timestamp}] {message}"
        print(log_line)
        self.log_fp.write(log_line + "\n")

    def run_command(self, cmd: List[str], cwd: Optional[Path] = None, check: bool = True) -> subprocess.CompletedProcess:
        """Run a command and log output"""
        self.log(f"Running: {' '.join(str(c) for c in cmd)}")
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.stdout:
            self.log(f"STDOUT:\n{result.stdout}")
        if result.stderr:
            self.log(f"STDERR:\n{result.stderr}")

        if check and result.returncode != 0:
            self.log(f"ERROR: Command failed with return code {result.returncode}")
            raise subprocess.CalledProcessError(result.returncode, cmd)

        return result

    def build_traces(self) -> tuple[dict[str, Path], None]:
        """Return trace artifacts for aggregation.

        The old Azure/OpenWhisk builders were removed from this branch. CPU
        top-functions replay now uses checked-in configs and pools under
        benchmarks/azure_trace plus the replay_top_functions/run_pool scripts.
        """
        self.log("=" * 60)
        self.log("Skipping deprecated Azure/OpenWhisk trace builders")
        self.log("=" * 60)
        return {}, None

    def check_remote_openwhisk(self) -> bool:
        """Check if OpenWhisk is accessible on remote testbed"""
        self.log("Checking remote OpenWhisk availability...")
        result = self.run_command(
            ["ssh", REMOTE_HOST, "wsk action list"],
            check=False,
        )
        return result.returncode == 0

    def sync_to_remote(self):
        """Sync COSMOS to remote testbed"""
        self.log("Syncing COSMOS to remote testbed...")
        self.run_command([
            "rsync", "-avz",
            "--exclude", "target",
            "--exclude", ".git",
            "--exclude", "benchmarks/results",
            f"{REPO_ROOT}/",
            f"{REMOTE_HOST}:{REMOTE_COSMOS_DIR}/",
        ])

        self.log("Building COSMOS on remote testbed...")
        self.run_command([
            "ssh", REMOTE_HOST,
            f"cd {REMOTE_COSMOS_DIR} && cargo build --release --workspace",
        ])

    def run_slo_calibration(self) -> Path:
        raise RuntimeError("remote OpenWhisk SLO calibration is deprecated in this branch")

    def run_azure_trace_benchmark(self, trace_dir: Path, calibration_file: Path, benchmark_name: str):
        raise RuntimeError("remote Azure/OpenWhisk replay is deprecated in this branch")

    def run_local_harness_benchmarks(self):
        """Run local harness benchmarks with all scheduler configurations"""
        self.log("=" * 60)
        self.log("Running local harness benchmarks")
        self.log("=" * 60)

        scheduler_bin = REPO_ROOT / "target/release/cosmos"
        if not scheduler_bin.exists():
            self.log("ERROR: COSMOS scheduler binary not found. Building...")
            self.run_command(["cargo", "build", "--release", "--workspace"], cwd=REPO_ROOT)

        for workload in WORKLOADS:
            for concurrency in CONCURRENCY_LEVELS.get(workload, [64]):
                for config in SCHEDULER_CONFIGS:
                    self.log(f"Running {config} / {workload} / concurrency={concurrency}")

                    out_dir = self.results_dir / "local_harness" / config / workload / f"c{concurrency}"
                    out_dir.mkdir(parents=True, exist_ok=True)

                    cmd = [
                        "sudo", "python3",
                        str(LOCAL_HARNESS_DIR / "burst_benchmark.py"),
                        "--config", config,
                        "--workload", workload,
                        "--concurrency", str(concurrency),
                        "--duration-ms", "250",
                        "--out-dir", str(out_dir),
                    ]

                    if "cosmos" in config:
                        cmd.extend(["--scheduler-bin", str(scheduler_bin)])

                    try:
                        self.run_command(cmd)
                    except subprocess.CalledProcessError as e:
                        self.log(f"WARNING: Benchmark failed: {e}")
                        continue

        self.log("Local harness benchmarks complete!")

    def append_coscheduling_summary(
        self,
        rows: list[dict],
        scenario_name: str,
        config: str,
        out_dir: Path,
    ):
        summary_path = out_dir / "latest" / "summary.json"
        if not summary_path.exists():
            self.log(f"WARNING: Missing co-scheduling summary: {summary_path}")
            return

        summary = json.loads(summary_path.read_text())
        scheduler_total = summary.get("scheduler", {}).get("total", {})
        scheduler_peak = summary.get("scheduler", {}).get("peak", {})
        latency = summary.get("latency", {})
        scheduler_stall_signals = (
            (scheduler_total.get("nr_failed_dispatches") or 0)
            + (scheduler_total.get("nr_sched_congested") or 0)
        )
        for slo_class, slo_summary in sorted(summary.get("per_slo_class", {}).items()):
            rows.append({
                "scenario": scenario_name,
                "config": config,
                "slo_class": slo_class,
                "count": slo_summary.get("count"),
                "successes": slo_summary.get("successes"),
                "failures": slo_summary.get("failures"),
                "p50_ms": slo_summary.get("p50_ms"),
                "p95_ms": slo_summary.get("p95_ms"),
                "p99_ms": slo_summary.get("p99_ms"),
                "slo_violations": slo_summary.get("client_slo_violations"),
                "goodput_per_s": slo_summary.get("goodput_per_s"),
                "mean_slowdown_vs_latency_critical": slo_summary.get(
                    "mean_slowdown_vs_latency_critical"
                ),
                "batch_slowdown_vs_latency_critical": slo_summary.get(
                    "batch_slowdown_vs_latency_critical"
                ),
                "overall_p99_ms": latency.get("p99_ms"),
                "scheduler_slo_boosted": scheduler_total.get("nr_slo_boosted"),
                "scheduler_pool_latency": scheduler_total.get("nr_pool_latency"),
                "scheduler_pool_batch": scheduler_total.get("nr_pool_batch"),
                "scheduler_pool_migrations": scheduler_total.get("nr_pool_migrations"),
                "scheduler_pool_overflow": scheduler_total.get("nr_pool_overflow"),
                "scheduler_failed_dispatches": scheduler_total.get("nr_failed_dispatches"),
                "scheduler_congested": scheduler_total.get("nr_sched_congested"),
                "scheduler_stall_signals": scheduler_stall_signals,
                "scheduler_peak_queued": scheduler_peak.get("nr_queued"),
                "scheduler_peak_scheduled": scheduler_peak.get("nr_scheduled"),
                "run_dir": str(summary_path.parent),
            })

    def write_coscheduling_summary(self, rows: list[dict]):
        if not rows:
            return
        output = self.results_dir / "local_harness" / "coscheduling_summary.csv"
        output.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = list(rows[0].keys())
        with output.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        self.log(f"Mixed co-scheduling summary written: {output}")

    def write_coscheduling_status(self, failures: list[dict]):
        output = self.results_dir / "local_harness" / "coscheduling_status.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps({"failures": failures}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def run_coscheduling_benchmarks(self):
        """Run first-class mixed latency-critical plus batch co-scheduling scenarios."""
        self.log("=" * 60)
        self.log("Running mixed co-scheduling benchmarks")
        self.log("=" * 60)

        scheduler_bin = REPO_ROOT / "target/release/cosmos"
        if not scheduler_bin.exists():
            self.log("ERROR: COSMOS scheduler binary not found. Building...")
            self.run_command(["cargo", "build", "--release", "--workspace"], cwd=REPO_ROOT)

        summary_rows: list[dict] = []
        failures: list[dict] = []
        for scenario_name, mix in COSCHEDULING_SCENARIOS:
            for config in COSCHEDULING_CONFIGS:
                self.log(f"Running {config} / {scenario_name}")
                out_dir = (
                    self.results_dir
                    / "local_harness"
                    / "coscheduling"
                    / scenario_name
                    / config
                )
                out_dir.mkdir(parents=True, exist_ok=True)

                cmd = [
                    "sudo", "python3",
                    str(LOCAL_HARNESS_DIR / "burst_benchmark.py"),
                    "--config", config,
                    "--mix", mix,
                    "--out-dir", str(out_dir),
                ]

                if "cosmos" in config:
                    cmd.extend(["--scheduler-bin", str(scheduler_bin)])

                try:
                    self.run_command(cmd)
                except subprocess.CalledProcessError as e:
                    self.log(f"WARNING: Co-scheduling benchmark failed: {e}")
                    failures.append(
                        {
                            "scenario": scenario_name,
                            "config": config,
                            "returncode": e.returncode,
                            "out_dir": str(out_dir),
                        }
                    )
                    continue
                self.append_coscheduling_summary(summary_rows, scenario_name, config, out_dir)

        self.write_coscheduling_summary(summary_rows)
        self.write_coscheduling_status(failures)
        self.log("Mixed co-scheduling benchmarks complete!")

    def run_ablation_experiments(self):
        """Run ablation experiments"""
        self.log("=" * 60)
        self.log("Running ablation experiments")
        self.log("=" * 60)

        # Ablation configurations
        ablation_configs = [
            ("cosmos-no-coscheduling", "COSMOS without co-scheduling"),
            ("cosmos-no-slo", "COSMOS without SLO awareness"),
            ("cosmos-no-dsq", "COSMOS without DSQ"),
        ]

        scheduler_bin = REPO_ROOT / "target/release/cosmos"

        # Run a subset of workloads for ablation
        test_workloads = ["cpu_burst", "io_mixed", "network_heavy"]

        for config_name, description in ablation_configs:
            self.log(f"Running ablation: {description}")

            for workload in test_workloads:
                concurrency = CONCURRENCY_LEVELS.get(workload, [64])[0]

                out_dir = self.results_dir / "ablation" / config_name / workload / f"c{concurrency}"
                out_dir.mkdir(parents=True, exist_ok=True)

                cmd = [
                    "sudo", "python3",
                    str(LOCAL_HARNESS_DIR / "burst_benchmark.py"),
                    "--config", config_name,
                    "--workload", workload,
                    "--concurrency", str(concurrency),
                    "--duration-ms", "250",
                    "--out-dir", str(out_dir),
                    "--scheduler-bin", str(scheduler_bin),
                ]

                try:
                    self.run_command(cmd, check=False)
                except Exception as e:
                    self.log(f"WARNING: Ablation experiment failed: {e}")
                    continue

        self.log("Ablation experiments complete!")

    def replay_artifact_summary(self, trace_dir: Path) -> dict:
        replay_path = trace_dir / "replay.json"
        if not replay_path.exists():
            return {"ok": False, "path": str(replay_path)}
        replay = read_json(replay_path)
        invocations = replay.get("invocations") or []
        workloads = Counter(str(item.get("workload") or "unknown") for item in invocations)
        scheduled_ms = [float(item.get("at_ms") or 0.0) for item in invocations]
        max_at_ms = max(scheduled_ms, default=0.0)
        min_at_ms = min(scheduled_ms, default=0.0)
        span_s = max(0.0, (max_at_ms - min_at_ms) / 1000.0)
        buckets = Counter(int(ms // 1000.0) for ms in scheduled_ms)
        return {
            "ok": True,
            "path": str(replay_path),
            "schema": replay.get("schema"),
            "invocations": len(invocations),
            "max_at_ms": max_at_ms,
            "scheduled_span_s": span_s,
            "average_arrival_rate_per_s": (len(invocations) / span_s) if span_s > 0 else None,
            "peak_1s_arrival_rate": max(buckets.values()) if buckets else 0,
            "arrival_mode": replay.get("window", {}).get("arrival_mode"),
            "workload_mix_mode": replay.get("window", {}).get("workload_mix_mode"),
            "deadline_mode": replay.get("window", {}).get("deadline_mode"),
            "workloads": dict(sorted(workloads.items())),
        }

    def collect_openwhisk_summaries(self) -> dict:
        openwhisk: dict[str, dict] = {}
        for summary_path in sorted(self.results_dir.glob("openwhisk-*/summary.json")):
            name = summary_path.parent.name.removeprefix("openwhisk-")
            summary = read_json(summary_path)
            slo = summary.get("slo", {})
            openwhisk[name] = {
                "count": summary.get("count"),
                "failures": summary.get("failures"),
                "ok": summary.get("ok"),
                "run_dir": str(summary_path.parent),
                "slo": slo,
            }
        return openwhisk

    def local_summary_kind(self, summary_path: Path, summary: dict) -> str:
        parts = set(summary_path.relative_to(self.results_dir).parts)
        if any("coscheduling" in part for part in parts):
            return "coscheduling"
        if "ablation" in parts:
            return "ablation"
        if "comparison" in parts or summary.get("config") in SCHEDULER_CONFIGS:
            return "comparison"
        return "local"

    def collect_local_harness_rows(self) -> list[dict]:
        local_root = self.results_dir / "local_harness"
        if not local_root.exists():
            return []

        rows: list[dict] = []
        seen: set[Path] = set()
        for summary_path in sorted(local_root.rglob("summary.json")):
            resolved = summary_path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            summary = read_json(summary_path)
            latency = summary.get("latency", {})
            load = summary.get("load", {})
            scheduler = summary.get("scheduler", {})
            scheduler_total = scheduler.get("total", {})
            count = latency.get("count") or 0
            successes = latency.get("successes") or 0
            violations = latency.get("client_slo_violations") or 0
            rows.append(
                {
                    "kind": self.local_summary_kind(summary_path, summary),
                    "config": summary.get("config"),
                    "workload": summary.get("workload"),
                    "concurrency": summary.get("concurrency"),
                    "duration_ms": summary.get("duration_ms"),
                    "deadline_us": summary.get("deadline_us"),
                    "successes": latency.get("successes"),
                    "failures": latency.get("failures"),
                    "p50_ms": latency.get("p50_ms"),
                    "p95_ms": latency.get("p95_ms"),
                    "p99_ms": latency.get("p99_ms"),
                    "mean_ms": latency.get("mean_ms"),
                    "slo_violations": violations,
                    "slo_success_rate": (
                        (successes - violations)
                        / count
                        if count
                        else None
                    ),
                    "load_ratio": load.get("load_ratio"),
                    "scheduler_slo_violations": scheduler_total.get("nr_slo_violations"),
                    "scheduler_slo_boosted": scheduler_total.get("nr_slo_boosted"),
                    "scheduler_failed_dispatches": scheduler_total.get("nr_failed_dispatches"),
                    "run_dir": str(summary_path.parent),
                }
            )
        return rows

    def write_local_harness_aggregate(self, rows: list[dict]) -> Path | None:
        if not rows:
            return None
        output = self.results_dir / "local_harness_aggregate.csv"
        fieldnames = list(rows[0].keys())
        with output.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        self.log(f"Local harness aggregate written: {output}")
        return output

    def comparison_table(self, rows: list[dict]) -> list[dict]:
        grouped: dict[tuple[str, int], dict[str, dict]] = {}
        for row in rows:
            if row.get("kind") != "comparison":
                continue
            workload = str(row.get("workload") or "")
            try:
                concurrency = int(row.get("concurrency"))
            except (TypeError, ValueError):
                continue
            grouped.setdefault((workload, concurrency), {})[str(row.get("config"))] = row

        table: list[dict] = []
        for (workload, concurrency), configs in sorted(grouped.items()):
            cfs = configs.get("cfs-default", {})
            sfs = configs.get("sfs", {})
            heuristic = configs.get("cosmos-heuristic", {})
            full = configs.get("cosmos-full", {})
            table.append(
                {
                    "workload": workload,
                    "concurrency": concurrency,
                    "cfs_p99": cfs.get("p99_ms"),
                    "sfs_p99": sfs.get("p99_ms"),
                    "heur_p99": heuristic.get("p99_ms"),
                    "full_p99": full.get("p99_ms"),
                    "full_vs_cfs_p99_pct": pct_delta(full.get("p99_ms"), cfs.get("p99_ms")),
                    "full_vs_sfs_p99_pct": pct_delta(full.get("p99_ms"), sfs.get("p99_ms")),
                    "cfs_viol": cfs.get("slo_violations"),
                    "sfs_viol": sfs.get("slo_violations"),
                    "heur_viol": heuristic.get("slo_violations"),
                    "full_viol": full.get("slo_violations"),
                }
            )
        return table

    def ablation_table(self, rows: list[dict]) -> list[dict]:
        full_by_workload: dict[tuple[str, int], dict] = {}
        for row in rows:
            if row.get("kind") != "comparison" or row.get("config") != "cosmos-full":
                continue
            try:
                key = (str(row.get("workload")), int(row.get("concurrency")))
            except (TypeError, ValueError):
                continue
            full_by_workload[key] = row

        table: list[dict] = []
        for row in rows:
            if row.get("kind") != "ablation":
                continue
            try:
                key = (str(row.get("workload")), int(row.get("concurrency")))
            except (TypeError, ValueError):
                continue
            full = full_by_workload.get(key, {})
            table.append(
                {
                    "workload": row.get("workload"),
                    "concurrency": row.get("concurrency"),
                    "config": row.get("config"),
                    "p99_ms": row.get("p99_ms"),
                    "violations": row.get("slo_violations"),
                    "p99_vs_full_pct": pct_delta(row.get("p99_ms"), full.get("p99_ms")),
                }
            )
        return table

    def coscheduling_summary_path(self) -> Path | None:
        candidates = [
            self.results_dir / "local_harness" / "coscheduling_summary.csv",
            self.results_dir
            / "local_harness"
            / "coscheduling_investigation"
            / "coscheduling_summary.csv",
        ]
        for output in candidates:
            if output.exists():
                return output
        return None

    def coscheduling_rows(self) -> list[dict]:
        output = self.coscheduling_summary_path()
        if output is None:
            return []
        with output.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
        deduped: dict[tuple[str, str, str], dict] = {}
        for row in rows:
            key = (
                str(row.get("scenario")),
                str(row.get("config")),
                str(row.get("slo_class")),
            )
            current = deduped.get(key)
            if current is None or str(row.get("run_dir", "")).endswith("/latest"):
                deduped[key] = row
        return list(deduped.values())

    def aggregate_results(self, direct_dirs: dict[str, Path] | None = None, sebs_dir: Path | None = None) -> dict:
        local_rows = self.collect_local_harness_rows()
        self.write_local_harness_aggregate(local_rows)
        comparison = self.comparison_table(local_rows)
        ablation = self.ablation_table(local_rows)

        replay_artifacts: dict[str, dict] = {}
        if direct_dirs:
            for name, trace_dir in sorted(direct_dirs.items()):
                replay_artifacts[name] = self.replay_artifact_summary(trace_dir)
        else:
            for trace_dir in sorted(self.results_dir.glob("azure-2019-direct-*")):
                replay_artifacts[trace_dir.name] = self.replay_artifact_summary(trace_dir)
        if sebs_dir is not None:
            replay_artifacts["azure-2019-sebs"] = self.replay_artifact_summary(sebs_dir)
        elif (self.results_dir / "azure-2019-sebs").exists():
            replay_artifacts["azure-2019-sebs"] = self.replay_artifact_summary(
                self.results_dir / "azure-2019-sebs"
            )
        else:
            for trace_dir in sorted(self.results_dir.glob("azure-2019-sebs*")):
                replay_artifacts[trace_dir.name] = self.replay_artifact_summary(trace_dir)

        local_harness = {
            "successful_summary_count": len(local_rows),
            "comparison_summary_count": sum(1 for row in local_rows if row.get("kind") == "comparison"),
            "ablation_summary_count": sum(1 for row in local_rows if row.get("kind") == "ablation"),
            "coscheduling_summary_count": len(self.coscheduling_rows()),
            "comparison_table": comparison,
            "ablation_table": ablation,
        }
        for name in ("batch_status", "retry_status"):
            status_path = self.results_dir / "local_harness" / f"{name}.json"
            if status_path.exists():
                local_harness[name] = read_json(status_path)
        coscheduling_status_path = self.results_dir / "local_harness" / "coscheduling_status.json"
        if coscheduling_status_path.exists():
            local_harness["coscheduling_status"] = read_json(coscheduling_status_path)

        existing_aggregate_path = self.results_dir / "aggregate_summary.json"
        existing_aggregate = (
            read_json(existing_aggregate_path) if existing_aggregate_path.exists() else {}
        )
        aggregate = {
            "result_dir": str(self.results_dir),
            "generated_at_epoch": time.time(),
            "openwhisk": self.collect_openwhisk_summaries(),
            "replay_artifacts": replay_artifacts,
            "local_harness": local_harness,
        }
        if "trace_volume" in existing_aggregate:
            aggregate["trace_volume"] = existing_aggregate["trace_volume"]
        output = existing_aggregate_path
        output.write_text(json.dumps(aggregate, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self.log(f"Aggregate summary written: {output}")
        return aggregate

    def generate_report(self, aggregate: dict | None = None):
        """Generate comprehensive benchmark report"""
        self.log("=" * 60)
        self.log("Generating comprehensive report")
        self.log("=" * 60)

        if aggregate is None:
            aggregate_path = self.results_dir / "aggregate_summary.json"
            aggregate = read_json(aggregate_path) if aggregate_path.exists() else self.aggregate_results()

        report_file = self.results_dir / "COMPREHENSIVE_REPORT.md"

        with open(report_file, "w") as f:
            f.write("# COSMOS Comprehensive Benchmark Report\n\n")
            f.write(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("## Benchmark Configuration\n\n")
            f.write(f"- Window: {DEFAULT_WINDOW_MS}ms ({DEFAULT_WINDOW_MS/60000:.0f} minutes)\n")
            f.write(f"- Scale: {DEFAULT_SCALE}\n")
            f.write(f"- SLO Multiplier: {DEFAULT_SLO_MULTIPLIER}x\n")
            f.write(f"- Calibration Repetitions: {DEFAULT_CALIBRATION_REPS}\n\n")
            f.write("## Benchmarks Run\n\n")

            if not self.skip_remote:
                f.write("### Remote OpenWhisk Benchmarks\n\n")
                f.write("- SLO Calibration\n")
                f.write("- Azure 2019 Direct Trace variants\n")
                f.write("- Azure 2019 SeBS Trace\n\n")

            if not self.skip_local:
                f.write("### Local Harness Benchmarks\n\n")
                f.write(f"- Schedulers: {', '.join(SCHEDULER_CONFIGS)}\n")
                f.write(f"- Workloads: {', '.join(WORKLOADS)}\n")
                f.write(f"- Mixed co-scheduling scenarios: {len(COSCHEDULING_SCENARIOS)}\n")
                f.write("- Ablation Experiments\n\n")

            f.write("## Results Location\n\n")
            f.write(f"All results are saved in: `{self.results_dir}`\n\n")

            openwhisk = aggregate.get("openwhisk", {})
            if openwhisk:
                replay_artifacts = aggregate.get("replay_artifacts", {})
                f.write("## Azure/OpenWhisk Results\n\n")
                f.write(
                    "| Replay | Invocations | Successes | Failures | Arrival avg/s | Peak 1s | "
                    "SLO success | SLO goodput/s | Submit lag p99 | Post-submit p99 | Impossible deadlines |\n"
                )
                f.write("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
                for name, summary in sorted(openwhisk.items()):
                    slo = summary.get("slo", {})
                    arrival = slo.get("arrival", {})
                    replay = replay_artifacts.get(name, {})
                    submit_lag = slo.get("submit_lag_ms", {})
                    post_submit = slo.get("post_submit_latency_ms", {})
                    target_deadline = slo.get("target_duration_vs_deadline", {})
                    arrival_avg = arrival.get("average_arrival_rate_per_s")
                    if arrival_avg is None:
                        arrival_avg = replay.get("average_arrival_rate_per_s")
                    peak_1s = arrival.get("peak_1s_arrival_rate")
                    if peak_1s is None:
                        peak_1s = replay.get("peak_1s_arrival_rate")
                    f.write(
                        f"| {name} | {summary.get('count', 'n/a')} | {slo.get('successes', 'n/a')} | "
                        f"{summary.get('failures', 'n/a')} | {fmt_float(arrival_avg, 2)} | "
                        f"{peak_1s if peak_1s is not None else 'n/a'} | "
                        f"{fmt_pct(slo.get('slo_success_rate') * 100 if slo.get('slo_success_rate') is not None else None, 1)} | "
                        f"{fmt_float(slo.get('slo_goodput_per_s'), 2)} | "
                        f"{fmt_float(submit_lag.get('p99'), 2)} | "
                        f"{fmt_float(post_submit.get('p99'), 2)} | "
                        f"{target_deadline.get('impossible_deadline_count', 'n/a')} |\n"
                    )
                f.write("\nReplay workload mixes:\n\n")
                for name, replay in sorted(replay_artifacts.items()):
                    workloads = replay.get("workloads") or {}
                    if not workloads:
                        continue
                    mix = ", ".join(f"{workload}={count}" for workload, count in workloads.items())
                    f.write(f"- {name}: {mix}\n")
                f.write("\nOpenWhisk action mixes:\n\n")
                for name, summary in sorted(openwhisk.items()):
                    action_mix = summary.get("slo", {}).get("action_mix") or {}
                    if not action_mix:
                        continue
                    mix = ", ".join(f"{action}={count}" for action, count in action_mix.items())
                    f.write(f"- {name}: {mix}\n")
                f.write(
                    "\nOpenWhisk numbers are end-to-end platform replay results. Interpret them with "
                    "arrival burstiness, action mix, submit lag, and post-submit latency rather than as "
                    "a scheduler-only throughput headline.\n\n"
                )
                f.write(
                    "Rows with `rich` in the name are controlled bounded reruns with the current "
                    "replay summarizer; capped replay artifacts are used to keep the OpenWhisk run "
                    "inside practical concurrency and wall-clock limits.\n\n"
                )

                workload_rows = []
                for name, summary in sorted(openwhisk.items()):
                    per_workload = summary.get("slo", {}).get("per_workload") or {}
                    for workload, workload_summary in sorted(per_workload.items()):
                        workload_rows.append((name, workload, workload_summary))
                if workload_rows:
                    f.write("### OpenWhisk Per-Workload SLO\n\n")
                    f.write(
                        "| Replay | Workload | Attempts | Successes | SLO success | "
                        "SLO goodput/s | p99 latency ms |\n"
                    )
                    f.write("| --- | --- | ---: | ---: | ---: | ---: | ---: |\n")
                    for name, workload, workload_summary in workload_rows:
                        latency = workload_summary.get("latency_ms", {})
                        success_rate = workload_summary.get("slo_success_rate")
                        f.write(
                            f"| {name} | {workload} | {workload_summary.get('attempts', 'n/a')} | "
                            f"{workload_summary.get('successes', 'n/a')} | "
                            f"{fmt_pct(success_rate * 100 if success_rate is not None else None)} | "
                            f"{fmt_float(workload_summary.get('slo_goodput_per_s'), 2)} | "
                            f"{fmt_float(latency.get('p99'), 1)} |\n"
                        )
                    f.write("\n")

            comparison = aggregate.get("local_harness", {}).get("comparison_table", [])
            if comparison:
                complete = [
                    row for row in comparison
                    if row.get("full_vs_cfs_p99_pct") is not None
                ]
                better = sum(1 for row in complete if row["full_vs_cfs_p99_pct"] < 0)
                mean_delta = (
                    sum(row["full_vs_cfs_p99_pct"] for row in complete) / len(complete)
                    if complete else None
                )
                f.write("## Local Harness Comparison\n\n")
                f.write(
                    f"Successful comparison summaries: {aggregate['local_harness'].get('comparison_summary_count', 0)}. "
                    f"COSMOS-full p99 versus CFS: {better}/{len(complete)} lower"
                )
                if mean_delta is not None:
                    f.write(f", mean delta {fmt_pct(mean_delta)}")
                f.write(".\n\n")
                f.write(
                    "| Workload | Conc | CFS p99 | SFS p99 | Heur p99 | Full p99 | "
                    "Full vs CFS | Full vs SFS | CFS viol | SFS viol | Heur viol | Full viol |\n"
                )
                f.write("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
                for row in comparison:
                    f.write(
                        f"| {row['workload']} | {row['concurrency']} | {fmt_float(row.get('cfs_p99'))} | "
                        f"{fmt_float(row.get('sfs_p99'))} | {fmt_float(row.get('heur_p99'))} | "
                        f"{fmt_float(row.get('full_p99'))} | {fmt_pct(row.get('full_vs_cfs_p99_pct'))} | "
                        f"{fmt_pct(row.get('full_vs_sfs_p99_pct'))} | {row.get('cfs_viol', 'n/a')} | "
                        f"{row.get('sfs_viol', 'n/a')} | {row.get('heur_viol', 'n/a')} | {row.get('full_viol', 'n/a')} |\n"
                    )
                f.write("\n")

            ablation = aggregate.get("local_harness", {}).get("ablation_table", [])
            if ablation:
                f.write("## Ablations\n\n")
                f.write(
                    f"Successful ablation summaries: {aggregate['local_harness'].get('ablation_summary_count', 0)}. "
                    "`p99 vs full` compares each ablation against the matching `cosmos-full` comparison run when available.\n\n"
                )
                f.write("| Workload | Conc | Config | p99 ms | SLO viol | p99 vs full |\n")
                f.write("| --- | ---: | --- | ---: | ---: | ---: |\n")
                for row in ablation:
                    f.write(
                        f"| {row.get('workload')} | {row.get('concurrency')} | {row.get('config')} | "
                        f"{fmt_float(row.get('p99_ms'))} | {row.get('violations', 'n/a')} | "
                        f"{fmt_pct(row.get('p99_vs_full_pct'))} |\n"
                    )
                f.write("\n")

            coscheduling = self.coscheduling_rows()
            if coscheduling:
                f.write("## Mixed Co-Scheduling\n\n")
                f.write(
                    "| Scenario | Config | SLO class | p50 ms | p95 ms | p99 ms | SLO violations | "
                    "Goodput/s | Batch slowdown | SLO boosts | Pool latency | Pool batch | "
                    "Migrations | Scheduler stalls |\n"
                )
                f.write(
                    "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | "
                    "---: | ---: | ---: | ---: |\n"
                )
                for row in coscheduling:
                    f.write(
                        f"| {row.get('scenario')} | {row.get('config')} | {row.get('slo_class')} | "
                        f"{fmt_float(row.get('p50_ms'))} | "
                        f"{fmt_float(row.get('p95_ms'))} | "
                        f"{fmt_float(row.get('p99_ms'))} | {row.get('slo_violations') or row.get('violations') or 'n/a'} | "
                        f"{fmt_float(row.get('goodput_per_s'), 2)} | "
                        f"{fmt_float(row.get('batch_slowdown_vs_latency_critical'), 2)} | "
                        f"{row.get('scheduler_slo_boosted', 'n/a')} | "
                        f"{row.get('scheduler_pool_latency', 'n/a')} | "
                        f"{row.get('scheduler_pool_batch', 'n/a')} | "
                        f"{row.get('scheduler_pool_migrations', 'n/a')} | "
                        f"{row.get('scheduler_stall_signals', 'n/a')} |\n"
                    )
                f.write("\n")

            retry_failures = (
                aggregate.get("local_harness", {})
                .get("retry_status", {})
                .get("failures", [])
            )
            batch_failures = (
                aggregate.get("local_harness", {})
                .get("batch_status", {})
                .get("failures", [])
            )
            coscheduling_failures = (
                aggregate.get("local_harness", {})
                .get("coscheduling_status", {})
                .get("failures", [])
            )
            failed_cases = retry_failures + batch_failures + coscheduling_failures
            if failed_cases:
                f.write("## Reproducible Failed Cases\n\n")
                f.write(
                    "These runs failed in the batch run or retry pass and are excluded from successful latency aggregates.\n\n"
                )
                f.write("| Kind | Config | Workload | Conc | RC | Timeout | Log |\n")
                f.write("| --- | --- | --- | ---: | ---: | --- | --- |\n")
                for row in failed_cases:
                    if "scenario" in row:
                        f.write(
                            f"| coscheduling | {row.get('config')} | {row.get('scenario')} | "
                            f"n/a | {row.get('returncode')} | n/a | {row.get('out_dir')} |\n"
                        )
                        continue
                    f.write(
                        f"| {row.get('kind')} | {row.get('config')} | {row.get('workload')} | "
                        f"{row.get('concurrency')} | {row.get('returncode')} | "
                        f"{row.get('timeout')} | {row.get('log')} |\n"
                    )
                f.write("\n")

            f.write("## Artifacts\n\n")
            f.write(f"- Aggregate JSON: `{self.results_dir / 'aggregate_summary.json'}`\n")
            f.write(f"- Local harness CSV: `{self.results_dir / 'local_harness_aggregate.csv'}`\n")
            coscheduling_path = self.coscheduling_summary_path()
            if coscheduling_path is not None:
                f.write(f"- Mixed co-scheduling CSV: `{coscheduling_path}`\n")

        self.log(f"Report generated: {report_file}")

    def run(self):
        """Run the complete benchmark suite"""
        try:
            self.log("=" * 60)
            self.log("COSMOS Comprehensive Benchmark Suite")
            self.log("=" * 60)
            self.log(f"Results directory: {self.results_dir}")

            # Step 1: Build traces
            azure_2019_direct_dirs, azure_2019_sebs_dir = self.build_traces()

            # Step 2: Remote benchmarks
            if not self.skip_remote:
                self.log("Skipping deprecated remote Azure/OpenWhisk benchmarks")
                self.skip_remote = True

            # Step 3: Local harness benchmarks
            if not self.skip_local:
                self.run_local_harness_benchmarks()
                self.run_coscheduling_benchmarks()

                # Step 4: Ablation experiments
                self.run_ablation_experiments()

            # Step 5: Aggregate and generate report
            aggregate = self.aggregate_results(azure_2019_direct_dirs, azure_2019_sebs_dir)
            self.generate_report(aggregate)

            self.log("=" * 60)
            self.log("Benchmark suite complete!")
            self.log("=" * 60)

        finally:
            self.log_fp.close()


def main():
    parser = argparse.ArgumentParser(
        description="Comprehensive COSMOS benchmarking orchestrator"
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=BENCHMARK_ROOT / f"results/comprehensive_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        help="Results directory",
    )
    parser.add_argument(
        "--skip-remote",
        action="store_true",
        help="Skip remote OpenWhisk benchmarks",
    )
    parser.add_argument(
        "--skip-local",
        action="store_true",
        help="Skip local harness benchmarks",
    )
    parser.add_argument(
        "--openwhisk-limit",
        type=int,
        default=DEFAULT_OPENWHISK_LIMIT,
        help="Bound each OpenWhisk replay to this many invocations",
    )
    parser.add_argument(
        "--openwhisk-max-inflight",
        type=int,
        default=DEFAULT_OPENWHISK_MAX_INFLIGHT,
        help="Pass-through max concurrent wsk invocations; 0 preserves replay schedule",
    )

    args = parser.parse_args()

    orchestrator = BenchmarkOrchestrator(
        results_dir=args.results_dir,
        skip_remote=args.skip_remote,
        skip_local=args.skip_local,
        openwhisk_limit=args.openwhisk_limit,
        openwhisk_max_inflight=args.openwhisk_max_inflight,
    )

    try:
        orchestrator.run()
    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
