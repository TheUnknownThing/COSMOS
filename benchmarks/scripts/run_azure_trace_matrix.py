#!/usr/bin/env python3
"""Run Azure top-functions pool sweeps across mixes and schedulers."""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent

DEFAULT_MIXES = [
    "balanced",
    "cpu-heavy",
    "io-heavy",
    "memory-heavy",
    "network-heavy",
]
DEFAULT_CONFIGS = ["cfs-default", "sfs", "cosmos-full"]


def timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Azure top-functions comparisons across workload mixes."
    )
    parser.add_argument(
        "--config-json",
        type=Path,
        default=REPO_ROOT / "benchmarks" / "azure_trace" / "top20_mixed_p75.json",
    )
    parser.add_argument(
        "--result-root",
        type=Path,
        default=SCRIPT_DIR / "results" / "azure_trace_matrix" / timestamp(),
    )
    parser.add_argument("--mix", action="append", dest="mixes", default=[])
    parser.add_argument("--config", action="append", dest="configs", default=[])
    parser.add_argument("--load-min", type=float, default=0.75)
    parser.add_argument("--load-max", type=float, default=1.00)
    parser.add_argument("--load-steps", type=int, default=6)
    parser.add_argument("--run-duration-s", type=float, default=180.0)
    parser.add_argument("--warmup-duration-s", type=float, default=30.0)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--pool-size", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-launch-workers", type=int, default=1024)
    parser.add_argument("--slo-miss-threshold", type=float, default=0.05)
    parser.add_argument(
        "--decision-trace",
        action="store_true",
        help="Enable per-dispatch scheduler decision tracing for SFS/COSMOS runs.",
    )
    parser.add_argument(
        "--decision-trace-limit",
        type=int,
        default=0,
        help="Maximum decision trace rows per run; 0 means unlimited.",
    )
    parser.add_argument(
        "--reserve-control-plane-cpus",
        type=int,
        default=4,
        help="Reserve the last N CPUs for scheduler/event-bridge/stats processes.",
    )
    parser.add_argument(
        "--chown-to",
        default="",
        help="Optional owner[:group] to apply to the result root at the end.",
    )
    return parser.parse_args()


def stream_command(
    cmd: list[str],
    *,
    cwd: Path,
    log_path: Path,
) -> tuple[int, list[str]]:
    lines: list[str] = []
    with log_path.open("a", encoding="utf-8") as log:
        log.write("\n$ " + " ".join(cmd) + "\n")
        log.flush()
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            log.write(line)
            log.flush()
            lines.append(line.rstrip("\n"))
        return proc.wait(), lines


def cleanup_scheduler() -> None:
    for pattern in (
        str(REPO_ROOT / "target" / "release" / "cosmos"),
        str(REPO_ROOT / "target" / "release" / "cosmos-event-bridge"),
    ):
        subprocess.run(["pkill", "-INT", "-f", pattern], check=False)


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def maybe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def collect_rows(result_root: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    seen_run_dirs: set[str] = set()
    for sweep_path in sorted((result_root / "runs").glob("*/*/*/sweep.json")):
        if "latest" in sweep_path.parts:
            continue
        sweep = load_json(sweep_path)
        mix = str(sweep.get("workload_mix", sweep_path.parents[2].name))
        config = str(sweep.get("config", sweep_path.parents[1].name))
        for candidate in sweep.get("candidates", []):
            run_dir = Path(str(candidate.get("run_dir", "")))
            if str(run_dir) in seen_run_dirs:
                continue
            seen_run_dirs.add(str(run_dir))
            summary_path = run_dir / "summary.json"
            summary = load_json(summary_path) if summary_path.exists() else {}
            latency = summary.get("latency", {}) if isinstance(summary, dict) else {}
            row = {
                "mix": mix,
                "config": config,
                "offered_load": maybe_float(candidate.get("offered_load")),
                "rate_inv_per_sec": maybe_float(candidate.get("rate_inv_per_sec")),
                "goodput_inv_per_sec": maybe_float(
                    candidate.get("goodput_inv_per_sec")
                ),
                "throughput_inv_per_sec": maybe_float(
                    candidate.get("throughput_inv_per_sec")
                ),
                "slo_miss_rate": maybe_float(candidate.get("slo_miss_rate")),
                "effective_utilization": maybe_float(
                    candidate.get("effective_utilization")
                ),
                "p50_ms": maybe_float(latency.get("p50_ms")),
                "p95_ms": maybe_float(latency.get("p95_ms")),
                "p99_ms": maybe_float(latency.get("p99_ms")),
                "client_slo_violations": latency.get("client_slo_violations"),
                "measurement_invocations": summary.get("measurement_invocations"),
                "run_dir": str(run_dir),
            }
            rows.append(row)
    return rows


def write_comparison(result_root: Path) -> None:
    rows = collect_rows(result_root)
    csv_path = result_root / "comparison.csv"
    fields = [
        "mix",
        "config",
        "offered_load",
        "rate_inv_per_sec",
        "goodput_inv_per_sec",
        "throughput_inv_per_sec",
        "slo_miss_rate",
        "effective_utilization",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "client_slo_violations",
        "measurement_invocations",
        "run_dir",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    by_key: dict[tuple[str, float], dict[str, dict[str, object]]] = {}
    for row in rows:
        load = row.get("offered_load")
        if not isinstance(load, float):
            continue
        by_key.setdefault((str(row["mix"]), round(load, 2)), {})[
            str(row["config"])
        ] = row

    md_lines = [
        "# Azure Trace Matrix Comparison",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "| Mix | Load | CFS goodput | CFS miss | SFS goodput | SFS miss | COSMOS goodput | COSMOS miss |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for mix in DEFAULT_MIXES:
        for load in sorted(k[1] for k in by_key if k[0] == mix):
            configs = by_key.get((mix, load), {})

            def cell(config: str, key: str) -> str:
                value = configs.get(config, {}).get(key)
                if isinstance(value, float):
                    return f"{value:.3f}"
                return ""

            md_lines.append(
                "| "
                + " | ".join(
                    [
                        mix,
                        f"{load:.2f}",
                        cell("cfs-default", "goodput_inv_per_sec"),
                        cell("cfs-default", "slo_miss_rate"),
                        cell("sfs", "goodput_inv_per_sec"),
                        cell("sfs", "slo_miss_rate"),
                        cell("cosmos-full", "goodput_inv_per_sec"),
                        cell("cosmos-full", "slo_miss_rate"),
                    ]
                )
                + " |"
            )
    (result_root / "comparison.md").write_text(
        "\n".join(md_lines) + "\n", encoding="utf-8"
    )


def main() -> int:
    args = parse_args()
    mixes = args.mixes or DEFAULT_MIXES
    configs = args.configs or DEFAULT_CONFIGS
    result_root = args.result_root.resolve()
    pool_dir = result_root / "pools"
    run_root = result_root / "runs"
    result_root.mkdir(parents=True, exist_ok=True)
    pool_dir.mkdir(parents=True, exist_ok=True)
    run_root.mkdir(parents=True, exist_ok=True)
    log_path = result_root / "matrix.log"
    status_path = result_root / "status.csv"

    status_fields = [
        "mix",
        "config",
        "phase",
        "start_utc",
        "end_utc",
        "duration_s",
        "return_code",
        "output",
    ]
    failures = 0
    with status_path.open("w", encoding="utf-8", newline="") as status_fh:
        status = csv.DictWriter(status_fh, fieldnames=status_fields)
        status.writeheader()

        for mix_index, mix in enumerate(mixes):
            pool_path = pool_dir / f"{mix}.json"
            pool_cmd = [
                sys.executable,
                str(SCRIPT_DIR / "generate_pool.py"),
                "--config-json",
                str(args.config_json),
                "--workload-mix",
                mix,
                "--pool-size",
                str(args.pool_size),
                "--seed",
                str(args.seed + mix_index),
                "--output",
                str(pool_path),
            ]
            start = time.monotonic()
            start_utc = datetime.now(timezone.utc).isoformat()
            rc, lines = stream_command(pool_cmd, cwd=REPO_ROOT, log_path=log_path)
            end_utc = datetime.now(timezone.utc).isoformat()
            status.writerow(
                {
                    "mix": mix,
                    "config": "",
                    "phase": "generate-pool",
                    "start_utc": start_utc,
                    "end_utc": end_utc,
                    "duration_s": f"{time.monotonic() - start:.3f}",
                    "return_code": rc,
                    "output": lines[-1] if lines else "",
                }
            )
            status_fh.flush()
            if rc != 0:
                failures += 1
                continue

            for config_index, config in enumerate(configs):
                out_dir = run_root / mix / config
                cmd = [
                    sys.executable,
                    str(SCRIPT_DIR / "run_pool.py"),
                    "--pool-json",
                    str(pool_path),
                    "--config",
                    config,
                    "--load-min",
                    str(args.load_min),
                    "--load-max",
                    str(args.load_max),
                    "--load-steps",
                    str(args.load_steps),
                    "--run-duration-s",
                    str(args.run_duration_s),
                    "--warmup-duration-s",
                    str(args.warmup_duration_s),
                    "--repeats",
                    str(args.repeats),
                    "--seed",
                    str(args.seed + mix_index * 100 + config_index),
                    "--max-launch-workers",
                    str(args.max_launch_workers),
                    "--slo-miss-threshold",
                    str(args.slo_miss_threshold),
                    "--out-dir",
                    str(out_dir),
                ]
                if args.reserve_control_plane_cpus:
                    cmd.extend(
                        [
                            "--reserve-control-plane-cpus",
                            str(args.reserve_control_plane_cpus),
                        ]
                    )
                if args.decision_trace:
                    cmd.append("--decision-trace")
                    if args.decision_trace_limit > 0:
                        cmd.extend(
                            [
                                "--decision-trace-limit",
                                str(args.decision_trace_limit),
                            ]
                        )
                start = time.monotonic()
                start_utc = datetime.now(timezone.utc).isoformat()
                rc, lines = stream_command(cmd, cwd=REPO_ROOT, log_path=log_path)
                cleanup_scheduler()
                end_utc = datetime.now(timezone.utc).isoformat()
                status.writerow(
                    {
                        "mix": mix,
                        "config": config,
                        "phase": "run-sweep",
                        "start_utc": start_utc,
                        "end_utc": end_utc,
                        "duration_s": f"{time.monotonic() - start:.3f}",
                        "return_code": rc,
                        "output": lines[-1] if lines else "",
                    }
                )
                status_fh.flush()
                write_comparison(result_root)
                if rc != 0:
                    failures += 1

    write_comparison(result_root)
    if args.chown_to:
        subprocess.run(["chown", "-R", args.chown_to, str(result_root)], check=False)
    print(result_root)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
