#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from semantic_calibration import warm_summary_ms
from semantic_catalog import (
    CALIBRATION_SCHEMA,
    DEFAULT_BUCKET_TARGET_MS,
    DURATION_BUCKETS,
    KERNEL_MODE_BY_REALIZATION,
    REALIZATIONS,
    bucket_upper_ms,
)
from trace_ir import summarize


DEFAULT_KERNEL = Path(__file__).resolve().parent / "kernels" / "semantic_kernel"
DEFAULT_OUTPUT_DIR = Path("benchmarks/semantic_benchmark/results/azure-2021-calibration")


def parse_bucket_target(value: str) -> tuple[str, int]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("expected BUCKET=MS")
    bucket, raw_ms = value.split("=", 1)
    if bucket not in DURATION_BUCKETS:
        raise argparse.ArgumentTypeError(f"unsupported duration bucket: {bucket}")
    try:
        target_ms = int(raw_ms)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid target ms: {raw_ms}") from exc
    if target_ms <= 0:
        raise argparse.ArgumentTypeError("target ms must be positive")
    return bucket, target_ms


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calibrate semantic benchmark duration realizations."
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--kernel-path", type=Path, default=DEFAULT_KERNEL)
    parser.add_argument(
        "--realization",
        action="append",
        choices=tuple(KERNEL_MODE_BY_REALIZATION),
        help="Controllable realization to calibrate. Defaults to all controllable realizations.",
    )
    parser.add_argument(
        "--duration-bucket",
        action="append",
        choices=DURATION_BUCKETS,
        help="Duration bucket to calibrate. Defaults to all buckets.",
    )
    parser.add_argument("--bucket-target-ms", action="append", type=parse_bucket_target)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument(
        "--tolerance",
        type=float,
        default=1.25,
        help="A bucket is supported when warm p99 stays within target*tolerance and, for bounded buckets, within bucket_upper*tolerance.",
    )
    parser.add_argument(
        "--upstream-calibration",
        type=Path,
        action="append",
        help="Optional upstream SeBS calibration artifact to merge into the output.",
    )
    return parser.parse_args(argv)


def run_kernel(kernel: Path, mode: str, target_ms: int) -> dict[str, Any]:
    command = [str(kernel), "--mode", mode, "--target-us", str(target_ms * 1000)]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(
            f"calibration command failed ({' '.join(command)}): {completed.stderr.strip()}"
        )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"calibration command returned invalid JSON: {completed.stdout}") from exc
    payload["command"] = command
    return payload


def supported_bucket(
    bucket: str,
    target_ms: int,
    warm_p99_ms: float | int | None,
    tolerance: float,
) -> bool:
    if warm_p99_ms is None:
        return False
    upper = bucket_upper_ms(bucket)
    target_limit = target_ms * tolerance
    bucket_limit = float("inf") if upper is None else upper * tolerance
    return float(warm_p99_ms) <= min(target_limit, bucket_limit)


def calibrate_realization_bucket(
    kernel: Path,
    realization_id: str,
    bucket: str,
    target_ms: int,
    repetitions: int,
    tolerance: float,
) -> dict[str, Any]:
    mode = KERNEL_MODE_BY_REALIZATION[realization_id]
    samples = [run_kernel(kernel, mode, target_ms) for _ in range(repetitions)]
    elapsed_us = [int(sample["elapsed_us"]) for sample in samples]
    cold_elapsed_us = elapsed_us[:1]
    warm_elapsed_us = elapsed_us[1:] if len(elapsed_us) > 1 else elapsed_us
    isolated_warm_ms = warm_summary_ms(warm_elapsed_us)
    warm_p99 = isolated_warm_ms["p99"]
    command = samples[-1]["command"]
    return {
        "supported": supported_bucket(bucket, target_ms, warm_p99, tolerance),
        "target_duration_ms": target_ms,
        "target_duration_us": target_ms * 1000,
        "isolated_warm_ms": isolated_warm_ms,
        "isolated_all_ms": warm_summary_ms(elapsed_us),
        "cold_start_ms": summarize([float(value) / 1000.0 for value in cold_elapsed_us]),
        "resource_counters": {
            "available": False,
            "reason": "native kernel self-report does not include perf or cgroup counters",
        },
        "openwhisk": {
            "available": False,
            "submit_lag_ms": None,
            "post_submit_latency_ms": None,
            "reason": "local controllable-kernel calibration path",
        },
        "command": command,
        "raw_samples": samples,
    }


def merge_upstream_calibration(
    measurements: dict[str, Any],
    upstream_paths: list[Path] | None,
) -> list[str]:
    merged: list[str] = []
    for path in upstream_paths or []:
        payload = json.loads(path.read_text(encoding="utf-8"))
        for realization_id, measurement in payload.get("measurements", {}).items():
            measurements[realization_id] = measurement
            merged.append(str(path))
    return merged


def build_calibration_payload(
    *,
    kernel_path: Path,
    realizations: list[str],
    buckets: list[str],
    bucket_targets_ms: dict[str, int],
    repetitions: int,
    tolerance: float,
    upstream_calibration: list[Path] | None = None,
) -> dict[str, Any]:
    if repetitions <= 0:
        raise ValueError("repetitions must be positive")
    if not kernel_path.exists():
        raise ValueError(f"kernel does not exist: {kernel_path}")

    created_at = datetime.now(timezone.utc).isoformat()
    measurements: dict[str, Any] = {}
    for realization_id in realizations:
        realization = REALIZATIONS[realization_id]
        bucket_payloads: dict[str, Any] = {}
        for bucket in buckets:
            if bucket not in realization["supported_duration_buckets"]:
                bucket_payloads[bucket] = {
                    "supported": False,
                    "status": "not-declared-by-catalog",
                }
                continue
            bucket_payloads[bucket] = calibrate_realization_bucket(
                kernel_path,
                realization_id,
                bucket,
                bucket_targets_ms[bucket],
                repetitions,
                tolerance,
            )
        measurements[realization_id] = {
            "semantic_source": realization["semantic_source"],
            "resource_class": realization["resource_class"],
            "uses_upstream_sebs_directly": realization["uses_upstream_sebs_directly"],
            "buckets": bucket_payloads,
        }

    merged_upstream = merge_upstream_calibration(measurements, upstream_calibration)
    unsupported = [
        {"realization_id": realization_id, "bucket": bucket}
        for realization_id, measurement in sorted(measurements.items())
        for bucket, bucket_payload in sorted(measurement.get("buckets", {}).items())
        if not bucket_payload.get("supported")
    ]
    return {
        "version": 1,
        "schema": CALIBRATION_SCHEMA,
        "created_at": created_at,
        "source": {
            "kernel_path": str(kernel_path),
            "repetitions": repetitions,
            "tolerance": tolerance,
            "upstream_calibration": merged_upstream,
        },
        "bucket_targets_ms": {bucket: bucket_targets_ms[bucket] for bucket in buckets},
        "measurements": measurements,
        "unsupported": unsupported,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    bucket_targets = dict(DEFAULT_BUCKET_TARGET_MS)
    for bucket, target_ms in args.bucket_target_ms or []:
        bucket_targets[bucket] = target_ms
    realizations = sorted(args.realization or list(KERNEL_MODE_BY_REALIZATION))
    buckets = list(args.duration_bucket or DURATION_BUCKETS)
    try:
        payload = build_calibration_payload(
            kernel_path=args.kernel_path,
            realizations=realizations,
            buckets=buckets,
            bucket_targets_ms=bucket_targets,
            repetitions=args.repetitions,
            tolerance=args.tolerance,
            upstream_calibration=args.upstream_calibration,
        )
    except (RuntimeError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "calibration.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
