#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import resource
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from semantic_calibration import warm_summary_ms
from semantic_catalog import (
    CALIBRATION_SCHEMA,
    DEFAULT_BUCKET_TARGET_MS,
    DURATION_BUCKETS,
    KERNEL_EXECUTABLE_BY_REALIZATION,
    KERNEL_MODE_BY_REALIZATION,
    REALIZATIONS,
    bucket_upper_ms,
)
from trace_ir import summarize


DEFAULT_KERNEL_DIR = Path(__file__).resolve().parent / "kernels"
DEFAULT_OUTPUT_DIR = Path("benchmarks/semantic_benchmark/results/azure-2021-calibration")
TIME_PREFIX = "COSMOS_TIME"
TIME_RE = re.compile(
    r"COSMOS_TIME real_s=(?P<real>[0-9.]+) user_s=(?P<user>[0-9.]+) "
    r"sys_s=(?P<sys>[0-9.]+) maxrss_kb=(?P<rss>[0-9]+)"
)


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
    parser.add_argument("--kernel-dir", type=Path, default=DEFAULT_KERNEL_DIR)
    parser.add_argument(
        "--kernel-path",
        type=Path,
        help="Compatibility override: run this single executable for every realization.",
    )
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


def bucket_scale(bucket: str) -> int:
    return {
        "0-50ms": 1,
        "50-200ms": 2,
        "200-400ms": 4,
        "400ms-2s": 8,
        "2s+": 16,
    }[bucket]


def kernel_extra_args(mode: str, bucket: str) -> list[str]:
    scale = bucket_scale(bucket)
    args: list[str] = []
    if mode in {"memory", "balanced", "memory_touch", "mixed_pipeline"}:
        args.extend(["--working-set", str(max(256 * 1024, 512 * 1024 * scale))])
    if mode in {"io", "network", "balanced", "network_wait", "local_io", "mixed_pipeline"}:
        args.extend(["--transfer-size", str(max(4096, 32 * 1024 * scale))])
    if mode == "workflow_fanout":
        args.extend(["--fanout", str(max(2, min(64, scale * 4)))])
    return args


def parse_kernel_knobs(command: list[str]) -> dict[str, int]:
    knobs: dict[str, int] = {}
    index = 0
    while index < len(command):
        item = command[index]
        if item in {"--target-us", "--working-set", "--transfer-size", "--fanout"}:
            if index + 1 < len(command):
                try:
                    knobs[item.removeprefix("--").replace("-", "_")] = int(command[index + 1])
                except ValueError:
                    pass
            index += 2
        else:
            index += 1
    return knobs


def parse_time_metrics(stderr: str) -> dict[str, float | int] | None:
    match = TIME_RE.search(stderr)
    if not match:
        return None
    return {
        "real_s": float(match.group("real")),
        "user_s": float(match.group("user")),
        "sys_s": float(match.group("sys")),
        "maxrss_kb": int(match.group("rss")),
    }


def usage_delta(
    before: resource.struct_rusage,
    after: resource.struct_rusage,
) -> dict[str, float | int]:
    return {
        "user_s": max(0.0, float(after.ru_utime) - float(before.ru_utime)),
        "sys_s": max(0.0, float(after.ru_stime) - float(before.ru_stime)),
        "maxrss_kb": int(after.ru_maxrss),
    }


def derived_resource_counters(
    mode: str,
    sample: dict[str, Any],
    time_metrics: dict[str, float | int] | None,
    usage_metrics: dict[str, float | int] | None,
) -> dict[str, Any]:
    elapsed_us = max(1, int(sample.get("elapsed_us") or 0))
    iterations = int(sample.get("iterations") or 0)
    command = [str(item) for item in sample.get("command", [])]
    knobs = parse_kernel_knobs(command)
    working_set = int(knobs.get("working_set", 0))
    transfer_size = int(knobs.get("transfer_size", 0))
    fanout = int(knobs.get("fanout", 0))

    read_bytes = 0
    write_bytes = 0
    network_bytes = 0
    memory_bytes_touched = 0
    dispatch_count = 0
    if mode in {"memory", "memory_touch"}:
        memory_bytes_touched = iterations * working_set
    elif mode == "io":
        read_bytes = iterations * transfer_size * 2
        write_bytes = iterations * transfer_size * 2
    elif mode == "local_io":
        read_bytes = iterations * transfer_size
        write_bytes = iterations * transfer_size
    elif mode == "network":
        network_bytes = iterations * transfer_size
    elif mode == "network_wait":
        network_bytes = iterations * transfer_size
    elif mode == "balanced":
        memory_bytes_touched = iterations * working_set
        read_bytes = iterations * transfer_size
        write_bytes = iterations * transfer_size
        network_bytes = iterations * transfer_size
    elif mode == "mixed_pipeline":
        memory_bytes_touched = iterations * working_set
        read_bytes = iterations * transfer_size
        write_bytes = iterations * transfer_size
        network_bytes = iterations * transfer_size
    elif mode == "workflow_fanout":
        dispatch_count = iterations * fanout

    cpu_time_ms = None
    maxrss_kb = None
    if usage_metrics:
        cpu_time_ms = 1000.0 * (
            float(usage_metrics["user_s"]) + float(usage_metrics["sys_s"])
        )
        maxrss_kb = int(usage_metrics["maxrss_kb"])
    if time_metrics:
        # /usr/bin/time reports RSS per child process; Python rusage gives CPU
        # with better precision for sub-50 ms buckets.
        cpu_time_ms = 1000.0 * (
            float(time_metrics["user_s"]) + float(time_metrics["sys_s"])
        ) if usage_metrics is None else cpu_time_ms
        maxrss_kb = int(time_metrics["maxrss_kb"])
    cpu_intensity = (
        min(1.0, max(0.0, cpu_time_ms / (elapsed_us / 1000.0)))
        if cpu_time_ms is not None
        else None
    )
    elapsed_s = elapsed_us / 1_000_000.0
    return {
        "available": time_metrics is not None or usage_metrics is not None,
        "source": "python-rusage-plus-usr-bin-time-plus-kernel-self-report",
        "elapsed_us": elapsed_us,
        "iterations": iterations,
        "cpu_time_ms": cpu_time_ms,
        "cpu_intensity": cpu_intensity,
        "maxrss_kb": maxrss_kb,
        "working_set_bytes": working_set or None,
        "memory_bytes_touched": memory_bytes_touched,
        "read_bytes": read_bytes,
        "write_bytes": write_bytes,
        "network_bytes": network_bytes,
        "dispatch_count": dispatch_count,
        "io_bandwidth_bytes_per_sec": (
            int((read_bytes + write_bytes) / elapsed_s) if elapsed_s > 0 and (read_bytes + write_bytes) else None
        ),
        "network_bandwidth_bytes_per_sec": (
            int(network_bytes / elapsed_s) if elapsed_s > 0 and network_bytes else None
        ),
        "measurement_note": (
            "CPU from Python child rusage when available; RSS from /usr/bin/time; "
            "byte counters derived from calibrated kernel iterations and configured "
            "working-set/transfer sizes."
        ),
    }


def median_numeric(values: list[float | int | None]) -> float | int | None:
    numeric = [float(value) for value in values if isinstance(value, (int, float))]
    if not numeric:
        return None
    ordered = sorted(numeric)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        value = ordered[mid]
    else:
        value = (ordered[mid - 1] + ordered[mid]) / 2.0
    return int(value) if value.is_integer() else value


def summarize_resource_counters(samples: list[dict[str, Any]]) -> dict[str, Any]:
    if not samples:
        return {
            "available": False,
            "reason": "no resource counter samples",
        }
    keys = [
        "cpu_time_ms",
        "cpu_intensity",
        "maxrss_kb",
        "working_set_bytes",
        "memory_bytes_touched",
        "read_bytes",
        "write_bytes",
        "network_bytes",
        "dispatch_count",
        "io_bandwidth_bytes_per_sec",
        "network_bandwidth_bytes_per_sec",
    ]
    summary = {
        "available": any(bool(sample.get("available")) for sample in samples),
        "source": "python-rusage-plus-usr-bin-time-plus-kernel-self-report",
        "samples": len(samples),
        "measurement_note": (
            "CPU from Python child rusage when available; RSS from /usr/bin/time; "
            "byte counters derived from calibrated kernel iterations and configured "
            "working-set/transfer sizes."
        ),
    }
    for key in keys:
        summary[key] = median_numeric([sample.get(key) for sample in samples])
    if not summary["available"]:
        summary["reason"] = "resource timing tool unavailable"
    return summary


def run_kernel(kernel: Path, mode: str, target_ms: int, bucket: str) -> dict[str, Any]:
    command = [str(kernel), "--target-us", str(target_ms * 1000), *kernel_extra_args(mode, bucket)]
    timed_command = [
        "/usr/bin/time",
        "-f",
        f"{TIME_PREFIX} real_s=%e user_s=%U sys_s=%S maxrss_kb=%M",
        *command,
    ]
    before_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    completed = subprocess.run(
        timed_command if Path("/usr/bin/time").exists() else command,
        check=False,
        capture_output=True,
        text=True,
    )
    after_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    if completed.returncode != 0:
        raise RuntimeError(
            f"calibration command failed ({' '.join(command)}): {completed.stderr.strip()}"
        )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"calibration command returned invalid JSON: {completed.stdout}") from exc
    payload["command"] = command
    payload["time_metrics"] = parse_time_metrics(completed.stderr)
    payload["usage_metrics"] = usage_delta(before_usage, after_usage)
    payload["resource_counters"] = derived_resource_counters(
        mode,
        payload,
        payload["time_metrics"],
        payload["usage_metrics"],
    )
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
    kernel_dir: Path,
    realization_id: str,
    bucket: str,
    target_ms: int,
    repetitions: int,
    tolerance: float,
    kernel_path: Path | None = None,
) -> dict[str, Any]:
    mode = KERNEL_MODE_BY_REALIZATION[realization_id]
    kernel = kernel_path or kernel_dir / KERNEL_EXECUTABLE_BY_REALIZATION[realization_id]
    if not kernel.exists():
        raise ValueError(f"kernel does not exist: {kernel}")
    samples = [run_kernel(kernel, mode, target_ms, bucket) for _ in range(repetitions)]
    elapsed_us = [int(sample["elapsed_us"]) for sample in samples]
    cold_elapsed_us = elapsed_us[:1]
    warm_elapsed_us = elapsed_us[1:] if len(elapsed_us) > 1 else elapsed_us
    isolated_warm_ms = warm_summary_ms(warm_elapsed_us)
    warm_p99 = isolated_warm_ms["p99"]
    command = samples[-1]["command"]
    warm_resource_samples = [
        sample.get("resource_counters", {})
        for sample in (samples[1:] if len(samples) > 1 else samples)
        if isinstance(sample.get("resource_counters"), dict)
    ]
    return {
        "supported": supported_bucket(bucket, target_ms, warm_p99, tolerance),
        "target_duration_ms": target_ms,
        "target_duration_us": target_ms * 1000,
        "isolated_warm_ms": isolated_warm_ms,
        "isolated_all_ms": warm_summary_ms(elapsed_us),
        "cold_start_ms": summarize([float(value) / 1000.0 for value in cold_elapsed_us]),
        "resource_counters": summarize_resource_counters(warm_resource_samples),
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
    kernel_dir: Path | None = None,
    kernel_path: Path | None = None,
    realizations: list[str],
    buckets: list[str],
    bucket_targets_ms: dict[str, int],
    repetitions: int,
    tolerance: float,
    upstream_calibration: list[Path] | None = None,
) -> dict[str, Any]:
    if repetitions <= 0:
        raise ValueError("repetitions must be positive")
    effective_kernel_dir = kernel_dir or DEFAULT_KERNEL_DIR
    if kernel_path is not None and not kernel_path.exists():
        raise ValueError(f"kernel does not exist: {kernel_path}")
    if kernel_path is None and not effective_kernel_dir.exists():
        raise ValueError(f"kernel directory does not exist: {effective_kernel_dir}")

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
                effective_kernel_dir,
                realization_id,
                bucket,
                bucket_targets_ms[bucket],
                repetitions,
                tolerance,
                kernel_path,
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
            "kernel_dir": str(effective_kernel_dir),
            "kernel_path": str(kernel_path) if kernel_path else None,
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
            kernel_dir=args.kernel_dir,
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
