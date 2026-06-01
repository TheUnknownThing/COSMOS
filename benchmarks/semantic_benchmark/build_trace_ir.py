#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from trace_ir import (
    DEFAULT_2021_TRACE,
    SUPPORTED_DOWNSAMPLE_MODES,
    fidelity_payload,
    load_contract,
    select_trace_window,
    write_trace_invocations_csv,
)


DEFAULT_OUTPUT_DIR = Path("benchmarks/semantic_benchmark/results/azure-2021-trace-ir")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a trace-preserving Azure 2021 invocation IR for semantic benchmarking."
    )
    parser.add_argument("--trace-2021", type=Path, default=DEFAULT_2021_TRACE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--window-start-ms", type=float, default=0.0)
    parser.add_argument("--window-ms", type=float)
    parser.add_argument("--scale", type=float, default=1.0)
    parser.add_argument("--limit", type=int)
    parser.add_argument(
        "--downsample-mode",
        choices=SUPPORTED_DOWNSAMPLE_MODES,
        default="top-apps",
    )
    parser.add_argument("--metrics-sample-limit", type=int, default=200_000)
    parser.add_argument(
        "--normalize-to-first-start",
        action="store_true",
        help=(
            "Treat --window-start-ms as an offset from the first observed start. "
            "By default Azure 2021 timestamps are already trace-relative, so no "
            "full pre-scan is needed."
        ),
    )
    return parser.parse_args(argv)


def build_trace_ir_payload(
    *,
    trace_2021: Path,
    output_dir: Path,
    window_start_ms: float,
    window_ms: float | None,
    scale: float,
    limit: int | None,
    downsample_mode: str,
    metrics_sample_limit: int,
    normalize_to_first_start: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    invocations, source_metrics, base_ms, selection = select_trace_window(
        trace_2021,
        window_start_ms,
        window_ms,
        scale,
        limit,
        downsample_mode,
        metrics_sample_limit,
        normalize_to_first_start,
    )
    created_at = datetime.now(timezone.utc).isoformat()
    contract = load_contract()
    trace_payload = {
        "version": 1,
        "schema": "cosmos.semantic.azure-2021-trace-ir",
        "created_at": created_at,
        "contract": {
            "version": contract["version"],
            "schema": contract["schema"],
            "benchmark_name": contract["benchmark_name"],
            "trace_time_unit": contract["trace_truth"]["time_unit"],
            "trace_truth_fields": contract["trace_truth"]["fields"],
            "modeled_fields_present": False,
        },
        "source": {
            "trace_2021": str(trace_2021),
            "trace_truth": "Azure Functions 2021 invocation trace",
            "time_unit": "seconds",
        },
        "window": {
            "base_ms": base_ms,
            "window_start_ms": window_start_ms,
            "window_ms": window_ms,
            "scale": scale,
            "limit": limit,
            "downsample_mode": downsample_mode,
            "normalize_to_first_start": normalize_to_first_start,
        },
        "selection": asdict(selection),
        "outputs": {
            "trace_invocations_csv": "trace_invocations.csv",
            "fidelity_json": "fidelity.json",
        },
        "invocations": [asdict(invocation) for invocation in invocations],
    }
    fidelity = fidelity_payload(source_metrics, invocations, selection)
    fidelity.update(
        {
            "created_at": created_at,
            "contract": trace_payload["contract"],
            "window": trace_payload["window"],
            "notes": [
                "This artifact preserves Azure 2021 trace timing and identity only.",
                "No executable semantic profile has been attached in Phase 2.",
            ],
        }
    )
    return trace_payload, fidelity


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.scale <= 0:
        raise SystemExit("--scale must be greater than zero")
    if args.limit is not None and args.limit <= 0:
        raise SystemExit("--limit must be positive")

    try:
        trace_payload, fidelity = build_trace_ir_payload(
            trace_2021=args.trace_2021,
            output_dir=args.output_dir,
            window_start_ms=args.window_start_ms,
            window_ms=args.window_ms,
            scale=args.scale,
            limit=args.limit,
            downsample_mode=args.downsample_mode,
            metrics_sample_limit=args.metrics_sample_limit,
            normalize_to_first_start=args.normalize_to_first_start,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    args.output_dir.mkdir(parents=True, exist_ok=True)
    invocations = trace_payload["invocations"]
    from trace_ir import TraceInvocation

    write_trace_invocations_csv(
        args.output_dir / "trace_invocations.csv",
        [TraceInvocation(**item) for item in invocations],
    )
    (args.output_dir / "trace_ir.json").write_text(
        json.dumps(trace_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (args.output_dir / "fidelity.json").write_text(
        json.dumps(fidelity, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    print(args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
