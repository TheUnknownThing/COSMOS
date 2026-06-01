#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from semantic_catalog import (
    ASSIGNMENT_SCHEMA,
    CATALOG_VERSION,
    SEBS_ANCHORS,
    REALIZATIONS,
    anchor_counts,
    catalog_payload,
    controllable_realization_candidates,
    choose_anchor,
    choose_realization,
    duration_bucket,
    is_upstream_sebs_realization,
    upstream_realization_candidates,
    validate_mix,
)
from semantic_calibration import (
    calibration_decision,
    filter_calibrated_candidates,
    load_calibration,
)
from trace_ir import CONTRACT_PATH, load_contract, stable_u64


DEFAULT_TRACE_IR = Path("benchmarks/semantic_benchmark/results/azure-2021-trace-ir/trace_ir.json")
DEFAULT_OUTPUT_DIR = Path("benchmarks/semantic_benchmark/results/azure-2021-semantic-assignments")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Attach deterministic SeBS semantic anchors and executable duration "
            "realizations to an Azure 2021 trace IR."
        )
    )
    parser.add_argument("--trace-ir", type=Path, default=DEFAULT_TRACE_IR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--semantic-mix",
        choices=("cpu-heavy", "io-heavy", "memory-heavy", "network-heavy", "balanced"),
        default="balanced",
    )
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument(
        "--allow-uncalibrated-upstream",
        action="store_true",
        help=(
            "Permit direct upstream SeBS realizations before Phase 5 calibration. "
            "By default Phase 4 uses controllable kernels only."
        ),
    )
    parser.add_argument(
        "--calibration",
        type=Path,
        help="Phase 5 calibration.json. When present, selected realizations must be calibrated for each invocation bucket.",
    )
    parser.add_argument(
        "--unsupported-mode",
        choices=("fail", "mark"),
        default="fail",
        help="Fail on uncalibrated/unsupported invocations or emit them with calibration_status=unsupported.",
    )
    return parser.parse_args(argv)


def read_trace_ir(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != "cosmos.semantic.azure-2021-trace-ir":
        raise ValueError(f"{path} is not a semantic Azure 2021 trace IR")
    if not isinstance(payload.get("invocations"), list):
        raise ValueError(f"{path} does not contain an invocations list")
    return payload


def resource_knobs(
    anchor_id: str,
    realization_id: str,
    target_duration_ms: int,
) -> dict[str, Any]:
    anchor = SEBS_ANCHORS[anchor_id]
    bucket = duration_bucket(target_duration_ms)
    scale = {
        "0-50ms": 1,
        "50-200ms": 2,
        "200-400ms": 4,
        "400ms-2s": 8,
        "2s+": 16,
    }[bucket]
    base_transfer = 32 * 1024 * scale
    base_working_set = 512 * 1024 * scale
    knobs: dict[str, Any] = {
        "target_duration_ms": target_duration_ms,
        "target_duration_us": target_duration_ms * 1000,
        "duration_bucket": bucket,
        "repeat_count": max(1, round(target_duration_ms / 50)),
    }
    if realization_id == "cpu-spin-controllable":
        knobs.update({"loop_count": max(1_000, target_duration_ms * 1_000)})
    elif realization_id == "memory-scan-controllable":
        knobs.update({"working_set_size": max(256 * 1024, base_working_set)})
    elif realization_id == "storage-io-controllable":
        knobs.update({"bytes": base_transfer * 4, "transfer_size": base_transfer})
    elif realization_id == "network-transfer-controllable":
        knobs.update({"transfer_size": base_transfer * 2})
    elif realization_id == "balanced-pipeline-controllable":
        knobs.update(
            {
                "bytes": base_transfer * 2,
                "loop_count": max(500, target_duration_ms * 500),
                "working_set_size": max(256 * 1024, base_working_set),
            }
        )
    elif realization_id == "upstream-sebs-calibrated":
        knobs.update({"input_size": anchor["input_size_options"][0]})
    return knobs


def semantic_invocation(
    invocation: dict[str, Any],
    function_mapping: dict[str, Any],
    realization_id: str,
    calibration: dict[str, Any] | None = None,
    selection_reason: str = "selected",
) -> dict[str, Any]:
    anchor_id = function_mapping["semantic_anchor"]
    realization = REALIZATIONS[realization_id]
    target_duration_ms = int(invocation["target_duration_ms"])
    decision = calibration_decision(calibration, realization_id, target_duration_ms, anchor_id)
    calibration_status = decision.status
    supports_slo = decision.supported if calibration is not None else False
    actual_workload = anchor_id if is_upstream_sebs_realization(realization_id) else realization_id
    return {
        **invocation,
        "semantic_source": realization["semantic_source"],
        "actual_workload": actual_workload,
        "actual_workload_source": (
            "upstream-sebs" if is_upstream_sebs_realization(realization_id) else "synthesized"
        ),
        "sebs_anchor": anchor_id,
        "sebs_action_name": SEBS_ANCHORS[anchor_id]["action_name"],
        "resource_class": SEBS_ANCHORS[anchor_id]["resource_class"],
        "duration_realization": realization_id,
        "duration_realization_reason": selection_reason,
        "target_duration_class": duration_bucket(target_duration_ms),
        "duration_realization_uses_upstream_sebs": realization["uses_upstream_sebs_directly"],
        "expected_phase_sequence": realization["expected_phase_sequence"],
        "resource_knobs": resource_knobs(anchor_id, realization_id, target_duration_ms),
        "calibration_status": calibration_status,
        "calibration_supports_slo": supports_slo,
        "calibration_metrics": decision.metrics,
    }


def build_assignments_payload(
    trace_ir: dict[str, Any],
    trace_ir_path: Path,
    semantic_mix: str,
    seed: int,
    allow_uncalibrated_upstream: bool = False,
    calibration: dict[str, Any] | None = None,
    unsupported_mode: str = "fail",
) -> dict[str, Any]:
    validate_mix(semantic_mix)
    invocations = trace_ir["invocations"]
    function_mappings: dict[str, dict[str, Any]] = {}
    enriched_invocations: list[dict[str, Any]] = []
    realization_counter: Counter[str] = Counter()
    bucket_counter: Counter[str] = Counter()
    source_counter: Counter[str] = Counter()

    for invocation in invocations:
        function_id = str(invocation["function_id"])
        if function_id not in function_mappings:
            anchor_id = choose_anchor(function_id, semantic_mix, seed)
            function_mappings[function_id] = {
                "function_id": function_id,
                "app": invocation["app"],
                "func": invocation["func"],
                "semantic_anchor": anchor_id,
                "sebs_action_name": SEBS_ANCHORS[anchor_id]["action_name"],
                "resource_class": SEBS_ANCHORS[anchor_id]["resource_class"],
                "assignment_policy": "seeded-weighted-resource-class-then-anchor",
            }

        anchor_id = function_mappings[function_id]["semantic_anchor"]
        target_duration_ms = int(invocation["target_duration_ms"])
        selection_reason = "selected"
        upstream_candidates = upstream_realization_candidates(anchor_id, target_duration_ms)
        if calibration is None and not allow_uncalibrated_upstream:
            upstream_candidates = []
        upstream_candidates = filter_calibrated_candidates(
            upstream_candidates,
            target_duration_ms,
            calibration,
            anchor_id,
        )
        if upstream_candidates:
            point = stable_u64(
                f"{seed}:{anchor_id}:{int(invocation['invocation_id'])}:{target_duration_ms}:upstream"
            )
            realization_id = upstream_candidates[point % len(upstream_candidates)]
            selection_reason = "calibrated-upstream-sebs"
        else:
            controllable_candidates = filter_calibrated_candidates(
                controllable_realization_candidates(anchor_id, target_duration_ms),
                target_duration_ms,
                calibration,
                anchor_id,
            )
            if controllable_candidates:
                point = stable_u64(
                    f"{seed}:{anchor_id}:{int(invocation['invocation_id'])}:{target_duration_ms}:controllable"
                )
                realization_id = controllable_candidates[point % len(controllable_candidates)]
                selection_reason = (
                    "synthetic-fallback-no-calibrated-upstream-sebs"
                    if calibration is not None
                    else "synthetic-fallback-no-calibration"
                )
            elif calibration is None:
                realization_id = choose_realization(
                    anchor_id,
                    int(invocation["invocation_id"]),
                    target_duration_ms,
                    seed,
                    allow_uncalibrated_upstream,
                )
                selection_reason = "uncalibrated-selection"
            elif unsupported_mode == "mark":
                fallback = upstream_realization_candidates(
                    anchor_id,
                    target_duration_ms,
                ) + controllable_realization_candidates(anchor_id, target_duration_ms)
                if not fallback:
                    bucket = duration_bucket(target_duration_ms)
                    raise ValueError(f"no realization supports anchor={anchor_id} bucket={bucket}")
                realization_id = fallback[0]
                selection_reason = "unsupported-marked-no-calibrated-fit"
            else:
                bucket = duration_bucket(target_duration_ms)
                raise ValueError(
                    f"no calibrated upstream SeBS or synthetic fallback supports "
                    f"anchor={anchor_id} bucket={bucket}; run upstream SeBS calibration "
                    "for that anchor/bucket, calibrate the controllable fallback, or "
                    "use --unsupported-mode mark"
                )
        decision = calibration_decision(calibration, realization_id, target_duration_ms, anchor_id)
        if (
            calibration is not None
            and unsupported_mode == "fail"
            and not decision.supported
        ):
            raise ValueError(
                f"selected realization is not calibrated-supported: "
                f"realization={realization_id} bucket={decision.bucket}"
            )
        realization_counter[realization_id] += 1
        source_counter[
            "upstream-sebs" if is_upstream_sebs_realization(realization_id) else "synthesized"
        ] += 1
        bucket_counter[duration_bucket(target_duration_ms)] += 1
        enriched_invocations.append(
            semantic_invocation(
                invocation,
                function_mappings[function_id],
                realization_id,
                calibration,
                selection_reason,
            )
        )

    created_at = datetime.now(timezone.utc).isoformat()
    contract = load_contract(CONTRACT_PATH)
    return {
        "version": 1,
        "schema": ASSIGNMENT_SCHEMA,
        "created_at": created_at,
        "contract": {
            "version": contract["version"],
            "schema": contract["schema"],
            "benchmark_name": contract["benchmark_name"],
            "trace_time_unit": contract["trace_truth"]["time_unit"],
            "semantic_catalog_version": CATALOG_VERSION,
            "modeled_fields_present": True,
        },
        "source": {
            "trace_ir": str(trace_ir_path),
            "trace_ir_schema": trace_ir["schema"],
            "trace_ir_version": trace_ir["version"],
        },
        "policy": {
            "semantic_mix": semantic_mix,
            "seed": seed,
            "function_assignment": "same function_id maps to one semantic_anchor per replay",
            "duration_realization": "chosen per invocation from target_duration_ms bucket",
            "realization_preference": (
                "prefer calibrated upstream SeBS for the exact anchor and bucket; "
                "use synthesized controllable fallback only when no upstream fit exists"
            ),
            "allow_uncalibrated_upstream": allow_uncalibrated_upstream,
            "calibration_required_for_slo": calibration is not None,
            "unsupported_mode": unsupported_mode,
        },
        "summary": {
            "invocations": len(enriched_invocations),
            "functions": len(function_mappings),
            "anchor_counts": anchor_counts(function_mappings),
            "realization_counts": dict(sorted(realization_counter.items())),
            "actual_workload_source_counts": dict(sorted(source_counter.items())),
            "duration_bucket_counts": dict(sorted(bucket_counter.items())),
        },
        "catalog_path": "semantic_catalog.json",
        "calibration": {
            "schema": calibration.get("schema") if calibration else None,
            "created_at": calibration.get("created_at") if calibration else None,
            "required_for_selected_realizations": calibration is not None,
        },
        "semantic_invocations_path": "semantic_invocations.csv",
        "function_mappings": dict(sorted(function_mappings.items())),
        "invocations": enriched_invocations,
    }


def write_semantic_invocations_csv(path: Path, invocations: list[dict[str, Any]]) -> None:
    fieldnames = [
        "invocation_id",
        "event_id",
        "at_ms",
        "source_start_ms",
        "source_end_ms",
        "target_duration_ms",
        "target_duration_class",
        "app",
        "func",
        "function_id",
        "semantic_source",
        "actual_workload",
        "actual_workload_source",
        "sebs_anchor",
        "sebs_action_name",
        "resource_class",
        "duration_realization",
        "duration_realization_reason",
        "duration_realization_uses_upstream_sebs",
        "calibration_status",
        "calibration_supports_slo",
        "resource_knobs",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for invocation in invocations:
            row = {field: invocation.get(field) for field in fieldnames}
            row["resource_knobs"] = json.dumps(row["resource_knobs"], sort_keys=True)
            writer.writerow(row)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        trace_ir = read_trace_ir(args.trace_ir)
        calibration = load_calibration(args.calibration)
        assignments = build_assignments_payload(
            trace_ir,
            args.trace_ir,
            args.semantic_mix,
            args.seed,
            args.allow_uncalibrated_upstream,
            calibration,
            args.unsupported_mode,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "semantic_catalog.json").write_text(
        json.dumps(catalog_payload(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (args.output_dir / "semantic_assignments.json").write_text(
        json.dumps(assignments, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_semantic_invocations_csv(
        args.output_dir / "semantic_invocations.csv",
        assignments["invocations"],
    )

    print(args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
