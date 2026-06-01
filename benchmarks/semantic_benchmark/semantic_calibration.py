#!/usr/bin/env python3

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from semantic_catalog import CALIBRATION_SCHEMA, duration_bucket
from trace_ir import summarize


@dataclass(frozen=True)
class CalibrationDecision:
    realization_id: str
    bucket: str
    anchor_id: str | None
    supported: bool
    status: str
    source: str | None
    metrics: dict[str, Any] | None


def load_calibration(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != CALIBRATION_SCHEMA:
        raise ValueError(f"{path} is not a semantic duration calibration artifact")
    if not isinstance(payload.get("measurements"), dict):
        raise ValueError(f"{path} does not contain calibration measurements")
    return payload


def warm_summary_ms(elapsed_us: list[int | float]) -> dict[str, float | int | None]:
    return summarize([float(value) / 1000.0 for value in elapsed_us])


def calibration_decision(
    calibration: dict[str, Any] | None,
    realization_id: str,
    target_duration_ms: int,
    anchor_id: str | None = None,
) -> CalibrationDecision:
    bucket = duration_bucket(target_duration_ms)
    if calibration is None:
        return CalibrationDecision(
            realization_id=realization_id,
            bucket=bucket,
            anchor_id=anchor_id,
            supported=False,
            status="uncalibrated",
            source=None,
            metrics=None,
        )

    realization = calibration.get("measurements", {}).get(realization_id)
    bucket_payload = None
    if isinstance(realization, dict):
        if anchor_id is not None:
            anchor_payload = realization.get("anchors", {}).get(anchor_id)
            if isinstance(anchor_payload, dict):
                bucket_payload = anchor_payload.get("buckets", {}).get(bucket)
        if bucket_payload is None:
            bucket_payload = realization.get("buckets", {}).get(bucket)
    if not isinstance(bucket_payload, dict):
        return CalibrationDecision(
            realization_id=realization_id,
            bucket=bucket,
            anchor_id=anchor_id,
            supported=False,
            status=(
                "missing-anchor-bucket-calibration"
                if anchor_id is not None
                else "missing-bucket-calibration"
            ),
            source=str(calibration.get("created_at") or "calibration"),
            metrics=None,
        )

    supported = bool(bucket_payload.get("supported"))
    return CalibrationDecision(
        realization_id=realization_id,
        bucket=bucket,
        anchor_id=anchor_id,
        supported=supported,
        status="calibrated-supported" if supported else "calibrated-unsupported",
        source=str(calibration.get("created_at") or "calibration"),
        metrics={
            "sebs_anchor": bucket_payload.get("sebs_anchor") or anchor_id,
            "input_size": bucket_payload.get("input_size"),
            "runtime": bucket_payload.get("runtime"),
            "target_duration_ms": bucket_payload.get("target_duration_ms"),
            "isolated_warm_ms": bucket_payload.get("isolated_warm_ms"),
            "cold_start_ms": bucket_payload.get("cold_start_ms"),
            "openwhisk": bucket_payload.get("openwhisk"),
        },
    )


def filter_calibrated_candidates(
    candidates: list[str],
    target_duration_ms: int,
    calibration: dict[str, Any] | None,
    anchor_id: str | None = None,
) -> list[str]:
    if calibration is None:
        return candidates
    return [
        candidate
        for candidate in candidates
        if calibration_decision(
            calibration,
            candidate,
            target_duration_ms,
            anchor_id,
        ).supported
    ]
