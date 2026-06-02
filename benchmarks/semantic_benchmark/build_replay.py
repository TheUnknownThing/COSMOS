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
    PROFILES_SCHEMA,
    REALIZATIONS,
    REPLAY_SCHEMA,
    is_upstream_sebs_realization,
)


DEFAULT_ASSIGNMENTS = Path(
    "benchmarks/semantic_benchmark/results/azure-2021-semantic-assignments/semantic_assignments.json"
)
DEFAULT_OUTPUT_DIR = Path("benchmarks/semantic_benchmark/results/azure-2021-replay")
DEFAULT_SLO_MIN_SLACK_US = 5_000
MIB = 1024 * 1024


PHASE_KIND_FOR_SCHEDULER: dict[str, str] = {
    "BlockedNetwork": "IoBound",
    "ControlPlane": "Idle",
    "CpuBound": "CpuBound",
    "CpuOnlyLong": "CpuBound",
    "FanOutFanIn": "Mixed",
    "Idle": "Idle",
    "IoBound": "IoBound",
    "LocalFileIo": "IoBound",
    "MemoryBound": "MemoryBound",
    "MemoryPressure": "MemoryBound",
    "Mixed": "Mixed",
    "NetworkBound": "IoBound",
    "NetworkOrIoBound": "IoBound",
    "Unknown": "Unknown",
    "UpstreamSeBS": "Mixed",
    "WaitBound": "Idle",
    "WriteBack": "IoBound",
}


CPU_INTENSITY_BY_RESOURCE_CLASS: dict[str, float] = {
    "control": 0.10,
    "wait": 0.05,
    "network_wait": 0.15,
    "cpu": 0.90,
    "io": 0.30,
    "memory": 0.55,
    "network": 0.25,
    "balanced": 0.50,
    "mixed": 0.55,
    "orchestration": 0.20,
}


IO_WEIGHT_BY_RESOURCE_CLASS: dict[str, int] = {
    "control": 100,
    "wait": 100,
    "network_wait": 250,
    "cpu": 350,
    "io": 750,
    "memory": 350,
    "network": 300,
    "balanced": 650,
    "mixed": 650,
    "orchestration": 450,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate OpenWhisk/local-harness replay artifacts from semantic assignments."
    )
    parser.add_argument("--semantic-assignments", type=Path, default=DEFAULT_ASSIGNMENTS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--allow-uncalibrated-slo",
        action="store_true",
        help="Generate replay even when selected invocations are not calibration-supported. They are marked calibration_supports_slo=false.",
    )
    return parser.parse_args(argv)


def read_assignments(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != "cosmos.semantic.azure-2021-sebs-assignment":
        raise ValueError(f"{path} is not a semantic assignment artifact")
    if not isinstance(payload.get("invocations"), list):
        raise ValueError(f"{path} does not contain an invocations list")
    return payload


def profile_id_for(invocation: dict[str, Any]) -> str:
    anchor = str(invocation["sebs_anchor"]).replace(".", "_").replace("-", "_")
    realization = str(invocation["duration_realization"]).replace("-", "_")
    return f"{anchor}__{realization}"


def default_deadline_us(target_duration_ms: int) -> int:
    target_us = target_duration_ms * 1000
    return target_us + max(target_us, DEFAULT_SLO_MIN_SLACK_US)


def slo_class(duration_class: str) -> int:
    return {
        "0-50ms": 0,
        "50-200ms": 1,
        "200-400ms": 1,
        "400ms-2s": 2,
        "2s+": 2,
    }[duration_class]


def scheduler_phase_sequence(invocation: dict[str, Any]) -> list[dict[str, Any]]:
    phases: list[dict[str, Any]] = []
    for entry in invocation.get("expected_phase_sequence") or []:
        if not isinstance(entry, dict):
            continue
        kind = PHASE_KIND_FOR_SCHEDULER.get(str(entry.get("kind")), "Unknown")
        try:
            duration_pct = int(entry.get("duration_pct", 0))
        except (TypeError, ValueError):
            continue
        if duration_pct > 0:
            phases.append({"kind": kind, "duration_pct": duration_pct})
    return phases or [{"kind": "Mixed", "duration_pct": 100}]


def scheduler_profile_hints(invocation: dict[str, Any]) -> dict[str, Any]:
    resource_class = str(invocation["resource_class"])
    profile = invocation.get("resource_profile") or {}
    measured = profile.get("measured") or {}
    target_duration_ms = int(invocation["target_duration_ms"])
    measured_cpu = measured.get("cpu_intensity") if isinstance(measured, dict) else None
    measured_io_weight = measured.get("io_weight") if isinstance(measured, dict) else None
    hints: dict[str, Any] = {
        "cpu_intensity": (
            float(measured_cpu)
            if isinstance(measured_cpu, (int, float))
            else CPU_INTENSITY_BY_RESOURCE_CLASS.get(resource_class, 0.50)
        ),
        "io_weight": (
            int(measured_io_weight)
            if isinstance(measured_io_weight, (int, float))
            else IO_WEIGHT_BY_RESOURCE_CLASS.get(resource_class, 500)
        ),
        "phase_sequence": scheduler_phase_sequence(invocation),
        "estimated_duration_ns": target_duration_ms * 1_000_000,
    }
    max_rss_mb = profile.get("max_rss_mb")
    measured_rss_kb = measured.get("maxrss_kb") if isinstance(measured, dict) else None
    if isinstance(measured_rss_kb, (int, float)) and measured_rss_kb > 0:
        max_rss_mb = max(float(max_rss_mb or 0), float(measured_rss_kb) / 1024.0)
    if isinstance(max_rss_mb, (int, float)) and max_rss_mb > 0:
        memory_bytes = max(16 * MIB, int(max_rss_mb) * MIB)
        hints["memory_bytes"] = memory_bytes
        hints["working_set_bytes"] = max(1, memory_bytes // 2)

    read_kb = profile.get("read_kb")
    write_kb = profile.get("write_kb")
    measured_read_bytes = measured.get("read_bytes") if isinstance(measured, dict) else None
    measured_write_bytes = measured.get("write_bytes") if isinstance(measured, dict) else None
    if isinstance(measured_read_bytes, (int, float)):
        read_kb = max(float(read_kb or 0), float(measured_read_bytes) / 1024.0)
    if isinstance(measured_write_bytes, (int, float)):
        write_kb = max(float(write_kb or 0), float(measured_write_bytes) / 1024.0)
    io_kb = 0
    if isinstance(read_kb, (int, float)):
        io_kb += int(read_kb)
    if isinstance(write_kb, (int, float)):
        io_kb += int(write_kb)
    if io_kb > 0 and target_duration_ms > 0:
        hints["io_bandwidth_bytes_per_sec"] = max(
            1,
            int(io_kb * 1024 * 1000 / target_duration_ms),
        )

    network_kb = profile.get("network_kb")
    measured_network_bytes = measured.get("network_bytes") if isinstance(measured, dict) else None
    if isinstance(measured_network_bytes, (int, float)):
        network_kb = max(float(network_kb or 0), float(measured_network_bytes) / 1024.0)
    if isinstance(network_kb, (int, float)) and network_kb > 0 and target_duration_ms > 0:
        hints["network_bandwidth_bytes_per_sec"] = max(
            1,
            int(network_kb * 1024 * 1000 / target_duration_ms),
        )

    metrics = invocation.get("calibration_metrics") or {}
    cold = metrics.get("cold_start_ms")
    warm = metrics.get("isolated_warm_ms")
    if isinstance(cold, dict) and isinstance(warm, dict):
        cold_p50 = cold.get("p50")
        warm_p50 = warm.get("p50")
        if isinstance(cold_p50, (int, float)) and isinstance(warm_p50, (int, float)):
            penalty_ms = cold_p50 - warm_p50
            if penalty_ms > 0:
                hints["cold_load_penalty_ns"] = int(penalty_ms * 1_000_000)

    return hints


def sebs_payload(invocation: dict[str, Any]) -> dict[str, Any]:
    if is_upstream_sebs_realization(invocation["duration_realization"]):
        metrics = invocation.get("calibration_metrics") or {}
        return {
            "_sebs_input_size": metrics.get("input_size")
            or invocation["resource_knobs"].get("input_size", "test"),
            "_target_duration_ms": invocation["target_duration_ms"],
            "_semantic_benchmark_anchor": invocation["sebs_anchor"],
        }
    return {
        "semantic_source": invocation["semantic_source"],
        "sebs_anchor": invocation["sebs_anchor"],
        "duration_realization": invocation["duration_realization"],
        "target_duration_ms": invocation["target_duration_ms"],
        "target_duration_class": invocation["target_duration_class"],
        "resource_knobs": invocation["resource_knobs"],
        "resource_profile": invocation.get("resource_profile"),
    }


def replay_invocation(invocation: dict[str, Any], profile_id: str) -> dict[str, Any]:
    target_duration_ms = int(invocation["target_duration_ms"])
    resource_class = str(invocation["resource_class"])
    workload = str(invocation.get("actual_workload") or invocation["duration_realization"])
    calibration_supports_slo = bool(invocation.get("calibration_supports_slo"))
    return {
        "invocation_id": int(invocation["invocation_id"]),
        "event_id": invocation["event_id"],
        "at_ms": float(invocation["at_ms"]),
        "source_start_ms": invocation["source_start_ms"],
        "source_end_ms": invocation["source_end_ms"],
        "target_duration_ms": target_duration_ms,
        "target_duration_class": invocation["target_duration_class"],
        "duration_ms": target_duration_ms,
        "deadline_us": default_deadline_us(target_duration_ms),
        "deadline_source": (
            "target-duration-aware; calibration-supported"
            if calibration_supports_slo
            else "target-duration-aware; uncalibrated-not-for-slo"
        ),
        "slo_class": slo_class(invocation["target_duration_class"]),
        "function_id": invocation["function_id"],
        "function_hash": invocation["function_id"],
        "app": invocation["app"],
        "func": invocation["func"],
        "profile_id": profile_id,
        "workload": workload,
        "kernel": workload,
        "actual_workload": workload,
        "actual_workload_source": invocation.get("actual_workload_source"),
        "semantic_source": invocation["semantic_source"],
        "sebs_anchor": invocation["sebs_anchor"],
        "sebs_action_name": invocation["sebs_action_name"],
        "resource_class": resource_class,
        "duration_realization": invocation["duration_realization"],
        "duration_realization_uses_upstream_sebs": invocation[
            "duration_realization_uses_upstream_sebs"
        ],
        "calibration_status": invocation.get("calibration_status"),
        "calibration_supports_slo": calibration_supports_slo,
        "profile_hints": scheduler_profile_hints(invocation),
        "semantic_metadata": {
            "resource_class": resource_class,
            "actual_workload": workload,
            "actual_workload_source": invocation.get("actual_workload_source"),
            "semantic_anchor": invocation["sebs_anchor"],
            "duration_realization": invocation["duration_realization"],
            "expected_phase_sequence": invocation["expected_phase_sequence"],
            "resource_profile": invocation.get("resource_profile"),
            "calibration_status": invocation.get("calibration_status"),
        },
        "payload": sebs_payload(invocation),
        "sebs_payload": sebs_payload(invocation),
    }


def build_profiles(
    assignments: dict[str, Any],
    replay_invocations: list[dict[str, Any]],
) -> dict[str, Any]:
    profile_lookup: dict[str, dict[str, Any]] = {}
    function_profiles: dict[str, str] = {}
    app_profiles: dict[str, list[str]] = {}

    for assigned, replay in zip(assignments["invocations"], replay_invocations):
        profile_id = replay["profile_id"]
        function_profiles.setdefault(assigned["function_id"], profile_id)
        app_profiles.setdefault(assigned["app"], [])
        if profile_id not in app_profiles[assigned["app"]]:
            app_profiles[assigned["app"]].append(profile_id)
        if profile_id in profile_lookup:
            continue
        realization = REALIZATIONS[assigned["duration_realization"]]
        profile_lookup[profile_id] = {
            "profile_id": profile_id,
            **replay["profile_hints"],
            "workload": replay["workload"],
            "actual_workload_source": assigned.get("actual_workload_source"),
            "actual_workload": replay["workload"],
            "semantic_source": assigned["semantic_source"],
            "sebs_anchor": assigned["sebs_anchor"],
            "sebs_action_name": assigned["sebs_action_name"],
            "resource_class": assigned["resource_class"],
            "duration_realization": assigned["duration_realization"],
            "uses_upstream_sebs_directly": realization["uses_upstream_sebs_directly"],
            "expected_phase_sequence": assigned["expected_phase_sequence"],
            "resource_knob_template": realization["resource_knobs"],
            "measured": (assigned.get("resource_profile") or {}).get("measured"),
            "calibration_status": assigned.get("calibration_status"),
            "calibration_supports_slo": bool(assigned.get("calibration_supports_slo")),
            "openwhisk_action_map_key": replay["workload"],
        }

    return {
        "version": 1,
        "schema": PROFILES_SCHEMA,
        "profiles": dict(sorted(profile_lookup.items())),
        "function_profiles": dict(sorted(function_profiles.items())),
        "app_profiles": {key: sorted(value) for key, value in sorted(app_profiles.items())},
    }


def write_invocations_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "invocation_id",
        "event_id",
        "at_ms",
        "source_start_ms",
        "source_end_ms",
        "target_duration_ms",
        "target_duration_class",
        "duration_ms",
        "deadline_us",
        "deadline_source",
        "slo_class",
        "app",
        "func",
        "function_id",
        "function_hash",
        "profile_id",
        "workload",
        "actual_workload",
        "actual_workload_source",
        "semantic_source",
        "sebs_anchor",
        "sebs_action_name",
        "resource_class",
        "duration_realization",
        "calibration_status",
        "calibration_supports_slo",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def build_fidelity(
    assignments: dict[str, Any],
    replay_invocations: list[dict[str, Any]],
    created_at: str,
) -> dict[str, Any]:
    durations = [item["target_duration_ms"] for item in replay_invocations]
    starts = [item["source_start_ms"] for item in replay_invocations]
    iats = [right - left for left, right in zip(sorted(starts), sorted(starts)[1:])]
    return {
        "schema": "cosmos.semantic.replay-fidelity",
        "created_at": created_at,
        "source": assignments["source"],
        "generated": {
            "invocations": len(replay_invocations),
            "functions": len({item["function_id"] for item in replay_invocations}),
            "apps": len({item["app"] for item in replay_invocations}),
            "duration_ms": {
                "count": len(durations),
                "min": min(durations) if durations else None,
                "max": max(durations) if durations else None,
            },
            "iat_count": len(iats),
        },
        "semantic_summary": assignments["summary"],
        "calibration": assignments.get("calibration"),
        "notes": [
            "Replay preserves source_start_ms, scaled at_ms, target_duration_ms, function_id, app, and func from the semantic assignment input.",
            "SLO conclusions should use only invocations with calibration_supports_slo=true.",
        ],
    }


def build_replay_payloads(
    assignments: dict[str, Any],
    assignments_path: Path,
    allow_uncalibrated_slo: bool,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    unsupported = [
        item
        for item in assignments["invocations"]
        if not bool(item.get("calibration_supports_slo"))
    ]
    if unsupported and not allow_uncalibrated_slo:
        first = unsupported[0]
        raise ValueError(
            "semantic assignments contain invocations without calibration-supported SLOs; "
            f"first unsupported invocation_id={first.get('invocation_id')} "
            f"realization={first.get('duration_realization')} "
            f"bucket={first.get('target_duration_class')}"
        )

    replay_invocations = [
        replay_invocation(invocation, profile_id_for(invocation))
        for invocation in assignments["invocations"]
    ]
    created_at = datetime.now(timezone.utc).isoformat()
    profiles = build_profiles(assignments, replay_invocations)
    profiles["created_at"] = created_at
    profiles["source"] = {
        "semantic_assignments": str(assignments_path),
        "semantic_assignments_schema": assignments["schema"],
        "semantic_mix": assignments["policy"]["semantic_mix"],
        "seed": assignments["policy"]["seed"],
    }

    workload_counts = Counter(item["workload"] for item in replay_invocations)
    replay = {
        "version": 1,
        "schema": REPLAY_SCHEMA,
        "created_at": created_at,
        "source": {
            "semantic_assignments": str(assignments_path),
            "trace_ir": assignments["source"]["trace_ir"],
        },
        "policy": {
            **assignments["policy"],
            "allow_uncalibrated_slo": allow_uncalibrated_slo,
        },
        "summary": {
            **assignments["summary"],
            "workload_counts": dict(sorted(workload_counts.items())),
            "calibration_supported_invocations": len(replay_invocations) - len(unsupported),
            "calibration_unsupported_invocations": len(unsupported),
        },
        "profiles_path": "profiles.json",
        "invocations_path": "invocations.csv",
        "scheduler_metadata": {
            "enabled_by_default": True,
            "event_bridge_port_flag": "--event-bridge-port",
            "metadata_target_default": "openwhisk-container",
            "inline_profile_hints": True,
            "profile_hints_schema": "cosmos_metadata_model.ProfileHints",
            "fields": [
                "profile_id",
                "profile_hints",
                "slo_class",
                "target_duration_ms",
                "deadline_us",
            ],
            "replay_runner": "benchmarks/semantic_benchmark/run_openwhisk_semantic_replay.py",
        },
        "invocations": replay_invocations,
    }
    fidelity = build_fidelity(assignments, replay_invocations, created_at)
    return profiles, replay, fidelity, replay_invocations


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        assignments = read_assignments(args.semantic_assignments)
        profiles, replay, fidelity, invocations = build_replay_payloads(
            assignments,
            args.semantic_assignments,
            args.allow_uncalibrated_slo,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "profiles.json").write_text(
        json.dumps(profiles, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (args.output_dir / "replay.json").write_text(
        json.dumps(replay, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (args.output_dir / "fidelity.json").write_text(
        json.dumps(fidelity, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_invocations_csv(args.output_dir / "invocations.csv", invocations)
    print(args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
