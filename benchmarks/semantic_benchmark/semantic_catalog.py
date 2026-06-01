#!/usr/bin/env python3

from __future__ import annotations

from collections import Counter
from typing import Any

from trace_ir import stable_u64


CATALOG_VERSION = 1
CATALOG_SCHEMA = "cosmos.semantic.sebs-anchor-realization-catalog"
ASSIGNMENT_SCHEMA = "cosmos.semantic.azure-2021-sebs-assignment"
DURATION_BUCKETS = ("0-50ms", "50-200ms", "200-400ms", "400ms-2s", "2s+")
RESOURCE_CLASSES = ("cpu", "io", "memory", "network", "balanced")
SUPPORTED_MIXES = ("cpu-heavy", "io-heavy", "memory-heavy", "network-heavy", "balanced")
CALIBRATION_SCHEMA = "cosmos.semantic.duration-calibration"
REPLAY_SCHEMA = "cosmos.semantic.azure-2021-openwhisk-replay"
PROFILES_SCHEMA = "cosmos.semantic.azure-2021-openwhisk-profiles"


BUCKET_BOUNDS_MS: dict[str, tuple[int, int | None]] = {
    "0-50ms": (0, 50),
    "50-200ms": (50, 200),
    "200-400ms": (200, 400),
    "400ms-2s": (400, 2000),
    "2s+": (2000, None),
}

DEFAULT_BUCKET_TARGET_MS: dict[str, int] = {
    "0-50ms": 25,
    "50-200ms": 125,
    "200-400ms": 300,
    "400ms-2s": 1000,
    "2s+": 2100,
}

KERNEL_MODE_BY_REALIZATION: dict[str, str] = {
    "cpu-spin-controllable": "cpu",
    "memory-scan-controllable": "memory",
    "storage-io-controllable": "io",
    "network-transfer-controllable": "network",
    "balanced-pipeline-controllable": "balanced",
}

UPSTREAM_SEBS_REALIZATION_ID = "upstream-sebs-calibrated"

CONTROLLABLE_REALIZATION_BY_RESOURCE_CLASS: dict[str, str] = {
    "cpu": "cpu-spin-controllable",
    "io": "storage-io-controllable",
    "memory": "memory-scan-controllable",
    "network": "network-transfer-controllable",
    "balanced": "balanced-pipeline-controllable",
}


SEBS_ANCHORS: dict[str, dict[str, Any]] = {
    "010.sleep": {
        "benchmark_id": "010.sleep",
        "action_name": "sebs_sleep",
        "benchmark_path": "000.microbenchmarks/010.sleep",
        "resource_class": "balanced",
        "runtime_options": ["python", "nodejs", "java", "cpp"],
        "input_size_options": ["test"],
        "resource_hints": {
            "source": "sebs-anchor-semantics",
            "measured": False,
            "primary_resource": "balanced",
        },
        "cold_warm_assumptions": {
            "cold_start": "runtime-only cold path; benchmark body is intentionally light",
            "warm": "duration dominated by requested sleep interval",
        },
        "requires_storage": False,
        "requires_external_service": False,
    },
    "020.network-benchmark": {
        "benchmark_id": "020.network-benchmark",
        "action_name": "sebs_network_benchmark",
        "benchmark_path": "000.microbenchmarks/020.network-benchmark",
        "resource_class": "network",
        "runtime_options": ["python"],
        "input_size_options": ["test"],
        "resource_hints": {
            "source": "sebs-anchor-semantics",
            "measured": False,
            "primary_resource": "network",
        },
        "cold_warm_assumptions": {
            "cold_start": "network client setup can appear on the cold path",
            "warm": "payload transfer and response handling dominate",
        },
        "requires_storage": False,
        "requires_external_service": True,
    },
    "030.clock-synchronization": {
        "benchmark_id": "030.clock-synchronization",
        "action_name": "sebs_clock_synchronization",
        "benchmark_path": "000.microbenchmarks/030.clock-synchronization",
        "resource_class": "network",
        "runtime_options": ["python"],
        "input_size_options": ["test"],
        "resource_hints": {
            "source": "sebs-anchor-semantics",
            "measured": False,
            "primary_resource": "network",
        },
        "cold_warm_assumptions": {
            "cold_start": "runtime initialization plus clock endpoint setup",
            "warm": "short network round-trip sequence",
        },
        "requires_storage": False,
        "requires_external_service": True,
    },
    "040.server-reply": {
        "benchmark_id": "040.server-reply",
        "action_name": "sebs_server_reply",
        "benchmark_path": "000.microbenchmarks/040.server-reply",
        "resource_class": "network",
        "runtime_options": ["python"],
        "input_size_options": ["test"],
        "resource_hints": {
            "source": "sebs-anchor-semantics",
            "measured": False,
            "primary_resource": "network",
        },
        "cold_warm_assumptions": {
            "cold_start": "server connection/client setup can be visible",
            "warm": "reply payload and framework overhead dominate",
        },
        "requires_storage": False,
        "requires_external_service": True,
    },
    "110.dynamic-html": {
        "benchmark_id": "110.dynamic-html",
        "action_name": "sebs_dynamic_html",
        "benchmark_path": "100.webapps/110.dynamic-html",
        "resource_class": "cpu",
        "runtime_options": ["python", "nodejs", "java"],
        "input_size_options": ["test", "small", "large"],
        "resource_hints": {
            "source": "sebs-anchor-semantics",
            "measured": False,
            "primary_resource": "cpu",
        },
        "cold_warm_assumptions": {
            "cold_start": "template/runtime imports are cold-path costs",
            "warm": "string generation and templating dominate",
        },
        "requires_storage": False,
        "requires_external_service": False,
    },
    "120.uploader": {
        "benchmark_id": "120.uploader",
        "action_name": "sebs_uploader",
        "benchmark_path": "100.webapps/120.uploader",
        "resource_class": "io",
        "runtime_options": ["python", "nodejs"],
        "input_size_options": ["test", "small", "large"],
        "resource_hints": {
            "source": "sebs-anchor-semantics",
            "measured": False,
            "primary_resource": "io",
        },
        "cold_warm_assumptions": {
            "cold_start": "storage client initialization is visible",
            "warm": "object upload/download path dominates",
        },
        "requires_storage": True,
        "requires_external_service": False,
    },
    "130.crud-api": {
        "benchmark_id": "130.crud-api",
        "action_name": "sebs_crud_api",
        "benchmark_path": "100.webapps/130.crud-api",
        "resource_class": "io",
        "runtime_options": ["python", "nodejs"],
        "input_size_options": ["test", "small"],
        "resource_hints": {
            "source": "sebs-anchor-semantics",
            "measured": False,
            "primary_resource": "io",
        },
        "cold_warm_assumptions": {
            "cold_start": "database client setup and connection establishment",
            "warm": "key-value read/write operations dominate",
        },
        "requires_storage": False,
        "requires_external_service": True,
    },
    "210.thumbnailer": {
        "benchmark_id": "210.thumbnailer",
        "action_name": "sebs_thumbnailer",
        "benchmark_path": "200.multimedia/210.thumbnailer",
        "resource_class": "balanced",
        "runtime_options": ["python", "nodejs", "cpp"],
        "input_size_options": ["test", "small", "large"],
        "resource_hints": {
            "source": "sebs-anchor-semantics",
            "measured": False,
            "primary_resource": "balanced",
        },
        "cold_warm_assumptions": {
            "cold_start": "image library import and storage client setup",
            "warm": "download, image transform, and upload phases",
        },
        "requires_storage": True,
        "requires_external_service": False,
    },
    "220.video-processing": {
        "benchmark_id": "220.video-processing",
        "action_name": "sebs_video_processing",
        "benchmark_path": "200.multimedia/220.video-processing",
        "resource_class": "balanced",
        "runtime_options": ["python"],
        "input_size_options": ["test", "small", "large"],
        "resource_hints": {
            "source": "sebs-anchor-semantics",
            "measured": False,
            "primary_resource": "balanced",
        },
        "cold_warm_assumptions": {
            "cold_start": "media toolchain and storage setup can be expensive",
            "warm": "decode, transform, encode, and storage phases",
        },
        "requires_storage": True,
        "requires_external_service": False,
    },
    "311.compression": {
        "benchmark_id": "311.compression",
        "action_name": "sebs_compression",
        "benchmark_path": "300.utilities/311.compression",
        "resource_class": "cpu",
        "runtime_options": ["python", "nodejs"],
        "input_size_options": ["test", "small", "large"],
        "resource_hints": {
            "source": "sebs-anchor-semantics",
            "measured": False,
            "primary_resource": "cpu",
        },
        "cold_warm_assumptions": {
            "cold_start": "compression library import and storage client setup",
            "warm": "compression loop dominates with bounded I/O",
        },
        "requires_storage": True,
        "requires_external_service": False,
    },
    "411.image-recognition": {
        "benchmark_id": "411.image-recognition",
        "action_name": "sebs_image_recognition",
        "benchmark_path": "400.inference/411.image-recognition",
        "resource_class": "memory",
        "runtime_options": ["python", "cpp"],
        "input_size_options": ["test", "small", "large"],
        "resource_hints": {
            "source": "sebs-anchor-semantics",
            "measured": False,
            "primary_resource": "memory",
        },
        "cold_warm_assumptions": {
            "cold_start": "model load is expected to dominate",
            "warm": "inference and image decode dominate",
        },
        "requires_storage": True,
        "requires_external_service": False,
    },
    "501.graph-pagerank": {
        "benchmark_id": "501.graph-pagerank",
        "action_name": "sebs_graph_pagerank",
        "benchmark_path": "500.scientific/501.graph-pagerank",
        "resource_class": "cpu",
        "runtime_options": ["python", "cpp"],
        "input_size_options": ["test", "small", "large"],
        "resource_hints": {
            "source": "sebs-anchor-semantics",
            "measured": False,
            "primary_resource": "cpu",
        },
        "cold_warm_assumptions": {
            "cold_start": "scientific runtime imports and graph allocation",
            "warm": "iterative graph computation dominates",
        },
        "requires_storage": False,
        "requires_external_service": False,
    },
    "502.graph-mst": {
        "benchmark_id": "502.graph-mst",
        "action_name": "sebs_graph_mst",
        "benchmark_path": "500.scientific/502.graph-mst",
        "resource_class": "cpu",
        "runtime_options": ["python"],
        "input_size_options": ["test", "small", "large"],
        "resource_hints": {
            "source": "sebs-anchor-semantics",
            "measured": False,
            "primary_resource": "cpu",
        },
        "cold_warm_assumptions": {
            "cold_start": "scientific runtime imports and graph allocation",
            "warm": "graph traversal and heap work dominate",
        },
        "requires_storage": False,
        "requires_external_service": False,
    },
    "503.graph-bfs": {
        "benchmark_id": "503.graph-bfs",
        "action_name": "sebs_graph_bfs",
        "benchmark_path": "500.scientific/503.graph-bfs",
        "resource_class": "cpu",
        "runtime_options": ["python", "cpp"],
        "input_size_options": ["test", "small", "large"],
        "resource_hints": {
            "source": "sebs-anchor-semantics",
            "measured": False,
            "primary_resource": "cpu",
        },
        "cold_warm_assumptions": {
            "cold_start": "graph allocation and runtime imports",
            "warm": "graph traversal dominates",
        },
        "requires_storage": False,
        "requires_external_service": False,
    },
    "504.dna-visualisation": {
        "benchmark_id": "504.dna-visualisation",
        "action_name": "sebs_dna_visualisation",
        "benchmark_path": "500.scientific/504.dna-visualisation",
        "resource_class": "memory",
        "runtime_options": ["python"],
        "input_size_options": ["test", "small", "large"],
        "resource_hints": {
            "source": "sebs-anchor-semantics",
            "measured": False,
            "primary_resource": "memory",
        },
        "cold_warm_assumptions": {
            "cold_start": "scientific package imports and working-set allocation",
            "warm": "memory-heavy scientific transformation dominates",
        },
        "requires_storage": False,
        "requires_external_service": False,
    },
}


REALIZATIONS: dict[str, dict[str, Any]] = {
    "cpu-spin-controllable": {
        "semantic_source": "sebs-inspired-controllable",
        "resource_class": "cpu",
        "supported_duration_buckets": list(DURATION_BUCKETS),
        "calibration_command": (
            "benchmarks/semantic_benchmark/kernels/semantic_kernel "
            "--mode cpu --target-us {target_duration_us}"
        ),
        "expected_phase_sequence": [{"kind": "CpuBound", "duration_pct": 100}],
        "resource_knobs": {
            "loop_count": "calibrated from target_duration_ms",
            "repeat_count": "ceil(target_duration_ms / calibrated_loop_ms)",
        },
        "uses_upstream_sebs_directly": False,
    },
    "memory-scan-controllable": {
        "semantic_source": "sebs-inspired-controllable",
        "resource_class": "memory",
        "supported_duration_buckets": list(DURATION_BUCKETS),
        "calibration_command": (
            "benchmarks/semantic_benchmark/kernels/semantic_kernel "
            "--mode memory --target-us {target_duration_us}"
        ),
        "expected_phase_sequence": [{"kind": "MemoryBound", "duration_pct": 100}],
        "resource_knobs": {
            "working_set_size": "bucket- and anchor-derived bytes",
            "repeat_count": "calibrated from target_duration_ms",
        },
        "uses_upstream_sebs_directly": False,
    },
    "storage-io-controllable": {
        "semantic_source": "sebs-inspired-controllable",
        "resource_class": "io",
        "supported_duration_buckets": list(DURATION_BUCKETS),
        "calibration_command": (
            "benchmarks/semantic_benchmark/kernels/semantic_kernel "
            "--mode io --target-us {target_duration_us}"
        ),
        "expected_phase_sequence": [{"kind": "IoBound", "duration_pct": 100}],
        "resource_knobs": {
            "bytes": "bucket- and anchor-derived byte count",
            "transfer_size": "chunk size for local object/storage emulation",
            "repeat_count": "calibrated from target_duration_ms",
        },
        "uses_upstream_sebs_directly": False,
    },
    "network-transfer-controllable": {
        "semantic_source": "sebs-inspired-controllable",
        "resource_class": "network",
        "supported_duration_buckets": list(DURATION_BUCKETS),
        "calibration_command": (
            "benchmarks/semantic_benchmark/kernels/semantic_kernel "
            "--mode network --target-us {target_duration_us}"
        ),
        "expected_phase_sequence": [{"kind": "NetworkBound", "duration_pct": 100}],
        "resource_knobs": {
            "transfer_size": "bucket- and anchor-derived payload bytes",
            "repeat_count": "calibrated from target_duration_ms",
        },
        "uses_upstream_sebs_directly": False,
    },
    "balanced-pipeline-controllable": {
        "semantic_source": "sebs-inspired-controllable",
        "resource_class": "balanced",
        "supported_duration_buckets": list(DURATION_BUCKETS),
        "calibration_command": (
            "benchmarks/semantic_benchmark/kernels/semantic_kernel "
            "--mode balanced --target-us {target_duration_us}"
        ),
        "expected_phase_sequence": [
            {"kind": "IoBound", "duration_pct": 30},
            {"kind": "CpuBound", "duration_pct": 40},
            {"kind": "MemoryBound", "duration_pct": 30},
        ],
        "resource_knobs": {
            "bytes": "bucket- and anchor-derived byte count",
            "loop_count": "calibrated from target_duration_ms",
            "working_set_size": "bounded by anchor memory hint",
            "repeat_count": "calibrated from target_duration_ms",
        },
        "uses_upstream_sebs_directly": False,
    },
    UPSTREAM_SEBS_REALIZATION_ID: {
        "semantic_source": "upstream-sebs",
        "resource_class": "anchor",
        "supported_duration_buckets": list(DURATION_BUCKETS),
        "calibration_command": (
            "sebs benchmark invoke {sebs_anchor} --config openwhisk --input-size {input_size}"
        ),
        "expected_phase_sequence": [{"kind": "UpstreamSeBS", "duration_pct": 100}],
        "resource_knobs": {
            "input_size": "SeBS input-size option",
            "repeat_count": "only after Phase 5 calibration proves fit",
        },
        "uses_upstream_sebs_directly": True,
        "calibration_required": True,
        "selection_policy": (
            "Prefer this realization only when Phase 5 calibration for the exact "
            "SeBS anchor and duration bucket marks it supported."
        ),
    },
}


MIX_POLICIES: dict[str, dict[str, int]] = {
    "cpu-heavy": {"cpu": 70, "io": 8, "memory": 8, "network": 6, "balanced": 8},
    "io-heavy": {"cpu": 8, "io": 68, "memory": 8, "network": 8, "balanced": 8},
    "memory-heavy": {"cpu": 8, "io": 8, "memory": 68, "network": 8, "balanced": 8},
    "network-heavy": {"cpu": 8, "io": 8, "memory": 8, "network": 68, "balanced": 8},
    "balanced": {"cpu": 24, "io": 20, "memory": 18, "network": 20, "balanced": 18},
}


def duration_bucket(target_duration_ms: int | float) -> str:
    if target_duration_ms < 50:
        return "0-50ms"
    if target_duration_ms < 200:
        return "50-200ms"
    if target_duration_ms < 400:
        return "200-400ms"
    if target_duration_ms < 2000:
        return "400ms-2s"
    return "2s+"


def bucket_upper_ms(bucket: str) -> int | None:
    return BUCKET_BOUNDS_MS[bucket][1]


def bucket_lower_ms(bucket: str) -> int:
    return BUCKET_BOUNDS_MS[bucket][0]


def is_controllable_realization(realization_id: str) -> bool:
    return realization_id in KERNEL_MODE_BY_REALIZATION


def is_upstream_sebs_realization(realization_id: str) -> bool:
    return realization_id == UPSTREAM_SEBS_REALIZATION_ID


def catalog_payload() -> dict[str, Any]:
    return {
        "version": CATALOG_VERSION,
        "schema": CATALOG_SCHEMA,
        "duration_buckets": list(DURATION_BUCKETS),
        "resource_classes": list(RESOURCE_CLASSES),
        "semantic_mixes": MIX_POLICIES,
        "anchors": SEBS_ANCHORS,
        "realizations": REALIZATIONS,
        "notes": [
            "Anchors describe SeBS semantic/resource shape.",
            "Phase 5/6 assignment prefers calibrated upstream SeBS for the exact "
            "anchor and duration bucket, then falls back to SeBS-inspired "
            "controllable kernels only when no upstream SeBS fit is available.",
        ],
    }


def validate_mix(mix: str) -> None:
    if mix not in MIX_POLICIES:
        raise ValueError(f"unsupported semantic mix: {mix}")


def anchors_by_resource_class() -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {resource_class: [] for resource_class in RESOURCE_CLASSES}
    for anchor_id, anchor in SEBS_ANCHORS.items():
        grouped[anchor["resource_class"]].append(anchor_id)
    return {key: sorted(value) for key, value in grouped.items()}


def choose_weighted_resource_class(function_id: str, mix: str, seed: int) -> str:
    validate_mix(mix)
    policy = MIX_POLICIES[mix]
    total = sum(policy.values())
    point = stable_u64(f"{seed}:{mix}:{function_id}:resource") % total
    cursor = 0
    for resource_class in RESOURCE_CLASSES:
        cursor += policy[resource_class]
        if point < cursor:
            return resource_class
    return RESOURCE_CLASSES[-1]


def choose_anchor(function_id: str, mix: str, seed: int) -> str:
    grouped = anchors_by_resource_class()
    resource_class = choose_weighted_resource_class(function_id, mix, seed)
    candidates = grouped[resource_class]
    index = stable_u64(f"{seed}:{mix}:{function_id}:anchor") % len(candidates)
    return candidates[index]


def realization_candidates(
    anchor_id: str,
    target_duration_ms: int,
    allow_uncalibrated_upstream: bool = False,
) -> list[str]:
    bucket = duration_bucket(target_duration_ms)
    anchor = SEBS_ANCHORS[anchor_id]
    resource_class = anchor["resource_class"]
    upstream_candidates: list[str] = []
    controllable_candidates: list[str] = []
    for realization_id, realization in REALIZATIONS.items():
        if bucket not in realization["supported_duration_buckets"]:
            continue
        if realization.get("uses_upstream_sebs_directly"):
            if allow_uncalibrated_upstream:
                upstream_candidates.append(realization_id)
            continue
        if realization["resource_class"] == resource_class:
            controllable_candidates.append(realization_id)
    if controllable_candidates:
        return sorted(upstream_candidates) + sorted(controllable_candidates)
    fallback = [
        realization_id
        for realization_id, realization in sorted(REALIZATIONS.items())
        if (
            bucket in realization["supported_duration_buckets"]
            and realization["resource_class"] == "balanced"
            and not realization.get("uses_upstream_sebs_directly", False)
        )
    ]
    return sorted(upstream_candidates) + fallback


def upstream_realization_candidates(anchor_id: str, target_duration_ms: int) -> list[str]:
    bucket = duration_bucket(target_duration_ms)
    realization = REALIZATIONS[UPSTREAM_SEBS_REALIZATION_ID]
    if bucket not in realization["supported_duration_buckets"]:
        return []
    if anchor_id not in SEBS_ANCHORS:
        raise ValueError(f"unsupported SeBS anchor: {anchor_id}")
    return [UPSTREAM_SEBS_REALIZATION_ID]


def controllable_realization_candidates(anchor_id: str, target_duration_ms: int) -> list[str]:
    bucket = duration_bucket(target_duration_ms)
    anchor = SEBS_ANCHORS[anchor_id]
    resource_class = anchor["resource_class"]
    preferred = CONTROLLABLE_REALIZATION_BY_RESOURCE_CLASS[resource_class]
    if bucket in REALIZATIONS[preferred]["supported_duration_buckets"]:
        return [preferred]
    fallback = CONTROLLABLE_REALIZATION_BY_RESOURCE_CLASS["balanced"]
    if bucket in REALIZATIONS[fallback]["supported_duration_buckets"]:
        return [fallback]
    return []


def choose_realization(
    anchor_id: str,
    invocation_id: int,
    target_duration_ms: int,
    seed: int,
    allow_uncalibrated_upstream: bool = False,
) -> str:
    candidates = realization_candidates(
        anchor_id,
        target_duration_ms,
        allow_uncalibrated_upstream,
    )
    if not candidates:
        bucket = duration_bucket(target_duration_ms)
        raise ValueError(f"no realization supports anchor={anchor_id} bucket={bucket}")
    point = stable_u64(f"{seed}:{anchor_id}:{invocation_id}:{target_duration_ms}:realization")
    return candidates[point % len(candidates)]


def anchor_counts(function_mappings: dict[str, dict[str, Any]]) -> dict[str, int]:
    return dict(
        sorted(Counter(item["semantic_anchor"] for item in function_mappings.values()).items())
    )
