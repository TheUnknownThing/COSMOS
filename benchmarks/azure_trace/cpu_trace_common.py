#!/usr/bin/env python3

from __future__ import annotations

import csv
import gzip
import math
import re
from pathlib import Path
from typing import Any, Iterable

DEFAULT_PRIMARY_DATASET_DIR = (
    Path("benchmarks/third_party/AzurePublicDataset/data/azurefunctions-dataset2019")
)
DEFAULT_FALLBACK_DATASET_DIR = (
    Path(
        "feat/co-schedule/benchmarks/third_party/AzurePublicDataset/data/"
        "azurefunctions-dataset2019"
    )
)

TRIGGERS = {
    "http",
    "timer",
    "event",
    "queue",
    "storage",
    "orchestration",
    "others",
}


def resolve_default_dataset_dir() -> Path:
    if DEFAULT_PRIMARY_DATASET_DIR.exists():
        return DEFAULT_PRIMARY_DATASET_DIR
    return DEFAULT_FALLBACK_DATASET_DIR


def csv_files(root: Path, pattern: str) -> list[Path]:
    return sorted(
        path
        for path in root.rglob(pattern)
        if path.is_file()
        and (path.suffix.lower() in {".csv", ".gz"} or path.name.endswith(".csv.gz"))
    )


def open_csv_rows(path: Path) -> Iterable[dict[str, str]]:
    if path.suffix.lower() == ".gz" or path.name.endswith(".csv.gz"):
        with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
            yield from csv.DictReader(fh)
    else:
        with path.open("r", encoding="utf-8", newline="") as fh:
            yield from csv.DictReader(fh)


def first_present(row: dict[str, Any], fields: Iterable[str]) -> Any | None:
    for field in fields:
        value = row.get(field)
        if value not in (None, ""):
            return value
    return None


def parse_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def require_2019_cpu_dataset(root: Path, max_days: int | None = None) -> tuple[list[Path], list[Path]]:
    if not root.exists():
        raise FileNotFoundError(f"dataset directory does not exist: {root}")
    invocation_files = csv_files(root, "invocations_per_function_md.anon.d*.csv*")
    duration_files = csv_files(root, "function_durations_percentiles.anon.d*.csv*")
    if max_days is not None:
        if max_days <= 0:
            raise ValueError("max_days must be positive")
        invocation_files = invocation_files[:max_days]
        duration_files = duration_files[:max_days]
    missing = []
    if not invocation_files:
        missing.append("invocations_per_function_md.anon.d*.csv*")
    if not duration_files:
        missing.append("function_durations_percentiles.anon.d*.csv*")
    if missing:
        raise FileNotFoundError(
            f"dataset at {root} is missing required file families: {', '.join(missing)}"
        )
    return invocation_files, duration_files


def weighted_mean(values: list[tuple[float, int]]) -> float:
    if not values:
        return 0.0
    total_weight = sum(max(1, weight) for _value, weight in values)
    if total_weight <= 0:
        return 0.0
    weighted_sum = sum(value * max(1, weight) for value, weight in values)
    return weighted_sum / total_weight


def weighted_percentile(values: list[tuple[float, int]], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted((value, max(1, weight)) for value, weight in values)
    total = sum(weight for _value, weight in ordered)
    if total <= 0:
        return ordered[-1][0]
    target = max(1.0, total * pct)
    running = 0
    for value, weight in ordered:
        running += weight
        if running >= target:
            return value
    return ordered[-1][0]


def percentile(values: list[int], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    index = (len(ordered) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def normalize_trigger(trigger: str | None) -> str:
    normalized = (trigger or "others").strip().lower() or "others"
    if normalized not in TRIGGERS:
        return "others"
    return normalized


def duration_bucket(duration_ms: float) -> str:
    if duration_ms <= 10:
        return "0-10ms"
    if duration_ms <= 20:
        return "10-20ms"
    if duration_ms <= 50:
        return "20-50ms"
    if duration_ms <= 100:
        return "50-100ms"
    if duration_ms <= 200:
        return "100-200ms"
    if duration_ms <= 400:
        return "200-400ms"
    if duration_ms <= 1000:
        return "400ms-1s"
    if duration_ms <= 2000:
        return "1-2s"
    if duration_ms <= 5000:
        return "2-5s"
    return "5s+"


def slo_class_for_duration(duration_ms: int) -> int:
    if duration_ms <= 250:
        return 0
    if duration_ms <= 1000:
        return 1
    return 2


def deadline_us_for_duration(duration_ms: int, min_slack_ms: int = 5) -> int:
    duration_us = max(1, duration_ms) * 1000
    return duration_us + max(duration_us, min_slack_ms * 1000)


def day_index_from_path(path: Path, fallback: int) -> int:
    match = re.search(r"\.d(\d+)\.", path.name)
    if not match:
        return fallback
    return max(0, int(match.group(1)) - 1)

