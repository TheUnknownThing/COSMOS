#!/usr/bin/env python3

from __future__ import annotations

import csv
import gzip
import hashlib
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, TypeVar


TRIGGER_GROUPS = {
    "http",
    "timer",
    "event",
    "queue",
    "storage",
    "orchestration",
    "others",
}

WORKLOAD_FAMILIES = {
    "cpu_bound",
    "network_service",
    "storage_io",
    "event_pipeline",
    "memory_heavy",
    "orchestration",
    "timer_control",
}

T = TypeVar("T")


@dataclass(frozen=True)
class FunctionFeatures:
    function_id: str
    owner: str
    app: str
    func: str
    trigger: str
    invocation_count: int
    active_minutes: int
    burstiness: float
    periodicity_score: float
    duration_p25_ms: float
    median_duration_ms: float
    duration_p75_ms: float
    p90_duration_ms: float
    duration_p99_ms: float
    duration_max_ms: float
    memory_mb: float
    memory_p75_mb: float
    memory_p95_mb: float
    memory_p99_mb: float
    app_function_count: int


@dataclass(frozen=True)
class ClassifiedProfile:
    profile_id: str
    family: str
    confidence: float
    probabilities: dict[str, float]
    features: FunctionFeatures
    reason: list[str]


def open_csv_rows(path: Path) -> Iterable[dict[str, Any]]:
    if path.suffix.lower() == ".gz" or path.name.endswith(".csv.gz"):
        with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
            yield from csv.DictReader(fh)
    else:
        with path.open("r", encoding="utf-8", newline="") as fh:
            yield from csv.DictReader(fh)


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


def first_present(row: dict[str, Any], fields: Iterable[str]) -> Any | None:
    for field in fields:
        value = row.get(field)
        if value not in (None, ""):
            return value
    return None


def csv_files(root: Path, pattern: str) -> list[Path]:
    return sorted(
        path
        for path in root.rglob(pattern)
        if path.is_file()
        and (path.suffix.lower() in {".csv", ".gz"} or path.name.endswith(".csv.gz"))
    )


def stable_u64(text: str) -> int:
    return int.from_bytes(hashlib.sha256(text.encode("utf-8")).digest()[:8], "big")


def stable_choice(values: list[T], key: str) -> T:
    if not values:
        raise ValueError("stable_choice requires at least one value")
    return values[stable_u64(key) % len(values)]


def percentile(values: list[float] | list[int], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def weighted_percentile(values: list[tuple[float, int]], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted((value, max(1, weight)) for value, weight in values)
    total = sum(weight for _, weight in ordered)
    target = max(1.0, total * pct)
    running = 0
    for value, weight in ordered:
        running += weight
        if running >= target:
            return value
    return ordered[-1][0]


def coefficient_of_variation(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    if mean <= 0:
        return 0.0
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance) / mean


def periodicity_score(counts: list[int]) -> float:
    active = [(index, count) for index, count in enumerate(counts) if count > 0]
    if len(active) < 3:
        return 0.0
    gaps = [right[0] - left[0] for left, right in zip(active, active[1:])]
    if not gaps:
        return 0.0
    cv = coefficient_of_variation([float(gap) for gap in gaps])
    density = len(active) / max(1, len(counts))
    return max(0.0, min(1.0, (1.0 - min(cv, 1.0)) * (1.0 - min(density, 0.95))))


def duration_class(duration_ms: float) -> str:
    if duration_ms <= 50:
        return "0-50ms"
    if duration_ms <= 200:
        return "50-200ms"
    if duration_ms <= 400:
        return "200-400ms"
    if duration_ms <= 2000:
        return "400ms-2s"
    return "2s+"


def memory_bucket(memory_mb: float) -> str:
    if memory_mb >= 1024:
        return "high"
    if memory_mb >= 512:
        return "medium"
    return "low"


class Missing2019DataError(RuntimeError):
    pass


def limit_files(paths: list[Path], max_files: int | None) -> list[Path]:
    if max_files is None:
        return paths
    if max_files <= 0:
        raise ValueError("max_files must be positive")
    return paths[:max_files]


def require_2019_dataset(
    root: Path, max_days: int | None = None
) -> tuple[list[Path], list[Path], list[Path]]:
    if not root.exists():
        raise Missing2019DataError(f"2019 dataset directory does not exist: {root}")
    invocation_files = limit_files(
        csv_files(root, "invocations_per_function_md.anon.d*.csv*"), max_days
    )
    duration_files = limit_files(
        csv_files(root, "function_durations_percentiles.anon.d*.csv*"), max_days
    )
    memory_files = limit_files(
        csv_files(root, "app_memory_percentiles.anon.d*.csv*"), max_days
    )
    missing = []
    if not invocation_files:
        missing.append("invocations_per_function_md.anon.d*.csv*")
    if not duration_files:
        missing.append("function_durations_percentiles.anon.d*.csv*")
    if not memory_files:
        missing.append("app_memory_percentiles.anon.d*.csv*")
    if missing:
        raise Missing2019DataError(
            f"2019 dataset at {root} is missing required file families: "
            + ", ".join(missing)
        )
    return invocation_files, duration_files, memory_files


def load_invocation_features(
    paths: list[Path],
    row_limit: int | None = None,
    allowed_functions: set[str] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    functions: dict[str, dict[str, Any]] = {}
    app_functions: dict[str, set[str]] = defaultdict(set)
    rows_seen = 0
    for path in paths:
        for row in open_csv_rows(path):
            if row_limit is not None and rows_seen >= row_limit:
                return functions, {
                    app: len(functions) for app, functions in app_functions.items()
                }
            rows_seen += 1
            app = first_present(row, ("HashApp", "app"))
            owner = first_present(row, ("HashOwner", "owner"))
            func = first_present(row, ("HashFunction", "func"))
            if app is None or func is None:
                continue
            owner = str(owner or "")
            app = str(app)
            func = str(func)
            function_id = f"{app}:{func}"
            if allowed_functions is not None and function_id not in allowed_functions:
                continue
            trigger = str(row.get("Trigger") or "others").strip().lower() or "others"
            if trigger not in TRIGGER_GROUPS:
                trigger = "others"
            minute_counts = [
                int(parse_float(value) or 0)
                for name, value in row.items()
                if str(name).isdigit()
            ]
            stats = functions.setdefault(
                function_id,
                {
                    "app": app,
                    "owner": owner,
                    "func": func,
                    "trigger_counts": Counter(),
                    "minute_counts": [],
                    "invocation_count": 0,
                },
            )
            total = sum(minute_counts)
            stats["trigger_counts"][trigger] += max(1, total)
            stats["minute_counts"].extend(minute_counts)
            stats["invocation_count"] += total
            app_functions[app].add(func)
    app_function_counts = {app: len(functions) for app, functions in app_functions.items()}
    return functions, app_function_counts


def load_duration_features(
    paths: list[Path], allowed_functions: set[str] | None = None
) -> dict[str, dict[str, float]]:
    values: dict[str, dict[str, list[tuple[float, int]]]] = defaultdict(lambda: defaultdict(list))
    remaining = set(allowed_functions) if allowed_functions is not None else None
    for path in paths:
        for row in open_csv_rows(path):
            if remaining is not None and not remaining:
                break
            app = first_present(row, ("HashApp", "app"))
            func = first_present(row, ("HashFunction", "func"))
            if app is None or func is None:
                continue
            function_id = f"{app}:{func}"
            if allowed_functions is not None and function_id not in allowed_functions:
                continue
            count = max(1, int(parse_float(row.get("Count")) or 1))
            fields = {
                "p25": ("percentile_Average_25", "percentile_Average_50", "Average"),
                "median": ("percentile_Average_50", "Average", "percentile_Average_75"),
                "p75": ("percentile_Average_75", "percentile_Average_50", "Average"),
                "p99": ("percentile_Average_99", "percentile_Average_75", "Maximum", "Average"),
                "max": ("Maximum", "percentile_Average_100", "percentile_Average_99", "Average"),
            }
            for key, candidates in fields.items():
                value = parse_float(first_present(row, candidates))
                if value is not None and value > 0:
                    values[function_id][key].append((value, count))
            if remaining is not None and function_id in remaining and values[function_id]:
                remaining.discard(function_id)
        if remaining is not None and not remaining:
            break
    return {
        function_id: {
            "duration_p25_ms": weighted_percentile(data.get("p25", data.get("median", [])), 0.25),
            "median_duration_ms": weighted_percentile(data.get("median", []), 0.50),
            "duration_p75_ms": weighted_percentile(data.get("p75", data.get("median", [])), 0.75),
            "p90_duration_ms": weighted_percentile(data.get("p99", data.get("median", [])), 0.90),
            "duration_p99_ms": weighted_percentile(data.get("p99", data.get("median", [])), 0.99),
            "duration_max_ms": weighted_percentile(data.get("max", data.get("p99", [])), 1.0),
        }
        for function_id, data in values.items()
    }


def load_memory_features(
    paths: list[Path], allowed_apps: set[str] | None = None
) -> dict[str, dict[str, float]]:
    app_values: dict[str, dict[str, list[tuple[float, int]]]] = defaultdict(lambda: defaultdict(list))
    remaining = set(allowed_apps) if allowed_apps is not None else None
    for path in paths:
        for row in open_csv_rows(path):
            if remaining is not None and not remaining:
                break
            app = first_present(row, ("HashApp", "app"))
            if app is None:
                continue
            app = str(app)
            if allowed_apps is not None and app not in allowed_apps:
                continue
            sample_count = max(1, int(parse_float(row.get("SampleCount")) or 1))
            fields = {
                "p50": ("AverageAllocatedMb_pct50", "AverageAllocatedMb", "AverageAllocatedMb_pct75"),
                "p75": ("AverageAllocatedMb_pct75", "AverageAllocatedMb_pct50", "AverageAllocatedMb"),
                "p95": ("AverageAllocatedMb_pct95", "AverageAllocatedMb_pct99", "AverageAllocatedMb_pct75"),
                "p99": ("AverageAllocatedMb_pct99", "AverageAllocatedMb_pct95", "AverageAllocatedMb_pct75"),
            }
            found = False
            for key, candidates in fields.items():
                mb = parse_float(first_present(row, candidates))
                if mb is not None and mb > 0:
                    app_values[app][key].append((mb, sample_count))
                    found = True
            if found:
                if remaining is not None:
                    remaining.discard(app)
        if remaining is not None and not remaining:
            break
    return {
        app: {
            "memory_mb": weighted_percentile(values.get("p50", []), 0.50),
            "memory_p75_mb": weighted_percentile(values.get("p75", values.get("p50", [])), 0.75),
            "memory_p95_mb": weighted_percentile(values.get("p95", values.get("p75", [])), 0.95),
            "memory_p99_mb": weighted_percentile(values.get("p99", values.get("p95", [])), 0.99),
        }
        for app, values in app_values.items()
        if values
    }


def build_feature_catalog(
    root: Path,
    max_profiles: int | None = None,
    max_days: int | None = None,
    invocation_row_limit: int | None = None,
    allowed_functions: set[str] | None = None,
) -> list[FunctionFeatures]:
    invocation_files, duration_files, memory_files = require_2019_dataset(root, max_days)
    invocation_features, app_function_counts = load_invocation_features(
        invocation_files, invocation_row_limit, allowed_functions
    )
    durations = load_duration_features(duration_files, set(invocation_features))
    memories = load_memory_features(
        memory_files, {str(item["app"]) for item in invocation_features.values()}
    )

    catalog: list[FunctionFeatures] = []
    for function_id, invocation in invocation_features.items():
        app = str(invocation["app"])
        duration = durations.get(function_id)
        memory = memories.get(app)
        if duration is None or memory is None:
            continue
        minute_counts = list(invocation["minute_counts"])
        active_counts = [float(count) for count in minute_counts if count > 0]
        trigger_counts: Counter[str] = invocation["trigger_counts"]
        trigger = trigger_counts.most_common(1)[0][0] if trigger_counts else "others"
        catalog.append(
            FunctionFeatures(
                function_id=function_id,
                owner=str(invocation["owner"]),
                app=app,
                func=str(invocation["func"]),
                trigger=trigger,
                invocation_count=int(invocation["invocation_count"]),
                active_minutes=len(active_counts),
                burstiness=coefficient_of_variation(active_counts),
                periodicity_score=periodicity_score(minute_counts),
                duration_p25_ms=float(duration["duration_p25_ms"]),
                median_duration_ms=float(duration["median_duration_ms"]),
                duration_p75_ms=float(duration["duration_p75_ms"]),
                p90_duration_ms=float(duration["p90_duration_ms"]),
                duration_p99_ms=float(duration["duration_p99_ms"]),
                duration_max_ms=float(duration["duration_max_ms"]),
                memory_mb=float(memory["memory_mb"]),
                memory_p75_mb=float(memory["memory_p75_mb"]),
                memory_p95_mb=float(memory["memory_p95_mb"]),
                memory_p99_mb=float(memory["memory_p99_mb"]),
                app_function_count=app_function_counts.get(app, 1),
            )
        )
    if not catalog:
        raise Missing2019DataError(
            f"2019 dataset at {root} had required files but no joinable functions "
            "with invocation, duration, and app-memory data"
        )
    catalog.sort(key=lambda item: item.function_id)
    if max_profiles is not None:
        if max_profiles <= 0:
            raise ValueError("max_profiles must be positive")
        if len(catalog) > max_profiles:
            catalog = sorted(
                catalog,
                key=lambda item: stable_u64(f"{item.function_id}:az2019-profile-cap"),
            )[:max_profiles]
            catalog.sort(key=lambda item: item.function_id)
    return catalog


def add_score(
    scores: dict[str, float],
    reason: list[str],
    family: str,
    amount: float,
    text: str,
) -> None:
    scores[family] = scores.get(family, 0.0) + amount
    reason.append(f"{family}+{amount:.2f}:{text}")


def classify_features(features: FunctionFeatures) -> tuple[dict[str, float], list[str]]:
    scores = {family: 0.05 for family in WORKLOAD_FAMILIES}
    reason: list[str] = []
    trigger = features.trigger
    cls = duration_class(features.median_duration_ms)
    mem = memory_bucket(features.memory_mb)

    if trigger == "http":
        add_score(scores, reason, "network_service", 2.2, "http trigger")
        if cls in {"0-50ms", "50-200ms"}:
            add_score(scores, reason, "cpu_bound", 0.6, "short http function")
    elif trigger == "storage":
        add_score(scores, reason, "storage_io", 2.6, "storage trigger")
        if cls in {"400ms-2s", "2s+"}:
            add_score(scores, reason, "event_pipeline", 0.5, "long storage function")
    elif trigger in {"queue", "event"}:
        add_score(scores, reason, "event_pipeline", 1.5, f"{trigger} trigger")
        add_score(scores, reason, "storage_io", 0.8, f"{trigger} trigger")
    elif trigger == "timer":
        add_score(scores, reason, "timer_control", 1.8, "timer trigger")
        if features.periodicity_score > 0.4:
            add_score(scores, reason, "timer_control", 0.6, "periodic invocations")
    elif trigger == "orchestration":
        add_score(scores, reason, "orchestration", 2.4, "orchestration trigger")
        add_score(scores, reason, "event_pipeline", 0.8, "orchestration trigger")
    else:
        add_score(scores, reason, "cpu_bound", 0.6, "unknown trigger fallback")

    if mem == "high":
        add_score(scores, reason, "memory_heavy", 2.1, "high app memory")
    elif mem == "medium":
        add_score(scores, reason, "memory_heavy", 0.6, "medium app memory")

    if cls in {"400ms-2s", "2s+"}:
        add_score(scores, reason, "event_pipeline", 0.9, "long median duration")
    elif cls in {"0-50ms", "50-200ms"}:
        add_score(scores, reason, "cpu_bound", 0.5, "short median duration")

    if features.p90_duration_ms > 750:
        add_score(scores, reason, "event_pipeline", 0.7, "high p90 duration")
    if features.burstiness > 2.0:
        add_score(scores, reason, "event_pipeline", 0.4, "bursty invocation counts")
    if features.app_function_count >= 4:
        add_score(scores, reason, "orchestration", 0.3, "multi-function app")

    total = sum(scores.values())
    probabilities = {
        family: score / total for family, score in sorted(scores.items())
    }
    return probabilities, reason


def classify_catalog(catalog: list[FunctionFeatures]) -> list[ClassifiedProfile]:
    profiles: list[ClassifiedProfile] = []
    for index, features in enumerate(catalog, start=1):
        probabilities, reason = classify_features(features)
        family, confidence = max(
            probabilities.items(), key=lambda item: (item[1], item[0])
        )
        profiles.append(
            ClassifiedProfile(
                profile_id=f"az2019p_{index:06d}",
                family=family,
                confidence=confidence,
                probabilities=probabilities,
                features=features,
                reason=reason,
            )
        )
    return profiles


def load_classified_profiles(
    root: Path,
    max_profiles: int | None = None,
    max_days: int | None = None,
    invocation_row_limit: int | None = None,
    allowed_functions: set[str] | None = None,
) -> list[ClassifiedProfile]:
    return classify_catalog(
        build_feature_catalog(
            root,
            max_profiles,
            max_days,
            invocation_row_limit,
            allowed_functions,
        )
    )


def select_profile_for_function(
    profiles: list[ClassifiedProfile],
    function_id: str,
    target_duration_ms: int,
) -> ClassifiedProfile:
    cls = duration_class(float(target_duration_ms))
    same_class = [
        profile
        for profile in profiles
        if duration_class(profile.features.median_duration_ms) == cls
    ]
    candidates = same_class or profiles
    return stable_choice(candidates, f"{function_id}:{target_duration_ms}:az2019-profile")
