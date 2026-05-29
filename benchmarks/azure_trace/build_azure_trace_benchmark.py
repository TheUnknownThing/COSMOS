#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import math
import shutil
import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


DEFAULT_2021_TRACE = Path(
    "benchmarks/third_party/AzurePublicDataset/data/AzureFunctionsInvocationTraceForTwoWeeksJan2021.rar"
)
DEFAULT_OUTPUT_DIR = Path("benchmarks/azure_trace/results/azure-trace-benchmark")
TRIGGER_WEIGHTS = {
    "http": 45,
    "timer": 18,
    "queue": 15,
    "event": 10,
    "storage": 7,
    "orchestration": 2,
    "others": 3,
}
DEFAULT_MEMORY_MB = [128, 256, 512, 768, 1024, 1536, 2048]
PROFILE_HINT_KEYS = {
    "cpu_intensity",
    "memory_bytes",
    "working_set_bytes",
    "io_weight",
    "io_bandwidth_bytes_per_sec",
    "network_bandwidth_bytes_per_sec",
    "phase_sequence",
    "cold_load_penalty_ns",
}


@dataclass(frozen=True)
class TraceEvent:
    source_start_ms: float
    function_id: str
    app: str
    func: str
    duration_ms: int


@dataclass
class Metrics:
    count: int = 0
    functions: Counter[str] | None = None
    apps: Counter[str] | None = None
    durations: list[float] | None = None
    starts: list[float] | None = None
    ends: list[float] | None = None

    def __post_init__(self) -> None:
        self.functions = Counter()
        self.apps = Counter()
        self.durations = []
        self.starts = []
        self.ends = []

    def add(self, event: TraceEvent, sample_limit: int) -> None:
        self.count += 1
        self.functions[event.function_id] += 1
        self.apps[event.app] += 1
        if len(self.durations) < sample_limit:
            self.durations.append(event.duration_ms)
            self.starts.append(event.source_start_ms)
            self.ends.append(event.source_start_ms + event.duration_ms)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a hybrid Azure Functions benchmark: 2021 arrivals plus "
            "2019-derived synthetic resource profiles."
        )
    )
    parser.add_argument("--trace-2021", type=Path, default=DEFAULT_2021_TRACE)
    parser.add_argument("--dataset-2019-dir", type=Path)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--window-start-ms", type=float, default=0.0)
    parser.add_argument("--window-ms", type=float)
    parser.add_argument("--scale", type=float, default=1.0)
    parser.add_argument("--limit", type=int)
    parser.add_argument(
        "--downsample-mode",
        choices=("top-apps", "stratified-apps", "hash-functions"),
        default="top-apps",
    )
    parser.add_argument("--min-slack-ms", type=int, default=5)
    parser.add_argument("--metrics-sample-limit", type=int, default=200_000)
    return parser.parse_args()


def open_csv_rows(path: Path) -> Iterable[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
            yield from csv.DictReader(fh)
    elif suffix == ".rar":
        if shutil.which("unrar") is None:
            raise RuntimeError(
                f"{path} is a RAR archive; install unrar or extract it before running this script"
            )
        process = subprocess.Popen(
            ["unrar", "p", "-inul", str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert process.stdout is not None
        with io.TextIOWrapper(process.stdout, encoding="utf-8", newline="") as fh:
            yield from csv.DictReader(fh)
        _, stderr = process.communicate()
        if process.returncode != 0:
            raise RuntimeError(
                f"unrar failed for {path}: {stderr.decode('utf-8', errors='replace')}"
            )
    else:
        with path.open("r", encoding="utf-8", newline="") as fh:
            yield from csv.DictReader(fh)


def parse_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def first_present(row: dict[str, Any], fields: Iterable[str]) -> Any | None:
    for field in fields:
        value = row.get(field)
        if value not in (None, ""):
            return value
    return None


def parse_2021_event(row: dict[str, Any]) -> TraceEvent | None:
    app = first_present(row, ("app", "HashApp"))
    func = first_present(row, ("func", "function", "HashFunction"))
    end_timestamp = parse_float(first_present(row, ("end_timestamp", "end_time", "end")))
    duration_s = parse_float(first_present(row, ("duration", "duration_s")))
    if app is None or func is None or end_timestamp is None or duration_s is None:
        return None
    duration_ms = max(1, int(round(duration_s * 1000.0)))
    start_ms = max(0.0, (end_timestamp - duration_s) * 1000.0)
    return TraceEvent(
        source_start_ms=start_ms,
        function_id=f"{app}:{func}",
        app=str(app),
        func=str(func),
        duration_ms=duration_ms,
    )


def iter_2021_events(path: Path) -> Iterable[TraceEvent]:
    for row in open_csv_rows(path):
        event = parse_2021_event(row)
        if event is not None:
            yield event


def stable_u64(text: str) -> int:
    return int.from_bytes(hashlib.sha256(text.encode("utf-8")).digest()[:8], "big")


def stable_choice(values: list[Any], key: str, default: Any) -> Any:
    if not values:
        return default
    return values[stable_u64(key) % len(values)]


def weighted_choice(weighted: list[tuple[Any, float]], key: str, default: Any) -> Any:
    total = sum(max(0.0, weight) for _, weight in weighted)
    if total <= 0:
        return default
    target = (stable_u64(key) / float(2**64)) * total
    running = 0.0
    for value, weight in weighted:
        running += max(0.0, weight)
        if running >= target:
            return value
    return weighted[-1][0]


def percentile(values: list[float] | list[int], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


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


def slo_for_duration(duration_ms: int) -> int:
    if duration_ms <= 250:
        return 0
    if duration_ms <= 1000:
        return 1
    return 2


def deadline_us(duration_ms: int, min_slack_ms: int) -> int:
    duration_us = duration_ms * 1000
    return duration_us + max(duration_us, min_slack_ms * 1000)


def csv_files(root: Path, pattern: str) -> list[Path]:
    return sorted(
        path
        for path in root.rglob(pattern)
        if path.is_file() and (path.suffix in {".csv", ".gz"} or path.name.endswith(".csv.gz"))
    )


def load_2019_distributions(root: Path | None) -> dict[str, Any]:
    distributions: dict[str, Any] = {
        "available": False,
        "trigger_weights": [(trigger, weight) for trigger, weight in TRIGGER_WEIGHTS.items()],
        "memory_mb": list(DEFAULT_MEMORY_MB),
        "duration_ms": [],
        "app_function_counts": [],
    }
    if root is None or not root.exists():
        return distributions

    trigger_counts: Counter[str] = Counter()
    app_functions: dict[str, set[str]] = defaultdict(set)
    for path in csv_files(root, "invocations_per_function_md.anon.d*.csv*"):
        distributions["available"] = True
        for row in open_csv_rows(path):
            app = first_present(row, ("HashApp", "app"))
            func = first_present(row, ("HashFunction", "func"))
            if app is not None and func is not None:
                app_functions[str(app)].add(str(func))
            trigger = str(row.get("Trigger") or "others").strip().lower() or "others"
            minute_total = 0
            for name, value in row.items():
                if str(name).isdigit():
                    minute_total += int(parse_float(value) or 0)
            trigger_counts[trigger] += max(1, minute_total)

    memories: list[int] = []
    for path in csv_files(root, "app_memory_percentiles.anon.d*.csv*"):
        distributions["available"] = True
        for row in open_csv_rows(path):
            mb = parse_float(
                first_present(
                    row,
                    (
                        "AverageAllocatedMb_pct50",
                        "AverageAllocatedMb",
                        "AverageAllocatedMb_pct75",
                    ),
                )
            )
            if mb is not None and mb > 0:
                memories.append(max(64, int(round(mb))))

    durations: list[int] = []
    for path in csv_files(root, "function_durations_percentiles.anon.d*.csv*"):
        distributions["available"] = True
        for row in open_csv_rows(path):
            value = parse_float(
                first_present(
                    row,
                    (
                        "percentile_Average_50",
                        "Average",
                        "percentile_Average_75",
                    ),
                )
            )
            count = int(parse_float(row.get("Count")) or 1)
            if value is not None and value > 0:
                durations.extend([max(1, int(round(value)))] * min(count, 100))

    if trigger_counts:
        distributions["trigger_weights"] = list(trigger_counts.items())
    if memories:
        distributions["memory_mb"] = memories
    if durations:
        distributions["duration_ms"] = durations
    if app_functions:
        distributions["app_function_counts"] = [
            len(functions) for functions in app_functions.values() if functions
        ]
    return distributions


def first_start_ms(trace_path: Path) -> float:
    minimum: float | None = None
    for event in iter_2021_events(trace_path):
        minimum = event.source_start_ms if minimum is None else min(minimum, event.source_start_ms)
    if minimum is None:
        raise SystemExit("no usable 2021 trace rows found")
    return minimum


def in_window(event: TraceEvent, base_ms: float, end_ms: float | None) -> bool:
    if event.source_start_ms < base_ms:
        return False
    if end_ms is not None and event.source_start_ms > end_ms:
        return False
    return True


def collect_window_events(
    trace_path: Path,
    base_ms: float,
    end_ms: float | None,
    metrics_sample_limit: int,
) -> tuple[list[TraceEvent], Metrics]:
    events: list[TraceEvent] = []
    metrics = Metrics()
    for event in iter_2021_events(trace_path):
        if not in_window(event, base_ms, end_ms):
            continue
        metrics.add(event, metrics_sample_limit)
        events.append(event)
    return events, metrics


def window_metrics_only(
    trace_path: Path,
    base_ms: float,
    end_ms: float | None,
    metrics_sample_limit: int,
) -> Metrics:
    metrics = Metrics()
    for event in iter_2021_events(trace_path):
        if in_window(event, base_ms, end_ms):
            metrics.add(event, metrics_sample_limit)
    return metrics


def count_window_keys(
    trace_path: Path,
    base_ms: float,
    end_ms: float | None,
) -> tuple[Counter[str], Counter[str], int]:
    app_counts: Counter[str] = Counter()
    function_counts: Counter[str] = Counter()
    total = 0
    for event in iter_2021_events(trace_path):
        if not in_window(event, base_ms, end_ms):
            continue
        total += 1
        app_counts[event.app] += 1
        function_counts[event.function_id] += 1
    return app_counts, function_counts, total


def select_downsample_keys(
    app_counts: Counter[str],
    function_counts: Counter[str],
    limit: int,
    mode: str,
) -> tuple[str, set[str]]:
    if mode == "top-apps":
        selected: set[str] = set()
        selected_count = 0
        for app, count in app_counts.most_common():
            if selected_count + count > limit and selected:
                continue
            selected.add(app)
            selected_count += count
            if selected_count >= limit:
                break
        return "app", selected

    if mode == "stratified-apps":
        ranked = sorted(app_counts.items(), key=lambda item: (-item[1], item[0]))
        strata = [ranked[i::4] for i in range(4)]
        selected: set[str] = set()
        selected_count = 0
        while selected_count < limit and any(strata):
            made_progress = False
            for stratum in strata:
                if not stratum:
                    continue
                app, count = min(stratum, key=lambda item: stable_u64(item[0]))
                stratum.remove((app, count))
                if selected_count + count > limit and selected:
                    continue
                selected.add(app)
                selected_count += count
                made_progress = True
                if selected_count >= limit:
                    break
            if not made_progress:
                break
        return "app", selected

    selected = set()
    selected_count = 0
    for function_id, count in sorted(
        function_counts.items(), key=lambda item: stable_u64(item[0])
    ):
        if selected_count + count > limit and selected:
            continue
        selected.add(function_id)
        selected_count += count
        if selected_count >= limit:
            break
    return "function", selected


def collect_selected_events(
    trace_path: Path,
    base_ms: float,
    end_ms: float | None,
    selector_kind: str | None,
    selected_keys: set[str] | None,
    limit: int | None,
) -> list[TraceEvent]:
    events: list[TraceEvent] = []
    for event in iter_2021_events(trace_path):
        if not in_window(event, base_ms, end_ms):
            continue
        if selector_kind == "app" and selected_keys is not None and event.app not in selected_keys:
            continue
        if (
            selector_kind == "function"
            and selected_keys is not None
            and event.function_id not in selected_keys
        ):
            continue
        events.append(event)
        if limit is not None and len(events) >= limit:
            break
    events.sort(key=lambda event: (event.source_start_ms, event.function_id))
    return events


def select_profile_workload(trigger: str, memory_mb: int, cls: str, p90_ms: float) -> str:
    if memory_mb >= 1024 or (cls in {"400ms-2s", "2s+"} and trigger == "storage"):
        return "memory_heavy"
    if trigger == "http" and cls not in {"0-50ms", "50-200ms"}:
        return "network_heavy"
    if trigger in {"queue", "storage", "event"}:
        return "io_mixed"
    if cls in {"400ms-2s", "2s+"} or p90_ms > 750:
        return "pipeline"
    return "cpu_burst"


def profile_hints(workload: str, memory_mb: int, cls: str) -> dict[str, Any]:
    memory_bytes = memory_mb * 1024 * 1024
    if workload == "memory_heavy":
        return {
            "cpu_intensity": 0.45,
            "memory_bytes": memory_bytes,
            "working_set_bytes": max(64 * 1024 * 1024, memory_bytes // 2),
            "cold_load_penalty_ns": 2_000_000_000,
            "io_weight": 350,
        }
    if workload == "io_mixed":
        return {
            "cpu_intensity": 0.35,
            "memory_bytes": max(memory_bytes, 128 * 1024 * 1024),
            "working_set_bytes": max(64 * 1024 * 1024, min(memory_bytes, 256 * 1024 * 1024)),
            "io_weight": 700,
            "io_bandwidth_bytes_per_sec": 64 * 1024 * 1024,
            "cold_load_penalty_ns": 750_000_000,
        }
    if workload == "network_heavy":
        return {
            "cpu_intensity": 0.30,
            "memory_bytes": max(memory_bytes, 128 * 1024 * 1024),
            "working_set_bytes": max(64 * 1024 * 1024, min(memory_bytes, 256 * 1024 * 1024)),
            "network_bandwidth_bytes_per_sec": 128 * 1024 * 1024,
            "io_weight": 300,
        }
    if workload == "pipeline":
        return {
            "cpu_intensity": 0.50,
            "memory_bytes": max(memory_bytes, 96 * 1024 * 1024),
            "working_set_bytes": max(48 * 1024 * 1024, min(memory_bytes, 256 * 1024 * 1024)),
            "cold_load_penalty_ns": 750_000_000 if cls != "2s+" else 1_500_000_000,
            "io_weight": 650,
            "io_bandwidth_bytes_per_sec": 96 * 1024 * 1024,
            "network_bandwidth_bytes_per_sec": 96 * 1024 * 1024,
            "phase_sequence": [
                {"kind": "IoBound", "duration_pct": 33},
                {"kind": "CpuBound", "duration_pct": 34},
                {"kind": "IoBound", "duration_pct": 33},
            ],
        }
    return {"cpu_intensity": 0.95, "io_weight": 400}


def function_stats(events: list[TraceEvent]) -> dict[str, dict[str, Any]]:
    by_function: dict[str, list[TraceEvent]] = defaultdict(list)
    for event in events:
        by_function[event.function_id].append(event)

    stats: dict[str, dict[str, Any]] = {}
    for function_id, items in by_function.items():
        items.sort(key=lambda event: event.source_start_ms)
        durations = [event.duration_ms for event in items]
        starts = [event.source_start_ms for event in items]
        iats = [b - a for a, b in zip(starts, starts[1:])]
        mean_iat = sum(iats) / len(iats) if iats else None
        if iats and mean_iat and mean_iat > 0:
            variance = sum((iat - mean_iat) ** 2 for iat in iats) / len(iats)
            iat_cv = math.sqrt(variance) / mean_iat
        else:
            iat_cv = None
        active_span = (starts[-1] - starts[0]) if len(starts) > 1 else 0.0
        stats[function_id] = {
            "invocation_count": len(items),
            "median_duration_ms": percentile(durations, 0.50),
            "p90_duration_ms": percentile(durations, 0.90),
            "mean_iat_ms": mean_iat,
            "iat_cv": iat_cv,
            "burstiness": (iat_cv or 0.0) if iats else 0.0,
            "active_span_ms": active_span,
            "app": items[0].app,
            "func": items[0].func,
        }
    return stats


def build_profiles(
    events: list[TraceEvent],
    distributions: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    profiles: dict[str, dict[str, Any]] = {}
    function_profiles: dict[str, str] = {}
    stats = function_stats(events)
    trigger_weights = distributions["trigger_weights"]
    memory_values = distributions["memory_mb"]
    duration_values = distributions.get("duration_ms") or []

    for index, function_id in enumerate(sorted(stats), start=1):
        stat = stats[function_id]
        median_ms = float(stat["median_duration_ms"] or 0.0)
        p90_ms = float(stat["p90_duration_ms"] or median_ms)
        profile_duration_ms = stable_choice(
            duration_values, f"{function_id}:duration-class", median_ms
        )
        cls = duration_class(float(profile_duration_ms))
        trigger = weighted_choice(trigger_weights, f"{function_id}:trigger", "http")
        memory_mb = int(stable_choice(memory_values, f"{function_id}:memory", 256))
        workload = select_profile_workload(str(trigger), memory_mb, cls, p90_ms)
        profile_id = f"azp_{index:06d}"
        hints = profile_hints(workload, memory_mb, cls)
        profiles[profile_id] = {
            **hints,
            "workload": workload,
            "kernel": workload,
            "trigger_type": trigger,
            "memory_mb": memory_mb,
            "profile_duration_class": cls,
            "duration_class": cls,
            "cold_start_model": {
                "source": "synthetic",
                "field_note": "2021 invocation trace has no cold-start/resource fields",
            },
            "azure_2021_stats": {
                key: value
                for key, value in stat.items()
                if key not in {"app", "func"}
            },
            "profile_duration_ms": int(round(float(profile_duration_ms))),
        }
        function_profiles[function_id] = profile_id
    return profiles, function_profiles


def known_profile_hints(profile: dict[str, Any]) -> dict[str, Any]:
    return {key: profile[key] for key in PROFILE_HINT_KEYS if key in profile}


def summarize(values: list[float] | list[int]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "min": None, "p50": None, "p90": None, "p99": None, "max": None}
    return {
        "count": len(values),
        "min": min(values),
        "p50": percentile(values, 0.50),
        "p90": percentile(values, 0.90),
        "p99": percentile(values, 0.99),
        "max": max(values),
    }


def iats_from_starts(starts: list[float]) -> list[float]:
    ordered = sorted(starts)
    return [b - a for a, b in zip(ordered, ordered[1:])]


def max_concurrency(starts: list[float], ends: list[float]) -> int:
    points: list[tuple[float, int]] = []
    for start in starts:
        points.append((start, 1))
    for end in ends:
        points.append((end, -1))
    concurrent = 0
    peak = 0
    for _, delta in sorted(points, key=lambda item: (item[0], item[1])):
        concurrent += delta
        peak = max(peak, concurrent)
    return peak


def ks_distance(left: list[float], right: list[float]) -> float | None:
    if not left or not right:
        return None
    a = sorted(left)
    b = sorted(right)
    i = j = 0
    distance = 0.0
    while i < len(a) and j < len(b):
        value = min(a[i], b[j])
        while i < len(a) and a[i] <= value:
            i += 1
        while j < len(b) and b[j] <= value:
            j += 1
        distance = max(distance, abs(i / len(a) - j / len(b)))
    return distance


def fidelity_payload(
    source: Metrics,
    generated: list[TraceEvent],
    base_ms: float,
    scale: float,
) -> dict[str, Any]:
    gen_starts = [(event.source_start_ms - base_ms) / scale for event in generated]
    gen_ends = [
        ((event.source_start_ms - base_ms) / scale) + event.duration_ms
        for event in generated
    ]
    gen_durations = [event.duration_ms for event in generated]
    source_durations = source.durations or []
    source_starts = source.starts or []
    source_ends = source.ends or []
    source_iats = iats_from_starts(source_starts)
    gen_iats = iats_from_starts(gen_starts)
    source_popularity = sorted((source.functions or Counter()).values(), reverse=True)
    generated_popularity = sorted(Counter(event.function_id for event in generated).values(), reverse=True)

    return {
        "source_window": {
            "invocations": source.count,
            "functions": len(source.functions or {}),
            "apps": len(source.apps or {}),
            "duration_ms": summarize(source_durations),
            "iat_ms": summarize(source_iats),
            "max_sampled_concurrency": max_concurrency(source_starts, source_ends),
        },
        "generated": {
            "invocations": len(generated),
            "functions": len({event.function_id for event in generated}),
            "apps": len({event.app for event in generated}),
            "duration_ms": summarize(gen_durations),
            "iat_ms": summarize(gen_iats),
            "max_concurrency": max_concurrency(gen_starts, gen_ends),
        },
        "distances": {
            "duration_ks": ks_distance(source_durations, gen_durations),
            "iat_ks": ks_distance(source_iats, gen_iats),
            "popularity_ks": ks_distance(source_popularity, generated_popularity),
        },
    }


def write_invocations_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "event_id",
        "invocation_id",
        "at_ms",
        "source_start_time_ms",
        "app",
        "func",
        "function_id",
        "profile_id",
        "workload",
        "target_duration_class",
        "profile_duration_class",
        "duration_class",
        "target_duration_ms",
        "deadline_us",
        "slo_class",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    if args.scale <= 0:
        raise SystemExit("--scale must be greater than zero")
    if args.limit is not None and args.limit <= 0:
        raise SystemExit("--limit must be positive")

    min_start_ms = first_start_ms(args.trace_2021)
    base_ms = min_start_ms + args.window_start_ms
    end_ms = base_ms + args.window_ms if args.window_ms is not None else None

    if args.limit is None:
        selected_events, source_metrics = collect_window_events(
            args.trace_2021, base_ms, end_ms, args.metrics_sample_limit
        )
    else:
        app_counts, function_counts, total = count_window_keys(args.trace_2021, base_ms, end_ms)
        if total == 0:
            raise SystemExit("no invocations matched the requested window")
        source_metrics = window_metrics_only(
            args.trace_2021, base_ms, end_ms, args.metrics_sample_limit
        )
        if total <= args.limit:
            selected_events = collect_selected_events(
                args.trace_2021, base_ms, end_ms, None, None, None
            )
        else:
            selector_kind, selected_keys = select_downsample_keys(
                app_counts, function_counts, args.limit, args.downsample_mode
            )
            selected_events = collect_selected_events(
                args.trace_2021,
                base_ms,
                end_ms,
                selector_kind,
                selected_keys,
                args.limit,
            )
    if not selected_events:
        raise SystemExit("no invocations matched the requested window")

    distributions = load_2019_distributions(args.dataset_2019_dir)
    profiles, function_profiles = build_profiles(selected_events, distributions)

    invocation_rows: list[dict[str, Any]] = []
    replay_invocations: list[dict[str, Any]] = []
    for invocation_id, event in enumerate(
        sorted(selected_events, key=lambda item: (item.source_start_ms, item.function_id)),
        start=1,
    ):
        profile_id = function_profiles[event.function_id]
        profile = profiles[profile_id]
        at_ms = (event.source_start_ms - base_ms) / args.scale
        target_cls = duration_class(event.duration_ms)
        row = {
            "event_id": f"az2021-{invocation_id:08d}",
            "invocation_id": invocation_id,
            "at_ms": round(at_ms, 3),
            "source_start_time_ms": round(event.source_start_ms, 3),
            "app": event.app,
            "func": event.func,
            "function_id": event.function_id,
            "profile_id": profile_id,
            "workload": profile["workload"],
            "target_duration_class": target_cls,
            "profile_duration_class": profile["profile_duration_class"],
            "duration_class": profile["profile_duration_class"],
            "target_duration_ms": event.duration_ms,
            "deadline_us": deadline_us(event.duration_ms, args.min_slack_ms),
            "slo_class": slo_for_duration(event.duration_ms),
        }
        invocation_rows.append(row)
        replay_invocations.append(
            {
                **row,
                "function_hash": event.function_id,
                "duration_ms": event.duration_ms,
                "profile_hints": known_profile_hints(profile),
            }
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_invocations_csv(args.output_dir / "invocations.csv", invocation_rows)

    created_at = datetime.now(timezone.utc).isoformat()
    profiles_payload = {
        "version": 1,
        "schema": "cosmos.azure.hybrid-profiles",
        "created_at": created_at,
        "source": {
            "profile_truth": "Azure Functions 2019 distributions plus synthetic SFS/ALPS-style kernels",
            "arrival_truth": "Azure Functions 2021 invocation trace",
            "dataset_2019_available": bool(distributions["available"]),
        },
        "profiles": profiles,
        "function_profiles": function_profiles,
    }
    (args.output_dir / "profiles.json").write_text(
        json.dumps(profiles_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    replay_payload = {
        "version": 1,
        "schema": "cosmos.azure.hybrid-replay",
        "created_at": created_at,
        "source": {
            "trace_2021": str(args.trace_2021),
            "dataset_2019_dir": str(args.dataset_2019_dir) if args.dataset_2019_dir else None,
        },
        "window": {
            "base_ms": base_ms,
            "window_start_ms": args.window_start_ms,
            "window_ms": args.window_ms,
            "scale": args.scale,
            "downsample_mode": args.downsample_mode,
            "limit": args.limit,
        },
        "profiles_path": "profiles.json",
        "invocations_path": "invocations.csv",
        "invocations": replay_invocations,
    }
    (args.output_dir / "replay.json").write_text(
        json.dumps(replay_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    fidelity = fidelity_payload(source_metrics, selected_events, base_ms, args.scale)
    fidelity.update(
        {
            "created_at": created_at,
            "notes": [
                "2021 trace is used only for invocation arrival and duration timing.",
                "Resource, trigger, memory, and cold-start fields are synthetic/profile-derived.",
            ],
        }
    )
    (args.output_dir / "fidelity.json").write_text(
        json.dumps(fidelity, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    print(args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
