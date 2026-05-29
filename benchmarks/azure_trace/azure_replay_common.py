#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import math
import shutil
import subprocess
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


DEFAULT_2021_TRACE = Path(
    "benchmarks/third_party/AzurePublicDataset/data/AzureFunctionsInvocationTraceForTwoWeeksJan2021.rar"
)


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


def select_trace_events(args: argparse.Namespace) -> tuple[list[TraceEvent], Metrics, float]:
    min_start_ms = first_start_ms(args.trace_2021)
    base_ms = min_start_ms + args.window_start_ms
    end_ms = base_ms + args.window_ms if args.window_ms is not None else None

    if args.limit is None:
        selected_events, source_metrics = collect_window_events(
            args.trace_2021, base_ms, end_ms, args.metrics_sample_limit
        )
    else:
        app_counts, function_counts, total = count_window_keys(
            args.trace_2021, base_ms, end_ms
        )
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
    return selected_events, source_metrics, base_ms


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
    generated_popularity = sorted(
        Counter(event.function_id for event in generated).values(), reverse=True
    )

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
