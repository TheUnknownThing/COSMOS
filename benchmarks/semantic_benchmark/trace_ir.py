#!/usr/bin/env python3

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import math
import shutil
import subprocess
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable


CONTRACT_PATH = Path(__file__).resolve().with_name("contract.json")
DEFAULT_2021_TRACE = Path(
    "benchmarks/third_party/AzurePublicDataset/data/AzureFunctionsInvocationTraceForTwoWeeksJan2021.rar"
)
SUPPORTED_DOWNSAMPLE_MODES = ("top-apps", "stratified-apps", "hash-functions")


@dataclass(frozen=True)
class TraceInvocation:
    invocation_id: int
    event_id: str
    app: str
    func: str
    function_id: str
    source_start_ms: float
    source_end_ms: float
    target_duration_ms: int
    at_ms: float


@dataclass(frozen=True)
class ParsedTraceRow:
    app: str
    func: str
    function_id: str
    source_start_ms: float
    source_end_ms: float
    target_duration_ms: int


@dataclass(frozen=True)
class SelectionMetadata:
    total_window_invocations: int
    selected_invocations: int
    selector_kind: str | None
    selected_keys: list[str]
    limit: int | None
    downsample_mode: str


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

    def add(self, row: ParsedTraceRow, sample_limit: int) -> None:
        self.count += 1
        assert self.functions is not None
        assert self.apps is not None
        self.functions[row.function_id] += 1
        self.apps[row.app] += 1
        if self.durations is not None and len(self.durations) < sample_limit:
            self.durations.append(row.target_duration_ms)
            assert self.starts is not None
            assert self.ends is not None
            self.starts.append(row.source_start_ms)
            self.ends.append(row.source_end_ms)


def load_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def stable_u64(text: str) -> int:
    return int.from_bytes(hashlib.sha256(text.encode("utf-8")).digest()[:8], "big")


def open_csv_rows(path: Path) -> Iterable[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".gz" or path.name.endswith(".csv.gz"):
        with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
            yield from csv.DictReader(fh)
        return
    if suffix == ".rar":
        if shutil.which("unrar") is None:
            raise RuntimeError(
                f"{path} is a RAR archive; install unrar or extract it before running this builder"
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
        return
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


def parse_azure_2021_row(row: dict[str, Any]) -> ParsedTraceRow | None:
    app = first_present(row, ("app", "HashApp"))
    func = first_present(row, ("func", "function", "HashFunction"))
    end_timestamp_s = parse_float(first_present(row, ("end_timestamp", "end_time", "end")))
    duration_s = parse_float(first_present(row, ("duration", "duration_s")))
    if app is None or func is None or end_timestamp_s is None or duration_s is None:
        return None
    if duration_s < 0:
        return None

    source_end_ms = end_timestamp_s * 1000.0
    source_start_ms = max(0.0, (end_timestamp_s - duration_s) * 1000.0)
    target_duration_ms = max(0, int(round(duration_s * 1000.0)))
    app_text = str(app)
    func_text = str(func)
    return ParsedTraceRow(
        app=app_text,
        func=func_text,
        function_id=f"{app_text}:{func_text}",
        source_start_ms=source_start_ms,
        source_end_ms=source_end_ms,
        target_duration_ms=target_duration_ms,
    )


def iter_azure_2021_rows(path: Path) -> Iterable[ParsedTraceRow]:
    for row in open_csv_rows(path):
        parsed = parse_azure_2021_row(row)
        if parsed is not None:
            yield parsed


def first_start_ms(trace_path: Path) -> float:
    minimum: float | None = None
    for row in iter_azure_2021_rows(trace_path):
        minimum = row.source_start_ms if minimum is None else min(minimum, row.source_start_ms)
    if minimum is None:
        raise ValueError("no usable Azure 2021 trace rows found")
    return minimum


def in_window(row: ParsedTraceRow, base_ms: float, end_ms: float | None) -> bool:
    if row.source_start_ms < base_ms:
        return False
    if end_ms is not None and row.source_start_ms > end_ms:
        return False
    return True


def collect_window_counts(
    trace_path: Path,
    base_ms: float,
    end_ms: float | None,
) -> tuple[Counter[str], Counter[str], int]:
    app_counts: Counter[str] = Counter()
    function_counts: Counter[str] = Counter()
    total = 0
    for row in iter_azure_2021_rows(trace_path):
        if not in_window(row, base_ms, end_ms):
            continue
        total += 1
        app_counts[row.app] += 1
        function_counts[row.function_id] += 1
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
        strata = [ranked[index::4] for index in range(4)]
        selected = set()
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


def collect_selected_rows(
    trace_path: Path,
    base_ms: float,
    end_ms: float | None,
    selector_kind: str | None,
    selected_keys: set[str] | None,
    limit: int | None,
) -> list[ParsedTraceRow]:
    rows: list[ParsedTraceRow] = []
    for row in iter_azure_2021_rows(trace_path):
        if not in_window(row, base_ms, end_ms):
            continue
        if selector_kind == "app" and selected_keys is not None and row.app not in selected_keys:
            continue
        if (
            selector_kind == "function"
            and selected_keys is not None
            and row.function_id not in selected_keys
        ):
            continue
        rows.append(row)
        if limit is not None and len(rows) >= limit:
            break
    return sorted(rows, key=lambda item: (item.source_start_ms, item.function_id))


def collect_source_metrics(
    trace_path: Path,
    base_ms: float,
    end_ms: float | None,
    sample_limit: int,
) -> Metrics:
    metrics = Metrics()
    for row in iter_azure_2021_rows(trace_path):
        if in_window(row, base_ms, end_ms):
            metrics.add(row, sample_limit)
    return metrics


def metrics_from_rows(rows: list[ParsedTraceRow], sample_limit: int) -> Metrics:
    metrics = Metrics()
    for row in rows:
        metrics.add(row, sample_limit)
    return metrics


def collect_window_rows(
    trace_path: Path,
    base_ms: float,
    end_ms: float | None,
) -> list[ParsedTraceRow]:
    return [
        row
        for row in iter_azure_2021_rows(trace_path)
        if in_window(row, base_ms, end_ms)
    ]


def counts_from_rows(rows: list[ParsedTraceRow]) -> tuple[Counter[str], Counter[str]]:
    app_counts: Counter[str] = Counter()
    function_counts: Counter[str] = Counter()
    for row in rows:
        app_counts[row.app] += 1
        function_counts[row.function_id] += 1
    return app_counts, function_counts


def select_rows(
    rows: list[ParsedTraceRow],
    selector_kind: str | None,
    selected_keys: set[str] | None,
    limit: int | None,
) -> list[ParsedTraceRow]:
    selected: list[ParsedTraceRow] = []
    for row in rows:
        if selector_kind == "app" and selected_keys is not None and row.app not in selected_keys:
            continue
        if (
            selector_kind == "function"
            and selected_keys is not None
            and row.function_id not in selected_keys
        ):
            continue
        selected.append(row)
    ordered = sorted(selected, key=lambda item: (item.source_start_ms, item.function_id))
    if limit is not None:
        return ordered[:limit]
    return ordered


def select_trace_window(
    trace_path: Path,
    window_start_ms: float,
    window_ms: float | None,
    scale: float,
    limit: int | None,
    downsample_mode: str,
    metrics_sample_limit: int,
    normalize_to_first_start: bool = False,
) -> tuple[list[TraceInvocation], Metrics, float, SelectionMetadata]:
    if scale <= 0:
        raise ValueError("scale must be greater than zero")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")
    if downsample_mode not in SUPPORTED_DOWNSAMPLE_MODES:
        raise ValueError(f"unsupported downsample mode: {downsample_mode}")

    base_ms = (
        first_start_ms(trace_path) + window_start_ms
        if normalize_to_first_start
        else window_start_ms
    )
    end_ms = base_ms + window_ms if window_ms is not None else None
    window_rows = collect_window_rows(trace_path, base_ms, end_ms)
    total_window_invocations = len(window_rows)
    if total_window_invocations == 0:
        raise ValueError("no invocations matched the requested window")
    app_counts, function_counts = counts_from_rows(window_rows)

    selector_kind: str | None = None
    selected_key_set: set[str] | None = None
    if limit is not None and total_window_invocations > limit:
        selector_kind, selected_key_set = select_downsample_keys(
            app_counts, function_counts, limit, downsample_mode
        )

    rows = select_rows(window_rows, selector_kind, selected_key_set, limit)
    if not rows:
        raise ValueError("no invocations matched the requested selection")

    invocations = [
        TraceInvocation(
            invocation_id=index,
            event_id=f"az2021ir-{index:08d}",
            app=row.app,
            func=row.func,
            function_id=row.function_id,
            source_start_ms=round(row.source_start_ms, 6),
            source_end_ms=round(row.source_end_ms, 6),
            target_duration_ms=row.target_duration_ms,
            at_ms=round((row.source_start_ms - base_ms) / scale, 6),
        )
        for index, row in enumerate(rows, start=1)
    ]
    metrics = metrics_from_rows(window_rows, metrics_sample_limit)
    selection = SelectionMetadata(
        total_window_invocations=total_window_invocations,
        selected_invocations=len(invocations),
        selector_kind=selector_kind,
        selected_keys=sorted(selected_key_set or []),
        limit=limit,
        downsample_mode=downsample_mode,
    )
    return invocations, metrics, base_ms, selection


def percentile(values: list[float] | list[int], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


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
    return [round(right - left, 6) for left, right in zip(ordered, ordered[1:])]


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


def ks_distance(left: list[float] | list[int], right: list[float] | list[int]) -> float | None:
    if not left or not right:
        return None
    a = sorted(float(value) for value in left)
    b = sorted(float(value) for value in right)
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
    generated: list[TraceInvocation],
    selection: SelectionMetadata,
) -> dict[str, Any]:
    source_durations = source.durations or []
    source_starts = [round(value, 6) for value in (source.starts or [])]
    source_ends = [round(value, 6) for value in (source.ends or [])]
    source_iats = iats_from_starts(source_starts)
    generated_source_starts = [invocation.source_start_ms for invocation in generated]
    generated_source_ends = [invocation.source_end_ms for invocation in generated]
    generated_iats = iats_from_starts(generated_source_starts)
    generated_durations = [invocation.target_duration_ms for invocation in generated]
    source_popularity = sorted((source.functions or Counter()).values(), reverse=True)
    generated_popularity = sorted(
        Counter(invocation.function_id for invocation in generated).values(), reverse=True
    )

    return {
        "schema": "cosmos.semantic.trace-fidelity",
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
            "functions": len({invocation.function_id for invocation in generated}),
            "apps": len({invocation.app for invocation in generated}),
            "duration_ms": summarize(generated_durations),
            "iat_ms": summarize(generated_iats),
            "max_concurrency": max_concurrency(generated_source_starts, generated_source_ends),
        },
        "selection": asdict(selection),
        "distances": {
            "duration_ks": ks_distance(source_durations, generated_durations),
            "iat_ks": ks_distance(source_iats, generated_iats),
            "popularity_ks": ks_distance(source_popularity, generated_popularity),
        },
    }


def write_trace_invocations_csv(path: Path, invocations: list[TraceInvocation]) -> None:
    fieldnames = [
        "invocation_id",
        "event_id",
        "at_ms",
        "source_start_ms",
        "source_end_ms",
        "target_duration_ms",
        "app",
        "func",
        "function_id",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for invocation in invocations:
            writer.writerow(
                {
                    "invocation_id": invocation.invocation_id,
                    "event_id": invocation.event_id,
                    "at_ms": invocation.at_ms,
                    "source_start_ms": invocation.source_start_ms,
                    "source_end_ms": invocation.source_end_ms,
                    "target_duration_ms": invocation.target_duration_ms,
                    "app": invocation.app,
                    "func": invocation.func,
                    "function_id": invocation.function_id,
                }
            )
