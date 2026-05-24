#!/usr/bin/env python3
import argparse
import json
import re
import statistics
import sys
from collections import Counter


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
TID_RE = re.compile(r"#(tid_[A-Za-z0-9]+)")
START_RE = re.compile(
    r"containerStart containerState: (?P<state>\w+).*? action: (?P<action>\S+) .*? activationId: (?P<activation>[0-9a-fA-F]+)"
)
RUN_ARGS_RE = re.compile(r"--cpu-shares (?P<cpu>\S+) --memory (?P<memory>\S+)")
MARKER_RE = re.compile(r"\[marker:(?P<name>[^:\]]+):(?P<offset>[0-9]+)(?::(?P<duration>[0-9]+))?\]")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Parse OpenWhisk invoker lifecycle markers into cold/warm startup records."
    )
    parser.add_argument("log", help="OpenWhisk standalone log")
    parser.add_argument("--tsv", help="write per-activation TSV")
    parser.add_argument("--summary-json", help="write aggregate JSON summary")
    return parser.parse_args()


def clean(line):
    return ANSI_RE.sub("", line.rstrip("\n"))


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def duration_stats(values):
    values = [value for value in values if value is not None]
    if not values:
        return {
            "count": 0,
            "min_ms": None,
            "median_ms": None,
            "p95_ms": None,
            "max_ms": None,
            "mean_ms": None,
        }
    return {
        "count": len(values),
        "min_ms": min(values),
        "median_ms": statistics.median(values),
        "p95_ms": percentile(values, 0.95),
        "max_ms": max(values),
        "mean_ms": statistics.fmean(values),
    }


def parse_log(path):
    by_tid = {}
    order = []
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        for line_no, raw in enumerate(handle, start=1):
            line = clean(raw)
            tid_match = TID_RE.search(line)
            if not tid_match:
                continue
            tid = tid_match.group(1)
            record = by_tid.setdefault(
                tid,
                {
                    "tid": tid,
                    "line": line_no,
                    "state": "",
                    "action": "",
                    "activation_id": "",
                    "cpu_shares": "",
                    "memory": "",
                    "docker_run_ms": None,
                    "activation_init_ms": None,
                    "activation_run_ms": None,
                },
            )
            if tid not in order:
                order.append(tid)

            start = START_RE.search(line)
            if start:
                record["state"] = start.group("state")
                record["action"] = start.group("action")
                record["activation_id"] = start.group("activation")

            run_args = RUN_ARGS_RE.search(line)
            if run_args:
                record["cpu_shares"] = run_args.group("cpu")
                record["memory"] = run_args.group("memory")

            marker = MARKER_RE.search(line)
            if marker and marker.group("duration") is not None:
                duration = int(marker.group("duration"))
                name = marker.group("name")
                if name == "invoker_docker.run_finish":
                    record["docker_run_ms"] = duration
                elif name == "invoker_activationInit_finish":
                    record["activation_init_ms"] = duration
                elif name == "invoker_activationRun_finish":
                    record["activation_run_ms"] = duration

    records = [by_tid[tid] for tid in order if by_tid[tid].get("activation_id")]
    for record in records:
        docker_ms = record.get("docker_run_ms")
        init_ms = record.get("activation_init_ms")
        record["startup_ms"] = docker_ms + init_ms if docker_ms is not None and init_ms is not None else None
    return records


def write_tsv(path, records):
    fields = [
        "line",
        "tid",
        "activation_id",
        "action",
        "state",
        "docker_run_ms",
        "activation_init_ms",
        "startup_ms",
        "activation_run_ms",
        "memory",
        "cpu_shares",
    ]
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\t".join(fields) + "\n")
        for record in records:
            values = [
                "" if record.get(field) is None else str(record.get(field, ""))
                for field in fields
            ]
            handle.write("\t".join(values) + "\n")


def build_summary(records):
    state_counts = Counter(record["state"] or "unknown" for record in records)
    memory_counts = Counter(record["memory"] for record in records if record.get("memory"))
    cpu_counts = Counter(record["cpu_shares"] for record in records if record.get("cpu_shares"))
    by_state = {}
    for state in sorted(state_counts):
        subset = [record for record in records if (record["state"] or "unknown") == state]
        by_state[state] = {
            "count": len(subset),
            "docker_run_ms": duration_stats([record.get("docker_run_ms") for record in subset]),
            "activation_init_ms": duration_stats([record.get("activation_init_ms") for record in subset]),
            "startup_ms": duration_stats([record.get("startup_ms") for record in subset]),
            "activation_run_ms": duration_stats([record.get("activation_run_ms") for record in subset]),
        }
    return {
        "activations": len(records),
        "state_counts": dict(state_counts),
        "memory_counts": dict(memory_counts),
        "cpu_share_counts": dict(cpu_counts),
        "by_state": by_state,
    }


def main():
    args = parse_args()
    records = parse_log(args.log)
    summary = build_summary(records)
    if args.tsv:
        write_tsv(args.tsv, records)
    if args.summary_json:
        with open(args.summary_json, "w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2, sort_keys=True)
            handle.write("\n")
    if not args.tsv and not args.summary_json:
        json.dump(summary, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
