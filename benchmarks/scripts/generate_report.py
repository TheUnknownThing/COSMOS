#!/usr/bin/env python3
"""Generate comprehensive comparison report from benchmark results."""

from __future__ import annotations

import json
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
RESULTS_BASE = SCRIPT_DIR / "results"

CONFIGS = ["cfs-default", "sfs", "cosmos-heuristic", "cosmos-full"]
BASELINE = "cfs-default"

WORKLOAD_MATRIX = {
    "cpu_burst":         ("64", "128"),
    "sleep_short":       ("64", "128"),
    "io_mixed":          ("32", "64"),
    "memory_heavy":      ("32", "64"),
    "network_heavy":     ("32", "64"),
    "compression_mixed": ("64", "128"),
    "graph_bfs":         ("32", "64"),
}

WORKLOAD_LABELS = {
    "cpu_burst": "CPU Burst (matrix multiply)",
    "sleep_short": "Sleep Short (idle baseline)",
    "io_mixed": "IO Mixed (file read/write/checksum)",
    "memory_heavy": "Memory Heavy (strided scan)",
    "network_heavy": "Network Heavy (loopback TCP)",
    "compression_mixed": "Compression Mixed (RLE)",
    "graph_bfs": "Graph BFS (irregular traversal)",
}


def load_summary(run_dir: Path) -> dict | None:
    path = run_dir / "summary.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def find_latest_run(config: str, workload: str, concurrency: str) -> Path | None:
    config_dir = RESULTS_BASE / f"{config}_comprehensive"
    if not config_dir.exists():
        return None
    best_dir, best_time = None, 0
    for entry in config_dir.iterdir():
        if entry.name == "latest" or not entry.is_dir():
            continue
        mp = entry / "manifest.json"
        if not mp.exists():
            continue
        try:
            m = json.loads(mp.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        if m.get("workload") == workload and str(m.get("concurrency")) == concurrency:
            ts = entry.stat().st_mtime
            if ts > best_time:
                best_time, best_dir = ts, entry
    return best_dir


def summary_val(s: dict | None, *keys) -> str:
    """Traverse nested dict to get value, return N/A if missing."""
    if s is None:
        return "N/A"
    v = s
    for k in keys:
        if isinstance(v, dict):
            v = v.get(k, "N/A")
        else:
            return "N/A"
    if isinstance(v, float):
        return f"{v:.2f}"
    return str(v)


def summary_float(s: dict | None, *keys) -> float:
    raw = summary_val(s, *keys)
    try:
        return float(raw)
    except (ValueError, TypeError):
        return float("inf")


def delta_fmt(base: str, cand: str) -> str:
    try:
        b, c = float(base), float(cand)
        d = c - b
        pct = ((c - b) / b) * 100 if b != 0 else 0
        s = "+" if d >= 0 else ""
        return f"{s}{d:.1f} ({s}{pct:.1f}%)"
    except (ValueError, TypeError):
        return "—"


# ── access helpers ────────────────────────────────────────────
def lat(s, key):   return summary_val(s, "latency", key)
def load(s, key):  return summary_val(s, "load", key)
def comp(s, key):  return summary_val(s, "compute", key)


def generate_report() -> str:
    L = []
    L.append("# COSMOS Comprehensive Benchmark Report")
    L.append("")
    L.append("**Date:** 2026-05-27 | **Host:** amd002 (64c AMD EPYC 7452)")
    L.append("")
    L.append("## Environment")
    L.append("")
    L.append("| Detail | Value |")
    L.append("|--------|-------|")
    L.append("| Kernel | 7.0.9-070009-generic |")
    L.append("| CPU | 64 logical cores |")
    L.append("| Workload duration | 250ms per invocation |")
    L.append("| SLO deadline | 500ms (2x duration) |")
    L.append("| Schedulers compared | CFS (baseline), SFS, COSMOS-heuristic, COSMOS-full |")
    L.append("| Workloads | 7 synthetic workloads x 2 concurrency levels = 14 configs x 4 schedulers = 56 runs |")
    L.append("")
    L.append("---")
    L.append("")

    # ── SLO summary ──────────────────────────────────────────
    L.append("## SLO Violation Summary")
    L.append("")
    L.append("| Workload | Concurrency | CFS | SFS | COSMOS-heuristic | COSMOS-full |")
    L.append("|---|---:|---:|---:|---:|---:|")

    for wl in WORKLOAD_MATRIX:
        for conc in WORKLOAD_MATRIX[wl]:
            vals = []
            for cfg in CONFIGS:
                d = find_latest_run(cfg, wl, conc)
                s = load_summary(d) if d else None
                vals.append(lat(s, "client_slo_violations"))
            L.append(f"| {wl} | {conc} | {vals[0]} | {vals[1]} | {vals[2]} | {vals[3]} |")

    L.append("")
    L.append("---")
    L.append("")

    # ── Per-workload tables ───────────────────────────────────
    for wl in WORKLOAD_MATRIX:
        label = WORKLOAD_LABELS.get(wl, wl)
        L.append(f"## {wl} — {label}")
        L.append("")

        for conc in WORKLOAD_MATRIX[wl]:
            cfs_dir = find_latest_run(BASELINE, wl, conc)
            cfs = load_summary(cfs_dir) if cfs_dir else None

            L.append(f"### concurrency = {conc}")
            L.append("")
            L.append("| Config | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | SLO viol. | Δ p50 vs CFS | Δ p99 vs CFS |")
            L.append("|---|---:|---:|---:|---:|---:|---|---|")

            cfs_p50 = lat(cfs, "p50_ms")
            cfs_p99 = lat(cfs, "p99_ms")

            for i, cfg in enumerate(CONFIGS):
                d = find_latest_run(cfg, wl, conc) if i > 0 else cfs_dir
                s = load_summary(d) if d else None
                p50 = lat(s, "p50_ms")
                p95 = lat(s, "p95_ms")
                p99 = lat(s, "p99_ms")
                mean_ = lat(s, "mean_ms")
                slo = lat(s, "client_slo_violations")

                if cfg == BASELINE:
                    L.append(f"| **{cfg}** | {p50} | {p95} | {p99} | {mean_} | {slo} | — | — |")
                else:
                    d50 = delta_fmt(cfs_p50, p50)
                    d99 = delta_fmt(cfs_p99, p99)
                    L.append(f"| **{cfg}** | {p50} | {p95} | {p99} | {mean_} | {slo} | {d50} | {d99} |")

            L.append("")
            L.append(f"**Load:** ratio={load(cfs,'load_ratio')} | class={load(cfs,'class')} | fair={load(cfs,'fair')}")
            L.append(f"**Compute:** total_cpu={comp(cfs,'total_cpu_ms')}ms | per-invocation mean={comp(cfs,'mean_cpu_ms')}ms")
            L.append("")

    # ── Key Findings ──────────────────────────────────────────
    L.append("---")
    L.append("## Head-to-Head: Best Scheduler per Workload")
    L.append("")

    # Find best for each workload-concurrency pair
    cols = ["Workload", "Concurrency", "Best p50", "Best p50 Config", "Best p99", "Best p99 Config", "Best SLO", "Best SLO Config"]
    L.append("| " + " | ".join(cols) + " |")
    L.append("|" + "|".join(["---"] * len(cols)) + "|")

    for wl in WORKLOAD_MATRIX:
        for conc in WORKLOAD_MATRIX[wl]:
            best_p50_cfg, best_p50_val = "", float("inf")
            best_p99_cfg, best_p99_val = "", float("inf")
            best_slo_cfg, best_slo_val = "", float("inf")

            for cfg in CONFIGS:
                d = find_latest_run(cfg, wl, conc)
                s = load_summary(d) if d else None
                p50 = summary_float(s, "latency", "p50_ms")
                p99 = summary_float(s, "latency", "p99_ms")
                slo = summary_float(s, "latency", "client_slo_violations")

                if p50 < best_p50_val:
                    best_p50_val, best_p50_cfg = p50, cfg
                if p99 < best_p99_val:
                    best_p99_val, best_p99_cfg = p99, cfg
                if slo < best_slo_val:
                    best_slo_val, best_slo_cfg = slo, cfg

            b50 = f"{best_p50_val:.1f}" if best_p50_val != float("inf") else "N/A"
            b99 = f"{best_p99_val:.1f}" if best_p99_val != float("inf") else "N/A"
            bslo = f"{best_slo_val:.0f}" if best_slo_val != float("inf") else "N/A"
            L.append(f"| {wl} | {conc} | {b50} | {best_p50_cfg} | {b99} | {best_p99_cfg} | {bslo} | {best_slo_cfg} |")

    L.append("")

    # ── Detailed analysis per workload ────────────────────────
    L.append("---")
    L.append("## Key Findings")
    L.append("")

    findings = []
    for wl in WORKLOAD_MATRIX:
        for conc in WORKLOAD_MATRIX[wl]:
            cfs_dir = find_latest_run(BASELINE, wl, conc)
            cfs = load_summary(cfs_dir) if cfs_dir else None
            cfs_p99 = summary_float(cfs, "latency", "p99_ms")
            cfs_slo = summary_float(cfs, "latency", "client_slo_violations")

            best_p99, best_p99_cfg = float("inf"), ""
            best_slo, best_slo_cfg = float("inf"), ""
            for cfg in CONFIGS[1:]:
                d = find_latest_run(cfg, wl, conc)
                s = load_summary(d) if d else None
                p99 = summary_float(s, "latency", "p99_ms")
                slo = summary_float(s, "latency", "client_slo_violations")
                if p99 < best_p99:
                    best_p99, best_p99_cfg = p99, cfg
                if slo < best_slo:
                    best_slo, best_slo_cfg = slo, cfg

            ratio = load(cfs, "load_ratio")
            if cfs_slo == 0 and best_slo == 0 and best_p99 < cfs_p99:
                pct = ((best_p99 - cfs_p99) / cfs_p99) * 100
                findings.append(f"- **{wl}@{conc}** (ratio={ratio}, underloaded): `{best_p99_cfg}` reduces p99 tail latency by {abs(pct):.1f}% vs CFS (all 0 SLO violations)")
            elif cfs_slo > 0 and best_slo < cfs_slo:
                reduction = ((best_slo - cfs_slo) / cfs_slo) * 100
                findings.append(f"- **{wl}@{conc}** (ratio={ratio}, overloaded): `{best_slo_cfg}` reduces SLO violations by {abs(reduction):.0f}% vs CFS ({cfs_slo:.0f} → {best_slo:.0f})")
            elif cfs_slo == 0 and best_slo == 0:
                if best_p99 < cfs_p99:
                    pct = ((best_p99 - cfs_p99) / cfs_p99) * 100
                    findings.append(f"- **{wl}@{conc}** (ratio={ratio}): `{best_p99_cfg}` p99 improved {abs(pct):.1f}% (all clean)")
                else:
                    findings.append(f"- **{wl}@{conc}** (ratio={ratio}): all schedulers have 0 SLO violations; CFS p99={cfs_p99:.1f}ms is competitive")
            else:
                findings.append(f"- **{wl}@{conc}** (ratio={ratio}): mixed results, see table above")

    for f in findings:
        L.append(f)

    L.append("")
    L.append("---")
    L.append("")
    L.append("*Generated by comprehensive benchmarking pipeline on amd002 (64 cores).*")
    L.append(f"*56 runs across 4 schedulers × 7 workloads × 2 concurrency levels.*")

    return "\n".join(L)


if __name__ == "__main__":
    report = generate_report()
    out_path = SCRIPT_DIR.parent / "result" / "COMPREHENSIVE_RESULT_0527.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report)
    print(f"Report written to {out_path}")
    print(report[:3000])
