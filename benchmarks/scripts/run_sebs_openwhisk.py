#!/usr/bin/env python3
"""SeBS + OpenWhisk integration benchmark: CFS vs SFS vs COSMOS-full"""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path("/opt/COSMOS")
PROFILER = REPO_ROOT / "target" / "release" / "cosmos-bench-profiler"
SCHEDULER = REPO_ROOT / "target" / "release" / "cosmos"
OUT_BASE = REPO_ROOT / "benchmarks" / "runs" / "sebs-openwhisk"

ACTIONS = [
    ("sebs_cpu_burn", "n=5000000"),
    ("sebs_sleep_node", ""),
    ("sebs_sleep_py", ""),
    ("sebs_html_node", ""),
]

REPETITIONS = 5
SCHEDULERS = ["cfs-default", "sfs", "cosmos-full"]


def run_cmd(cmd, timeout=120):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)


def start_scheduler(config, workdir):
    if config == "cfs-default":
        return None
    flags = ["--policy", "sfs"] if config == "sfs" else ["--slo-target-us", "500000"]
    log = open(str(OUT_BASE / f"{config}_scheduler.log"), "w")
    proc = subprocess.Popen(
        [str(SCHEDULER), *flags],
        stdout=log, stderr=subprocess.STDOUT, cwd=str(workdir),
    )
    time.sleep(3)
    return {"proc": proc, "log": log}


def stop_scheduler(entry):
    if entry is None:
        return
    proc = entry["proc"]
    try:
        proc.send_signal(signal.SIGINT)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    entry["log"].close()
    time.sleep(2)


def run_action(config, action, param, run_id):
    parts = [
        "sudo", str(PROFILER), "open-whisk",
        "--action", action, "--skip-update", "--insecure",
        "--warmth", "warm", "--sample-ms", "50",
        "--out-dir", str(OUT_BASE),
    ]
    if param:
        parts.extend(["--param", param])

    log_path = OUT_BASE / f"{config}_{action}_r{run_id}.log"
    start_ns = time.monotonic_ns()
    result = subprocess.run(parts, capture_output=True, text=True, timeout=120)
    elapsed_ms = (time.monotonic_ns() - start_ns) / 1_000_000
    log_path.write_text(result.stdout + "\n" + result.stderr)
    return elapsed_ms, result.returncode == 0


def main():
    OUT_BASE.mkdir(parents=True, exist_ok=True)

    all_results = {}

    for action, param in ACTIONS:
        print(f"\n=== Action: {action} {'(' + param + ')' if param else ''} ===")
        action_results = {}

        for config in SCHEDULERS:
            print(f"  {config}: ", end="", flush=True)

            sched_entry = start_scheduler(config, REPO_ROOT)
            latencies = []
            failures = 0

            for i in range(1, REPETITIONS + 1):
                try:
                    ms, ok = run_action(config, action, param, i)
                    if ok:
                        latencies.append(ms)
                        print(".", end="", flush=True)
                    else:
                        failures += 1
                        print("x", end="", flush=True)
                except Exception as e:
                    failures += 1
                    print("E", end="", flush=True)

                time.sleep(0.3)

            stop_scheduler(sched_entry)

            if latencies:
                sorted_lats = sorted(latencies)
                n = len(sorted_lats)
                p50 = sorted_lats[n // 2]
                p95_idx = min(int(n * 0.95), n - 1)
                p95 = sorted_lats[p95_idx]
                mean = sum(sorted_lats) / n
                print(f" p50={p50:.0f}ms p95={p95:.0f}ms mean={mean:.0f}ms fails={failures}")
                action_results[config] = {
                    "p50_ms": p50, "p95_ms": p95, "mean_ms": mean,
                    "latencies": sorted_lats, "failures": failures, "count": n,
                }
            else:
                print(f" ALL FAILED")

        all_results[action] = action_results

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY: SeBS + OpenWhisk — CFS vs SFS vs COSMOS-full")
    print("=" * 70)
    print(f"{'Action':<22} {'CFS p50/p95':>15} {'SFS p50/p95':>15} {'COSMOS p50/p95':>15}")
    print("-" * 67)

    for action, _ in ACTIONS:
        r = all_results.get(action, {})
        cfs = r.get("cfs-default", {})
        sfs = r.get("sfs", {})
        cosmos = r.get("cosmos-full", {})

        cfs_s = f"{cfs.get('p50_ms',0):.0f}/{cfs.get('p95_ms',0):.0f}ms" if cfs else "N/A"
        sfs_s = f"{sfs.get('p50_ms',0):.0f}/{sfs.get('p95_ms',0):.0f}ms" if sfs else "N/A"
        cos_s = f"{cosmos.get('p50_ms',0):.0f}/{cosmos.get('p95_ms',0):.0f}ms" if cosmos else "N/A"

        print(f"{action:<22} {cfs_s:>15} {sfs_s:>15} {cos_s:>15}")

    # Save to JSON
    (OUT_BASE / "summary.json").write_text(json.dumps(all_results, indent=2))
    print(f"\nSaved to {OUT_BASE / 'summary.json'}")


if __name__ == "__main__":
    main()
