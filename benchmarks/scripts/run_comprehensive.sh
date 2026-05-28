#!/bin/bash
set -euo pipefail

# Comprehensive Benchmark: CFS vs SFS vs COSMOS on all 7 workloads
# Host: amd002 (64c AMD EPYC 7452)
# Date: $(date +%Y-%m-%d)
#
# Matrix: 4 configs × 7 workloads × 2 concurrency levels = 56 runs
# Configs: cfs-default, sfs, cosmos-heuristic, cosmos-full
# Workloads: cpu_burst, sleep_short, io_mixed, memory_heavy,
#            network_heavy, compression_mixed, graph_bfs

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BENCH_SCRIPT="$SCRIPT_DIR/burst_benchmark.py"
RESULTS_BASE="$SCRIPT_DIR/results"
LOG_DIR="$RESULTS_BASE/_logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DURATION_MS=250
DEADLINE_US=500000

mkdir -p "$LOG_DIR"

# ── workload → concurrency_levels matrix ──────────────────────────
declare -A WORKLOAD_CONCURRENCIES
WORKLOAD_CONCURRENCIES["cpu_burst"]="64 128"
WORKLOAD_CONCURRENCIES["sleep_short"]="64 128"
WORKLOAD_CONCURRENCIES["io_mixed"]="32 64"
WORKLOAD_CONCURRENCIES["memory_heavy"]="32 64"
WORKLOAD_CONCURRENCIES["network_heavy"]="32 64"
WORKLOAD_CONCURRENCIES["compression_mixed"]="64 128"
WORKLOAD_CONCURRENCIES["graph_bfs"]="32 64"

CONFIGS=("cfs-default" "sfs" "cosmos-heuristic" "cosmos-full")

total_runs=0
for wl in "${!WORKLOAD_CONCURRENCIES[@]}"; do
    for c in ${WORKLOAD_CONCURRENCIES[$wl]}; do
        total_runs=$((total_runs + ${#CONFIGS[@]}))
    done
done

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  COSMOS Comprehensive Benchmark — $TIMESTAMP           ║"
echo "║  Host: $(hostname 2>/dev/null || echo amd002) ($(nproc) cores)                     ║"
echo "║  Configs: ${CONFIGS[*]}"
echo "║  Workloads: ${!WORKLOAD_CONCURRENCIES[@]}"
echo "║  Duration: ${DURATION_MS}ms / Deadline: ${DEADLINE_US}us"
echo "║  Total runs: $total_runs                                   ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

run_count=0
failed_runs=0
declare -a FAILED_RUNS

run_bench() {
    local config=$1
    local workload=$2
    local concurrency=$3
    local out_dir="$RESULTS_BASE/${config}_comprehensive"

    run_count=$((run_count + 1))
    local pct=$(( run_count * 100 / total_runs ))

    local log_file="$LOG_DIR/${config}_${workload}_c${concurrency}.log"

    echo "─────────────────────────────────────────────────────────────"
    echo "[$run_count/$total_runs] ${config} | ${workload} | c=${concurrency}"
    echo "  out: $out_dir"

    local cmd
    local need_sudo=false
    if [ "$config" = "cfs-default" ]; then
        cmd="python3 $BENCH_SCRIPT \
            --config $config \
            --workload $workload \
            --concurrency $concurrency \
            --duration-ms $DURATION_MS \
            --deadline-us $DEADLINE_US \
            --out-dir $out_dir"
    else
        need_sudo=true
        cmd="sudo python3 $BENCH_SCRIPT \
            --config $config \
            --workload $workload \
            --concurrency $concurrency \
            --duration-ms $DURATION_MS \
            --deadline-us $DEADLINE_US \
            --out-dir $out_dir"
    fi

    # Capture both stdout and stderr
    if eval "$cmd" > "$log_file" 2>&1; then
        local result_dir=$(tail -1 "$log_file" | tr -d '\n')
        echo "  OK  → $result_dir"
    else
        local rc=$?
        echo "  FAIL (exit=$rc) — see $log_file"
        failed_runs=$((failed_runs + 1))
        FAILED_RUNS+=("$config|$workload|c=$concurrency")
        # Show last 5 lines of log for diagnostics
        echo "  --- tail of log ---"
        tail -5 "$log_file" | sed 's/^/  | /'
        echo "  -------------------"
        return 1
    fi
    return 0
}

# ── Run all benchmarks ───────────────────────────────────────────
start_time=$(date +%s)

for config in "${CONFIGS[@]}"; do
    for workload in "${!WORKLOAD_CONCURRENCIES[@]}"; do
        for concurrency in ${WORKLOAD_CONCURRENCIES[$workload]}; do
            run_bench "$config" "$workload" "$concurrency" || true
            sleep 1  # brief cooldown between runs
        done
    done
done

end_time=$(date +%s)
elapsed=$((end_time - start_time))
elapsed_min=$((elapsed / 60))
elapsed_sec=$((elapsed % 60))

# ── Summary ──────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  BENCHMARK COMPLETE                                         ║"
echo "║  Total: $total_runs  |  Failed: $failed_runs                   ║"
echo "║  Elapsed: ${elapsed_min}m ${elapsed_sec}s                                      ║"
echo "╚══════════════════════════════════════════════════════════════╝"

if [ $failed_runs -gt 0 ]; then
    echo ""
    echo "Failed runs:"
    for f in "${FAILED_RUNS[@]}"; do
        echo "  • $f"
    done
fi

echo ""
echo "Logs: $LOG_DIR"
echo "Results:"
for config in "${CONFIGS[@]}"; do
    echo "  $RESULTS_BASE/${config}_comprehensive/"
done
