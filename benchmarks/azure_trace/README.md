# Azure 2019 CPU Trace Tools

This directory contains tools for processing the Azure Functions 2019 trace and
benchmarking COSMOS with realistic workload characteristics:

- `measure_cpu_time_distribution.py` - Extract CPU time distribution from trace
- `generate_top_functions_config.rs` - Generate mixed top-functions config
- `../scripts/replay_top_functions.py` - Replay top functions through COSMOS

## Key Concept: Expected vs Actual Time

**The critical insight for scheduler benchmarking:**

- **Expected time**: What we tell the scheduler (FIXED per function)
- **Actual time**: What the workload actually takes (SAMPLED from distribution, varies per invocation)

This tests scheduler robustness to **estimation error** - a critical real-world scenario where the scheduler's estimates don't match reality.

You can configure which percentile to use as expected time: p25, p50, p75, p99, or mean.

---

## Quick Start

### 1. Download the Azure dataset

```sh
mkdir -p benchmarks/third_party/AzurePublicDataset/data
cd benchmarks/third_party/AzurePublicDataset/data
wget https://azurecloudpublicdataset2.blob.core.windows.net/azurepublicdatasetv2/azurefunctions_dataset2019/azurefunctions-dataset2019.tar.xz
tar -xf azurefunctions-dataset2019.tar.xz
cd -
```

### 2. Measure CPU-time distribution

```sh
python3 benchmarks/azure_trace/measure_cpu_time_distribution.py \
  --dataset-dir benchmarks/third_party/AzurePublicDataset/data/azurefunctions-dataset2019 \
  --output benchmarks/azure_trace/results/azure_2019_cpu_distribution.json \
  --days 7 \
  --top-functions 20
```

This extracts:
- CPU time distribution (min, p25, p50, p75, p99, max) for each function
- Invocation counts (used as sampling weights)
- Candidate functions for mixed selection

### 3. Generate top functions config

```sh
cargo run --release -p cosmos-bench-profiler --bin generate_top_functions_config -- \
  --dataset-dir benchmarks/third_party/AzurePublicDataset/data/azurefunctions-dataset2019 \
  --output benchmarks/azure_trace/results/top20_p50.json \
  --top-functions 20 \
  --expected-time-percentile p50
```

This creates a config with:
- A mixed function set: by default top 10 by invocation count plus top 10 by total CPU time
- Frequency (sampling weight) for each function
- **Expected time** = p50 (what we tell the scheduler)
- **Time distribution** = full quantiles (what we sample from)

For a different split, pass `--frequency-top-functions` and
`--time-top-functions`. Total CPU time is computed as
`invocation_count * mean_cpu_time_ms`, so low-frequency expensive functions are
represented alongside the hottest functions.

**Try different expected time percentiles:**
- `--expected-time-percentile p50` - Optimistic (scheduler underestimates)
- `--expected-time-percentile p75` - Realistic
- `--expected-time-percentile p99` - Pessimistic (scheduler overestimates)

### 4. Benchmark with top functions

```sh
python3 benchmarks/scripts/replay_top_functions.py \
  --config-json benchmarks/azure_trace/results/top20_p50.json \
  --scheduler-config cosmos-full \
  --duration-s 30 \
  --max-concurrency 256
```

This:
1. Samples functions based on frequency
2. Uses **expected time** for scheduler metadata (fixed per function)
3. Samples **actual time** from distribution (varies per invocation)
4. Measures throughput, SLO violations, and estimation error

**Scheduler configs:**
- `cfs-default` - Linux CFS (no COSMOS)
- `sfs` - Shortest-First Scheduling
- `cosmos-metadata` - COSMOS with metadata only
- `cosmos-full` - COSMOS with metadata, deadline scoring, and short-task preemption

**Output:**
- `manifest.json` - Run configuration
- `results.json` - Summary statistics including estimation error
- `invocations.csv` - Per-invocation results with expected vs actual time
- Individual invocation details
- Scheduler and event bridge logs

---

## Example: Compare Estimation Strategies

```sh
# Generate configs with different expected time percentiles
for pct in p50 p75 p99; do
  cargo run --release -p cosmos-bench-profiler --bin generate_top_functions_config -- \
    --dataset-dir benchmarks/third_party/AzurePublicDataset/data/azurefunctions-dataset2019 \
    --output benchmarks/azure_trace/results/top20_${pct}.json \
    --top-functions 20 \
    --expected-time-percentile $pct
done

# Benchmark COSMOS with each strategy
for pct in p50 p75 p99; do
  python3 benchmarks/scripts/replay_top_functions.py \
    --config-json benchmarks/azure_trace/results/top20_${pct}.json \
    --scheduler-config cosmos-full \
    --duration-s 30
done
```

This tests how COSMOS handles:
- **p50**: Optimistic estimates (50% of invocations take longer than expected)
- **p75**: Balanced estimates (25% take longer)
- **p99**: Pessimistic estimates (1% take longer)

---

## Example: Compare Schedulers

```sh
# Generate config
cargo run --release -p cosmos-bench-profiler --bin generate_top_functions_config -- \
  --dataset-dir benchmarks/third_party/AzurePublicDataset/data/azurefunctions-dataset2019 \
  --output benchmarks/azure_trace/results/top20_p75.json \
  --top-functions 20 \
  --expected-time-percentile p75

# Run with different schedulers
for config in cfs-default sfs cosmos-full; do
  python3 benchmarks/scripts/replay_top_functions.py \
    --config-json benchmarks/azure_trace/results/top20_p75.json \
    --scheduler-config $config \
    --duration-s 30
done
```

---

## What You Get

```
Loading config from benchmarks/azure_trace/results/top20_p50.json
Loaded 20 functions
Median expected time: 85.3ms (SLO target: 85300us)
Scheduler started (PID: 12345)
Stats socket ready
Event bridge started (PID: 12346)

Firing invocations for 30s (max concurrency: 256)...
  Expected time: FIXED per function
  Actual time: SAMPLED from distribution

Benchmark completed in 30.12s

Results:
  Completed: 8543
  Failed: 0
  Throughput: 283.7 invocations/s
  SLO violations: 127 (1.49%)
  Latency (ms):
    min: 48.23
    p50: 82.45
    p95: 156.78
    p99: 189.34
    max: 245.67
    mean: 88.12
  Estimation error (actual - expected, ms):
    mean: 5.23
    p50: 3.45
    p95: 18.67
    p99: 32.11

Results written to benchmarks/scripts/results/azure_topn_cosmos_full/20260531T140523Z
```

---

## Input Dataset

By default the scripts look for the dataset in:

- `benchmarks/third_party/AzurePublicDataset/data/azurefunctions-dataset2019`

You can override with `--dataset-dir`.

Required CSV families:

- `invocations_per_function_md.anon.d*.csv*`
- `function_durations_percentiles.anon.d*.csv*`

---

## Notes

- The trace uses execution duration as a proxy for CPU time, which may include I/O wait
- **Expected time is fixed** per function (what scheduler sees)
- **Actual time varies** per invocation (sampled from distribution)
- This tests scheduler robustness to estimation error
- Top 20 functions typically cover 60-80% of total invocations
