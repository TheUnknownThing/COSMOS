# Benchmarks

This directory contains the active benchmark tooling for COSMOS. The current
benchmark design has two local paths:

- `azure_trace/`: retained Azure Functions 2019 CPU top-functions artifacts and
  config generation helpers.
- `local_harness/`: run the same workload shapes against local CFS, COSMOS, and
  SFS-style scheduler configurations.

## Layout

```text
benchmarks/
  azure_trace/
    cpu_trace_common.py
    generate_top_functions_config.rs
    top20_mixed_p75.json
    pool_mixed_p75_cap10000.json
    README.md
  local_harness/
    harness.py
    burst_benchmark.py
    run_cosmos.py
    run_baseline.py
    measure_latency.py
  configs/
    profile_catalog.json
  workloads/runner/
    Cargo.toml
    src/main.rs
  third_party/AzurePublicDataset    # Azure trace submodule
  third_party/serverless-benchmarks # optional SeBS action source submodule
```

## Azure CPU Trace Benchmark

The retained Azure trace path is CPU-only. It uses Azure Functions 2019 duration
percentiles and invocation counts to generate `cosmos.azure.top-functions-config`
files for the local replay scripts.

```sh
cargo run --release -p cosmos-offline --bin generate_top_functions_config -- \
  --dataset-dir benchmarks/third_party/AzurePublicDataset/data/azurefunctions-dataset2019 \
  --output benchmarks/azure_trace/results/top20_p75.json \
  --top-functions 20 \
  --expected-time-percentile p75
```

Replay the generated top-functions config locally:

```sh
python3 benchmarks/scripts/replay_top_functions.py \
  --config-json benchmarks/azure_trace/top20_mixed_p75.json \
  --config cosmos-full \
  --run-duration-s 30 \
  --warmup-duration-s 5
```

## Local Harness

`local_harness/burst_benchmark.py` dispatches local CFS/COSMOS/SFS benchmark
runs. It uses `cosmos-benchmark-workload` from `benchmarks/workloads/runner` and
can run built-in workload shapes and mixed co-scheduling scenarios.

Build the scheduler and workload runner:

```sh
cargo build --release --workspace
```

Run a CFS baseline:

```sh
sudo python3 benchmarks/local_harness/burst_benchmark.py \
  --config cfs-default \
  --workload cpu_burst \
  --concurrency 100 \
  --duration-ms 5000 \
  --out-dir /tmp/cosmos-local
```

Run COSMOS full policy:

```sh
sudo python3 benchmarks/local_harness/burst_benchmark.py \
  --config cosmos-full \
  --workload cpu_burst \
  --concurrency 100 \
  --duration-ms 5000 \
  --out-dir /tmp/cosmos-local \
  --scheduler-bin target/release/cosmos
```

Run a deterministic Azure CPU pool replay:

```sh
sudo python3 benchmarks/scripts/run_pool.py \
  --pool-json benchmarks/azure_trace/pool_mixed_p75_cap10000.json \
  --config cosmos-full \
  --run-duration-s 180 \
  --warmup-duration-s 30 \
  --scheduler-bin target/release/cosmos
```

## Active Workload Shapes

The local harness supports these executable kernels:

- `cpu_burst`
- `sleep_short`
- `io_mixed`
- `memory_heavy`
- `network_heavy`
- `compression_mixed`
- `graph_bfs`

The Azure CPU replay maps all selected functions to the local CPU workload path
while preserving per-function runtime distributions and expected-time metadata.

## Notes

- `benchmarks/third_party/AzurePublicDataset` is a submodule and is the
  canonical location for the Azure Functions 2019 dataset.
- `benchmarks/third_party/serverless-benchmarks` is retained only as optional
  source material. The active Azure CPU replay path does not depend on SeBS
  experiment orchestration.
