# Azure 2019 Trace Replay Tools

This directory is intentionally narrow. The older Azure/OpenWhisk replay

By default the replay remains CPU-only for backward compatibility, but the
scripts can now assign local semantic workload types without going through
OpenWhisk.

## Files

- `cpu_trace_common.py`: shared Azure Functions 2019 CSV helpers.
- `generate_top_functions_config.rs`: Rust generator that reads the 2019 CSVs
  and writes a `cosmos.azure.top-functions-config` file.
- `generate_top_functions_config.py`: Python converter for an existing
  `cosmos.azure.2019.cpu-distribution` JSON.
- `top20_p75.json`, `top20_mixed_p75.json`: generated top-functions configs.
- `pool_mixed_p75_cap10000.json`: deterministic capped invocation pool.
- `results/`: summary artifacts for the checked-in pool.

Replay and pool execution live in `benchmarks/scripts/replay_top_functions.py`,
`benchmarks/scripts/generate_pool.py`, and `benchmarks/scripts/run_pool.py`.

## Generate A Config

```sh
cargo run --release -p cosmos-offline --bin generate_top_functions_config -- \
  --dataset-dir benchmarks/third_party/AzurePublicDataset/data/azurefunctions-dataset2019 \
  --output benchmarks/azure_trace/results/top20_p75.json \
  --top-functions 20 \
  --expected-time-percentile p75
```

The generator selects a mixed set of frequent and total-CPU-heavy functions.
`expected_time_ms` is the fixed runtime estimate sent to the scheduler; actual
runtime is sampled from each function's distribution by the replay scripts.

## Replay Top Functions

```sh
python3 benchmarks/scripts/replay_top_functions.py \
  --config-json benchmarks/azure_trace/top20_mixed_p75.json \
  --config cosmos-full \
  --run-duration-s 30 \
  --warmup-duration-s 5
```

Use a semantic local workload mix when measuring scheduling policy behavior
under heterogeneous work:

```sh
python3 benchmarks/scripts/replay_top_functions.py \
  --config-json benchmarks/azure_trace/top20_mixed_p75.json \
  --workload-mix balanced \
  --config cosmos-full \
  --run-duration-s 30 \
  --warmup-duration-s 5
```

Supported mix names are `balanced`, `cpu-heavy`, `io-heavy`, `memory-heavy`,
and `network-heavy`. The default `config` mode reads per-function `workload`
fields when they exist and otherwise falls back to `cpu_burst`; `cpu-only`
forces the old all-CPU behavior.

For deterministic replay, generate or reuse an invocation pool:

```sh
python3 benchmarks/scripts/generate_pool.py \
  --config-json benchmarks/azure_trace/top20_mixed_p75.json \
  --workload-mix balanced \
  --pool-size 100000 \
  --seed 42 \
  --output benchmarks/azure_trace/pool_mixed_p75_cap10000.json

python3 benchmarks/scripts/run_pool.py \
  --pool-json benchmarks/azure_trace/pool_mixed_p75_cap10000.json \
  --config cosmos-full \
  --run-duration-s 180 \
  --warmup-duration-s 30
```

Generated pools include a short per-workload CPU-demand calibration. Load
sweeps use `actual_mean_cpu_time_ms` for `offered_load`, so the x-axis tracks
CPU demand divided by CPU capacity. The original wall-duration load is still
reported as `wall_duration_offered_load` for comparison with older artifacts.
