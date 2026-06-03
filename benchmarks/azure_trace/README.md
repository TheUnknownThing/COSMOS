# Azure 2019 CPU Trace Tools

This directory is intentionally narrow. The older Azure/OpenWhisk replay
builders from `co-schedule` are deprecated here; the retained files are the CPU
top-functions artifacts and helpers used by `cpu-azure-trace`.

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

For deterministic replay, generate or reuse an invocation pool:

```sh
python3 benchmarks/scripts/generate_pool.py \
  --config-json benchmarks/azure_trace/top20_mixed_p75.json \
  --pool-size 100000 \
  --seed 42 \
  --output benchmarks/azure_trace/pool_mixed_p75_cap10000.json

python3 benchmarks/scripts/run_pool.py \
  --pool-json benchmarks/azure_trace/pool_mixed_p75_cap10000.json \
  --config cosmos-full \
  --run-duration-s 180 \
  --warmup-duration-s 30
```
