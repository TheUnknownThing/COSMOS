# Azure 2019 CPU Trace Tools

This directory contains two standalone scripts for the Azure Functions 2019
trace, scoped to the CPU-only path:

- `measure_cpu_time_distribution.py`
- `generate_cpu_workload_stream.py`

The 2019 public trace does not expose CPU cycles. It exposes execution duration
percentiles and per-minute invocation counts. These scripts therefore treat
duration-derived service time as the CPU target for synthetic `cpu_burst`
workloads.

## Input dataset

By default the scripts look for the dataset in one of these locations:

- `benchmarks/third_party/AzurePublicDataset/data/azurefunctions-dataset2019`
- `feat/co-schedule/benchmarks/third_party/AzurePublicDataset/data/azurefunctions-dataset2019`

You can override that with `--dataset-dir`.

Required CSV families:

- `invocations_per_function_md.anon.d*.csv*`
- `function_durations_percentiles.anon.d*.csv*`

## 1. Measure CPU-time distribution

```sh
python3 benchmarks/azure_trace/measure_cpu_time_distribution.py \
  --dataset-dir feat/co-schedule/benchmarks/third_party/AzurePublicDataset/data/azurefunctions-dataset2019 \
  --output benchmarks/azure_trace/results/azure_2019_cpu_distribution.json
```

The output JSON contains:

- `arrival_distribution.per_minute_counts`: aggregate minute-level arrival shape
- `cpu_time_distribution`: weighted global CPU-time summary
- `profiles`: compact weighted CPU profiles grouped by `trigger + cpu_time_bucket`
- `top_functions`: heavy hitters for inspection

## 2. Generate a workload stream

```sh
python3 benchmarks/azure_trace/generate_cpu_workload_stream.py \
  --distribution-json benchmarks/azure_trace/results/azure_2019_cpu_distribution.json \
  --output-dir benchmarks/azure_trace/results/azure_2019_cpu_stream \
  --window-minutes 180 \
  --count-scale 1.0 \
  --time-scale 1.0 \
  --arrival-mode clustered-bursty
```

This writes:

- `invocations.csv`
- `replay.json`

Each invocation is a synthetic `cpu_burst` request with:

- `duration_ms` / `target_duration_ms`
- `expected_duration_ms`
- `deadline_us`
- `slo_class`
- `at_ms`

That stream is independent of the old `feat/co-schedule` builders and is meant
to be a simple CPU-only source for local profiling and throughput experiments.

