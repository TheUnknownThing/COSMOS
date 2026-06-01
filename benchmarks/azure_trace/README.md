# Azure Trace Replay

This directory owns the Azure trace-shaped benchmark generators and OpenWhisk
replay driver. The classified generators should be described as
**Azure trace-shaped, 2019-classified synthetic replay**, not as a real Azure
benchmark: arrival and duration skew come from public Azure traces, while
resource and cold/warm behavior are constructed for local OpenWhisk/SeBS actions.

## Builder

`build_azure_trace_benchmark.py` reads the Azure Functions 2021 invocation trace
and emits:

- `invocations.csv`
- `profiles.json`
- `replay.json`
- `fidelity.json`

The 2021 trace provides only arrival and target duration truth. The builder
computes:

```text
function_id = app + ':' + func
source_start_ms = (end_timestamp - duration) * 1000
target_duration_ms = duration * 1000
```

Synthetic profile fields such as trigger type, memory, kernel, cold-start model,
and profile duration class are assigned separately and reproducibly per function.
Use `target_duration_class` when analyzing real Azure durations.

`build_azure_trace_2019_synthetic.py` is the stricter classified path. It
requires the Azure Functions 2019 CSV families and refuses to generate when they
are unavailable. That script uses 2019 trigger, duration, memory, and invocation
shape as a probabilistic workload-family classifier, then maps families to the
local synthetic kernels.

`build_azure_trace_2019_direct.py` is the direct 2019-arrival path. It expands
the public 2019 per-minute invocation counts into deterministic sub-minute
arrivals using one of four modes:

- `uniform-within-minute`
- `front-loaded-burst`
- `evenly-spaced`
- `clustered-bursty`

It also carries richer 2019 fields into `invocations.csv` and `replay.json`:
duration p25/p50/p75/p99/max, memory p50/p75/p95/p99, `HashOwner`, and
app-level function grouping.

`build_azure_trace_2019_sebs.py` uses the same classifier but maps workload
families to SeBS benchmark IDs such as `120.uploader`, `210.thumbnailer`,
`220.video-processing`, and `411.image-recognition`. It emits SeBS payload hints
in `replay.json`; OpenWhisk action names are still supplied by `--action-map`.

Example:

```sh
python3 benchmarks/azure_trace/build_azure_trace_benchmark.py \
  --trace-2021 benchmarks/third_party/AzurePublicDataset/data/AzureFunctionsInvocationTraceForTwoWeeksJan2021.rar \
  --window-ms 5400000 \
  --scale 45 \
  --output-dir /tmp/cosmos-azure-90m
```

Classified synthetic example:

```sh
python3 benchmarks/azure_trace/build_azure_trace_2019_synthetic.py \
  --trace-2021 benchmarks/third_party/AzurePublicDataset/data/AzureFunctionsInvocationTraceForTwoWeeksJan2021.rar \
  --dataset-2019-dir /path/to/azurefunctions-dataset2019 \
  --window-ms 5400000 \
  --scale 45 \
  --output-dir /tmp/cosmos-azure-2019-classified-90m
```

Direct 2019-arrival example:

```sh
python3 benchmarks/azure_trace/build_azure_trace_2019_direct.py \
  --dataset-2019-dir /path/to/azurefunctions-dataset2019 \
  --arrival-mode clustered-bursty \
  --window-ms 5400000 \
  --scale 45 \
  --output-dir /tmp/cosmos-azure-2019-direct-clustered-90m
```

Use direct replay variants instead of a single headline plan when separating
OpenWhisk queueing from scheduler behavior:

```sh
# Bursty Azure-shaped
--arrival-mode clustered-bursty --workload-mix-mode trace

# Smoothed Azure-shaped
--arrival-mode evenly-spaced --workload-mix-mode trace

# Balanced workload mix
--arrival-mode evenly-spaced --workload-mix-mode balanced

# Peak-stress dominant workload
--arrival-mode front-loaded-burst --workload-mix-mode peak-stress

# Target-duration-aware SLO labeling
--deadline-mode target-duration-aware
```

Classified SeBS example:

```sh
python3 benchmarks/azure_trace/build_azure_trace_2019_sebs.py \
  --trace-2021 benchmarks/third_party/AzurePublicDataset/data/AzureFunctionsInvocationTraceForTwoWeeksJan2021.rar \
  --dataset-2019-dir /path/to/azurefunctions-dataset2019 \
  --window-ms 5400000 \
  --scale 45 \
  --output-dir /tmp/cosmos-azure-2019-sebs-90m
```

## OpenWhisk Replay

`run_openwhisk_azure_replay.py` reads `replay.json` and invokes OpenWhisk actions
at `at_ms` offsets with `wsk action invoke`.

Example:

```sh
python3 benchmarks/azure_trace/run_openwhisk_azure_replay.py \
  --replay /tmp/cosmos-azure-90m/replay.json \
  --out-dir /tmp/cosmos-openwhisk-replay \
  --action-map cpu_burst=ow_cpu_burst \
  --action-map pipeline=ow_pipeline \
  --action-map memory_heavy=ow_memory_heavy \
  --action-map io_mixed=ow_io_mixed \
  --action-map network_heavy=ow_network_heavy
```

The driver writes `client_latency.csv`, payloads, raw `wsk` output, and fetched
activation records when polling is enabled.

For calibrated SLO reporting, first run isolated action calibration and then
replay with that calibration:

```sh
python3 benchmarks/azure_trace/run_openwhisk_azure_replay.py \
  --calibrate \
  --calibration-repetitions 5 \
  --action-map cpu_burst=ow_cpu_burst \
  --action-map network_heavy=ow_network_heavy \
  --action-map pipeline=ow_pipeline

python3 benchmarks/azure_trace/run_openwhisk_azure_replay.py \
  --replay /tmp/cosmos-azure-2019-direct-clustered-90m/replay.json \
  --slo-calibration /path/to/calibration.json \
  --slo-deadline-multiplier 2 \
  --action-map cpu_burst=ow_cpu_burst \
  --action-map network_heavy=ow_network_heavy \
  --action-map pipeline=ow_pipeline
```

With calibration, deadlines are computed as
`deadline = k * isolated_warm_p99[action]`. The replay summary reports
arrival shape, peak 1-second arrival rate, action mix, per-workload SLO success
and goodput, submit lag, post-submit latency, target-duration/deadline ratios,
impossible-deadline counts, `slo_goodput_per_s`, `slo_success_rate`, and
normalized slowdown percentiles.
