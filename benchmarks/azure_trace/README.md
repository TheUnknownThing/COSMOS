# Azure Trace Replay

This directory owns the hybrid Azure benchmark generator and OpenWhisk replay
driver.

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
