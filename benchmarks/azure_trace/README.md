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

Example:

```sh
python3 benchmarks/azure_trace/build_azure_trace_benchmark.py \
  --trace-2021 benchmarks/third_party/AzurePublicDataset/data/AzureFunctionsInvocationTraceForTwoWeeksJan2021.rar \
  --window-ms 5400000 \
  --scale 45 \
  --output-dir /tmp/cosmos-azure-90m
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
