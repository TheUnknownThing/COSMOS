# Benchmarks

This directory contains the active benchmark tooling for COSMOS. The current
benchmark design has two layers:

- `azure_trace/`: build Azure-derived replay plans and drive OpenWhisk actions.
- `local_harness/`: run the same workload shapes against local CFS, COSMOS, and
  SFS-style scheduler configurations.

## Layout

```text
benchmarks/
  azure_trace/
    build_azure_trace_benchmark.py
    run_openwhisk_azure_replay.py
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

## Azure Trace Benchmark

`azure_trace/build_azure_trace_benchmark.py` builds explicit benchmark artifacts
from Azure Functions traces:

- `invocations.csv`: flat invocation table.
- `profiles.json`: stable synthetic profile assignment per Azure function.
- `replay.json`: combined replay plan for COSMOS/OpenWhisk harnesses.
- `fidelity.json`: distribution comparison for the selected source window.

The 2021 Azure trace is used as arrival and duration truth. For each row:

```text
function_id = app + ':' + func
start_time_ms = (end_timestamp - duration) * 1000
target_duration_ms = duration * 1000
```

Resource behavior is not claimed to come from the 2021 trace. Profiles are
synthetic and distribution-guided by optional 2019 Azure data when available.
The generated files separate real target duration class from synthetic profile
duration class:

- `target_duration_class`: bucket from the real 2021 invocation duration.
- `profile_duration_class`: bucket used for profile/kernel assignment.
- `duration_class`: legacy alias for `profile_duration_class`.

Build a replay plan:

```sh
python3 benchmarks/azure_trace/build_azure_trace_benchmark.py \
  --trace-2021 benchmarks/third_party/AzurePublicDataset/data/AzureFunctionsInvocationTraceForTwoWeeksJan2021.rar \
  --window-ms 5400000 \
  --scale 45 \
  --output-dir /tmp/cosmos-azure-90m
```

## OpenWhisk Replay

`azure_trace/run_openwhisk_azure_replay.py` invokes OpenWhisk actions according
to `replay.json` offsets. It is intentionally external to SeBS and OpenWhisk
core.

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

The replay driver records submit/completion timing, exit status, and activation
IDs in `client_latency.csv`. It can also send local COSMOS event-bridge metadata
around each `wsk` invocation when `--event-bridge-port` is set.

## Local Harness

`local_harness/burst_benchmark.py` dispatches local CFS/COSMOS/SFS benchmark
runs. It uses `cosmos-benchmark-workload` from `benchmarks/workloads/runner` and
can run either built-in workload shapes or a generated replay plan.

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

Run a generated Azure replay locally:

```sh
sudo python3 benchmarks/local_harness/burst_benchmark.py \
  --config cosmos-full \
  --replay-plan /tmp/cosmos-azure-90m/replay.json \
  --out-dir /tmp/cosmos-local-replay \
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

The Azure profile layer maps functions to these kernel classes, with OpenWhisk
action names supplied by `--action-map`.

## Notes

- `benchmarks/third_party/AzurePublicDataset` is a submodule and is the
  canonical location for the Azure 2021 trace archive.
- `benchmarks/third_party/serverless-benchmarks` is retained only as optional
  source material for action implementations. The active replay path does not
  depend on SeBS experiment orchestration.
