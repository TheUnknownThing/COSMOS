# COSMOS

**CO-Scheduling Multi-resource OS for Serverless**

COSMOS is an invocation-aware CPU scheduler for serverless workloads, built on
`sched_ext`. The kernel-side BPF dispatcher stays generic — all policy lives in
Rust user space. Classification uses invocation metadata (deadlines, SLO
classes, cold-start flags) with heuristic fallback.

## Status

| Version | Branch / Ref | Status |
|---------|-------------|--------|
| **V1** | commit `484e993` (`main`) | **stable** — runs on kernel 7.0.9, passes benchmarks |
| **V2** | uncommitted (workspace) | WIP — 3-layer architecture (registry / adapter / policy); builds and survives CloudLab smoke tests on kernel 7.0.9 |

Work continues on V2 policy tuning; headline benchmarks below are from V1.

## Quick Start

**Requirements:** Linux >= 6.12 with `CONFIG_SCHED_CLASS_EXT=y`.

```sh
cargo build --release
sudo target/release/cosmos --slo-target-us 10000
```

Check status:
```sh
cat /sys/kernel/sched_ext/state    # enabled / disabled
cat /sys/kernel/sched_ext/root/ops # cosmos_*
```

## Policy

Tasks are classified into three buckets:

| Class | Criteria |
|-------|----------|
| **ColdStart** | Metadata `is_cold_start=1`, or first-seen heuristic match |
| **HotInvocation** | Latency-critical follow-ups or repeated short-running heuristic matches |
| **Background** | Explicit batch SLO class or non-invocation fallback work |

When invocation metadata is present (via shim or event bridge), the scheduler
uses metadata-first classification with EDF scoring. Without metadata it falls
back to runtime / wakeup heuristics, so mixed deployments still get sensible
latency-aware behavior.

## CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--policy <cosmos|sfs>` | `cosmos` | Select COSMOS or SFS-inspired policy |
| `-s, --slice-us` | 20000 | Scheduling slice (us) |
| `-S, --slice-us-min` | 500 | Minimum slice (us) |
| `--slo-target-us` | 10000 | Target invocation SLO (us) |
| `--cold-start-boost-us` | 20000 | Extra boost for cold-start tasks (us) |
| `--invocation-comm` | — | Comma-separated comm patterns for invocation workers |
| `--disable-pools` | false | Disable CPU pool partitioning |
| `--disable-builtin-idle` | false | Disable direct idle-CPU dispatch |
| `--disable-deadline-scoring` | false | Use vtime scoring only |
| `--tail-guard-threshold-us` | — | Slack threshold for tail guard promotion |
| `--profile-catalog` | — | Static JSON profile catalog loaded once at startup |
| `--latency-pool-pct` | 50 | % of CPUs in latency pool |
| `--sfs-threshold-window` | 100 | Arrival samples per SFS threshold update |
| `--sfs-min-credit-us` | 6000 | Minimum SFS short-job credit (us) |
| `--sfs-queue-delay-factor` | 3 | SFS demotion factor relative to threshold |
| `--stats <N>` | off | Print stats every N seconds |

`--policy sfs` keeps COSMOS's current scheduler intact and adds an alternate
comparison mode. This port preserves the SFS control idea inside `sched_ext`
(adaptive short-job credit, wakeup credit carryover, one-way demotion) but does
not literally flip Linux tasks between `FIFO` and `CFS` with `schedtool`.

## Runtime Actuators

The co-scheduling actuator loop is enabled in the scheduler and reads desired
resource allocations from the invocation registry.

| Environment variable | Default | Description |
|----------------------|---------|-------------|
| `COSMOS_CGROUP_POLICY_ROOT` | `/sys/fs/cgroup/cosmos-policy` | Safety boundary for cgroup writes. The cgroup actuator only writes controls for invocation cgroups under this root. |
| `COSMOS_NET_TC_IFACE` | unset | Network interface for the Aya tc actuator. When set, COSMOS loads and attaches the embedded `cosmos_net_tc` classifier on egress and updates its `flow_policies` map from registry allocations. |

## Benchmarks

Active benchmark tooling lives under two explicit paths:

- `benchmarks/azure_trace/` builds Azure-derived replay plans and replays them
  against OpenWhisk actions.
- `benchmarks/local_harness/` runs local CFS, COSMOS, and SFS-style scheduler
  experiments with the same workload kernels.

The local harness supports these synthetic workload shapes via
`cosmos-benchmark-workload`: `cpu_burst`, `sleep_short`, `io_mixed`,
`memory_heavy`, `network_heavy`, `compression_mixed`, `graph_bfs`.

Supported local configs: `cfs-default`, `cosmos-heuristic`,
`cosmos-metadata`, `cosmos-pooled`, `cosmos-full`, `sfs`.

```sh
# Build everything
cargo build --release --workspace

# CFS baseline
sudo python3 benchmarks/local_harness/burst_benchmark.py \
    --config cfs-default --workload cpu_burst --concurrency 100 \
    --duration-ms 5000 --out-dir results/

# COSMOS full
sudo python3 benchmarks/local_harness/burst_benchmark.py \
    --config cosmos-full --workload cpu_burst --concurrency 100 \
    --duration-ms 5000 --out-dir results/ \
    --scheduler-bin target/release/cosmos
```

Build an Azure replay plan:

```sh
python3 benchmarks/azure_trace/build_azure_trace_benchmark.py \
    --trace-2021 benchmarks/third_party/AzurePublicDataset/data/AzureFunctionsInvocationTraceForTwoWeeksJan2021.rar \
    --window-ms 5400000 \
    --scale 45 \
    --output-dir /tmp/cosmos-azure-90m
```

Replay it through OpenWhisk:

```sh
python3 benchmarks/azure_trace/run_openwhisk_azure_replay.py \
    --replay /tmp/cosmos-azure-90m/replay.json \
    --action-map cpu_burst=ow_cpu_burst \
    --action-map pipeline=ow_pipeline \
    --action-map memory_heavy=ow_memory_heavy \
    --action-map io_mixed=ow_io_mixed \
    --action-map network_heavy=ow_network_heavy
```

### Results (V1, 48-core CloudLab, cpu_burst @ 100 concurrency)

| Metric | CFS | COSMOS V1 | Delta |
|--------|-----|-----------|-------|
| p50 latency | 10347 ms | 7598 ms | **-26.6%** |
| Mean latency | 10087 ms | 8647 ms | **-14.3%** |
| SLO violations | 73 | 25 | **-65.8%** |

COSMOS eliminated two-thirds of SLO violations while cutting mean latency 14%.

## Repository Layout

```
.
├── src/               # COSMOS scheduler (V1: main.rs; V2: adapter/, policy/, registry/, scheduler.rs)
├── rust/              # Vendored sched_ext crates
│   ├── scx_rustland_core/   # User-space scheduler core + BPF assets
│   ├── scx_utils/           # Topology, exit info, compat
│   ├── scx_stats/           # Stats transport + derive macro
│   └── scx_cargo/           # BPF binding / skeleton generation
├── main.bpf.c         # BPF kernel-side dispatcher
├── intf.h             # Shared BPF/user-space structs
├── cosmos-event-bridge/  # OpenWhisk/local events → scheduler metadata TCP
├── benchmarks/
│   ├── azure_trace/      # Azure trace builder + OpenWhisk replay driver
│   ├── local_harness/    # CFS/COSMOS/SFS local benchmark harness
│   ├── workloads/runner/ # Rust workload binary (7 synthetic workloads)
│   └── third_party/AzurePublicDataset/ # Azure trace submodule
├── scheds/include/    # Vendored sched-ext BPF headers
└── test_ssh.ignore.md # Testbed setup & operations guide
```

## Vendored Library Fix

The vendored `scx_rustland_core` (v2.4.11) overwrites `src/bpf.rs` during
build, removing COSMOS-specific APIs.  Two changes are needed in
`rust/scx_rustland_core/assets/bpf.rs` (the template):

1. `shutdown` field → `pub shutdown` (line 196)
2. `fn task_tgid()` → `pub fn task_tgid()` (line 549)

Apply the same fixes to `src/bpf.rs` so the generated wrapper preserves the
COSMOS-specific pinned maps and helpers.

## Metadata Injection

Two paths:

| Path | Mechanism | Use case |
|------|-----------|----------|
| Event bridge | TCP `127.0.0.1:9731`, NDJSON | OpenWhisk integration |
| Local events | Direct TCP to event bridge | Benchmark harness |

Metadata writes may carry an optional `profile_id` plus optional inline
`profile_hints`. When `--profile-catalog <path>` is set, COSMOS resolves
`profile_id` against the loaded catalog and then applies inline hints as
field-by-field overrides.

## Testbed

See `test_ssh.ignore.md` for the CloudLab testbed setup (kernel 7.0.9,
toolchains, Docker, benchmark workflow).
