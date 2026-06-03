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
| `--disable-builtin-idle` | false | Disable direct idle-CPU dispatch |
| `--disable-deadline-scoring` | false | Use vtime scoring only |
| `--profile-catalog` | — | Static JSON profile catalog loaded once at startup |
| `--short-task-threshold-us` | `--slice-us` | Runtime threshold for short-task preemption |
| `--disable-short-preemption` | false | Disable preempt kicks for short latency-sensitive tasks |
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

- `benchmarks/azure_trace/` keeps Azure Functions 2019 CPU top-functions
  configs and pool artifacts for local CPU replay.
- `benchmarks/local_harness/` runs local CFS, COSMOS, and SFS-style scheduler
  experiments with the same workload kernels.

The local harness supports these synthetic workload shapes via
`cosmos-benchmark-workload`: `cpu_burst`, `sleep_short`, `io_mixed`,
`memory_heavy`, `network_heavy`, `compression_mixed`, `graph_bfs`.

Supported local configs: `cfs-default`, `cosmos-heuristic`,
`cosmos-metadata`, `cosmos-full`, `sfs`.

Mixed co-scheduling scenarios are first-class in the local harness through
`--mix`, with per-SLO-class summaries for p50/p95/p99 latency, SLO violations,
goodput, and latency-critical-relative slowdown. Use this path for scheduler
claims about LC plus batch interference:

```sh
sudo python3 benchmarks/local_harness/burst_benchmark.py \
    --config cosmos-full \
    --mix 'network_heavy:24(slo=0;duration_ms=125;deadline_ms=250),cpu_burst:72(slo=2;duration_ms=500;deadline_ms=2000)' \
    --out-dir results/mixed-lc-network-vs-batch-cpu \
    --scheduler-bin target/release/cosmos
```

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

Before running metadata-enabled COSMOS benchmarks on a fresh host, prepare the
benchmark cgroup root so child cgroups expose the `cpu`, `io`, `memory`,
`cpuset`, and `pids` controllers:

```sh
sudo benchmarks/scripts/prepare_cosmos_cgroup_root.sh
```

Build an Azure CPU top-functions config:

```sh
cargo run --release -p cosmos-offline --bin generate_top_functions_config -- \
    --dataset-dir benchmarks/third_party/AzurePublicDataset/data/azurefunctions-dataset2019 \
    --output benchmarks/azure_trace/results/top20_p75.json \
    --top-functions 20 \
    --expected-time-percentile p75
```

Replay the CPU top-functions workload locally:

```sh
python3 benchmarks/scripts/replay_top_functions.py \
    --config-json benchmarks/azure_trace/top20_mixed_p75.json \
    --config cosmos-full \
    --run-duration-s 30 \
    --warmup-duration-s 5
```

### OpenWhisk Testbed Setup

The CloudLab OpenWhisk path is intentionally script-driven so a cloned image or
fresh boot does not depend on ad hoc shell state. On `amd006` this was validated
with `openwhisk/standalone:nightly`, Docker 29.5.2, kernel
`7.0.9-070009-generic`, and `wsk` configured against port `3233`. Image-owned
assets live under `/opt/COSMOS`, `/opt/OpenWhisk`, `/opt/cargo`, and
`/opt/rustup`; `wsk` uses `/opt/OpenWhisk/wskprops` through the
`/usr/local/bin/wsk` wrapper. Do not rely on files under the CloudLab user home
because home directories are not saved into images.

Start OpenWhisk standalone and configure `wsk`:

```sh
cd /opt/COSMOS
benchmarks/scripts/start_openwhisk_standalone.sh
```

For upstream SeBS, the startup script raises OpenWhisk standalone beyond its
defaults so 768 MB and 2048 MB actions can run. The default script settings are
`OPENWHISK_ACTION_MEMORY_MAX=2048m`, `OPENWHISK_ACTION_MEMORY_STD=256m`, and
`OPENWHISK_INVOKER_USER_MEMORY=4096m`.

Deploy the synthetic and SeBS-mapped OpenWhisk actions used by compatibility
experiments:

```sh
cd /opt/COSMOS
benchmarks/scripts/deploy_openwhisk_benchmark_actions.sh
```

Synthetic action map:

```sh
--action-map cpu_burst=ow_cpu_burst \
--action-map pipeline=ow_pipeline \
--action-map memory_heavy=ow_memory_heavy \
--action-map io_mixed=ow_io_mixed \
--action-map network_heavy=ow_network_heavy
```

SeBS-mapped action map:

```sh
--action-map 010.sleep=sebs_sleep \
--action-map 020.network-benchmark=sebs_network_benchmark \
--action-map 030.clock-synchronization=sebs_clock_synchronization \
--action-map 040.server-reply=sebs_server_reply \
--action-map 110.dynamic-html=sebs_dynamic_html \
--action-map 120.uploader=sebs_uploader \
--action-map 130.crud-api=sebs_crud_api \
--action-map 210.thumbnailer=sebs_thumbnailer \
--action-map 220.video-processing=sebs_video_processing \
--action-map 311.compression=sebs_compression \
--action-map 411.image-recognition=sebs_image_recognition \
--action-map 501.graph-pagerank=sebs_graph_pagerank \
--action-map 502.graph-mst=sebs_graph_mst \
--action-map 503.graph-bfs=sebs_graph_bfs \
--action-map 504.dna-visualisation=sebs_dna_visualisation
```

The OpenWhisk actions above are COSMOS compatibility actions for external
replay experiments. They are intentionally separate from the full upstream SeBS
benchmarks below.

For full upstream SeBS behavior on OpenWhisk, prepare the SeBS virtualenv and
self-hosted storage services separately:

```sh
cd /opt/COSMOS
benchmarks/scripts/prepare_upstream_sebs_openwhisk.sh
```

That script starts MinIO object storage on port `9011`, ScyllaDB/Alternator
NoSQL storage on port `9012`, a local Docker registry on port `5000`, rewrites
storage endpoints to the host-reachable CloudLab IP, and writes
`/opt/sebs/openwhisk.json` for SeBS. Use that config for storage-backed upstream
SeBS invocations, for example:

```sh
/opt/sebs-venv/bin/sebs benchmark invoke 120.uploader test \
  --config /opt/sebs/openwhisk.json \
  --deployment openwhisk \
  --architecture x64 \
  --system-variant container \
  --language python \
  --language-version 3.11 \
  --update-storage \
  --cache /opt/sebs/cache \
  --repetitions 1
```

On `amd006`, all upstream SeBS workload types were validated end-to-end through
OpenWhisk with Python 3.11 container actions, MinIO object storage, ScyllaDB
NoSQL storage, and the local Docker registry. Direct `benchmark invoke` coverage
passed for:

```text
010.sleep
110.dynamic-html
120.uploader
130.crud-api
210.thumbnailer
220.video-processing
311.compression
411.image-recognition
501.graph-pagerank
502.graph-mst
503.graph-bfs
504.dna-visualisation
```

The upstream `000.microbenchmarks` are internal SeBS experiment components, so
their E2E validation path is the SeBS experiment driver rather than direct
`benchmark invoke`:

```text
020.network-benchmark      via network-ping-pong
030.clock-synchronization  via invocation-overhead
040.server-reply           via eviction-model
```

Validation logs for the image setup are under
`/tmp/sebs-full-e2e-20260530`. The authoritative pass logs are the 12 direct
`*.console.log` files plus `experiment_network_ping_pong_v3.console.log`,
`experiment_invocation_overhead_v4.console.log`, and
`experiment_eviction_model_v2.console.log`.

### Image Validation Checklist

The `amd006` image was validated with these checks:

```sh
cd /opt/COSMOS
python3 -m unittest benchmarks.tests.test_phase6_harness -q
cargo build --release --workspace

sudo benchmarks/scripts/prepare_cosmos_cgroup_root.sh
benchmarks/scripts/start_openwhisk_standalone.sh
benchmarks/scripts/deploy_openwhisk_benchmark_actions.sh
```

Additional validation performed on `amd006`:

- all seven local workload kernels under `cfs-default`
- all seven local workload kernels under `cosmos-full`
- bounded local replay under CFS and COSMOS
- Azure CPU top-functions local replay under CFS and COSMOS
- upstream SeBS OpenWhisk E2E validation for all 15 workload types, including
  MinIO-backed, ScyllaDB-backed, model/data-backed, network, and high-memory
  workloads
- `bpftool`, `rg`, `pidstat`, `unrar`, protobuf tools, Rust, Docker, and SeBS
  Python dependencies available
- GRUB saved entry pinned to `Advanced options for Ubuntu>Ubuntu, with Linux
  7.0.9-070009-generic`

CloudLab note: on `amd006`, `systemctl start docker` is not the reliable startup
path after image provisioning because the Docker socket/service can conflict
with a manually started daemon and stale PID file. Use
`benchmarks/scripts/start_openwhisk_standalone.sh`; it starts Docker directly
when needed, injects a static Docker CLI into the OpenWhisk container, waits for
the API, and configures `wsk`.

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
│   ├── azure_trace/      # Azure CPU top-functions configs and artifacts
│   ├── local_harness/    # CFS/COSMOS/SFS local benchmark harness
│   ├── scripts/          # testbed cgroup/OpenWhisk setup scripts
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

CloudLab testbed setup is now captured by the benchmark scripts and checklist
above. `test_ssh.ignore.md` may contain operator-specific notes, but the
standard image path should use the repo-contained scripts under
`benchmarks/scripts/`.
