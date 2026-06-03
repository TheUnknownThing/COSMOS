# COSMOS Benchmarks

The COSMOS benchmarking infrastructure consists of two complementary paths:
a **lightweight Rust path** for fast CFS-vs-COSMOS SLO comparison, and a
**profiler path** for deep resource tracing, SeBS benchmark integration,
and profile database generation. Both paths converge on the same comparison
pipeline via `compare.py`.

---

## Quick start

### Lightweight path (primary CFS-vs-COSMOS)

Build once:

```sh
cargo build --release -p cosmos-benchmark-workload -p cosmos -p cosmos-event-bridge
```

CFS baseline:

```sh
python3 benchmarks/scripts/burst_benchmark.py --config cfs-default --workload cpu_burst --concurrency 100
```

COSMOS comparison:

```sh
sudo python3 benchmarks/scripts/burst_benchmark.py --config cosmos-full --workload cpu_burst --concurrency 100
```

Compare:

```sh
python3 benchmarks/scripts/compare.py benchmarks/scripts/results/cfs-default/ benchmarks/scripts/results/cosmos-full/
```

### Profiler path (deep-trace + SeBS)

Build:

```sh
cargo build --release -p cosmos-bench-profiler
```

Quick smoke test (built-in micro workload):

```sh
sudo cosmos-bench-profiler standalone --workload cpu --mode burst --concurrency 10 --repetitions 30
```

SeBS workload (Python sleep):

```sh
echo '{"sleep":1}' > /tmp/sleep.json
sudo cosmos-bench-profiler standalone \
  --workload command --workload-label sebs-010.sleep-python \
  --mode burst --concurrency 4 --repetitions 10 \
  --sample-ms 50 --out-dir benchmarks/runs \
  -- python3 benchmarks/profiler/scripts/sebs_local_python_runner.py \
    benchmarks/third_party/serverless-benchmarks/benchmarks/000.microbenchmarks/010.sleep/python/function.py \
    /tmp/sleep.json
```

---

## Architecture overview

```
                          ┌──────────────────────────┐
                          │    burst_benchmark.py     │  single dispatcher
                          └─────────┬────────────────┘
                                    │
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
     ┌────────▼────────┐  ┌────────▼────────┐  ┌─────────▼─────────┐
     │  run_baseline.py │  │  run_cosmos.py   │  │ analyze_trace.py  │
     │  (CFS, no root)  │  │  (scheduler+sck) │  │ (profiler→summary)│
     └────────┬────────┘  └────────┬────────┘  └─────────┬─────────┘
              │                     │                     │
              │           ┌─────────▼─────────┐           │
              │           │    harness.py      │           │
              │           │  concurrent invoc  │           │
              │           │  stats + manifest  │           │
              │           └─────────┬─────────┘           │
              │                     │                     │
              │           ┌─────────▼─────────┐           │
              │           │  cosmos-benchmark- │           │
              │           │  workload (Rust)   │           │
              │           │  7 synthetic wls   │           │
              │           └───────────────────┘           │
              │                                           │
     ┌────────▼──────────────────────────────────┐       │
     │         cosmos-bench-profiler (Rust)        │◄──────┘
     │  burst / continuous / throughput modes      │
     │  built-in micro + SeBS via command runner   │
     │  perf + cgroup + qdisc + lifecycle traces   │
     └────────────────────┬───────────────────────┘
                          │
              ┌───────────┴───────────┐
              │                       │
     ┌────────▼────────┐    ┌────────▼────────┐
     │  summary.json    │    │ profile_db.json  │
     │  (per-run)       │    │  (cross-run)     │
     └────────┬────────┘    └─────────────────┘
              │
     ┌────────▼────────┐
     │   compare.py     │  p50/p95/p99/SLO/load/fairness
     └─────────────────┘
```

**Two paths, one comparison**:

- **Lightweight path**: CFS baseline + COSMOS runs via `burst_benchmark.py` →
  `summary.json` → `compare.py`. Fast, no external dependencies.
- **Profiler path**: `cosmos-bench-profiler` → `client_latency.csv` + trace
  files → `analyze_trace.py` → `compat_summary.json` → `compare.py`. Provides
  phase classification, resource profiling, and SeBS integration.

---

## Path 1: Lightweight CFS-vs-COSMOS comparison

### Entry point

`burst_benchmark.py` is the single dispatcher. It routes by config:

| config | runner | scheduler | metadata | short preemption | needs root |
|---|---|---|---|---|---|
| `cfs-default` | `run_baseline.py` | CFS | no | no | no |
| `cosmos-heuristic` | `run_cosmos.py` | COSMOS | no | no | yes |
| `cosmos-metadata` | `run_cosmos.py` | COSMOS | yes | no | yes |
| `cosmos-full` | `run_cosmos.py` | COSMOS | yes | yes | yes |
| `sfs` | `run_cosmos.py` | SFS-inspired | yes | no | yes |

For COSMOS configs, `run_cosmos.py`:
1. Starts `cosmos` scheduler with config-derived flags
2. Waits for `scx_stats` Unix socket at `/var/run/scx/root/stats`
3. Starts `cosmos-event-bridge` for metadata injection (metadata configs only)
4. Begins scheduler stats capture at 100ms intervals
5. Launches concurrent invocations via `ThreadPoolExecutor`
6. Each invocation runs the Rust workload binary wrapped with `/usr/bin/time`
7. Writes `summary.json`, creates `latest` symlink

Options:

```
--config           required, one of: cfs-default | cosmos-heuristic |
                   cosmos-metadata | cosmos-full | sfs
--workload         required, one of: cpu_burst | sleep_short | io_mixed |
                   memory_heavy | network_heavy | compression_mixed | graph_bfs
--concurrency      parallel invocations (default 1)
--duration-ms      per-invocation work budget (default 250)
--deadline-us      SLO deadline in microseconds
--out-dir          custom results output directory
--scheduler-bin    path to the COSMOS scheduler binary
--stats-socket     scheduler scx_stats socket path
--event-bridge-port metadata bridge TCP port (default 9731)
--scheduler-flag   extra flag passed through to the scheduler (repeatable)
```

### Synthetic workloads

Seven calibrated workloads compiled to a single Rust binary
(`target/release/cosmos-benchmark-workload`). The harness auto-builds if sources
are newer. Default duration: 250ms. Default deadline: 2x duration.

| workload | description | calibration | resource |
|---|---|---|---|
| `cpu_burst` | calibrated matrix-multiply CPU burst | CPU time | CPU |
| `sleep_short` | thread sleep baseline | wall time | idle |
| `io_mixed` | file write/read/sync/checksum | wall time | CPU+IO |
| `memory_heavy` | large-buffer strided scan | wall time | memory BW |
| `network_heavy` | loopback TCP echo | wall time | network |
| `compression_mixed` | RLE compress/decompress | wall time | CPU |
| `graph_bfs` | irregular graph BFS traversal | wall time | memory |

Workloads use fixed-cost calibration: run one iteration, measure elapsed time,
scale iterations to target duration. No clock-polling hot loops.

### Run directory layout

```
results/<config>/<timestamp>/
  manifest.json              — config, workload, concurrency, flags, cpu count
  client_latency.csv         — per-invocation wall-clock durations
  invocations/               — per-invocation {id}.json + .stderr
  scheduler_stats.jsonl      — scx_stats samples at 100ms (COSMOS only)
  summary.json               — aggregated latency, load, compute, scheduler
  scheduler.log              — scheduler stdout
  event_bridge.log           — metadata bridge log (metadata configs only)
latest -> <timestamp>        — symlink to most recent run
```

### Comparison

```sh
python3 benchmarks/scripts/compare.py <baseline-dir> <candidate-dir>
```

Produces:

```
baseline : cfs-default (/path/to/run)
candidate: cosmos-full (/path/to/run)
workload : cpu_burst @ concurrency=64
baseline : underloaded fair=true ratio=0.559 demand_ms=17880 capacity_ms=32000 cpus=64
candidate: underloaded fair=true ratio=0.511 demand_ms=16350 capacity_ms=32000 cpus=64
verdict  : fair case; candidate improves both p99 and SLO hit rate.

metric                         baseline    candidate        delta     delta%
----------------------------------------------------------------------------
p50_ms                          275.482      265.396      -10.086      -3.7%
p95_ms                          383.116      268.559     -114.557     -29.9%
p99_ms                          385.894      281.829     -104.065     -27.0%
mean_ms                         292.708      266.062      -26.647      -9.1%
client_slo_violations             0.000        0.000        0.000       0.0%
client_slo_violation_rate         0.000        0.000        0.000       0.0%
scheduler_slo_violations          0.000        0.000        0.000       0.0%
```

### Fair-case judgment

COSMOS should be judged when host capacity is sufficient to meet the SLO.
Fair load classes:

| class | load ratio | meaning |
|---|---|---|
| `underloaded` | < 0.80 | plenty of slack |
| `full` | 0.80–0.95 | near capacity |
| `slightly-overloaded` | 0.95–1.00 | marginal |
| `unfair-overloaded` | ≥ 1.00 | demand exceeds capacity |

Load ratio = `total_compute_ms / (deadline_ms * cpu_cores)`. Compute is
measured from `/usr/bin/time` when available, falling back to
`duration_ms * concurrency`.

---

## Path 2: Profiler — deep-trace + SeBS

`cosmos-bench-profiler` records per-invocation resource traces for offline
phase classification and profile database generation.

### CLI

```
cosmos-bench-profiler <COMMAND>

Commands:
  preflight          Check host prerequisites
  standalone         Run a workload with cgroup/perf/qdisc trace collection
  open-whisk         Invoke an OpenWhisk action and collect traces
  analyze            Recompute summary.json for an existing run
  verify-run         Verify a run directory has all required files
  matrix             Print the benchmark matrix
  profile-db         Aggregate runs into a scheduler-facing profile DB
  import-activation  Import an OpenWhisk activation JSON
```

### Standalone mode — invocation patterns

The `standalone` subcommand supports three modes:

#### Burst (default)

Fire `--concurrency` invocations in parallel, wait for all to complete, repeat
`--repetitions` times. Equivalent to the lightweight path's burst model.

```sh
sudo cosmos-bench-profiler standalone \
  --mode burst --concurrency 16 --repetitions 50 \
  --workload cpu --duration-ms 250 --sample-ms 50 --out-dir benchmarks/runs
```

#### Continuous

Open-loop arrival process. Spawns at `--rate` invocations/second for
`--duration-s` seconds, capped at `--concurrency` in-flight.

```sh
sudo cosmos-bench-profiler standalone \
  --mode continuous --rate 50 --duration-s 30 --concurrency 128 \
  --workload cpu --duration-ms 250 --sample-ms 50 --out-dir benchmarks/runs
```

#### Throughput

Fire invocations as fast as possible for `--duration-s` seconds, capped at
`--concurrency` in-flight. Measures max sustained throughput within SLO.

```sh
sudo cosmos-bench-profiler standalone \
  --mode throughput --duration-s 30 --concurrency 128 \
  --workload cpu --duration-ms 250 --sample-ms 50 --out-dir benchmarks/runs
```

### Common flags

| Flag | Default | Description |
|---|---|---|
| `--out-dir` | `benchmarks/runs` | Parent output directory |
| `--name` | `standalone` | Run name prefix |
| `--workload` | `cpu` | `cpu`, `memory`, `io`, `network`, or `command` |
| `--workload-label` | auto | Stable label in `run_meta.json` |
| `--mode` | `burst` | Invocation pattern |
| `--input` | `small` | Input size label |
| `--warmth` | `warm` | Warmth label |
| `--duration-ms` | `1000` | Per-invocation work budget |
| `--sample-ms` | `100` | Trace sampling interval |
| `--concurrency` | `1` | Parallel invocations (burst) / max concurrency (continuous/throughput) |
| `--repetitions` | `1` | Total samples in burst mode |
| `--rate` | `10.0` | Arrival rate in invocations/second (continuous) |
| `--duration-s` | `30` | Total run duration (continuous/throughput) |
| `--skip-perf` | false | Skip perf collection |
| `--allow-cgroup-fallback` | false | Run without dedicated cgroup |
| `--metadata-port` | `0` | COSMOS metadata TCP port (0=disabled) |
| `--slo-class` | `1` | 0=LatencyCritical, 1=Standard, 2=Batch |

### SeBS benchmarks via `--workload command`

SeBS functions are invoked through language-specific runner scripts:

| Runner | Language | Usage |
|---|---|---|
| `sebs_local_python_runner.py` | Python | loads handler, calls `handler(event)` |
| `sebs_local_node_runner.js` | Node.js | requires, calls `exports.main(args)` |

**Standalone-compatible SeBS benchmarks** (no external services):

| Benchmark | Languages | Input | Description |
|---|---|---|---|
| `010.sleep` | Python, Node.js | `{"sleep": N}` | Configurable sleep |
| `110.dynamic-html` | Python, Node.js | `{"username": "...", "random_len": N}` | CPU-bound HTML gen |
| `501.graph-pagerank` | Python | `{"size": N}` | Graph pagerank (needs `igraph`) |
| `502.graph-mst` | Python | `{"size": N}` | Graph MST (needs `igraph`) |
| `503.graph-bfs` | Python | `{"size": N}` | Graph BFS (needs `igraph`) |

**OpenWhisk-only benchmarks** (need backend services):
`120.uploader`, `130.crud-api`, `210.thumbnailer`, `220.video-processing`,
`311.compression`, `411.image-recognition`, `504.dna-visualisation`.

Benchmarks requiring external services (S3, DB, ffmpeg) only work through
the full OpenWhisk deployment with SeBS storage backends.

### Running SeBS workloads

```sh
# Python burst
sudo cosmos-bench-profiler standalone \
  --workload command --workload-label sebs-010.sleep-python \
  --mode burst --concurrency 8 --repetitions 20 \
  --input test --sample-ms 50 --out-dir benchmarks/runs \
  -- python3 benchmarks/profiler/scripts/sebs_local_python_runner.py \
    benchmarks/third_party/serverless-benchmarks/benchmarks/000.microbenchmarks/010.sleep/python/function.py \
    /tmp/sleep-input.json

# Node.js continuous
echo '{"username":"test","random_len":1000}' > /tmp/html-input.json
sudo cosmos-bench-profiler standalone \
  --workload command --workload-label sebs-110.dynamic-html-nodejs \
  --mode continuous --rate 50 --duration-s 15 --concurrency 64 \
  --input small --sample-ms 50 --out-dir benchmarks/runs \
  -- node benchmarks/profiler/scripts/sebs_local_node_runner.js \
    benchmarks/third_party/serverless-benchmarks/benchmarks/100.webapps/110.dynamic-html/nodejs/function.js \
    /tmp/html-input.json
```

### Profiler run directory layout

```
benchmarks/runs/<run-id>/
  run_meta.json              — workload, input, warmth, env, config hash
  events.jsonl               — invocation_started/finished lifecycle events
  client_latency.csv         — per-invocation send_ns, response_end_ns, status
  cgroup_cpu.csv             — cgroup v2 cpu.stat samples
  cgroup_memory.csv          — cgroup memory.current/peak samples
  cgroup_io.csv              — cgroup io.stat samples
  cgroup_pressure.csv        — cgroup pressure samples
  perf_stat.csv              — perf stat counter samples
  net.csv                    — container/host network rx/tx
  qdisc.csv                  — tc qdisc stats
  scheduler_stats.csv        — COSMOS scheduler counter samples
  host_cpu.csv               — /proc/stat host CPU
  host_memory.csv            — /proc/meminfo
  host_pressure.csv          — /proc/pressure/*
  process_stats.csv          — per-process CPU/IO deltas
  stdout.log / stderr.log    — workload output
  summary.json               — aggregated latency, resources, phase windows
```

### Profile database

Aggregate multiple profiler runs into a scheduler-facing workload profile:

```sh
cargo run -p cosmos-bench-profiler -- profile-db \
  --runs-dir benchmarks/runs --out benchmarks/profile_db.json --strict
```

Each profile contains:
- Per-invocation latency percentiles (p50/p95/p99/mean)
- Cgroup resource metrics (CPU usage, memory peak, IO bytes, network)
- 50ms-resolution phase windows (CPU_BOUND, IO_PAGECACHE, CACHE_OR_MEM_BOUND, MIXED_UNKNOWN)
- Scheduler feature counters

### Trace analysis bridge

`analyze_trace.py` reads profiler trace data and produces a
`compat_summary.json` compatible with the lightweight path's `compare.py`:

```sh
# Analyze a single profiler run
python3 benchmarks/scripts/analyze_trace.py analyze \
  --run-dir benchmarks/runs/<run-id> --deadline-us 500000

# Compare two profiler runs
python3 benchmarks/scripts/analyze_trace.py compare \
  --baseline benchmarks/runs/<cfs-run> \
  --candidate benchmarks/runs/<cosmos-run> \
  --deadline-us 500000 \
  --baseline-config cfs-default --candidate-config cosmos-full
```

What it extracts:

| Trace file | Extracted metric |
|---|---|
| `client_latency.csv` | Per-invocation duration, p50/p95/p99/mean, SLO violations |
| `cgroup_cpu.csv` | Per-invocation CPU time delta |
| `run_meta.json` | Workload label, input, warmth, config |
| `scheduler_stats.csv` | Peak/accumulated scheduler counters |
| `events.jsonl` | Run duration, invocation count |

---

## CFS-vs-COSMOS with the profiler

The `cosmos-bench-profiler` does not require a running scheduler — it collects
traces regardless. To compare schedulers:

1. **CFS run** — run the profiler without COSMOS:
   ```sh
   sudo cosmos-bench-profiler standalone --workload command ... -- ...
   ```
2. **COSMOS run** — start the scheduler, then run the profiler:
   ```sh
   sudo cosmos --slo-target-us 500000 > /tmp/cosmos.log 2>&1 &
   sleep 3
   sudo cosmos-bench-profiler standalone --workload command ... -- ...
   sudo kill %1
   ```
3. **Compare** — extract latency from `client_latency.csv` or use
   `analyze_trace.py` for full SLO comparison.

The profiler's `--metadata-port` flag enables integration with the
`cosmos-event-bridge` for annotated invocation metadata.

---

## SeBS submodule & OpenWhisk

### Submodules

```sh
git clone --recurse-submodules <repo-url>
# or after clone:
git submodule update --init --recursive
```

Submodules:
- `benchmarks/third_party/serverless-benchmarks` — SeBS benchmark functions
- `benchmarks/third_party/openwhisk` — Apache OpenWhisk (for OpenWhisk mode)

### OpenWhisk mode (requires Docker + JDK 17)

Not tested in this session (Docker daemon inactive on amd002). For setup
instructions see `profiler/README.md` and `profiler/configs/`.

---

## Build system

### Local build

```sh
cargo build --release -p cosmos-benchmark-workload -p cosmos -p cosmos-event-bridge
```

### CloudLab build target

On CloudLab hosts, the cargo target directory is often configured at
`/usr/local/cosmos/build/cosmos-target`. Create a symlink:

```sh
ln -sf /usr/local/cosmos/build/cosmos-target /path/to/COSMOS/target
```

The harness scripts resolve binaries at `target/release/` relative to the
repo root.

### Auto-rebuild

The harness checks source file mtimes against binary mtimes and triggers
`cargo build --release` if sources are newer. To suppress, touch the binaries:

```sh
touch target/release/cosmos target/release/cosmos-benchmark-workload target/release/cosmos-event-bridge
```

---

## Results

Result reports are stored in `benchmarks/result/`:

| File | Date | Host | Content |
|---|---|---|---|
| `BENCHMARK_RESULT_0525.md` | 2026-05-25 | amd246 (48c) | CFS vs COSMOS-heuristic vs COSMOS-full |
| `BENCHMARK_RESULT_0527.md` | 2026-05-27 | amd002 (64c) | Full lightweight + SeBS profiler + profile DB |

Each report includes environment, test matrix, per-config latency tables,
SLO violation counts, load analysis, and key findings.

---

## Prerequisites

| Dependency | Path 1 (lightweight) | Path 2 (profiler) |
|---|---|---|
| Rust toolchain | required | required |
| Python 3.12+ | required | required |
| cgroup v2 | — | required |
| perf | — | required |
| tc (qdisc) | — | required |
| Docker | — | optional (OpenWhisk) |
| wsk CLI | — | optional (OpenWhisk) |
| JDK 17 | — | optional (OpenWhisk) |
| Node.js | — | optional (Node.js SeBS) |
| npm packages | — | `mustache` (dynamic-html Node.js) |
| pip packages | — | `igraph` (graph benchmarks) |

Run `cosmos-bench-profiler preflight` to check all profiler prerequisites.
