# COSMOS Benchmark Profiler

`cosmos-bench-profiler` records deep resource traces: perf, cgroup, eBPF,
qdisc, and lifecycle data. It supports standalone cgroup mode and OpenWhisk
activation mode with SeBS workloads. The trace data can be analyzed into
COSMOS-compatible SLO/latency summaries via `analyze_trace.py`.

## Architecture

```
cosmos-bench-profiler standalone --mode <mode> --workload command -- <runner> <function> <input>
        │
        ├─ creates cgroup v2 scope
        ├─ spawns workload(s) according to invocation mode
        ├─ samples cgroup/perf/scheduler/qdisc/process stats
        ├─ records lifecycle events (events.jsonl)
        └─ writes client_latency.csv, summary.json
                │
                ▼
analyze_trace.py analyze --run-dir <dir> --deadline-us <us>
        │
        └─ produces compat_summary.json
                │
                ▼
compare.py <cfs-dir> <cosmos-dir>   (full SLO comparison)
```

## Build

```sh
cargo build -p cosmos-bench-profiler
```

## Preflight

```sh
cargo run -p cosmos-bench-profiler -- preflight --strict
```

Requires cgroup v2, `perf`, Docker, `tc`, OpenWhisk/SeBS submodules, `wsk`,
JDK 17, and a writable benchmark directory.

On Debian 13, export JDK 17 before building OpenWhisk:

```sh
export JAVA_HOME=/opt/jdk-17
export PATH=$JAVA_HOME/bin:$PATH
```

## Invocation Modes

The `standalone` subcommand supports three invocation patterns via `--mode`:

### Burst (default)

Fire `--concurrency` invocations in parallel, wait for all to complete, repeat
until `--repetitions` total samples are collected. Equivalent to the
lightweight path's burst model.

```sh
sudo cosmos-bench-profiler standalone \
  --mode burst \
  --concurrency 10 \
  --repetitions 50 \
  --workload cpu \
  --duration-ms 250 \
  --sample-ms 50 \
  --out-dir benchmarks/runs
```

### Continuous

Open-loop arrival process. Spawns invocations at `--rate` invocations/second
for `--duration-s` seconds, capped at `--concurrency` in-flight.

```sh
sudo cosmos-bench-profiler standalone \
  --mode continuous \
  --rate 50 \
  --duration-s 30 \
  --concurrency 128 \
  --workload cpu \
  --duration-ms 250 \
  --sample-ms 50 \
  --out-dir benchmarks/runs
```

### Throughput

Fire invocations as fast as possible for `--duration-s` seconds, capped at
`--concurrency` in-flight. Measures max sustained throughput within SLO.

```sh
sudo cosmos-bench-profiler standalone \
  --mode throughput \
  --duration-s 30 \
  --concurrency 128 \
  --workload cpu \
  --duration-ms 250 \
  --sample-ms 50 \
  --out-dir benchmarks/runs
```

### Common flags

| Flag | Default | Description |
|------|---------|-------------|
| `--out-dir` | `benchmarks/runs` | Parent output directory |
| `--name` | `standalone` | Run name (used in run ID) |
| `--workload` | `cpu` | `cpu`, `memory`, `io`, `network`, or `command` |
| `--workload-label` | auto | Stable label in `run_meta.json` |
| `--input` | `small` | Input size label |
| `--warmth` | `warm` | Warmth label (`cold`, `warm`) |
| `--duration-ms` | `1000` | Per-invocation work budget |
| `--sample-ms` | `100` | Trace sampling interval |
| `--skip-perf` | false | Skip perf collection |
| `--allow-cgroup-fallback` | false | Run without dedicated cgroup |

## Running SeBS Workloads

SeBS benchmarks are invoked via `--workload command` with a language-specific
runner. Two benchmarks are available in standalone mode (no storage backends
required):

| Benchmark | Languages | Input sizes |
|-----------|-----------|-------------|
| `010.sleep` | nodejs, python, java, cpp | `test` (1s), `small` (100s), `large` (1000s) |
| `110.dynamic-html` | nodejs, python, java | `test` (10 rand), `small` (1000), `large` (100000) |

### Language runners

- **Node.js**: `benchmarks/profiler/scripts/sebs_local_node_runner.js`
- **Python**: `benchmarks/profiler/scripts/sebs_local_python_runner.py`

Each runner loads the SeBS function handler and calls it with JSON input:

```sh
# Node.js
node sebs_local_node_runner.js <function.js> <input.json>

# Python
python3 sebs_local_python_runner.py <function.py> <input.json>
```

### Examples

Burst of 4 concurrent Python sleep invocations:

```sh
echo '{"sleep":1}' > /tmp/sleep-input.json
sudo cosmos-bench-profiler standalone \
  --workload command \
  --workload-label sebs-010.sleep-python \
  --mode burst --concurrency 4 --repetitions 4 \
  --input test --sample-ms 50 \
  --out-dir benchmarks/runs \
  -- \
  python3 benchmarks/profiler/scripts/sebs_local_python_runner.py \
    benchmarks/third_party/serverless-benchmarks/benchmarks/000.microbenchmarks/010.sleep/python/function.py \
    /tmp/sleep-input.json
```

Node.js dynamic-html at 10 invocations/sec:

```sh
echo '{"username":"test","random_len":1000}' > /tmp/html-input.json
sudo cosmos-bench-profiler standalone \
  --workload command \
  --workload-label sebs-110.dynamic-html-nodejs \
  --mode continuous --rate 10 --duration-s 15 \
  --input small --sample-ms 50 \
  --out-dir benchmarks/runs \
  -- env NODE_PATH=./node_modules \
    node benchmarks/profiler/scripts/sebs_local_node_runner.js \
    benchmarks/third_party/serverless-benchmarks/benchmarks/100.webapps/110.dynamic-html/nodejs/function.js \
    /tmp/html-input.json
```

### Automated suite

`run_sebs_standalone_suite.sh` runs both benchmarks in all supported languages
with configurable mode:

```sh
# Burst mode, 5 repetitions each
REPETITIONS=5 MODE=burst CONCURRENCY=4 \
  benchmarks/profiler/scripts/run_sebs_standalone_suite.sh

# Continuous mode, 20 inv/sec for 30s
REPETITIONS=1 MODE=continuous RATE=20 CONCURRENCY=32 DURATION_S=30 \
  benchmarks/profiler/scripts/run_sebs_standalone_suite.sh

# Throughput mode, 30s
REPETITIONS=1 MODE=throughput CONCURRENCY=64 DURATION_S=30 \
  benchmarks/profiler/scripts/run_sebs_standalone_suite.sh
```

## Trace Analyzer

`benchmarks/scripts/analyze_trace.py` reads profiler trace data and produces
a COSMOS-compatible `summary.json` that feeds into `compare.py`.

**What it extracts:**

| Trace file | Extracted metric |
|-----------|-----------------|
| `client_latency.csv` | Per-invocation duration, p50/p95/p99/mean, SLO violations |
| `cgroup_cpu.csv` | Per-invocation CPU time delta |
| `run_meta.json` | Workload label, input, warmth, config |
| `scheduler_stats.csv` | Peak/accumulated scheduler counters |
| `events.jsonl` | Run duration, invocation count |

### Usage

```sh
# Analyze a single profiler run
python3 benchmarks/scripts/analyze_trace.py analyze \
  --run-dir benchmarks/runs/<run-id> \
  --deadline-us 500000 \
  --config cosmos-full \
  --workload cpu

# Compare two profiler runs (writes compat summaries + runs compare.py)
python3 benchmarks/scripts/analyze_trace.py compare \
  --baseline benchmarks/runs/<cfs-run> \
  --candidate benchmarks/runs/<cosmos-run> \
  --deadline-us 500000 \
  --baseline-config cfs-default \
  --candidate-config cosmos-full
```

The `compare` subcommand produces the same output as the lightweight path's
`compare.py`:

```
baseline : cfs-default (/path/to/run)
candidate: cosmos-full (/path/to/run)
workload : cpu @ concurrency=15
baseline : underloaded fair=true ratio=0.750 demand_ms=1500.486 capacity_ms=2000.000 cpus=4
candidate: unfair-overloaded fair=false ratio=4.535 demand_ms=9070.249 capacity_ms=2000.000 cpus=4
verdict  : unfair overloaded case; ...

metric                         baseline    candidate        delta     delta%
----------------------------------------------------------------------------
p50_ms                          112.412      113.750        1.338       1.2%
p95_ms                          117.836      124.758        6.922       5.9%
p99_ms                          118.331      127.813        9.482       8.0%
mean_ms                         112.517      114.885        2.368       2.1%
client_slo_violations             0.000        0.000        0.000       0.0%
```

## Standalone Built-in Workloads

```sh
# Built-in suite
benchmarks/profiler/scripts/run_standalone_suite.sh

# Single workload with cgroup collection
sudo cosmos-bench-profiler standalone \
  --out-dir benchmarks/runs \
  --name benchmark-cpu \
  --workload cpu \
  --duration-ms 1000 \
  --sample-ms 50

# Arbitrary command
sudo cosmos-bench-profiler standalone \
  --out-dir benchmarks/runs \
  --name custom \
  --workload command \
  --workload-label custom-workload \
  --duration-ms 1000 \
  --sample-ms 100 \
  -- /path/to/workload --arg
```

## OpenWhisk Runs

```sh
# Suite
benchmarks/profiler/scripts/run_openwhisk_suite.sh

# Single action
sudo cosmos-bench-profiler open-whisk \
  --out-dir benchmarks/runs \
  --name openwhisk-smoke \
  --action cosmos_hello \
  --file /tmp/cosmos-hello.js \
  --kind nodejs:20 \
  --insecure \
  --apihost "$OPENWHISK_APIHOST" \
  --auth "$OPENWHISK_AUTH" \
  --param name=benchmark

# Concurrent burst
REQUESTS=1000 CONCURRENCY=128 BURN_MS=25 \
  benchmarks/profiler/scripts/run_openwhisk_concurrent_load.sh

# Capacity suite (host stress)
DURATION_S=60 CPU_WORKERS=$(nproc) MEM_WORKERS=32 MEM_BYTES_PER_WORKER=2G \
  FIO_JOBS=16 NET_PARALLEL=32 OW_REQUESTS=512 OW_CONCURRENCY=128 \
  benchmarks/profiler/scripts/run_capacity_suite.sh
```

## SeBS OpenWhisk Matrix

```sh
# Print matrix
cargo run -p cosmos-bench-profiler -- matrix --kind sebs-openwhisk
cargo run -p cosmos-bench-profiler -- matrix --kind sebs-openwhisk-cold-warm

# Cold/warm matrix
WARM_REPETITIONS=5 OUT_DIR=/usr/local/cosmos/benchmarks/runs \
  benchmarks/profiler/scripts/run_sebs_openwhisk_cold_warm_matrix.sh
```

## Verification and Profile DB

```sh
cargo run -p cosmos-bench-profiler -- verify-run --run-dir benchmarks/runs/<run-id>

cargo run -p cosmos-bench-profiler -- profile-db \
  --runs-dir benchmarks/runs \
  --out benchmarks/profile_db.json \
  --strict
```

## Run Directory Layout

```
benchmarks/runs/<run-id>/
  run_meta.json              — workload, input, warmth, env, config hash
  events.jsonl               — invocation_started/finished lifecycle events
  client_latency.csv         — per-invocation send_ns, response_end_ns, status
  cgroup_cpu.csv             — cgroup v2 cpu.stat samples (usage_usec, throttled)
  cgroup_memory.csv          — cgroup memory.current/peak samples
  cgroup_io.csv              — cgroup io.stat samples
  cgroup_pressure.csv        — cgroup pressure samples
  perf_stat.csv              — perf stat counter samples
  net.csv                    — container/host network rx/tx
  qdisc.csv                  — tc qdisc stats
  scheduler_stats.csv        — COSMOS scheduler counter samples
  host_cpu.csv               — /proc/stat host CPU
  host_memory.csv            — /proc/meminfo
  process_stats.csv          — per-process CPU/IO deltas
  stdout.log / stderr.log    — workload output
  summary.json               — aggregated latency, resources, phase windows
  compat_summary.json        — (via analyze_trace.py) COSMOS-compatible summary
```

## Stats Socket

The profiler samples COSMOS scheduler stats from `/var/run/scx/root/stats`.
Set `COSMOS_STATS_SOCKET` to override. Runs remain valid when the scheduler
is not running.
