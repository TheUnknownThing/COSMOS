# COSMOS Benchmark Profiler (secondary path)

`cosmos-bench-profiler` records deep resource traces: perf, cgroup, eBPF,
qdisc, and lifecycle data. It supports standalone cgroup mode and OpenWhisk
activation mode with SeBS workloads.

This is the offline analysis path. For the primary day-to-day CFS-vs-COSMOS
SLO comparison loop, use the lightweight Rust path (`scripts/` + `workloads/`)
documented in the parent `README.md`.

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

## Standalone runs

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

## OpenWhisk runs

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

## SeBS OpenWhisk matrix

```sh
# Print matrix
cargo run -p cosmos-bench-profiler -- matrix --kind sebs-openwhisk
cargo run -p cosmos-bench-profiler -- matrix --kind sebs-openwhisk-cold-warm

# Cold/warm matrix
WARM_REPETITIONS=5 OUT_DIR=/usr/local/cosmos/benchmarks/runs \
  benchmarks/profiler/scripts/run_sebs_openwhisk_cold_warm_matrix.sh
```

## Verification and profile DB

```sh
cargo run -p cosmos-bench-profiler -- verify-run --run-dir benchmarks/runs/<run-id>

cargo run -p cosmos-bench-profiler -- profile-db \
  --runs-dir benchmarks/runs \
  --out benchmarks/profile_db.json \
  --strict
```

## Stats socket

The profiler samples COSMOS scheduler stats from `/var/run/scx/root/stats`.
Set `COSMOS_STATS_SOCKET` to override. Runs remain valid when the scheduler
is not running.
