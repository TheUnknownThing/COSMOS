# COSMOS Benchmark Profiler

`cosmos-bench-profiler` is the first implementation slice of
`benchmarks/plan.md`. It provides a standalone cgroup profiler, preflight
checks, benchmark matrix output, OpenWhisk activation import, analysis, and run
verification.

## Build

```sh
cargo build -p cosmos-bench-profiler
```

## Preflight

```sh
cargo run -p cosmos-bench-profiler -- preflight --strict
```

The strict mode requires cgroup v2, `perf`, Docker, an OpenWhisk action network
path, `tc`, initialized OpenWhisk and SeBS submodules, `wsk`, JDK 17, and a
writable benchmark directory. The OpenWhisk network path can be Docker's
default `bridge` network or this repository's host-network override.

On Debian 13, the repository default Java may be newer than OpenWhisk's Gradle
wrapper supports. A JDK 17 installation such as `/opt/jdk-17` should be exported
before building or launching OpenWhisk:

```sh
export JAVA_HOME=/opt/jdk-17
export PATH=$JAVA_HOME/bin:$PATH
```

OpenWhisk action containers use Docker bridge networking by default. If
`docker network ls` only shows `host` and `none`, use
`configs/openwhisk-host-network.conf` with `configs/runtimes-no-prewarm.json`.
That path requires the host's port 8080 to be free while actions are invoked.

## Standalone Runs

The ready-to-run standalone benchmark suite script builds the profiler, runs
CPU, memory, IO, and network workloads for `REPETITIONS` iterations, verifies
each run, and rebuilds the profile DB:

```sh
benchmarks/profiler/scripts/run_standalone_suite.sh
```

Run built-in micro-workloads through a dedicated cgroup:

```sh
sudo target/debug/cosmos-bench-profiler standalone \
  --out-dir benchmarks/runs \
  --name benchmark-cpu \
  --workload cpu \
  --duration-ms 1000 \
  --sample-ms 50
```

Run an arbitrary command:

```sh
sudo target/debug/cosmos-bench-profiler standalone \
  --out-dir benchmarks/runs \
  --name custom \
  --workload command \
  --duration-ms 1000 \
  --sample-ms 100 \
  -- /path/to/workload --arg
```

Each successful run writes the file set required by the plan:
`run_meta.json`, `events.jsonl`, `stdout.log`, `stderr.log`,
`perf_stat.csv`, cgroup CSVs, network/qdisc CSVs, `client_latency.csv`,
`openwhisk_activation.json`, `scheduler_stats.csv`, and `summary.json`.
`scheduler_stats.csv` samples the COSMOS `scx_stats` socket at
`/var/run/scx/root/stats`; set `COSMOS_STATS_SOCKET` to override it. If the
scheduler is not running, the file records unavailable samples instead of
failing the benchmark run.

## OpenWhisk Runs

The ready-to-run OpenWhisk benchmark suite script starts vendored OpenWhisk
standalone with the host-network config, profiles a real action for
`REPETITIONS` iterations, rebuilds the profile DB, and cleans up OpenWhisk
containers before exiting:

```sh
benchmarks/profiler/scripts/run_openwhisk_suite.sh
```

With OpenWhisk standalone already running, invoke an action through `wsk` and
collect activation, Docker PID/cgroup, cgroup resource, network, qdisc, perf,
client latency, and lifecycle traces:

```sh
sudo target/debug/cosmos-bench-profiler open-whisk \
  --out-dir benchmarks/runs \
  --name openwhisk-benchmark \
  --action cosmos_hello \
  --file /tmp/cosmos-hello.js \
  --kind nodejs:20 \
  --insecure \
  --apihost http://127.0.0.1:3233 \
  --auth "$OPENWHISK_AUTH" \
  --param name=benchmark
```

Use `--skip-update` to invoke an action that already exists. SeBS actions often
need nested JSON input; pass that with `--param-file input.json --invoke-http`
so the profiler posts directly to the OpenWhisk API instead of `wsk`'s stricter
parameter parser. On host-network OpenWhisk launches, keep host port 8080 free
while the action runs. If SeBS bound storage credentials as action parameters,
leave those credentials out of the invocation payload and pass only the
benchmark input fields.

HTTP OpenWhisk invocations record first-byte and total response timing from
`curl --write-out`. `wsk` and standalone process runs record response-end timing
only and leave `first_byte_ns` empty instead of fabricating a value.

## Verification

```sh
cargo run -p cosmos-bench-profiler -- verify-run --run-dir benchmarks/runs/<run-id>
```

Verification checks that the required files exist and that the lifecycle,
client-latency, host PID, and cgroup path joins are present.

## Matrices

The plan matrices are stored in `configs/` and can also be printed by the CLI:

```sh
cargo run -p cosmos-bench-profiler -- matrix --kind sanity
cargo run -p cosmos-bench-profiler -- matrix --kind profile
cargo run -p cosmos-bench-profiler -- matrix --kind interference
```

## Profile DB

Aggregate complete run directories into a scheduler-facing profile database:

```sh
cargo run -p cosmos-bench-profiler -- profile-db \
  --runs-dir benchmarks/runs \
  --out benchmarks/profile_db.json \
  --strict
```

The generated JSON groups runs by workload, input, and warmth, and records
median/p95/p99 latency, platform wait/init/run medians, dominant resource
phases, COSMOS scheduler counters when available, and stable resource counters
that can be used without live `perf` recording. Host-global network and qdisc
samples are retained in raw CSVs for diagnosis but are marked as host-scoped and
are not used as isolated per-run network consumption features.
