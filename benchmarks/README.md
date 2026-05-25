# COSMOS Benchmarks

This directory now has two clearly separated benchmark paths:

- `scripts/` + `workloads/`: the lightweight Phase 6 harness for the
  sched_ext first product. Use this for direct CFS vs COSMOS comparisons.
- `profiler/` + `third_party/`: the older deep-trace OpenWhisk/SeBS profiling
  stack. Keep this for full lifecycle analysis, not for the day-to-day
  first-product benchmark loop.

## Contents

- `scripts/`: Phase 6 comparison runners, latency summarizer, and result
  comparison tooling.
- `workloads/`: synthetic local workloads plus the metadata wrapper used by the
  Phase 6 harness.
- `plan.md`: benchmark architecture, required stats, matrices, and first-week
  implementation plan.
- `profiler/`: Rust standalone profiling harness and analysis CLI for the older
  OpenWhisk-heavy benchmark path.
- `third_party/serverless-benchmarks`: SeBS, added as a Git submodule.
- `third_party/openwhisk`: Apache OpenWhisk, added as a Git submodule.

## Direction

For the first product, prefer the lightweight harness first:

```sh
cd benchmarks/scripts
./burst_benchmark.py --concurrency 100 --workload cpu_burst --config cfs-default
sudo ./burst_benchmark.py --concurrency 100 --workload cpu_burst --config cosmos-full
python3 compare.py results/cfs-default/ results/cosmos-full/
```

Those scripts map directly to the Phase 6 baseline table:

- `cfs-default`
- `cosmos-heuristic`
- `cosmos-metadata`
- `cosmos-pooled`
- `cosmos-full`

The lightweight local workload set now includes these first-product synthetic
profiles:

- `cpu_burst`: calibrated dense matrix CPU burst, inspired by SeBS `010.sleep` as a low-overhead latency test
- `sleep_short`: minimal baseline calibration, also inspired by `010.sleep`
- `io_mixed`: small CPU + disk mix, loosely aligned with SeBS `311.compression` / `220.video-processing`
- `memory_heavy`: allocation and scan pressure, inspired by `411.image-recognition` / `220.video-processing`
- `network_heavy`: loopback TCP transfer, inspired by `120.uploader`
- `compression_mixed`: repeated compress/decompress cycles, inspired by `311.compression`
- `graph_bfs`: irregular graph traversal, inspired by `503.graph-bfs` / `501.graph-pagerank`

The lightweight workloads now run through a compiled Rust runner under
`target/release/cosmos-benchmark-workload`, so the harness no longer pays
Python interpreter startup cost on every invocation. By default each workload
runs 250 ms of synthetic work and uses a 500 ms deadline / `--slo-target-us`.
If you override `--duration-ms` without `--deadline-us`, the harness derives a
matching default deadline with the same headroom policy. Pass `--deadline-us`
only when you want an explicit tighter or looser SLO experiment.

These Rust workloads also avoid the old "poll the clock inside the hot loop"
pattern. `cpu_burst` now calibrates fixed matrix-multiply work to the target
CPU budget, and the other shapes similarly calibrate fixed work units instead
of repeatedly checking whether a timer has expired mid-iteration.

## Fair-case judgment

COSMOS should be judged against CFS on cases where the host has enough CPU
capacity to make the SLO feasible. The comparison tooling now records measured
CPU demand from `/usr/bin/time` when available and applies this hard-capacity
rule:

```text
total_compute_ms < deadline_ms * cpu_cores
```

If `total_compute_ms >= deadline_ms * cpu_cores`, the run is marked
`unfair-overloaded`: no scheduler can simultaneously optimize p99 and SLO hit
rate once the workload demands at least the entire SLO window on every CPU. In
that case `compare.py` still prints metrics, but its verdict says not to use the
run as the p99/SLO judgment.

Fair runs are labeled as:

- `underloaded`: load ratio `< 0.80`
- `full`: load ratio `0.80-0.95`
- `slightly-overloaded`: load ratio `0.95-1.00`
- `unfair-overloaded`: load ratio `>= 1.00`

Use `cfs-default` as the baseline and `cosmos-full` as the candidate when asking
whether COSMOS improves or preserves both p99 and SLO hit rate in the fair case.

The older benchmark stack still uses OpenWhisk standalone as the primary FaaS
target because SeBS supports OpenWhisk directly and OpenWhisk activation
records expose useful lifecycle metadata such as duration, status, `waitTime`,
and `initTime`.

The COSMOS profiler should still support a standalone mode that runs workload
code or containers inside controlled cgroups. That mode is the local debug path
for cgroup, perf, network, and phase-classification work.

## Submodules

Clone with submodules:

```sh
git clone --recurse-submodules <repo-url>
```

Initialize after cloning without submodules:

```sh
git submodule update --init --recursive
```

Update submodules to their recorded commits:

```sh
git submodule update --recursive
```

## Implementation

For the prepared CloudLab development machine, see
[`REMOTE_DEV_MACHINE.md`](REMOTE_DEV_MACHINE.md). That runbook covers the
`/local` layout, wrappers, OpenWhisk/SeBS service commands, scheduler injection,
resource policies, and benchmark examples.

The current prepared CloudLab benchmark host is
`Hanning@amd027.utah.cloudlab.us`. Persistent runtime state is kept under
`/local`, `/usr/local`, and `/opt`; do not rely on `/home` for benchmark
dependencies or storage. The May 7, 2026 runtime baseline has:

- OpenWhisk standalone host-network mode with local SeBS image pull bypass.
- OpenWhisk action memory raised to 2048 MiB and container-pool user memory set
  to 32768 MiB.
- Persistent MinIO object storage and ScyllaDB/Alternator NoSQL storage.
- `cosmos-sebs-microservers.service` enabled for the SeBS UDP/TCP
  microbenchmarks.

The full SeBS OpenWhisk matrix has been verified at both `test` and `small`
input sizes on that host:

```text
/local/benchmarks/runs/full-sebs-verify-20260507-020118
/local/benchmarks/runs/full-sebs-small-20260507-025115
```

`220.video-processing small` requires the benchmark action memory limit to be
2048 MiB on this 32c/64t host; the default 512 MiB limit lets ffmpeg start but
can fail before producing a valid SeBS result.

Build the profiler:

```sh
cargo build -p cosmos-bench-profiler
```

Run the standalone benchmark suite script:

```sh
benchmarks/profiler/scripts/run_standalone_suite.sh
```

Run the verified local-standalone SeBS adapter set:

```sh
benchmarks/profiler/scripts/run_sebs_standalone_suite.sh
```

Run the OpenWhisk benchmark suite script:

```sh
benchmarks/profiler/scripts/run_openwhisk_suite.sh
```

Run a concurrent OpenWhisk burst stress benchmark:

```sh
REQUESTS=1000 CONCURRENCY=128 BURN_MS=25 \
  benchmarks/profiler/scripts/run_openwhisk_concurrent_load.sh
```

Run the capacity suite for larger machines:

```sh
DURATION_S=60 CPU_WORKERS=$(nproc) MEM_WORKERS=32 MEM_BYTES_PER_WORKER=2G \
  FIO_JOBS=16 NET_PARALLEL=32 OW_REQUESTS=512 OW_CONCURRENCY=128 \
  benchmarks/profiler/scripts/run_capacity_suite.sh
```

Check host prerequisites:

```sh
cargo run -p cosmos-bench-profiler -- preflight --strict
```

OpenWhisk standalone currently needs a Java 17 runtime with this vendored
Gradle version. On hosts whose default Java is newer, set `JAVA_HOME` and
`PATH` explicitly:

```sh
export JAVA_HOME=/opt/jdk-17
export PATH=$JAVA_HOME/bin:$PATH
```

Build OpenWhisk standalone:

```sh
cd benchmarks/third_party/openwhisk
JAVA_HOME=/opt/jdk-17 PATH=/opt/jdk-17/bin:$PATH ./gradlew :core:standalone:build -x test
```

If Docker exposes its default `bridge` network, launch standalone with explicit
host properties:

```sh
HOST_IP=$(ip -4 route get 8.8.8.8 | awk '/src/{for(i=1;i<=NF;i++) if($i=="src") print $(i+1)}' | head -1)
java \
  -Dwhisk.standalone.host.name=$HOST_IP \
  -Dwhisk.standalone.host.ip=$HOST_IP \
  -Dwhisk.standalone.host.internal=$HOST_IP \
  -jar bin/openwhisk-standalone.jar --no-ui --dev-mode
```

Some kernels or Docker daemons do not provide bridge networking. This repository
also carries a host-network OpenWhisk override for that case:

```sh
REPO_ROOT=$(git -C ../.. rev-parse --show-toplevel)
JAVA_HOME=/opt/jdk-17 PATH=/opt/jdk-17/bin:$PATH java \
  -Dwhisk.standalone.host.name=$HOST_IP \
  -Dwhisk.standalone.host.ip=$HOST_IP \
  -Dwhisk.standalone.host.internal=$HOST_IP \
  -jar bin/openwhisk-standalone.jar \
  -c "$REPO_ROOT/benchmarks/profiler/configs/openwhisk-host-network.conf" \
  -m "$REPO_ROOT/benchmarks/profiler/configs/runtimes-no-prewarm.json" \
  --no-ui --dev-mode
```

The host-network override maps action containers to `127.0.0.1` inside the
vendored OpenWhisk Docker client and disables prewarm containers to avoid port
conflicts. Because action runtimes listen on port 8080 in host networking, keep
host port 8080 free while invoking actions. Only one action runtime can be
resident at a time in this mode; restart standalone or remove the paused action
container before switching to a different SeBS action.

Install SeBS' local dependencies:

```sh
cd benchmarks/third_party/serverless-benchmarks
./install.py --openwhisk --local
. python-venv/bin/activate
```

On a normal Docker bridge host, use SeBS' storage command to start object
storage and write the storage configuration JSON. On a host-network-only
machine, run MinIO on host networking and point the storage configuration at
`127.0.0.1:<port>` instead. Build and invoke SeBS OpenWhisk actions with the
repository OpenWhisk config:

```sh
SEBS_DOCKER_BUILD_NETWORK=host SEBS_SKIP_IMAGE_PUSH=1 \
python -m sebs.cli benchmark invoke 110.dynamic-html test \
  --config "$REPO_ROOT/benchmarks/profiler/configs/sebs-openwhisk.json" \
  --storage-configuration /tmp/sebs-storage.json \
  --deployment openwhisk \
  --language nodejs \
  --language-version 20 \
  --architecture x64 \
  --system-variant container \
  --repetitions 1 \
  --trigger library \
  --output-dir /tmp/sebs-dynamic \
  --cache /tmp/sebs-cache \
  --update-code
```

Run a standalone benchmark workload with cgroup, perf, cgroup resource, network,
qdisc, COSMOS scheduler, latency, lifecycle, and phase-summary collection:

```sh
sudo target/debug/cosmos-bench-profiler standalone \
  --out-dir benchmarks/runs \
  --name benchmark-cpu \
  --workload cpu \
  --duration-ms 1000 \
  --sample-ms 50
```

Verify an existing run directory:

```sh
cargo run -p cosmos-bench-profiler -- verify-run --run-dir benchmarks/runs/<run-id>
```

Run an OpenWhisk action through the same run-directory format:

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

For SeBS-generated actions, use `--skip-update --invoke-http` with
`--param-file` to pass the nested benchmark input JSON that SeBS expects. Do
not repeat storage credentials in the invocation payload for actions where SeBS
already bound them as action parameters; OpenWhisk treats those final parameters
as reserved and rejects the request.

The profiler samples COSMOS scheduler stats from `/var/run/scx/root/stats` when
the scheduler is running with stats enabled. Set `COSMOS_STATS_SOCKET` to point
at a different `scx_stats` socket. Runs remain valid when the scheduler is not
running; `scheduler_stats.csv` records unavailable samples in that case.

The profiler keeps source and scope explicit in raw outputs. Cgroup CPU,
memory, IO, pressure, perf, scheduler stats, OpenWhisk activation timing, and
HTTP client timing come from their source interfaces. OpenWhisk action
containers are sampled through `/proc/<host-pid>/net/dev` when possible, so
container rows in `net.csv` are model-safe per-action network counters. If the
container veth can be resolved, `qdisc.csv` is filtered to that host veth.
Host-global network and qdisc samples remain as host-scoped diagnostics and
are not treated as isolated per-run network consumption for modeling.

Print a plan matrix:

```sh
cargo run -p cosmos-bench-profiler -- matrix --kind sanity
cargo run -p cosmos-bench-profiler -- matrix --kind sebs-openwhisk
cargo run -p cosmos-bench-profiler -- matrix --kind sebs-openwhisk-cold-warm
cargo run -p cosmos-bench-profiler -- matrix --kind sebs-standalone
```

The SeBS-specific matrices are generated from
`profiler/configs/sebs-capabilities.json`. Use the OpenWhisk matrix as the
canonical FaaS benchmark set and the standalone matrix as the collector-debug
set; workloads without a verified local adapter stay excluded with blockers
listed in the manifest.

For cold/warm OpenWhisk behavior, run:

```sh
WARM_REPETITIONS=5 OUT_DIR=/usr/local/cosmos/benchmarks/runs \
  benchmarks/profiler/scripts/run_sebs_openwhisk_cold_warm_matrix.sh
```

This mode records one forced-cold invocation and five warm container reuses per
SeBS workload/input cell, then parses OpenWhisk lifecycle markers into
`openwhisk_lifecycle.tsv` and `openwhisk_lifecycle_summary.json`.

Build the aggregate profile DB from complete runs:

```sh
cargo run -p cosmos-bench-profiler -- profile-db \
  --runs-dir benchmarks/runs \
  --out benchmarks/profile_db.json \
  --strict
```

See `plan.md` for the full benchmark plan and `profiler/README.md` for CLI
details.
