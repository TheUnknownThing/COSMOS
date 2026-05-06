# COSMOS Benchmarks

This directory contains the benchmark plan and external benchmark/runtime
dependencies for COSMOS.

## Contents

- `plan.md`: benchmark architecture, required stats, matrices, and first-week
  implementation plan.
- `profiler/`: Rust standalone profiling harness and analysis CLI.
- `third_party/serverless-benchmarks`: SeBS, added as a Git submodule.
- `third_party/openwhisk`: Apache OpenWhisk, added as a Git submodule.

## Direction

The benchmark stack uses OpenWhisk standalone as the primary FaaS target because
SeBS supports OpenWhisk directly and OpenWhisk activation records expose useful
lifecycle metadata such as duration, status, `waitTime`, and `initTime`.

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

Build the profiler:

```sh
cargo build -p cosmos-bench-profiler
```

Run the standalone benchmark suite script:

```sh
benchmarks/profiler/scripts/run_standalone_suite.sh
```

Run the OpenWhisk benchmark suite script:

```sh
benchmarks/profiler/scripts/run_openwhisk_suite.sh
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
HTTP client timing come from their source interfaces. Host-global network and
qdisc samples are written as host-scoped observations and are not treated as
isolated per-run network consumption for modeling.

Print a plan matrix:

```sh
cargo run -p cosmos-bench-profiler -- matrix --kind sanity
```

Build the aggregate profile DB from complete runs:

```sh
cargo run -p cosmos-bench-profiler -- profile-db \
  --runs-dir benchmarks/runs \
  --out benchmarks/profile_db.json \
  --strict
```

See `plan.md` for the full benchmark plan and `profiler/README.md` for CLI
details.
