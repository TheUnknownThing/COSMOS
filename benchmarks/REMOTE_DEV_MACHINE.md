# Remote Benchmark Machine Runbook

This runbook describes the prepared COSMOS benchmark environment on:

```sh
ssh Hanning@amd027.utah.cloudlab.us
```

The machine is intended to be image-bakeable. Persistent software, build
artifacts, benchmark data, caches, and service state live under `/local`.
Avoid storing benchmark state under `/home` or `/users/Hanning`.

## Quick Start

```sh
ssh Hanning@amd027.utah.cloudlab.us
source /etc/profile.d/cosmos-benchmark.sh
cd /local/src/COSMOS
```

Check the environment:

```sh
cargo run -p cosmos-bench-profiler -- preflight --strict
uname -r
findmnt /local
docker info --format 'DockerRoot={{.DockerRootDir}} Cgroup={{.CgroupDriver}}'
```

Expected basics:

- kernel: `7.0.0-070000-generic`
- `/local`: ext4 on `/dev/sdb`
- Docker root: `/local/docker`
- Docker cgroup driver: `systemd`
- COSMOS repo: `/local/src/COSMOS`
- benchmark output root: `/local/benchmarks`

## Important Paths

```text
/local/src/COSMOS                         COSMOS source tree
/local/build/cosmos-target                Cargo target directory
/local/cargo                              Cargo home
/local/rustup                             Rustup home
/local/opt/jdk-17                         JDK 17 used by OpenWhisk
/local/cache/gradle                       Gradle cache
/local/venvs/sebs                         SeBS Python environment
/local/benchmarks/runs                    benchmark run output
/local/benchmarks/storage                 SeBS/MinIO storage config and data
/local/benchmarks/openwhisk-home          OpenWhisk standalone home/state
/local/benchmarks/openwhisk-host.env      OpenWhisk API/auth/PID environment file
/local/benchmarks/wskprops                OpenWhisk CLI config
/local/bin                                benchmark convenience commands
```

The login profile `/etc/profile.d/cosmos-benchmark.sh` exports the standard
paths for Rust, Java, Gradle, Go, npm, pip, OpenWhisk, SeBS, and `/local/bin`.

## Convenience Commands

These commands are installed in `/local/bin`:

```text
cosmos-scheduler                         run the COSMOS sched_ext scheduler
cosmos-profiler                          run cosmos-bench-profiler
cosmos-sebs                              run the SeBS CLI from /local/venvs/sebs
cosmos-run-standalone-suite              run built-in standalone profiler suite
cosmos-run-sebs-standalone-suite         run verified local SeBS standalone adapters
cosmos-run-openwhisk-suite               run the OpenWhisk profiler suite
cosmos-run-openwhisk-concurrent-load     run concurrent OpenWhisk burst stress test
cosmos-openwhisk-start                   start OpenWhisk standalone
cosmos-openwhisk-stop                    stop OpenWhisk standalone
cosmos-net-policy                        apply/show/clear tc network policies
cosmos-cgroup-policy                     create/run/show/delete cgroup policies
cosmos-sebs-openwhisk-invoke             invoke one SeBS workload on OpenWhisk
```

Use `--help` on the underlying tool where applicable:

```sh
cosmos-profiler --help
cosmos-sebs --help
cosmos-net-policy
cosmos-cgroup-policy
cosmos-sebs-openwhisk-invoke --help
```

## Services

Start OpenWhisk standalone:

```sh
cosmos-openwhisk-start
source /local/benchmarks/openwhisk-host.env
wsk --apihost "$OPENWHISK_APIHOST" --auth "$OPENWHISK_AUTH" property get
```

Stop OpenWhisk:

```sh
cosmos-openwhisk-stop
```

OpenWhisk standalone uses `/local/benchmarks/openwhisk-home` as its home/state
directory and writes connection details to
`/local/benchmarks/openwhisk-host.env`.

Start or stop SeBS object storage with the SeBS wrapper if needed:

```sh
cosmos-sebs storage start object \
  /local/benchmarks/storage/sebs-storage-input.json \
  --output /local/benchmarks/storage/sebs-storage.json

cosmos-sebs storage stop object /local/benchmarks/storage/sebs-storage.json
```

MinIO state lives in `/local/benchmarks/storage/minio-data`.

## Custom CPU Scheduler

Build and run the scheduler:

```sh
cd /local/src/COSMOS
cargo build --release
sudo -E cosmos-scheduler --slo-target-us 10000
```

The profiler samples COSMOS scheduler stats from
`/var/run/scx/root/stats` when the scheduler is running. Override the socket
with `COSMOS_STATS_SOCKET` if needed:

```sh
COSMOS_STATS_SOCKET=/var/run/scx/root/stats cosmos-profiler ...
```

Stop the scheduler with `Ctrl-C`. A clean stop should unregister the scheduler.

## Resource Policies

Create and use a cgroup with memory, swap, CPU, and pids controls:

```sh
sudo cosmos-cgroup-policy create test \
  --mem 1G \
  --swap 0 \
  --cpu 'max 100000' \
  --pids 4096

sudo cosmos-cgroup-policy show test
sudo cosmos-cgroup-policy run test -- bash -lc 'cat /proc/self/cgroup'
sudo cosmos-cgroup-policy delete test
```

Apply a network delay or rate policy with `tc`:

```sh
sudo cosmos-net-policy show docker0
sudo cosmos-net-policy netem docker0 delay 20ms loss 0.1%
sudo cosmos-net-policy rate docker0 100mbit
sudo cosmos-net-policy clear docker0
```

Use the interface that matches the target path. For OpenWhisk action-container
diagnosis, inspect run outputs for discovered veth information before applying
per-interface policies.

## Standalone Benchmarks

Run the built-in standalone suite:

```sh
OUT_DIR=/local/benchmarks/runs REPETITIONS=1 DURATION_MS=1000 SAMPLE_MS=50 \
  cosmos-run-standalone-suite
```

Run the verified local SeBS standalone adapters:

```sh
OUT_DIR=/local/benchmarks/runs REPETITIONS=1 INPUT_SIZE=test SAMPLE_MS=50 \
  cosmos-run-sebs-standalone-suite
```

Run one profiler workload directly:

```sh
sudo -E cosmos-profiler standalone \
  --out-dir /local/benchmarks/runs \
  --name standalone-cpu \
  --workload cpu \
  --duration-ms 1000 \
  --sample-ms 50
```

Run an arbitrary command inside the profiler cgroup:

```sh
sudo -E cosmos-profiler standalone \
  --out-dir /local/benchmarks/runs \
  --name custom-command \
  --workload command \
  --workload-label custom-command \
  --duration-ms 1000 \
  --sample-ms 50 \
  -- /path/to/command --arg
```

## OpenWhisk Benchmarks

Start OpenWhisk first:

```sh
cosmos-openwhisk-start
source /local/benchmarks/openwhisk-host.env
```

Run the OpenWhisk profiler suite:

```sh
OUT_DIR=/local/benchmarks/runs REPETITIONS=1 cosmos-run-openwhisk-suite
```

Run one OpenWhisk action with the profiler:

```sh
sudo -E cosmos-profiler open-whisk \
  --out-dir /local/benchmarks/runs \
  --name openwhisk-smoke \
  --action cosmos_smoke \
  --file /tmp/cosmos-smoke.js \
  --kind nodejs:20 \
  --insecure \
  --apihost "$OPENWHISK_APIHOST" \
  --auth "$OPENWHISK_AUTH" \
  --param name=benchmark
```

Run the concurrent OpenWhisk stress benchmark:

```sh
REQUESTS=1000 CONCURRENCY=128 BURN_MS=25 OUT_DIR=/local/benchmarks/runs \
  cosmos-run-openwhisk-concurrent-load
```

The concurrent benchmark writes `config.json`, `requests.jsonl`, and
`summary.json` under its run directory. `summary.json` includes throughput,
status counts, success/error counts, and latency percentiles.

The current host-network OpenWhisk standalone path can keep only one action
runtime resident on host port 8080. Clear old action containers before switching
actions or before cold-start experiments:

```sh
sudo docker ps -aq --filter name=wsk0_ | xargs -r sudo docker rm -f
```

Concurrent cold bursts may return `502` while OpenWhisk tries to create
additional host-network action containers. Treat those failures as part of the
stress signal unless you are specifically measuring warmed-action concurrency.

## Capacity Suite

The 32-core/64-thread, 128 GiB remote machine should use the capacity suite for
host-level stress. It leaves the smoke suites fast and adds larger standalone
command workloads:

```sh
cd /local/src/COSMOS
DURATION_S=60 \
CPU_WORKERS=64 \
MEM_WORKERS=32 \
MEM_BYTES_PER_WORKER=2G \
FIO_JOBS=16 \
FIO_SIZE=2G \
NET_PARALLEL=32 \
OW_REQUESTS=512 \
OW_CONCURRENCY=128 \
OW_BURN_MS=50 \
OUT_DIR=/local/benchmarks/runs \
benchmarks/profiler/scripts/run_capacity_suite.sh
```

The default capacity profile drives:

- all 64 logical CPUs with `stress-ng --cpu`
- about 64 GiB of memory with `stress-ng --vm`
- parallel random read/write IO with `fio`
- 32 parallel loopback network streams with `iperf3`
- 512 OpenWhisk HTTP requests at concurrency 128 when OpenWhisk is running

Tune `MEM_WORKERS` and `MEM_BYTES_PER_WORKER` upward for memory-pressure tests,
for example `MEM_WORKERS=32 MEM_BYTES_PER_WORKER=3G` for about 96 GiB. Keep
some memory headroom for OpenWhisk, Docker, MinIO, and the profiler.

## SeBS OpenWhisk Benchmarks

Use the generic helper for one SeBS workload:

```sh
cosmos-sebs-openwhisk-invoke 110.dynamic-html test nodejs 20
```

The helper uses:

- OpenWhisk config: `/local/src/COSMOS/benchmarks/profiler/configs/sebs-openwhisk.json`
- storage config: `/local/benchmarks/storage/sebs-storage.json`
- cache: `/local/cache/sebs`
- output: `/local/benchmarks/sebs-output`

Print the repo's supported matrices:

```sh
cd /local/src/COSMOS
cargo run -p cosmos-bench-profiler -- matrix --kind sebs-openwhisk
cargo run -p cosmos-bench-profiler -- matrix --kind sebs-standalone
```

The SeBS capability manifest is
`/local/src/COSMOS/benchmarks/profiler/configs/sebs-capabilities.json`.
Some workloads are intentionally excluded by the harness manifest because they
need storage adapters, external services, model assets, or runtime work that is
not yet implemented. That is a benchmark harness limitation, not a missing
remote-machine dependency.

## Vendored Dependency Patches

The prepared remote checkout has two local patches applied inside upstream
submodules:

```text
benchmarks/profiler/patches/openwhisk-host-network.patch
benchmarks/profiler/patches/sebs-local-container.patch
```

Apply them after initializing submodules on a fresh machine:

```sh
cd /local/src/COSMOS/benchmarks/third_party/openwhisk
git apply ../../profiler/patches/openwhisk-host-network.patch

cd /local/src/COSMOS/benchmarks/third_party/serverless-benchmarks
git apply ../../profiler/patches/sebs-local-container.patch
```

The OpenWhisk patch makes host-network action containers report
`127.0.0.1` as their action endpoint. The SeBS patch allows local container
builds with `SEBS_DOCKER_BUILD_NETWORK`, skips Docker image pushes when
`SEBS_SKIP_IMAGE_PUSH=1`, and carries the SeBS benchmark fixes needed for the
prepared OpenWhisk environment.

## Run Outputs

Profiler runs under `/local/benchmarks/runs/<run-id>` contain:

```text
run_meta.json
events.jsonl
stdout.log
stderr.log
perf_stat.csv
cgroup_cpu.csv
cgroup_memory.csv
cgroup_io.csv
cgroup_pressure.csv
net.csv
qdisc.csv
scheduler_stats.csv
client_latency.csv
openwhisk_activation.json
summary.json
```

Verify a run:

```sh
cd /local/src/COSMOS
cargo run -p cosmos-bench-profiler -- verify-run \
  --run-dir /local/benchmarks/runs/<run-id>
```

Rebuild the aggregate profile DB:

```sh
cd /local/src/COSMOS
cargo run -p cosmos-bench-profiler -- profile-db \
  --runs-dir /local/benchmarks/runs \
  --out /local/src/COSMOS/benchmarks/profile_db.json \
  --strict
```

## Image-Baking Notes

Keep these settings when baking a new image:

- keep `/local` mounted and writable before Docker/OpenWhisk/SeBS startup
- keep Docker's data root at `/local/docker`
- keep `/etc/profile.d/cosmos-benchmark.sh`
- keep `/local/bin` wrappers
- keep Rust/Cargo, Gradle, npm, pip, Go, and SeBS caches under `/local`
- keep OpenWhisk state/config under `/local/benchmarks/openwhisk-home` and
  `/local/benchmarks/wskprops`
- do not bake benchmark output into `/home` or `/users/Hanning`

After booting a baked image, re-run:

```sh
source /etc/profile.d/cosmos-benchmark.sh
cd /local/src/COSMOS
cargo run -p cosmos-bench-profiler -- preflight --strict
cosmos-openwhisk-start
REQUESTS=4 CONCURRENCY=2 BURN_MS=10 OUT_DIR=/local/benchmarks/runs \
  cosmos-run-openwhisk-concurrent-load
```

## Troubleshooting

If `wsk` points at `https://host`, source the generated OpenWhisk environment:

```sh
source /local/benchmarks/openwhisk-host.env
wsk --apihost "$OPENWHISK_APIHOST" --auth "$OPENWHISK_AUTH" property set \
  --apihost "$OPENWHISK_APIHOST" --auth "$OPENWHISK_AUTH"
```

If an OpenWhisk action times out during initialization on the host-network
path, remove stale action containers and retry:

```sh
sudo docker ps -aq --filter name=wsk0_ | xargs -r sudo docker rm -f
```

If profiler runs cannot collect scheduler stats, confirm the scheduler is
running and the stats socket exists:

```sh
sudo -E cosmos-scheduler --slo-target-us 10000
ls -l /var/run/scx/root/stats
```

If a benchmark accidentally writes state under `/users/Hanning`, move or delete
that state before baking an image and update the relevant environment variable
or wrapper to point at `/local`.
