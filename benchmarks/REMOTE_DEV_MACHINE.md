# Remote Benchmark Machine Runbook

This runbook describes the prepared COSMOS benchmark environment on:

```sh
ssh Hanning@amd108.utah.cloudlab.us
```

The machine is intended to be image-bakeable. Persistent software, build
artifacts, benchmark data, caches, and service state live under `/usr/local/cosmos`.
Avoid storing benchmark state under `/home` or `/users/Hanning`.

## Quick Start

```sh
ssh Hanning@amd108.utah.cloudlab.us
source /etc/profile.d/cosmos-benchmark.sh
cd /usr/local/src/COSMOS
```

Check the environment:

```sh
cargo run -p cosmos-bench-profiler -- preflight --strict
uname -r
df -h /usr/local/cosmos
docker info --format 'DockerRoot={{.DockerRootDir}} Cgroup={{.CgroupDriver}}'
```

Expected basics:

- kernel: `7.0.0-070000-generic`
- `/usr/local/cosmos`: root-image persistent benchmark prefix
- Docker root: `/usr/local/cosmos/docker`
- Docker cgroup driver: `systemd`
- COSMOS repo: `/usr/local/src/COSMOS`
- benchmark output root: `/usr/local/cosmos/benchmarks`

## Important Paths

```text
/usr/local/src/COSMOS                         COSMOS source tree
/usr/local/cosmos/build/cosmos-target                Cargo target directory
/usr/local/cosmos/cargo                              Cargo home
/usr/local/cosmos/rustup                             Rustup home
/usr                         JDK 17 used by OpenWhisk
/usr/local/cosmos/cache/gradle                       Gradle cache
/usr/local/cosmos/venvs/sebs                         SeBS Python environment
/usr/local/cosmos/benchmarks/runs                    benchmark run output
/usr/local/cosmos/benchmarks/storage                 SeBS/MinIO storage config and data
/usr/local/cosmos/benchmarks/openwhisk-home          OpenWhisk standalone home/state
/usr/local/cosmos/benchmarks/openwhisk-host.env      OpenWhisk API/auth/PID environment file
/usr/local/cosmos/benchmarks/wskprops                OpenWhisk CLI config
/usr/local/cosmos/bin                                benchmark convenience commands
```

The login profile `/etc/profile.d/cosmos-benchmark.sh` exports the standard
paths for Rust, Java, Gradle, Go, npm, pip, OpenWhisk, SeBS, and `/usr/local/cosmos/bin`.

## Convenience Commands

These commands are installed in `/usr/local/cosmos/bin`:

```text
cosmos-scheduler                         run the COSMOS sched_ext scheduler
cosmos-profiler                          run cosmos-bench-profiler
cosmos-sebs                              run the SeBS CLI from /usr/local/cosmos/venvs/sebs
cosmos-run-standalone-suite              run built-in standalone profiler suite
cosmos-run-sebs-standalone-suite         run verified local SeBS standalone adapters
cosmos-run-openwhisk-suite               run the OpenWhisk profiler suite
cosmos-run-openwhisk-concurrent-load     run concurrent OpenWhisk burst stress test
cosmos-openwhisk-start                   start OpenWhisk standalone
cosmos-openwhisk-stop                    stop OpenWhisk standalone
cosmos-sebs-openwhisk-invoke             invoke one SeBS workload on OpenWhisk
```

Use `--help` on the underlying tool where applicable:

```sh
cosmos-profiler --help
cosmos-sebs --help
cosmos-sebs-openwhisk-invoke --help
```

## Services

Start OpenWhisk standalone:

```sh
cosmos-openwhisk-start
source /usr/local/cosmos/benchmarks/openwhisk-host.env
wsk --apihost "$OPENWHISK_APIHOST" --auth "$OPENWHISK_AUTH" property get
```

Stop OpenWhisk:

```sh
cosmos-openwhisk-stop
```

OpenWhisk standalone uses `/usr/local/cosmos/benchmarks/openwhisk-home` as its home/state
directory and writes connection details to
`/usr/local/cosmos/benchmarks/openwhisk-host.env`.

Start or stop SeBS object storage with the SeBS wrapper if needed:

```sh
cosmos-sebs storage start object \
  /usr/local/cosmos/benchmarks/storage/sebs-storage-input.json \
  --output /usr/local/cosmos/benchmarks/storage/sebs-storage.json

cosmos-sebs storage stop object /usr/local/cosmos/benchmarks/storage/sebs-storage.json
```

MinIO state lives in `/usr/local/cosmos/benchmarks/storage/minio-data`.

## Custom CPU Scheduler

Build and run the scheduler:

```sh
cd /usr/local/src/COSMOS
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

No `cosmos-net-policy` or `cosmos-cgroup-policy` wrapper is installed on this image.
Use the host `tc` and cgroup-v2 interfaces directly if a benchmark needs manual
resource shaping, or add those wrappers to the repo before documenting them here.

## Standalone Benchmarks

Run the built-in standalone suite:

```sh
OUT_DIR=/usr/local/cosmos/benchmarks/runs REPETITIONS=1 DURATION_MS=1000 SAMPLE_MS=50 \
  cosmos-run-standalone-suite
```

Run the verified local SeBS standalone adapters:

```sh
OUT_DIR=/usr/local/cosmos/benchmarks/runs REPETITIONS=1 INPUT_SIZE=test SAMPLE_MS=50 \
  cosmos-run-sebs-standalone-suite
```

Run one profiler workload directly:

```sh
sudo -E cosmos-profiler standalone \
  --out-dir /usr/local/cosmos/benchmarks/runs \
  --name standalone-cpu \
  --workload cpu \
  --duration-ms 1000 \
  --sample-ms 50
```

Run an arbitrary command inside the profiler cgroup:

```sh
sudo -E cosmos-profiler standalone \
  --out-dir /usr/local/cosmos/benchmarks/runs \
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
source /usr/local/cosmos/benchmarks/openwhisk-host.env
```

Run the OpenWhisk profiler suite:

```sh
OUT_DIR=/usr/local/cosmos/benchmarks/runs REPETITIONS=1 cosmos-run-openwhisk-suite
```

Run one OpenWhisk action with the profiler:

```sh
sudo -E cosmos-profiler open-whisk \
  --out-dir /usr/local/cosmos/benchmarks/runs \
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
REQUESTS=1000 CONCURRENCY=128 BURN_MS=25 OUT_DIR=/usr/local/cosmos/benchmarks/runs \
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
cd /usr/local/src/COSMOS
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
OUT_DIR=/usr/local/cosmos/benchmarks/runs \
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

- OpenWhisk config: `/usr/local/src/COSMOS/benchmarks/profiler/configs/sebs-openwhisk.json`
- storage config: `/usr/local/cosmos/benchmarks/storage/sebs-storage.json`
- cache: `/usr/local/cosmos/cache/sebs`
- output: `/usr/local/cosmos/benchmarks/sebs-output`

Print the repo's supported matrices:

```sh
cd /usr/local/src/COSMOS
cargo run -p cosmos-bench-profiler -- matrix --kind sebs-openwhisk
cargo run -p cosmos-bench-profiler -- matrix --kind sebs-standalone
```

The SeBS capability manifest is
`/usr/local/src/COSMOS/benchmarks/profiler/configs/sebs-capabilities.json`.
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
cd /usr/local/src/COSMOS/benchmarks/third_party/openwhisk
git apply ../../profiler/patches/openwhisk-host-network.patch

cd /usr/local/src/COSMOS/benchmarks/third_party/serverless-benchmarks
git apply ../../profiler/patches/sebs-local-container.patch
```

The OpenWhisk patch makes host-network action containers report
`127.0.0.1` as their action endpoint. The SeBS patch allows local container
builds with `SEBS_DOCKER_BUILD_NETWORK`, skips Docker image pushes when
`SEBS_SKIP_IMAGE_PUSH=1`, and carries the SeBS benchmark fixes needed for the
prepared OpenWhisk environment.

## Run Outputs

Profiler runs under `/usr/local/cosmos/benchmarks/runs/<run-id>` contain:

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
cd /usr/local/src/COSMOS
cargo run -p cosmos-bench-profiler -- verify-run \
  --run-dir /usr/local/cosmos/benchmarks/runs/<run-id>
```

Rebuild the aggregate profile DB:

```sh
cd /usr/local/src/COSMOS
cargo run -p cosmos-bench-profiler -- profile-db \
  --runs-dir /usr/local/cosmos/benchmarks/runs \
  --out /usr/local/src/COSMOS/benchmarks/profile_db.json \
  --strict
```

## Image-Baking Notes

Keep these settings when baking a new image:

- keep `/usr/local/cosmos` created and writable before Docker/OpenWhisk/SeBS startup
- keep Docker's data root at `/usr/local/cosmos/docker`
- keep `/etc/profile.d/cosmos-benchmark.sh`
- keep `/usr/local/cosmos/bin` wrappers symlinked from `/usr/local/bin`
- keep Rust/Cargo, Gradle, npm, pip, Go, and SeBS caches under `/usr/local/cosmos`
- keep OpenWhisk state/config under `/usr/local/cosmos/benchmarks/openwhisk-home` and
  `/usr/local/cosmos/benchmarks/wskprops`
- do not bake benchmark output into `/home` or `/users/Hanning`

After booting a baked image, re-run:

```sh
source /etc/profile.d/cosmos-benchmark.sh
cd /usr/local/src/COSMOS
cargo run -p cosmos-bench-profiler -- preflight --strict
cosmos-openwhisk-start
REQUESTS=4 CONCURRENCY=2 BURN_MS=10 OUT_DIR=/usr/local/cosmos/benchmarks/runs \
  cosmos-run-openwhisk-concurrent-load
```

## Troubleshooting

If `wsk` points at `https://host`, source the generated OpenWhisk environment:

```sh
source /usr/local/cosmos/benchmarks/openwhisk-host.env
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
or wrapper to point at `/usr/local/cosmos`.
