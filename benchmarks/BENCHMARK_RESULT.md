# COSMOS Benchmark Result Report

Date written: 2026-05-13

This report summarizes the benchmark environment, what was tested, how the
benchmark stack works, and what we learned from the saved SeBS/OpenWhisk matrix
run on the CloudLab benchmark host.

## Executive Summary

We prepared a standard benchmark environment for COSMOS on a CloudLab AMD
machine and ran a full runnable SeBS OpenWhisk matrix for the configured runtime
environment.

The matrix was:

- 15 SeBS workloads
- 3 input sizes: `test`, `small`, `large`
- 1 runtime per workload, using the verified OpenWhisk runtime path:
  Node.js 20 for Node workloads and Python 3.11 for Python workloads
- 45 benchmark cells total

The raw shell-level matrix completed all 45 cells, but SeBS-level result parsing
shows:

- 43 successful invocations
- 2 failed invocations:
  - `110.dynamic-html` with `large` input
  - `503.graph-bfs` with `large` input

The results show clear phase diversity across the workload set. Some workloads
are mostly invocation/runtime overhead, some are storage-heavy, some are
compute/model-heavy, and some scale strongly with input size. That is exactly
the kind of behavior a phase-aware resource co-scheduler should target.

The strongest scaling signals were:

- `220.video-processing`: `large` input was about 5.1x slower than `test`.
- `120.uploader`: `large` input was about 2.1x slower than `test`.
- `501.graph-pagerank` and `502.graph-mst`: `large` input was about 2.1-2.2x
  slower than `test`.

The longest workloads were:

- `504.dna-visualisation`: about 9.0 seconds across all tested inputs.
- `411.image-recognition`: 5.8 seconds on `test`, about 2.7 seconds on
  `small`/`large`, likely due to warm asset/cache effects after the first run.
- `220.video-processing large`: about 7.1 seconds.

The saved OpenWhisk log shows that this full matrix was cold-container
dominated: all 45 OpenWhisk container starts were recorded as `cold`. This makes
the run useful for cold-start characterization, but it is not a warm-vs-cold
comparison by itself.

## Where Results Are Saved

The benchmark host was backed up locally before termination. The archive is:

```text
benchmarks/remote-backup/amd108-20260507-200353/benchmark-results.tgz
```

The extracted matrix result is:

```text
benchmarks/remote-backup/amd108-20260507-200353/usr/local/cosmos/benchmarks/runs/full-sebs-openwhisk-matrix-20260507-055458/results.tsv
```

The backup also includes:

- all 45 per-cell logs,
- SeBS `experiments.json` output directories,
- OpenWhisk logs,
- benchmark scripts and configs,
- remote machine inventory.

Archive checksum:

```text
40503163790a78de8d7c4d005f4bd344225cefd416dbdf08e1905a4d93ceb033
```

## Benchmark Machine and Environment

Remote host:

```text
amd108.utah.cloudlab.us
```

Recorded environment:

```text
kernel: 7.0.0-070000-generic
repo: /usr/local/src/COSMOS
benchmark prefix: /usr/local/cosmos
git head: 48b79b4c6a11ba64439357defa1abc9606e73a01
```

Persistent benchmark state was installed under `/usr/local/cosmos`, not under
`/home`, so the environment can be captured into a CloudLab image.

Important components:

- Docker data root: `/usr/local/cosmos/docker`
- SeBS virtualenv: `/usr/local/cosmos/venvs/sebs`
- SeBS cache: `/usr/local/cosmos/cache/sebs`
- OpenWhisk state: `/usr/local/cosmos/benchmarks/openwhisk-home`
- Benchmark outputs: `/usr/local/cosmos/benchmarks/runs`
- SeBS storage config: `/usr/local/cosmos/benchmarks/storage/sebs-storage.json`

Services used by the benchmark:

- OpenWhisk standalone
- MinIO object storage
- ScyllaDB/Alternator NoSQL storage
- COSMOS SeBS microserver service for network/reply microbenchmarks

## What the Benchmark Stack Is Testing

The benchmark stack combines three layers:

1. SeBS, the Serverless Benchmark Suite.
2. Apache OpenWhisk standalone, used as the FaaS runtime.
3. COSMOS profiler and resource-control wrappers.

SeBS provides the serverless workloads. OpenWhisk gives the workloads realistic
serverless activation behavior: action deployment, cold start, container launch,
runtime initialization, function invocation, and result collection. COSMOS
profiling/resource scripts provide host-side measurement and controls for later
scheduler experiments.

The benchmark is not only measuring application compute. For serverless systems,
the observed latency includes several layers:

- client-side invocation overhead,
- OpenWhisk controller/invoker overhead,
- container/action initialization,
- language runtime startup,
- benchmark application work,
- object storage or NoSQL service access,
- upload/download time,
- result validation and return path.

This is important for scheduler work. A serverless function can be short at the
application level but still experience meaningful cold-start or platform
overhead. A good scheduler must distinguish these phases rather than treating the
whole activation as one uniform CPU-bound task.

## Workloads in the Matrix

The runnable OpenWhisk matrix used these 15 workloads:

| Workload | Runtime | What it exercises |
|---|---|---|
| `010.sleep` | Node.js 20 | lifecycle/control workload; mostly runtime/platform overhead |
| `020.network-benchmark` | Python 3.11 | network path to external microserver |
| `030.clock-synchronization` | Python 3.11 | UDP/time coordination path |
| `040.server-reply` | Node.js 20 | server reply path |
| `110.dynamic-html` | Node.js 20 | HTML/string generation |
| `120.uploader` | Node.js 20 | object upload and external fetch path |
| `130.crud-api` | Node.js 20 | NoSQL-backed CRUD path |
| `210.thumbnailer` | Node.js 20 | object-storage-backed image processing |
| `220.video-processing` | Python 3.11 | video processing plus object storage |
| `311.compression` | Node.js 20 | compression plus object storage |
| `411.image-recognition` | Python 3.11 | model/inference path |
| `501.graph-pagerank` | Python 3.11 | graph algorithm |
| `502.graph-mst` | Python 3.11 | graph algorithm |
| `503.graph-bfs` | Python 3.11 | graph algorithm |
| `504.dna-visualisation` | Python 3.11 | scientific/data visualization path |

Scope note: upstream SeBS contains more language implementations than this run
used. This run is the full runnable workload/input matrix for the configured
OpenWhisk environment, using the verified Node.js 20 and Python 3.11 paths.

## Result Table

The table below reports SeBS client-side invocation time. This is the useful
end-to-end number for a serverless caller. It includes platform and runtime
overhead, not just application compute.

| Workload | Runtime | Test ms | Small ms | Large ms | Large/Test | SeBS status |
|---|---:|---:|---:|---:|---:|---|
| `010.sleep` | nodejs 20 | 928.2 | 372.2 | 375.3 | 0.40x | ok |
| `020.network-benchmark` | python 3.11 | 856.4 | 523.8 | 551.5 | 0.64x | ok |
| `030.clock-synchronization` | python 3.11 | 546.9 | 576.6 | 560.1 | 1.02x | ok |
| `040.server-reply` | nodejs 20 | 381.3 | 408.7 | 371.8 | 0.97x | ok |
| `110.dynamic-html` | nodejs 20 | 424.7 | 414.2 | 590.0 | 1.39x | failed on large |
| `120.uploader` | nodejs 20 | 1133.7 | 1881.5 | 2401.6 | 2.12x | ok |
| `130.crud-api` | nodejs 20 | 678.7 | 536.6 | 552.1 | 0.81x | ok |
| `210.thumbnailer` | nodejs 20 | 647.3 | 536.7 | 526.9 | 0.81x | ok |
| `220.video-processing` | python 3.11 | 1387.0 | 1492.8 | 7093.1 | 5.11x | ok |
| `311.compression` | nodejs 20 | 1184.9 | 1132.0 | 1101.5 | 0.93x | ok |
| `411.image-recognition` | python 3.11 | 5834.5 | 2675.7 | 2677.6 | 0.46x | ok |
| `501.graph-pagerank` | python 3.11 | 431.4 | 425.6 | 950.4 | 2.20x | ok |
| `502.graph-mst` | python 3.11 | 418.5 | 418.7 | 900.4 | 2.15x | ok |
| `503.graph-bfs` | python 3.11 | 456.3 | 431.2 | 900.4 | 1.97x | failed on large |
| `504.dna-visualisation` | python 3.11 | 9043.9 | 9069.0 | 8997.0 | 0.99x | ok |

Two cells were marked as failures by SeBS even though the shell wrapper recorded
the command as `ok`:

- `110.dynamic-html large`
- `503.graph-bfs large`

The shell status is therefore not enough for correctness. For analysis, use the
SeBS `experiments.json` failure flag.

## Cold vs Warm Container Behavior

This matrix does not contain a warm-container comparison. The OpenWhisk log
records 45 container starts and all 45 were `cold`.

The reason is methodological, not a property of the workloads. The matrix runner
removed `wsk0_` action containers after each benchmark cell to avoid collisions
from the host-network OpenWhisk runtime path. With that cleanup policy, the next
cell cannot reuse a resident action container, so the measured OpenWhisk
activation path is cold-start dominated.

What was recorded:

- SeBS `experiments.json` records a `cold_start` boolean for successful
  invocations.
- In this OpenWhisk path, SeBS startup/init timing fields were present but
  reported `0.0`, so they are not useful for startup-time analysis.
- OpenWhisk standalone logs record the useful lifecycle markers:
  `containerStart containerState`, `invoker_docker.run_*`,
  `invoker_activationInit_*`, and `invoker_activationRun_*`.

From the saved OpenWhisk log:

| Metric | Value |
|---|---:|
| Container starts | 45 |
| Cold starts | 45 |
| Warm starts | 0 |
| Docker run median / p95 | 121 ms / 127 ms |
| Activation init median / p95 | 160 ms / 181 ms |
| Docker run + activation init median / p95 | 283 ms / 305 ms |
| Docker run + activation init max | 633 ms |

Here, "startup time" means the invoker-visible cold-start path from Docker
container creation through action initialization. It does not include action
deployment time or earlier client/controller queueing before the invoker starts
the container.

The cold-start resource allocation pattern is visible in the Docker run
arguments recorded by OpenWhisk. The matrix used fixed memory limits and
proportional Docker CPU shares:

| Memory limit | CPU shares | Activations | Workloads |
|---:|---:|---:|---|
| 128 MiB | 4 | 21 | `010`, `020`, `030`, `040`, `110`, `120`, `130` |
| 256 MiB | 8 | 6 | `210`, `311` |
| 512 MiB | 16 | 9 | `501`, `502`, `503` |
| 768 MiB | 24 | 3 | `411` |
| 2048 MiB | 64 | 6 | `220`, `504` |

Each action container was also launched with `--memory-swap` equal to
`--memory`, `--network host`, `--pids-limit 1024`, and restricted capabilities
such as `--cap-drop NET_RAW` and `--cap-drop NET_ADMIN`.

The benchmark archive therefore records the configured cold-start allocation and
the cold-start lifecycle timing. It does not record a detailed CPU/memory/I/O
time series for each SeBS cold start. The standalone COSMOS profiler smoke tests
record cgroup-level resource counters, but the full SeBS matrix was run through
the SeBS CLI and OpenWhisk logs rather than through per-cell COSMOS profiler
collection.

To measure warm behavior, we should add a separate mode:

1. Clean existing `wsk0_` containers.
2. Deploy one action.
3. Invoke once and record the cold sample.
4. Invoke the same action repeatedly without deleting the container.
5. Parse OpenWhisk lifecycle markers and profiler/cgroup samples for cold and
   warm invocations separately.

Because the current runtime path uses host networking, this should be done one
resident action at a time unless action concurrency is explicitly configured.

## Application-Phase Details

For several workloads, SeBS reports internal timing fields. These help identify
which resource phase dominates.

| Workload/Input | Client ms | Benchmark ms | Compute ms | Download ms | Upload ms | Model ms |
|---|---:|---:|---:|---:|---:|---:|
| `120.uploader test` | 1133.7 | 520.0 | 519.8 | 0.0 | 0.0 | 0.0 |
| `120.uploader small` | 1881.5 | 1360.0 | 1360.0 | 0.0 | 0.0 | 0.0 |
| `120.uploader large` | 2401.6 | 1888.0 | 1887.5 | 0.0 | 0.0 | 0.0 |
| `220.video-processing test` | 1387.0 | 1020.6 | 384.2 | 35.9 | 40.7 | 0.0 |
| `220.video-processing small` | 1492.8 | 1150.0 | 656.0 | 35.1 | 44.8 | 0.0 |
| `220.video-processing large` | 7093.1 | 6746.6 | 6264.5 | 35.1 | 22.1 | 0.0 |
| `311.compression test` | 1184.9 | 566.0 | 341.0 | 115.0 | 108.0 | 0.0 |
| `311.compression small` | 1132.0 | 637.0 | 400.0 | 123.0 | 113.0 | 0.0 |
| `311.compression large` | 1101.5 | 607.0 | 384.0 | 114.0 | 107.0 | 0.0 |
| `411.image-recognition test` | 5834.5 | 5471.3 | 2764.8 | 294.1 | 0.0 | 2600.0 |
| `411.image-recognition small` | 2675.7 | 2334.4 | 700.6 | 304.3 | 0.0 | 626.4 |
| `411.image-recognition large` | 2677.6 | 2329.5 | 705.5 | 296.9 | 0.0 | 630.7 |
| `504.dna-visualisation test` | 9043.9 | 8686.3 | 1482.8 | 19.4 | 1482.8 | 0.0 |
| `504.dna-visualisation small` | 9069.0 | 8723.8 | 1490.4 | 19.0 | 1490.4 | 0.0 |
| `504.dna-visualisation large` | 8997.0 | 8657.6 | 1420.5 | 18.8 | 1420.5 | 0.0 |
| `501.graph-pagerank large` | 950.4 | 600.9 | 235.0 | 0.0 | 0.0 | 0.0 |
| `502.graph-mst large` | 900.4 | 418.6 | 49.0 | 0.0 | 0.0 | 0.0 |

Important observations:

- `220.video-processing` is the clearest scalable compute-heavy workload. Its
  large input is dominated by compute time.
- `120.uploader` scales steadily with input size and is useful for storage or
  object-path interference studies.
- `311.compression` has a stable storage component and moderate compute. In this
  matrix the large input did not become slower, likely because the tested input
  path is dominated by fixed archive/object behavior.
- `411.image-recognition` is expensive on the first run and then much faster,
  suggesting model/runtime/cache warming.
- `504.dna-visualisation` is consistently long but does not scale across input
  labels in this run, suggesting the selected SeBS inputs may map to similar work
  for this workload.

## Does the Workload Exhibit Phase-Aware Resource Consumption?

Yes.

The workload set is not homogeneous. It contains:

- lifecycle/control workloads: `010.sleep`,
- network/reply workloads: `020.network-benchmark`, `030.clock-synchronization`,
  `040.server-reply`,
- storage/object workloads: `120.uploader`, `210.thumbnailer`, `311.compression`,
- compute plus storage workloads: `220.video-processing`, `504.dna-visualisation`,
- model/inference workloads: `411.image-recognition`,
- graph/scientific compute workloads: `501`, `502`, `503`.

This matters for COSMOS because different phases place pressure on different
host resources:

- CPU-bound phases benefit from CPU scheduling and core allocation.
- Storage phases are sensitive to object-store latency, page cache, and I/O.
- Network phases are sensitive to packet path and service placement.
- Cold-start phases are sensitive to container/runtime startup, image/cache
  state, and OpenWhisk scheduling.
- Model/inference phases can shift between CPU, memory bandwidth, and native
  library execution.

A phase-aware scheduler should not treat a serverless activation as a single
constant type of work. The benchmark data supports that design assumption.

## Python Processes and the GIL

During the benchmark, `htop` showed many Python processes. This does not imply
one global Python GIL bottleneck across the benchmark.

Python's GIL is per interpreter process. Separate Python processes have separate
GILs. Separate OpenWhisk action containers also have separate interpreter
processes and can run on different cores.

What was observed after the run:

- Remaining Python processes were mostly service/control processes, such as
  ScyllaDB entrypoint/supervisor, `networkd-dispatcher`, and
  `cosmos-sebs-microservers`.
- The Python benchmark source files do not use Python `threading`,
  `multiprocessing`, `concurrent.futures`, `Thread`, `Process`, or `Pool`.
- Python benchmark actions run as separate OpenWhisk action processes, not as
  many threads inside one shared interpreter.

Conclusion:

- There is no single cross-benchmark GIL bottleneck.
- A single pure-Python action process can still be limited by its own GIL if it
  is CPU-bound and implemented in Python bytecode.
- Some workloads call native libraries or external tools. Those may release or
  bypass the GIL, so the GIL is not necessarily the limiting factor even inside a
  Python workload.

## Known Limitations

This matrix is useful, but it is not a complete scheduler evaluation yet.

Limitations:

- `REPETITIONS=1`; there is no statistical confidence interval.
- The run was sequential, not concurrent.
- It did not compare baseline Linux scheduling against COSMOS scheduling.
- It used the configured runnable OpenWhisk runtime matrix, not every language
  implementation shipped by upstream SeBS.
- Two large-input invocations failed at the SeBS level.
- It does not compare cold and warm containers; the saved OpenWhisk log shows
  45 cold starts and zero warm starts for this matrix.
- It records OpenWhisk cold-start lifecycle timing and configured Docker
  resource limits, but not detailed per-cell cgroup time series for every SeBS
  activation.
- The run includes cold-start/cache effects. Some workloads, especially
  `411.image-recognition`, changed significantly after assets/runtime state were
  warmed.

## Recommended Next Experiment

For a scheduler paper or collaborator discussion, the next useful experiment is
not another sequential full matrix. The next experiment should compare baseline
Linux scheduling against COSMOS under concurrent mixed workloads.

Recommended design:

1. Select representative workloads:
   - `220.video-processing large` for compute-heavy scaling,
   - `120.uploader large` for object/storage behavior,
   - `411.image-recognition small` or `large` for model/inference behavior,
   - `010.sleep` or `040.server-reply` as a latency-sensitive control workload.
2. Run mixed concurrent bursts at several concurrency levels, for example 4, 16,
   32, and 64.
3. Repeat each cell at least 5 times.
4. Compare:
   - default Linux scheduling,
   - COSMOS scheduler,
   - COSMOS plus explicit resource policies if needed.
5. Record:
   - p50/p95/p99 latency,
   - success/error rate,
   - CPU time,
   - memory peak,
   - I/O bytes,
   - scheduler stats,
   - phase-window labels from the profiler.

The current matrix proves that the benchmark environment works and that the
workload set has phase diversity. The next experiment should test whether COSMOS
uses that diversity to improve latency or isolation under contention.
