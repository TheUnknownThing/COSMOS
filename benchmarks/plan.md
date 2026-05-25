# COSMOS Benchmark Architecture

## Two benchmark paths

### Lightweight Rust path (`scripts/` + `workloads/`) — primary

The Rust workload runner (`cosmos-benchmark-workload`) provides seven synthetic
calibrated workloads. A Python harness (`harness.py`) runs them concurrently,
launches the COSMOS scheduler, collects stats, and produces comparison-ready
output. Dispatch happens through `burst_benchmark.py`.

This is the path for day-to-day CFS-vs-COSMOS SLO comparison. It avoids
OpenWhisk, SeBS, Docker, and the profiler stack.

Workloads use fixed-cost calibration — each runs a unit of work, measures how
long it takes, then estimates total iterations to fill the target budget. No
clock-polling hot loops.

### Profiler path (`profiler/`) — secondary / offline analysis

`cosmos-bench-profiler` records deep resource traces (perf, cgroup, eBPF,
qdisc, lifecycle) for offline phase classification and profile DB generation.
It supports standalone cgroup mode and OpenWhisk activation mode with SeBS
workloads. See `profiler/README.md`.

## Configs

| config              | scheduler | metadata | pools | deadline scoring | tail guard |
|---------------------|-----------|----------|-------|------------------|------------|
| `cfs-default`       | CFS       | no       | no    | no               | no         |
| `cosmos-heuristic`  | COSMOS    | no       | no    | no               | no         |
| `cosmos-metadata`   | COSMOS    | yes      | no    | no               | no         |
| `cosmos-pooled`     | COSMOS    | yes      | yes   | no               | no         |
| `cosmos-full`       | COSMOS    | yes      | yes   | yes              | yes        |

`cosmos-heuristic` relies on the scheduler's built-in heuristic classification.
`cosmos-full` enables the complete pipeline.

## Workloads

| workload            | calibration | resource pressure |
|---------------------|-------------|-------------------|
| `cpu_burst`         | CPU time    | dense matrix multiply |
| `sleep_short`       | wall time   | baseline (thread sleep) |
| `io_mixed`          | wall time   | file write/read/sync |
| `memory_heavy`      | wall time   | large-buffer strided scan |
| `network_heavy`     | wall time   | loopback TCP echo |
| `compression_mixed` | wall time   | RLE compress/decompress roundtrip |
| `graph_bfs`         | wall time   | irregular graph BFS |

Default duration: 250 ms. Default deadline: 2x duration.

## Run lifecycle

1. Build: `cargo build --release -p cosmos-benchmark-workload` (harness
   auto-checks and rebuilds if sources are newer).
2. For COSMOS configs: the harness starts the scheduler, waits for the
   `scx_stats` socket, optionally starts the metadata event bridge, then begins
   stats capture.
3. Concurrent invocations launch via `ThreadPoolExecutor`. Each invocation runs
   the Rust binary wrapped with `/usr/bin/time` for CPU accounting.
4. After all invocations complete, the harness stops the scheduler and event
   bridge, writes `summary.json`, and creates a `latest` symlink.

## Result directory layout

```
results/<config>/<timestamp>/
  manifest.json              — config, workload, concurrency, flags
  client_latency.csv         — per-invocation wall-clock durations
  invocations/               — per-invocation json + stderr
  scheduler_stats.jsonl      — scx_stats samples (COSMOS only)
  scheduler.log              — scheduler stdout
  event_bridge.log           — metadata bridge log (metadata configs only)
  summary.json               — aggregated latency, load, compute, scheduler
```

## Compare

```sh
python3 benchmarks/scripts/compare.py results/cfs-default/ results/cosmos-full/
```

Prints a metric table (p50, p95, p99, mean, SLO violations) plus a
capacity-fairness verdict.

## Fair-case judgment

A run is `unfair-overloaded` when:

```
total_compute_ms >= deadline_ms * cpu_cores
```

Fair load classes: `underloaded` (< 0.80), `full` (0.80–0.95),
`slightly-overloaded` (0.95–1.00), `unfair-overloaded` (>= 1.00).

CPU demand comes from `/usr/bin/time` when available, falling back to
`duration_ms * concurrency`.

## Metrics that matter

For scheduler evaluation, the key comparison metrics are:
- Client-side p99 latency and SLO hit rate
- Per-invocation CPU time (from `/usr/bin/time`)
- Scheduler counters: SLO violations, dispatch failures, boosts, pool migrations
- Load ratio and fairness class

## Build system note

On the CloudLab benchmark hosts, the cargo target directory is configured at
`/usr/local/cosmos/build/cosmos-target` rather than the project-local `target/`.
The harness Python scripts expect `REPO_ROOT/target/release/`. Create a symlink:

```sh
ln -s /usr/local/cosmos/build/cosmos-target /path/to/COSMOS/target
```
