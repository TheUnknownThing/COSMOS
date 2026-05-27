# COSMOS Benchmarks

## Quick start

Build everything once:

```sh
cargo build --release -p cosmos-benchmark-workload
```

CFS baseline (no root needed):

```sh
benchmarks/scripts/burst_benchmark.py --concurrency 100 --workload cpu_burst --config cfs-default
```

COSMOS comparison (needs root — it launches the scheduler). If the harness
stats-socket wait times out, start the scheduler manually first:

```sh
sudo benchmarks/scripts/burst_benchmark.py --concurrency 100 --workload cpu_burst --config cosmos-full

# alternate: start scheduler in background, then run workload
sudo target/release/cosmos --slo-target-us 500000 > /tmp/cosmos.log 2>&1 &
sleep 3
benchmarks/scripts/run_cosmos.py --config cosmos-full --workload cpu_burst --concurrency 100 --out-dir /tmp/results
sudo kill %1
```

Print the comparison:

```sh
python3 benchmarks/scripts/compare.py benchmarks/scripts/results/cfs-default/ benchmarks/scripts/results/cosmos-full/
```

## Architecture

### Lightweight path (`scripts/` + `workloads/`) — primary

Uses a Rust workload runner (`cosmos-benchmark-workload`) with seven synthetic
calibrated workloads. A Python harness launches the COSMOS scheduler, runs
concurrent invocations, captures stats, and produces comparison-ready output.
This is the fastest path for day-to-day CFS-vs-COSMOS SLO comparison. No
Docker, OpenWhisk, or SeBS dependencies.

### Profiler path (`profiler/` + `third_party/`) — deep-trace + SeBS

`cosmos-bench-profiler` records deep resource traces (perf, cgroup, eBPF,
qdisc, lifecycle) for offline phase classification and profile DB generation.
Supports three invocation modes (burst, continuous, throughput) and runs
real SeBS benchmarks via language-specific runners (Python, Node.js).

A trace analyzer (`scripts/analyze_trace.py`) bridges profiler output to the
COSMOS SLO comparison pipeline, producing `compat_summary.json` files usable
by `compare.py` with the same p50/p95/p99/SLO/fairness metrics.

See `profiler/README.md` for full documentation.

## Entry point

`burst_benchmark.py` is the single dispatcher.  It routes:

| config              | runner          | needs root |
|---------------------|-----------------|------------|
| `cfs-default`       | `run_baseline.py` | no       |
| `cosmos-heuristic`  | `run_cosmos.py`   | yes      |
| `cosmos-metadata`   | `run_cosmos.py`   | yes      |
| `cosmos-pooled`     | `run_cosmos.py`   | yes      |
| `cosmos-full`       | `run_cosmos.py`   | yes      |
| `sfs`               | `run_cosmos.py`   | yes      |

## Workloads

Seven synthetic workloads, all compiled to a single Rust binary
(`target/release/cosmos-benchmark-workload`, crate `cosmos-benchmark-workload`).
The harness auto-builds the binary if sources are newer.

Each workload runs 250 ms of calibrated work by default and uses a
2x-duration deadline.  Override with `--duration-ms` and `--deadline-us`.

| workload            | description |
|---------------------|-------------|
| `cpu_burst`         | calibrated blocked-matrix-multiply CPU burst |
| `sleep_short`       | minimal baseline (thread sleep) |
| `io_mixed`          | CPU + synchronous file write/read/checksum |
| `memory_heavy`      | large-buffer strided write + chunk scan |
| `network_heavy`     | loopback TCP echo over an in-process server |
| `compression_mixed` | repeated RLE compress/decompress roundtrips |
| `graph_bfs`         | BFS over a deterministic irregular graph |

Workloads use fixed-cost calibration (comparable to SeBS profiles) rather than
clock-polling hot loops.

## Options

`burst_benchmark.py` accepts:

```
--config           required, one of the five configs above
--workload         required, one of the seven workload names
--concurrency      number of parallel invocations (default 1)
--duration-ms      per-invocation work budget (default 250)
--deadline-us      SLO deadline in microseconds
--out-dir          custom results output directory
--scheduler-bin    path to the COSMOS scheduler binary
--stats-socket     scheduler scx_stats socket path
--scheduler-flag   extra flag passed through to the scheduler
```

## Results

Each run produces a timestamped directory under `results/<config>/`
(e.g. `results/cosmos-full/20260525T042929Z/`) containing:

```
manifest.json              — config, workload, concurrency, flags
client_latency.csv         — per-invocation wall-clock durations
invocations/               — per-invocation JSON + stderr
scheduler_stats.jsonl      — scx_stats samples at 100 ms intervals
summary.json               — aggregated latency, load, scheduler counters
scheduler.log              — scheduler stdout
event_bridge.log           — metadata event-bridge log (COSMOS runs only)
```

A `latest` symlink points at the most recent run under each config.

## Compare two runs

```sh
python3 benchmarks/scripts/compare.py <baseline-dir> <candidate-dir>
```

The comparison prints a full metric table (p50, p95, p99, mean, SLO violations)
plus a capacity-fairness verdict.

## Fair-case judgment

COSMOS should be judged on cases where the host has enough CPU to make the
SLO feasible.  A run is `unfair-overloaded` when

```
total_compute_ms >= deadline_ms * cpu_cores
```

`compare.py` still prints metrics for unfair runs but its verdict says not to
use them for p99/SLO judgment.

Fair load classes:

- `underloaded`        ratio < 0.80
- `full`               0.80 ≤ ratio < 0.95
- `slightly-overloaded` 0.95 ≤ ratio < 1.00
- `unfair-overloaded`  ratio ≥ 1.00

CPU demand is measured from `/usr/bin/time` when available; otherwise it falls
back to `duration_ms * concurrency`.

## Build target dir

If your cargo target directory is not the project-local `target/` (common on
CloudLab machines), create a symlink before running:

```sh
ln -sf /path/to/cargo-target-dir target
```

The harness scripts resolve `target/release/cosmos-benchmark-workload` and
`target/release/cosmos` relative to the repo root.

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

## Older profiler stack

`profiler/` contains a Rust standalone profiling harness for offline resource
trace analysis (SeBS workloads under OpenWhisk or cgroup-standalone mode).
See `profiler/README.md` and `plan.md` for details.
