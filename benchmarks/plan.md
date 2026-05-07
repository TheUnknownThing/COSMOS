# COSMOS Benchmark Plan

## Summary
- Learn resource phases from traces, not workload names: offline profiling uses `perf`, cgroup, eBPF, qdisc, lifecycle, and client-side latency data.
- Use **OpenWhisk standalone + SeBS native OpenWhisk backend** as the main FaaS benchmark target.
- Keep a **standalone COSMOS profiling harness** for local/debug runs so profiling work is not blocked by OpenWhisk.
- Avoid Kubernetes, SeBS core changes, and OpenLambda integration unless OpenWhisk fails the initial 3-day OpenWhisk viability run.

## Key Changes
- Add benchmark tooling under `benchmarks/profiler/` using rust:
  - `configs/`: workload, event, sanity/profile/interference matrices.
  - `runners/`: standalone runner, OpenWhisk runner, interference launcher.
  - `collectors/`: cgroup sampler, `perf stat -G`, network/qdisc sampler, OpenWhisk activation collector, client latency collector.
  - `analysis/`: trace joiner, feature extraction, phase segmentation, profile DB generation.
- Produce one run directory containing:
  - `run_meta.json`, `events.jsonl`, `stdout.log`, `stderr.log`, `perf_stat.csv`
  - `cgroup_cpu.csv`, `cgroup_memory.csv`, `cgroup_io.csv`, `cgroup_pressure.csv`
  - `net.csv`, `qdisc.csv`, `scheduler_stats.csv`, `client_latency.csv`, `openwhisk_activation.json`, `summary.json`
- In standalone mode:
  - Create one cgroup per run.
  - Launch workload process/container inside that cgroup.
  - Emit synthetic invocation events matching the OpenWhisk/OpenLambda-style lifecycle.
- In OpenWhisk mode:
  - Collect activation records: activation id, action, start/end, duration, status, `waitTime`, `initTime`, limits.
  - Patch only the invoker if needed to emit `activation_id -> container_id -> host_pid -> cgroup_path`.

## Required Stats
- Client latency:
  - request send timestamp
  - first byte timestamp
  - response end timestamp
  - timeout/error/status
- Platform lifecycle:
  - activation/run id
  - queue wait
  - init time
  - run duration
  - cold/warm/prewarm classification
  - container id
  - host pid
  - cgroup path
  - reuse age
- COSMOS scheduler:
  - existing scheduler counters from `src/stats.rs`
  - per-window queued/scheduled/running counts
  - class counts: cold, hot, background
  - boost counts
  - dispatch failures/cancellations/bounces
  - scheduler congestion
  - later: sched_ext queue delay from enqueue/runnable to dispatch
- Per-cgroup resources:
  - `cpu.stat`: usage, user/system, throttling periods, throttled time
  - `memory.current`, `memory.peak`, `memory.stat`, `memory.events`, swap if enabled
  - `io.stat` and IO pressure
  - `cpu.pressure`, `memory.pressure`, `io.pressure`
- Perf counters:
  - cycles, instructions, cache references/misses
  - branches, branch misses
  - context switches, CPU migrations
  - page faults and major faults
- Network/storage:
  - container network-namespace rx/tx bytes where a container host pid is known
  - host veth rx/tx bytes and qdisc backlog/drops where veth mapping is available
  - host `/proc/net/dev` rx/tx bytes only as a diagnostic fallback
  - retransmits/RTT where available
  - MinIO/S3 request count, bytes, latency, status
- Run environment:
  - kernel version
  - CPU model/core count
  - RAM
  - CPU governor/frequency state
  - Docker/OpenWhisk/SeBS versions
  - COSMOS git commit
  - benchmark config hash

## Benchmark Matrices
- SeBS capability manifest:
  - `profiler/configs/sebs-capabilities.json` records OpenWhisk standalone
    support, local standalone adapter support, required services, and blockers.
  - `matrix --kind sebs-openwhisk` is the canonical FaaS benchmark set.
  - `matrix --kind sebs-standalone` is the local collector-debug set.
- Sanity matrix:
  - Workloads: `dynamic-html`, `thumbnailer`, `compression`, `image-recognition`
  - Inputs: `small`, `medium`
  - Warmth: `cold`, `warm`
  - Repetitions: `5`
  - Concurrency: `1`
- Profile matrix:
  - Workloads: `dynamic-html`, `uploader`, `thumbnailer`, `video-processing`, `compression`, `image-recognition`, `pagerank`, `bfs`
  - Inputs: `small`, `medium`, `large`
  - Warmth: `cold`, `warm`, `lukewarm`
  - Repetitions: `10`
  - Concurrency: `1`
- Interference matrix:
  - Targets: `thumbnailer`, `compression`, `image-recognition`, `uploader`
  - Interference: `none`, `cpu`, `memory`, `network`, `io`
  - Input: `medium`
  - Warmth: `warm`
  - Repetitions: `10`

## Analysis Outputs
- Per-run `summary.json` with:
  - latency decomposition: client latency, platform wait/init/run, resource windows
  - peak and aggregate CPU/memory/IO/network metrics
  - phase windows: `CPU_BOUND`, `CACHE_OR_MEM_BOUND`, `IO_PAGECACHE`, `NETWORK_WAIT`, `MIXED_UNKNOWN`
- Aggregated profile DB:
  - one profile per workload/input/warmth
  - median, p95, p99 latency
  - dominant resource phases
  - stable online-counter features for scheduler use
- Scheduler-facing rules must use cheap online signals, not live `perf record`.

## Test Plan
- Preflight:
  - cgroup v2 mounted
  - `perf stat` usable
  - Docker usable
  - OpenWhisk standalone starts
  - SeBS can run at least two OpenWhisk workloads
  - disk space and permissions are sufficient
- Integration:
  - run CPU, memory, IO, and network micro-workloads and confirm expected classification
  - run one standalone SeBS workload and produce all required files
  - run two OpenWhisk SeBS workloads and join activation id to client latency and resource trace
  - verify missing lifecycle/cgroup mappings fail the run clearly
- Acceptance:
  - every successful run has complete latency decomposition
  - every run has a stable run id joining client, platform, cgroup, perf, and scheduler data
  - phase extraction works from 50ms or 100ms windows

## Assumptions
- Main benchmark platform is OpenWhisk standalone.
- Standalone harness remains required for local testing and fallback.
- `actionConcurrency = 1`, one invoker, one host, and fixed prewarm policy are required.
- OpenLambda integration is paused unless OpenWhisk + SeBS cannot run two workloads locally within three days.
- P2-only additions, such as RAPL energy, NUMA locality, LLC occupancy, memory bandwidth, and selective flamegraphs, are deferred until the core trace pipeline is stable.
