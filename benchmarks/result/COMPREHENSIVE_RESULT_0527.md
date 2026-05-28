# COSMOS Comprehensive Benchmark Report

**Date:** 2026-05-27 | **Host:** amd002 (64c AMD EPYC 7452)

## Environment

| Detail | Value |
|--------|-------|
| Kernel | 7.0.9-070009-generic |
| CPU | 64 logical cores |
| Workload duration | 250ms per invocation |
| SLO deadline | 500ms (2x duration) |
| Schedulers compared | CFS (baseline), SFS, COSMOS-heuristic, COSMOS-full |
| Workloads | 7 synthetic workloads x 2 concurrency levels = 14 configs x 4 schedulers = 56 runs |

---

## SLO Violation Summary

| Workload | Concurrency | CFS | SFS | COSMOS-heuristic | COSMOS-full |
|---|---:|---:|---:|---:|---:|
| cpu_burst | 64 | 0 | 0 | 0 | 0 |
| cpu_burst | 128 | 65 | 23 | 57 | 30 |
| sleep_short | 64 | 0 | 0 | 0 | 0 |
| sleep_short | 128 | 0 | 0 | 0 | 0 |
| io_mixed | 32 | 0 | 0 | 0 | 0 |
| io_mixed | 64 | 0 | 0 | 0 | 0 |
| memory_heavy | 32 | 0 | 0 | 0 | 0 |
| memory_heavy | 64 | 0 | 0 | 0 | 0 |
| network_heavy | 32 | 0 | 0 | 0 | 0 |
| network_heavy | 64 | 0 | 0 | 34 | 0 |
| compression_mixed | 64 | 0 | 0 | 0 | 0 |
| compression_mixed | 128 | 1 | 0 | 44 | 29 |
| graph_bfs | 32 | 0 | 0 | 0 | 0 |
| graph_bfs | 64 | 0 | 0 | 0 | 0 |

---

## cpu_burst — CPU Burst (matrix multiply)

### concurrency = 64

| Config | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | SLO viol. | Δ p50 vs CFS | Δ p99 vs CFS |
|---|---:|---:|---:|---:|---:|---|---|
| **cfs-default** | 275.60 | 386.09 | 390.42 | 296.45 | 0 | — | — |
| **sfs** | 266.39 | 269.90 | 271.54 | 266.50 | 0 | -9.2 (-3.3%) | -118.9 (-30.4%) |
| **cosmos-heuristic** | 282.01 | 388.37 | 393.00 | 300.55 | 0 | +6.4 (+2.3%) | +2.6 (+0.7%) |
| **cosmos-full** | 266.94 | 269.88 | 270.63 | 266.98 | 0 | -8.7 (-3.1%) | -119.8 (-30.7%) |

**Load:** ratio=0.57 | class=underloaded | fair=True
**Compute:** total_cpu=18340.00ms | per-invocation mean=286.56ms

### concurrency = 128

| Config | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | SLO viol. | Δ p50 vs CFS | Δ p99 vs CFS |
|---|---:|---:|---:|---:|---:|---|---|
| **cfs-default** | 500.40 | 613.44 | 652.28 | 491.77 | 65 | — | — |
| **sfs** | 461.29 | 577.40 | 650.45 | 478.80 | 23 | -39.1 (-7.8%) | -1.8 (-0.3%) |
| **cosmos-heuristic** | 429.85 | 597.81 | 624.59 | 468.84 | 57 | -70.5 (-14.1%) | -27.7 (-4.2%) |
| **cosmos-full** | 439.75 | 613.86 | 644.99 | 462.67 | 30 | -60.6 (-12.1%) | -7.3 (-1.1%) |

**Load:** ratio=1.09 | class=unfair-overloaded | fair=False
**Compute:** total_cpu=34770.00ms | per-invocation mean=271.64ms

## sleep_short — Sleep Short (idle baseline)

### concurrency = 64

| Config | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | SLO viol. | Δ p50 vs CFS | Δ p99 vs CFS |
|---|---:|---:|---:|---:|---:|---|---|
| **cfs-default** | 252.87 | 264.39 | 265.64 | 254.93 | 0 | — | — |
| **sfs** | 255.00 | 258.37 | 259.72 | 255.23 | 0 | +2.1 (+0.8%) | -5.9 (-2.2%) |
| **cosmos-heuristic** | 252.91 | 270.98 | 272.27 | 255.70 | 0 | +0.0 (+0.0%) | +6.6 (+2.5%) |
| **cosmos-full** | 255.08 | 257.47 | 258.03 | 255.12 | 0 | +2.2 (+0.9%) | -7.6 (-2.9%) |

**Load:** ratio=0.00 | class=underloaded | fair=True
**Compute:** total_cpu=0.00ms | per-invocation mean=0.00ms

### concurrency = 128

| Config | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | SLO viol. | Δ p50 vs CFS | Δ p99 vs CFS |
|---|---:|---:|---:|---:|---:|---|---|
| **cfs-default** | 275.23 | 291.29 | 293.17 | 269.97 | 0 | — | — |
| **sfs** | 257.61 | 263.57 | 265.16 | 258.00 | 0 | -17.6 (-6.4%) | -28.0 (-9.6%) |
| **cosmos-heuristic** | 273.41 | 293.70 | 297.02 | 270.26 | 0 | -1.8 (-0.7%) | +3.8 (+1.3%) |
| **cosmos-full** | 257.74 | 263.20 | 267.11 | 258.02 | 0 | -17.5 (-6.4%) | -26.1 (-8.9%) |

**Load:** ratio=0.00 | class=underloaded | fair=True
**Compute:** total_cpu=0.00ms | per-invocation mean=0.00ms

## io_mixed — IO Mixed (file read/write/checksum)

### concurrency = 32

| Config | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | SLO viol. | Δ p50 vs CFS | Δ p99 vs CFS |
|---|---:|---:|---:|---:|---:|---|---|
| **cfs-default** | 237.63 | 262.34 | 262.81 | 238.27 | 0 | — | — |
| **sfs** | 268.85 | 270.13 | 270.38 | 268.89 | 0 | +31.2 (+13.1%) | +7.6 (+2.9%) |
| **cosmos-heuristic** | 292.95 | 303.81 | 304.19 | 282.53 | 0 | +55.3 (+23.3%) | +41.4 (+15.7%) |
| **cosmos-full** | 248.19 | 249.24 | 249.51 | 240.72 | 0 | +10.6 (+4.4%) | -13.3 (-5.1%) |

**Load:** ratio=0.02 | class=underloaded | fair=True
**Compute:** total_cpu=550.00ms | per-invocation mean=17.19ms

### concurrency = 64

| Config | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | SLO viol. | Δ p50 vs CFS | Δ p99 vs CFS |
|---|---:|---:|---:|---:|---:|---|---|
| **cfs-default** | 340.25 | 349.20 | 350.33 | 335.64 | 0 | — | — |
| **sfs** | 273.11 | 283.09 | 283.67 | 273.64 | 0 | -67.1 (-19.7%) | -66.7 (-19.0%) |
| **cosmos-heuristic** | 414.43 | 458.91 | 471.04 | 404.47 | 0 | +74.2 (+21.8%) | +120.7 (+34.5%) |
| **cosmos-full** | 270.17 | 278.91 | 280.13 | 269.17 | 0 | -70.1 (-20.6%) | -70.2 (-20.0%) |

**Load:** ratio=0.03 | class=underloaded | fair=True
**Compute:** total_cpu=1040.00ms | per-invocation mean=16.25ms

## memory_heavy — Memory Heavy (strided scan)

### concurrency = 32

| Config | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | SLO viol. | Δ p50 vs CFS | Δ p99 vs CFS |
|---|---:|---:|---:|---:|---:|---|---|
| **cfs-default** | 283.44 | 303.73 | 306.22 | 285.01 | 0 | — | — |
| **sfs** | 266.60 | 287.88 | 289.93 | 259.57 | 0 | -16.8 (-5.9%) | -16.3 (-5.3%) |
| **cosmos-heuristic** | 274.60 | 361.78 | 369.08 | 281.02 | 0 | -8.8 (-3.1%) | +62.9 (+20.5%) |
| **cosmos-full** | 248.92 | 311.26 | 346.41 | 251.36 | 0 | -34.5 (-12.2%) | +40.2 (+13.1%) |

**Load:** ratio=0.27 | class=underloaded | fair=True
**Compute:** total_cpu=8630.00ms | per-invocation mean=269.69ms

### concurrency = 64

| Config | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | SLO viol. | Δ p50 vs CFS | Δ p99 vs CFS |
|---|---:|---:|---:|---:|---:|---|---|
| **cfs-default** | 337.61 | 428.02 | 460.52 | 340.88 | 0 | — | — |
| **sfs** | 302.38 | 305.45 | 305.78 | 298.96 | 0 | -35.2 (-10.4%) | -154.7 (-33.6%) |
| **cosmos-heuristic** | 338.60 | 422.77 | 445.77 | 344.18 | 0 | +1.0 (+0.3%) | -14.8 (-3.2%) |
| **cosmos-full** | 304.75 | 306.84 | 307.34 | 301.20 | 0 | -32.9 (-9.7%) | -153.2 (-33.3%) |

**Load:** ratio=0.62 | class=underloaded | fair=True
**Compute:** total_cpu=19990.00ms | per-invocation mean=312.34ms

## network_heavy — Network Heavy (loopback TCP)

### concurrency = 32

| Config | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | SLO viol. | Δ p50 vs CFS | Δ p99 vs CFS |
|---|---:|---:|---:|---:|---:|---|---|
| **cfs-default** | 262.93 | 273.67 | 280.93 | 259.78 | 0 | — | — |
| **sfs** | 145.84 | 186.71 | 194.98 | 155.06 | 0 | -117.1 (-44.5%) | -86.0 (-30.6%) |
| **cosmos-heuristic** | 267.05 | 279.41 | 282.52 | 263.47 | 0 | +4.1 (+1.6%) | +1.6 (+0.6%) |
| **cosmos-full** | 123.50 | 148.38 | 154.84 | 126.99 | 0 | -139.4 (-53.0%) | -126.1 (-44.9%) |

**Load:** ratio=0.40 | class=underloaded | fair=True
**Compute:** total_cpu=12690.00ms | per-invocation mean=396.56ms

### concurrency = 64

| Config | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | SLO viol. | Δ p50 vs CFS | Δ p99 vs CFS |
|---|---:|---:|---:|---:|---:|---|---|
| **cfs-default** | 294.26 | 336.86 | 347.56 | 285.28 | 0 | — | — |
| **sfs** | 253.11 | 257.77 | 260.62 | 250.90 | 0 | -41.1 (-14.0%) | -86.9 (-25.0%) |
| **cosmos-heuristic** | 504.05 | 557.53 | 569.33 | 465.77 | 34 | +209.8 (+71.3%) | +221.8 (+63.8%) |
| **cosmos-full** | 268.80 | 273.11 | 276.32 | 267.64 | 0 | -25.5 (-8.7%) | -71.2 (-20.5%) |

**Load:** ratio=0.57 | class=underloaded | fair=True
**Compute:** total_cpu=18390.00ms | per-invocation mean=287.34ms

## compression_mixed — Compression Mixed (RLE)

### concurrency = 64

| Config | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | SLO viol. | Δ p50 vs CFS | Δ p99 vs CFS |
|---|---:|---:|---:|---:|---:|---|---|
| **cfs-default** | 284.12 | 372.96 | 375.89 | 300.85 | 0 | — | — |
| **sfs** | 267.86 | 273.10 | 280.23 | 266.23 | 0 | -16.3 (-5.7%) | -95.7 (-25.4%) |
| **cosmos-heuristic** | 287.36 | 374.04 | 383.27 | 304.31 | 0 | +3.2 (+1.1%) | +7.4 (+2.0%) |
| **cosmos-full** | 269.49 | 273.69 | 276.10 | 268.64 | 0 | -14.6 (-5.1%) | -99.8 (-26.5%) |

**Load:** ratio=0.58 | class=underloaded | fair=True
**Compute:** total_cpu=18460.00ms | per-invocation mean=288.44ms

### concurrency = 128

| Config | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | SLO viol. | Δ p50 vs CFS | Δ p99 vs CFS |
|---|---:|---:|---:|---:|---:|---|---|
| **cfs-default** | 310.56 | 458.16 | 494.11 | 318.97 | 1 | — | — |
| **sfs** | 318.25 | 414.15 | 463.42 | 317.84 | 0 | +7.7 (+2.5%) | -30.7 (-6.2%) |
| **cosmos-heuristic** | 437.01 | 645.39 | 670.01 | 464.82 | 44 | +126.4 (+40.7%) | +175.9 (+35.6%) |
| **cosmos-full** | 447.66 | 608.70 | 652.73 | 471.52 | 29 | +137.1 (+44.1%) | +158.6 (+32.1%) |

**Load:** ratio=0.74 | class=underloaded | fair=True
**Compute:** total_cpu=23640.00ms | per-invocation mean=184.69ms

## graph_bfs — Graph BFS (irregular traversal)

### concurrency = 32

| Config | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | SLO viol. | Δ p50 vs CFS | Δ p99 vs CFS |
|---|---:|---:|---:|---:|---:|---|---|
| **cfs-default** | 267.94 | 269.54 | 271.21 | 267.90 | 0 | — | — |
| **sfs** | 265.49 | 336.79 | 357.29 | 266.18 | 0 | -2.4 (-0.9%) | +86.1 (+31.7%) |
| **cosmos-heuristic** | 265.69 | 268.88 | 269.32 | 265.33 | 0 | -2.2 (-0.8%) | -1.9 (-0.7%) |
| **cosmos-full** | 263.78 | 359.65 | 387.40 | 269.17 | 0 | -4.2 (-1.6%) | +116.2 (+42.8%) |

**Load:** ratio=0.26 | class=underloaded | fair=True
**Compute:** total_cpu=8320.00ms | per-invocation mean=260.00ms

### concurrency = 64

| Config | p50 (ms) | p95 (ms) | p99 (ms) | mean (ms) | SLO viol. | Δ p50 vs CFS | Δ p99 vs CFS |
|---|---:|---:|---:|---:|---:|---|---|
| **cfs-default** | 283.12 | 364.69 | 369.12 | 294.48 | 0 | — | — |
| **sfs** | 269.32 | 273.67 | 274.85 | 267.83 | 0 | -13.8 (-4.9%) | -94.3 (-25.5%) |
| **cosmos-heuristic** | 273.39 | 360.16 | 363.62 | 288.40 | 0 | -9.7 (-3.4%) | -5.5 (-1.5%) |
| **cosmos-full** | 268.16 | 364.90 | 379.48 | 288.87 | 0 | -15.0 (-5.3%) | +10.4 (+2.8%) |

**Load:** ratio=0.56 | class=underloaded | fair=True
**Compute:** total_cpu=18030.00ms | per-invocation mean=281.72ms

---
## Head-to-Head: Best Scheduler per Workload

| Workload | Concurrency | Best p50 | Best p50 Config | Best p99 | Best p99 Config | Best SLO | Best SLO Config |
|---|---|---|---|---|---|---|---|
| cpu_burst | 64 | 266.4 | sfs | 270.6 | cosmos-full | 0 | cfs-default |
| cpu_burst | 128 | 429.9 | cosmos-heuristic | 624.6 | cosmos-heuristic | 23 | sfs |
| sleep_short | 64 | 252.9 | cfs-default | 258.0 | cosmos-full | 0 | cfs-default |
| sleep_short | 128 | 257.6 | sfs | 265.2 | sfs | 0 | cfs-default |
| io_mixed | 32 | 237.6 | cfs-default | 249.5 | cosmos-full | 0 | cfs-default |
| io_mixed | 64 | 270.2 | cosmos-full | 280.1 | cosmos-full | 0 | cfs-default |
| memory_heavy | 32 | 248.9 | cosmos-full | 289.9 | sfs | 0 | cfs-default |
| memory_heavy | 64 | 302.4 | sfs | 305.8 | sfs | 0 | cfs-default |
| network_heavy | 32 | 123.5 | cosmos-full | 154.8 | cosmos-full | 0 | cfs-default |
| network_heavy | 64 | 253.1 | sfs | 260.6 | sfs | 0 | cfs-default |
| compression_mixed | 64 | 267.9 | sfs | 276.1 | cosmos-full | 0 | cfs-default |
| compression_mixed | 128 | 310.6 | cfs-default | 463.4 | sfs | 0 | sfs |
| graph_bfs | 32 | 263.8 | cosmos-full | 269.3 | cosmos-heuristic | 0 | cfs-default |
| graph_bfs | 64 | 268.2 | cosmos-full | 274.9 | sfs | 0 | cfs-default |

---
## Key Findings

- **cpu_burst@64** (ratio=0.57, underloaded): `cosmos-full` reduces p99 tail latency by 30.7% vs CFS (all 0 SLO violations)
- **cpu_burst@128** (ratio=1.09, overloaded): `sfs` reduces SLO violations by 65% vs CFS (65 → 23)
- **sleep_short@64** (ratio=0.00, underloaded): `cosmos-full` reduces p99 tail latency by 2.9% vs CFS (all 0 SLO violations)
- **sleep_short@128** (ratio=0.00, underloaded): `sfs` reduces p99 tail latency by 9.6% vs CFS (all 0 SLO violations)
- **io_mixed@32** (ratio=0.02, underloaded): `cosmos-full` reduces p99 tail latency by 5.1% vs CFS (all 0 SLO violations)
- **io_mixed@64** (ratio=0.03, underloaded): `cosmos-full` reduces p99 tail latency by 20.0% vs CFS (all 0 SLO violations)
- **memory_heavy@32** (ratio=0.27, underloaded): `sfs` reduces p99 tail latency by 5.3% vs CFS (all 0 SLO violations)
- **memory_heavy@64** (ratio=0.62, underloaded): `sfs` reduces p99 tail latency by 33.6% vs CFS (all 0 SLO violations)
- **network_heavy@32** (ratio=0.40, underloaded): `cosmos-full` reduces p99 tail latency by 44.9% vs CFS (all 0 SLO violations)
- **network_heavy@64** (ratio=0.57, underloaded): `sfs` reduces p99 tail latency by 25.0% vs CFS (all 0 SLO violations)
- **compression_mixed@64** (ratio=0.58, underloaded): `cosmos-full` reduces p99 tail latency by 26.5% vs CFS (all 0 SLO violations)
- **compression_mixed@128** (ratio=0.74, overloaded): `sfs` reduces SLO violations by 100% vs CFS (1 → 0)
- **graph_bfs@32** (ratio=0.26, underloaded): `cosmos-heuristic` reduces p99 tail latency by 0.7% vs CFS (all 0 SLO violations)
- **graph_bfs@64** (ratio=0.56, underloaded): `sfs` reduces p99 tail latency by 25.5% vs CFS (all 0 SLO violations)

---

## Part 2: Throughput — CFS vs SFS vs COSMOS

Sustained throughput test: fire invocations as fast as possible for 10 seconds,
max 128 concurrent. 250ms work per invocation, 500ms SLO deadline.

| Workload | Config | Throughput (inv/s) | vs CFS | p50 (ms) | p99 (ms) | SLO rate |
|---|---|---|---|---|---|---|
| **cpu_burst** | CFS | 201.5 | — | 484.6 | 684.8 | **40.6%** |
| (pure CPU)    | **SFS** | **232.4** | **+15.4%** | 308.0 | 643.6 | 4.7% |
|               | COSMOS-full | 233.3 | +15.8% | 337.2 | 675.0 | 7.3% |
| **network_heavy** | **CFS** | **259.8** | — | 300.0 | 525.1 | **1.7%** |
| (loopback TCP)   | SFS | 182.1 | -29.9% | 450.1 | 761.6 | 30.0% |
|                  | COSMOS-full | 182.5 | -29.8% | 433.3 | 676.6 | 17.9% |
| **compression** | **CFS** | **265.8** | — | 294.1 | 504.3 | **1.1%** |
| (RLE)           | SFS | 241.5 | -9.2% | 296.3 | 624.5 | 4.4% |
|                 | COSMOS-full | 231.3 | -13.0% | 334.5 | 693.7 | 6.0% |
| **io_mixed** | CFS | 260.7 | — | 305.8 | 564.8 | 6.2% |
| (file I/O)    | SFS | 251.4 | -3.6% | 311.7 | 601.0 | 4.9% |
|               | COSMOS-full | 250.8 | -3.8% | 320.6 | 580.1 | 5.8% |

### Throughput Key Findings

1. **CPU-bound workloads: COSMOS/SFS win.** +15% throughput on cpu_burst with
   SLO violations dropping from 40.6% (CFS) to 4.7% (SFS). Scheduler fairness
   isolation prevents CPU monopolization under contention.

2. **I/O-bound workloads: CFS dominates.** On network_heavy (loopback TCP), CFS
   achieves 259.8 inv/s while SFS/COSMOS drop to ~182 inv/s (-30%). The
   sched_ext user-space dispatch path adds per-wakeup overhead that penalizes
   I/O-heavy workloads where tasks spend most time blocked on sockets. CFS
   handles I/O scheduling natively in kernel without this overhead.

3. **Mixed/CPU workloads (compression, file I/O): CFS holds moderate edge.** 3-13%
   higher throughput than COSMOS, but SFS has slightly better SLO rates on
   io_mixed (4.9% vs 6.2% CFS). The sched_ext dispatch tax outweighs
   scheduling precision when tasks aren't purely CPU-bound.

4. **SFS vs COSMOS-full throughput**: For CPU-bound, COSMOS-full edges SFS
   (233.3 vs 232.4 inv/s). For I/O-bound both regress equally. Throughput
   between the two is near-identical across all workloads.

---

## Part 3: SeBS + OpenWhisk Integration

OpenWhisk standalone on Docker, Node.js 20 prewarmed containers. Each action
invoked 10x via `wsk action invoke --blocking`. Cold-start cleared between
scheduler switches (containers deleted). Measurements include full OpenWhisk
activation lifecycle (API → Docker → container → response).

| Action | CFS p50/p95/p99 | SFS p50/p95/p99 | COSMOS-full p50/p95/p99 |
|---|---|---|---|
| **ow_cpu_burn** (Node.js, prewarmed) | 246/302/302ms | 247/247/247ms | 247/249/249ms |
| **ow_sleep_slow** (Node.js, prewarmed) | 247/269/269ms | 246/251/251ms | 247/251/251ms |
| **sebs_cpu_burn** (Node.js, n=5M ops) | 64/89/89ms | 64/66/66ms | 64/65/65ms |

### OpenWhisk Key Findings

1. **No meaningful scheduler difference on OpenWhisk actions.** All three
   schedulers deliver near-identical p50 latency (~246ms for prewarmed
   containers). The OpenWhisk/Docker container lifecycle (activation dispatch,
   HTTP API, container attach) dominates invocation time at 200-250ms.

2. **Prewarmed containers mask scheduling effects.** OpenWhisk's prewarmed
   Node.js containers eliminate cold-start variance. The scheduler only
   matters for the `wsk` CLI process and Docker daemon, which are
   lightweight compared to the activation pipeline.

3. **Smaller work shows slight COSMOS advantage.** On sebs_cpu_burn (64ms
   work), COSMOS-full achieves 64/65ms p50/p99 vs CFS 64/89ms. The 24ms
   tail reduction (27%) suggests COSMOS helps when work is short and
   container overhead is proportionally larger.

4. **OpenWhisk's 1-invocation-per-action throttle** prevents throughput
   testing at scale. The standalone controller serializes concurrent
   requests to the same action (HTTP 429), making it impossible to stress
   the scheduler through OpenWhisk alone. Real serverless platforms (AWS
   Lambda, Azure Functions) would show different behavior with true
   multi-tenant scheduling.

5. **SeBS benchmarks deploy successfully** via `wsk action update` with
   `--kind nodejs:20` or `--kind python:3.11`. The `cosmos-bench-profiler
   open-whisk` mode captures full cgroup + Docker + activation traces for
   offline analysis.

---

*Generated by comprehensive benchmarking pipeline on amd002 (64 cores).*
*56 burst runs + 12 throughput runs + 30 OpenWhisk SeBS invocations.*