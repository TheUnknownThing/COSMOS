# COSMOS Comprehensive Benchmark Report

**Date:** 2026-05-31 09:15:49

## Benchmark Configuration

- Window: 5400000ms (90 minutes)
- Scale: 45
- SLO Multiplier: 2.0x
- Calibration Repetitions: 5

## Benchmarks Run

### Remote OpenWhisk Benchmarks

- SLO Calibration
- Azure 2019 Direct Trace variants
- Azure 2019 SeBS Trace

### Local Harness Benchmarks

- Schedulers: cfs-default, sfs, cosmos-heuristic, cosmos-full
- Workloads: cpu_burst, sleep_short, io_mixed, memory_heavy, network_heavy, compression_mixed, graph_bfs
- Mixed co-scheduling scenarios: 6
- Ablation Experiments

## Results Location

All results are saved in: `benchmarks/results/comprehensive_remote_20260531_043541`

## Azure/OpenWhisk Results

| Replay | Invocations | Successes | Failures | Arrival avg/s | Peak 1s | SLO success | SLO goodput/s | Submit lag p99 | Post-submit p99 | Impossible deadlines |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| azure-2019-direct-balanced-rich-40-20260531T0825 | 40 | 40 | 0 | 0.35 | 22 | 100.0% | 0.35 | 0.90 | 1026.35 | 8 |
| azure-2019-direct-bounded-1000 | 1000 | 1000 | 0 | 8.52 | 151 | 22.1% | 1.88 | n/a | n/a | n/a |
| azure-2019-direct-bounded-500-rich-20260531T0822 | 500 | 500 | 0 | 153.55 | 151 | 23.0% | 17.13 | 8.81 | 3365.81 | 8 |
| azure-2019-direct-peak-rich-480-20260531T0836 | 480 | 480 | 0 | 4.09 | 193 | 61.3% | 2.50 | 82.26 | 2484.58 | 6 |
| azure-2019-direct-smoothed-rich-500-20260531T0836 | 500 | 500 | 0 | 4.24 | 147 | 69.0% | 2.92 | 2.17 | 2123.77 | 12 |
| azure-2019-direct-target-rich-40-20260531T0836 | 40 | 40 | 0 | 0.35 | 22 | 100.0% | 0.35 | 0.93 | 1051.91 | 8 |
| azure-2019-sebs-bounded-1000 | 832 | 832 | 0 | 6.94 | 53 | 60.7% | 4.18 | n/a | n/a | n/a |

Replay workload mixes:

- azure-2019-direct-balanced-rich-500: cpu_burst=20, network_heavy=20
- azure-2019-direct-bounded-1000: cpu_burst=35, network_heavy=940, pipeline=25
- azure-2019-direct-peak-rich-capped: network_heavy=480
- azure-2019-direct-smoothed-rich-capped: cpu_burst=20, network_heavy=480
- azure-2019-direct-target-rich-capped: cpu_burst=20, network_heavy=20
- azure-2019-sebs-bounded-1000: 010.sleep=313, 110.dynamic-html=182, 120.uploader=121, 210.thumbnailer=32, 220.video-processing=109, 311.compression=75

OpenWhisk action mixes:

- azure-2019-direct-balanced-rich-40-20260531T0825: ow_cpu_burst=20, ow_network_heavy=20
- azure-2019-direct-bounded-500-rich-20260531T0822: ow_cpu_burst=4, ow_network_heavy=495, ow_pipeline=1
- azure-2019-direct-peak-rich-480-20260531T0836: ow_network_heavy=480
- azure-2019-direct-smoothed-rich-500-20260531T0836: ow_cpu_burst=20, ow_network_heavy=480
- azure-2019-direct-target-rich-40-20260531T0836: ow_cpu_burst=20, ow_network_heavy=20

OpenWhisk numbers are end-to-end platform replay results. Interpret them with arrival burstiness, action mix, submit lag, and post-submit latency rather than as a scheduler-only throughput headline.

Rows with `rich` in the name are controlled bounded reruns with the current replay summarizer; capped replay artifacts are used to keep the OpenWhisk run inside practical concurrency and wall-clock limits.

### OpenWhisk Per-Workload SLO

| Replay | Workload | Attempts | Successes | SLO success | SLO goodput/s | p99 latency ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| azure-2019-direct-balanced-rich-40-20260531T0825 | cpu_burst | 20 | 20 | 100.0% | 0.18 | 1091.9 |
| azure-2019-direct-balanced-rich-40-20260531T0825 | network_heavy | 20 | 20 | 100.0% | 22.61 | 815.2 |
| azure-2019-direct-bounded-500-rich-20260531T0822 | cpu_burst | 4 | 4 | 50.0% | 0.74 | 2176.4 |
| azure-2019-direct-bounded-500-rich-20260531T0822 | network_heavy | 495 | 495 | 22.8% | 16.83 | 3330.7 |
| azure-2019-direct-bounded-500-rich-20260531T0822 | pipeline | 1 | 1 | 0.0% | 0.00 | 3417.9 |
| azure-2019-direct-peak-rich-480-20260531T0836 | network_heavy | 480 | 480 | 61.3% | 2.50 | 2484.6 |
| azure-2019-direct-smoothed-rich-500-20260531T0836 | cpu_burst | 20 | 20 | 95.0% | 0.17 | 1634.4 |
| azure-2019-direct-smoothed-rich-500-20260531T0836 | network_heavy | 480 | 480 | 67.9% | 2.76 | 2124.0 |
| azure-2019-direct-target-rich-40-20260531T0836 | cpu_burst | 20 | 20 | 100.0% | 0.18 | 1126.3 |
| azure-2019-direct-target-rich-40-20260531T0836 | network_heavy | 20 | 20 | 100.0% | 22.85 | 813.0 |

## Local Harness Comparison

Successful comparison summaries: 57. COSMOS-full p99 versus CFS: 11/14 lower, mean delta -12.7%.

| Workload | Conc | CFS p99 | SFS p99 | Heur p99 | Full p99 | Full vs CFS | Full vs SFS | CFS viol | SFS viol | Heur viol | Full viol |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| compression_mixed | 64 | 391.2 | 289.8 | 421.3 | 284.5 | -27.3% | -1.8% | 0 | 0 | 0 | 0 |
| compression_mixed | 128 | 516.9 | 578.4 | 552.0 | 347.0 | -32.9% | -40.0% | 3 | 45 | 4 | 0 |
| cpu_burst | 64 | 415.0 | 376.7 | 403.5 | 364.8 | -12.1% | -3.2% | 0 | 0 | 0 | 0 |
| cpu_burst | 128 | 710.4 | 709.3 | 490.3 | 755.4 | 6.3% | 6.5% | 98 | 105 | 1 | 122 |
| graph_bfs | 32 | 282.4 | 271.3 | 295.6 | 360.1 | 27.5% | 32.7% | 0 | 0 | 0 | 0 |
| graph_bfs | 64 | 363.8 | 305.0 | 375.5 | 328.2 | -9.8% | 7.6% | 0 | 0 | 0 | 0 |
| io_mixed | 32 | 262.4 | 254.4 | 291.8 | 279.2 | 6.4% | 9.7% | 0 | 0 | 0 | 0 |
| io_mixed | 64 | 405.6 | 271.1 | 373.9 | 265.0 | -34.7% | -2.2% | 0 | 0 | 0 | 0 |
| memory_heavy | 32 | 353.2 | 307.2 | 354.6 | 282.0 | -20.1% | -8.2% | 0 | 0 | 0 | 0 |
| memory_heavy | 64 | 510.8 | 367.7 | 480.9 | 364.7 | -28.6% | -0.8% | 1 | 0 | 0 | 0 |
| network_heavy | 32 | 321.0 | 278.2 | 351.7 | 279.6 | -12.9% | 0.5% | 0 | 0 | 0 | 0 |
| network_heavy | 64 | 348.5 | 257.5 | 451.4 | 253.2 | -27.3% | -1.7% | 0 | 0 | 0 | 0 |
| sleep_short | 64 | 263.8 | 258.0 | 270.3 | 262.8 | -0.4% | 1.9% | 0 | 0 | 0 | 0 |
| sleep_short | 128 | 303.5 | 266.6 | 299.8 | 265.8 | -12.4% | -0.3% | 0 | 0 | 0 | 0 |

## Ablations

Successful ablation summaries: 36. `p99 vs full` compares each ablation against the matching `cosmos-full` comparison run when available.

| Workload | Conc | Config | p99 ms | SLO viol | p99 vs full |
| --- | ---: | --- | ---: | ---: | ---: |
| compression_mixed | 128 | cosmos-metadata | 577.2 | 31 | 66.3% |
| compression_mixed | 64 | cosmos-metadata | 302.6 | 0 | 6.4% |
| cpu_burst | 128 | cosmos-metadata | 763.5 | 128 | 1.1% |
| cpu_burst | 64 | cosmos-metadata | 371.2 | 0 | 1.8% |
| network_heavy | 32 | cosmos-metadata | 254.7 | 0 | -8.9% |
| network_heavy | 64 | cosmos-metadata | 244.0 | 0 | -3.6% |
| compression_mixed | 128 | cosmos-no-phase-predict | 575.3 | 31 | 65.8% |
| compression_mixed | 64 | cosmos-no-phase-predict | 296.9 | 0 | 4.4% |
| cpu_burst | 128 | cosmos-no-phase-predict | 735.2 | 110 | -2.7% |
| cpu_burst | 64 | cosmos-no-phase-predict | 369.4 | 0 | 1.3% |
| network_heavy | 32 | cosmos-no-phase-predict | 220.3 | 0 | -21.2% |
| network_heavy | 64 | cosmos-no-phase-predict | 253.3 | 0 | 0.0% |
| compression_mixed | 128 | cosmos-pooled | 611.7 | 96 | 76.3% |
| compression_mixed | 64 | cosmos-pooled | 318.7 | 0 | 12.0% |
| cpu_burst | 128 | cosmos-pooled | 782.7 | 128 | 3.6% |
| cpu_burst | 64 | cosmos-pooled | 368.8 | 0 | 1.1% |
| network_heavy | 32 | cosmos-pooled | 228.4 | 0 | -18.3% |
| network_heavy | 64 | cosmos-pooled | 259.4 | 0 | 2.5% |
| compression_mixed | 128 | cosmos-slack+xres | 598.8 | 26 | 72.6% |
| compression_mixed | 64 | cosmos-slack+xres | 303.5 | 0 | 6.7% |
| cpu_burst | 128 | cosmos-slack+xres | 833.0 | 128 | 10.3% |
| cpu_burst | 64 | cosmos-slack+xres | 371.4 | 0 | 1.8% |
| network_heavy | 32 | cosmos-slack+xres | 271.7 | 0 | -2.8% |
| network_heavy | 64 | cosmos-slack+xres | 258.4 | 0 | 2.0% |
| compression_mixed | 128 | cosmos-slack+xres+phase | 625.8 | 22 | 80.3% |
| compression_mixed | 64 | cosmos-slack+xres+phase | 287.9 | 0 | 1.2% |
| cpu_burst | 128 | cosmos-slack+xres+phase | 1830.6 | 128 | 142.3% |
| cpu_burst | 64 | cosmos-slack+xres+phase | 371.6 | 0 | 1.9% |
| network_heavy | 32 | cosmos-slack+xres+phase | 213.9 | 0 | -23.5% |
| network_heavy | 64 | cosmos-slack+xres+phase | 262.4 | 0 | 3.6% |
| compression_mixed | 128 | cosmos-slack-only | 557.7 | 17 | 60.7% |
| compression_mixed | 64 | cosmos-slack-only | 292.6 | 0 | 2.9% |
| cpu_burst | 128 | cosmos-slack-only | 740.4 | 116 | -2.0% |
| cpu_burst | 64 | cosmos-slack-only | 373.7 | 0 | 2.4% |
| network_heavy | 32 | cosmos-slack-only | 177.3 | 0 | -36.6% |
| network_heavy | 64 | cosmos-slack-only | 267.6 | 0 | 5.7% |

## Mixed Co-Scheduling

| Scenario | Config | SLO class | p50 ms | p95 ms | p99 ms | SLO violations | Goodput/s | Batch slowdown | SLO boosts | Pool latency | Pool batch | Migrations | Scheduler stalls |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| lc-cpu_vs_batch-cpu_c96 | cfs-default | 0 | 270.4 | 370.5 | 418.1 | 17 | 54.01 | n/a | 0 | 0 | 0 | 0 | 0 |
| lc-cpu_vs_batch-cpu_c96 | cfs-default | 2 | 748.0 | 877.1 | 915.1 | 0 | 75.14 | 2.67 | 0 | 0 | 0 | 0 | 0 |
| lc-cpu_vs_batch-cpu_c96 | cosmos-full | 0 | 192.4 | 305.6 | 381.4 | 3 | 59.69 | n/a | 2300 | 108 | 501 | 1 | 0 |
| lc-cpu_vs_batch-cpu_c96 | cosmos-full | 2 | 830.9 | 893.3 | 955.6 | 0 | 73.36 | 4.04 | 2300 | 108 | 501 | 1 | 0 |
| lc-cpu_vs_batch-cpu_c96 | cosmos-pooled | 0 | 161.8 | 228.7 | 254.8 | 1 | 92.01 | n/a | 2489 | 115 | 466 | 1 | 0 |
| lc-cpu_vs_batch-cpu_c96 | cosmos-pooled | 2 | 847.5 | 909.0 | 954.6 | 0 | 75.20 | 4.90 | 2489 | 115 | 466 | 1 | 0 |
| lc-cpu_vs_batch-memory_c96 | cfs-default | 0 | 332.3 | 537.0 | 549.5 | 19 | 43.36 | n/a | 0 | 0 | 0 | 0 | 0 |
| lc-cpu_vs_batch-memory_c96 | cfs-default | 2 | 661.4 | 959.0 | 986.3 | 0 | 69.92 | 1.91 | 0 | 0 | 0 | 0 | 0 |
| lc-cpu_vs_batch-memory_c96 | cosmos-full | 0 | 252.3 | 457.9 | 470.8 | 14 | 50.66 | n/a | 1131 | 200 | 516 | 0 | 0 |
| lc-cpu_vs_batch-memory_c96 | cosmos-full | 2 | 561.4 | 749.9 | 820.4 | 0 | 76.86 | 1.82 | 1131 | 200 | 516 | 0 | 0 |
| lc-cpu_vs_batch-memory_c96 | cosmos-pooled | 0 | 290.9 | 583.9 | 608.1 | 23 | 39.07 | n/a | 702 | 191 | 623 | 0 | 0 |
| lc-cpu_vs_batch-memory_c96 | cosmos-pooled | 2 | 723.4 | 877.9 | 915.5 | 0 | 76.88 | 1.97 | 702 | 191 | 623 | 0 | 0 |
| lc-network_vs_batch-cpu_c96 | cfs-default | 0 | 179.3 | 225.6 | 233.1 | 0 | 100.35 | n/a | 0 | 0 | 0 | 0 | 0 |
| lc-network_vs_batch-cpu_c96 | cfs-default | 2 | 778.0 | 888.7 | 905.5 | 0 | 76.18 | 4.18 | 0 | 0 | 0 | 0 | 0 |
| lc-network_vs_batch-cpu_c96 | cosmos-full | 0 | 202.9 | 291.2 | 295.0 | 8 | 81.22 | n/a | 3843 | 318 | 418 | 1 | 0 |
| lc-network_vs_batch-cpu_c96 | cosmos-full | 2 | 941.8 | 1109.0 | 1208.8 | 0 | 59.21 | 5.16 | 3843 | 318 | 418 | 1 | 0 |
| lc-network_vs_batch-cpu_c96 | cosmos-pooled | 0 | 151.9 | 241.7 | 263.3 | 1 | 89.12 | n/a | 3143 | 257 | 459 | 0 | 0 |
| lc-network_vs_batch-cpu_c96 | cosmos-pooled | 2 | 883.7 | 1149.0 | 1239.1 | 0 | 56.68 | 6.34 | 3143 | 257 | 459 | 0 | 0 |
| lc-io_vs_batch-cpu_c96 | cfs-default | 0 | 179.3 | 205.6 | 208.4 | 0 | 112.47 | n/a | 0 | 0 | 0 | 0 | 0 |
| lc-io_vs_batch-cpu_c96 | cfs-default | 2 | 813.9 | 952.7 | 1007.1 | 0 | 68.87 | 4.43 | 0 | 0 | 0 | 0 | 0 |
| lc-io_vs_batch-cpu_c96 | cosmos-full | 0 | 223.6 | 242.7 | 244.7 | 0 | 97.84 | n/a | 1158 | 188 | 401 | 1 | 0 |
| lc-io_vs_batch-cpu_c96 | cosmos-full | 2 | 904.1 | 1108.1 | 1182.0 | 0 | 56.88 | 4.24 | 1158 | 188 | 401 | 1 | 0 |
| lc-sleep_vs_batch-memory_c96 | cfs-default | 0 | 98.7 | 197.6 | 244.3 | 2 | 91.45 | n/a | 0 | 0 | 0 | 0 | 0 |
| lc-sleep_vs_batch-memory_c96 | cfs-default | 2 | 854.3 | 992.0 | 1008.2 | 0 | 69.41 | 7.64 | 0 | 0 | 0 | 0 | 0 |
| lc-sleep_vs_batch-memory_c96 | cosmos-full | 0 | 141.0 | 404.1 | 458.4 | 9 | 50.98 | n/a | 78 | 69 | 266 | 0 | 0 |
| lc-sleep_vs_batch-memory_c96 | cosmos-full | 2 | 6474.4 | 6494.0 | 6496.4 | 66 | 11.08 | 34.29 | 78 | 69 | 266 | 0 | 0 |
| lc-standard-batch_three-class_c96 | cfs-default | 0 | 186.1 | 311.9 | 331.6 | 3 | 69.40 | n/a | 0 | 0 | 0 | 0 | 0 |
| lc-standard-batch_three-class_c96 | cfs-default | 1 | 406.2 | 445.7 | 450.1 | 0 | 51.83 | n/a | 0 | 0 | 0 | 0 | 0 |
| lc-standard-batch_three-class_c96 | cfs-default | 2 | 345.9 | 447.3 | 455.9 | 0 | 93.69 | 1.75 | 0 | 0 | 0 | 0 | 0 |
| lc-standard-batch_three-class_c96 | cosmos-full | 0 | 190.5 | 399.9 | 429.4 | 11 | 55.10 | n/a | 6638 | 437 | 472 | 0 | 0 |
| lc-standard-batch_three-class_c96 | cosmos-full | 1 | 293.1 | 462.3 | 470.2 | 0 | 50.81 | n/a | 6638 | 437 | 472 | 0 | 0 |
| lc-standard-batch_three-class_c96 | cosmos-full | 2 | 490.9 | 751.1 | 829.5 | 0 | 56.00 | 2.28 | 6638 | 437 | 472 | 0 | 0 |
| lc-standard-batch_three-class_c96 | cosmos-pooled | 0 | 228.4 | 320.4 | 330.1 | 11 | 72.29 | n/a | 7924 | 399 | 408 | 0 | 0 |
| lc-standard-batch_three-class_c96 | cosmos-pooled | 1 | 295.0 | 369.6 | 391.5 | 0 | 60.43 | n/a | 7924 | 399 | 408 | 0 | 0 |
| lc-standard-batch_three-class_c96 | cosmos-pooled | 2 | 374.5 | 575.3 | 712.9 | 0 | 66.19 | 1.85 | 7924 | 399 | 408 | 0 | 0 |
| lc-io_vs_batch-cpu_c96 | cosmos-pooled | 0 | 243.1 | 325.7 | 571.7 | 2 | 37.34 | n/a | 613 | 166 | 554 | 0 | 0 |
| lc-io_vs_batch-cpu_c96 | cosmos-pooled | 2 | 773.7 | 847.1 | 950.0 | 0 | 74.16 | 3.04 | 613 | 166 | 554 | 0 | 0 |
| lc-sleep_vs_batch-memory_c96 | cosmos-pooled | 0 | 80.4 | 602.5 | 658.5 | 6 | 35.55 | n/a | 176 | 69 | 356 | 0 | 0 |
| lc-sleep_vs_batch-memory_c96 | cosmos-pooled | 2 | 747.6 | 893.0 | 940.9 | 0 | 72.82 | 4.18 | 176 | 69 | 356 | 0 | 0 |

## Artifacts

- Aggregate JSON: `benchmarks/results/comprehensive_remote_20260531_043541/aggregate_summary.json`
- Local harness CSV: `benchmarks/results/comprehensive_remote_20260531_043541/local_harness_aggregate.csv`
- Mixed co-scheduling CSV: `benchmarks/results/comprehensive_remote_20260531_043541/local_harness/coscheduling_summary.csv`
