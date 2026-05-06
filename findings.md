# COSMOS Benchmark Profiling Findings

## Summary

The SeBS workloads do exhibit phase-aware resource consumption, but the signal is workload dependent. The clearest candidates for a phase-aware resource co-scheduler are storage/network or compression-heavy functions, while very short web/template and sleep workloads mostly expose OpenWhisk lifecycle overhead rather than useful application phases.

All measurements below were collected through `cosmos-bench-profiler` on OpenWhisk standalone with blackbox Node.js actions, no runtime prewarm, and explicit cold/warm profiling. Cold paths include platform/container startup; warm paths reuse the action container.

## Cold vs Warm Behavior

| Workload | Cold client | Warm client | Cold/Warm | Cold wait | Warm wait | Cold run | Warm run | CPU cold/warm | Peak memory | IO |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `010.sleep` | 19.26s | 0.35s | 54.9x | 19.10s | 338ms | 140ms | 5ms | 188ms / 4ms | 16.7 MiB | 0 |
| `110.dynamic-html small` | 13.35s | 55ms | 242x | 13.11s | 37ms | 216ms | 7ms | 194ms / 13ms | 17.8 MiB | 0 |
| `210.thumbnailer` | 29.09s | 0.13s | 217.3x | 28.63s | 48ms | 450ms | 63ms | 452ms / 62ms | 45.9 MiB | 9.3 MiB cold |
| `311.compression` | 11.48s | 0.69s | 16.6x | 10.40s | 31ms | 1073ms | 648ms | 1363ms / 842ms | 64-90 MiB | 18.6 MiB cold |
| `120.uploader small` | 15.67s | 2.40s | 6.5x | 13.24s | 42ms | 2.41s | 2.35s | 770ms / 381ms | 48.6 MiB | 6.7 MiB writes |

Cold invocations are dominated by OpenWhisk container setup and image/runtime startup. Warm invocations are the better source for application resource phases because they remove most platform wait.

## Phase Awareness

`311.compression` has the strongest phase signal. It downloads an object, performs compression, and uploads the result. The profiler sees sustained cache/memory-bound and CPU-bound windows, high instruction volume, and meaningful storage/network activity. This is the best benchmark in the current set for validating a phase-aware co-scheduler.

`120.uploader small` is also useful. Warm execution remains around 2.35s after platform wait disappears, and the action performs a real download from GitHub followed by upload to MinIO. It exposes storage and network pressure with moderate CPU use. Host-observed network traffic was about 39 MiB per run, but this counter is host-scoped and should not be treated as isolated per-action network use.

`210.thumbnailer` shows a strong cold IO/page-cache phase and a much shorter warm path. It is useful for studying cold cache and image-processing startup behavior, but warm execution is brief.

`110.dynamic-html small` is mostly compute/string generation, but the warm action is only a few milliseconds. It is useful for demonstrating cold-start amplification, not for rich phase scheduling.

`010.sleep` is not a good phase benchmark in the current Node.js implementation because the measured action does not produce meaningful resource pressure.

## Scheduler Implications

Use cold/warm state as an explicit scheduler feature. Cold-start wait dominates many invocations and is not captured well by cgroup-only action counters because much of that time occurs before the action container is discoverable.

Prefer warm-path application windows when training or validating phase-aware resource decisions. For co-scheduling, `compression`, `uploader small`, and `thumbnailer` provide the most useful phase variation.

Treat network measurements cautiously. The current profiler records `network_host_bytes_observed` from host `/proc/net/dev`; this is useful for identifying that network activity occurred, but it is not model-safe for per-run attribution.

No CPU throttling was observed in these runs. COSMOS scheduler statistics were unavailable because `/var/run/scx/root/stats` was not connected in the test environment.

## Additional Benchmark Attempts

Python OpenWhisk benchmarks such as `501.graph-pagerank` and `220.video-processing` could not be completed in this environment because required Python build images and dependencies were unavailable or stalled during SeBS packaging.

`110.dynamic-html large` exceeded the OpenWhisk response size limit: the response was about 2.09 MiB, above the 1 MiB limit. The smaller input succeeds and was profiled.

Benchmarks such as `040.server-reply`, `020.network-benchmark`, `030.clock-synchronization`, and `130.crud-api` require additional external servers, UDP coordination, or NoSQL storage and were not suitable for the current standalone OpenWhisk setup.

## Generated Artifacts

The extended scheduler-facing profile database was rebuilt at:

```text
/tmp/cosmos_profile_db_more.json
```

It contains 18 complete run profiles.
