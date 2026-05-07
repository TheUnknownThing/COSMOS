# COSMOS Benchmark Profiling Findings

## Summary

The SeBS workloads do exhibit phase-aware resource consumption, but the signal is workload dependent. The clearest candidates for a phase-aware resource co-scheduler are storage/network, media, inference, compression, and scientific functions. Very short web/template and sleep workloads mostly expose OpenWhisk lifecycle overhead rather than useful application phases.

All measurements below were collected through `cosmos-bench-profiler` on OpenWhisk standalone with blackbox Node.js actions, no runtime prewarm, and explicit cold/warm profiling. Cold paths include platform/container startup; warm paths reuse the action container.

On May 7, 2026, the prepared CloudLab host `amd027.utah.cloudlab.us` was also
verified against the full SeBS OpenWhisk matrix at both `test` and `small`
input sizes. All 15 benchmarks completed after adding the missing persistent
runtime pieces: ScyllaDB/Alternator for NoSQL, UDP/TCP helper servers for the
microbenchmarks, larger OpenWhisk action memory limits, and a 2048 MiB action
limit for `220.video-processing`.

Verification artifacts:

```text
/local/benchmarks/runs/full-sebs-verify-20260507-020118
/local/benchmarks/runs/full-sebs-small-20260507-025115
```

The `small` input pass completed with these corrected wall-clock wrapper times:

| Workload | Runtime | Result | Seconds |
| --- | --- | --- | ---: |
| `010.sleep` | nodejs 20 | ok | 4 |
| `020.network-benchmark` | python 3.11 | ok | 3 |
| `030.clock-synchronization` | python 3.11 | ok | 3 |
| `040.server-reply` | nodejs 20 | ok | 3 |
| `110.dynamic-html` | nodejs 20 | ok | 3 |
| `120.uploader` | nodejs 20 | ok | 4 |
| `130.crud-api` | nodejs 20 | ok | 4 |
| `210.thumbnailer` | nodejs 20 | ok | 3 |
| `220.video-processing` | python 3.11 | ok | 9 |
| `311.compression` | nodejs 20 | ok | 4 |
| `411.image-recognition` | python 3.11 | ok | 6 |
| `501.graph-pagerank` | python 3.11 | ok | 3 |
| `502.graph-mst` | python 3.11 | ok | 3 |
| `503.graph-bfs` | python 3.11 | ok | 3 |
| `504.dna-visualisation` | python 3.11 | ok | 11 |

## Cold vs Warm Behavior

| Workload | Cold client | Warm client | Cold/Warm | Cold wait | Warm wait | Cold run | Warm run | CPU cold/warm | Peak memory | IO |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `010.sleep` | 19.26s | 0.35s | 54.9x | 19.10s | 338ms | 140ms | 5ms | 188ms / 4ms | 16.7 MiB | 0 |
| `110.dynamic-html small` | 13.35s | 55ms | 242x | 13.11s | 37ms | 216ms | 7ms | 194ms / 13ms | 17.8 MiB | 0 |
| `210.thumbnailer` | 29.09s | 0.13s | 217.3x | 28.63s | 48ms | 450ms | 63ms | 452ms / 62ms | 45.9 MiB | 9.3 MiB cold |
| `311.compression` | 11.48s | 0.69s | 16.6x | 10.40s | 31ms | 1073ms | 648ms | 1363ms / 842ms | 64-90 MiB | 18.6 MiB cold |
| `120.uploader small` | 15.67s | 2.40s | 6.5x | 13.24s | 42ms | 2.41s | 2.35s | 770ms / 381ms | 48.6 MiB | 6.7 MiB writes |
| `220.video-processing small` | 1.32s | 0.59s | 2.2x | 173ms | 4ms | 1.14s | 584ms | 5.73s / 5.20s | 775-776 MiB | 76.1 MiB cold |

Cold invocations are dominated by OpenWhisk container setup and image/runtime startup. Warm invocations are the better source for application resource phases because they remove most platform wait.

## Cold-Start Investigation

The cold path measured here should be interpreted as: action image already exists, but no action container is currently serving the request. It is not intended to include SeBS/OpenWhisk image build time.

The current OpenWhisk blackbox/container path can still add unexpected image-management latency before the action container is visible to the profiler. In `DockerContainer.create`, OpenWhisk calls `docker pull` before `docker run` for user-provided images. For `latest` tags this pull is mandatory; for non-`latest` tags it is attempted and tolerated if it fails. With local SeBS images and no pushed registry image, this can make “cold start” include registry lookup/pull delay even though the local image is already built.

To isolate the 10-30s cold wait, the profiler now records:

- `docker_events.jsonl`: Docker daemon events beginning at invocation send time. This should show whether time is spent before `container create`, between `create` and `start`, or after `start`.
- `events.jsonl` now includes `container_discovered` with elapsed time since request send. If this appears near the end of the 18s wait, the delay is before the action container is observable.
- `host_cpu.csv`, `host_memory.csv`, `host_pressure.csv`, and `process_stats.csv`: host-wide pressure plus `openwhisk-standalone`, `dockerd`, `containerd`, `containerd-shim`, and `runc` CPU/IO samples during the whole blocking invocation.

For local SeBS images, the OpenWhisk standalone config now enables `whisk.runtimes.bypass-pull-for-local-images` and sets `local-image-prefix = "spcleth"`. This is intentionally the parsed Docker image prefix, not the full repository path: OpenWhisk parses `spcleth/serverless-benchmarks:tag` as prefix `spcleth`, name `serverless-benchmarks`. Using `spcleth/serverless-benchmarks` does not match and still triggers the remote pull attempt.

### Rerun: 2026-05-06

I reran the hot/cold path on OpenWhisk standalone with no prewarm containers, host networking, and an already-created blackbox SeBS action using the local image `spcleth/serverless-benchmarks:function.openwhisk.010.sleep.nodejs-20-x64-1.2.1`. The action was updated before measurement; profiler runs used `--skip-update --invoke-http`. Cold was forced by removing `wsk0_` action containers first. Hot reused the resident container from the cold run.

| Run | Client | OpenWhisk wait | Init | Run | Container observable | Docker path |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| cold `010.sleep` | 11.16s | 11.07s | 72ms | 78ms | 11.12s after send | `pull` attempted, then `run` |
| hot `010.sleep` | 52ms | 38ms | 0 | 4ms | 54ms after send | `unpause` only |

Rerun artifacts:

```text
/tmp/cosmos-rerun-runs/rerun-010-sleep-cold-1778058986518041620-37558
/tmp/cosmos-rerun-runs/rerun-010-sleep-hot-1778059014286221268-39932
```

The cold Docker event timeline shows `create` at about 8.59s after request send and `start` at about 11.08s. OpenWhisk logs explain the pre-create gap: the invoker ran `docker pull spcleth/serverless-benchmarks:function.openwhisk.010.sleep.nodejs-20-x64-1.2.1`, which failed after about 2.42s because the tag does not exist in the remote registry, then continued with the local image. The subsequent `docker run` took about 8.60s. Therefore the noisy cold path is mostly Docker image resolution/container materialization, not action init or user code.

The hot path stayed normal: OpenWhisk classified the container as `warmed`, issued `docker unpause`, completed unpause in about 29ms, and finished the activation in about 52ms client-observed time.

Host/process samples reinforce this split. During the cold collection window, `dockerd` accounted for most of the observed process work and wrote about 2.5 GiB according to `/proc/<pid>/io`; during the hot run it wrote only 16 KiB. These process counters include the profiler's post-response collection tail, so they are diagnostic rather than exact per-request accounting, but the difference is large enough to identify Docker as the resource-heavy component.

### Rerun with Local Pull Bypass: 2026-05-06

I reran the same hot/cold path after fixing the OpenWhisk local-image prefix to `spcleth`. This eliminated the `docker pull` attempt. The invoker log goes directly from `containerStart cold` to `docker run`, and `docker_events.jsonl` contains only `create`, `connect`, `start`, and the later `pause` for the cold run.

| Run | Client | OpenWhisk wait | Init | Run | Container observable | Docker path |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| cold `010.sleep`, pull bypassed | 8.43s | 8.12s | 274ms | 280ms | 8.13s after send | `run` only |
| hot `010.sleep`, pull bypassed | 55ms | 36ms | 0 | 4ms | 37ms after send | `unpause` only |

Rerun artifacts:

```text
/tmp/cosmos-rerun-runs/rerun-010-sleep-cold-localprefix-1778061306930319911-44522
/tmp/cosmos-rerun-runs/rerun-010-sleep-hot-localprefix-1778061332768961526-46285
```

The pull bypass removes about 2.7s from this cold measurement compared with the previous rerun (`11.16s` to `8.43s` client latency). The remaining cold wait is still abnormal for a trivial sleep function. OpenWhisk measured `docker run` at about `7.83s`; Docker events show container `create` about `6.41s` after request send and `start` about `8.10s` after send. This means the current image-ready cold path is dominated by Docker container materialization, including overlay2 layer/container setup and daemon work, not by function init or user code.

The resource samples still show the same heavy Docker signature after pull removal: `dockerd` used most of the observed process CPU and reported about 2.5 GiB of writes, while the hot path reported only 16 KiB of `dockerd` writes. Because the counter is `/proc/<pid>/io`, the number should be treated as a host/process diagnostic, but it is consistent with overlay2/container materialization being the expensive part.

To investigate the remaining cost, the next useful split is below OpenWhisk's single `docker run -d` command. Trace `dockerd`/`containerd` with tools such as `perf trace`, `bpftrace`/eBPF block and filesystem probes, or `strace -f -ttT` in a controlled standalone run, and compare with manual `docker create` followed by `docker start` for the same image. If `docker create` carries most of the cost, OpenWhisk cannot make image-ready cold starts fast without changing semantics to pre-create stopped containers, keep a small per-action warm/paused pool, or move away from heavyweight overlay2 materialization for this benchmark path.

## Phase Awareness

`311.compression` has the strongest phase signal. It downloads an object, performs compression, and uploads the result. The profiler sees sustained cache/memory-bound and CPU-bound windows, high instruction volume, and meaningful storage/network activity. This is the best benchmark in the current set for validating a phase-aware co-scheduler.

`120.uploader small` is also useful. Warm execution remains around 2.35s after platform wait disappears, and the action performs a real download from GitHub followed by upload to MinIO. It exposes storage and network pressure with moderate CPU use. Host-observed network traffic was about 39 MiB per run, but this counter is host-scoped and should not be treated as isolated per-action network use.

`220.video-processing small` is now a useful media benchmark once its action
memory is raised to 2048 MiB. The warm path runs for about 584ms with roughly
5.2s of aggregate CPU time and about 776 MiB peak memory. The profiler
classifies the warm windows as entirely cache/memory-bound. The cold path adds
page-cache and IO windows while downloading and materializing the video.

`411.image-recognition` and `504.dna-visualisation` are strong high-memory
signals. In the May 7 profiling batch, image recognition peaked around 608 MiB
and had a large cold/warm split after model/data load. DNA visualisation peaked
around 1.04 GiB and stayed expensive even when warm, making it a better
sustained co-scheduling stressor than the short graph kernels.

`130.crud-api` is primarily useful to validate NoSQL/storage integration and
short warm request behavior. Its cold path shows cache/memory-bound startup and
initialization, but the warm request is only about 15ms.

`210.thumbnailer` shows a strong cold IO/page-cache phase and a much shorter warm path. It is useful for studying cold cache and image-processing startup behavior, but warm execution is brief.

`110.dynamic-html small` is mostly compute/string generation, but the warm action is only a few milliseconds. It is useful for demonstrating cold-start amplification, not for rich phase scheduling.

`010.sleep` is not a good phase benchmark in the current Node.js implementation because the measured action does not produce meaningful resource pressure.

## Scheduler Implications

Use cold/warm state as an explicit scheduler feature. Cold-start wait dominates many invocations and is not captured well by cgroup-only action counters because much of that time occurs before the action container is discoverable.

Prefer warm-path application windows when training or validating phase-aware resource decisions. For co-scheduling, `compression`, `uploader small`, `220.video-processing small`, `411.image-recognition`, and `504.dna-visualisation` provide the most useful phase variation in the current OpenWhisk setup.

Treat network measurements cautiously. The current profiler records `network_host_bytes_observed` from host `/proc/net/dev`; this is useful for identifying that network activity occurred, but it is not model-safe for per-run attribution.

No CPU throttling was observed in these runs. COSMOS scheduler statistics were unavailable because `/var/run/scx/root/stats` was not connected in the test environment.

## Additional Benchmark Notes

The May 7 remote runtime fixes removed the previous OpenWhisk blockers for
`020.network-benchmark`, `030.clock-synchronization`, `040.server-reply`,
`130.crud-api`, `411.image-recognition`, and `504.dna-visualisation`.

`220.video-processing small` initially failed with ffmpeg under the benchmark's
512 MiB action limit. Raising only this benchmark's memory declaration to 2048
MiB made the same small input complete successfully. This is a benchmark
resource declaration issue, not a missing ffmpeg or storage dependency.

`110.dynamic-html large` still exceeds the OpenWhisk response size limit: the
response is about 2.09 MiB, above the 1 MiB limit. The `test` and `small`
inputs both succeed.

## Generated Artifacts

The extended scheduler-facing profile database was rebuilt at:

```text
/tmp/cosmos_profile_db_more.json
/local/benchmarks/runs/env-fix-profile-20260507-020303
/local/benchmarks/runs/video-small-profile-20260507-025548/profile_db.json
```

The May 7 remote profile batches add cold/warm profiles for `130.crud-api`,
`411.image-recognition`, `504.dna-visualisation`, and `220.video-processing
small`.
