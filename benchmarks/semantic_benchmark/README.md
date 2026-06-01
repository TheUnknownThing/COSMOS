# Azure 2021 Semantic Benchmark

This package builds a trace-preserving Azure Functions 2021 benchmark IR and
then attaches SeBS semantic anchors plus executable duration realizations. The
intended executable path is real SeBS first, with synthesized controllable
kernels used only when no calibrated SeBS benchmark fits an invocation's
resource pattern and duration bucket.

Phase 2 trace IR:

```bash
python3 benchmarks/semantic_benchmark/build_trace_ir.py \
  --trace-2021 benchmarks/third_party/AzurePublicDataset/data/AzureFunctionsInvocationTraceForTwoWeeksJan2021.rar \
  --output-dir /tmp/cosmos-semantic/trace \
  --window-ms 60000 \
  --scale 2 \
  --limit 100 \
  --downsample-mode hash-functions
```

Phase 3/4 semantic assignment:

```bash
make -C benchmarks/semantic_benchmark/kernels

python3 benchmarks/semantic_benchmark/build_semantic_assignments.py \
  --trace-ir /tmp/cosmos-semantic/trace/trace_ir.json \
  --output-dir /tmp/cosmos-semantic/semantic \
  --semantic-mix balanced \
  --seed 123
```

Phase 5 calibration:

```bash
python3 benchmarks/semantic_benchmark/build_calibration.py \
  --output-dir /tmp/cosmos-semantic/calibration \
  --repetitions 5
```

The calibration step emits `calibration.json` with isolated warm p50/p95/p99,
cold-start samples, resource-counter availability, and OpenWhisk latency split
placeholders. Native controllable kernels are calibrated by this command. Real
SeBS actions are enabled by merging an upstream calibration artifact with
`--upstream-calibration`; that artifact must mark support for the exact
`sebs_anchor` and duration bucket.

Use the calibration artifact to gate assignment for SLO-capable replay:

```bash
python3 benchmarks/semantic_benchmark/build_semantic_assignments.py \
  --trace-ir /tmp/cosmos-semantic/trace/trace_ir.json \
  --output-dir /tmp/cosmos-semantic/semantic \
  --semantic-mix balanced \
  --seed 123 \
  --calibration /tmp/cosmos-semantic/calibration/calibration.json
```

Phase 6 replay generation:

```bash
python3 benchmarks/semantic_benchmark/build_replay.py \
  --semantic-assignments /tmp/cosmos-semantic/semantic/semantic_assignments.json \
  --output-dir /tmp/cosmos-semantic/replay
```

The replay step emits:

- `profiles.json`: OpenWhisk profile definitions and function profile mapping.
- `invocations.csv`: flat replay rows for inspection.
- `replay.json`: schedule consumed by the existing OpenWhisk replay driver.
- `fidelity.json`: replay-level preservation summary.

The semantic assignment step emits:

- `semantic_catalog.json`: SeBS anchor catalog and duration realization catalog.
- `semantic_assignments.json`: policy, seed, counts, per-function anchor mapping,
  and enriched per-invocation semantic metadata.
- `semantic_invocations.csv`: flat invocation rows for inspection and downstream
  tools.

With `--calibration`, assignment first tries `upstream-sebs-calibrated` for the
assigned SeBS anchor and target duration bucket. It falls back to the matching
controllable kernel only when no calibrated upstream SeBS fit exists. Without
calibration, assignment can only emit synthesized fallback rows unless
`--allow-uncalibrated-upstream` is used for exploratory inspection.

When `--calibration` is supplied, assignment fails if no calibrated realization
supports an invocation's anchor and target duration bucket. Use
`--unsupported-mode mark` only when you explicitly want replay artifacts that are
not valid for SLO conclusions; those rows carry
`calibration_supports_slo=false`.

Current executable workload provenance:

| Resource class | Preferred workload | Fallback workload | Fallback provenance |
| --- | --- | --- | --- |
| `cpu` | assigned SeBS anchor, for example `110.dynamic-html`, `311.compression`, `501.graph-pagerank`, `502.graph-mst`, or `503.graph-bfs` when calibrated | `cpu-spin-controllable` | synthesized C kernel |
| `io` | assigned SeBS anchor, for example `120.uploader` or `130.crud-api` when calibrated | `storage-io-controllable` | synthesized C kernel |
| `memory` | assigned SeBS anchor, for example `411.image-recognition` or `504.dna-visualisation` when calibrated | `memory-scan-controllable` | synthesized C kernel |
| `network` | assigned SeBS anchor, for example `020.network-benchmark`, `030.clock-synchronization`, or `040.server-reply` when calibrated | `network-transfer-controllable` | synthesized C kernel |
| `balanced` | assigned SeBS anchor, for example `010.sleep`, `210.thumbnailer`, or `220.video-processing` when calibrated | `balanced-pipeline-controllable` | synthesized C kernel |

The replay `workload` field is the actual selected workload: a SeBS benchmark
ID when `actual_workload_source=upstream-sebs`, otherwise the synthesized
fallback realization ID.

Register synthesized fallback kernels as OpenWhisk actions:

```bash
benchmarks/semantic_benchmark/deploy_openwhisk_fallback_actions.sh
```

This packages a statically linked `semantic_kernel` with a Node.js OpenWhisk
wrapper and registers actions named:

- `cpu-spin-controllable`
- `memory-scan-controllable`
- `storage-io-controllable`
- `network-transfer-controllable`
- `balanced-pipeline-controllable`

Because the action names match replay fallback `workload` values, the replay
driver can invoke fallback rows without extra `--action-map` entries.

The catalog does not contain profiled numeric resource measurements yet.
`resource_hints.measured=false` marks anchor resource class labels that come from
SeBS benchmark semantics and source structure. Numeric timings, bandwidths,
memory footprints, and cold-start penalties belong in Phase 5 calibration
artifacts.

The controllable kernels are implemented as a native C binary in `kernels/`.
Python is intentionally not used for sub-50ms realizations because interpreter
startup and runtime overhead would dominate those workloads.
