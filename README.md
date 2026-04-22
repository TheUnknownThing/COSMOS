# COSMOS

**CO-Scheduling Multi-resource OS for Serverless**

COSMOS is an MVP invocation-aware scheduler for serverless applications,
optimized around SLO and p99 latency. The first scheduler prototype is based on
`scx_rustland_core`, so the low-level sched_ext BPF dispatcher stays generic and
the scheduling policy lives in Rust.

## Goals

- Prefer cold-start and hot invocation workers when CPU contention threatens p99
  latency.
- Keep the first version easy to modify: classification, scoring, and slicing
  are all in `src/main.rs`.
- Preserve forward progress for background work through the inherited
  vruntime/deadline accounting from `scx_rustland`.
- Provide a foundation for multi-resource serverless co-scheduling, starting
  with CPU scheduling and leaving clear hooks for memory, IO, accelerator, and
  per-function SLO signals.

## Policy Sketch

The current scheduler classifies queued tasks into three buckets:

- `ColdStart`: first-seen tasks or tasks matching an explicit invocation
  runtime hint.
- `HotInvocation`: short-running tasks with repeated wakeups, which approximates
  active request handling.
- `Background`: everything else.

Each task receives a fair deadline from the `scx_rustland` vruntime model. The
invocation classes then receive an SLO-oriented boost before tasks are inserted
into the user-space ordered set. Smaller scores dispatch first.

This is SLO-aware in the MVP sense: the scheduler is parameterized by
`--slo-target-us` and uses that budget in scoring and slicing. It is not yet
feedback-driven by observed per-function p99 or SLO miss signals.

## Usage

This repository is self-contained for the scheduler prototype. The minimal
`scx` support crates used by the scheduler are vendored under `rust/`, including
`scx_rustland_core` and its BPF assets.

Build the scheduler:

```sh
cargo build --release
```

Run with a 10ms target SLO:

```sh
sudo target/release/cosmos --slo-target-us 10000
```

Provide runtime hints for serverless workers:

```sh
sudo target/release/cosmos \
  --slo-target-us 10000 \
  --cold-start-boost-us 20000 \
  --invocation-comm node,python,bootstrap,firecracker
```

Monitor scheduler stats without launching it:

```sh
sudo target/release/cosmos --monitor 1
```

## Repository Layout

- `src/`: COSMOS scheduler policy, CLI, and stats.
- `rust/scx_rustland_core/`: RustLand user-space scheduler core and BPF
  dispatcher assets.
- `rust/scx_cargo/`: build helpers used to generate BPF bindings and skeletons.
- `rust/scx_utils/`: sched_ext utility library.
- `rust/scx_stats/`: stats transport and derive macro support.

## Next Steps

- Add a cgroup or sidecar-fed invocation metadata source so the scheduler can
  distinguish tenants, functions, and request classes directly.
- Track request-level latency from the runtime and feed p99/SLO miss signals
  back into `task_score()`.
- Add per-function admission controls for noisy-neighbor protection under CPU
  saturation.
- Extend the policy beyond CPU into multi-resource co-scheduling for memory,
  IO, network, and accelerator contention.
