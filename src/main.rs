// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

mod bpf_skel;
pub use bpf_skel::*;
pub mod bpf_intf;

#[rustfmt::skip]
mod bpf;
use bpf::*;

mod pool;
mod stats;

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::io;
use std::mem::MaybeUninit;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;
use clap::Parser;
use libbpf_rs::OpenObject;
use log::debug;
use log::info;
use log::warn;
use pool::PoolManager;
use pool::PoolMetrics;
use pool::TaskPool;
use procfs::process::Process;
use scx_stats::prelude::*;
use scx_utils::build_id;
use scx_utils::libbpf_clap_opts::LibbpfOpts;
use scx_utils::UserExitInfo;
use stats::Metrics;

pub const SCHEDULER_NAME: &str = "COSMOS";

const NSEC_PER_USEC: u64 = 1_000;
const TASK_STATE_TTL_NS: u64 = 60_000_000_000;

fn monotonic_now_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };

    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    if rc != 0 {
        panic!(
            "clock_gettime(CLOCK_MONOTONIC) failed: {}",
            io::Error::last_os_error()
        );
    }

    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

/// COSMOS: invocation-aware user-space scheduler for serverless workloads.
///
/// Built on scx_rustland_core. BPF stays policy-agnostic and only forwards
/// runnable tasks to user space; the Rust policy classifies tasks using
/// invocation metadata written by the shim library (libcosmos_meta.so) and
/// orders them by a latency-oriented score.
///
/// The policy has three classes:
///
/// - ColdStart: explicitly marked cold-start invocations (is_cold_start=1) or
///   first-seen heuristic matches.
/// - HotInvocation: latency-critical or standard SLO tasks (slo_class <= 1) or
///   repeated short-running heuristic matches.
/// - Background: batch tasks (slo_class >= 2) and non-invocation fallback work.
///
/// Invocation metadata comes first when available. When the shim is absent, the
/// scheduler falls back to runtime / wakeup heuristics so mixed deployments
/// still get sensible latency-aware behavior.
#[derive(Debug, Parser)]
struct Opts {
    /// Scheduling slice duration in microseconds.
    #[clap(short = 's', long, default_value = "20000")]
    slice_us: u64,

    /// Scheduling minimum slice duration in microseconds.
    #[clap(short = 'S', long, default_value = "500")]
    slice_us_min: u64,

    /// Target invocation SLO in microseconds. The policy uses this as its p99 latency budget.
    #[clap(long, default_value = "10000")]
    slo_target_us: u64,

    /// Extra boost in microseconds for cold-start tasks.
    #[clap(long, default_value = "20000")]
    cold_start_boost_us: u64,

    /// Treat tasks whose comm contains any of these comma-delimited strings as invocation workers.
    #[clap(long, value_delimiter = ',')]
    invocation_comm: Vec<String>,

    /// If set, per-CPU tasks are dispatched directly to their only eligible CPU.
    #[clap(short = 'l', long, action = clap::ArgAction::SetTrue)]
    percpu_local: bool,

    /// Enable NUMA-local idle CPU selection.
    #[clap(short = 'n', long, action = clap::ArgAction::SetTrue)]
    numa_local: bool,

    /// If specified, only tasks which have their scheduling policy set to SCHED_EXT are switched.
    #[clap(short = 'p', long, action = clap::ArgAction::SetTrue)]
    partial: bool,

    /// Exit debug dump buffer length. 0 indicates default.
    #[clap(long, default_value = "0")]
    exit_dump_len: u32,

    /// Enable verbose output, including libbpf details.
    #[clap(short = 'v', long, action = clap::ArgAction::SetTrue)]
    verbose: bool,

    /// Percentage of CPUs to assign to the latency pool (Phase 2).
    #[clap(long, default_value = "50")]
    latency_pool_pct: u32,

    /// Number of CPUs to reserve for the tail guard pool (0 = disabled) (Phase 2).
    #[clap(long, default_value = "1")]
    tail_guard_cpus: u32,

    /// Pool rebalance interval in milliseconds (Phase 2).
    #[clap(long, default_value = "500")]
    pool_rebalance_ms: u64,

    /// Keep every task on the legacy shared DSQ path and disable pool rebalancing.
    #[clap(long, action = clap::ArgAction::SetTrue)]
    disable_pools: bool,

    /// Disable direct idle-CPU dispatch so user space can refresh late metadata before dispatch.
    #[clap(long, action = clap::ArgAction::SetTrue)]
    disable_builtin_idle: bool,

    /// Use heuristic vtime scoring even when invocation deadlines are present.
    #[clap(long, action = clap::ArgAction::SetTrue)]
    disable_deadline_scoring: bool,

    /// Slack threshold in microseconds below which tasks are promoted to tail guard (Phase 3).
    /// Default: slo_target_us / 2.  Set to 0 to disable tail guard promotion.
    #[clap(long)]
    tail_guard_threshold_us: Option<u64>,

    /// Enable stats monitoring with the specified interval.
    #[clap(long)]
    stats: Option<f64>,

    /// Run in stats monitoring mode with the specified interval. Scheduler is not launched.
    #[clap(long)]
    monitor: Option<f64>,

    /// Show descriptions for statistics.
    #[clap(long)]
    help_stats: bool,

    /// Print scheduler version and exit.
    #[clap(short = 'V', long, action = clap::ArgAction::SetTrue)]
    version: bool,

    #[clap(flatten, next_help_heading = "Libbpf Options")]
    pub libbpf: LibbpfOpts,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum TaskClass {
    ColdStart,
    HotInvocation,
    Background,
}

#[derive(Debug, Default, Clone)]
struct TaskState {
    avg_runtime_ns: u64,
    wakeups: u64,
    last_seen_ns: u64,
    last_invocation_id: u64,
}

#[derive(Debug, Clone)]
struct Task {
    qtask: QueuedTask,
    class: TaskClass,
    pool: TaskPool,
    score: u64,
    timestamp: u64,
    slice_ns: u64,
    has_metadata: bool,
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
            && class_rank(self.class) == class_rank(other.class)
            && self.has_metadata == other.has_metadata
            && self.timestamp == other.timestamp
            && self.qtask.pid == other.qtask.pid
    }
}

impl Eq for Task {}

impl Ord for Task {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .cmp(&other.score)
            .then_with(|| class_rank(self.class).cmp(&class_rank(other.class)))
            // Phase 4: prefer metadata tasks over heuristic tasks at equal score/class.
            // metadata=true sorts before metadata=false (false < true, so we reverse).
            .then_with(|| other.has_metadata.cmp(&self.has_metadata))
            .then_with(|| self.timestamp.cmp(&other.timestamp))
            .then_with(|| self.qtask.pid.cmp(&other.qtask.pid))
    }
}

impl PartialOrd for Task {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct SchedulerPolicy {
    task_state: HashMap<i32, TaskState>,
    vruntime_now: u64,
    slice_ns: u64,
    slice_ns_min: u64,
    slo_target_ns: u64,
    cold_start_boost_ns: u64,
    invocation_comm: Vec<String>,
    /// Phase 3: slack threshold below which tasks are promoted to tail guard pool.
    tail_guard_threshold_ns: u64,
    pools_enabled: bool,
    deadline_scoring_enabled: bool,
    nr_cold_start_tasks: u64,
    nr_hot_invocation_tasks: u64,
    nr_background_tasks: u64,
    nr_slo_boosted: u64,
    max_pending: u64,
    nr_metadata_classified: u64,
    nr_heuristic_classified: u64,
    nr_metadata_refreshed: u64,
    nr_pool_latency: u64,
    nr_pool_batch: u64,
    /// Phase 3: tasks promoted to tail guard pool.
    nr_tail_guard_dispatches: u64,
    /// Phase 3: tasks dispatched past their deadline.
    nr_slo_violations: u64,
}

impl SchedulerPolicy {
    fn new(opts: &Opts) -> Self {
        let slo_target_ns = opts.slo_target_us * NSEC_PER_USEC;
        let tail_guard_threshold_ns = match opts.tail_guard_threshold_us {
            Some(us) => us * NSEC_PER_USEC,
            None => slo_target_ns / 2, // default: half the SLO target
        };
        Self {
            task_state: HashMap::new(),
            vruntime_now: 0,
            slice_ns: opts.slice_us * NSEC_PER_USEC,
            slice_ns_min: opts.slice_us_min * NSEC_PER_USEC,
            slo_target_ns,
            cold_start_boost_ns: opts.cold_start_boost_us * NSEC_PER_USEC,
            invocation_comm: opts.invocation_comm.clone(),
            tail_guard_threshold_ns,
            pools_enabled: !opts.disable_pools,
            deadline_scoring_enabled: !opts.disable_deadline_scoring,
            nr_cold_start_tasks: 0,
            nr_hot_invocation_tasks: 0,
            nr_background_tasks: 0,
            nr_slo_boosted: 0,
            max_pending: 0,
            nr_metadata_classified: 0,
            nr_heuristic_classified: 0,
            nr_metadata_refreshed: 0,
            nr_pool_latency: 0,
            nr_pool_batch: 0,
            nr_tail_guard_dispatches: 0,
            nr_slo_violations: 0,
        }
    }

    fn scale_by_task_weight(task: &QueuedTask, value: u64) -> u64 {
        value.saturating_mul(task.weight) / 100
    }

    fn scale_by_task_weight_inverse(task: &QueuedTask, value: u64) -> u64 {
        value.saturating_mul(100) / task.weight.max(1)
    }

    fn task_matches_invocation_hint(&self, task: &QueuedTask) -> bool {
        let comm = task.comm_str();
        self.invocation_comm
            .iter()
            .any(|needle| !needle.is_empty() && comm.contains(needle))
    }

    fn is_new_metadata_invocation(&self, task: &QueuedTask) -> bool {
        if task.has_invocation_meta != 1 || task.invocation_id == 0 {
            return false;
        }

        self.task_state
            .get(&task.pid)
            .map(|state| state.last_invocation_id != task.invocation_id)
            .unwrap_or(true)
    }

    fn classify_task(&self, task: &QueuedTask) -> TaskClass {
        // Metadata-first classification from the shim library.
        if task.has_invocation_meta == 1 {
            return if task.is_cold_start == 1 {
                TaskClass::ColdStart
            } else if self.is_new_metadata_invocation(task) && task.slo_class <= 1 {
                // A fresh invocation still needs bootstrap runway even when the
                // runtime marks it as warm. Otherwise every new metadata-backed
                // burst starts in the shortest "hot" slice configuration.
                TaskClass::ColdStart
            } else if task.slo_class <= 1 {
                TaskClass::HotInvocation
            } else {
                TaskClass::Background
            };
        }

        let Some(state) = self.task_state.get(&task.pid) else {
            return if self.task_matches_invocation_hint(task)
                || task.exec_runtime <= self.slo_target_ns
            {
                TaskClass::ColdStart
            } else {
                TaskClass::Background
            };
        };

        if self.task_matches_invocation_hint(task) {
            return if state.wakeups <= 1 {
                TaskClass::ColdStart
            } else {
                TaskClass::HotInvocation
            };
        }

        if task.exec_runtime <= self.slo_target_ns
            && state.avg_runtime_ns <= self.slo_target_ns.saturating_mul(2)
        {
            TaskClass::HotInvocation
        } else {
            TaskClass::Background
        }
    }

    fn update_task_state(&mut self, task: &QueuedTask, now: u64) {
        let state = self.task_state.entry(task.pid).or_default();
        let new_metadata_invocation = task.has_invocation_meta == 1
            && task.invocation_id != 0
            && state.last_invocation_id != task.invocation_id;

        if new_metadata_invocation {
            // Per-invocation runtime state should not bleed across requests that
            // reuse the same process.
            state.avg_runtime_ns = 0;
            state.wakeups = 0;
        }

        state.avg_runtime_ns = if state.avg_runtime_ns == 0 {
            task.exec_runtime
        } else {
            state
                .avg_runtime_ns
                .saturating_mul(7)
                .saturating_add(task.exec_runtime)
                / 8
        };
        state.wakeups = state.wakeups.saturating_add(1);
        state.last_seen_ns = now;
        if task.has_invocation_meta == 1 && task.invocation_id != 0 {
            state.last_invocation_id = task.invocation_id;
        }
    }

    fn update_vruntime(&mut self, task: &mut QueuedTask) {
        task.vtime = if task.vtime == 0 {
            self.vruntime_now
        } else {
            let vruntime_min = self.vruntime_now.saturating_sub(self.slice_ns);
            task.vtime.max(vruntime_min)
        };

        let slice_ns = task.stop_ts.saturating_sub(task.start_ts);
        let vslice = Self::scale_by_task_weight_inverse(task, slice_ns);
        task.vtime = task.vtime.saturating_add(vslice);
        self.vruntime_now = self.vruntime_now.saturating_add(vslice);
    }

    fn fair_deadline_score(&self, task: &QueuedTask) -> u64 {
        let runtime_penalty = task.exec_runtime.min(self.slice_ns.saturating_mul(100));
        task.vtime.saturating_add(runtime_penalty)
    }

    fn task_score(&self, task: &QueuedTask, class: TaskClass, _pool: TaskPool, now: u64) -> u64 {
        let fair_deadline = self.fair_deadline_score(task);

        // Phase 4: EDF scoring for all tasks with invocation metadata + deadline.
        // Lower slack still matters, but we preserve vruntime fairness by applying
        // only a bounded urgency adjustment to the fair-deadline score.
        if self.deadline_scoring_enabled && task.has_invocation_meta == 1 && task.deadline_ns > 0 {
            let urgency_window = self.slice_ns.max(self.slice_ns_min);
            let metadata_anchor = urgency_window
                .saturating_add(self.slo_target_ns)
                .saturating_add(self.cold_start_boost_ns);
            let estimated_remaining = self
                .task_state
                .get(&task.pid)
                .map(|state| state.avg_runtime_ns.max(self.slice_ns_min))
                .unwrap_or_else(|| task.exec_runtime.max(self.slice_ns_min));
            let remaining_budget = task.deadline_ns.saturating_sub(now);

            if remaining_budget > estimated_remaining {
                let slack_ns = remaining_budget.saturating_sub(estimated_remaining);
                let urgency_boost = urgency_window.saturating_sub(slack_ns.min(urgency_window));
                let metadata_base = fair_deadline
                    .saturating_add(metadata_anchor)
                    .saturating_sub(Self::scale_by_task_weight(task, urgency_boost));
                return match class {
                    TaskClass::ColdStart => metadata_base.saturating_sub(
                        Self::scale_by_task_weight(
                            task,
                            self.slo_target_ns.saturating_add(self.cold_start_boost_ns),
                        ),
                    ),
                    TaskClass::HotInvocation => metadata_base
                        .saturating_sub(Self::scale_by_task_weight(task, self.slo_target_ns)),
                    TaskClass::Background => metadata_base.saturating_add(self.slo_target_ns),
                };
            }
        }

        // Existing heuristic scoring for tasks without metadata (unchanged)
        let boost = match class {
            TaskClass::ColdStart => self
                .slo_target_ns
                .saturating_mul(2)
                .saturating_add(self.cold_start_boost_ns),
            TaskClass::HotInvocation => self.slo_target_ns,
            TaskClass::Background => 0,
        };

        fair_deadline.saturating_sub(Self::scale_by_task_weight(task, boost))
    }

    fn task_slice_ns(&self, task: &QueuedTask, class: TaskClass, pool: TaskPool) -> u64 {
        // Phase 3: Tail guard tasks get full SLO budget to maximize chance of completion
        if pool == TaskPool::TailGuard {
            return self.slo_target_ns.max(self.slice_ns_min);
        }

        let base = match class {
            TaskClass::ColdStart => self.slo_target_ns / 4,
            TaskClass::HotInvocation => self.slo_target_ns / 8,
            TaskClass::Background => self.slice_ns,
        };

        Self::scale_by_task_weight(task, base.max(self.slice_ns_min)).max(self.slice_ns_min)
    }

    fn update_enqueued(
        &mut self,
        task: &mut QueuedTask,
        now: u64,
    ) -> (TaskClass, TaskPool, u64, u64) {
        let class = self.classify_task(task);
        self.update_vruntime(task);
        self.update_task_state(task, now);

        // Phase 1: pool assignment based on classification
        let mut pool = if self.pools_enabled {
            match class {
                TaskClass::ColdStart | TaskClass::HotInvocation => TaskPool::Latency,
                TaskClass::Background => TaskPool::Batch,
            }
        } else {
            TaskPool::None
        };

        // Phase 3: Tail guard promotion — if the task has metadata with a
        // deadline and the remaining slack is below the threshold, promote
        // it to the TailGuard pool for priority execution.
        if task.has_invocation_meta == 1 && task.deadline_ns > 0 {
            if now > task.deadline_ns {
                self.nr_slo_violations = self.nr_slo_violations.saturating_add(1);
            } else if self.pools_enabled && self.tail_guard_threshold_ns > 0 {
                let task_state = self.task_state.get(&task.pid);
                let estimated_remaining = task_state.map_or(0, |s| s.avg_runtime_ns);
                let slack_ns = task
                    .deadline_ns
                    .saturating_sub(now)
                    .saturating_sub(estimated_remaining);

                if slack_ns < self.tail_guard_threshold_ns {
                    pool = TaskPool::TailGuard;
                    self.nr_tail_guard_dispatches = self.nr_tail_guard_dispatches.saturating_add(1);
                }
            }
        }

        let score = self.task_score(task, class, pool, now);
        let slice_ns = self.task_slice_ns(task, class, pool);

        // Track whether this was classified via metadata
        if task.has_invocation_meta == 1 {
            self.nr_metadata_classified = self.nr_metadata_classified.saturating_add(1);
        } else {
            self.nr_heuristic_classified = self.nr_heuristic_classified.saturating_add(1);
        }

        match class {
            TaskClass::ColdStart => {
                self.nr_cold_start_tasks = self.nr_cold_start_tasks.saturating_add(1);
                self.nr_slo_boosted = self.nr_slo_boosted.saturating_add(1);
            }
            TaskClass::HotInvocation => {
                self.nr_hot_invocation_tasks = self.nr_hot_invocation_tasks.saturating_add(1);
                self.nr_slo_boosted = self.nr_slo_boosted.saturating_add(1);
            }
            TaskClass::Background => {
                self.nr_background_tasks = self.nr_background_tasks.saturating_add(1);
            }
        }

        match pool {
            TaskPool::Latency => {
                self.nr_pool_latency = self.nr_pool_latency.saturating_add(1);
            }
            TaskPool::Batch => {
                self.nr_pool_batch = self.nr_pool_batch.saturating_add(1);
            }
            TaskPool::TailGuard => {
                // already counted via nr_tail_guard_dispatches above
            }
            TaskPool::None => {}
        }

        (class, pool, score, slice_ns)
    }

    fn prune_task_state(&mut self, now: u64) {
        self.task_state
            .retain(|_, state| now.saturating_sub(state.last_seen_ns) < TASK_STATE_TTL_NS);
    }
}

struct Scheduler<'a> {
    bpf: BpfScheduler<'a>,
    opts: &'a Opts,
    stats_server: StatsServer<(), Metrics>,
    tasks: BTreeSet<Task>,
    pending_latency_tasks: u64,
    pending_batch_tasks: u64,
    pending_tail_guard_tasks: u64,
    policy: SchedulerPolicy,
    pool_manager: PoolManager,
    init_page_faults: u64,
}

impl<'a> Scheduler<'a> {
    fn init(opts: &'a Opts, open_object: &'a mut MaybeUninit<OpenObject>) -> Result<Self> {
        let stats_server = StatsServer::new(stats::server_data()).launch()?;
        let mut policy = SchedulerPolicy::new(opts);

        let mut bpf = BpfScheduler::init(
            open_object,
            opts.libbpf.clone().into_bpf_open_opts(),
            opts.exit_dump_len,
            opts.partial,
            opts.verbose,
            !opts.disable_builtin_idle,
            opts.numa_local,
            policy.slice_ns_min,
            policy.slice_ns.max(policy.slice_ns_min),
            "cosmos",
        )?;

        info!(
            "{} version {} - scx_rustland_core {}",
            SCHEDULER_NAME,
            build_id::full_version(env!("CARGO_PKG_VERSION")),
            scx_rustland_core::VERSION
        );

        // Phase 2: Initialize the pool manager and apply initial CPU assignments
        let nr_cpus = *bpf.nr_online_cpus_mut() as usize;
        let effective_tail_guard_cpus =
            pool::effective_tail_guard_cpus(nr_cpus, opts.tail_guard_cpus);
        let auto_disable_small_host_features = nr_cpus <= 4;
        if effective_tail_guard_cpus != opts.tail_guard_cpus {
            info!(
                "Phase 2: tail guard auto-disabled on {}-CPU host (requested {}, using {})",
                nr_cpus, opts.tail_guard_cpus, effective_tail_guard_cpus
            );
            policy.tail_guard_threshold_ns = 0;
        }
        if auto_disable_small_host_features {
            if policy.pools_enabled {
                info!(
                    "Phase 2: auto-disabling pools on {}-CPU host to avoid partitioning limited CPU capacity",
                    nr_cpus
                );
                policy.pools_enabled = false;
            }
            if policy.deadline_scoring_enabled {
                info!(
                    "Phase 4: auto-disabling deadline scoring on {}-CPU host to protect tail latency under oversubscription",
                    nr_cpus
                );
                policy.deadline_scoring_enabled = false;
            }
        }
        let pool_manager = if opts.disable_pools || auto_disable_small_host_features {
            PoolManager::disabled(nr_cpus)
        } else {
            PoolManager::new(nr_cpus, opts.latency_pool_pct, effective_tail_guard_cpus)
        };

        // Write initial pool assignments to the BPF cpu_pool_map
        pool_manager.apply_all(|cpu, pool| {
            if let Err(e) = bpf.update_cpu_pool(cpu, pool) {
                log::warn!("Failed to set initial pool for CPU {}: {}", cpu, e);
            }
        });
        if opts.disable_pools {
            info!("Phase 2: CPU pools disabled, tasks stay on the shared DSQ path");
        } else {
            info!(
                "Phase 2: Pool manager initialized ({} CPUs, {}% latency, {} tail guard, {}ms rebalance)",
                nr_cpus, opts.latency_pool_pct, effective_tail_guard_cpus, opts.pool_rebalance_ms
            );
        }
        info!(
            "Phase 3: Tail guard threshold = {}us ({}ns)",
            policy.tail_guard_threshold_ns / NSEC_PER_USEC,
            policy.tail_guard_threshold_ns
        );
        info!(
            "Phase 4: deadline scoring {}",
            if opts.disable_deadline_scoring {
                "disabled"
            } else {
                "enabled"
            }
        );

        Ok(Self {
            bpf,
            opts,
            stats_server,
            tasks: BTreeSet::new(),
            pending_latency_tasks: 0,
            pending_batch_tasks: 0,
            pending_tail_guard_tasks: 0,
            policy,
            pool_manager,
            init_page_faults: 0,
        })
    }

    fn get_metrics(&mut self) -> Metrics {
        let page_faults = Self::get_page_faults().unwrap_or_default();
        if self.init_page_faults == 0 {
            self.init_page_faults = page_faults;
        }
        let nr_page_faults = page_faults - self.init_page_faults;

        Metrics {
            nr_running: *self.bpf.nr_running_mut(),
            nr_cpus: *self.bpf.nr_online_cpus_mut(),
            nr_queued: *self.bpf.nr_queued_mut(),
            nr_scheduled: *self.bpf.nr_scheduled_mut(),
            nr_page_faults,
            nr_cold_start_tasks: self.policy.nr_cold_start_tasks,
            nr_hot_invocation_tasks: self.policy.nr_hot_invocation_tasks,
            nr_background_tasks: self.policy.nr_background_tasks,
            nr_slo_boosted: self.policy.nr_slo_boosted,
            max_pending: self.policy.max_pending,
            nr_user_dispatches: *self.bpf.nr_user_dispatches_mut(),
            nr_kernel_dispatches: *self.bpf.nr_kernel_dispatches_mut(),
            nr_cancel_dispatches: *self.bpf.nr_cancel_dispatches_mut(),
            nr_bounce_dispatches: *self.bpf.nr_bounce_dispatches_mut(),
            nr_failed_dispatches: *self.bpf.nr_failed_dispatches_mut(),
            nr_sched_congested: *self.bpf.nr_sched_congested_mut(),
            nr_metadata_classified: self.policy.nr_metadata_classified,
            nr_heuristic_classified: self.policy.nr_heuristic_classified,
            nr_metadata_refreshed: self.policy.nr_metadata_refreshed,
            nr_invocation_meta_enqueues: {
                let v = *self.bpf.nr_invocation_meta_enqueues_mut();
                if self.policy.nr_metadata_classified > 0 || v > 0 {
                    eprintln!(
                        "COSMOS_DEBUG: get_metrics meta_classified={} meta_enq={} heur={}",
                        self.policy.nr_metadata_classified, v, self.policy.nr_heuristic_classified
                    );
                }
                v
            },
            nr_pool_latency: self.policy.nr_pool_latency,
            nr_pool_batch: self.policy.nr_pool_batch,
            nr_pool_migrations: self.pool_manager.nr_pool_migrations,
            nr_tail_guard_dispatches: self.policy.nr_tail_guard_dispatches,
            nr_slo_violations: self.policy.nr_slo_violations,
        }
    }

    fn now() -> u64 {
        monotonic_now_ns()
    }

    fn track_pending_pool(&mut self, pool: TaskPool, delta: i64) {
        let target = match pool {
            TaskPool::Latency => Some(&mut self.pending_latency_tasks),
            TaskPool::Batch => Some(&mut self.pending_batch_tasks),
            TaskPool::TailGuard => Some(&mut self.pending_tail_guard_tasks),
            TaskPool::None => None,
        };

        let Some(counter) = target else {
            return;
        };

        if delta >= 0 {
            *counter = counter.saturating_add(delta as u64);
        } else {
            *counter = counter.saturating_sub((-delta) as u64);
        }
    }

    fn effective_dispatch_pool(&self, pool: TaskPool) -> TaskPool {
        if !self.pool_manager.is_enabled() {
            return TaskPool::None;
        }

        let latency_like = self.pending_latency_tasks
            .saturating_add(self.pending_tail_guard_tasks)
            .saturating_add(u64::from(matches!(pool, TaskPool::Latency | TaskPool::TailGuard)));
        let batch_like = self
            .pending_batch_tasks
            .saturating_add(u64::from(matches!(pool, TaskPool::Batch)));

        if latency_like == 0 || batch_like == 0 {
            TaskPool::None
        } else {
            pool
        }
    }

    fn dispatch_task(&mut self) -> bool {
        let Some(task) = self.tasks.pop_first() else {
            return true;
        };
        self.track_pending_pool(task.pool, -1);

        let mut dispatched_task = DispatchedTask::new(&task.qtask);
        dispatched_task.slice_ns = task.slice_ns;
        dispatched_task.vtime = task.score;
        dispatched_task.pool = self.effective_dispatch_pool(task.pool) as u32;

        dispatched_task.cpu = if self.opts.percpu_local {
            task.qtask.cpu
        } else {
            match self
                .bpf
                .select_cpu(task.qtask.pid, task.qtask.cpu, task.qtask.flags)
            {
                cpu if cpu >= 0 => cpu,
                _ => RL_CPU_ANY,
            }
        };

        if self.bpf.dispatch_task(&dispatched_task).is_err() {
            self.track_pending_pool(task.pool, 1);
            self.tasks.insert(task);
            return false;
        }

        true
    }

    fn drain_queued_tasks(&mut self) {
        loop {
            match self.bpf.dequeue_task() {
                Ok(Some(mut task)) => {
                    let timestamp = Self::now();
                    if task.has_invocation_meta != 1 {
                        match self.bpf.refresh_invocation_meta(&mut task) {
                            Ok(true) => {
                                self.policy.nr_metadata_refreshed =
                                    self.policy.nr_metadata_refreshed.saturating_add(1);
                            }
                            Ok(false) => {}
                            Err(err) => warn!("Failed to refresh invocation metadata: {err}"),
                        }
                    }
                    let (class, pool, score, slice_ns) =
                        self.policy.update_enqueued(&mut task, timestamp);

                    let has_metadata = task.has_invocation_meta == 1;
                    self.tasks.insert(Task {
                        qtask: task,
                        class,
                        pool,
                        score,
                        timestamp,
                        slice_ns,
                        has_metadata,
                    });
                    self.track_pending_pool(pool, 1);
                }
                Ok(None) => break,
                Err(err) => {
                    warn!("Error: {err}");
                    break;
                }
            }
        }
    }

    fn refresh_pending_metadata(&mut self) {
        if self.tasks.is_empty() {
            return;
        }

        let timestamp = Self::now();
        let mut refreshed = BTreeSet::new();
        self.pending_latency_tasks = 0;
        self.pending_batch_tasks = 0;
        self.pending_tail_guard_tasks = 0;

        while let Some(mut task) = self.tasks.pop_first() {
            if !task.has_metadata {
                match self.bpf.refresh_invocation_meta(&mut task.qtask) {
                    Ok(true) => {
                        self.policy.nr_metadata_refreshed =
                            self.policy.nr_metadata_refreshed.saturating_add(1);
                        let (class, pool, score, slice_ns) =
                            self.policy.update_enqueued(&mut task.qtask, timestamp);
                        task.class = class;
                        task.pool = pool;
                        task.score = score;
                        task.timestamp = timestamp;
                        task.slice_ns = slice_ns;
                        task.has_metadata = true;
                    }
                    Ok(false) => {}
                    Err(err) => warn!("Failed to refresh pending invocation metadata: {err}"),
                }
            }
            self.track_pending_pool(task.pool, 1);
            refreshed.insert(task);
        }

        self.tasks = refreshed;
    }

    fn schedule(&mut self) {
        self.drain_queued_tasks();
        self.refresh_pending_metadata();
        self.dispatch_task();

        let pending = self.tasks.len() as u64;
        self.policy.max_pending = self.policy.max_pending.max(pending);
        if pending == 0 {
            self.policy.prune_task_state(Self::now());
        }

        self.bpf.notify_complete(pending);
    }

    fn get_page_faults() -> Result<u64, io::Error> {
        let myself = Process::myself().map_err(io::Error::other)?;
        let stat = myself.stat().map_err(io::Error::other)?;

        Ok(stat.minflt + stat.majflt)
    }

    /// Read pool queue depths by counting pending tasks in the user-space task queue.
    ///
    /// This measures the tasks waiting to be dispatched in each pool, providing
    /// pressure signals for the rebalancer. We count tasks in the BTreeSet since
    /// the BPF DSQ depth isn't directly queryable from user-space.
    fn read_pool_metrics(&self) -> PoolMetrics {
        let mut latency = 0u64;
        let mut batch = 0u64;
        let mut tail_guard = 0u64;

        for task in &self.tasks {
            match task.pool {
                TaskPool::Latency => latency += 1,
                TaskPool::Batch => batch += 1,
                TaskPool::TailGuard => tail_guard += 1,
                TaskPool::None => {}
            }
        }

        PoolMetrics {
            latency_queue_depth: latency,
            batch_queue_depth: batch,
            tail_guard_queue_depth: tail_guard,
        }
    }

    fn run(&mut self) -> Result<UserExitInfo> {
        let (res_ch, req_ch) = self.stats_server.channels();
        let pool_rebalance_interval = Duration::from_millis(self.opts.pool_rebalance_ms);
        let mut last_rebalance = Instant::now();

        while !self.bpf.exited() {
            self.schedule();

            // Phase 2: Periodic pool rebalancing
            if self.pool_manager.is_enabled() && last_rebalance.elapsed() >= pool_rebalance_interval
            {
                let metrics = self.read_pool_metrics();
                let changes = self.pool_manager.rebalance(&metrics);
                if !changes.is_empty() {
                    pool::PoolManager::apply_changes(&changes, |cpu, pool| {
                        if let Err(e) = self.bpf.update_cpu_pool(cpu, pool) {
                            log::warn!("Failed to update pool for CPU {}: {}", cpu, e);
                        }
                    });
                    debug!(
                        "Pool rebalance: {} changes, depths: lat={} batch={} tg={}",
                        changes.len(),
                        metrics.latency_queue_depth,
                        metrics.batch_queue_depth,
                        metrics.tail_guard_queue_depth,
                    );
                }
                last_rebalance = Instant::now();
            }

            if req_ch.try_recv().is_ok() {
                res_ch.send(self.get_metrics())?;
            }
        }

        self.bpf.shutdown_and_report()
    }
}

impl Drop for Scheduler<'_> {
    fn drop(&mut self) {
        info!("Unregister {SCHEDULER_NAME} scheduler");
    }
}

fn class_rank(class: TaskClass) -> u8 {
    match class {
        TaskClass::ColdStart => 0,
        TaskClass::HotInvocation => 1,
        TaskClass::Background => 2,
    }
}

fn main() -> Result<()> {
    let opts = Opts::parse();

    if opts.version {
        println!(
            "{} version {} - scx_rustland_core {}",
            SCHEDULER_NAME,
            build_id::full_version(env!("CARGO_PKG_VERSION")),
            scx_rustland_core::VERSION
        );
        return Ok(());
    }

    if opts.help_stats {
        stats::server_data().describe_meta(&mut std::io::stdout(), None)?;
        return Ok(());
    }

    let loglevel = simplelog::LevelFilter::Info;
    let mut lcfg = simplelog::ConfigBuilder::new();
    lcfg.set_time_offset_to_local()
        .expect("Failed to set local time offset")
        .set_time_level(simplelog::LevelFilter::Error)
        .set_location_level(simplelog::LevelFilter::Off)
        .set_target_level(simplelog::LevelFilter::Off)
        .set_thread_level(simplelog::LevelFilter::Off);
    simplelog::TermLogger::init(
        loglevel,
        lcfg.build(),
        simplelog::TerminalMode::Stderr,
        simplelog::ColorChoice::Auto,
    )?;

    if let Some(intv) = opts.monitor.or(opts.stats) {
        let jh = std::thread::spawn(move || stats::monitor(Duration::from_secs_f64(intv)).unwrap());
        if opts.monitor.is_some() {
            let _ = jh.join();
            return Ok(());
        }
    }

    let mut open_object = MaybeUninit::uninit();
    loop {
        let mut sched = Scheduler::init(&opts, &mut open_object)?;
        if !sched.run()?.should_restart() {
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    const MS: u64 = 1_000_000;

    fn opts(args: &[&str]) -> Opts {
        let mut argv = vec!["cosmos"];
        argv.extend_from_slice(args);
        Opts::parse_from(argv)
    }

    fn task(
        pid: i32,
        comm: &str,
        exec_runtime: u64,
        weight: u64,
        start_ts: u64,
        stop_ts: u64,
        vtime: u64,
    ) -> QueuedTask {
        let mut comm_buf = [0; 16];
        for (idx, byte) in comm.as_bytes().iter().take(comm_buf.len() - 1).enumerate() {
            comm_buf[idx] = *byte as libc::c_char;
        }

        QueuedTask {
            pid,
            cpu: 0,
            nr_cpus_allowed: 4,
            flags: 0,
            start_ts,
            stop_ts,
            exec_runtime,
            weight,
            vtime,
            enq_cnt: 0,
            comm: comm_buf,
            // Phase 1: default to no metadata
            deadline_ns: 0,
            slo_class: 0xFF, // SLO_CLASS_NONE
            has_invocation_meta: 0,
            is_cold_start: 0,
            invocation_id: 0,
        }
    }

    /// Create a QueuedTask with invocation metadata set.
    fn task_with_meta(
        pid: i32,
        comm: &str,
        exec_runtime: u64,
        weight: u64,
        slo_class: u32,
        is_cold_start: u32,
        deadline_ns: u64,
    ) -> QueuedTask {
        let mut t = task(pid, comm, exec_runtime, weight, 0, 0, 0);
        t.has_invocation_meta = 1;
        t.slo_class = slo_class;
        t.is_cold_start = is_cold_start;
        t.deadline_ns = deadline_ns;
        t.invocation_id = pid as u64;
        t
    }

    fn expected_heuristic_score(
        policy: &SchedulerPolicy,
        task: &QueuedTask,
        class: TaskClass,
    ) -> u64 {
        let fair_deadline = policy.fair_deadline_score(task);
        let boost = match class {
            TaskClass::ColdStart => policy
                .slo_target_ns
                .saturating_mul(2)
                .saturating_add(policy.cold_start_boost_ns),
            TaskClass::HotInvocation => policy.slo_target_ns,
            TaskClass::Background => 0,
        };

        fair_deadline.saturating_sub(SchedulerPolicy::scale_by_task_weight(task, boost))
    }

    #[test]
    fn no_metadata_short_task_classifies_as_cold_start() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let policy = SchedulerPolicy::new(&opts);

        // Without metadata, the original runtime heuristic still applies.
        let t = task(101, "worker", 5 * MS, 100, 0, 0, 0);
        assert_eq!(policy.classify_task(&t), TaskClass::ColdStart);
    }

    #[test]
    fn no_metadata_long_task_classifies_as_background() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let policy = SchedulerPolicy::new(&opts);

        let t = task(102, "node", 80 * MS, 100, 0, 0, 0);
        assert_eq!(policy.classify_task(&t), TaskClass::Background);
    }

    #[test]
    fn metadata_classifies_fresh_latency_critical_as_cold_start() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let policy = SchedulerPolicy::new(&opts);

        // slo_class=0 (latency-critical), not cold start
        let t = task_with_meta(901, "worker", 80 * MS, 100, 0, 0, 100 * MS);
        assert_eq!(policy.classify_task(&t), TaskClass::ColdStart);
    }

    #[test]
    fn metadata_repeated_standard_invocation_becomes_hot() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;
        let mut first = task_with_meta(901, "worker", 5 * MS, 100, 1, 0, now + 100 * MS);
        let (first_class, _, _, _) = policy.update_enqueued(&mut first, now);
        assert_eq!(first_class, TaskClass::ColdStart);

        let second = task_with_meta(901, "worker", 5 * MS, 100, 1, 0, now + 100 * MS);
        assert_eq!(policy.classify_task(&second), TaskClass::HotInvocation);
    }

    #[test]
    fn metadata_new_invocation_resets_per_pid_state() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;
        let mut first = task_with_meta(902, "worker", 8 * MS, 100, 0, 0, now + 100 * MS);
        policy.update_enqueued(&mut first, now);

        let state_after_first = policy.task_state.get(&902).unwrap().clone();
        assert_eq!(state_after_first.wakeups, 1);
        assert_eq!(state_after_first.last_invocation_id, 902);

        let mut next_invocation = task_with_meta(902, "worker", 2 * MS, 100, 0, 0, now + 200 * MS);
        next_invocation.invocation_id = 9902;
        let (class, _, _, _) = policy.update_enqueued(&mut next_invocation, now + MS);
        assert_eq!(class, TaskClass::ColdStart);

        let state = policy.task_state.get(&902).unwrap();
        assert_eq!(state.wakeups, 1, "new invocation should reset wakeup history");
        assert_eq!(state.avg_runtime_ns, 2 * MS);
        assert_eq!(state.last_invocation_id, 9902);
    }

    #[test]
    fn metadata_classifies_cold_start() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let policy = SchedulerPolicy::new(&opts);

        // slo_class=0, is_cold_start=1
        let t = task_with_meta(902, "worker", 80 * MS, 100, 0, 1, 100 * MS);
        assert_eq!(policy.classify_task(&t), TaskClass::ColdStart);
    }

    #[test]
    fn metadata_classifies_batch_as_background() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let policy = SchedulerPolicy::new(&opts);

        // slo_class=2 (batch)
        let t = task_with_meta(903, "worker", 5 * MS, 100, 2, 0, 100 * MS);
        assert_eq!(policy.classify_task(&t), TaskClass::Background);
    }

    #[test]
    fn heuristic_repeated_short_task_becomes_hot_invocation() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 10 * MS;
        let mut first = task(904, "worker", 5 * MS, 100, 0, 0, 0);
        let (class_first, _, _, _) = policy.update_enqueued(&mut first, now);
        assert_eq!(class_first, TaskClass::ColdStart);

        let mut second = task(904, "worker", 5 * MS, 100, 0, 0, 0);
        assert_eq!(policy.classify_task(&second), TaskClass::HotInvocation);
        let (class_second, _, _, _) = policy.update_enqueued(&mut second, now + MS);
        assert_eq!(class_second, TaskClass::HotInvocation);
    }

    #[test]
    fn invocation_comm_hint_is_used_in_heuristic_fallback() {
        let opts = opts(&[
            "--slo-target-us",
            "10000",
            "--invocation-comm",
            "python,node",
        ]);
        let mut policy = SchedulerPolicy::new(&opts);

        let hinted = task(905, "python-worker", 80 * MS, 100, 0, 0, 0);
        assert_eq!(policy.classify_task(&hinted), TaskClass::ColdStart);

        let mut hinted_again = hinted;
        let (first_class, _, _, _) = policy.update_enqueued(&mut hinted_again, 1);
        assert_eq!(first_class, TaskClass::ColdStart);

        let mut hinted_repeat = task(905, "python-worker", 80 * MS, 100, 0, 0, 0);
        let (second_class, _, _, _) = policy.update_enqueued(&mut hinted_repeat, 2);
        assert_eq!(second_class, TaskClass::ColdStart);

        let hinted_third = task(905, "python-worker", 80 * MS, 100, 0, 0, 0);
        assert_eq!(
            policy.classify_task(&hinted_third),
            TaskClass::HotInvocation
        );
    }

    #[test]
    fn metadata_classification_overrides_heuristic_hint() {
        let opts = opts(&["--slo-target-us", "10000", "--invocation-comm", "python"]);
        let policy = SchedulerPolicy::new(&opts);

        let t = task_with_meta(906, "python-worker", 5 * MS, 100, 2, 0, 100 * MS);
        assert_eq!(policy.classify_task(&t), TaskClass::Background);
    }

    #[test]
    fn pool_assignment_matches_class() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        // Latency-critical metadata → Latency pool
        let mut t = task_with_meta(905, "worker", 5 * MS, 100, 0, 0, 100 * MS);
        let (class, pool, _, _) = policy.update_enqueued(&mut t, 1);
        assert_eq!(class, TaskClass::ColdStart);
        assert_eq!(pool, TaskPool::Latency);

        // Batch metadata → Batch pool
        let mut t2 = task_with_meta(906, "worker", 5 * MS, 100, 2, 0, 100 * MS);
        let (class2, pool2, _, _) = policy.update_enqueued(&mut t2, 2);
        assert_eq!(class2, TaskClass::Background);
        assert_eq!(pool2, TaskPool::Batch);

        // No metadata but short runtime → heuristic ColdStart, Latency pool
        let mut t3 = task(907, "worker", 5 * MS, 100, 0, 0, 0);
        let (class3, pool3, _, _) = policy.update_enqueued(&mut t3, 3);
        assert_eq!(class3, TaskClass::ColdStart);
        assert_eq!(pool3, TaskPool::Latency);

        assert_eq!(policy.nr_metadata_classified, 2);
        assert_eq!(policy.nr_heuristic_classified, 1);
        assert_eq!(policy.nr_pool_latency, 2);
        assert_eq!(policy.nr_pool_batch, 1);
    }

    #[test]
    fn pools_can_be_disabled_for_phase_benchmarks() {
        let opts = opts(&["--slo-target-us", "10000", "--disable-pools"]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;
        let mut t = task_with_meta(908, "worker", 1 * MS, 100, 0, 0, now + MS);
        let (_, pool, _, _) = policy.update_enqueued(&mut t, now);

        assert_eq!(pool, TaskPool::None);
        assert_eq!(policy.nr_tail_guard_dispatches, 0);
    }

    // =========================================================================
    // Phase 3: Tail Guard Mechanism tests
    // =========================================================================

    #[test]
    fn tail_guard_promotion_when_slack_below_threshold() {
        // SLO target = 10ms, threshold defaults to 5ms (half)
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);
        assert_eq!(policy.tail_guard_threshold_ns, 5 * MS);

        // Task with deadline only 3ms in the future (slack = 3ms < threshold 5ms)
        let now = 100 * MS;
        let deadline = now + 3 * MS;
        let mut t = task_with_meta(1001, "worker", 1 * MS, 100, 0, 0, deadline);
        let (_class, pool, _score, _slice) = policy.update_enqueued(&mut t, now);
        assert_eq!(
            pool,
            TaskPool::TailGuard,
            "should be promoted to tail guard"
        );
        assert_eq!(policy.nr_tail_guard_dispatches, 1);
    }

    #[test]
    fn no_tail_guard_when_slack_above_threshold() {
        // SLO target = 10ms, threshold = 5ms
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        // Task with deadline 20ms in the future (slack = 20ms > threshold 5ms)
        let now = 100 * MS;
        let deadline = now + 20 * MS;
        let mut t = task_with_meta(1002, "worker", 1 * MS, 100, 0, 0, deadline);
        let (_class, pool, _score, _slice) = policy.update_enqueued(&mut t, now);
        assert_eq!(pool, TaskPool::Latency, "should stay in latency pool");
        assert_eq!(policy.nr_tail_guard_dispatches, 0);
    }

    #[test]
    fn slo_violation_detected_when_past_deadline() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        // Task with deadline already in the past
        let now = 200 * MS;
        let deadline = 100 * MS; // expired 100ms ago
        let mut t = task_with_meta(1003, "worker", 1 * MS, 100, 0, 0, deadline);
        let (_class, pool, _score, _slice) = policy.update_enqueued(&mut t, now);

        // Overdue work is counted but not promoted into tail guard.
        assert_eq!(pool, TaskPool::Latency);
        assert_eq!(policy.nr_tail_guard_dispatches, 0);
        // Should count as SLO violation
        assert_eq!(policy.nr_slo_violations, 1);
    }

    #[test]
    fn no_slo_violation_when_within_deadline() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;
        let deadline = now + 20 * MS;
        let mut t = task_with_meta(1004, "worker", 1 * MS, 100, 0, 0, deadline);
        policy.update_enqueued(&mut t, now);
        assert_eq!(policy.nr_slo_violations, 0);
    }

    #[test]
    fn tail_guard_gets_edf_scoring() {
        // Two tasks with different deadlines — earlier deadline should get lower score
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;
        let early_deadline = now + 2 * MS;
        let late_deadline = now + 4 * MS;

        let mut t_early = task_with_meta(1005, "worker", 1 * MS, 100, 0, 0, early_deadline);
        let (_, pool_e, score_early, _) = policy.update_enqueued(&mut t_early, now);
        assert_eq!(pool_e, TaskPool::TailGuard);

        let mut t_late = task_with_meta(1006, "worker", 1 * MS, 100, 0, 0, late_deadline);
        let (_, pool_l, score_late, _) = policy.update_enqueued(&mut t_late, now);
        assert_eq!(pool_l, TaskPool::TailGuard);

        // EDF: earlier deadline → lower score → dispatched first
        assert!(
            score_early < score_late,
            "earlier deadline should have lower score: {} vs {}",
            score_early,
            score_late
        );
    }

    #[test]
    fn tail_guard_gets_full_slo_slice() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;
        let deadline = now + 2 * MS; // tight deadline → tail guard
        let mut t = task_with_meta(1007, "worker", 1 * MS, 100, 0, 0, deadline);
        let (_, pool, _, slice_ns) = policy.update_enqueued(&mut t, now);
        assert_eq!(pool, TaskPool::TailGuard);
        // Tail guard tasks get full SLO budget
        assert_eq!(slice_ns, policy.slo_target_ns);
    }

    #[test]
    fn tail_guard_threshold_cli_override() {
        // Override threshold to 2ms
        let opts = opts(&[
            "--slo-target-us",
            "10000",
            "--tail-guard-threshold-us",
            "2000",
        ]);
        let mut policy = SchedulerPolicy::new(&opts);
        assert_eq!(policy.tail_guard_threshold_ns, 2 * MS);

        let now = 100 * MS;

        // 3ms slack — above 2ms threshold → NOT tail guard
        let mut t1 = task_with_meta(1008, "worker", 1 * MS, 100, 0, 0, now + 3 * MS);
        let (_, pool1, _, _) = policy.update_enqueued(&mut t1, now);
        assert_eq!(pool1, TaskPool::Latency);

        // 1ms slack — below 2ms threshold → tail guard
        let mut t2 = task_with_meta(1009, "worker", 1 * MS, 100, 0, 0, now + 1 * MS);
        let (_, pool2, _, _) = policy.update_enqueued(&mut t2, now);
        assert_eq!(pool2, TaskPool::TailGuard);
    }

    #[test]
    fn tail_guard_disabled_with_zero_threshold() {
        // Explicitly disable tail guard promotion
        let opts = opts(&["--slo-target-us", "10000", "--tail-guard-threshold-us", "0"]);
        let mut policy = SchedulerPolicy::new(&opts);
        assert_eq!(policy.tail_guard_threshold_ns, 0);

        let now = 100 * MS;
        // Even with tight deadline, should NOT be promoted
        let mut t = task_with_meta(1010, "worker", 1 * MS, 100, 0, 0, now + 1 * MS);
        let (_, pool, _, _) = policy.update_enqueued(&mut t, now);
        assert_ne!(pool, TaskPool::TailGuard);
        assert_eq!(policy.nr_tail_guard_dispatches, 0);
    }

    #[test]
    fn no_tail_guard_without_metadata() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        // Task without metadata should never get tail guard, even with tight timing
        let now = 100 * MS;
        let mut t = task(1011, "worker", 1 * MS, 100, 0, 0, 0);
        let (_, pool, _, _) = policy.update_enqueued(&mut t, now);
        assert_eq!(pool, TaskPool::Latency); // heuristic ColdStart → Latency
        assert_eq!(policy.nr_tail_guard_dispatches, 0);
    }

    #[test]
    fn no_tail_guard_without_deadline() {
        // Metadata present but deadline_ns=0 → no tail guard promotion
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;
        let mut t = task_with_meta(1012, "worker", 1 * MS, 100, 0, 0, 0); // deadline=0
        let (_, pool, _, _) = policy.update_enqueued(&mut t, now);
        assert_eq!(pool, TaskPool::Latency); // stays in latency
        assert_eq!(policy.nr_tail_guard_dispatches, 0);
    }

    #[test]
    fn tail_guard_considers_estimated_runtime() {
        // If estimated remaining runtime eats into the slack, it should trigger tail guard
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;
        // First enqueue to build up avg_runtime_ns state
        let mut t1 = task_with_meta(1013, "worker", 8 * MS, 100, 0, 0, now + 20 * MS);
        policy.update_enqueued(&mut t1, now);

        // Second enqueue: deadline 12ms away, but avg_runtime ~8ms → slack ~4ms < 5ms threshold
        let mut t2 = task_with_meta(1013, "worker", 8 * MS, 100, 0, 0, now + 12 * MS);
        let (_, pool, _, _) = policy.update_enqueued(&mut t2, now);
        assert_eq!(
            pool,
            TaskPool::TailGuard,
            "should promote because estimated_remaining eats into slack"
        );
    }

    #[test]
    fn multiple_tail_guard_stats_accumulate() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);
        let now = 100 * MS;

        // 3 tasks with tight deadlines → all promoted to tail guard
        for pid in 2001..2004 {
            let mut t = task_with_meta(pid, "worker", 1 * MS, 100, 0, 0, now + 1 * MS);
            let (_, pool, _, _) = policy.update_enqueued(&mut t, now);
            assert_eq!(pool, TaskPool::TailGuard);
        }
        assert_eq!(policy.nr_tail_guard_dispatches, 3);

        // 1 past-deadline task
        let mut t_late = task_with_meta(2005, "worker", 1 * MS, 100, 0, 0, now - 1 * MS);
        policy.update_enqueued(&mut t_late, now);
        assert_eq!(policy.nr_slo_violations, 1);
        assert_eq!(policy.nr_tail_guard_dispatches, 3);
    }

    // =========================================================================
    // Phase 4: Enhanced EDF Scoring tests
    // =========================================================================

    #[test]
    fn edf_earlier_deadline_gets_lower_score() {
        let opts = opts(&[
            "--slo-target-us",
            "10000",
            "--tail-guard-threshold-us",
            "0",
        ]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;

        // Two tasks with different deadlines, both in latency pool (not tail guard)
        let early_deadline = now + 2 * MS;
        let late_deadline = now + 3 * MS;

        let mut t_early = task_with_meta(3001, "worker", 1 * MS, 100, 0, 0, early_deadline);
        let (_, pool_e, score_early, _) = policy.update_enqueued(&mut t_early, now);
        assert_eq!(pool_e, TaskPool::Latency);

        let mut t_late = task_with_meta(3002, "worker", 1 * MS, 100, 0, 0, late_deadline);
        let (_, pool_l, score_late, _) = policy.update_enqueued(&mut t_late, now);
        assert_eq!(pool_l, TaskPool::Latency);

        // EDF: earlier deadline → lower score
        assert!(
            score_early < score_late,
            "earlier deadline should have lower EDF score: {} vs {}",
            score_early,
            score_late
        );
    }

    #[test]
    fn edf_cold_start_boost_applied() {
        let opts = opts(&["--slo-target-us", "10000", "--cold-start-boost-us", "5000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;
        let deadline = now + 20 * MS;

        // Same deadline, one cold start and one hot invocation
        let mut t_cold = task_with_meta(3003, "worker", 1 * MS, 100, 0, 1, deadline);
        let (class_c, _, score_cold, _) = policy.update_enqueued(&mut t_cold, now);
        assert_eq!(class_c, TaskClass::ColdStart);

        let mut t_hot_bootstrap = task_with_meta(3004, "worker", 1 * MS, 100, 0, 0, deadline);
        let (bootstrap_class, _, _, _) = policy.update_enqueued(&mut t_hot_bootstrap, now);
        assert_eq!(bootstrap_class, TaskClass::ColdStart);

        let mut t_hot = task_with_meta(3004, "worker", 1 * MS, 100, 0, 0, deadline);
        let (class_h, _, score_hot, _) = policy.update_enqueued(&mut t_hot, now + MS);
        assert_eq!(class_h, TaskClass::HotInvocation);

        // Cold start should have LOWER score (boosted) than hot invocation
        assert!(
            score_cold < score_hot,
            "cold start should get EDF boost: cold={} vs hot={}",
            score_cold,
            score_hot
        );
    }

    #[test]
    fn edf_background_penalty_applied() {
        let opts = opts(&[
            "--slo-target-us",
            "10000",
            "--tail-guard-threshold-us",
            "0",
        ]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;
        let deadline = now + 3 * MS;

        // Hot invocation (slo_class=0, no cold start)
        let mut t_hot = task_with_meta(3005, "worker", 1 * MS, 100, 0, 0, deadline);
        let (_, _, score_hot, _) = policy.update_enqueued(&mut t_hot, now);

        // Batch/background (slo_class=2) — same deadline
        let mut t_bg = task_with_meta(3006, "worker", 1 * MS, 100, 2, 0, deadline);
        let (class_bg, _, score_bg, _) = policy.update_enqueued(&mut t_bg, now);
        assert_eq!(class_bg, TaskClass::Background);

        // Background should get a HIGHER score (penalized with slo_target_ns addition)
        // → lower priority than hot invocation
        assert!(
            score_bg > score_hot,
            "background should have higher score (lower priority) than hot: bg={} vs hot={}",
            score_bg,
            score_hot
        );
    }

    #[test]
    fn metadata_deadline_scoring_still_respects_vruntime_progress() {
        let opts = opts(&[
            "--slo-target-us",
            "10000",
            "--tail-guard-threshold-us",
            "0",
        ]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;
        let deadline = now + 3 * MS;

        let mut short = task_with_meta(3007, "worker", 1 * MS, 100, 0, 0, deadline);
        let (_, _, score_short, _) = policy.update_enqueued(&mut short, now);

        let mut long = task_with_meta(3008, "worker", 1 * MS, 100, 0, 0, deadline);
        long.stop_ts = 6 * MS;
        let (_, _, score_long, _) = policy.update_enqueued(&mut long, now);

        assert!(
            score_short < score_long,
            "metadata scoring should preserve fair progress for equal deadlines: short={} long={}",
            score_short,
            score_long
        );
    }

    #[test]
    fn metadata_deadline_boost_is_bounded_for_large_slack() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;
        let deadline = now + 120 * MS;
        let mut t = task_with_meta(3014, "worker", 1 * MS, 100, 0, 0, deadline);
        let (class, _, score, _) = policy.update_enqueued(&mut t, now);

        assert_eq!(class, TaskClass::ColdStart);
        assert!(
            score >= expected_heuristic_score(&policy, &t, class),
            "wide-slack metadata tasks should not get more urgency than the heuristic path: meta={} heur={}",
            score,
            expected_heuristic_score(&policy, &t, class)
        );
    }

    #[test]
    fn no_metadata_uses_heuristic_scoring() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;
        let mut t = task(3009, "worker", 5 * MS, 100, 0, 0, 0);
        let (class, _, score, _) = policy.update_enqueued(&mut t, now);

        assert_eq!(class, TaskClass::ColdStart);
        assert_eq!(score, expected_heuristic_score(&policy, &t, class));
    }

    #[test]
    fn metadata_zero_deadline_uses_heuristic_scoring() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;

        // Task with metadata but deadline_ns=0 → should fall through to heuristic scoring
        let mut t_meta_no_dl = task_with_meta(3011, "worker", 5 * MS, 100, 0, 0, 0);
        let (class, _, score, _) = policy.update_enqueued(&mut t_meta_no_dl, now);

        assert_eq!(class, TaskClass::ColdStart);
        assert_eq!(
            score,
            expected_heuristic_score(&policy, &t_meta_no_dl, class)
        );
    }

    #[test]
    fn deadline_scoring_can_be_disabled_for_metadata_only_baseline() {
        let opts = opts(&["--slo-target-us", "10000", "--disable-deadline-scoring"]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;
        let mut t = task_with_meta(3012, "worker", 5 * MS, 100, 0, 0, now + 20 * MS);
        let (class, _, score, _) = policy.update_enqueued(&mut t, now);

        assert_eq!(class, TaskClass::ColdStart);
        assert_eq!(score, expected_heuristic_score(&policy, &t, class));
    }

    #[test]
    fn infeasible_deadline_falls_back_to_heuristic_scoring() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;
        let deadline = now + 2 * MS;
        let mut t = task_with_meta(3013, "worker", 8 * MS, 100, 0, 0, deadline);
        let (class, _, score, _) = policy.update_enqueued(&mut t, now);

        assert_eq!(class, TaskClass::ColdStart);
        assert_eq!(score, expected_heuristic_score(&policy, &t, class));
    }

    #[test]
    fn mixed_ordering_metadata_preferred_at_equal_score() {
        // Phase 4.2: at equal score and class, metadata tasks should sort before heuristic tasks
        let score = 1000u64;
        let ts = 50u64;

        let meta_task = Task {
            qtask: task_with_meta(4001, "worker", 1 * MS, 100, 0, 0, 100 * MS),
            class: TaskClass::HotInvocation,
            pool: TaskPool::Latency,
            score,
            timestamp: ts,
            slice_ns: 10 * MS,
            has_metadata: true,
        };

        let heuristic_task = Task {
            qtask: task(4002, "worker", 1 * MS, 100, 0, 0, 0),
            class: TaskClass::HotInvocation,
            pool: TaskPool::Batch,
            score,
            timestamp: ts,
            slice_ns: 10 * MS,
            has_metadata: false,
        };

        // Metadata task should sort BEFORE heuristic task (lower ordering)
        assert!(
            meta_task < heuristic_task,
            "metadata task should be dispatched before heuristic task at equal score/class"
        );
        assert!(
            heuristic_task > meta_task,
            "heuristic task should sort after metadata task"
        );
    }

    #[test]
    fn mixed_ordering_score_still_dominates() {
        // Even with metadata preference, a lower score should still win
        let meta_task = Task {
            qtask: task_with_meta(4003, "worker", 1 * MS, 100, 0, 0, 100 * MS),
            class: TaskClass::HotInvocation,
            pool: TaskPool::Latency,
            score: 2000,
            timestamp: 50,
            slice_ns: 10 * MS,
            has_metadata: true,
        };

        let heuristic_task = Task {
            qtask: task(4004, "worker", 1 * MS, 100, 0, 0, 0),
            class: TaskClass::HotInvocation,
            pool: TaskPool::Batch,
            score: 1000, // lower score wins
            timestamp: 50,
            slice_ns: 10 * MS,
            has_metadata: false,
        };

        // Score dominates over metadata preference
        assert!(
            heuristic_task < meta_task,
            "lower score should win even without metadata: heuristic={} vs meta={}",
            heuristic_task.score,
            meta_task.score
        );
    }

    #[test]
    fn btreeset_ordering_with_mixed_tasks() {
        // Verify BTreeSet correctly orders a mix of metadata and heuristic tasks
        let mut tasks = BTreeSet::new();

        // Insert tasks in arbitrary order
        tasks.insert(Task {
            qtask: task(5001, "bg1", 1 * MS, 100, 0, 0, 0),
            class: TaskClass::Background,
            pool: TaskPool::Batch,
            score: 500,
            timestamp: 10,
            slice_ns: 10 * MS,
            has_metadata: false,
        });

        tasks.insert(Task {
            qtask: task_with_meta(5002, "hot1", 1 * MS, 100, 0, 0, 100 * MS),
            class: TaskClass::HotInvocation,
            pool: TaskPool::Latency,
            score: 500,
            timestamp: 10,
            slice_ns: 10 * MS,
            has_metadata: true,
        });

        tasks.insert(Task {
            qtask: task_with_meta(5003, "cold1", 1 * MS, 100, 0, 1, 80 * MS),
            class: TaskClass::ColdStart,
            pool: TaskPool::Latency,
            score: 300,
            timestamp: 10,
            slice_ns: 10 * MS,
            has_metadata: true,
        });

        // pop_first should give us tasks in ascending order:
        // 1. cold1 (score=300) - lowest score
        // 2. hot1 (score=500, metadata=true, class=HotInvocation) - same score, metadata wins
        // 3. bg1 (score=500, metadata=false, class=Background) - same score, no metadata
        let first = tasks.pop_first().unwrap();
        assert_eq!(
            first.qtask.pid, 5003,
            "cold start with lowest score should be first"
        );

        let second = tasks.pop_first().unwrap();
        // score=500 tie: class_rank comparison: HotInvocation(1) vs Background(2)
        // HotInvocation sorts first
        assert_eq!(
            second.qtask.pid, 5002,
            "metadata hot invocation should be second"
        );

        let third = tasks.pop_first().unwrap();
        assert_eq!(third.qtask.pid, 5001, "background heuristic should be last");
    }

    #[test]
    fn edf_scoring_consistent_with_tail_guard() {
        // Verify that tail guard tasks still use EDF scoring via the unified path
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        let now = 100 * MS;

        // Two tail guard tasks with different deadlines
        let deadline_early = now + 2 * MS;
        let deadline_late = now + 4 * MS;

        let mut t_early = task_with_meta(6001, "worker", 1 * MS, 100, 0, 0, deadline_early);
        let (_, pool_e, score_early, _) = policy.update_enqueued(&mut t_early, now);
        assert_eq!(pool_e, TaskPool::TailGuard);

        let mut t_late = task_with_meta(6002, "worker", 1 * MS, 100, 0, 0, deadline_late);
        let (_, pool_l, score_late, _) = policy.update_enqueued(&mut t_late, now);
        assert_eq!(pool_l, TaskPool::TailGuard);

        // EDF scoring should still work for tail guard
        assert!(
            score_early < score_late,
            "tail guard: earlier deadline should get lower score: {} vs {}",
            score_early,
            score_late
        );
    }
}
