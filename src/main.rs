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
use std::time::SystemTime;

use anyhow::Result;
use clap::Parser;
use libbpf_rs::OpenObject;
use log::info;
use log::debug;
use log::warn;
use procfs::process::Process;
use scx_stats::prelude::*;
use scx_utils::build_id;
use scx_utils::libbpf_clap_opts::LibbpfOpts;
use scx_utils::UserExitInfo;
use pool::PoolManager;
use pool::PoolMetrics;
use stats::Metrics;

pub const SCHEDULER_NAME: &str = "COSMOS";

const NSEC_PER_USEC: u64 = 1_000;
const TASK_STATE_TTL_NS: u64 = 60_000_000_000;

/// COSMOS: invocation-aware user-space scheduler for serverless workloads.
///
/// Built on scx_rustland_core. BPF stays policy-agnostic and only forwards
/// runnable tasks to user space; the Rust policy classifies tasks using
/// invocation metadata written by the shim library (libcosmos_meta.so) and
/// orders them by a latency-oriented score.
///
/// The policy has three classes:
///
/// - ColdStart: explicitly marked cold-start invocations (is_cold_start=1).
///   These receive the strongest boost because cold starts dominate tail latency.
/// - HotInvocation: latency-critical or standard SLO tasks (slo_class <= 1).
///   These receive SLO-aware preference to reduce p99 queueing.
/// - Background: batch tasks (slo_class >= 2) and any tasks without metadata.
///   Background tasks still make forward progress through vruntime accounting.
///
/// Invocation metadata is supplied by the shim library via a pinned BPF map.
/// The scheduler requires the shim to be loaded for proper classification.
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

/// CPU pool assignment for dispatched tasks.
/// Maps to the cosmos_pool enum in intf.h.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(u32)]
enum TaskPool {
    None = 0,
    Latency = 1,
    Batch = 2,
    TailGuard = 3,
}

#[derive(Debug, Default, Clone)]
struct TaskState {
    avg_runtime_ns: u64,
    wakeups: u64,
    last_seen_ns: u64,
}

#[derive(Debug, Clone)]
struct Task {
    qtask: QueuedTask,
    class: TaskClass,
    pool: TaskPool,
    score: u64,
    timestamp: u64,
    slice_ns: u64,
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
            && class_rank(self.class) == class_rank(other.class)
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
    nr_cold_start_tasks: u64,
    nr_hot_invocation_tasks: u64,
    nr_background_tasks: u64,
    nr_slo_boosted: u64,
    max_pending: u64,
    nr_metadata_classified: u64,
    nr_pool_latency: u64,
    nr_pool_batch: u64,
}

impl SchedulerPolicy {
    fn new(opts: &Opts) -> Self {
        Self {
            task_state: HashMap::new(),
            vruntime_now: 0,
            slice_ns: opts.slice_us * NSEC_PER_USEC,
            slice_ns_min: opts.slice_us_min * NSEC_PER_USEC,
            slo_target_ns: opts.slo_target_us * NSEC_PER_USEC,
            cold_start_boost_ns: opts.cold_start_boost_us * NSEC_PER_USEC,
            nr_cold_start_tasks: 0,
            nr_hot_invocation_tasks: 0,
            nr_background_tasks: 0,
            nr_slo_boosted: 0,
            max_pending: 0,
            nr_metadata_classified: 0,
            nr_pool_latency: 0,
            nr_pool_batch: 0,
        }
    }

    fn scale_by_task_weight(task: &QueuedTask, value: u64) -> u64 {
        value.saturating_mul(task.weight) / 100
    }

    fn scale_by_task_weight_inverse(task: &QueuedTask, value: u64) -> u64 {
        value.saturating_mul(100) / task.weight.max(1)
    }

    fn classify_task(&self, task: &QueuedTask) -> TaskClass {
        // Classification via invocation metadata from the shim library.
        // Tasks without metadata are classified as Background.
        if task.has_invocation_meta != 1 {
            return TaskClass::Background;
        }

        if task.is_cold_start == 1 {
            TaskClass::ColdStart
        } else if task.slo_class <= 1 {
            // 0=latency-critical, 1=standard
            TaskClass::HotInvocation
        } else {
            // 2=batch
            TaskClass::Background
        }
    }

    fn update_task_state(&mut self, task: &QueuedTask, now: u64) {
        let state = self.task_state.entry(task.pid).or_default();
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

    fn task_score(&self, task: &QueuedTask, class: TaskClass) -> u64 {
        let runtime_penalty = task.exec_runtime.min(self.slice_ns.saturating_mul(100));
        let fair_deadline = task.vtime.saturating_add(runtime_penalty);
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

    fn task_slice_ns(&self, task: &QueuedTask, class: TaskClass) -> u64 {
        let base = match class {
            TaskClass::ColdStart => self.slo_target_ns / 4,
            TaskClass::HotInvocation => self.slo_target_ns / 8,
            TaskClass::Background => self.slice_ns,
        };

        Self::scale_by_task_weight(task, base.max(self.slice_ns_min)).max(self.slice_ns_min)
    }

    fn update_enqueued(&mut self, task: &mut QueuedTask, now: u64) -> (TaskClass, TaskPool, u64, u64) {
        let class = self.classify_task(task);
        self.update_vruntime(task);
        let score = self.task_score(task, class);
        let slice_ns = self.task_slice_ns(task, class);
        self.update_task_state(task, now);

        // Phase 1: pool assignment based on classification
        let pool = match class {
            TaskClass::ColdStart | TaskClass::HotInvocation => TaskPool::Latency,
            TaskClass::Background => TaskPool::Batch,
        };

        // Track whether this was classified via metadata
        if task.has_invocation_meta == 1 {
            self.nr_metadata_classified = self.nr_metadata_classified.saturating_add(1);
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
            _ => {}
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
    policy: SchedulerPolicy,
    pool_manager: PoolManager,
    init_page_faults: u64,
}

impl<'a> Scheduler<'a> {
    fn init(opts: &'a Opts, open_object: &'a mut MaybeUninit<OpenObject>) -> Result<Self> {
        let stats_server = StatsServer::new(stats::server_data()).launch()?;
        let policy = SchedulerPolicy::new(opts);

        let mut bpf = BpfScheduler::init(
            open_object,
            opts.libbpf.clone().into_bpf_open_opts(),
            opts.exit_dump_len,
            opts.partial,
            opts.verbose,
            true,
            opts.numa_local,
            policy.slice_ns_min,
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
        let pool_manager = PoolManager::new(
            nr_cpus,
            opts.latency_pool_pct,
            opts.tail_guard_cpus,
        );

        // Write initial pool assignments to the BPF cpu_pool_map
        pool_manager.apply_all(|cpu, pool| {
            if let Err(e) = bpf.update_cpu_pool(cpu, pool) {
                log::warn!("Failed to set initial pool for CPU {}: {}", cpu, e);
            }
        });
        info!(
            "Phase 2: Pool manager initialized ({} CPUs, {}% latency, {} tail guard, {}ms rebalance)",
            nr_cpus, opts.latency_pool_pct, opts.tail_guard_cpus, opts.pool_rebalance_ms
        );

        Ok(Self {
            bpf,
            opts,
            stats_server,
            tasks: BTreeSet::new(),
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
            nr_pool_latency: self.policy.nr_pool_latency,
            nr_pool_batch: self.policy.nr_pool_batch,
            nr_pool_migrations: self.pool_manager.nr_pool_migrations,
        }
    }

    fn now() -> u64 {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        ts.as_nanos() as u64
    }

    fn dispatch_task(&mut self) -> bool {
        let Some(task) = self.tasks.pop_first() else {
            return true;
        };

        let mut dispatched_task = DispatchedTask::new(&task.qtask);
        dispatched_task.slice_ns = task.slice_ns;
        dispatched_task.vtime = task.score;
        dispatched_task.pool = task.pool as u32;  // Phase 1: pass pool to BPF

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
                    let (class, pool, score, slice_ns) =
                        self.policy.update_enqueued(&mut task, timestamp);

                    self.tasks.insert(Task {
                        qtask: task,
                        class,
                        pool,
                        score,
                        timestamp,
                        slice_ns,
                    });
                }
                Ok(None) => break,
                Err(err) => {
                    warn!("Error: {err}");
                    break;
                }
            }
        }
    }

    fn schedule(&mut self) {
        self.drain_queued_tasks();
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
            if last_rebalance.elapsed() >= pool_rebalance_interval {
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

    #[test]
    fn no_metadata_classifies_as_background() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let policy = SchedulerPolicy::new(&opts);

        // Tasks without metadata (e.g. system tasks) → always Background
        let t = task(101, "worker", 5 * MS, 100, 0, 0, 0);
        assert_eq!(policy.classify_task(&t), TaskClass::Background);

        let t2 = task(102, "node", 80 * MS, 100, 0, 0, 0);
        assert_eq!(policy.classify_task(&t2), TaskClass::Background);
    }

    #[test]
    fn metadata_classifies_latency_critical_as_hot_invocation() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let policy = SchedulerPolicy::new(&opts);

        // slo_class=0 (latency-critical), not cold start
        let t = task_with_meta(901, "worker", 80 * MS, 100, 0, 0, 100 * MS);
        assert_eq!(policy.classify_task(&t), TaskClass::HotInvocation);
    }

    #[test]
    fn metadata_classifies_standard_as_hot_invocation() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let policy = SchedulerPolicy::new(&opts);

        // slo_class=1 (standard), not cold start
        let t = task_with_meta(901, "worker", 5 * MS, 100, 1, 0, 100 * MS);
        assert_eq!(policy.classify_task(&t), TaskClass::HotInvocation);
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
    fn no_metadata_vs_metadata_classification() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let policy = SchedulerPolicy::new(&opts);

        // Without metadata → Background regardless of runtime
        let no_meta = task(904, "worker", 5 * MS, 100, 0, 0, 0);
        assert_eq!(policy.classify_task(&no_meta), TaskClass::Background);

        // With metadata slo_class=0 → HotInvocation
        let with_meta = task_with_meta(904, "worker", 80 * MS, 100, 0, 0, 100 * MS);
        assert_eq!(policy.classify_task(&with_meta), TaskClass::HotInvocation);
    }

    #[test]
    fn pool_assignment_matches_class() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);

        // Latency-critical metadata → Latency pool
        let mut t = task_with_meta(905, "worker", 5 * MS, 100, 0, 0, 100 * MS);
        let (class, pool, _, _) = policy.update_enqueued(&mut t, 1);
        assert_eq!(class, TaskClass::HotInvocation);
        assert_eq!(pool, TaskPool::Latency);

        // Batch metadata → Batch pool
        let mut t2 = task_with_meta(906, "worker", 5 * MS, 100, 2, 0, 100 * MS);
        let (class2, pool2, _, _) = policy.update_enqueued(&mut t2, 2);
        assert_eq!(class2, TaskClass::Background);
        assert_eq!(pool2, TaskPool::Batch);

        // No metadata → Background, Batch pool
        let mut t3 = task(907, "worker", 5 * MS, 100, 0, 0, 0);
        let (class3, pool3, _, _) = policy.update_enqueued(&mut t3, 3);
        assert_eq!(class3, TaskClass::Background);
        assert_eq!(pool3, TaskPool::Batch);

        assert_eq!(policy.nr_metadata_classified, 2);
        assert_eq!(policy.nr_pool_latency, 1);
        assert_eq!(policy.nr_pool_batch, 2);
    }
}
