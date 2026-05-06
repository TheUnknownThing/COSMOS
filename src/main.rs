// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

mod bpf_skel;
pub use bpf_skel::*;
pub mod bpf_intf;

#[rustfmt::skip]
mod bpf;
use bpf::*;

mod stats;

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::io;
use std::mem::MaybeUninit;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Result;
use clap::Parser;
use libbpf_rs::OpenObject;
use log::info;
use log::warn;
use procfs::process::Process;
use scx_stats::prelude::*;
use scx_utils::build_id;
use scx_utils::libbpf_clap_opts::LibbpfOpts;
use scx_utils::UserExitInfo;
use stats::Metrics;

pub const SCHEDULER_NAME: &str = "COSMOS";

const NSEC_PER_USEC: u64 = 1_000;
const TASK_STATE_TTL_NS: u64 = 60_000_000_000;

/// COSMOS: invocation-aware user-space scheduler for serverless workloads.
///
/// This MVP is intentionally based on scx_rustland_core. BPF stays policy-agnostic and only
/// forwards runnable tasks to user space; the Rust policy classifies likely serverless invocation
/// work and orders tasks by a latency-oriented score.
///
/// The policy has three classes:
///
/// - ColdStart: first-seen or explicitly hinted runtime workers. These receive the strongest
///   boost because cold starts dominate tail latency.
/// - HotInvocation: short-running workers with repeated wakeups. These receive SLO-aware
///   preference to reduce p99 queueing.
/// - Background: everything else. Background tasks still make forward progress through the
///   inherited vruntime/deadline accounting from scx_rustland.
///
/// Invocation hints are supplied with --invocation-comm. Without hints, the scheduler still
/// detects short sleep/wakeup cycles as invocation-like, which makes the scaffold usable before
/// plumbing in cgroup or runtime-specific metadata.
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

    /// Extra boost in microseconds for first-seen or hinted cold-start tasks.
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
}

#[derive(Debug, Clone)]
struct Task {
    qtask: QueuedTask,
    class: TaskClass,
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

struct SchedulerPolicy<'a> {
    opts: &'a Opts,
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
}

impl<'a> SchedulerPolicy<'a> {
    fn new(opts: &'a Opts) -> Self {
        Self {
            opts,
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
        self.opts
            .invocation_comm
            .iter()
            .any(|needle| !needle.is_empty() && comm.contains(needle))
    }

    fn classify_task(&self, task: &QueuedTask) -> TaskClass {
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

    fn update_enqueued(&mut self, task: &mut QueuedTask, now: u64) -> (TaskClass, u64, u64) {
        let class = self.classify_task(task);
        self.update_vruntime(task);
        let score = self.task_score(task, class);
        let slice_ns = self.task_slice_ns(task, class);
        self.update_task_state(task, now);

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

        (class, score, slice_ns)
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
    policy: SchedulerPolicy<'a>,
    init_page_faults: u64,
}

impl<'a> Scheduler<'a> {
    fn init(opts: &'a Opts, open_object: &'a mut MaybeUninit<OpenObject>) -> Result<Self> {
        let stats_server = StatsServer::new(stats::server_data()).launch()?;
        let policy = SchedulerPolicy::new(opts);

        let bpf = BpfScheduler::init(
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

        Ok(Self {
            bpf,
            opts,
            stats_server,
            tasks: BTreeSet::new(),
            policy,
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
                    let (class, score, slice_ns) =
                        self.policy.update_enqueued(&mut task, timestamp);

                    self.tasks.insert(Task {
                        qtask: task,
                        class,
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

    fn run(&mut self) -> Result<UserExitInfo> {
        let (res_ch, req_ch) = self.stats_server.channels();

        while !self.bpf.exited() {
            self.schedule();

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
        }
    }

    #[test]
    fn classifies_first_seen_tasks_from_slo_and_runtime_hints() {
        let opts = opts(&[
            "--slo-target-us",
            "10000",
            "--invocation-comm",
            "node,bootstrap",
        ]);
        let policy = SchedulerPolicy::new(&opts);

        let short_unhinted = task(101, "worker", 5 * MS, 100, 0, 0, 0);
        let long_hinted = task(102, "node", 80 * MS, 100, 0, 0, 0);
        let long_background = task(103, "postgres", 80 * MS, 100, 0, 0, 0);

        assert_eq!(policy.classify_task(&short_unhinted), TaskClass::ColdStart);
        assert_eq!(policy.classify_task(&long_hinted), TaskClass::ColdStart);
        assert_eq!(
            policy.classify_task(&long_background),
            TaskClass::Background
        );
    }

    #[test]
    fn repeated_short_wakeup_becomes_hot_invocation() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);
        let mut task = task(201, "python", 4 * MS, 100, 0, 1 * MS, 0);

        let (first_class, _, _) = policy.update_enqueued(&mut task, 1);
        let (second_class, _, _) = policy.update_enqueued(&mut task, 2);

        assert_eq!(first_class, TaskClass::ColdStart);
        assert_eq!(second_class, TaskClass::HotInvocation);
        assert_eq!(policy.nr_cold_start_tasks, 1);
        assert_eq!(policy.nr_hot_invocation_tasks, 1);
        assert_eq!(policy.nr_slo_boosted, 2);
    }

    #[test]
    fn runtime_history_keeps_long_running_tasks_in_background() {
        let opts = opts(&["--slo-target-us", "10000"]);
        let mut policy = SchedulerPolicy::new(&opts);
        let pid = 301;
        policy.task_state.insert(
            pid,
            TaskState {
                avg_runtime_ns: 25 * MS,
                wakeups: 5,
                last_seen_ns: 10,
            },
        );

        let task = task(pid, "worker", 4 * MS, 100, 0, 0, 0);

        assert_eq!(policy.classify_task(&task), TaskClass::Background);
    }

    #[test]
    fn slo_boost_orders_invocation_work_before_background() {
        let opts = opts(&["--slo-target-us", "10000", "--cold-start-boost-us", "20000"]);
        let policy = SchedulerPolicy::new(&opts);
        let task = task(401, "worker", 0, 100, 0, 0, 100 * MS);

        let cold = policy.task_score(&task, TaskClass::ColdStart);
        let hot = policy.task_score(&task, TaskClass::HotInvocation);
        let background = policy.task_score(&task, TaskClass::Background);

        assert!(cold < hot, "cold-start score should dispatch first");
        assert!(
            hot < background,
            "hot invocation score should beat background"
        );
    }

    #[test]
    fn slices_are_latency_oriented_but_respect_minimum() {
        let opts = opts(&[
            "--slice-us",
            "20000",
            "--slice-us-min",
            "500",
            "--slo-target-us",
            "10000",
        ]);
        let policy = SchedulerPolicy::new(&opts);
        let default_weight = task(501, "worker", 0, 100, 0, 0, 0);
        let low_weight = task(502, "worker", 0, 10, 0, 0, 0);

        assert_eq!(
            policy.task_slice_ns(&default_weight, TaskClass::ColdStart),
            2_500_000
        );
        assert_eq!(
            policy.task_slice_ns(&default_weight, TaskClass::HotInvocation),
            1_250_000
        );
        assert_eq!(
            policy.task_slice_ns(&default_weight, TaskClass::Background),
            20_000_000
        );
        assert_eq!(
            policy.task_slice_ns(&low_weight, TaskClass::HotInvocation),
            500_000
        );
    }

    #[test]
    fn vruntime_accounts_executed_slice_and_task_weight() {
        let opts = opts(&["--slice-us", "20000"]);
        let mut policy = SchedulerPolicy::new(&opts);
        let mut default_weight = task(601, "worker", 50 * MS, 100, 2 * MS, 6 * MS, 0);
        let mut double_weight = task(602, "worker", 50 * MS, 200, 6 * MS, 10 * MS, 0);

        policy.update_vruntime(&mut default_weight);
        assert_eq!(default_weight.vtime, 4 * MS);
        assert_eq!(policy.vruntime_now, 4 * MS);

        policy.update_vruntime(&mut double_weight);
        assert_eq!(double_weight.vtime, 6 * MS);
        assert_eq!(policy.vruntime_now, 6 * MS);
    }

    #[test]
    fn btree_order_uses_score_class_timestamp_and_pid() {
        let qtask = task(701, "worker", 0, 100, 0, 0, 0);
        let mut tasks = BTreeSet::new();

        tasks.insert(Task {
            qtask: task(703, "worker", 0, 100, 0, 0, 0),
            class: TaskClass::Background,
            score: 10,
            timestamp: 1,
            slice_ns: 1,
        });
        tasks.insert(Task {
            qtask: qtask.clone(),
            class: TaskClass::ColdStart,
            score: 10,
            timestamp: 1,
            slice_ns: 1,
        });
        tasks.insert(Task {
            qtask: task(702, "worker", 0, 100, 0, 0, 0),
            class: TaskClass::HotInvocation,
            score: 10,
            timestamp: 1,
            slice_ns: 1,
        });

        assert_eq!(tasks.pop_first().unwrap().class, TaskClass::ColdStart);
        assert_eq!(tasks.pop_first().unwrap().class, TaskClass::HotInvocation);
        assert_eq!(tasks.pop_first().unwrap().class, TaskClass::Background);
    }

    #[test]
    fn task_state_pruning_keeps_recent_workers() {
        let opts = opts(&[]);
        let mut policy = SchedulerPolicy::new(&opts);
        let now = TASK_STATE_TTL_NS * 2;

        policy.task_state.insert(
            801,
            TaskState {
                avg_runtime_ns: 1,
                wakeups: 1,
                last_seen_ns: now - TASK_STATE_TTL_NS - 1,
            },
        );
        policy.task_state.insert(
            802,
            TaskState {
                avg_runtime_ns: 1,
                wakeups: 1,
                last_seen_ns: now - TASK_STATE_TTL_NS + 1,
            },
        );

        policy.prune_task_state(now);

        assert!(!policy.task_state.contains_key(&801));
        assert!(policy.task_state.contains_key(&802));
    }
}
