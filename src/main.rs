// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

mod bpf_skel;
pub use bpf_skel::*;
pub mod bpf_intf;

#[rustfmt::skip]
mod bpf;
use bpf::*;

mod stats;
mod metadata;

mod registry;
mod adapter;
mod policy;
mod scheduler;

use std::io;
use std::mem::MaybeUninit;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use log::info;
use scx_stats::prelude::*;
use scx_utils::build_id;
use scx_utils::libbpf_clap_opts::LibbpfOpts;

use adapter::scx::ScxAdapter;
use metadata::spawn_metadata_listener;
use policy::cosmos::CosmosPolicy;
use policy::cosmos_pool::{effective_tail_guard_cpus, PoolManager};
use registry::{InvocationRegistry, RegistryHandle};
use scheduler::Scheduler;

pub const SCHEDULER_NAME: &str = "COSMOS";

const NSEC_PER_USEC: u64 = 1_000;

pub fn monotonic_now_ns() -> u64 {
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

/// COSMOS v2: invocation-centric user-space scheduler for serverless workloads.
///
/// Architecture:
///   Layer 1: Invocation Metadata Registry (canonical metadata store)
///   Layer 2: CPU Scheduling Adapter (mechanism, abstracts kernel interface)
///   Layer 3: Scheduling Policy (pluggable via trait)
///
/// Built on scx_rustland_core. BPF stays policy-agnostic and only forwards
/// runnable tasks to user space; the Rust policy classifies tasks using
/// invocation metadata from the registry and orders them by a latency-oriented
/// score.
#[derive(Debug, Parser)]
struct Opts {
    /// Scheduling slice duration in microseconds.
    #[clap(short = 's', long, default_value = "20000")]
    slice_us: u64,

    /// Scheduling minimum slice duration in microseconds.
    #[clap(short = 'S', long, default_value = "500")]
    slice_us_min: u64,

    /// Target invocation SLO in microseconds.
    #[clap(long, default_value = "10000")]
    slo_target_us: u64,

    /// Extra boost in microseconds for cold-start tasks.
    #[clap(long, default_value = "20000")]
    cold_start_boost_us: u64,

    /// Treat tasks whose comm contains these strings as invocation workers.
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

    /// Percentage of CPUs to assign to the latency pool.
    #[clap(long, default_value = "50")]
    latency_pool_pct: u32,

    /// Number of CPUs to reserve for the tail guard pool (0 = disabled).
    #[clap(long, default_value = "0")]
    tail_guard_cpus: u32,

    /// Pool rebalance interval in milliseconds.
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

    /// Slack threshold in microseconds below which tasks are promoted to tail guard.
    #[clap(long)]
    tail_guard_threshold_us: Option<u64>,

    /// Enable stats monitoring with the specified interval.
    #[clap(long)]
    stats: Option<f64>,

    /// Run in stats monitoring mode with the specified interval.
    #[clap(long)]
    monitor: Option<f64>,

    /// TCP port for metadata ingestion from the event bridge.
    #[clap(long, default_value = "9732")]
    metadata_port: u16,

    /// Show descriptions for statistics.
    #[clap(long)]
    help_stats: bool,

    /// Print scheduler version and exit.
    #[clap(short = 'V', long, action = clap::ArgAction::SetTrue)]
    version: bool,

    #[clap(flatten, next_help_heading = "Libbpf Options")]
    pub libbpf: LibbpfOpts,
}

/// Framework-level options extracted from CLI, used to construct the policy.
#[derive(Debug, Clone)]
pub struct CosmosOpts {
    pub slice_us: u64,
    pub slice_us_min: u64,
    pub slo_target_us: u64,
    pub cold_start_boost_us: u64,
    pub invocation_comm: Vec<String>,
    pub percpu_local: bool,
    pub tail_guard_threshold_us: Option<u64>,
    pub disable_pools: bool,
    pub disable_deadline_scoring: bool,
    pub latency_pool_pct: u32,
    pub pool_rebalance_ms: u64,
}

impl From<&Opts> for CosmosOpts {
    fn from(opts: &Opts) -> Self {
        Self {
            slice_us: opts.slice_us,
            slice_us_min: opts.slice_us_min,
            slo_target_us: opts.slo_target_us,
            cold_start_boost_us: opts.cold_start_boost_us,
            invocation_comm: opts.invocation_comm.clone(),
            percpu_local: opts.percpu_local,
            tail_guard_threshold_us: opts.tail_guard_threshold_us,
            disable_pools: opts.disable_pools,
            disable_deadline_scoring: opts.disable_deadline_scoring,
            latency_pool_pct: opts.latency_pool_pct,
            pool_rebalance_ms: opts.pool_rebalance_ms,
        }
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
    let cosmos_opts: CosmosOpts = (&opts).into();
    let registry: RegistryHandle = Arc::new(RwLock::new(InvocationRegistry::new()));

    loop {
        let stats_server = StatsServer::new(stats::server_data()).launch()?;

        let mut bpf = BpfScheduler::init(
            &mut open_object,
            opts.libbpf.clone().into_bpf_open_opts(),
            opts.exit_dump_len,
            opts.partial,
            opts.verbose,
            !opts.disable_builtin_idle,
            opts.numa_local,
            cosmos_opts.slice_us_min * NSEC_PER_USEC,
            (cosmos_opts.slice_us * NSEC_PER_USEC).max(cosmos_opts.slice_us_min * NSEC_PER_USEC),
            "cosmos",
        )?;

        let nr_cpus = *bpf.nr_online_cpus_mut() as usize;
        let effective_tg = effective_tail_guard_cpus(nr_cpus, opts.tail_guard_cpus);
        let auto_disable_small = nr_cpus <= 4;

        info!(
            "{} version {} - scx_rustland_core {}",
            SCHEDULER_NAME,
            build_id::full_version(env!("CARGO_PKG_VERSION")),
            scx_rustland_core::VERSION
        );

        if auto_disable_small {
            info!("Auto-disabling pools on {}-CPU host to avoid partitioning limited CPU capacity", nr_cpus);
        }

        let mut policy = CosmosPolicy::new(&cosmos_opts);

        // Override pool/deadline settings for small hosts
        if auto_disable_small {
            policy.pools_enabled = false;
            policy.deadline_scoring_enabled = false;
        }

        // Create pool manager
        let pool_mgr = if cosmos_opts.disable_pools || auto_disable_small {
            PoolManager::disabled(nr_cpus)
        } else {
            PoolManager::new(nr_cpus, cosmos_opts.latency_pool_pct, effective_tg)
        };

        let adapter = ScxAdapter::new(bpf)?;

        // Spawn metadata ingestion listener (receives from event bridge)
        let _metadata_handle = spawn_metadata_listener(registry.clone(), opts.metadata_port);

        let mut sched = Scheduler::new(
            registry.clone(),
            adapter,
            policy,
            pool_mgr,
            stats_server,
        );

        if !sched.run()?.should_restart() {
            break;
        }
    }

    Ok(())
}
