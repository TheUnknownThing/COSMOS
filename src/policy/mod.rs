// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

pub mod cosmos;
pub mod cosmos_pool;
pub mod sfs;

use crate::bpf::QueuedTask;
use crate::registry::InvocationRegistry;
use scx_utils::Topology;

/// Generic policy counters exposed through the scheduling trait.
/// Pool-aware policies (e.g. COSMOS) expose richer stats via `stats()`.
#[derive(Debug, Default, Clone)]
pub struct PolicyCounters {
    pub nr_cold_start_tasks: u64,
    pub nr_hot_invocation_tasks: u64,
    pub nr_background_tasks: u64,
    pub nr_slo_boosted: u64,
    pub max_pending: u64,
    pub nr_metadata_classified: u64,
    pub nr_heuristic_classified: u64,
    pub nr_pool_latency: u64,
    pub nr_pool_batch: u64,
    pub nr_tail_guard_dispatches: u64,
    pub nr_slo_violations: u64,
    pub nr_pool_migrations: u64,
    pub nr_latency_pool_borrows: u64,
    pub nr_batch_pool_borrows: u64,
}

/// A single scheduling decision: dispatch this task with these parameters.
#[derive(Debug, Clone)]
pub struct DispatchDecision {
    pub pid: i32,
    pub cpu: i32,
    pub slice_ns: u64,
    pub vtime: u64,
    pub enq_flags: u64,
    pub enq_cnt: u64,
}

/// Scheduling policy trait.
pub trait SchedulingPolicy {
    /// Policy-specific statistics exposed to monitoring.
    type Stats: Clone;

    /// Given the registry, raw kernel tasks, and topology, produce ranked
    /// dispatch decisions. The main loop dispatches in returned order.
    fn schedule(
        &mut self,
        registry: &InvocationRegistry,
        raw_tasks: &[QueuedTask],
        topology: &Topology,
        now_ns: u64,
    ) -> Vec<DispatchDecision>;

    /// Periodic housekeeping (state pruning, pool rebalancing, etc.).
    fn tick(&mut self, _registry: &InvocationRegistry, _now_ns: u64) {}

    /// Called once before the main scheduling loop.
    fn init(&mut self, _nr_cpus: usize, _tail_guard_cpus: u32) {}

    /// Snapshot of policy-specific statistics.
    fn stats(&self) -> Self::Stats;

    /// Policy counters for metrics composition. Default returns zeros.
    fn counters(&self) -> PolicyCounters {
        PolicyCounters::default()
    }
}
