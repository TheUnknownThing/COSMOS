// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

pub mod cosmos;
pub mod cosmos_pool;
pub mod sfs;

use cosmos::CosmosCounters;

pub type PolicyCounters = CosmosCounters;

use crate::bpf::QueuedTask;
use crate::registry::InvocationRegistry;
use scx_utils::Topology;

/// A single scheduling decision: dispatch this task with these parameters.
#[derive(Debug, Clone)]
pub struct DispatchDecision {
    pub pid: i32,
    pub cpu: i32,
    pub slice_ns: u64,
    pub vtime: u64,
    pub pool: u32,
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
    /// Returns pending CPU assignment changes to apply.
    fn tick(&mut self, _registry: &InvocationRegistry, _now_ns: u64) -> Vec<(u32, u32)> {
        Vec::new()
    }

    /// Called once before the main scheduling loop. Returns initial CPU
    /// assignments for the scheduler to apply.
    fn init(&mut self, _nr_cpus: usize, _tail_guard_cpus: u32) -> Vec<(u32, u32)> {
        Vec::new()
    }

    /// Snapshot of policy-specific statistics.
    fn stats(&self) -> Self::Stats;

    /// Policy counters for metrics composition. Default returns zeros.
    fn counters(&self) -> CosmosCounters {
        CosmosCounters::default()
    }
}
