// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

pub mod cosmos;
pub mod cosmos_pool;

use cosmos::CosmosCounters;

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

    /// Periodic housekeeping (state pruning, etc.).
    fn tick(&mut self, _registry: &InvocationRegistry, _now_ns: u64) {}

    /// Snapshot of policy-specific statistics.
    fn stats(&self) -> Self::Stats;

    /// Policy counters for metrics composition. Default returns zeros.
    fn counters(&self) -> CosmosCounters {
        CosmosCounters::default()
    }
}
