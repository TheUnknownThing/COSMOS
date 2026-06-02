// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

pub mod cosmos;
pub mod cosmos_pool;
pub mod sfs;

use crate::bpf::QueuedTask;
use crate::registry::{InvocationMeta, InvocationRegistry};
use scx_utils::Topology;

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
    pub nr_starvation_guard_dispatches: u64,
    pub nr_slo_violations: u64,
    pub nr_pool_migrations: u64,
    pub nr_pool_overflow: u64,
}

#[derive(Debug, Clone)]
pub struct DispatchDecision {
    pub pid: i32,
    pub cpu: i32,
    pub slice_ns: u64,
    pub vtime: u64,
    pub enq_flags: u64,
    pub enq_cnt: u64,
}

pub trait SchedulingPolicy {
    type Stats: Clone;
    fn schedule(
        &mut self,
        resolved_meta: &[Option<InvocationMeta>],
        raw_tasks: &[QueuedTask],
        topology: &Topology,
        now_ns: u64,
    ) -> Vec<DispatchDecision>;
    fn tick(&mut self, _registry: &InvocationRegistry, _now_ns: u64) {}
    fn init(&mut self, _nr_cpus: usize, _tail_guard_cpus: u32) {}
    fn stats(&self) -> Self::Stats;
    fn counters(&self) -> PolicyCounters {
        PolicyCounters::default()
    }
}
