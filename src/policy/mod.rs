// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

pub mod cosmos;
pub mod sfs;

use crate::bpf::QueuedTask;
use crate::registry::{InvocationMeta, InvocationRegistry, InvocationState};
use scx_utils::Topology;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SchedulingContext {
    pub nr_online_cpus: u64,
    pub nr_running: u64,
}

impl SchedulingContext {
    pub fn has_idle_capacity(self) -> Option<bool> {
        self.has_idle_capacity_for(0)
    }

    pub fn has_idle_capacity_for(self, nr_cpus_allowed: u64) -> Option<bool> {
        if self.nr_online_cpus == 0 {
            return None;
        }
        let capacity = if nr_cpus_allowed == 0 {
            self.nr_online_cpus
        } else {
            self.nr_online_cpus.min(nr_cpus_allowed)
        };
        if capacity == 0 {
            None
        } else {
            Some(self.nr_running < capacity)
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct PolicyCounters {
    pub nr_cold_start_tasks: u64,
    pub nr_hot_invocation_tasks: u64,
    pub nr_background_tasks: u64,
    pub nr_slo_boosted: u64,
    pub max_pending: u64,
    pub nr_metadata_classified: u64,
    pub nr_heuristic_classified: u64,
    pub nr_short_preemptions: u64,
    pub nr_starvation_guard_dispatches: u64,
    pub nr_slo_violations: u64,
}

#[derive(Debug, Clone)]
pub struct DispatchDecision {
    pub pid: i32,
    pub cpu: i32,
    pub slice_ns: u64,
    pub vtime: u64,
    pub enq_flags: u64,
    pub dispatch_flags: u64,
    pub enq_cnt: u64,
    pub preempt: bool,
}

pub trait SchedulingPolicy {
    type Stats: Clone;
    fn schedule(
        &mut self,
        resolved_state: &[Option<InvocationState>],
        raw_tasks: &[QueuedTask],
        topology: &Topology,
        now_ns: u64,
    ) -> Vec<DispatchDecision>;
    fn schedule_with_context(
        &mut self,
        resolved_state: &[Option<InvocationState>],
        raw_tasks: &[QueuedTask],
        topology: &Topology,
        now_ns: u64,
        _context: SchedulingContext,
    ) -> Vec<DispatchDecision> {
        self.schedule(resolved_state, raw_tasks, topology, now_ns)
    }
    fn tick(&mut self, _registry: &InvocationRegistry, _now_ns: u64) {}
    fn stats(&self) -> Self::Stats;
    fn counters(&self) -> PolicyCounters {
        PolicyCounters::default()
    }
}
