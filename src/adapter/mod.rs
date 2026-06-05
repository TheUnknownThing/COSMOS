// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

pub mod scx;

use crate::bpf::QueuedTask;
use anyhow::Result;
use scx_utils::Topology;
use scx_utils::UserExitInfo;
use std::time::Duration;

/// Raw BPF counter values exposed by the adapter.
#[derive(Debug, Default, Clone)]
pub struct BpfCounters {
    pub nr_online_cpus: u64,
    pub nr_running: u64,
    pub nr_queued: u64,
    pub nr_scheduled: u64,
    pub nr_user_dispatches: u64,
    pub nr_kernel_dispatches: u64,
    pub nr_cancel_dispatches: u64,
    pub nr_bounce_dispatches: u64,
    pub nr_failed_dispatches: u64,
    pub nr_sched_congested: u64,
    pub nr_has_invocation_enqueues: u64,
    pub nr_preempt_dispatches: u64,
}

/// Pure mechanism: abstracts over the kernel scheduling interface.
pub trait CpuAdapter {
    /// Drain all pending tasks from the kernel.
    fn drain(&mut self) -> Vec<QueuedTask>;

    /// Dispatch a task back to the kernel with the given scheduling info.
    fn dispatch(
        &mut self,
        pid: i32,
        cpu: i32,
        slice_ns: u64,
        vtime: u64,
        enq_flags: u64,
        dispatch_flags: u64,
        enq_cnt: u64,
    ) -> bool;

    /// Host CPU topology.
    fn topology(&self) -> &Topology;

    /// Whether the scheduler should exit.
    fn exited(&self) -> bool;

    /// Notify kernel that scheduling cycle is complete.
    fn notify_complete(&mut self, pending: u64);

    /// Wait briefly for new scheduler work when the current cycle was idle.
    fn wait_for_work(&mut self, _timeout: Duration) {}

    /// Snapshot of BPF-level counters.
    fn bpf_counters(&mut self) -> BpfCounters;

    /// Shutdown and return exit info.
    fn shutdown_and_report(&mut self) -> Result<UserExitInfo>;
}
