// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use crate::bpf::{BpfScheduler, DispatchedTask, QueuedTask};
use anyhow::Result;
use log::warn;
use scx_utils::Topology;
use scx_utils::UserExitInfo;
use std::time::Duration;

use super::BpfCounters;
use super::CpuAdapter;

/// sched_ext implementation of CpuAdapter.
pub struct ScxAdapter<'cb> {
    bpf: BpfScheduler<'cb>,
    topology: Topology,
}

impl<'cb> ScxAdapter<'cb> {
    pub fn new(bpf: BpfScheduler<'cb>) -> Result<Self> {
        let topology = Topology::new()?;
        Ok(Self { bpf, topology })
    }
}

impl CpuAdapter for ScxAdapter<'_> {
    fn drain(&mut self) -> Vec<QueuedTask> {
        let mut tasks = Vec::new();
        loop {
            match self.bpf.dequeue_task() {
                Ok(Some(task)) => {
                    tasks.push(task);
                }
                Ok(None) => break,
                Err(err) => {
                    warn!("Error draining tasks: {err}");
                    break;
                }
            }
        }
        tasks
    }

    fn dispatch(
        &mut self,
        pid: i32,
        cpu: i32,
        slice_ns: u64,
        vtime: u64,
        enq_flags: u64,
        dispatch_flags: u64,
        enq_cnt: u64,
    ) -> bool {
        let d = DispatchedTask {
            pid,
            cpu,
            flags: enq_flags,
            dispatch_flags,
            slice_ns,
            vtime,
            enq_cnt,
        };
        self.bpf.dispatch_task(&d).is_ok()
    }

    fn topology(&self) -> &Topology {
        &self.topology
    }

    fn exited(&self) -> bool {
        self.bpf.shutdown.load(std::sync::atomic::Ordering::Relaxed)
            || scx_utils::uei_exited!(&self.bpf.skel, uei)
    }

    fn notify_complete(&mut self, pending: u64) {
        self.bpf.notify_complete(pending);
    }

    fn wait_for_work(&mut self, timeout: Duration) {
        if let Err(err) = self.bpf.wait_for_queued(timeout) {
            warn!("Error waiting for queued scheduler work: {err}");
        }
    }

    fn bpf_counters(&mut self) -> BpfCounters {
        BpfCounters {
            nr_online_cpus: *self.bpf.nr_online_cpus_mut(),
            nr_running: *self.bpf.nr_running_mut(),
            nr_queued: *self.bpf.nr_queued_mut(),
            nr_scheduled: *self.bpf.nr_scheduled_mut(),
            nr_user_dispatches: *self.bpf.nr_user_dispatches_mut(),
            nr_kernel_dispatches: *self.bpf.nr_kernel_dispatches_mut(),
            nr_cancel_dispatches: *self.bpf.nr_cancel_dispatches_mut(),
            nr_bounce_dispatches: *self.bpf.nr_bounce_dispatches_mut(),
            nr_failed_dispatches: *self.bpf.nr_failed_dispatches_mut(),
            nr_sched_congested: *self.bpf.nr_sched_congested_mut(),
            nr_has_invocation_enqueues: *self.bpf.nr_has_invocation_enqueues_mut(),
            nr_preempt_dispatches: *self.bpf.nr_preempt_dispatches_mut(),
        }
    }

    fn shutdown_and_report(&mut self) -> Result<UserExitInfo> {
        self.bpf.shutdown_and_report()
    }
}
