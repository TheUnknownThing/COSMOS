// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use crate::actuator::ResourceActuator;
use crate::adapter::CpuAdapter;
use crate::bpf::RL_CPU_ANY;
use crate::coordinator::CoordinationEngine;
use crate::metadata::delete_invocation_hint;
use crate::policy::SchedulingPolicy;
use crate::registry::InvocationMeta;
use crate::registry::RegistryHandle;
use crate::stats::Metrics;
use anyhow::Result;
use procfs::process::Process;
use scx_stats::prelude::*;
use scx_utils::UserExitInfo;
use std::io;
use std::time::Duration;

pub struct Scheduler<P: SchedulingPolicy, A: CpuAdapter> {
    registry: RegistryHandle,
    adapter: A,
    policy: P,
    stats_server: StatsServer<(), Metrics>,
    prune_counter: u64,
    init_page_faults: u64,
    coordinator: CoordinationEngine,
    actuator: ResourceActuator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerOptions {
    pub cgroup_actuator_enabled: bool,
    pub network_actuator_enabled: bool,
    pub phase_prediction_enabled: bool,
    pub warm_value_enabled: bool,
}

impl Default for SchedulerOptions {
    fn default() -> Self {
        Self {
            cgroup_actuator_enabled: true,
            network_actuator_enabled: true,
            phase_prediction_enabled: true,
            warm_value_enabled: true,
        }
    }
}

impl<P: SchedulingPolicy, A: CpuAdapter> Scheduler<P, A> {
    pub fn new(
        registry: RegistryHandle,
        adapter: A,
        policy: P,
        stats_server: StatsServer<(), Metrics>,
    ) -> Self {
        Self::new_with_options(
            registry,
            adapter,
            policy,
            stats_server,
            SchedulerOptions::default(),
        )
    }

    pub fn new_with_options(
        registry: RegistryHandle,
        adapter: A,
        policy: P,
        stats_server: StatsServer<(), Metrics>,
        options: SchedulerOptions,
    ) -> Self {
        Self {
            registry,
            adapter,
            policy,
            stats_server,
            prune_counter: 0,
            init_page_faults: 0,
            coordinator: CoordinationEngine::with_options(
                Duration::from_millis(100),
                options.phase_prediction_enabled,
                options.warm_value_enabled,
            ),
            actuator: ResourceActuator::with_enabled(
                options.cgroup_actuator_enabled,
                options.network_actuator_enabled,
            ),
        }
    }
    pub fn run(&mut self) -> Result<UserExitInfo> {
        let (res_ch, req_ch) = self.stats_server.channels();
        while !self.adapter.exited() {
            let now = crate::monotonic_now_ns();
            if self.coordinator.should_tick(now) {
                if let Ok(mut reg) = self.registry.try_write() {
                    self.coordinator.tick(&mut reg, now);
                }
                if let Ok(reg) = self.registry.try_read() {
                    self.actuator.apply_from_registry(&reg);
                }
            }
            let raw = self.adapter.drain();
            let decisions = {
                let resolved: Vec<Option<InvocationMeta>> = match self.registry.try_read() {
                    Ok(reg) => raw
                        .iter()
                        .map(|task| reg.lookup_tgid_meta(task.tgid).cloned())
                        .collect(),
                    Err(_) => vec![None; raw.len()],
                };
                self.policy
                    .schedule(&resolved, &raw, self.adapter.topology(), now)
            };
            for dec in &decisions {
                let dispatched = self.adapter.dispatch(
                    dec.pid,
                    dec.cpu,
                    dec.slice_ns,
                    dec.vtime,
                    dec.enq_flags,
                    dec.enq_cnt,
                );
                if !dispatched && dec.cpu != RL_CPU_ANY {
                    self.adapter.dispatch(
                        dec.pid,
                        RL_CPU_ANY,
                        dec.slice_ns,
                        dec.vtime,
                        dec.enq_flags,
                        dec.enq_cnt,
                    );
                }
            }
            {
                if let Ok(reg) = self.registry.try_read() {
                    self.policy.tick(&reg, now);
                }
            }
            self.prune_counter += 1;
            if self.prune_counter % 1000 == 0 {
                if let Ok(mut reg) = self.registry.try_write() {
                    let pruned = reg.prune(now, 60_000_000_000);
                    for tgid in pruned {
                        delete_invocation_hint(tgid);
                        self.coordinator.remove_tgid(tgid);
                    }
                }
            }
            let pending = raw.len() as u64;
            self.adapter.notify_complete(pending);
            if req_ch.try_recv().is_ok() {
                res_ch.send(self.get_metrics())?;
            }
        }
        self.adapter.shutdown_and_report()
    }
    fn get_metrics(&mut self) -> Metrics {
        let page_faults = Self::get_page_faults().unwrap_or_default();
        if self.init_page_faults == 0 {
            self.init_page_faults = page_faults;
        }
        let nr_page_faults = page_faults - self.init_page_faults;
        let bpf = self.adapter.bpf_counters();
        let policy = self.policy.counters();
        Metrics {
            nr_cpus: bpf.nr_online_cpus,
            nr_running: bpf.nr_running,
            nr_queued: bpf.nr_queued,
            nr_scheduled: bpf.nr_scheduled,
            nr_page_faults,
            nr_cold_start_tasks: policy.nr_cold_start_tasks,
            nr_hot_invocation_tasks: policy.nr_hot_invocation_tasks,
            nr_background_tasks: policy.nr_background_tasks,
            nr_slo_boosted: policy.nr_slo_boosted,
            max_pending: policy.max_pending,
            nr_user_dispatches: bpf.nr_user_dispatches,
            nr_kernel_dispatches: bpf.nr_kernel_dispatches,
            nr_cancel_dispatches: bpf.nr_cancel_dispatches,
            nr_bounce_dispatches: bpf.nr_bounce_dispatches,
            nr_failed_dispatches: bpf.nr_failed_dispatches,
            nr_sched_congested: bpf.nr_sched_congested,
            nr_metadata_classified: policy.nr_metadata_classified,
            nr_heuristic_classified: policy.nr_heuristic_classified,
            nr_metadata_refreshed: 0,
            nr_has_invocation_enqueues: bpf.nr_has_invocation_enqueues,
            nr_pool_latency: policy.nr_pool_latency,
            nr_pool_batch: policy.nr_pool_batch,
            nr_tail_guard_dispatches: policy.nr_tail_guard_dispatches,
            nr_slo_violations: policy.nr_slo_violations,
            nr_pool_migrations: policy.nr_pool_migrations,
            nr_pool_overflow: policy.nr_pool_overflow,
        }
    }
    fn get_page_faults() -> Result<u64, io::Error> {
        let myself = Process::myself().map_err(io::Error::other)?;
        let stat = myself.stat().map_err(io::Error::other)?;
        Ok(stat.minflt + stat.majflt)
    }
}
