// SFS-inspired scheduling policy for COSMOS.
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use std::collections::HashMap;

use scx_utils::Topology;

use crate::bpf::{QueuedTask, RL_CPU_ANY};
use crate::policy::{DispatchDecision, PolicyCounters, SchedulingPolicy};
use crate::registry::{InvocationMeta, InvocationRegistry};

const NSEC_PER_USEC: u64 = 1_000;
const TASK_STATE_TTL_NS: u64 = 60_000_000_000;
const SYSTEM_GUARD_WAIT_NS: u64 = 500_000_000;

#[derive(Debug, Clone, Default)]
struct SfsTaskState {
    remaining_credit_ns: u64,
    last_exec_runtime_ns: u64,
    last_seen_ns: u64,
    last_invocation_id: u64,
    demoted: bool,
}

#[derive(Debug, Clone)]
struct ThresholdState {
    current_ns: u64,
    min_credit_ns: u64,
    window: usize,
    count: usize,
    sum_iat_ns: u128,
    last_event_ns: Option<u64>,
}

impl ThresholdState {
    fn new(min_credit_ns: u64, window: usize) -> Self {
        Self {
            current_ns: min_credit_ns,
            min_credit_ns,
            window: window.max(1),
            count: 0,
            sum_iat_ns: 0,
            last_event_ns: None,
        }
    }

    fn observe(&mut self, event_ns: u64, nr_cpus: usize) {
        if let Some(last) = self.last_event_ns {
            self.sum_iat_ns = self
                .sum_iat_ns
                .saturating_add(event_ns.saturating_sub(last) as u128);
            self.count += 1;
            if self.count >= self.window {
                let avg_iat_ns = (self.sum_iat_ns / self.count as u128) as u64;
                self.current_ns = self
                    .min_credit_ns
                    .max(avg_iat_ns.saturating_mul(nr_cpus as u64));
                self.count = 0;
                self.sum_iat_ns = 0;
            }
        }
        self.last_event_ns = Some(event_ns);
    }
}

pub struct SfsPolicy {
    task_state: HashMap<u32, SfsTaskState>,
    threshold: ThresholdState,
    cfs_slice_ns: u64,
    slice_ns_min: u64,
    queue_delay_factor: u64,
    counters: PolicyCounters,
}

impl SfsPolicy {
    pub fn new(opts: &crate::SfsOpts) -> Self {
        let min_credit_ns = opts.sfs_min_credit_us * NSEC_PER_USEC;
        Self {
            task_state: HashMap::new(),
            threshold: ThresholdState::new(min_credit_ns, opts.sfs_threshold_window as usize),
            cfs_slice_ns: opts.slice_us * NSEC_PER_USEC,
            slice_ns_min: opts.slice_us_min * NSEC_PER_USEC,
            queue_delay_factor: opts.sfs_queue_delay_factor.max(1),
            counters: PolicyCounters::default(),
        }
    }

    fn meta_for<'a>(
        &self,
        task: &QueuedTask,
        reg: &'a InvocationRegistry,
    ) -> Option<&'a InvocationMeta> {
        reg.lookup_tgid(task.tgid).and_then(|id| reg.get(id))
    }

    fn is_new_invocation(state: Option<&SfsTaskState>, meta: Option<&InvocationMeta>) -> bool {
        match (state, meta) {
            (_, Some(meta)) if meta.id == 0 => state.is_none(),
            (Some(state), Some(meta)) => state.last_invocation_id != meta.id,
            (None, _) => true,
            (Some(_), None) => false,
        }
    }

    fn observe_arrival(
        &mut self,
        task: &QueuedTask,
        meta: Option<&InvocationMeta>,
        state: Option<&SfsTaskState>,
        now: u64,
        nr_cpus: usize,
    ) {
        let event_ns = if Self::is_new_invocation(state, meta) {
            meta.map(|m| m.created_at_ns)
                .filter(|ts| *ts > 0)
                .unwrap_or(now)
        } else if state.is_some_and(|s| task.exec_runtime < s.last_exec_runtime_ns) {
            now
        } else {
            return;
        };
        self.threshold.observe(event_ns, nr_cpus.max(1));
    }

    fn update_state(
        &mut self,
        task: &QueuedTask,
        meta: Option<&InvocationMeta>,
        now: u64,
    ) -> (u64, bool, bool, bool) {
        let prior = self.task_state.get(&task.tgid).cloned();
        let is_new = Self::is_new_invocation(prior.as_ref(), meta);
        let woke_from_sleep = prior
            .as_ref()
            .is_some_and(|s| !is_new && task.exec_runtime < s.last_exec_runtime_ns);
        let mut state = prior.unwrap_or_default();

        if is_new {
            state.remaining_credit_ns = self.threshold.current_ns;
            state.demoted = false;
            state.last_exec_runtime_ns = 0;
        }

        let delta_runtime_ns = if woke_from_sleep {
            task.exec_runtime
        } else {
            task.exec_runtime.saturating_sub(state.last_exec_runtime_ns)
        };

        if !state.demoted {
            state.remaining_credit_ns = state.remaining_credit_ns.saturating_sub(delta_runtime_ns);
            if state.remaining_credit_ns == 0 {
                state.demoted = true;
            }
        }

        state.last_exec_runtime_ns = task.exec_runtime;
        state.last_seen_ns = now;
        if let Some(meta) = meta.filter(|m| m.id != 0) {
            state.last_invocation_id = meta.id;
        }

        let remaining_credit_ns = state.remaining_credit_ns;
        let demoted = state.demoted;
        self.task_state.insert(task.tgid, state);
        (remaining_credit_ns, demoted, is_new, woke_from_sleep)
    }

    fn queue_delay_ns(&self, meta: Option<&InvocationMeta>, is_new: bool, now: u64) -> u64 {
        if is_new {
            meta.map(|m| now.saturating_sub(m.created_at_ns))
                .unwrap_or(0)
        } else {
            0
        }
    }

    fn sfs_score(&self, task: &QueuedTask, remaining_credit_ns: u64) -> u64 {
        let credit = remaining_credit_ns.max(self.slice_ns_min);
        credit
            .saturating_mul(100)
            .checked_div(task.weight.max(1))
            .unwrap_or(credit)
    }

    fn system_guard_applies(task: &QueuedTask, meta: Option<&InvocationMeta>, now: u64) -> bool {
        meta.is_none()
            && task.stop_ts > 0
            && now.saturating_sub(task.stop_ts) >= SYSTEM_GUARD_WAIT_NS
    }

    fn cfs_score(&self, task: &QueuedTask) -> u64 {
        task.vtime
            .saturating_add(task.exec_runtime)
            .saturating_add(self.cfs_slice_ns)
    }

    fn prune_task_state(&mut self, now: u64) {
        self.task_state
            .retain(|_, state| now.saturating_sub(state.last_seen_ns) < TASK_STATE_TTL_NS);
    }

    fn schedule_internal(
        &mut self,
        resolved_meta: &[Option<InvocationMeta>],
        raw: &[QueuedTask],
        now: u64,
        nr_cpus: usize,
    ) -> Vec<DispatchDecision> {
        let mut ranked: Vec<(u64, i32, u64, bool, u64, u64)> = Vec::with_capacity(raw.len());

        for (i, task) in raw.iter().enumerate() {
            let meta = resolved_meta.get(i).and_then(|m| m.as_ref());
            let prior = self.task_state.get(&task.tgid).cloned();
            self.observe_arrival(task, meta, prior.as_ref(), now, nr_cpus);
            let (remaining_credit_ns, mut demoted, is_new, _woke_from_sleep) =
                self.update_state(task, meta, now);
            let queue_delay_ns = self.queue_delay_ns(meta, is_new, now);
            if !demoted
                && queue_delay_ns
                    > self
                        .threshold
                        .current_ns
                        .saturating_mul(self.queue_delay_factor)
            {
                demoted = true;
                if let Some(state) = self.task_state.get_mut(&task.tgid) {
                    state.demoted = true;
                    state.remaining_credit_ns = 0;
                }
            }

            if meta.is_some() {
                self.counters.nr_metadata_classified =
                    self.counters.nr_metadata_classified.saturating_add(1);
            } else {
                self.counters.nr_heuristic_classified =
                    self.counters.nr_heuristic_classified.saturating_add(1);
            }

            let (mut score, slice_ns, active_short) = if demoted {
                (
                    self.cfs_score(task),
                    self.cfs_slice_ns.max(self.slice_ns_min),
                    false,
                )
            } else {
                (
                    self.sfs_score(task, remaining_credit_ns),
                    remaining_credit_ns.max(self.slice_ns_min),
                    true,
                )
            };
            if Self::system_guard_applies(task, meta, now) {
                score = 0;
            }

            if active_short {
                self.counters.nr_slo_boosted = self.counters.nr_slo_boosted.saturating_add(1);
                if is_new {
                    self.counters.nr_cold_start_tasks =
                        self.counters.nr_cold_start_tasks.saturating_add(1);
                } else {
                    self.counters.nr_hot_invocation_tasks =
                        self.counters.nr_hot_invocation_tasks.saturating_add(1);
                }
            } else {
                self.counters.nr_background_tasks =
                    self.counters.nr_background_tasks.saturating_add(1);
            }

            ranked.push((
                score,
                task.pid,
                slice_ns,
                active_short,
                task.flags,
                task.enq_cnt,
            ));
        }

        self.counters.max_pending = self.counters.max_pending.max(ranked.len() as u64);
        ranked.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| b.3.cmp(&a.3))
                .then_with(|| a.1.cmp(&b.1))
        });

        ranked
            .into_iter()
            .map(
                |(score, pid, slice_ns, _, enq_flags, enq_cnt)| DispatchDecision {
                    pid,
                    cpu: RL_CPU_ANY,
                    slice_ns,
                    vtime: score,
                    enq_flags,
                    enq_cnt,
                },
            )
            .collect()
    }
}

impl SchedulingPolicy for SfsPolicy {
    type Stats = PolicyCounters;

    fn schedule(
        &mut self,
        resolved_meta: &[Option<InvocationMeta>],
        raw: &[QueuedTask],
        topo: &Topology,
        now: u64,
    ) -> Vec<DispatchDecision> {
        self.schedule_internal(resolved_meta, raw, now, topo.all_cpus.len())
    }

    fn tick(&mut self, _registry: &InvocationRegistry, now_ns: u64) {
        self.prune_task_state(now_ns);
    }

    fn stats(&self) -> PolicyCounters {
        self.counters.clone()
    }

    fn counters(&self) -> PolicyCounters {
        self.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{InvocationMeta, SloClass};

    const MS: u64 = 1_000_000;

    fn opts() -> crate::SfsOpts {
        crate::SfsOpts {
            slice_us: 20_000,
            slice_us_min: 500,
            sfs_threshold_window: 2,
            sfs_min_credit_us: 6_000,
            sfs_queue_delay_factor: 3,
        }
    }

    fn qt(pid: i32, tgid: u32, rt: u64, stop_ts: u64) -> QueuedTask {
        QueuedTask {
            pid,
            tgid,
            cpu: 0,
            nr_cpus_allowed: 4,
            flags: 0,
            start_ts: 0,
            stop_ts,
            exec_runtime: rt,
            weight: 100,
            vtime: 0,
            enq_cnt: 0,
            comm: [0; 16],
        }
    }

    fn meta(id: u64, tgid: u32, created_at_ns: u64) -> InvocationMeta {
        InvocationMeta {
            id,
            tgid,
            deadline_ns: 0,
            estimated_duration_ns: 0,
            slo_class: SloClass::LatencyCritical,
            is_cold_start: id == 1,
            profile_id: None,
            created_at_ns,
        }
    }

    fn resolve_meta(reg: &InvocationRegistry, tasks: &[QueuedTask]) -> Vec<Option<InvocationMeta>> {
        tasks
            .iter()
            .map(|task| {
                reg.lookup_tgid(task.tgid)
                    .and_then(|id| reg.get(id).cloned())
            })
            .collect()
    }

    #[test]
    fn threshold_tracks_arrival_rate_with_floor() {
        let mut policy = SfsPolicy::new(&opts());
        let mut reg = InvocationRegistry::new();
        reg.upsert(meta(1, 101, 10 * MS));
        reg.upsert(meta(2, 102, 12 * MS));
        reg.upsert(meta(3, 103, 16 * MS));

        let _ = policy.schedule_internal(
            &resolve_meta(&reg, &[qt(1001, 101, 0, 0)]),
            &[qt(1001, 101, 0, 0)],
            10 * MS,
            4,
        );
        let _ = policy.schedule_internal(
            &resolve_meta(&reg, &[qt(1002, 102, 0, 0)]),
            &[qt(1002, 102, 0, 0)],
            12 * MS,
            4,
        );
        let _ = policy.schedule_internal(
            &resolve_meta(&reg, &[qt(1003, 103, 0, 0)]),
            &[qt(1003, 103, 0, 0)],
            16 * MS,
            4,
        );

        assert_eq!(policy.threshold.current_ns, 12 * MS);
    }

    #[test]
    fn sleep_wakeup_preserves_remaining_credit() {
        let mut policy = SfsPolicy::new(&opts());
        let mut reg = InvocationRegistry::new();
        reg.upsert(meta(1, 200, 1 * MS));

        let _ = policy.schedule_internal(
            &resolve_meta(&reg, &[qt(2001, 200, 2 * MS, 0)]),
            &[qt(2001, 200, 2 * MS, 0)],
            2 * MS,
            4,
        );
        let before_sleep = policy.task_state.get(&200).unwrap().remaining_credit_ns;

        let _ = policy.schedule_internal(
            &resolve_meta(&reg, &[qt(2001, 200, 500_000, 0)]),
            &[qt(2001, 200, 500_000, 0)],
            5 * MS,
            4,
        );
        let after_wakeup = policy.task_state.get(&200).unwrap().remaining_credit_ns;

        assert_eq!(before_sleep.saturating_sub(500_000), after_wakeup);
    }

    #[test]
    fn credit_exhaustion_demotes_task() {
        let mut policy = SfsPolicy::new(&opts());
        let mut reg = InvocationRegistry::new();
        reg.upsert(meta(1, 300, 1 * MS));

        let _ = policy.schedule_internal(
            &resolve_meta(&reg, &[qt(3001, 300, 7 * MS, 0)]),
            &[qt(3001, 300, 7 * MS, 0)],
            8 * MS,
            4,
        );
        let state = policy.task_state.get(&300).unwrap();

        assert!(state.demoted);
        assert_eq!(state.remaining_credit_ns, 0);
    }

    #[test]
    fn stale_unmetadata_task_is_guarded_ahead_of_fresh_background() {
        let mut policy = SfsPolicy::new(&opts());
        let now = 1_000 * MS;
        let fresh = qt(5001, 501, 1 * MS, 0);
        let stale = qt(5002, 502, 1 * MS, now.saturating_sub(SYSTEM_GUARD_WAIT_NS));
        let decisions = policy.schedule_internal(&[None, None], &[fresh, stale], now, 4);

        assert_eq!(decisions[0].pid, 5002);
        assert_eq!(decisions[0].vtime, 0);
    }

    #[test]
    fn new_invocation_resets_demoted_state() {
        let mut policy = SfsPolicy::new(&opts());
        let mut reg = InvocationRegistry::new();
        reg.upsert(meta(1, 400, 1 * MS));
        let _ = policy.schedule_internal(
            &resolve_meta(&reg, &[qt(4001, 400, 7 * MS, 0)]),
            &[qt(4001, 400, 7 * MS, 0)],
            8 * MS,
            4,
        );
        assert!(policy.task_state.get(&400).unwrap().demoted);

        reg.upsert(meta(2, 400, 20 * MS));
        let _ = policy.schedule_internal(
            &resolve_meta(&reg, &[qt(4001, 400, 0, 0)]),
            &[qt(4001, 400, 0, 0)],
            20 * MS,
            4,
        );
        let state = policy.task_state.get(&400).unwrap();

        assert!(!state.demoted);
        assert_eq!(state.remaining_credit_ns, policy.threshold.current_ns);
    }
}
