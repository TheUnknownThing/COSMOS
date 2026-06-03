// COSMOS v2: Invocation-centric scheduling policy.
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use std::collections::HashMap;

use scx_utils::Topology;

use crate::bpf::{QueuedTask, RL_CPU_ANY, RL_DISPATCH_FORCE_PREEMPT, RL_DISPATCH_PREEMPT};
use crate::policy::{DispatchDecision, PolicyCounters, SchedulingPolicy};
use crate::registry::{InvocationMeta, InvocationRegistry, SloClass};

const NSEC_PER_USEC: u64 = 1_000;
const TASK_STATE_TTL_NS: u64 = 60_000_000_000;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum TaskClass {
    ColdStart,
    HotInvocation,
    Background,
}

fn class_rank(c: TaskClass) -> u8 {
    match c {
        TaskClass::ColdStart => 0,
        TaskClass::HotInvocation => 1,
        TaskClass::Background => 2,
    }
}

#[derive(Debug, Default, Clone)]
pub struct TaskState {
    pub avg_runtime_ns: u64,
    pub wakeups: u64,
    pub last_seen_ns: u64,
    pub last_invocation_id: u64,
}

#[derive(Debug, Clone, Default)]
pub struct CosmosCounters {
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

impl From<CosmosCounters> for PolicyCounters {
    fn from(c: CosmosCounters) -> Self {
        PolicyCounters {
            nr_cold_start_tasks: c.nr_cold_start_tasks,
            nr_hot_invocation_tasks: c.nr_hot_invocation_tasks,
            nr_background_tasks: c.nr_background_tasks,
            nr_slo_boosted: c.nr_slo_boosted,
            max_pending: c.max_pending,
            nr_metadata_classified: c.nr_metadata_classified,
            nr_heuristic_classified: c.nr_heuristic_classified,
            nr_short_preemptions: c.nr_short_preemptions,
            nr_starvation_guard_dispatches: c.nr_starvation_guard_dispatches,
            nr_slo_violations: c.nr_slo_violations,
        }
    }
}

pub struct CosmosPolicy {
    pub task_state: HashMap<u32, TaskState>,
    vruntime_now: u64,
    slice_ns: u64,
    slice_ns_min: u64,
    slo_target_ns: u64,
    cold_start_boost_ns: u64,
    invocation_comm: Vec<String>,
    pub starvation_guard_threshold_ns: u64,
    percpu_local: bool,
    pub deadline_scoring_enabled: bool,
    short_preemption_enabled: bool,
    short_task_threshold_ns: u64,
    nr_cold_start_tasks: u64,
    nr_hot_invocation_tasks: u64,
    nr_background_tasks: u64,
    nr_slo_boosted: u64,
    max_pending: u64,
    nr_metadata_classified: u64,
    nr_heuristic_classified: u64,
    nr_short_preemptions: u64,
    nr_starvation_guard_dispatches: u64,
    nr_slo_violations: u64,
}

#[derive(Debug)]
struct RankedDecision {
    dispatch_rank: u8,
    score: u64,
    class_rank: u8,
    has_meta: bool,
    observed_ns: u64,
    pid: i32,
    cpu: i32,
    slice_ns: u64,
    enq_flags: u64,
    enq_cnt: u64,
    preempt: bool,
    force_preempt: bool,
}

impl CosmosPolicy {
    pub fn new(opts: &crate::CosmosOpts) -> Self {
        Self {
            task_state: HashMap::new(),
            vruntime_now: 0,
            slice_ns: opts.slice_us * NSEC_PER_USEC,
            slice_ns_min: opts.slice_us_min * NSEC_PER_USEC,
            slo_target_ns: opts.slo_target_us * NSEC_PER_USEC,
            cold_start_boost_ns: opts.cold_start_boost_us * NSEC_PER_USEC,
            invocation_comm: opts.invocation_comm.clone(),
            starvation_guard_threshold_ns: opts.starvation_guard_threshold_us * NSEC_PER_USEC,
            percpu_local: opts.percpu_local,
            deadline_scoring_enabled: !opts.disable_deadline_scoring,
            short_preemption_enabled: !opts.disable_short_preemption,
            short_task_threshold_ns: opts.short_task_threshold_us * NSEC_PER_USEC,
            nr_cold_start_tasks: 0,
            nr_hot_invocation_tasks: 0,
            nr_background_tasks: 0,
            nr_slo_boosted: 0,
            max_pending: 0,
            nr_metadata_classified: 0,
            nr_heuristic_classified: 0,
            nr_short_preemptions: 0,
            nr_starvation_guard_dispatches: 0,
            nr_slo_violations: 0,
        }
    }

    pub fn with_registry(self, _registry: crate::registry::RegistryHandle) -> Self {
        self
    }

    pub fn scale_by_weight(task: &QueuedTask, v: u64) -> u64 {
        v.saturating_mul(task.weight) / 100
    }

    fn inv_scale(task: &QueuedTask, v: u64) -> u64 {
        v.saturating_mul(100) / task.weight.max(1)
    }

    fn task_slo_target(&self, meta: Option<&InvocationMeta>) -> u64 {
        meta.and_then(|m| {
            if m.estimated_duration_ns > 0 {
                Some(m.estimated_duration_ns)
            } else {
                None
            }
        })
        .unwrap_or(self.slo_target_ns)
    }

    fn hint_match(&self, task: &QueuedTask) -> bool {
        let c = task.comm_str();
        self.invocation_comm
            .iter()
            .any(|n| !n.is_empty() && c.contains(n))
    }

    fn meta_for<'a>(
        &self,
        task: &QueuedTask,
        reg: &'a InvocationRegistry,
    ) -> Option<&'a InvocationMeta> {
        reg.lookup_tgid(task.tgid).and_then(|id| reg.get(id))
    }

    fn heuristic_classify(&self, task: &QueuedTask, _effective_slo: u64) -> TaskClass {
        if self.hint_match(task) {
            return self.heuristic_classify_hint(task, _effective_slo);
        }
        TaskClass::Background
    }

    fn heuristic_classify_hint(&self, task: &QueuedTask, effective_slo: u64) -> TaskClass {
        let Some(st) = self.task_state.get(&task.tgid) else {
            return if task.exec_runtime <= effective_slo {
                TaskClass::ColdStart
            } else {
                TaskClass::Background
            };
        };
        if st.wakeups <= 1 {
            TaskClass::ColdStart
        } else {
            TaskClass::HotInvocation
        }
    }

    fn classify(&self, task: &QueuedTask, meta: Option<&InvocationMeta>) -> TaskClass {
        let Some(m) = meta else {
            return self.heuristic_classify(task, self.slo_target_ns);
        };
        if m.slo_class == SloClass::None {
            return self.heuristic_classify(task, self.slo_target_ns);
        }
        let effective_slo = self.task_slo_target(Some(m));
        match m.slo_class {
            SloClass::LatencyCritical => {
                let is_new = self
                    .task_state
                    .get(&task.tgid)
                    .map_or(true, |s| s.last_invocation_id != m.id);
                if m.is_cold_start || is_new {
                    TaskClass::ColdStart
                } else {
                    TaskClass::HotInvocation
                }
            }
            SloClass::Standard => self.heuristic_classify(task, effective_slo),
            SloClass::Batch => TaskClass::Background,
            SloClass::None => self.heuristic_classify(task, effective_slo),
        }
    }

    fn update_task_state(&mut self, task: &QueuedTask, meta: Option<&InvocationMeta>, now: u64) {
        let is_new = meta.map_or(false, |m| {
            m.id != 0
                && self
                    .task_state
                    .get(&task.tgid)
                    .map_or(true, |s| s.last_invocation_id != m.id)
        });
        let meta_id = meta.map(|m| m.id);
        let state = self.task_state.entry(task.tgid).or_default();
        if is_new {
            state.avg_runtime_ns = 0;
            state.wakeups = 0;
        }
        state.avg_runtime_ns = if state.avg_runtime_ns == 0 {
            task.exec_runtime
        } else {
            (state
                .avg_runtime_ns
                .saturating_mul(7)
                .saturating_add(task.exec_runtime))
                / 8
        };
        state.wakeups = state.wakeups.saturating_add(1);
        state.last_seen_ns = now;
        if let Some(id) = meta_id.filter(|&i| i != 0) {
            state.last_invocation_id = id;
        }
    }

    fn prune_task_state(&mut self, now: u64) {
        self.task_state
            .retain(|_, s| now.saturating_sub(s.last_seen_ns) < TASK_STATE_TTL_NS);
    }

    fn observe_deadline(&mut self, meta: Option<&InvocationMeta>, now: u64) {
        if let Some(m) = meta {
            if m.slo_class == SloClass::LatencyCritical && m.deadline_ns > 0 && now > m.deadline_ns
            {
                self.nr_slo_violations = self.nr_slo_violations.saturating_add(1);
            }
        }
    }

    fn score_for_vtime(
        &self,
        task: &QueuedTask,
        class: TaskClass,
        now: u64,
        meta: Option<&InvocationMeta>,
        vtime: u64,
    ) -> u64 {
        let fair = vtime.saturating_add(task.exec_runtime.min(self.slice_ns.saturating_mul(100)));
        if self.deadline_scoring_enabled {
            if let Some(m) = meta {
                if m.slo_class == SloClass::LatencyCritical && m.deadline_ns > 0 {
                    let win = self.slice_ns.max(self.slice_ns_min);
                    let anchor = win
                        .saturating_add(self.slo_target_ns)
                        .saturating_add(self.cold_start_boost_ns);
                    let est = self
                        .task_state
                        .get(&task.tgid)
                        .map(|s| s.avg_runtime_ns.max(self.slice_ns_min))
                        .unwrap_or_else(|| task.exec_runtime.max(self.slice_ns_min));
                    let remaining = m.deadline_ns.saturating_sub(now);
                    if remaining > est {
                        let slack = remaining.saturating_sub(est);
                        let urgency = win.saturating_sub(slack.min(win));
                        let base = fair
                            .saturating_add(anchor)
                            .saturating_sub(Self::scale_by_weight(task, urgency));
                        return match class {
                            TaskClass::ColdStart => base.saturating_sub(Self::scale_by_weight(
                                task,
                                self.slo_target_ns.saturating_add(self.cold_start_boost_ns),
                            )),
                            TaskClass::HotInvocation => {
                                base.saturating_sub(Self::scale_by_weight(task, self.slo_target_ns))
                            }
                            TaskClass::Background => base.saturating_add(self.slo_target_ns),
                        };
                    }
                }
            }
        }
        let effective_slo = self.task_slo_target(meta);
        let boost = match class {
            TaskClass::ColdStart => effective_slo
                .saturating_div(2)
                .saturating_add(self.cold_start_boost_ns),
            TaskClass::HotInvocation => effective_slo.saturating_div(2),
            TaskClass::Background => 0,
        };
        fair.saturating_sub(Self::scale_by_weight(task, boost))
    }

    fn cap_vtime_lead(&self, vtime: u64) -> (u64, bool) {
        if self.starvation_guard_threshold_ns == 0 {
            return (vtime, false);
        }
        let max_vtime = self
            .vruntime_now
            .saturating_add(self.starvation_guard_threshold_ns);
        if vtime > max_vtime {
            (max_vtime, true)
        } else {
            (vtime, false)
        }
    }

    fn runnable_age_ns(&self, task: &QueuedTask, now: u64) -> u64 {
        if task.stop_ts > 0 {
            now.saturating_sub(task.stop_ts)
        } else {
            0
        }
    }

    fn guarded_score(&self) -> u64 {
        self.vruntime_now.saturating_sub(self.slice_ns)
    }

    fn should_force_starvation_guard(&self, task: &QueuedTask, now: u64) -> bool {
        self.starvation_guard_threshold_ns > 0
            && self.runnable_age_ns(task, now) >= self.starvation_guard_threshold_ns
    }

    fn short_runtime_estimate(
        &self,
        task: &QueuedTask,
        meta: Option<&InvocationMeta>,
    ) -> Option<u64> {
        meta.and_then(|m| {
            if m.estimated_duration_ns > 0 {
                Some(m.estimated_duration_ns)
            } else {
                None
            }
        })
        .or_else(|| {
            self.task_state
                .get(&task.tgid)
                .map(|s| s.avg_runtime_ns)
                .filter(|rt| *rt > 0)
        })
        .or_else(|| {
            if task.exec_runtime > 0 {
                Some(task.exec_runtime)
            } else {
                None
            }
        })
    }

    fn should_preempt_short(
        &self,
        task: &QueuedTask,
        class: TaskClass,
        meta: Option<&InvocationMeta>,
    ) -> bool {
        if !self.short_preemption_enabled || self.short_task_threshold_ns == 0 {
            return false;
        }
        if !matches!(class, TaskClass::ColdStart | TaskClass::HotInvocation) {
            return false;
        }
        self.short_runtime_estimate(task, meta)
            .is_some_and(|rt| rt <= self.short_task_threshold_ns)
    }

    fn should_force_preempt_kick(
        &self,
        preempt: bool,
        meta: Option<&InvocationMeta>,
        now: u64,
    ) -> bool {
        if !preempt {
            return false;
        }
        let Some(m) = meta else {
            return false;
        };
        if m.slo_class != SloClass::LatencyCritical || m.deadline_ns == 0 {
            return false;
        }
        let remaining = m.deadline_ns.saturating_sub(now);
        remaining <= self.short_task_threshold_ns.max(self.slice_ns_min)
    }

    fn slice_for(
        &self,
        task: &QueuedTask,
        class: TaskClass,
        effective_slo: u64,
        preempt: bool,
    ) -> u64 {
        let base = match class {
            TaskClass::ColdStart => effective_slo / 4,
            TaskClass::HotInvocation => effective_slo / 8,
            TaskClass::Background => self.slice_ns,
        };
        let mut slice =
            Self::scale_by_weight(task, base.max(self.slice_ns_min)).max(self.slice_ns_min);
        if preempt {
            slice = slice
                .min(self.short_task_threshold_ns)
                .max(self.slice_ns_min);
        }
        slice
    }

    fn count_classification(&mut self, class: TaskClass, has_meta: bool, preempt: bool) {
        if has_meta {
            self.nr_metadata_classified = self.nr_metadata_classified.saturating_add(1);
        } else {
            self.nr_heuristic_classified = self.nr_heuristic_classified.saturating_add(1);
        }
        match class {
            TaskClass::ColdStart => {
                self.nr_cold_start_tasks = self.nr_cold_start_tasks.saturating_add(1);
                self.nr_slo_boosted = self.nr_slo_boosted.saturating_add(1);
            }
            TaskClass::HotInvocation => {
                self.nr_hot_invocation_tasks = self.nr_hot_invocation_tasks.saturating_add(1);
                self.nr_slo_boosted = self.nr_slo_boosted.saturating_add(1);
            }
            TaskClass::Background => {
                self.nr_background_tasks = self.nr_background_tasks.saturating_add(1);
            }
        }
        if preempt {
            self.nr_short_preemptions = self.nr_short_preemptions.saturating_add(1);
        }
    }

    fn rank_one(
        &mut self,
        task: &QueuedTask,
        meta: Option<&InvocationMeta>,
        has_meta: bool,
        now: u64,
    ) -> (TaskClass, bool, bool, bool, u64, u64) {
        let class = self.classify(task, meta);

        let effective_vtime = if task.vtime == 0 {
            self.vruntime_now
        } else {
            task.vtime
                .max(self.vruntime_now.saturating_sub(self.slice_ns))
        };
        let (effective_vtime, vtime_capped) = self.cap_vtime_lead(effective_vtime);
        let vs = Self::inv_scale(task, task.stop_ts.saturating_sub(task.start_ts));
        let effective_vtime = effective_vtime.saturating_add(vs);
        let (effective_vtime, delta_vtime_capped) = self.cap_vtime_lead(effective_vtime);
        self.vruntime_now = self.vruntime_now.saturating_add(vs);

        self.update_task_state(task, meta, now);
        self.observe_deadline(meta, now);

        let mut score = self.score_for_vtime(task, class, now, meta, effective_vtime);
        let force_guard = self.should_force_starvation_guard(task, now);
        let guard = force_guard || vtime_capped || delta_vtime_capped;
        if guard {
            score = if force_guard {
                self.guarded_score()
            } else {
                score.min(
                    self.vruntime_now
                        .saturating_add(self.starvation_guard_threshold_ns),
                )
            };
            self.nr_starvation_guard_dispatches =
                self.nr_starvation_guard_dispatches.saturating_add(1);
        }

        let preempt = !guard && self.should_preempt_short(task, class, meta);
        let force_preempt = self.should_force_preempt_kick(preempt, meta, now);
        let effective_slo = self.task_slo_target(meta);
        let slice = self.slice_for(task, class, effective_slo, preempt);
        self.count_classification(class, has_meta, preempt);
        (class, guard, preempt, force_preempt, score, slice)
    }

    #[cfg(test)]
    pub(crate) fn classify_test(&self, t: &QueuedTask, r: &InvocationRegistry) -> TaskClass {
        let meta = self.meta_for(t, r);
        self.classify(t, meta)
    }

    #[cfg(test)]
    pub(crate) fn enqueue_test(
        &mut self,
        task: &mut QueuedTask,
        reg: &InvocationRegistry,
        now: u64,
    ) -> (TaskClass, bool, u64, u64, bool) {
        let meta = self.meta_for(task, reg).cloned();
        let has_meta = meta.is_some();
        let (class, _guard, preempt, _force_preempt, score, slice) =
            self.rank_one(task, meta.as_ref(), has_meta, now);
        (class, preempt, score, slice, has_meta)
    }

    #[cfg(test)]
    pub(crate) fn fair_test(&self, t: &QueuedTask) -> u64 {
        t.vtime
            .saturating_add(t.exec_runtime.min(self.slice_ns.saturating_mul(100)))
    }

    #[cfg(test)]
    pub(crate) fn score_test(
        &self,
        t: &QueuedTask,
        c: TaskClass,
        now: u64,
        m: Option<&InvocationMeta>,
    ) -> u64 {
        self.score_for_vtime(t, c, now, m, t.vtime)
    }

    #[cfg(test)]
    pub(crate) fn heuristic_test(&self, t: &QueuedTask) -> TaskClass {
        self.heuristic_classify(t, self.slo_target_ns)
    }
}

impl SchedulingPolicy for CosmosPolicy {
    type Stats = CosmosCounters;

    fn schedule(
        &mut self,
        resolved_meta: &[Option<InvocationMeta>],
        raw: &[QueuedTask],
        _topo: &Topology,
        now: u64,
    ) -> Vec<DispatchDecision> {
        let mut decisions: Vec<RankedDecision> = Vec::with_capacity(raw.len());

        for (i, task) in raw.iter().enumerate() {
            let meta = resolved_meta.get(i).and_then(|m| m.as_ref());
            let has_meta = meta.is_some();
            let (class, guard, preempt, force_preempt, score, slice) =
                self.rank_one(task, meta, has_meta, now);
            decisions.push(RankedDecision {
                dispatch_rank: if guard {
                    0
                } else if preempt {
                    1
                } else {
                    2
                },
                score,
                class_rank: class_rank(class),
                has_meta,
                observed_ns: now,
                pid: task.pid,
                cpu: task.cpu,
                slice_ns: slice,
                enq_flags: task.flags,
                enq_cnt: task.enq_cnt,
                preempt,
                force_preempt,
            });
        }

        decisions.sort_by(|a, b| {
            a.dispatch_rank
                .cmp(&b.dispatch_rank)
                .then_with(|| a.score.cmp(&b.score))
                .then_with(|| a.class_rank.cmp(&b.class_rank))
                .then_with(|| b.has_meta.cmp(&a.has_meta))
                .then_with(|| a.observed_ns.cmp(&b.observed_ns))
                .then_with(|| a.enq_cnt.cmp(&b.enq_cnt))
                .then_with(|| a.pid.cmp(&b.pid))
        });

        self.max_pending = self.max_pending.max(decisions.len() as u64);

        decisions
            .into_iter()
            .map(|d| {
                let cpu = if self.percpu_local { d.cpu } else { RL_CPU_ANY };
                let mut dispatch_flags = if d.preempt { RL_DISPATCH_PREEMPT } else { 0 };
                if d.force_preempt {
                    dispatch_flags |= RL_DISPATCH_FORCE_PREEMPT;
                }
                DispatchDecision {
                    pid: d.pid,
                    cpu,
                    slice_ns: d.slice_ns,
                    vtime: d.score,
                    enq_flags: d.enq_flags,
                    dispatch_flags,
                    enq_cnt: d.enq_cnt,
                    preempt: d.preempt,
                }
            })
            .collect()
    }

    fn tick(&mut self, _registry: &InvocationRegistry, now_ns: u64) {
        self.prune_task_state(now_ns);
    }

    fn stats(&self) -> CosmosCounters {
        CosmosCounters {
            nr_cold_start_tasks: self.nr_cold_start_tasks,
            nr_hot_invocation_tasks: self.nr_hot_invocation_tasks,
            nr_background_tasks: self.nr_background_tasks,
            nr_slo_boosted: self.nr_slo_boosted,
            max_pending: self.max_pending,
            nr_metadata_classified: self.nr_metadata_classified,
            nr_heuristic_classified: self.nr_heuristic_classified,
            nr_short_preemptions: self.nr_short_preemptions,
            nr_starvation_guard_dispatches: self.nr_starvation_guard_dispatches,
            nr_slo_violations: self.nr_slo_violations,
        }
    }

    fn counters(&self) -> PolicyCounters {
        PolicyCounters::from(self.stats())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const MS: u64 = 1_000_000;

    fn opts() -> crate::CosmosOpts {
        crate::CosmosOpts {
            slice_us: 20_000,
            slice_us_min: 500,
            slo_target_us: 10_000,
            cold_start_boost_us: 20_000,
            invocation_comm: vec![],
            percpu_local: false,
            starvation_guard_threshold_us: 2_000_000,
            disable_deadline_scoring: false,
            short_task_threshold_us: 20_000,
            disable_short_preemption: false,
        }
    }

    fn qt(pid: i32, tgid: u32, name: &str, rt: u64, wt: u64) -> QueuedTask {
        let mut c = [0i8; 16];
        for (i, b) in name.bytes().take(15).enumerate() {
            c[i] = b as libc::c_char;
        }
        QueuedTask {
            pid,
            tgid,
            cpu: 0,
            nr_cpus_allowed: 4,
            flags: 0,
            start_ts: 0,
            stop_ts: 0,
            exec_runtime: rt,
            weight: wt,
            vtime: 0,
            enq_cnt: 0,
            comm: c,
        }
    }

    fn pl(opts: &crate::CosmosOpts) -> CosmosPolicy {
        CosmosPolicy::new(opts)
    }

    fn expected_heuristic(policy: &CosmosPolicy, task: &QueuedTask, class: TaskClass) -> u64 {
        let fair = policy.fair_test(task);
        let boost = match class {
            TaskClass::ColdStart => policy
                .slo_target_ns
                .saturating_div(2)
                .saturating_add(policy.cold_start_boost_ns),
            TaskClass::HotInvocation => policy.slo_target_ns.saturating_div(2),
            TaskClass::Background => 0,
        };
        fair.saturating_sub(CosmosPolicy::scale_by_weight(task, boost))
    }

    fn build_reg(entries: &[(u64, u32, u64, u64, u32, u32)]) -> InvocationRegistry {
        let mut r = InvocationRegistry::new();
        for &(id, tgid, dl, est, slo, cold) in entries {
            r.upsert(InvocationMeta {
                id,
                tgid,
                deadline_ns: dl,
                estimated_duration_ns: est,
                slo_class: match slo {
                    0 => SloClass::LatencyCritical,
                    1 => SloClass::Standard,
                    2 => SloClass::Batch,
                    _ => SloClass::None,
                },
                is_cold_start: cold != 0,
                profile_id: None,
                created_at_ns: 1000,
            });
        }
        r
    }

    #[test]
    fn reg_latency_cold() {
        let o = opts();
        let p = pl(&o);
        let r = build_reg(&[(1, 100, 200 * MS, 0, 0, 1)]);
        assert_eq!(
            p.classify_test(&qt(1001, 100, "w", 5 * MS, 100), &r),
            TaskClass::ColdStart
        );
    }

    #[test]
    fn reg_batch_bg() {
        let o = opts();
        let p = pl(&o);
        let r = build_reg(&[(1, 100, 200 * MS, 0, 2, 0)]);
        assert_eq!(
            p.classify_test(&qt(1001, 100, "w", 5 * MS, 100), &r),
            TaskClass::Background
        );
    }

    #[test]
    fn no_reg_fallback() {
        let o = opts();
        let p = pl(&o);
        let r = InvocationRegistry::new();
        assert_eq!(
            p.classify_test(&qt(1001, 999, "w", 5 * MS, 100), &r),
            TaskClass::Background
        );
    }

    #[test]
    fn new_inv_resets() {
        let o = opts();
        let mut p = pl(&o);
        let now = 100 * MS;
        let r1 = build_reg(&[(1, 100, now + 200 * MS, 0, 0, 0)]);
        let mut t = qt(1001, 100, "w", 8 * MS, 100);
        p.enqueue_test(&mut t, &r1, now);
        assert_eq!(p.task_state.get(&100).unwrap().wakeups, 1);
        let r2 = build_reg(&[(2, 100, now + 200 * MS, 0, 0, 0)]);
        let mut t2 = qt(1001, 100, "w", 2 * MS, 100);
        let (c, _, _, _, _) = p.enqueue_test(&mut t2, &r2, now + MS);
        assert_eq!(c, TaskClass::ColdStart);
        assert_eq!(p.task_state.get(&100).unwrap().wakeups, 1);
    }

    #[test]
    fn reg_hot_after_first() {
        let o = opts();
        let mut p = pl(&o);
        let r = build_reg(&[(1, 100, 200 * MS, 0, 0, 0)]);
        p.enqueue_test(&mut qt(1001, 100, "w", 5 * MS, 100), &r, 100 * MS);
        assert_eq!(
            p.classify_test(&qt(1001, 100, "w", 5 * MS, 100), &r),
            TaskClass::HotInvocation
        );
    }

    #[test]
    fn reg_standard_heuristic() {
        let o = opts();
        let p = pl(&o);
        let r = build_reg(&[(1, 100, 200 * MS, 0, 1, 0)]);
        assert_eq!(
            p.classify_test(&qt(1001, 100, "w", 5 * MS, 100), &r),
            TaskClass::Background
        );
    }

    #[test]
    fn short_latency_task_preempts() {
        let o = opts();
        let mut p = pl(&o);
        let now = 100 * MS;
        let r = build_reg(&[(1, 100, now + 100 * MS, 5 * MS, 0, 1)]);
        let (_, preempt, _, _, _) = p.enqueue_test(&mut qt(1001, 100, "w", 0, 100), &r, now);
        assert!(preempt);
        assert_eq!(p.nr_short_preemptions, 1);
    }

    #[test]
    fn long_latency_task_does_not_preempt() {
        let mut o = opts();
        o.short_task_threshold_us = 5_000;
        let mut p = pl(&o);
        let now = 100 * MS;
        let r = build_reg(&[(1, 100, now + 100 * MS, 10 * MS, 0, 1)]);
        let (_, preempt, _, _, _) = p.enqueue_test(&mut qt(1001, 100, "w", 0, 100), &r, now);
        assert!(!preempt);
    }

    #[test]
    fn batch_background_task_never_preempts() {
        let o = opts();
        let mut p = pl(&o);
        let now = 100 * MS;
        let r = build_reg(&[(1, 100, now + 100 * MS, 1 * MS, 2, 0)]);
        let (_, preempt, _, _, _) = p.enqueue_test(&mut qt(1001, 100, "w", 1 * MS, 100), &r, now);
        assert!(!preempt);
    }

    #[test]
    fn disable_short_preemption_disables_decisions() {
        let mut o = opts();
        o.disable_short_preemption = true;
        let mut p = pl(&o);
        let now = 100 * MS;
        let r = build_reg(&[(1, 100, now + 100 * MS, 0, 0, 1)]);
        let (_, preempt, _, _, _) = p.enqueue_test(&mut qt(1001, 100, "w", 1 * MS, 100), &r, now);
        assert!(!preempt);
        assert_eq!(p.nr_short_preemptions, 0);
    }

    #[test]
    fn unknown_zero_duration_task_does_not_preempt() {
        let o = opts();
        let mut p = pl(&o);
        let now = 100 * MS;
        let r = build_reg(&[(1, 100, now + 100 * MS, 0, 0, 1)]);
        let (_, preempt, _, _, _) = p.enqueue_test(&mut qt(1001, 100, "w", 0, 100), &r, now);
        assert!(!preempt);
    }

    #[test]
    fn preempt_candidate_sorts_ahead_of_normal_work() {
        let o = opts();
        let mut p = pl(&o);
        let now = 100 * MS;
        let tasks = [
            qt(2000, 200, "batch", 1 * MS, 100),
            qt(1000, 100, "lat", 1 * MS, 100),
        ];
        let metas = vec![
            Some(InvocationMeta {
                id: 2,
                tgid: 200,
                deadline_ns: now + 10 * MS,
                estimated_duration_ns: 50 * MS,
                slo_class: SloClass::Batch,
                is_cold_start: false,
                profile_id: None,
                created_at_ns: now,
            }),
            Some(InvocationMeta {
                id: 1,
                tgid: 100,
                deadline_ns: now + 100 * MS,
                estimated_duration_ns: 1 * MS,
                slo_class: SloClass::LatencyCritical,
                is_cold_start: true,
                profile_id: None,
                created_at_ns: now,
            }),
        ];
        let out = p.schedule(&metas, &tasks, &Topology::new().unwrap(), now);
        assert_eq!(out[0].pid, 1000);
        assert!(out[0].preempt);
    }

    #[test]
    fn starvation_guard_wins_over_preemption() {
        let o = opts();
        let mut p = pl(&o);
        let now = 5_000 * MS;
        let mut old = qt(2000, 200, "old", 1 * MS, 100);
        old.stop_ts = now - 3_000 * MS;
        let short = qt(1000, 100, "lat", 1 * MS, 100);
        let metas = vec![
            Some(InvocationMeta {
                id: 2,
                tgid: 200,
                deadline_ns: now + 100 * MS,
                estimated_duration_ns: 1 * MS,
                slo_class: SloClass::LatencyCritical,
                is_cold_start: true,
                profile_id: None,
                created_at_ns: now,
            }),
            Some(InvocationMeta {
                id: 1,
                tgid: 100,
                deadline_ns: now + 100 * MS,
                estimated_duration_ns: 1 * MS,
                slo_class: SloClass::LatencyCritical,
                is_cold_start: true,
                profile_id: None,
                created_at_ns: now,
            }),
        ];
        let out = p.schedule(&metas, &[old, short], &Topology::new().unwrap(), now);
        assert_eq!(out[0].pid, 2000);
        assert!(!out[0].preempt);
        assert!(out[1].preempt);
    }

    #[test]
    fn preempt_slice_is_capped_by_short_threshold() {
        let mut o = opts();
        o.short_task_threshold_us = 2_000;
        o.slo_target_us = 40_000;
        let mut p = pl(&o);
        let now = 100 * MS;
        let r = build_reg(&[(1, 100, now + 100 * MS, 0, 0, 1)]);
        let (_, preempt, _, slice, _) =
            p.enqueue_test(&mut qt(1001, 100, "w", 1 * MS, 100), &r, now);
        assert!(preempt);
        assert_eq!(slice, 2 * MS);
    }

    #[test]
    fn starvation_guard_caps_vtime_lead() {
        let o = opts();
        let mut p = pl(&o);
        p.vruntime_now = 1_000 * MS;
        let (vtime, guarded) = p.cap_vtime_lead(5_000 * MS);
        assert!(guarded);
        assert_eq!(vtime, 3_000 * MS);
    }

    #[test]
    fn starvation_guard_forces_old_runnable_task() {
        let o = opts();
        let mut p = pl(&o);
        p.vruntime_now = 1_000 * MS;
        let now = 5_000 * MS;
        let mut t = qt(3000, 300, "w", 1 * MS, 100);
        t.stop_ts = now - 3_000 * MS;
        assert!(p.should_force_starvation_guard(&t, now));
        assert_eq!(p.guarded_score(), 980 * MS);
    }

    #[test]
    fn edf_earlier_lower() {
        let o = opts();
        let mut p = pl(&o);
        let now = 100 * MS;
        let r = build_reg(&[
            (1, 101, now + 2 * MS, 1 * MS, 0, 0),
            (2, 102, now + 4 * MS, 1 * MS, 0, 0),
        ]);
        let (_, _, se, _, _) = p.enqueue_test(&mut qt(1001, 101, "w", 1 * MS, 100), &r, now);
        let (_, _, sl, _, _) = p.enqueue_test(&mut qt(1002, 102, "w", 1 * MS, 100), &r, now);
        assert!(se < sl, "{} vs {}", se, sl);
    }

    #[test]
    fn cold_edf_boost() {
        let o = opts();
        let mut p = pl(&o);
        let now = 100 * MS;
        let dl = now + 20 * MS;
        let r = build_reg(&[(1, 103, dl, 1 * MS, 0, 1), (2, 104, dl, 1 * MS, 0, 0)]);
        let (cc, _, sc, _, _) = p.enqueue_test(&mut qt(1003, 103, "w", 1 * MS, 100), &r, now);
        assert_eq!(cc, TaskClass::ColdStart);
        p.enqueue_test(&mut qt(1004, 104, "w", 1 * MS, 100), &r, now);
        let (ch, _, sh, _, _) = p.enqueue_test(&mut qt(1004, 104, "w", 1 * MS, 100), &r, now + MS);
        assert_eq!(ch, TaskClass::HotInvocation);
        assert!(sc < sh, "{} vs {}", sc, sh);
    }

    #[test]
    fn batch_heuristic() {
        let mut p = pl(&opts());
        let now = 100 * MS;
        let r = build_reg(&[(1, 105, now + 3 * MS, 1 * MS, 2, 0)]);
        let mut t = qt(1005, 105, "w", 1 * MS, 100);
        let (c, _, sc, _, _) = p.enqueue_test(&mut t, &r, now);
        assert_eq!(c, TaskClass::Background);
        assert_eq!(sc, expected_heuristic(&p, &t, c));
    }

    #[test]
    fn no_meta_heuristic() {
        let o = opts();
        let mut p = pl(&o);
        let r = InvocationRegistry::new();
        let now = 100 * MS;
        let mut t = qt(1006, 999, "w", 5 * MS, 100);
        let (c, _, sc, _, _) = p.enqueue_test(&mut t, &r, now);
        assert_eq!(c, TaskClass::Background);
        assert_eq!(sc, expected_heuristic(&p, &t, c));
    }

    #[test]
    fn zero_deadline_heuristic() {
        let mut p = pl(&opts());
        let now = 100 * MS;
        let r = build_reg(&[(1, 200, 0, 1 * MS, 0, 0)]);
        let mut t = qt(2000, 200, "w", 5 * MS, 100);
        let (c, _, sc, _, _) = p.enqueue_test(&mut t, &r, now);
        assert_eq!(c, TaskClass::ColdStart);
        assert_eq!(sc, expected_heuristic(&p, &t, c));
    }

    #[test]
    fn scoring_disabled() {
        let mut o = opts();
        o.disable_deadline_scoring = true;
        let mut p = pl(&o);
        let now = 100 * MS;
        let r = build_reg(&[(1, 201, now + 20 * MS, 1 * MS, 0, 0)]);
        let mut t = qt(2001, 201, "w", 5 * MS, 100);
        let (c, _, sc, _, _) = p.enqueue_test(&mut t, &r, now);
        assert_eq!(c, TaskClass::ColdStart);
        assert_eq!(sc, expected_heuristic(&p, &t, c));
    }

    #[test]
    fn meta_counter() {
        let o = opts();
        let mut p = pl(&o);
        let r = build_reg(&[(1, 200, 300 * MS, 5 * MS, 0, 0)]);
        p.enqueue_test(&mut qt(2000, 200, "w", 5 * MS, 100), &r, 100 * MS);
        assert_eq!(p.nr_metadata_classified, 1);
        assert_eq!(p.nr_heuristic_classified, 0);
    }

    #[test]
    fn heuristic_counter() {
        let o = opts();
        let mut p = pl(&o);
        let r = InvocationRegistry::new();
        p.enqueue_test(&mut qt(2001, 999, "w", 5 * MS, 100), &r, 100 * MS);
        assert_eq!(p.nr_heuristic_classified, 1);
        assert_eq!(p.nr_metadata_classified, 0);
    }

    #[test]
    fn slo_violation() {
        let o = opts();
        let mut p = pl(&o);
        let now = 200 * MS;
        let r = build_reg(&[(1, 100, 100 * MS, 1 * MS, 0, 0)]);
        p.enqueue_test(&mut qt(1001, 100, "w", 1 * MS, 100), &r, now);
        assert_eq!(p.nr_slo_violations, 1);
    }
}
