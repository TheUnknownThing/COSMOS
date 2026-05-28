// COSMOS v2: Invocation-centric scheduling policy.
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use std::collections::HashMap;

use scx_utils::Topology;

use crate::bpf::{QueuedTask, RL_CPU_ANY};
use crate::policy::cosmos_pool::{effective_tail_guard_cpus, PoolManager, PoolMetrics, TaskPool};
use crate::policy::{DispatchDecision, PolicyCounters, SchedulingPolicy};
use crate::registry::{InvocationMeta, InvocationRegistry, PhaseSlackContext, RegistryHandle};

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
    pub nr_pool_latency: u64,
    pub nr_pool_batch: u64,
    pub nr_tail_guard_dispatches: u64,
    pub nr_slo_violations: u64,
    pub nr_pool_migrations: u64,
    pub nr_pool_overflow: u64,
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
            nr_pool_latency: c.nr_pool_latency,
            nr_pool_batch: c.nr_pool_batch,
            nr_tail_guard_dispatches: c.nr_tail_guard_dispatches,
            nr_slo_violations: c.nr_slo_violations,
            nr_pool_migrations: c.nr_pool_migrations,
            nr_pool_overflow: c.nr_pool_overflow,
        }
    }
}

pub struct CosmosPolicy {
    pub task_state: HashMap<u32, TaskState>,
    registry: Option<RegistryHandle>,
    vruntime_now: u64,
    slice_ns: u64,
    slice_ns_min: u64,
    slo_target_ns: u64,
    cold_start_boost_ns: u64,
    invocation_comm: Vec<String>,
    pub tail_guard_threshold_ns: u64,
    pub pools_enabled: bool,
    percpu_local: bool,
    pub deadline_scoring_enabled: bool,
    pool_mgr: Option<PoolManager>,
    latency_pool_pct: u32,
    pool_rebalance_interval_ns: u64,
    next_pool_rebalance_at_ns: u64,
    last_latency_depth: u64,
    last_batch_depth: u64,
    interval_latency_home: u64,
    interval_batch_home: u64,
    interval_latency_empty_samples: u64,
    interval_batch_empty_samples: u64,
    interval_samples: u64,
    latency_sel_idx: usize,
    batch_sel_idx: usize,
    tail_guard_sel_idx: usize,
    nr_cold_start_tasks: u64,
    nr_hot_invocation_tasks: u64,
    nr_background_tasks: u64,
    nr_slo_boosted: u64,
    max_pending: u64,
    nr_metadata_classified: u64,
    nr_heuristic_classified: u64,
    nr_pool_latency: u64,
    nr_pool_batch: u64,
    nr_tail_guard_dispatches: u64,
    nr_slo_violations: u64,
    nr_pool_overflow: u64,
}

impl CosmosPolicy {
    pub fn new(opts: &crate::CosmosOpts) -> Self {
        let slo_ns = opts.slo_target_us * NSEC_PER_USEC;
        let tg_thr_ns = match opts.tail_guard_threshold_us {
            Some(u) => u * NSEC_PER_USEC,
            None => slo_ns / 2,
        };
        Self {
            task_state: HashMap::new(),
            registry: None,
            vruntime_now: 0,
            slice_ns: opts.slice_us * NSEC_PER_USEC,
            slice_ns_min: opts.slice_us_min * NSEC_PER_USEC,
            slo_target_ns: slo_ns,
            cold_start_boost_ns: opts.cold_start_boost_us * NSEC_PER_USEC,
            invocation_comm: opts.invocation_comm.clone(),
            tail_guard_threshold_ns: tg_thr_ns,
            pools_enabled: !opts.disable_pools,
            percpu_local: opts.percpu_local,
            deadline_scoring_enabled: !opts.disable_deadline_scoring,
            pool_mgr: None,
            latency_pool_pct: opts.latency_pool_pct,
            pool_rebalance_interval_ns: opts.pool_rebalance_ms * 1_000_000,
            next_pool_rebalance_at_ns: 0,
            last_latency_depth: 0,
            last_batch_depth: 0,
            interval_latency_home: 0,
            interval_batch_home: 0,
            interval_latency_empty_samples: 0,
            interval_batch_empty_samples: 0,
            interval_samples: 0,
            latency_sel_idx: 0,
            batch_sel_idx: 0,
            tail_guard_sel_idx: 0,
            nr_cold_start_tasks: 0,
            nr_hot_invocation_tasks: 0,
            nr_background_tasks: 0,
            nr_slo_boosted: 0,
            max_pending: 0,
            nr_metadata_classified: 0,
            nr_heuristic_classified: 0,
            nr_pool_latency: 0,
            nr_pool_batch: 0,
            nr_tail_guard_dispatches: 0,
            nr_slo_violations: 0,
            nr_pool_overflow: 0,
        }
    }

    pub fn with_registry(mut self, registry: RegistryHandle) -> Self {
        self.registry = Some(registry);
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

    fn phase_ctx_for<'a>(
        &self,
        task: &QueuedTask,
        reg: &'a InvocationRegistry,
    ) -> Option<&'a PhaseSlackContext> {
        reg.lookup_tgid_state(task.tgid)
            .map(|state| &state.phase_ctx)
    }

    fn phase_contexts_from_registry(&self, raw: &[QueuedTask]) -> HashMap<u32, PhaseSlackContext> {
        let Some(registry) = &self.registry else {
            return HashMap::new();
        };
        let Ok(reg) = registry.try_read() else {
            return HashMap::new();
        };
        raw.iter()
            .filter_map(|task| {
                reg.lookup_tgid_state(task.tgid)
                    .map(|state| (task.tgid, state.phase_ctx.clone()))
            })
            .collect()
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
        if m.slo_class == crate::registry::SloClass::None {
            return self.heuristic_classify(task, self.slo_target_ns);
        }
        let effective_slo = self.task_slo_target(Some(m));
        match m.slo_class {
            crate::registry::SloClass::LatencyCritical => {
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
            crate::registry::SloClass::Standard => self.heuristic_classify(task, effective_slo),
            crate::registry::SloClass::Batch => TaskClass::Background,
            crate::registry::SloClass::None => self.heuristic_classify(task, effective_slo),
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

    fn choose_pool(
        &mut self,
        task: &QueuedTask,
        _class: TaskClass,
        meta: Option<&InvocationMeta>,
        now: u64,
    ) -> TaskPool {
        let mut pool = if self.pools_enabled {
            match meta {
                Some(ref m) if m.slo_class == crate::registry::SloClass::LatencyCritical => {
                    TaskPool::Latency
                }
                Some(ref m) if m.slo_class == crate::registry::SloClass::Batch => TaskPool::Batch,
                Some(ref m) if m.slo_class == crate::registry::SloClass::Standard => TaskPool::None,
                _ => TaskPool::None,
            }
        } else {
            TaskPool::None
        };

        if let Some(m) = meta {
            if m.slo_class == crate::registry::SloClass::LatencyCritical && m.deadline_ns > 0 {
                if now > m.deadline_ns {
                    self.nr_slo_violations = self.nr_slo_violations.saturating_add(1);
                } else if self.pools_enabled && self.tail_guard_threshold_ns > 0 {
                    let est = self
                        .task_state
                        .get(&task.tgid)
                        .map_or(0, |s| s.avg_runtime_ns);
                    if m.deadline_ns.saturating_sub(now).saturating_sub(est)
                        < self.tail_guard_threshold_ns
                    {
                        pool = TaskPool::TailGuard;
                        self.nr_tail_guard_dispatches =
                            self.nr_tail_guard_dispatches.saturating_add(1);
                    }
                }
            }
        }

        pool
    }

    fn score_for_vtime(
        &self,
        task: &QueuedTask,
        class: TaskClass,
        now: u64,
        meta: Option<&InvocationMeta>,
        phase_ctx: Option<&PhaseSlackContext>,
        vtime: u64,
    ) -> u64 {
        let fair = vtime.saturating_add(task.exec_runtime.min(self.slice_ns.saturating_mul(100)));
        if self.deadline_scoring_enabled {
            if let Some(m) = meta {
                if m.slo_class == crate::registry::SloClass::LatencyCritical && m.deadline_ns > 0 {
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
                        let score = match class {
                            TaskClass::ColdStart => base.saturating_sub(Self::scale_by_weight(
                                task,
                                self.slo_target_ns.saturating_add(self.cold_start_boost_ns),
                            )),
                            TaskClass::HotInvocation => {
                                base.saturating_sub(Self::scale_by_weight(task, self.slo_target_ns))
                            }
                            TaskClass::Background => base.saturating_add(self.slo_target_ns),
                        };
                        return Self::apply_phase_bias(task, score, phase_ctx);
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
        let score = fair.saturating_sub(Self::scale_by_weight(task, boost));
        Self::apply_phase_bias(task, score, phase_ctx)
    }

    fn apply_phase_bias(
        task: &QueuedTask,
        score: u64,
        phase_ctx: Option<&PhaseSlackContext>,
    ) -> u64 {
        let Some(ctx) = phase_ctx else {
            return score;
        };
        let magnitude = ctx.cpu_priority_modifier.unsigned_abs();
        let scaled = Self::scale_by_weight(task, magnitude);
        if ctx.cpu_priority_modifier >= 0 {
            score.saturating_sub(scaled)
        } else {
            score.saturating_add(scaled)
        }
    }

    fn slice_for(
        &self,
        task: &QueuedTask,
        class: TaskClass,
        pool: TaskPool,
        effective_slo: u64,
        phase_ctx: Option<&PhaseSlackContext>,
    ) -> u64 {
        if phase_ctx.is_some_and(|ctx| !ctx.needs_cpu) {
            return self.slice_ns_min;
        }
        if pool == TaskPool::TailGuard {
            return effective_slo.max(self.slice_ns_min);
        }
        let base = match class {
            TaskClass::ColdStart => effective_slo / 4,
            TaskClass::HotInvocation => effective_slo / 8,
            TaskClass::Background => self.slice_ns,
        };
        Self::scale_by_weight(task, base.max(self.slice_ns_min)).max(self.slice_ns_min)
    }

    fn select_cpu_for_pool(&mut self, pool: TaskPool) -> i32 {
        let mgr = match self.pool_mgr.as_ref() {
            Some(m) => m,
            None => return RL_CPU_ANY,
        };
        if pool == TaskPool::None {
            return RL_CPU_ANY;
        }
        let cpus: Vec<usize> = mgr
            .assignments
            .iter()
            .enumerate()
            .filter(|(_, &p)| p == pool)
            .map(|(i, _)| i)
            .collect();
        if cpus.is_empty() {
            return RL_CPU_ANY;
        }
        let idx = match pool {
            TaskPool::Latency => {
                let i = self.latency_sel_idx % cpus.len();
                self.latency_sel_idx = self.latency_sel_idx.wrapping_add(1);
                i
            }
            TaskPool::Batch => {
                let i = self.batch_sel_idx % cpus.len();
                self.batch_sel_idx = self.batch_sel_idx.wrapping_add(1);
                i
            }
            TaskPool::TailGuard => {
                let i = self.tail_guard_sel_idx % cpus.len();
                self.tail_guard_sel_idx = self.tail_guard_sel_idx.wrapping_add(1);
                i
            }
            TaskPool::None => {
                return RL_CPU_ANY;
            }
        };
        cpus[idx] as i32
    }

    fn reset_pool_interval(&mut self) {
        self.interval_latency_home = 0;
        self.interval_batch_home = 0;
        self.interval_latency_empty_samples = 0;
        self.interval_batch_empty_samples = 0;
        self.interval_samples = 0;
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
    ) -> (TaskClass, TaskPool, u64, u64, bool) {
        let meta = self.meta_for(task, reg);
        let phase_ctx = self.phase_ctx_for(task, reg);
        let has_meta = meta.is_some();
        let class = self.classify(task, meta);

        task.vtime = if task.vtime == 0 {
            self.vruntime_now
        } else {
            task.vtime
                .max(self.vruntime_now.saturating_sub(self.slice_ns))
        };
        let vs = Self::inv_scale(task, task.stop_ts.saturating_sub(task.start_ts));
        task.vtime = task.vtime.saturating_add(vs);
        self.vruntime_now = self.vruntime_now.saturating_add(vs);

        self.update_task_state(task, meta, now);
        let pool = self.choose_pool(task, class, meta, now);
        let score = self.score_for_vtime(task, class, now, meta, phase_ctx, task.vtime);
        let effective_slo = self.task_slo_target(meta);
        let slice = self.slice_for(task, class, pool, effective_slo, phase_ctx);

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
        match pool {
            TaskPool::Latency => {
                self.nr_pool_latency = self.nr_pool_latency.saturating_add(1);
            }
            TaskPool::Batch => {
                self.nr_pool_batch = self.nr_pool_batch.saturating_add(1);
            }
            _ => {}
        }

        (class, pool, score, slice, has_meta)
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
        _p: TaskPool,
        now: u64,
        m: Option<&InvocationMeta>,
    ) -> u64 {
        self.score_for_vtime(t, c, now, m, None, t.vtime)
    }

    #[cfg(test)]
    pub(crate) fn score_with_phase_test(
        &self,
        t: &QueuedTask,
        c: TaskClass,
        now: u64,
        m: Option<&InvocationMeta>,
        phase_ctx: Option<&PhaseSlackContext>,
    ) -> u64 {
        self.score_for_vtime(t, c, now, m, phase_ctx, t.vtime)
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
        let mut decisions: Vec<(u64, u8, bool, u64, i32, i32, TaskPool, u64, u64, u64)> =
            Vec::with_capacity(raw.len());
        let mut lat = 0u64;
        let mut bat = 0u64;
        let phase_contexts = self.phase_contexts_from_registry(raw);

        for (i, task) in raw.iter().enumerate() {
            let meta = resolved_meta.get(i).and_then(|m| m.as_ref());
            let phase_ctx = phase_contexts.get(&task.tgid);
            let has_meta = meta.is_some();
            let class = self.classify(task, meta);

            let effective_vtime = if task.vtime == 0 {
                self.vruntime_now
            } else {
                task.vtime
                    .max(self.vruntime_now.saturating_sub(self.slice_ns))
            };
            let vs = Self::inv_scale(task, task.stop_ts.saturating_sub(task.start_ts));
            let effective_vtime = effective_vtime.saturating_add(vs);
            self.vruntime_now = self.vruntime_now.saturating_add(vs);

            self.update_task_state(task, meta, now);
            let pool = self.choose_pool(task, class, meta, now);
            let score = self.score_for_vtime(task, class, now, meta, phase_ctx, effective_vtime);
            let effective_slo = self.task_slo_target(meta);
            let slice = self.slice_for(task, class, pool, effective_slo, phase_ctx);

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
            match pool {
                TaskPool::Latency => {
                    lat += 1;
                    self.nr_pool_latency = self.nr_pool_latency.saturating_add(1);
                }
                TaskPool::Batch => {
                    bat += 1;
                    self.nr_pool_batch = self.nr_pool_batch.saturating_add(1);
                }
                _ => {}
            }

            decisions.push((
                score,
                class_rank(class),
                has_meta,
                now,
                task.pid,
                task.cpu,
                pool,
                slice,
                task.flags,
                task.enq_cnt,
            ));
        }

        decisions.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| b.2.cmp(&a.2))
                .then_with(|| a.3.cmp(&b.3))
                .then_with(|| a.9.cmp(&b.9))
                .then_with(|| a.4.cmp(&b.4))
        });

        let pending = decisions.len() as u64;
        let latency_cpus = self
            .pool_mgr
            .as_ref()
            .map_or(0, |m| m.count_pool(TaskPool::Latency) as u64);
        let batch_cpus = self
            .pool_mgr
            .as_ref()
            .map_or(0, |m| m.count_pool(TaskPool::Batch) as u64);

        let mut latency_dispatched = 0u64;
        let mut batch_dispatched = 0u64;
        let mut overflow = 0u64;

        let mut out = Vec::with_capacity(decisions.len());
        for (_score, _cr, _hm, _obs, pid, task_cpu, pool, slice, enq_flags, enq_cnt) in decisions {
            let cpu = if self.percpu_local {
                task_cpu
            } else {
                match pool {
                    TaskPool::Latency => {
                        if latency_dispatched < latency_cpus {
                            latency_dispatched += 1;
                            self.select_cpu_for_pool(TaskPool::Latency)
                        } else {
                            overflow += 1;
                            RL_CPU_ANY
                        }
                    }
                    TaskPool::Batch => {
                        if batch_dispatched < batch_cpus {
                            batch_dispatched += 1;
                            self.select_cpu_for_pool(TaskPool::Batch)
                        } else {
                            overflow += 1;
                            RL_CPU_ANY
                        }
                    }
                    _ => RL_CPU_ANY,
                }
            };
            out.push(DispatchDecision {
                pid,
                cpu,
                slice_ns: slice,
                vtime: _score,
                enq_flags,
                enq_cnt,
            });
        }

        self.max_pending = self.max_pending.max(pending);
        self.last_latency_depth = lat;
        self.last_batch_depth = bat;
        self.nr_pool_overflow = self.nr_pool_overflow.saturating_add(overflow);
        self.interval_latency_home = self.interval_latency_home.saturating_add(lat);
        self.interval_batch_home = self.interval_batch_home.saturating_add(bat);
        self.interval_samples = self.interval_samples.saturating_add(1);
        if lat == 0 {
            self.interval_latency_empty_samples =
                self.interval_latency_empty_samples.saturating_add(1);
        }
        if bat == 0 {
            self.interval_batch_empty_samples = self.interval_batch_empty_samples.saturating_add(1);
        }

        out
    }

    fn init(&mut self, nr_cpus: usize, tail_guard_cpus: u32) {
        if self.pools_enabled {
            let tg = effective_tail_guard_cpus(nr_cpus, tail_guard_cpus);
            if tg == 0 {
                self.tail_guard_threshold_ns = 0;
            }
            self.pool_mgr = Some(PoolManager::new(nr_cpus, self.latency_pool_pct, tg));
        } else {
            self.pool_mgr = Some(PoolManager::disabled(nr_cpus));
        }
    }

    fn tick(&mut self, _registry: &InvocationRegistry, now_ns: u64) {
        self.prune_task_state(now_ns);
        if self.pool_rebalance_interval_ns > 0 && now_ns < self.next_pool_rebalance_at_ns {
            return;
        }
        let metrics = PoolMetrics {
            latency_queue_depth: self.last_latency_depth,
            batch_queue_depth: self.last_batch_depth,
            latency_cpu_count: self
                .pool_mgr
                .as_ref()
                .map_or(0, |m| m.count_pool(TaskPool::Latency) as u64),
            batch_cpu_count: self
                .pool_mgr
                .as_ref()
                .map_or(0, |m| m.count_pool(TaskPool::Batch) as u64),
            latency_home_depth: self.interval_latency_home,
            batch_home_depth: self.interval_batch_home,
            latency_empty_samples: self.interval_latency_empty_samples,
            batch_empty_samples: self.interval_batch_empty_samples,
            samples: self.interval_samples,
        };
        if let Some(mgr) = self.pool_mgr.as_mut() {
            mgr.rebalance(&metrics);
        }
        self.reset_pool_interval();
        if self.pool_rebalance_interval_ns > 0 {
            self.next_pool_rebalance_at_ns = now_ns.saturating_add(self.pool_rebalance_interval_ns);
        }
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
            nr_pool_latency: self.nr_pool_latency,
            nr_pool_batch: self.nr_pool_batch,
            nr_tail_guard_dispatches: self.nr_tail_guard_dispatches,
            nr_slo_violations: self.nr_slo_violations,
            nr_pool_migrations: self.pool_mgr.as_ref().map_or(0, |m| m.nr_pool_migrations),
            nr_pool_overflow: self.nr_pool_overflow,
        }
    }

    fn counters(&self) -> PolicyCounters {
        PolicyCounters {
            nr_cold_start_tasks: self.nr_cold_start_tasks,
            nr_hot_invocation_tasks: self.nr_hot_invocation_tasks,
            nr_background_tasks: self.nr_background_tasks,
            nr_slo_boosted: self.nr_slo_boosted,
            max_pending: self.max_pending,
            nr_metadata_classified: self.nr_metadata_classified,
            nr_heuristic_classified: self.nr_heuristic_classified,
            nr_pool_latency: self.nr_pool_latency,
            nr_pool_batch: self.nr_pool_batch,
            nr_tail_guard_dispatches: self.nr_tail_guard_dispatches,
            nr_slo_violations: self.nr_slo_violations,
            nr_pool_migrations: self.pool_mgr.as_ref().map_or(0, |m| m.nr_pool_migrations),
            nr_pool_overflow: self.nr_pool_overflow,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{PhaseKind, PhaseSlackContext, SlackLevel, SloClass};
    const MS: u64 = 1_000_000;

    fn opts() -> crate::CosmosOpts {
        crate::CosmosOpts {
            slice_us: 20000,
            slice_us_min: 500,
            slo_target_us: 10000,
            cold_start_boost_us: 20000,
            invocation_comm: vec![],
            percpu_local: false,
            tail_guard_threshold_us: None,
            disable_pools: false,
            disable_deadline_scoring: false,
            latency_pool_pct: 50,
            pool_rebalance_ms: 500,
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

    fn build_reg(entries: &[(u64, u32, u64, u32, u32)]) -> InvocationRegistry {
        let mut r = InvocationRegistry::new();
        for &(id, tgid, dl, slo, cold) in entries {
            r.upsert(InvocationMeta {
                id,
                tgid,
                deadline_ns: dl,
                estimated_duration_ns: 0,
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
        let r = build_reg(&[(1, 100, 200 * MS, 0, 1)]);
        assert_eq!(
            p.classify_test(&qt(1001, 100, "w", 5 * MS, 100), &r),
            TaskClass::ColdStart
        );
    }
    #[test]
    fn reg_batch_bg() {
        let o = opts();
        let p = pl(&o);
        let r = build_reg(&[(1, 100, 200 * MS, 2, 0)]);
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
        let r1 = build_reg(&[(1, 100, now + 200 * MS, 0, 0)]);
        let mut t = qt(1001, 100, "w", 8 * MS, 100);
        p.enqueue_test(&mut t, &r1, now);
        assert_eq!(p.task_state.get(&100).unwrap().wakeups, 1);
        let r2 = build_reg(&[(2, 100, now + 200 * MS, 0, 0)]);
        let mut t2 = qt(1001, 100, "w", 2 * MS, 100);
        let (c, _, _, _, _) = p.enqueue_test(&mut t2, &r2, now + MS);
        assert_eq!(c, TaskClass::ColdStart);
        assert_eq!(p.task_state.get(&100).unwrap().wakeups, 1);
    }
    #[test]
    fn reg_hot_after_first() {
        let o = opts();
        let mut p = pl(&o);
        let r = build_reg(&[(1, 100, 200 * MS, 0, 0)]);
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
        let r = build_reg(&[(1, 100, 200 * MS, 1, 0)]);
        assert_eq!(
            p.classify_test(&qt(1001, 100, "w", 5 * MS, 100), &r),
            TaskClass::Background
        );
    }

    #[test]
    fn lat_crit_lat_pool() {
        let o = opts();
        let mut p = pl(&o);
        let r = build_reg(&[(1, 100, 200 * MS, 0, 0)]);
        let (_, pl, _, _, _) = p.enqueue_test(&mut qt(1001, 100, "w", 5 * MS, 100), &r, 100 * MS);
        assert_eq!(pl, TaskPool::Latency);
    }
    #[test]
    fn batch_bat_pool() {
        let o = opts();
        let mut p = pl(&o);
        let r = build_reg(&[(1, 100, 200 * MS, 2, 0)]);
        let (_, pl, _, _, _) = p.enqueue_test(&mut qt(1001, 100, "w", 5 * MS, 100), &r, 100 * MS);
        assert_eq!(pl, TaskPool::Batch);
    }
    #[test]
    fn standard_metadata_uses_shared_pool() {
        let o = opts();
        let mut p = pl(&o);
        let r = build_reg(&[(1, 100, 200 * MS, 1, 0)]);
        let (_, pl, _, _, _) = p.enqueue_test(&mut qt(1001, 100, "w", 5 * MS, 100), &r, 100 * MS);
        assert_eq!(pl, TaskPool::None);
    }
    #[test]
    fn pools_disabled_none() {
        let mut o = opts();
        o.disable_pools = true;
        let mut p = pl(&o);
        let r = build_reg(&[(1, 100, 200 * MS, 0, 0)]);
        let (_, pl, _, _, _) = p.enqueue_test(&mut qt(1001, 100, "w", 5 * MS, 100), &r, 100 * MS);
        assert_eq!(pl, TaskPool::None);
    }

    #[test]
    fn tg_promote() {
        let o = opts();
        let mut p = pl(&o);
        let now = 100 * MS;
        let r = build_reg(&[(1, 100, now + 3 * MS, 0, 0)]);
        let (_, pool, _, _, _) = p.enqueue_test(&mut qt(1001, 100, "w", 1 * MS, 100), &r, now);
        assert_eq!(pool, TaskPool::TailGuard);
        assert_eq!(p.nr_tail_guard_dispatches, 1);
    }
    #[test]
    fn init_without_tail_guard_cpus_disables_tg() {
        let o = opts();
        let mut p = pl(&o);
        p.init(48, 0);
        let now = 100 * MS;
        let r = build_reg(&[(1, 100, now + 3 * MS, 0, 0)]);
        let (_, pool, _, _, _) = p.enqueue_test(&mut qt(1001, 100, "w", 1 * MS, 100), &r, now);
        assert_eq!(pool, TaskPool::Latency);
        assert_eq!(p.nr_tail_guard_dispatches, 0);
    }
    #[test]
    fn no_tg_no_meta() {
        let o = opts();
        let mut p = pl(&o);
        let r = InvocationRegistry::new();
        let (_, pool, _, _, _) = p.enqueue_test(&mut qt(1001, 999, "w", 1 * MS, 100), &r, 100 * MS);
        assert_eq!(pool, TaskPool::None);
        assert_eq!(p.nr_tail_guard_dispatches, 0);
    }
    #[test]
    fn slo_violation() {
        let o = opts();
        let mut p = pl(&o);
        let now = 200 * MS;
        let r = build_reg(&[(1, 100, 100 * MS, 0, 0)]);
        let (_, pool, _, _, _) = p.enqueue_test(&mut qt(1001, 100, "w", 1 * MS, 100), &r, now);
        assert_eq!(pool, TaskPool::Latency);
        assert_eq!(p.nr_slo_violations, 1);
    }
    #[test]
    fn no_tg_above_thr() {
        let o = opts();
        let mut p = pl(&o);
        let now = 100 * MS;
        let r = build_reg(&[(1, 100, now + 20 * MS, 0, 0)]);
        let (_, pool, _, _, _) = p.enqueue_test(&mut qt(1001, 100, "w", 1 * MS, 100), &r, now);
        assert_eq!(pool, TaskPool::Latency);
        assert_eq!(p.nr_tail_guard_dispatches, 0);
    }
    #[test]
    fn tg_multi() {
        let o = opts();
        let mut p = pl(&o);
        let now = 100 * MS;
        let r = build_reg(&[
            (1, 101, now + 1 * MS, 0, 0),
            (2, 102, now + 2 * MS, 0, 0),
            (3, 103, now + 1 * MS, 0, 0),
        ]);
        for (pid, tgid) in [(1001, 101), (1002, 102), (1003, 103)] {
            let (_, pl, _, _, _) = p.enqueue_test(&mut qt(pid, tgid, "w", 1 * MS, 100), &r, now);
            assert_eq!(pl, TaskPool::TailGuard);
        }
        assert_eq!(p.nr_tail_guard_dispatches, 3);
    }

    #[test]
    fn none_pool_uses_shared_dispatch() {
        let o = opts();
        let mut p = pl(&o);
        p.init(4, 0);
        assert_eq!(
            p.select_cpu_for_pool(TaskPool::None),
            crate::bpf::RL_CPU_ANY
        );
    }

    #[test]
    fn edf_earlier_lower() {
        let mut o = opts();
        o.tail_guard_threshold_us = Some(0);
        let mut p = pl(&o);
        let now = 100 * MS;
        let r = build_reg(&[(1, 101, now + 2 * MS, 0, 0), (2, 102, now + 4 * MS, 0, 0)]);
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
        let r = build_reg(&[(1, 103, dl, 0, 1), (2, 104, dl, 0, 0)]);
        let (cc, _, sc, _, _) = p.enqueue_test(&mut qt(1003, 103, "w", 1 * MS, 100), &r, now);
        assert_eq!(cc, TaskClass::ColdStart);
        p.enqueue_test(&mut qt(1004, 104, "w", 1 * MS, 100), &r, now);
        let (ch, _, sh, _, _) = p.enqueue_test(&mut qt(1004, 104, "w", 1 * MS, 100), &r, now + MS);
        assert_eq!(ch, TaskClass::HotInvocation);
        assert!(sc < sh, "{} vs {}", sc, sh);
    }
    #[test]
    fn batch_heuristic() {
        let mut o = opts();
        o.tail_guard_threshold_us = Some(0);
        let mut p = pl(&o);
        let now = 100 * MS;
        let r = build_reg(&[(1, 105, now + 3 * MS, 2, 0)]);
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
        let mut o = opts();
        o.tail_guard_threshold_us = Some(0);
        let mut p = pl(&o);
        let now = 100 * MS;
        let r = build_reg(&[(1, 200, 0, 0, 0)]);
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
        let r = build_reg(&[(1, 201, now + 20 * MS, 0, 0)]);
        let mut t = qt(2001, 201, "w", 5 * MS, 100);
        let (c, _, sc, _, _) = p.enqueue_test(&mut t, &r, now);
        assert_eq!(c, TaskClass::ColdStart);
        assert_eq!(sc, expected_heuristic(&p, &t, c));
    }

    #[test]
    fn phase_priority_bias_changes_score() {
        let o = opts();
        let p = pl(&o);
        let mut t = qt(2100, 210, "w", 1 * MS, 100);
        t.vtime = 100 * MS;
        let base = p.score_test(&t, TaskClass::Background, TaskPool::None, 100 * MS, None);
        let boost = PhaseSlackContext {
            phase: PhaseKind::CpuBound,
            slack_level: SlackLevel::Critical,
            cpu_priority_modifier: 10 * MS as i64,
            needs_cpu: true,
            last_phase_update_ns: 0,
        };
        let penalize = PhaseSlackContext {
            phase: PhaseKind::IoBound,
            slack_level: SlackLevel::Relaxed,
            cpu_priority_modifier: -(10 * MS as i64),
            needs_cpu: false,
            last_phase_update_ns: 0,
        };

        let boosted =
            p.score_with_phase_test(&t, TaskClass::Background, 100 * MS, None, Some(&boost));
        let penalized =
            p.score_with_phase_test(&t, TaskClass::Background, 100 * MS, None, Some(&penalize));

        assert!(boosted < base);
        assert!(penalized > base);
    }

    #[test]
    fn non_cpu_phase_gets_min_slice() {
        let o = opts();
        let mut p = pl(&o);
        let now = 100 * MS;
        let mut r = build_reg(&[(1, 211, now + 100 * MS, 0, 0)]);
        r.update_phase_ctx(
            211,
            1,
            PhaseSlackContext {
                phase: PhaseKind::IoBound,
                slack_level: SlackLevel::Normal,
                cpu_priority_modifier: -5 * MS as i64,
                needs_cpu: false,
                last_phase_update_ns: now,
            },
        );
        let (_, _, _, slice, _) = p.enqueue_test(&mut qt(2110, 211, "w", 1 * MS, 100), &r, now);

        assert_eq!(slice, p.slice_ns_min);
    }

    #[test]
    fn meta_counter() {
        let o = opts();
        let mut p = pl(&o);
        let r = build_reg(&[(1, 200, 300 * MS, 0, 0)]);
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
    fn tick_rebalance_happens() {
        let o = opts();
        let mut p = pl(&o);
        p.init(4, 0);
        p.last_latency_depth = 5;
        p.last_batch_depth = 0;
        p.interval_latency_home = 5;
        p.interval_batch_empty_samples = 1;
        p.interval_samples = 1;
        p.tick(&InvocationRegistry::new(), 500 * MS);
        assert!(p.pool_mgr.as_ref().unwrap().nr_pool_migrations > 0);
    }

    #[test]
    fn tick_no_rebalance_when_balanced() {
        let o = opts();
        let mut p = pl(&o);
        p.init(4, 0);
        let mig_before = p.pool_mgr.as_ref().unwrap().nr_pool_migrations;
        p.tick(&InvocationRegistry::new(), 0);
        assert_eq!(p.pool_mgr.as_ref().unwrap().nr_pool_migrations, mig_before);
    }
}
