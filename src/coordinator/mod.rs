// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

mod phase_tracker;
mod policy;
mod slack;

use std::time::Duration;

use crate::registry::{InvocationRegistry, PhaseKind, PhaseSlackContext};

use self::phase_tracker::PredictivePhaseTracker;
use self::policy::compute_allocation_for_state;
use self::slack::compute_phase_slack_context;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CoordinationTickStats {
    pub phase_samples: u64,
    pub phase_ctx_updates: u64,
    pub allocation_updates: u64,
    pub cgroup_updates: u64,
}

pub struct CoordinationEngine {
    predictor: PredictivePhaseTracker,
    sample_interval_ns: u64,
    next_sample_at_ns: u64,
    phase_prediction_enabled: bool,
    warm_value_enabled: bool,
}

impl CoordinationEngine {
    pub fn new(sample_interval: Duration) -> Self {
        Self::with_options(sample_interval, true, true)
    }

    pub fn with_phase_prediction(
        sample_interval: Duration,
        phase_prediction_enabled: bool,
    ) -> Self {
        Self::with_options(sample_interval, phase_prediction_enabled, true)
    }

    pub fn with_options(
        sample_interval: Duration,
        phase_prediction_enabled: bool,
        warm_value_enabled: bool,
    ) -> Self {
        Self {
            predictor: PredictivePhaseTracker::new(),
            sample_interval_ns: sample_interval.as_nanos() as u64,
            next_sample_at_ns: 0,
            phase_prediction_enabled,
            warm_value_enabled,
        }
    }

    pub fn should_tick(&self, now_ns: u64) -> bool {
        now_ns >= self.next_sample_at_ns
    }

    pub fn tick(
        &mut self,
        registry: &mut InvocationRegistry,
        now_ns: u64,
    ) -> CoordinationTickStats {
        if !self.should_tick(now_ns) {
            return CoordinationTickStats::default();
        }
        self.next_sample_at_ns = now_ns.saturating_add(self.sample_interval_ns.max(1));

        let states = registry.active_state_snapshot();
        let mut stats = CoordinationTickStats::default();

        for state in states {
            let (phase_ctx, allocation) = if state.completed_at_ns.is_some() {
                let phase_ctx = completed_phase_ctx(now_ns, &state.phase_ctx);
                let allocation = compute_allocation_for_state(
                    &state,
                    &phase_ctx,
                    now_ns,
                    self.warm_value_enabled,
                );
                (phase_ctx, allocation)
            } else {
                let predicted_phase = self
                    .phase_prediction_enabled
                    .then(|| self.predictor.predict(&state, now_ns))
                    .flatten();
                let phase = predicted_phase.unwrap_or(state.phase_ctx.phase);
                let phase_ctx = compute_phase_slack_context(
                    &state.meta,
                    phase,
                    now_ns,
                    &state.phase_ctx,
                    predicted_phase.is_some(),
                );
                let allocation = compute_allocation_for_state(
                    &state,
                    &phase_ctx,
                    now_ns,
                    self.warm_value_enabled,
                );
                (phase_ctx, allocation)
            };

            if phase_ctx != state.phase_ctx {
                registry.update_phase_ctx(state.meta.tgid, state.meta.id, phase_ctx);
                stats.phase_ctx_updates = stats.phase_ctx_updates.saturating_add(1);
            }
            if allocation != state.allocation {
                registry.update_allocation(state.meta.tgid, state.meta.id, allocation);
                stats.allocation_updates = stats.allocation_updates.saturating_add(1);
            }
        }

        stats
    }

    pub fn remove_tgid(&mut self, _tgid: u32) {}
}

fn completed_phase_ctx(now_ns: u64, prev: &PhaseSlackContext) -> PhaseSlackContext {
    PhaseSlackContext {
        phase: PhaseKind::Idle,
        slack_level: crate::registry::SlackLevel::Normal,
        cpu_priority_modifier: 0,
        needs_cpu: false,
        last_phase_update_ns: if prev.phase == PhaseKind::Idle {
            prev.last_phase_update_ns
        } else {
            now_ns
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{
        InvocationMeta, InvocationRegistry, PhaseKind, PhaseSlackContext, ResourceProfile,
        SlackLevel, SloClass,
    };

    #[test]
    fn coordination_tick_updates_slack_and_allocation_in_registry() {
        let now = 100_000_000;
        let mut registry = InvocationRegistry::new();
        registry.upsert_with_profile(
            InvocationMeta {
                id: 1,
                tgid: 101,
                deadline_ns: now + 5_000_000,
                estimated_duration_ns: 10_000_000,
                slo_class: SloClass::LatencyCritical,
                is_cold_start: false,
                profile_id: None,
                created_at_ns: now.saturating_sub(100_000_000),
            },
            Some(ResourceProfile {
                memory_bytes: Some(128 * 1024 * 1024),
                working_set_bytes: Some(64 * 1024 * 1024),
                io_bandwidth_bytes_per_sec: Some(1_000_000),
                ..ResourceProfile::default()
            }),
        );

        let mut engine = CoordinationEngine::new(Duration::from_millis(100));
        let stats = engine.tick(&mut registry, now);
        let state = registry.lookup_tgid_state(101).unwrap();

        assert_eq!(stats.phase_samples, 0);
        assert_eq!(stats.phase_ctx_updates, 1);
        assert_eq!(stats.allocation_updates, 1);
        assert_eq!(stats.cgroup_updates, 0);
        assert_eq!(state.phase_ctx.slack_level, SlackLevel::Critical);
        assert_eq!(state.phase_ctx.phase, PhaseKind::Unknown);
        assert_eq!(state.allocation.memory_min_bytes, Some(64 * 1024 * 1024));
        assert!(state.allocation.memory_high_bytes.unwrap() > 128 * 1024 * 1024);
    }

    #[test]
    fn coordination_tick_prefers_profile_phase_prediction() {
        let now = 50_000_000;
        let mut registry = InvocationRegistry::new();
        registry.upsert_with_profile(
            InvocationMeta {
                id: 1,
                tgid: 101,
                deadline_ns: 200_000_000,
                estimated_duration_ns: 100_000_000,
                slo_class: SloClass::LatencyCritical,
                is_cold_start: false,
                profile_id: Some("pipeline".to_string()),
                created_at_ns: 0,
            },
            Some(ResourceProfile {
                phase_sequence: Some(vec![
                    cosmos_metadata_model::PhaseSequenceEntry {
                        kind: "IoBound".to_string(),
                        duration_pct: 30,
                    },
                    cosmos_metadata_model::PhaseSequenceEntry {
                        kind: "CpuBound".to_string(),
                        duration_pct: 50,
                    },
                    cosmos_metadata_model::PhaseSequenceEntry {
                        kind: "IoBound".to_string(),
                        duration_pct: 20,
                    },
                ]),
                ..ResourceProfile::default()
            }),
        );

        let mut engine =
            CoordinationEngine::with_phase_prediction(Duration::from_millis(100), true);
        let stats = engine.tick(&mut registry, now);
        let state = registry.lookup_tgid_state(101).unwrap();

        assert_eq!(stats.phase_samples, 0);
        assert_eq!(stats.phase_ctx_updates, 1);
        assert_eq!(stats.cgroup_updates, 0);
        assert_eq!(state.phase_ctx.phase, PhaseKind::CpuBound);
        assert!(state.phase_ctx.cpu_priority_modifier > 0);
    }

    #[test]
    fn coordination_tick_can_disable_profile_phase_prediction() {
        let now = 50_000_000;
        let mut registry = InvocationRegistry::new();
        registry.upsert_with_profile(
            InvocationMeta {
                id: 1,
                tgid: 101,
                deadline_ns: 200_000_000,
                estimated_duration_ns: 100_000_000,
                slo_class: SloClass::LatencyCritical,
                is_cold_start: false,
                profile_id: Some("pipeline".to_string()),
                created_at_ns: 0,
            },
            Some(ResourceProfile {
                phase_sequence: Some(vec![cosmos_metadata_model::PhaseSequenceEntry {
                    kind: "CpuBound".to_string(),
                    duration_pct: 100,
                }]),
                ..ResourceProfile::default()
            }),
        );

        let mut engine =
            CoordinationEngine::with_phase_prediction(Duration::from_millis(100), false);
        engine.tick(&mut registry, now);
        let state = registry.lookup_tgid_state(101).unwrap();

        assert_eq!(state.phase_ctx.phase, PhaseKind::Unknown);
    }

    #[test]
    fn coordination_tick_updates_completed_warm_value_allocation() {
        let completed_at = 1_000_000_000;
        let now = completed_at + 1_000_000_000;
        let mut registry = InvocationRegistry::new();
        registry.upsert_with_profile(
            InvocationMeta {
                id: 1,
                tgid: 101,
                deadline_ns: 0,
                estimated_duration_ns: 250_000_000,
                slo_class: SloClass::Standard,
                is_cold_start: false,
                profile_id: Some("memory_heavy".to_string()),
                created_at_ns: 0,
            },
            Some(ResourceProfile {
                memory_bytes: Some(512 * 1024 * 1024),
                working_set_bytes: Some(256 * 1024 * 1024),
                cold_load_penalty_ns: Some(2_000_000_000),
                ..ResourceProfile::default()
            }),
        );
        registry.mark_completed_by_tgid(101, completed_at);

        let mut engine = CoordinationEngine::with_options(Duration::from_millis(100), true, true);
        let stats = engine.tick(&mut registry, now);
        let state = registry.lookup_tgid_state(101).unwrap();

        assert_eq!(stats.phase_ctx_updates, 1);
        assert_eq!(stats.allocation_updates, 1);
        assert_eq!(state.phase_ctx.phase, PhaseKind::Idle);
        assert!(!state.phase_ctx.needs_cpu);
        assert_eq!(state.allocation.memory_min_bytes, Some(256 * 1024 * 1024));
        assert_eq!(state.allocation.io_weight, None);
    }

    #[test]
    fn coordination_tick_never_samples_cgroups() {
        let now = 100_000_000;
        let mut registry = InvocationRegistry::new();
        registry.upsert(InvocationMeta {
            id: 1,
            tgid: 201,
            deadline_ns: now + 50_000_000,
            estimated_duration_ns: 0,
            slo_class: SloClass::Standard,
            is_cold_start: false,
            profile_id: None,
            created_at_ns: now.saturating_sub(50_000_000),
        });
        registry.update_phase_ctx(
            201,
            1,
            PhaseSlackContext {
                phase: PhaseKind::Unknown,
                slack_level: SlackLevel::Normal,
                cpu_priority_modifier: 0,
                needs_cpu: true,
                last_phase_update_ns: 7,
            },
        );

        let mut engine = CoordinationEngine::new(Duration::from_millis(100));
        let stats = engine.tick(&mut registry, now);
        let state = registry.lookup_tgid_state(201).unwrap();

        assert_eq!(stats.phase_samples, 0);
        assert_eq!(stats.cgroup_updates, 0);
        assert_eq!(state.phase_ctx.last_phase_update_ns, 7);
    }
}
