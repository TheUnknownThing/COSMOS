// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

mod policy;
mod slack;

use std::time::Duration;

use crate::registry::InvocationRegistry;

use self::policy::compute_allocation;
use self::slack::compute_phase_slack_context;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CoordinationTickStats {
    pub phase_samples: u64,
    pub phase_ctx_updates: u64,
    pub allocation_updates: u64,
    pub cgroup_updates: u64,
}

pub struct CoordinationEngine {
    sample_interval_ns: u64,
    next_sample_at_ns: u64,
}

impl CoordinationEngine {
    pub fn new(sample_interval: Duration) -> Self {
        Self {
            sample_interval_ns: sample_interval.as_nanos() as u64,
            next_sample_at_ns: 0,
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
            if state.completed_at_ns.is_some() {
                continue;
            }
            let phase_ctx = compute_phase_slack_context(&state.meta, now_ns, &state.phase_ctx);
            let allocation = compute_allocation(&state.meta, state.profile.as_ref(), &phase_ctx);

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
