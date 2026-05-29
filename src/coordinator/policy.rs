// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use crate::registry::{
    InvocationMeta, InvocationState, PhaseKind, PhaseSlackContext, ResourceAllocation,
    ResourceProfile, SlackLevel, SloClass,
};

const MIB: u64 = 1024 * 1024;
const WARM_REUSE_HALF_LIFE_NS: f64 = 30_000_000_000.0;
const WARM_VALUE_HIGH_THRESHOLD: f64 = 0.25;
const WARM_VALUE_LOW_THRESHOLD: f64 = 0.05;

pub fn compute_allocation(
    meta: &InvocationMeta,
    profile: Option<&ResourceProfile>,
    phase_ctx: &PhaseSlackContext,
) -> ResourceAllocation {
    let mut allocation = ResourceAllocation {
        io_weight: Some(io_weight(meta, profile, phase_ctx)),
        network_priority: Some(network_priority(meta, phase_ctx)),
        ..ResourceAllocation::default()
    };

    if let Some(profile) = profile {
        apply_memory_profile(&mut allocation, profile, phase_ctx);
        apply_io_profile(&mut allocation, meta, profile, phase_ctx);
        apply_network_profile(&mut allocation, meta, profile, phase_ctx);
    }

    allocation
}

pub fn compute_allocation_for_state(
    state: &InvocationState,
    phase_ctx: &PhaseSlackContext,
    now_ns: u64,
    warm_value_enabled: bool,
) -> ResourceAllocation {
    if state.completed_at_ns.is_some() {
        return compute_completed_allocation(state, now_ns, warm_value_enabled);
    }
    compute_allocation(&state.meta, state.profile.as_ref(), phase_ctx)
}

fn io_weight(
    meta: &InvocationMeta,
    profile: Option<&ResourceProfile>,
    phase_ctx: &PhaseSlackContext,
) -> u64 {
    let base = profile.and_then(|p| p.io_weight).unwrap_or(500);
    let slack_bias: i64 = match phase_ctx.slack_level {
        SlackLevel::Critical => 300,
        SlackLevel::Tight => 150,
        SlackLevel::Normal => 0,
        SlackLevel::Relaxed => -150,
    };
    let phase_bias: i64 = match phase_ctx.phase {
        PhaseKind::IoBound => 250,
        PhaseKind::Mixed => 100,
        PhaseKind::MemoryBound => -50,
        PhaseKind::Idle => -250,
        PhaseKind::CpuBound | PhaseKind::Unknown => 0,
    };
    let class_bias: i64 = match meta.slo_class {
        SloClass::LatencyCritical => 100,
        SloClass::Standard | SloClass::None => 0,
        SloClass::Batch => -250,
    };
    ((base as i64) + slack_bias + phase_bias + class_bias).clamp(10, 1000) as u64
}

fn network_priority(meta: &InvocationMeta, phase_ctx: &PhaseSlackContext) -> u32 {
    let base: u32 = match phase_ctx.slack_level {
        SlackLevel::Critical => 1,
        SlackLevel::Tight => 2,
        SlackLevel::Normal => 4,
        SlackLevel::Relaxed => 6,
    };
    let phase_adjusted = match phase_ctx.phase {
        PhaseKind::IoBound | PhaseKind::Mixed => base.saturating_sub(1),
        PhaseKind::Idle => base.saturating_add(1),
        PhaseKind::CpuBound | PhaseKind::MemoryBound | PhaseKind::Unknown => base,
    };
    match meta.slo_class {
        SloClass::LatencyCritical => phase_adjusted.min(2),
        SloClass::Batch => phase_adjusted.max(6),
        SloClass::Standard | SloClass::None => phase_adjusted,
    }
    .clamp(1, 7)
}

fn apply_memory_profile(
    allocation: &mut ResourceAllocation,
    profile: &ResourceProfile,
    phase_ctx: &PhaseSlackContext,
) {
    let working_set = profile.working_set_bytes.or(profile.memory_bytes);
    if let Some(working_set) = working_set.filter(|v| *v > 0) {
        allocation.memory_min_bytes = match phase_ctx.slack_level {
            SlackLevel::Critical | SlackLevel::Tight => Some(working_set),
            SlackLevel::Normal if phase_ctx.phase == PhaseKind::MemoryBound => {
                Some(working_set.saturating_div(2).max(1))
            }
            _ => None,
        };
    }

    if let Some(memory_bytes) = profile.memory_bytes.filter(|v| *v > 0) {
        let (num, den) = match phase_ctx.slack_level {
            SlackLevel::Critical => (3, 1),
            SlackLevel::Tight => (2, 1),
            SlackLevel::Normal => (3, 2),
            SlackLevel::Relaxed => (5, 4),
        };
        let high = memory_bytes
            .saturating_mul(num)
            .saturating_div(den)
            .saturating_add(64 * MIB);
        allocation.memory_high_bytes = Some(match working_set {
            Some(ws) => high.max(ws.saturating_add(32 * MIB)),
            None => high,
        });
    }
}

fn compute_completed_allocation(
    state: &InvocationState,
    now_ns: u64,
    warm_value_enabled: bool,
) -> ResourceAllocation {
    let mut allocation = ResourceAllocation::default();
    let Some(profile) = state.profile.as_ref() else {
        return allocation;
    };

    if let Some(memory_bytes) = profile.memory_bytes.filter(|v| *v > 0) {
        let working_set = profile.working_set_bytes.or(profile.memory_bytes);
        let high = memory_bytes.saturating_add(64 * MIB);
        allocation.memory_high_bytes = Some(match working_set {
            Some(ws) => high.max(ws.saturating_add(32 * MIB)),
            None => high,
        });
    }

    if !warm_value_enabled {
        return allocation;
    }

    let Some(working_set) = profile
        .working_set_bytes
        .or(profile.memory_bytes)
        .filter(|v| *v > 0)
    else {
        return allocation;
    };

    let Some(value) = warm_state_value(state, now_ns) else {
        allocation.memory_min_bytes = None;
        return allocation;
    };

    allocation.memory_min_bytes = if value > WARM_VALUE_HIGH_THRESHOLD {
        Some(working_set)
    } else if value > WARM_VALUE_LOW_THRESHOLD {
        Some(working_set.saturating_div(2).max(1))
    } else {
        None
    };
    allocation
}

fn warm_state_value(state: &InvocationState, now_ns: u64) -> Option<f64> {
    let completed_at_ns = state.completed_at_ns?;
    let idle_ns = now_ns.saturating_sub(completed_at_ns).max(1);
    let cold_load_penalty_ns = state
        .cold_load_penalty_ns
        .or_else(|| state.profile.as_ref().and_then(|p| p.cold_load_penalty_ns))?;
    if cold_load_penalty_ns == 0 {
        return None;
    }

    let history_probability = 1.0 - 0.5_f64.powf(state.invocation_count.max(1) as f64);
    let decay = 0.5_f64.powf(idle_ns as f64 / WARM_REUSE_HALF_LIFE_NS);
    let reuse_probability = (history_probability * decay).clamp(0.0, 1.0);
    Some(reuse_probability * (cold_load_penalty_ns as f64 / idle_ns as f64))
}

fn apply_io_profile(
    allocation: &mut ResourceAllocation,
    meta: &InvocationMeta,
    profile: &ResourceProfile,
    phase_ctx: &PhaseSlackContext,
) {
    let Some(bps) = profile.io_bandwidth_bytes_per_sec.filter(|v| *v > 0) else {
        return;
    };
    let should_limit =
        phase_ctx.slack_level == SlackLevel::Relaxed || meta.slo_class == SloClass::Batch;
    if should_limit {
        allocation.io_max_read_bps = Some(bps);
        allocation.io_max_write_bps = Some(bps);
    }
    if phase_ctx.phase == PhaseKind::IoBound {
        allocation.io_latency_target_us = Some(match phase_ctx.slack_level {
            SlackLevel::Critical => 1_000,
            SlackLevel::Tight => 2_500,
            SlackLevel::Normal => 5_000,
            SlackLevel::Relaxed => 10_000,
        });
    }
}

fn apply_network_profile(
    allocation: &mut ResourceAllocation,
    meta: &InvocationMeta,
    profile: &ResourceProfile,
    phase_ctx: &PhaseSlackContext,
) {
    let Some(bps) = profile.network_bandwidth_bytes_per_sec.filter(|v| *v > 0) else {
        return;
    };
    if phase_ctx.slack_level == SlackLevel::Relaxed || meta.slo_class == SloClass::Batch {
        allocation.network_bandwidth_bytes_per_sec = Some(bps);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(slo_class: SloClass) -> InvocationMeta {
        InvocationMeta {
            id: 1,
            tgid: 100,
            deadline_ns: 0,
            estimated_duration_ns: 0,
            slo_class,
            is_cold_start: false,
            profile_id: None,
            created_at_ns: 0,
        }
    }

    fn ctx(phase: PhaseKind, slack_level: SlackLevel) -> PhaseSlackContext {
        PhaseSlackContext {
            phase,
            slack_level,
            ..PhaseSlackContext::default()
        }
    }

    #[test]
    fn critical_io_gets_high_weight_and_priority() {
        let allocation = compute_allocation(
            &meta(SloClass::LatencyCritical),
            None,
            &ctx(PhaseKind::IoBound, SlackLevel::Critical),
        );
        assert_eq!(allocation.io_weight, Some(1000));
        assert_eq!(allocation.network_priority, Some(1));
    }

    #[test]
    fn batch_relaxed_profile_gets_caps() {
        let allocation = compute_allocation(
            &meta(SloClass::Batch),
            Some(&ResourceProfile {
                io_bandwidth_bytes_per_sec: Some(10_000_000),
                network_bandwidth_bytes_per_sec: Some(20_000_000),
                ..ResourceProfile::default()
            }),
            &ctx(PhaseKind::Idle, SlackLevel::Relaxed),
        );
        assert!(allocation.io_weight.unwrap() <= 100);
        assert_eq!(allocation.io_max_read_bps, Some(10_000_000));
        assert_eq!(allocation.network_bandwidth_bytes_per_sec, Some(20_000_000));
        assert_eq!(allocation.network_priority, Some(7));
    }

    #[test]
    fn memory_profile_sets_safe_high_and_min() {
        let allocation = compute_allocation(
            &meta(SloClass::LatencyCritical),
            Some(&ResourceProfile {
                memory_bytes: Some(256 * MIB),
                working_set_bytes: Some(128 * MIB),
                ..ResourceProfile::default()
            }),
            &ctx(PhaseKind::MemoryBound, SlackLevel::Tight),
        );
        assert_eq!(allocation.memory_min_bytes, Some(128 * MIB));
        assert!(allocation.memory_high_bytes.unwrap() > 256 * MIB);
    }

    #[test]
    fn completed_warm_state_gets_value_based_memory_min() {
        let mut state = InvocationState::with_profile(
            meta(SloClass::Standard),
            Some(ResourceProfile {
                memory_bytes: Some(512 * MIB),
                working_set_bytes: Some(256 * MIB),
                cold_load_penalty_ns: Some(2_000_000_000),
                ..ResourceProfile::default()
            }),
        );
        state.completed_at_ns = Some(1_000_000_000);
        state.last_completed_ns = Some(1_000_000_000);
        state.cold_load_penalty_ns = Some(2_000_000_000);
        state.invocation_count = 3;

        let allocation = compute_allocation_for_state(
            &state,
            &ctx(PhaseKind::Idle, SlackLevel::Normal),
            2_000_000_000,
            true,
        );
        assert_eq!(allocation.memory_min_bytes, Some(256 * MIB));
        assert!(allocation.memory_high_bytes.unwrap() > 512 * MIB);
        assert_eq!(allocation.io_weight, None);
        assert_eq!(allocation.network_priority, None);
    }

    #[test]
    fn completed_low_value_warm_state_withdraws_memory_min() {
        let mut state = InvocationState::with_profile(
            meta(SloClass::Standard),
            Some(ResourceProfile {
                memory_bytes: Some(512 * MIB),
                working_set_bytes: Some(256 * MIB),
                cold_load_penalty_ns: Some(100_000_000),
                ..ResourceProfile::default()
            }),
        );
        state.completed_at_ns = Some(1_000_000_000);
        state.cold_load_penalty_ns = Some(100_000_000);

        let allocation = compute_allocation_for_state(
            &state,
            &ctx(PhaseKind::Idle, SlackLevel::Normal),
            61_000_000_000,
            true,
        );
        assert_eq!(allocation.memory_min_bytes, None);
    }
}
