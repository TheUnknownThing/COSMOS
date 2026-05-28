// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use crate::registry::{InvocationMeta, PhaseKind, PhaseSlackContext, SlackLevel};

const NS_PER_MS: i64 = 1_000_000;

pub fn compute_slack(meta: &InvocationMeta, now_ns: u64) -> SlackLevel {
    if meta.deadline_ns == 0 {
        return SlackLevel::Normal;
    }
    if now_ns >= meta.deadline_ns {
        return SlackLevel::Critical;
    }

    let remaining = meta.deadline_ns.saturating_sub(now_ns);
    let total_budget = meta
        .deadline_ns
        .checked_sub(meta.created_at_ns)
        .filter(|budget| *budget > 0)
        .unwrap_or(0);

    // estimated_duration_ns is useful only when it is a real execution estimate.
    // The bridge falls back to timeout, which should not make fresh invocations tight.
    let meaningful_estimate = meta.estimated_duration_ns > 0
        && (total_budget == 0
            || meta.estimated_duration_ns.saturating_mul(10) < total_budget.saturating_mul(9));

    if meaningful_estimate {
        let estimate = meta.estimated_duration_ns.max(1);
        if remaining <= estimate.saturating_div(2).max(1) {
            return SlackLevel::Critical;
        }
        if remaining <= estimate {
            return SlackLevel::Tight;
        }
    }

    if total_budget > 0 {
        let remaining_pct = remaining.saturating_mul(100) / total_budget;
        if remaining_pct <= 10 {
            SlackLevel::Critical
        } else if remaining_pct <= 25 {
            SlackLevel::Tight
        } else if remaining_pct >= 60 {
            SlackLevel::Relaxed
        } else {
            SlackLevel::Normal
        }
    } else {
        SlackLevel::Normal
    }
}

pub fn compute_phase_slack_context(
    meta: &InvocationMeta,
    phase: PhaseKind,
    now_ns: u64,
    previous: &PhaseSlackContext,
    phase_sampled: bool,
) -> PhaseSlackContext {
    let slack_level = compute_slack(meta, now_ns);
    PhaseSlackContext {
        phase,
        slack_level,
        cpu_priority_modifier: cpu_priority_modifier(phase, slack_level),
        needs_cpu: needs_cpu(phase),
        last_phase_update_ns: if phase_sampled {
            now_ns
        } else {
            previous.last_phase_update_ns
        },
    }
}

fn needs_cpu(phase: PhaseKind) -> bool {
    matches!(
        phase,
        PhaseKind::CpuBound | PhaseKind::Mixed | PhaseKind::Unknown
    )
}

fn cpu_priority_modifier(phase: PhaseKind, slack: SlackLevel) -> i64 {
    let slack_ms = match slack {
        SlackLevel::Critical => 30,
        SlackLevel::Tight => 12,
        SlackLevel::Normal => 0,
        SlackLevel::Relaxed => -4,
    };
    let phase_ms = match phase {
        PhaseKind::CpuBound => 8,
        PhaseKind::Mixed => 4,
        PhaseKind::Unknown => 0,
        PhaseKind::MemoryBound => -8,
        PhaseKind::IoBound => -10,
        PhaseKind::Idle => -12,
    };
    (slack_ms + phase_ms) * NS_PER_MS
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::SloClass;

    fn meta(created_at_ns: u64, deadline_ns: u64, estimated_duration_ns: u64) -> InvocationMeta {
        InvocationMeta {
            id: 1,
            tgid: 100,
            deadline_ns,
            estimated_duration_ns,
            slo_class: SloClass::LatencyCritical,
            is_cold_start: false,
            profile_id: None,
            created_at_ns,
        }
    }

    #[test]
    fn slack_uses_deadline_fraction_when_estimate_is_timeout() {
        let m = meta(1_000, 101_000, 100_000);
        assert_eq!(compute_slack(&m, 1_000), SlackLevel::Relaxed);
        assert_eq!(compute_slack(&m, 80_000), SlackLevel::Tight);
        assert_eq!(compute_slack(&m, 95_000), SlackLevel::Critical);
    }

    #[test]
    fn slack_uses_meaningful_runtime_estimate() {
        let m = meta(0, 1_000_000, 100_000);
        assert_eq!(compute_slack(&m, 850_000), SlackLevel::Tight);
        assert_eq!(compute_slack(&m, 960_000), SlackLevel::Critical);
    }

    #[test]
    fn context_marks_non_cpu_phases() {
        let m = meta(0, 1_000_000, 0);
        let ctx = compute_phase_slack_context(
            &m,
            PhaseKind::IoBound,
            500_000,
            &PhaseSlackContext::default(),
            true,
        );
        assert!(!ctx.needs_cpu);
        assert!(ctx.cpu_priority_modifier < 0);
        assert_eq!(ctx.last_phase_update_ns, 500_000);
    }
}
