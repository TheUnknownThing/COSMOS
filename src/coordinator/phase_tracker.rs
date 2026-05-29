// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use crate::registry::{InvocationState, PhaseKind};

#[derive(Debug, Default)]
pub struct PredictivePhaseTracker;

impl PredictivePhaseTracker {
    pub fn new() -> Self {
        Self
    }

    pub fn predict(&self, state: &InvocationState, now_ns: u64) -> Option<PhaseKind> {
        let profile = state.profile.as_ref()?;
        let sequence = profile.phase_sequence.as_ref()?;
        if sequence.is_empty() || state.meta.estimated_duration_ns == 0 {
            return None;
        }

        let elapsed_ns = now_ns.saturating_sub(state.meta.created_at_ns);
        let duration_ns = state.meta.estimated_duration_ns.max(1);
        let total_pct: u64 = sequence
            .iter()
            .map(|entry| u64::from(entry.duration_pct))
            .sum::<u64>()
            .max(1);
        let position = elapsed_ns
            .saturating_mul(total_pct)
            .saturating_div(duration_ns)
            .min(total_pct);

        let mut accumulated = 0_u64;
        let mut last_valid = None;
        for entry in sequence {
            let phase = parse_phase_kind(&entry.kind)?;
            last_valid = Some(phase);
            accumulated = accumulated.saturating_add(u64::from(entry.duration_pct));
            if position < accumulated {
                return Some(phase);
            }
        }
        last_valid
    }
}

fn parse_phase_kind(kind: &str) -> Option<PhaseKind> {
    match kind {
        "CpuBound" | "cpu" | "cpu_bound" => Some(PhaseKind::CpuBound),
        "MemoryBound" | "memory" | "memory_bound" => Some(PhaseKind::MemoryBound),
        "IoBound" | "io" | "io_bound" | "network" | "network_bound" => Some(PhaseKind::IoBound),
        "Mixed" | "mixed" => Some(PhaseKind::Mixed),
        "Idle" | "idle" => Some(PhaseKind::Idle),
        "Unknown" | "unknown" => Some(PhaseKind::Unknown),
        _ => None,
    }
}
