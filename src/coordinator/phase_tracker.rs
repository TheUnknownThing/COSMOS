// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use std::collections::HashMap;

use crate::registry::{PhaseKind, ResourceSnapshot};

#[derive(Debug, Clone)]
struct TrackerState {
    last: ResourceSnapshot,
    phase: PhaseKind,
}

#[derive(Debug, Default)]
pub struct PhaseTracker {
    by_tgid: HashMap<u32, TrackerState>,
}

impl PhaseTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, tgid: u32, snapshot: ResourceSnapshot, _now_ns: u64) -> PhaseKind {
        let phase = match self.by_tgid.get(&tgid) {
            None => PhaseKind::Unknown,
            Some(prev) => classify_phase(&prev.last, &snapshot),
        };

        self.by_tgid.insert(
            tgid,
            TrackerState {
                last: snapshot,
                phase,
            },
        );
        phase
    }

    pub fn phase_of(&self, tgid: u32) -> PhaseKind {
        self.by_tgid
            .get(&tgid)
            .map(|state| state.phase)
            .unwrap_or(PhaseKind::Unknown)
    }

    pub fn remove(&mut self, tgid: u32) {
        self.by_tgid.remove(&tgid);
    }
}

fn classify_phase(prev: &ResourceSnapshot, cur: &ResourceSnapshot) -> PhaseKind {
    let dt_ns = cur.timestamp_ns.saturating_sub(prev.timestamp_ns).max(1);
    let cpu_ns = cur
        .cpu_usage_usec
        .saturating_sub(prev.cpu_usage_usec)
        .saturating_mul(1_000);
    let cpu_ratio = cpu_ns as f64 / dt_ns as f64;

    let mem_growth = cur
        .memory_current_bytes
        .saturating_sub(prev.memory_current_bytes);
    let pgmajfaults = cur.memory_pgmajfault.saturating_sub(prev.memory_pgmajfault);
    let mem_pressure = cur
        .memory_pressure
        .some_total
        .saturating_sub(prev.memory_pressure.some_total)
        .saturating_add(
            cur.memory_pressure
                .full_total
                .saturating_sub(prev.memory_pressure.full_total),
        );

    let io_bytes = cur
        .io_read_bytes
        .saturating_sub(prev.io_read_bytes)
        .saturating_add(cur.io_write_bytes.saturating_sub(prev.io_write_bytes))
        .saturating_add(cur.io_discard_bytes.saturating_sub(prev.io_discard_bytes));
    let io_ios = cur
        .io_read_ios
        .saturating_sub(prev.io_read_ios)
        .saturating_add(cur.io_write_ios.saturating_sub(prev.io_write_ios))
        .saturating_add(cur.io_discard_ios.saturating_sub(prev.io_discard_ios));
    let io_pressure = cur
        .io_pressure
        .some_total
        .saturating_sub(prev.io_pressure.some_total)
        .saturating_add(
            cur.io_pressure
                .full_total
                .saturating_sub(prev.io_pressure.full_total),
        );

    let idle = cpu_ratio < 0.05
        && mem_growth < 1 * 1024 * 1024
        && io_bytes < 128 * 1024
        && io_ios < 8
        && mem_pressure == 0
        && io_pressure == 0;
    if idle {
        return PhaseKind::Idle;
    }

    let cpu_active = cpu_ratio >= 0.60;
    let mem_active = mem_growth >= 4 * 1024 * 1024 || pgmajfaults > 0 || mem_pressure >= 5_000;
    let io_active = io_bytes >= 1 * 1024 * 1024 || io_ios >= 64 || io_pressure >= 5_000;

    let active_count = cpu_active as u8 + mem_active as u8 + io_active as u8;
    if mem_active && !io_active && mem_growth >= 32 * 1024 * 1024 {
        return PhaseKind::MemoryBound;
    }
    if io_active && !mem_active && io_bytes >= 8 * 1024 * 1024 {
        return PhaseKind::IoBound;
    }
    if active_count >= 2 {
        if mem_active && !io_active {
            return PhaseKind::MemoryBound;
        }
        if io_active && !mem_active {
            return PhaseKind::IoBound;
        }
        return PhaseKind::Mixed;
    }
    if io_active {
        return PhaseKind::IoBound;
    }
    if mem_active {
        return PhaseKind::MemoryBound;
    }
    if cpu_active {
        return PhaseKind::CpuBound;
    }
    if cpu_ratio >= 0.20 {
        return PhaseKind::CpuBound;
    }
    if io_bytes >= 256 * 1024 || io_ios >= 16 {
        return PhaseKind::IoBound;
    }
    if mem_growth >= 2 * 1024 * 1024 {
        return PhaseKind::MemoryBound;
    }
    PhaseKind::Unknown
}
