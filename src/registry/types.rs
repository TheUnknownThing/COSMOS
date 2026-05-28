// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use std::path::PathBuf;

/// A unique opaque invocation identifier.
pub type InvocationId = u64;

/// SLO classification for an invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum SloClass {
    LatencyCritical = 0,
    Standard = 1,
    Batch = 2,
    None = 0xFF,
}

/// Metadata for a single invocation. Written by the event bridge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvocationMeta {
    pub id: InvocationId,
    pub tgid: u32,
    pub deadline_ns: u64,
    pub estimated_duration_ns: u64,
    pub slo_class: SloClass,
    pub is_cold_start: bool,
    pub created_at_ns: u64,
}

/// Optional static resource hints attached to an invocation at start time.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResourceProfile {
    pub cpu_intensity: Option<f64>,
    pub memory_bytes: Option<u64>,
    pub working_set_bytes: Option<u64>,
    pub io_weight: Option<u64>,
    pub io_bandwidth_bytes_per_sec: Option<u64>,
    pub network_bandwidth_bytes_per_sec: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PhaseKind {
    #[default]
    Unknown,
    CpuBound,
    MemoryBound,
    IoBound,
    Mixed,
    Idle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SlackLevel {
    Critical,
    Tight,
    #[default]
    Normal,
    Relaxed,
}

/// Shared runtime context produced by the co-scheduling pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhaseSlackContext {
    pub phase: PhaseKind,
    pub slack_level: SlackLevel,
    pub cpu_priority_modifier: i64,
    pub needs_cpu: bool,
    pub last_phase_update_ns: u64,
}

impl Default for PhaseSlackContext {
    fn default() -> Self {
        Self {
            phase: PhaseKind::Unknown,
            slack_level: SlackLevel::Normal,
            cpu_priority_modifier: 0,
            needs_cpu: true,
            last_phase_update_ns: 0,
        }
    }
}

/// Desired non-CPU resource controls computed by the coordinator.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceAllocation {
    pub memory_high_bytes: Option<u64>,
    pub memory_min_bytes: Option<u64>,
    pub io_weight: Option<u64>,
    pub io_max_read_bps: Option<u64>,
    pub io_max_write_bps: Option<u64>,
    pub io_latency_target_us: Option<u64>,
    pub network_priority: Option<u32>,
    pub network_bandwidth_bytes_per_sec: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourcePressureTotals {
    pub some_total: u64,
    pub full_total: u64,
}

/// Raw cgroup resource counters captured at a point in time.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceSnapshot {
    pub timestamp_ns: u64,
    pub cpu_usage_usec: u64,
    pub cpu_user_usec: u64,
    pub cpu_system_usec: u64,
    pub memory_current_bytes: u64,
    pub memory_peak_bytes: u64,
    pub memory_anon_bytes: u64,
    pub memory_file_bytes: u64,
    pub memory_pgfault: u64,
    pub memory_pgmajfault: u64,
    pub io_read_bytes: u64,
    pub io_write_bytes: u64,
    pub io_read_ios: u64,
    pub io_write_ios: u64,
    pub io_discard_bytes: u64,
    pub io_discard_ios: u64,
    pub cpu_pressure: ResourcePressureTotals,
    pub memory_pressure: ResourcePressureTotals,
    pub io_pressure: ResourcePressureTotals,
}

/// Canonical runtime state for a live invocation.
#[derive(Debug, Clone, PartialEq)]
pub struct InvocationState {
    pub meta: InvocationMeta,
    pub profile: Option<ResourceProfile>,
    pub phase_ctx: PhaseSlackContext,
    pub allocation: ResourceAllocation,
    pub cgroup_path: Option<PathBuf>,
    pub cgroup_id: u64,
}

impl InvocationState {
    pub fn new(meta: InvocationMeta) -> Self {
        Self {
            meta,
            profile: None,
            phase_ctx: PhaseSlackContext::default(),
            allocation: ResourceAllocation::default(),
            cgroup_path: None,
            cgroup_id: 0,
        }
    }

    pub fn with_profile(meta: InvocationMeta, profile: Option<ResourceProfile>) -> Self {
        let mut state = Self::new(meta);
        state.profile = profile;
        state
    }
}
