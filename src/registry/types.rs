// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

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
#[derive(Debug, Clone)]
pub struct InvocationMeta {
    pub id: InvocationId,
    pub tgid: u32,
    pub deadline_ns: u64,
    pub estimated_duration_ns: u64,
    pub slo_class: SloClass,
    pub is_cold_start: bool,
    pub created_at_ns: u64,
}
