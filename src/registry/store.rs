// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use std::path::PathBuf;

use super::types::{
    InvocationId, InvocationMeta, InvocationState, PhaseSlackContext, ResourceAllocation,
    ResourceProfile,
};

const MIN_COMPLETED_INVOCATION_RETENTION_NS: u64 = 5_000_000_000;
const MAX_COMPLETED_INVOCATION_RETENTION_NS: u64 = 60_000_000_000;

/// Thread-safe central store of all active invocation metadata.
/// Shared via Arc<RwLock<...>> between the event bridge (writer) and the
/// scheduler loop (reader).
pub struct InvocationRegistry {
    by_id: HashMap<InvocationId, InvocationState>,
    by_tgid: HashMap<u32, InvocationId>,
}

impl InvocationRegistry {
    pub fn new() -> Self {
        Self {
            by_id: HashMap::new(),
            by_tgid: HashMap::new(),
        }
    }

    /// Insert or update invocation metadata.
    pub fn upsert(&mut self, meta: InvocationMeta) {
        let profile = self
            .by_id
            .get(&meta.id)
            .and_then(|state| state.profile.clone());
        self.upsert_with_profile(meta, profile);
    }

    /// Insert or update invocation metadata with optional resource hints.
    pub fn upsert_with_profile(&mut self, meta: InvocationMeta, profile: Option<ResourceProfile>) {
        if let Some(previous_id) = self.by_tgid.insert(meta.tgid, meta.id) {
            if previous_id != meta.id {
                self.by_id.remove(&previous_id);
            }
        }

        match self.by_id.get_mut(&meta.id) {
            Some(state) => {
                let was_completed = state.completed_at_ns.is_some();
                state.meta = meta;
                state.completed_at_ns = None;
                state.profile = profile;
                state.cold_load_penalty_ns = state
                    .profile
                    .as_ref()
                    .and_then(|profile| profile.cold_load_penalty_ns)
                    .or(state.cold_load_penalty_ns);
                if was_completed {
                    state.invocation_count = state.invocation_count.saturating_add(1);
                }
            }
            None => {
                self.by_id
                    .insert(meta.id, InvocationState::with_profile(meta, profile));
            }
        }
    }

    /// Remove invocation when it completes.
    #[allow(dead_code)]
    pub fn remove(&mut self, id: InvocationId) {
        if let Some(state) = self.by_id.remove(&id) {
            if self.by_tgid.get(&state.meta.tgid) == Some(&id) {
                self.by_tgid.remove(&state.meta.tgid);
            }
        }
    }

    /// Remove an invocation by TGID.
    pub fn remove_by_tgid(&mut self, tgid: u32) {
        if let Some(id) = self.by_tgid.remove(&tgid) {
            self.by_id.remove(&id);
        }
    }

    /// Mark an invocation complete but retain it briefly so already-queued
    /// scheduler records can still resolve metadata after the BPF hint is deleted.
    pub fn mark_completed_by_tgid(&mut self, tgid: u32, now_ns: u64) {
        if let Some(id) = self.by_tgid.get(&tgid).copied() {
            if let Some(state) = self.by_id.get_mut(&id) {
                state.completed_at_ns = Some(now_ns);
                state.last_completed_ns = Some(now_ns);
            }
        }
    }

    /// Resolve a thread-group ID to its invocation.
    /// Assumption: At most one active invocation exists per TGID.
    /// If a task's TGID is not found, the policy falls back to a default
    /// classification (e.g., SloClass::None, no deadline).
    pub fn lookup_tgid(&self, tgid: u32) -> Option<InvocationId> {
        self.by_tgid.get(&tgid).copied()
    }

    /// Get metadata for an invocation.
    pub fn get(&self, id: InvocationId) -> Option<&InvocationMeta> {
        self.by_id.get(&id).map(|state| &state.meta)
    }

    /// Get the full invocation state.
    pub fn get_state(&self, id: InvocationId) -> Option<&InvocationState> {
        self.by_id.get(&id)
    }

    /// Resolve a TGID directly to invocation metadata.
    pub fn lookup_tgid_meta(&self, tgid: u32) -> Option<&InvocationMeta> {
        self.lookup_tgid(tgid).and_then(|id| self.get(id))
    }

    /// Resolve a TGID directly to full invocation state.
    pub fn lookup_tgid_state(&self, tgid: u32) -> Option<&InvocationState> {
        self.lookup_tgid(tgid).and_then(|id| self.get_state(id))
    }

    /// Get a specific invocation by both TGID and invocation ID.
    pub fn get_invocation(&self, tgid: u32, id: InvocationId) -> Option<&InvocationState> {
        (self.by_tgid.get(&tgid) == Some(&id))
            .then(|| self.by_id.get(&id))
            .flatten()
    }

    /// Update phase context for a live invocation.
    pub fn update_phase_ctx(&mut self, tgid: u32, id: InvocationId, phase_ctx: PhaseSlackContext) {
        if let Some(state) = self.get_invocation_mut(tgid, id) {
            state.phase_ctx = phase_ctx;
        }
    }

    /// Update desired resource allocation for a live invocation.
    pub fn update_allocation(
        &mut self,
        tgid: u32,
        id: InvocationId,
        allocation: ResourceAllocation,
    ) {
        if let Some(state) = self.get_invocation_mut(tgid, id) {
            state.allocation = allocation;
        }
    }

    /// Cache resolved cgroup identity for an invocation.
    pub fn update_cgroup(&mut self, tgid: u32, id: InvocationId, path: PathBuf, cgroup_id: u64) {
        if let Some(state) = self.get_invocation_mut(tgid, id) {
            state.cgroup_path = Some(path);
            state.cgroup_id = cgroup_id;
        }
    }

    /// Iterate over all active invocations. Returns a snapshot to
    /// minimise lock hold time.
    #[allow(dead_code)]
    pub fn active_snapshot(&self) -> Vec<InvocationMeta> {
        self.by_id
            .values()
            .map(|state| state.meta.clone())
            .collect()
    }

    /// Iterate over full invocation state. Returns a snapshot to minimise lock hold time.
    pub fn active_state_snapshot(&self) -> Vec<InvocationState> {
        self.by_id.values().cloned().collect()
    }

    /// Prune stale entries older than TTL.
    /// Called periodically (not every scheduling iteration) to avoid
    /// holding the write lock excessively.
    /// Returns the list of tgids that were pruned so callers can clean
    /// associated BPF map entries.
    pub fn prune(&mut self, now_ns: u64, ttl_ns: u64) -> Vec<u32> {
        let cutoff = now_ns.saturating_sub(ttl_ns);
        let mut pruned_tgids = Vec::new();
        self.by_id.retain(|_, state| {
            let completed_expired = state.completed_at_ns.is_some_and(|completed_at_ns| {
                now_ns.saturating_sub(completed_at_ns) >= completed_retention_ns(state)
            });
            if completed_expired || state.meta.created_at_ns < cutoff {
                pruned_tgids.push(state.meta.tgid);
                false
            } else {
                true
            }
        });
        self.by_tgid.retain(|_, id| self.by_id.contains_key(id));
        pruned_tgids
    }

    fn get_invocation_mut(&mut self, tgid: u32, id: InvocationId) -> Option<&mut InvocationState> {
        if self.by_tgid.get(&tgid) == Some(&id) {
            self.by_id.get_mut(&id)
        } else {
            None
        }
    }
}

fn completed_retention_ns(state: &InvocationState) -> u64 {
    state
        .cold_load_penalty_ns
        .map(|penalty| penalty.saturating_mul(15))
        .unwrap_or(MIN_COMPLETED_INVOCATION_RETENTION_NS)
        .clamp(
            MIN_COMPLETED_INVOCATION_RETENTION_NS,
            MAX_COMPLETED_INVOCATION_RETENTION_NS,
        )
}

impl Default for InvocationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// Type alias for the shared handle used throughout the system.
pub type RegistryHandle = Arc<RwLock<InvocationRegistry>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::types::{PhaseKind, ResourceProfile, SloClass};

    fn test_meta(
        id: u64,
        tgid: u32,
        deadline_ns: u64,
        slo: SloClass,
        cold: bool,
    ) -> InvocationMeta {
        InvocationMeta {
            id,
            tgid,
            deadline_ns,
            estimated_duration_ns: 0,
            slo_class: slo,
            is_cold_start: cold,
            profile_id: None,
            created_at_ns: 1000,
        }
    }

    #[test]
    fn upsert_and_get() {
        let mut reg = InvocationRegistry::new();
        let meta = test_meta(1, 100, 5000, SloClass::LatencyCritical, true);
        reg.upsert(meta.clone());
        let found = reg.get(1);
        assert!(found.is_some());
        assert_eq!(found.unwrap().tgid, 100);
    }

    #[test]
    fn lookup_tgid() {
        let mut reg = InvocationRegistry::new();
        reg.upsert(test_meta(1, 100, 5000, SloClass::LatencyCritical, false));
        assert_eq!(reg.lookup_tgid(100), Some(1));
        assert_eq!(reg.lookup_tgid(999), None);
    }

    #[test]
    fn remove_by_id() {
        let mut reg = InvocationRegistry::new();
        reg.upsert(test_meta(1, 100, 5000, SloClass::LatencyCritical, false));
        reg.remove(1);
        assert!(reg.get(1).is_none());
        assert_eq!(reg.lookup_tgid(100), None);
    }

    #[test]
    fn remove_by_tgid() {
        let mut reg = InvocationRegistry::new();
        reg.upsert(test_meta(2, 200, 5000, SloClass::Batch, false));
        reg.remove_by_tgid(200);
        assert!(reg.get(2).is_none());
    }

    #[test]
    fn last_write_wins_for_same_tgid() {
        let mut reg = InvocationRegistry::new();
        reg.upsert(test_meta(1, 100, 5000, SloClass::LatencyCritical, false));
        reg.upsert(test_meta(2, 100, 8000, SloClass::Batch, false));
        // Last written invocation wins for the same tgid
        assert_eq!(reg.lookup_tgid(100), Some(2));
        assert!(reg.get(1).is_none());
    }

    #[test]
    fn prune_removes_stale_entries() {
        let mut reg = InvocationRegistry::new();
        reg.upsert(InvocationMeta {
            id: 1,
            tgid: 100,
            deadline_ns: 5000,
            estimated_duration_ns: 0,
            slo_class: SloClass::LatencyCritical,
            is_cold_start: false,
            profile_id: None,
            created_at_ns: 1000,
        });
        reg.upsert(InvocationMeta {
            id: 2,
            tgid: 200,
            deadline_ns: 6000,
            estimated_duration_ns: 0,
            slo_class: SloClass::Batch,
            is_cold_start: false,
            profile_id: None,
            created_at_ns: 5000,
        });
        // Prune with now=6000, TTL=2000: entries older than 4000 are removed
        reg.prune(6000, 2000);
        assert!(reg.get(1).is_none()); // created at 1000 < 4000
        assert!(reg.get(2).is_some()); // created at 5000 >= 4000
    }

    #[test]
    fn active_snapshot() {
        let mut reg = InvocationRegistry::new();
        reg.upsert(test_meta(1, 100, 5000, SloClass::LatencyCritical, false));
        reg.upsert(test_meta(2, 200, 6000, SloClass::Batch, false));
        let snap = reg.active_snapshot();
        assert_eq!(snap.len(), 2);
    }

    #[test]
    fn upsert_with_profile_preserves_runtime_state() {
        let mut reg = InvocationRegistry::new();
        let meta = test_meta(1, 100, 5000, SloClass::LatencyCritical, true);
        reg.upsert_with_profile(
            meta.clone(),
            Some(ResourceProfile {
                cpu_intensity: Some(0.9),
                ..ResourceProfile::default()
            }),
        );

        reg.update_phase_ctx(
            100,
            1,
            PhaseSlackContext {
                phase: PhaseKind::CpuBound,
                last_phase_update_ns: 123,
                ..PhaseSlackContext::default()
            },
        );

        let mut refreshed = meta.clone();
        refreshed.deadline_ns = 9999;
        reg.upsert(refreshed);

        let state = reg.get_state(1).unwrap();
        assert_eq!(state.meta.deadline_ns, 9999);
        assert_eq!(state.phase_ctx.phase, PhaseKind::CpuBound);
        assert_eq!(state.profile.as_ref().unwrap().cpu_intensity, Some(0.9));
        assert_eq!(state.completed_at_ns, None);
    }

    #[test]
    fn upsert_with_profile_can_clear_effective_profile() {
        let mut reg = InvocationRegistry::new();
        let meta = test_meta(1, 100, 5000, SloClass::LatencyCritical, true);
        reg.upsert_with_profile(
            meta.clone(),
            Some(ResourceProfile {
                cpu_intensity: Some(0.9),
                ..ResourceProfile::default()
            }),
        );

        reg.upsert_with_profile(meta, None);

        assert!(reg.get_state(1).unwrap().profile.is_none());
    }

    #[test]
    fn completed_invocation_is_retained_then_pruned() {
        let mut reg = InvocationRegistry::new();
        reg.upsert(test_meta(1, 100, 5000, SloClass::LatencyCritical, false));

        reg.mark_completed_by_tgid(100, 10_000);
        assert!(reg.lookup_tgid_meta(100).is_some());
        assert_eq!(
            reg.lookup_tgid_state(100).unwrap().completed_at_ns,
            Some(10_000)
        );

        reg.prune(
            10_000 + MIN_COMPLETED_INVOCATION_RETENTION_NS - 1,
            60_000_000_000,
        );
        assert!(reg.lookup_tgid_meta(100).is_some());

        reg.prune(
            10_000 + MIN_COMPLETED_INVOCATION_RETENTION_NS,
            60_000_000_000,
        );
        assert!(reg.lookup_tgid_meta(100).is_none());
    }

    #[test]
    fn warm_profile_extends_completed_retention_and_counts_reuse() {
        let mut reg = InvocationRegistry::new();
        let meta = test_meta(1, 100, 5000, SloClass::LatencyCritical, true);
        reg.upsert_with_profile(
            meta.clone(),
            Some(ResourceProfile {
                cold_load_penalty_ns: Some(2_000_000_000),
                ..ResourceProfile::default()
            }),
        );
        reg.mark_completed_by_tgid(100, 10_000);

        reg.prune(
            10_000 + MIN_COMPLETED_INVOCATION_RETENTION_NS,
            60_000_000_000,
        );
        assert!(reg.lookup_tgid_meta(100).is_some());
        assert_eq!(
            reg.lookup_tgid_state(100).unwrap().last_completed_ns,
            Some(10_000)
        );

        reg.upsert(meta);
        let state = reg.lookup_tgid_state(100).unwrap();
        assert_eq!(state.invocation_count, 2);
        assert_eq!(state.cold_load_penalty_ns, Some(2_000_000_000));
        assert_eq!(state.completed_at_ns, None);
    }
}
