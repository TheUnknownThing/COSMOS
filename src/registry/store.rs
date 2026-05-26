// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::types::{InvocationId, InvocationMeta};

/// Thread-safe central store of all active invocation metadata.
/// Shared via Arc<RwLock<...>> between the event bridge (writer) and the
/// scheduler loop (reader).
pub struct InvocationRegistry {
    by_id: HashMap<InvocationId, InvocationMeta>,
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
        self.by_id.insert(meta.id, meta.clone());
        // Assumption: At most one active invocation exists per TGID.
        // If a TGID spawns multiple concurrent invocations (rare), the
        // last-written wins.
        self.by_tgid.insert(meta.tgid, meta.id);
    }

    /// Remove invocation when it completes.
    #[allow(dead_code)]
    pub fn remove(&mut self, id: InvocationId) {
        if let Some(meta) = self.by_id.remove(&id) {
            if self.by_tgid.get(&meta.tgid) == Some(&id) {
                self.by_tgid.remove(&meta.tgid);
            }
        }
    }

    /// Remove an invocation by TGID.
    pub fn remove_by_tgid(&mut self, tgid: u32) {
        if let Some(id) = self.by_tgid.remove(&tgid) {
            self.by_id.remove(&id);
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
        self.by_id.get(&id)
    }

    /// Iterate over all active invocations. Returns a snapshot to
    /// minimise lock hold time.
    #[allow(dead_code)]
    pub fn active_snapshot(&self) -> Vec<InvocationMeta> {
        self.by_id.values().cloned().collect()
    }

    /// Prune stale entries older than TTL.
    /// Called periodically (not every scheduling iteration) to avoid
    /// holding the write lock excessively.
    pub fn prune(&mut self, now_ns: u64, ttl_ns: u64) {
        let cutoff = now_ns.saturating_sub(ttl_ns);
        self.by_id.retain(|_, meta| meta.created_at_ns >= cutoff);
        self.by_tgid.retain(|_, id| self.by_id.contains_key(id));
    }
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
    use crate::registry::types::SloClass;

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
            slo_class: slo,
            is_cold_start: cold,
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
        assert!(reg.get(1).is_some()); // old ID still in by_id
    }

    #[test]
    fn prune_removes_stale_entries() {
        let mut reg = InvocationRegistry::new();
        reg.upsert(InvocationMeta {
            id: 1,
            tgid: 100,
            deadline_ns: 5000,
            slo_class: SloClass::LatencyCritical,
            is_cold_start: false,
            created_at_ns: 1000,
        });
        reg.upsert(InvocationMeta {
            id: 2,
            tgid: 200,
            deadline_ns: 6000,
            slo_class: SloClass::Batch,
            is_cold_start: false,
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
}
