// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use std::collections::HashMap;

use crate::registry::{InvocationId, InvocationState, ResourceProfile};

#[derive(Debug, Default, Clone)]
pub struct ResourceProfileStore {
    by_invocation: HashMap<InvocationId, ResourceProfile>,
}

impl ResourceProfileStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert_invocation(&mut self, id: InvocationId, profile: ResourceProfile) {
        self.by_invocation.insert(id, profile);
    }

    pub fn remove_invocation(&mut self, id: InvocationId) {
        self.by_invocation.remove(&id);
    }

    pub fn profile_for(&self, state: &InvocationState) -> Option<ResourceProfile> {
        state
            .profile
            .clone()
            .or_else(|| self.by_invocation.get(&state.meta.id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{InvocationMeta, SloClass};

    #[test]
    fn invocation_hint_overrides_store_default() {
        let meta = InvocationMeta {
            id: 7,
            tgid: 70,
            deadline_ns: 0,
            estimated_duration_ns: 0,
            slo_class: SloClass::Standard,
            is_cold_start: false,
            created_at_ns: 0,
        };
        let mut store = ResourceProfileStore::new();
        store.upsert_invocation(
            7,
            ResourceProfile {
                cpu_intensity: Some(0.1),
                ..ResourceProfile::default()
            },
        );
        let state = InvocationState::with_profile(
            meta,
            Some(ResourceProfile {
                cpu_intensity: Some(0.9),
                ..ResourceProfile::default()
            }),
        );

        assert_eq!(store.profile_for(&state).unwrap().cpu_intensity, Some(0.9));
    }
}
