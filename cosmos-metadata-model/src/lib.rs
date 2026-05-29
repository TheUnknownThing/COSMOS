use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub type ProfileId = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseSequenceEntry {
    pub kind: String,
    pub duration_pct: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProfileHints {
    #[serde(default)]
    pub cpu_intensity: Option<f64>,
    #[serde(default)]
    pub memory_bytes: Option<u64>,
    #[serde(default)]
    pub working_set_bytes: Option<u64>,
    #[serde(default)]
    pub io_weight: Option<u64>,
    #[serde(default)]
    pub io_bandwidth_bytes_per_sec: Option<u64>,
    #[serde(default)]
    pub network_bandwidth_bytes_per_sec: Option<u64>,
    #[serde(default)]
    pub phase_sequence: Option<Vec<PhaseSequenceEntry>>,
}

impl ProfileHints {
    pub fn is_empty(&self) -> bool {
        self.cpu_intensity.is_none()
            && self.memory_bytes.is_none()
            && self.working_set_bytes.is_none()
            && self.io_weight.is_none()
            && self.io_bandwidth_bytes_per_sec.is_none()
            && self.network_bandwidth_bytes_per_sec.is_none()
            && self.phase_sequence.is_none()
    }

    pub fn overlay(&mut self, override_hints: &Self) {
        if override_hints.cpu_intensity.is_some() {
            self.cpu_intensity = override_hints.cpu_intensity;
        }
        if override_hints.memory_bytes.is_some() {
            self.memory_bytes = override_hints.memory_bytes;
        }
        if override_hints.working_set_bytes.is_some() {
            self.working_set_bytes = override_hints.working_set_bytes;
        }
        if override_hints.io_weight.is_some() {
            self.io_weight = override_hints.io_weight;
        }
        if override_hints.io_bandwidth_bytes_per_sec.is_some() {
            self.io_bandwidth_bytes_per_sec = override_hints.io_bandwidth_bytes_per_sec;
        }
        if override_hints.network_bandwidth_bytes_per_sec.is_some() {
            self.network_bandwidth_bytes_per_sec = override_hints.network_bandwidth_bytes_per_sec;
        }
        if override_hints.phase_sequence.is_some() {
            self.phase_sequence = override_hints.phase_sequence.clone();
        }
    }

    pub fn merged(baseline: Option<&Self>, override_hints: Option<&Self>) -> Option<Self> {
        let mut profile = baseline
            .cloned()
            .or_else(|| override_hints.filter(|hints| !hints.is_empty()).cloned())?;
        if let Some(override_hints) = override_hints {
            profile.overlay(override_hints);
        }
        (!profile.is_empty()).then_some(profile)
    }
}

fn default_catalog_version() -> u32 {
    1
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProfileCatalogFile {
    #[serde(default = "default_catalog_version")]
    pub version: u32,
    #[serde(default)]
    pub profiles: BTreeMap<ProfileId, ProfileHints>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataDelete {
    pub tgid: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetadataWrite {
    pub tgid: u32,
    pub deadline_ns: u64,
    #[serde(default)]
    pub estimated_duration_ns: u64,
    pub slo_class: u32,
    pub is_cold_start: u32,
    pub invocation_id: u64,
    #[serde(default)]
    pub profile_id: Option<ProfileId>,
    #[serde(default)]
    pub profile_hints: Option<ProfileHints>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetadataCommand {
    Write(MetadataWrite),
    Delete(MetadataDelete),
}
