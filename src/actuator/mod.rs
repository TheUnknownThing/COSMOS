// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use std::collections::{HashMap as StdHashMap, HashSet};
use std::path::PathBuf;

use anyhow::{Context, Result};
use aya::{
    include_bytes_aligned,
    maps::HashMap as AyaHashMap,
    programs::{tc, SchedClassifier, TcAttachType},
    Ebpf, EbpfLoader, Pod, VerifierLogLevel,
};
use log::{info, warn};

use crate::cgroup::CgroupWriter;
use crate::registry::{InvocationRegistry, ResourceAllocation};

const DEFAULT_IO_WEIGHT: u64 = 100;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ActuatorStats {
    pub cgroup_applied: u64,
    pub cgroup_resets: u64,
    pub cgroup_errors: u64,
    pub network_applied: u64,
    pub network_removed: u64,
    pub network_errors: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppliedCgroupAllocation {
    path: PathBuf,
    allocation: ResourceAllocation,
}

#[derive(Debug, Default)]
pub struct CgroupActuator {
    writer: CgroupWriter,
    applied: StdHashMap<u64, AppliedCgroupAllocation>,
}

impl CgroupActuator {
    pub fn new() -> Self {
        Self::with_writer(CgroupWriter::new())
    }

    pub fn with_writer(writer: CgroupWriter) -> Self {
        Self {
            writer,
            applied: StdHashMap::new(),
        }
    }

    pub fn apply_from_registry(&mut self, registry: &InvocationRegistry) -> ActuatorStats {
        let mut stats = ActuatorStats::default();
        let states = registry.active_state_snapshot();
        let mut active_cgroups = HashSet::new();

        for state in states
            .iter()
            .filter(|state| state.completed_at_ns.is_none())
        {
            let Some(path) = state.cgroup_path.clone() else {
                continue;
            };
            if !self.writer.manages(&path) {
                continue;
            }
            if state.cgroup_id == 0 {
                continue;
            }
            active_cgroups.insert(state.cgroup_id);

            let previous = self.applied.get(&state.cgroup_id).cloned();
            if previous.as_ref().is_some_and(|applied| {
                applied.path == path && applied.allocation == state.allocation
            }) {
                continue;
            }

            let previous_allocation = previous.as_ref().map(|applied| &applied.allocation);
            match self.apply_delta(&path, previous_allocation, &state.allocation) {
                Ok(()) => {
                    self.applied.insert(
                        state.cgroup_id,
                        AppliedCgroupAllocation {
                            path,
                            allocation: state.allocation.clone(),
                        },
                    );
                    stats.cgroup_applied = stats.cgroup_applied.saturating_add(1);
                }
                Err(err) => {
                    stats.cgroup_errors = stats.cgroup_errors.saturating_add(1);
                    warn!(
                        "failed to apply cgroup allocation for tgid {} cgroup {}: {err:#}",
                        state.meta.tgid, state.cgroup_id
                    );
                }
            }
        }

        let stale: Vec<_> = self
            .applied
            .keys()
            .copied()
            .filter(|cgroup_id| !active_cgroups.contains(cgroup_id))
            .collect();
        for cgroup_id in stale {
            let Some(previous) = self.applied.remove(&cgroup_id) else {
                continue;
            };
            match self.reset(&previous.path, &previous.allocation) {
                Ok(()) => stats.cgroup_resets = stats.cgroup_resets.saturating_add(1),
                Err(err) => {
                    stats.cgroup_errors = stats.cgroup_errors.saturating_add(1);
                    warn!(
                        "failed to reset stale cgroup allocation for cgroup {}: {err:#}",
                        cgroup_id
                    );
                }
            }
        }

        stats
    }

    fn apply_delta(
        &self,
        path: &PathBuf,
        previous: Option<&ResourceAllocation>,
        desired: &ResourceAllocation,
    ) -> Result<()> {
        if previous.map(|prev| prev.memory_high_bytes) != Some(desired.memory_high_bytes) {
            self.writer
                .set_memory_high(path, desired.memory_high_bytes)?;
        }
        if previous.map(|prev| prev.memory_min_bytes) != Some(desired.memory_min_bytes) {
            self.writer.set_memory_min(path, desired.memory_min_bytes)?;
        }
        if previous.map(|prev| prev.io_weight) != Some(desired.io_weight) {
            self.writer
                .set_io_weight(path, desired.io_weight.unwrap_or(DEFAULT_IO_WEIGHT))?;
        }
        if previous.map(|prev| prev.io_latency_target_us) != Some(desired.io_latency_target_us) {
            for device in self.writer.io_devices(path) {
                self.writer
                    .set_io_latency(path, device, desired.io_latency_target_us)?;
            }
        }
        if previous.map(|prev| (prev.io_max_read_bps, prev.io_max_write_bps))
            != Some((desired.io_max_read_bps, desired.io_max_write_bps))
        {
            for device in self.writer.io_devices(path) {
                self.writer.set_io_max(
                    path,
                    device,
                    desired.io_max_read_bps,
                    desired.io_max_write_bps,
                )?;
            }
        }
        Ok(())
    }

    fn reset(&self, path: &PathBuf, previous: &ResourceAllocation) -> Result<()> {
        if previous.memory_high_bytes.is_some() {
            self.writer.set_memory_high(path, None)?;
        }
        if previous.memory_min_bytes.is_some() {
            self.writer.set_memory_min(path, None)?;
        }
        if previous.io_weight.is_some() {
            self.writer.set_io_weight(path, DEFAULT_IO_WEIGHT)?;
        }
        if previous.io_latency_target_us.is_some() {
            for device in self.writer.io_devices(path) {
                self.writer.set_io_latency(path, device, None)?;
            }
        }
        if previous.io_max_read_bps.is_some() || previous.io_max_write_bps.is_some() {
            for device in self.writer.io_devices(path) {
                self.writer.set_io_max(path, device, None, None)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowPolicy {
    pub priority: u32,
    pub rate_bps: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct EbpfFlowPolicy {
    priority: u32,
    _pad: u32,
    rate_bps: u64,
}

unsafe impl Pod for EbpfFlowPolicy {}

impl From<FlowPolicy> for EbpfFlowPolicy {
    fn from(policy: FlowPolicy) -> Self {
        Self {
            priority: policy.priority,
            _pad: 0,
            rate_bps: policy.rate_bps,
        }
    }
}

pub trait NetworkPolicySink {
    fn upsert(&mut self, cgroup_id: u64, policy: FlowPolicy) -> Result<()>;
    fn remove(&mut self, cgroup_id: u64) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct NoopNetworkPolicySink;

impl NetworkPolicySink for NoopNetworkPolicySink {
    fn upsert(&mut self, _cgroup_id: u64, _policy: FlowPolicy) -> Result<()> {
        Ok(())
    }

    fn remove(&mut self, _cgroup_id: u64) -> Result<()> {
        Ok(())
    }
}

pub struct AyaTcNetworkPolicySink {
    bpf: Ebpf,
}

impl AyaTcNetworkPolicySink {
    pub fn load(interface: &str) -> Result<Self> {
        let mut bpf = EbpfLoader::new()
            .verifier_log_level(VerifierLogLevel::DISABLE)
            .load(include_bytes_aligned!(concat!(
                env!("OUT_DIR"),
                "/cosmos_net_tc.bpf.o"
            )))
            .context("load cosmos tc eBPF object")?;
        tc::qdisc_add_clsact(interface)
            .with_context(|| format!("add clsact qdisc to {interface}"))?;
        let program: &mut SchedClassifier = bpf
            .program_mut("cosmos_net_tc")
            .context("cosmos_net_tc program not found")?
            .try_into()
            .context("cosmos_net_tc is not a sched classifier")?;
        program.load().context("load cosmos_net_tc program")?;
        program
            .attach(interface, TcAttachType::Egress)
            .with_context(|| format!("attach cosmos_net_tc to {interface} egress"))?;
        Ok(Self { bpf })
    }

    fn policies(&mut self) -> Result<AyaHashMap<&mut aya::maps::MapData, u64, EbpfFlowPolicy>> {
        let map = self
            .bpf
            .map_mut("flow_policies")
            .context("flow_policies map not found")?;
        AyaHashMap::try_from(map).context("flow_policies has unexpected map type")
    }
}

impl NetworkPolicySink for AyaTcNetworkPolicySink {
    fn upsert(&mut self, cgroup_id: u64, policy: FlowPolicy) -> Result<()> {
        self.policies()?
            .insert(cgroup_id, EbpfFlowPolicy::from(policy), 0)
            .with_context(|| format!("update tc flow policy for cgroup {cgroup_id}"))
    }

    fn remove(&mut self, cgroup_id: u64) -> Result<()> {
        match self.policies()?.remove(&cgroup_id) {
            Ok(()) => Ok(()),
            Err(aya::maps::MapError::KeyNotFound) => Ok(()),
            Err(err) => {
                Err(err).with_context(|| format!("remove tc flow policy for cgroup {cgroup_id}"))
            }
        }
    }
}

pub struct NetworkActuator {
    sink: Box<dyn NetworkPolicySink>,
    applied: StdHashMap<u64, FlowPolicy>,
}

impl NetworkActuator {
    pub fn new() -> Self {
        Self::with_sink(NoopNetworkPolicySink)
    }

    pub fn from_env() -> Self {
        match std::env::var("COSMOS_NET_TC_IFACE") {
            Ok(interface) if !interface.trim().is_empty() => {
                match AyaTcNetworkPolicySink::load(interface.trim()) {
                    Ok(sink) => {
                        info!("loaded Aya tc network actuator on {}", interface.trim());
                        Self::with_sink(sink)
                    }
                    Err(err) => {
                        warn!("failed to load Aya tc network actuator on {interface}: {err:#}");
                        Self::new()
                    }
                }
            }
            _ => Self::new(),
        }
    }

    pub fn with_sink<S: NetworkPolicySink + 'static>(sink: S) -> Self {
        Self {
            sink: Box::new(sink),
            applied: StdHashMap::new(),
        }
    }

    pub fn apply_from_registry(&mut self, registry: &InvocationRegistry) -> ActuatorStats {
        let mut stats = ActuatorStats::default();
        let states = registry.active_state_snapshot();
        let mut active_cgroups = HashSet::new();

        for state in states
            .iter()
            .filter(|state| state.completed_at_ns.is_none())
        {
            if state.cgroup_id == 0 {
                continue;
            }
            let Some(policy) = flow_policy(&state.allocation) else {
                continue;
            };
            active_cgroups.insert(state.cgroup_id);
            if self.applied.get(&state.cgroup_id) == Some(&policy) {
                continue;
            }
            match self.sink.upsert(state.cgroup_id, policy) {
                Ok(()) => {
                    self.applied.insert(state.cgroup_id, policy);
                    stats.network_applied = stats.network_applied.saturating_add(1);
                }
                Err(err) => {
                    stats.network_errors = stats.network_errors.saturating_add(1);
                    warn!(
                        "failed to update network policy for cgroup {}: {err:#}",
                        state.cgroup_id
                    );
                }
            }
        }

        let stale: Vec<_> = self
            .applied
            .keys()
            .copied()
            .filter(|cgroup_id| !active_cgroups.contains(cgroup_id))
            .collect();
        for cgroup_id in stale {
            match self.sink.remove(cgroup_id) {
                Ok(()) => {
                    self.applied.remove(&cgroup_id);
                    stats.network_removed = stats.network_removed.saturating_add(1);
                }
                Err(err) => {
                    stats.network_errors = stats.network_errors.saturating_add(1);
                    warn!(
                        "failed to remove stale network policy for cgroup {}: {err:#}",
                        cgroup_id
                    );
                }
            }
        }

        stats
    }
}

impl Default for NetworkActuator {
    fn default() -> Self {
        Self::new()
    }
}

fn flow_policy(allocation: &ResourceAllocation) -> Option<FlowPolicy> {
    let priority = allocation.network_priority?;
    Some(FlowPolicy {
        priority,
        rate_bps: allocation
            .network_bandwidth_bytes_per_sec
            .unwrap_or(0)
            .saturating_mul(8),
    })
}

pub struct ResourceActuator {
    cgroup: Option<CgroupActuator>,
    network: Option<NetworkActuator>,
}

impl ResourceActuator {
    pub fn new() -> Self {
        Self::with_enabled(true, true)
    }

    pub fn with_enabled(cgroup_enabled: bool, network_enabled: bool) -> Self {
        Self {
            cgroup: cgroup_enabled.then(CgroupActuator::new),
            network: network_enabled.then(NetworkActuator::from_env),
        }
    }

    pub fn apply_from_registry(&mut self, registry: &InvocationRegistry) -> ActuatorStats {
        let cgroup = self
            .cgroup
            .as_mut()
            .map(|actuator| actuator.apply_from_registry(registry))
            .unwrap_or_default();
        let network = self
            .network
            .as_mut()
            .map(|actuator| actuator.apply_from_registry(registry))
            .unwrap_or_default();
        ActuatorStats {
            cgroup_applied: cgroup.cgroup_applied,
            cgroup_resets: cgroup.cgroup_resets,
            cgroup_errors: cgroup.cgroup_errors,
            network_applied: network.network_applied,
            network_removed: network.network_removed,
            network_errors: network.network_errors,
        }
    }
}

impl Default for ResourceActuator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::MetadataExt;
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::cgroup::CgroupWriter;
    use crate::registry::{InvocationMeta, InvocationState, SloClass};

    fn temp_root(name: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("cosmos-actuator-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn test_cgroup(root: &Path, name: &str) -> PathBuf {
        let cgroup = root.join(name);
        fs::create_dir_all(&cgroup).unwrap();
        fs::write(cgroup.join("memory.high"), "max\n").unwrap();
        fs::write(cgroup.join("memory.min"), "0\n").unwrap();
        fs::write(cgroup.join("io.weight"), "100\n").unwrap();
        fs::write(cgroup.join("io.latency"), b"").unwrap();
        fs::write(cgroup.join("io.max"), b"").unwrap();
        fs::write(cgroup.join("io.stat"), "8:0 rbytes=1 wbytes=2\n").unwrap();
        cgroup
    }

    fn state(id: u64, tgid: u32, cgroup_path: PathBuf) -> InvocationState {
        let mut state = InvocationState::new(InvocationMeta {
            id,
            tgid,
            deadline_ns: 0,
            estimated_duration_ns: 0,
            slo_class: SloClass::Standard,
            is_cold_start: false,
            profile_id: None,
            created_at_ns: 0,
        });
        state.cgroup_id = fs::metadata(&cgroup_path).unwrap().ino();
        state.cgroup_path = Some(cgroup_path);
        state
    }

    #[test]
    fn cgroup_actuator_applies_diffs_and_resets_stale_cgroups() {
        let root = temp_root("cgroup");
        let cgroup = test_cgroup(&root, "cg");
        let mut registry = InvocationRegistry::new();
        let mut state = state(1, 100, cgroup.clone());
        state.allocation = ResourceAllocation {
            memory_high_bytes: Some(64 * 1024 * 1024),
            memory_min_bytes: Some(32 * 1024 * 1024),
            io_weight: Some(800),
            io_max_read_bps: Some(10_000),
            io_max_write_bps: Some(20_000),
            io_latency_target_us: Some(2_500),
            ..ResourceAllocation::default()
        };
        registry.upsert(state.meta.clone());
        registry.update_cgroup(
            state.meta.tgid,
            state.meta.id,
            state.cgroup_path.clone().unwrap(),
            state.cgroup_id,
        );
        registry.update_allocation(state.meta.tgid, state.meta.id, state.allocation.clone());

        let mut actuator = CgroupActuator::with_writer(CgroupWriter::with_root(&root));
        let stats = actuator.apply_from_registry(&registry);
        assert_eq!(stats.cgroup_applied, 1);
        assert_eq!(stats.cgroup_errors, 0);
        assert_eq!(
            fs::read_to_string(cgroup.join("memory.high")).unwrap(),
            "67108864\n"
        );
        assert_eq!(
            fs::read_to_string(cgroup.join("memory.min")).unwrap(),
            "33554432\n"
        );
        assert_eq!(
            fs::read_to_string(cgroup.join("io.weight")).unwrap(),
            "800\n"
        );
        assert_eq!(
            fs::read_to_string(cgroup.join("io.max")).unwrap(),
            "8:0 rbps=10000 wbps=20000\n"
        );

        let no_change = actuator.apply_from_registry(&registry);
        assert_eq!(no_change.cgroup_applied, 0);

        registry.mark_completed_by_tgid(100, 1);
        let reset = actuator.apply_from_registry(&registry);
        assert_eq!(reset.cgroup_resets, 1);
        assert_eq!(
            fs::read_to_string(cgroup.join("memory.high")).unwrap(),
            "max\n"
        );
        assert_eq!(
            fs::read_to_string(cgroup.join("memory.min")).unwrap(),
            "0\n"
        );
        assert_eq!(
            fs::read_to_string(cgroup.join("io.weight")).unwrap(),
            "100\n"
        );
        assert_eq!(
            fs::read_to_string(cgroup.join("io.max")).unwrap(),
            "8:0 rbps=max wbps=max\n"
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[derive(Default)]
    struct RecordingSink {
        upserts: Vec<(u64, FlowPolicy)>,
        removes: Vec<u64>,
    }

    impl NetworkPolicySink for RecordingSink {
        fn upsert(&mut self, cgroup_id: u64, policy: FlowPolicy) -> Result<()> {
            self.upserts.push((cgroup_id, policy));
            Ok(())
        }

        fn remove(&mut self, cgroup_id: u64) -> Result<()> {
            self.removes.push(cgroup_id);
            Ok(())
        }
    }

    #[test]
    fn network_actuator_maps_allocations_by_cgroup_id() {
        let root = temp_root("network");
        let cgroup = test_cgroup(&root, "net");
        let mut registry = InvocationRegistry::new();
        let mut state = state(1, 100, cgroup);
        state.allocation = ResourceAllocation {
            network_priority: Some(2),
            network_bandwidth_bytes_per_sec: Some(1_000),
            ..ResourceAllocation::default()
        };
        registry.upsert(state.meta.clone());
        registry.update_cgroup(
            state.meta.tgid,
            state.meta.id,
            state.cgroup_path.clone().unwrap(),
            state.cgroup_id,
        );
        registry.update_allocation(state.meta.tgid, state.meta.id, state.allocation.clone());

        let mut actuator = NetworkActuator::with_sink(RecordingSink::default());
        let stats = actuator.apply_from_registry(&registry);
        assert_eq!(stats.network_applied, 1);
        assert_eq!(
            actuator.applied.get(&state.cgroup_id),
            Some(&FlowPolicy {
                priority: 2,
                rate_bps: 8_000
            })
        );

        let no_change = actuator.apply_from_registry(&registry);
        assert_eq!(no_change.network_applied, 0);

        registry.mark_completed_by_tgid(100, 1);
        let removed = actuator.apply_from_registry(&registry);
        assert_eq!(removed.network_removed, 1);
        assert!(!actuator.applied.contains_key(&state.cgroup_id));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    #[ignore = "requires writable cgroup v2 and root execution"]
    fn live_cgroup_actuator_applies_and_resets_controls() {
        let root = PathBuf::from("/sys/fs/cgroup");
        let cgroup = root
            .join("cosmos-policy")
            .join(format!("cosmos-actuator-live-{}", std::process::id()));
        if cgroup.exists() {
            let _ = fs::remove_dir(&cgroup);
        }
        fs::create_dir(&cgroup).expect("create live test cgroup");

        fn io_weight_matches(raw: &str, expected: u64) -> bool {
            raw.trim() == expected.to_string() || raw.trim() == format!("default {expected}")
        }

        let result = std::panic::catch_unwind(|| {
            let mut registry = InvocationRegistry::new();
            let mut state = state(1, std::process::id(), cgroup.clone());
            state.allocation = ResourceAllocation {
                memory_high_bytes: Some(256 * 1024 * 1024),
                memory_min_bytes: Some(64 * 1024 * 1024),
                io_weight: Some(700),
                ..ResourceAllocation::default()
            };
            registry.upsert(state.meta.clone());
            registry.update_cgroup(
                state.meta.tgid,
                state.meta.id,
                state.cgroup_path.clone().unwrap(),
                state.cgroup_id,
            );
            registry.update_allocation(state.meta.tgid, state.meta.id, state.allocation.clone());

            let mut actuator = CgroupActuator::new();
            let stats = actuator.apply_from_registry(&registry);
            assert_eq!(stats.cgroup_applied, 1);
            assert_eq!(stats.cgroup_errors, 0);
            assert_eq!(
                fs::read_to_string(cgroup.join("memory.high"))
                    .unwrap()
                    .trim(),
                "268435456"
            );
            assert_eq!(
                fs::read_to_string(cgroup.join("memory.min"))
                    .unwrap()
                    .trim(),
                "67108864"
            );
            assert!(io_weight_matches(
                &fs::read_to_string(cgroup.join("io.weight")).unwrap(),
                700
            ));

            registry.mark_completed_by_tgid(std::process::id(), 1);
            let reset = actuator.apply_from_registry(&registry);
            assert_eq!(reset.cgroup_resets, 1);
            assert_eq!(
                fs::read_to_string(cgroup.join("memory.high"))
                    .unwrap()
                    .trim(),
                "max"
            );
            assert_eq!(
                fs::read_to_string(cgroup.join("memory.min"))
                    .unwrap()
                    .trim(),
                "0"
            );
            assert!(io_weight_matches(
                &fs::read_to_string(cgroup.join("io.weight")).unwrap(),
                100
            ));
        });

        let _ = fs::remove_dir(&cgroup);
        if let Err(err) = result {
            std::panic::resume_unwind(err);
        }
    }

    fn run_command(program: &str, args: &[&str]) {
        let output = std::process::Command::new(program)
            .args(args)
            .output()
            .unwrap_or_else(|err| panic!("failed to execute {program}: {err}"));
        assert!(
            output.status.success(),
            "{program} {:?} failed: stdout={} stderr={}",
            args,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    #[ignore = "requires root, tc, and veth creation"]
    fn live_aya_tc_network_actuator_loads_attaches_and_updates_map() {
        let left = format!("cosv{}", std::process::id() % 100_000);
        let right = format!("cosp{}", std::process::id() % 100_000);
        let _ = std::process::Command::new("ip")
            .args(["link", "del", &left])
            .output();

        run_command(
            "ip",
            &["link", "add", &left, "type", "veth", "peer", "name", &right],
        );
        run_command("ip", &["link", "set", &left, "up"]);
        run_command("ip", &["link", "set", &right, "up"]);

        let result = std::panic::catch_unwind(|| {
            let mut sink = AyaTcNetworkPolicySink::load(&left).expect("load Aya tc actuator");
            sink.upsert(
                42,
                FlowPolicy {
                    priority: 3,
                    rate_bps: 1_000_000,
                },
            )
            .expect("insert flow policy");
            sink.remove(42).expect("remove flow policy");

            let output = std::process::Command::new("tc")
                .args(["qdisc", "show", "dev", &left])
                .output()
                .expect("run tc qdisc show");
            assert!(
                output.status.success(),
                "tc qdisc show failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let text = String::from_utf8_lossy(&output.stdout);
            assert!(
                text.contains("clsact"),
                "expected clsact qdisc after Aya attach, got: {text}"
            );
        });

        let _ = std::process::Command::new("ip")
            .args(["link", "del", &left])
            .output();
        if let Err(err) = result {
            std::panic::resume_unwind(err);
        }
    }
}
