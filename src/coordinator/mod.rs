// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

mod phase_tracker;
mod policy;
mod slack;

use std::collections::HashMap;
use std::time::Duration;

use crate::cgroup::{CgroupReader, CgroupResolver};
use crate::registry::{InvocationRegistry, InvocationState, PhaseKind, PhaseSlackContext};

use self::policy::compute_allocation;
use self::slack::compute_phase_slack_context;
pub use phase_tracker::{PhaseTracker, PredictivePhaseTracker};

#[derive(Debug, Clone)]
pub struct PhaseUpdate {
    pub tgid: u32,
    pub invocation_id: u64,
    pub phase_ctx: PhaseSlackContext,
    pub cgroup_path: Option<std::path::PathBuf>,
    pub cgroup_id: Option<u64>,
}

pub struct PhaseCoordinator {
    resolver: CgroupResolver,
    reader: CgroupReader,
    tracker: PhaseTracker,
    sample_interval_ns: u64,
    next_sample_at_ns: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CoordinationTickStats {
    pub phase_samples: u64,
    pub phase_ctx_updates: u64,
    pub allocation_updates: u64,
    pub cgroup_updates: u64,
}

pub struct CoordinationEngine {
    phase: PhaseCoordinator,
    predictor: PredictivePhaseTracker,
    phase_prediction_enabled: bool,
}

impl CoordinationEngine {
    pub fn new(sample_interval: Duration) -> Self {
        Self::with_phase_prediction(sample_interval, true)
    }

    pub fn with_phase_prediction(
        sample_interval: Duration,
        phase_prediction_enabled: bool,
    ) -> Self {
        Self {
            phase: PhaseCoordinator::new(sample_interval),
            predictor: PredictivePhaseTracker::new(),
            phase_prediction_enabled,
        }
    }

    pub fn should_tick(&self, now_ns: u64) -> bool {
        self.phase.should_sample(now_ns)
    }

    pub fn tick(
        &mut self,
        registry: &mut InvocationRegistry,
        now_ns: u64,
    ) -> CoordinationTickStats {
        if !self.should_tick(now_ns) {
            return CoordinationTickStats::default();
        }

        let states = registry.active_state_snapshot();
        let active_states: Vec<_> = states
            .iter()
            .filter(|state| state.completed_at_ns.is_none())
            .cloned()
            .collect();
        let phase_updates = self.phase.sample(&active_states, now_ns);
        let mut stats = CoordinationTickStats {
            phase_samples: phase_updates.len() as u64,
            ..CoordinationTickStats::default()
        };
        let mut sampled_phase_by_invocation = HashMap::with_capacity(phase_updates.len());

        for update in phase_updates {
            if let (Some(path), Some(cgroup_id)) = (update.cgroup_path.clone(), update.cgroup_id) {
                registry.update_cgroup(update.tgid, update.invocation_id, path, cgroup_id);
                stats.cgroup_updates = stats.cgroup_updates.saturating_add(1);
            }
            sampled_phase_by_invocation.insert((update.tgid, update.invocation_id), update);
        }

        for state in states {
            if state.completed_at_ns.is_some() {
                continue;
            }
            let sampled = sampled_phase_by_invocation.get(&(state.meta.tgid, state.meta.id));
            let observed_phase = sampled
                .map(|update| update.phase_ctx.phase)
                .unwrap_or(state.phase_ctx.phase);
            let predicted_phase = self
                .phase_prediction_enabled
                .then(|| self.predictor.predict(&state, now_ns))
                .flatten();
            let phase = predicted_phase.unwrap_or(observed_phase);
            let phase_ctx = compute_phase_slack_context(
                &state.meta,
                phase,
                now_ns,
                &state.phase_ctx,
                sampled.is_some() || predicted_phase.is_some(),
            );
            let allocation = compute_allocation(&state.meta, state.profile.as_ref(), &phase_ctx);

            if phase_ctx != state.phase_ctx {
                registry.update_phase_ctx(state.meta.tgid, state.meta.id, phase_ctx);
                stats.phase_ctx_updates = stats.phase_ctx_updates.saturating_add(1);
            }
            if allocation != state.allocation {
                registry.update_allocation(state.meta.tgid, state.meta.id, allocation);
                stats.allocation_updates = stats.allocation_updates.saturating_add(1);
            }
        }

        stats
    }

    pub fn remove_tgid(&mut self, tgid: u32) {
        self.phase.remove_tgid(tgid);
    }
}

impl PhaseCoordinator {
    pub fn new(sample_interval: Duration) -> Self {
        Self {
            resolver: CgroupResolver::new(),
            reader: CgroupReader::new(),
            tracker: PhaseTracker::new(),
            sample_interval_ns: sample_interval.as_nanos() as u64,
            next_sample_at_ns: 0,
        }
    }

    pub fn should_sample(&self, now_ns: u64) -> bool {
        now_ns >= self.next_sample_at_ns
    }

    pub fn sample(&mut self, states: &[InvocationState], now_ns: u64) -> Vec<PhaseUpdate> {
        if !self.should_sample(now_ns) {
            return Vec::new();
        }
        self.next_sample_at_ns = now_ns.saturating_add(self.sample_interval_ns.max(1));

        let mut updates = Vec::new();
        for state in states {
            let tgid = state.meta.tgid;
            let (cgroup_path, cgroup_id) = match state.cgroup_path.as_ref() {
                Some(path) => (path.clone(), state.cgroup_id),
                None => match self.resolver.resolve(tgid) {
                    Ok((path, cgroup_id)) => (path, cgroup_id),
                    Err(_) => continue,
                },
            };

            let snapshot = match self.reader.read(&cgroup_path, now_ns) {
                Ok(snapshot) => snapshot,
                Err(_) => continue,
            };
            let phase = self.tracker.update(tgid, snapshot, now_ns);
            updates.push(PhaseUpdate {
                tgid,
                invocation_id: state.meta.id,
                phase_ctx: build_phase_ctx(phase, now_ns, &state.phase_ctx),
                cgroup_path: Some(cgroup_path),
                cgroup_id: Some(cgroup_id),
            });
        }
        updates
    }

    pub fn remove_tgid(&mut self, tgid: u32) {
        self.tracker.remove(tgid);
    }
}

fn build_phase_ctx(phase: PhaseKind, now_ns: u64, prev: &PhaseSlackContext) -> PhaseSlackContext {
    let mut ctx = prev.clone();
    ctx.phase = phase;
    ctx.last_phase_update_ns = now_ns;
    ctx.needs_cpu = matches!(
        phase,
        PhaseKind::CpuBound | PhaseKind::Mixed | PhaseKind::Unknown
    );
    ctx
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::MetadataExt;
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::registry::{
        InvocationMeta, InvocationRegistry, InvocationState, PhaseKind, ResourceProfile,
        SlackLevel, SloClass,
    };

    fn state_for(tgid: u32, id: u64, cgroup_path: PathBuf) -> InvocationState {
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

    fn wait_for_cgroup_phase(
        coordinator: &mut PhaseCoordinator,
        state: &InvocationState,
        deadline: Instant,
        acceptable: &[PhaseKind],
    ) -> Option<PhaseKind> {
        let mut last = None;
        while Instant::now() < deadline {
            let now_ns = crate::monotonic_now_ns();
            let updates = coordinator.sample(std::slice::from_ref(state), now_ns);
            if let Some(found) = updates.last().map(|update| update.phase_ctx.phase) {
                last = Some(found);
                if acceptable.contains(&found) {
                    return Some(found);
                }
            }
            thread::sleep(Duration::from_millis(110));
        }
        last
    }

    fn create_test_cgroup(name: &str) -> Option<PathBuf> {
        let path = PathBuf::from(format!("/sys/fs/cgroup/cosmos-policy/{name}"));
        if path.exists() {
            let _ = fs::remove_dir(&path);
        }
        if fs::create_dir(&path).is_err() {
            return None;
        }
        Some(path)
    }

    #[test]
    fn coordination_tick_updates_slack_and_allocation_in_registry() {
        let now = 100_000_000;
        let mut registry = InvocationRegistry::new();
        registry.upsert_with_profile(
            InvocationMeta {
                id: 1,
                tgid: 101,
                deadline_ns: now + 5_000_000,
                estimated_duration_ns: 10_000_000,
                slo_class: SloClass::LatencyCritical,
                is_cold_start: false,
                profile_id: None,
                created_at_ns: 0,
            },
            Some(ResourceProfile {
                memory_bytes: Some(128 * 1024 * 1024),
                working_set_bytes: Some(64 * 1024 * 1024),
                io_bandwidth_bytes_per_sec: Some(1_000_000),
                ..ResourceProfile::default()
            }),
        );

        let mut engine = CoordinationEngine::new(Duration::from_millis(100));
        let stats = engine.tick(&mut registry, now);
        let state = registry.lookup_tgid_state(101).unwrap();

        assert_eq!(stats.phase_ctx_updates, 1);
        assert_eq!(stats.allocation_updates, 1);
        assert_eq!(state.phase_ctx.slack_level, SlackLevel::Critical);
        assert_eq!(state.phase_ctx.phase, PhaseKind::Unknown);
        assert_eq!(state.allocation.memory_min_bytes, Some(64 * 1024 * 1024));
        assert!(state.allocation.memory_high_bytes.unwrap() > 128 * 1024 * 1024);
    }

    #[test]
    fn coordination_tick_prefers_profile_phase_prediction() {
        let now = 50_000_000;
        let mut registry = InvocationRegistry::new();
        registry.upsert_with_profile(
            InvocationMeta {
                id: 1,
                tgid: 101,
                deadline_ns: 200_000_000,
                estimated_duration_ns: 100_000_000,
                slo_class: SloClass::LatencyCritical,
                is_cold_start: false,
                profile_id: Some("pipeline".to_string()),
                created_at_ns: 0,
            },
            Some(ResourceProfile {
                phase_sequence: Some(vec![
                    cosmos_metadata_model::PhaseSequenceEntry {
                        kind: "IoBound".to_string(),
                        duration_pct: 30,
                    },
                    cosmos_metadata_model::PhaseSequenceEntry {
                        kind: "CpuBound".to_string(),
                        duration_pct: 50,
                    },
                    cosmos_metadata_model::PhaseSequenceEntry {
                        kind: "IoBound".to_string(),
                        duration_pct: 20,
                    },
                ]),
                ..ResourceProfile::default()
            }),
        );

        let mut engine =
            CoordinationEngine::with_phase_prediction(Duration::from_millis(100), true);
        let stats = engine.tick(&mut registry, now);
        let state = registry.lookup_tgid_state(101).unwrap();

        assert_eq!(stats.phase_ctx_updates, 1);
        assert_eq!(state.phase_ctx.phase, PhaseKind::CpuBound);
        assert!(state.phase_ctx.cpu_priority_modifier > 0);
    }

    #[test]
    fn coordination_tick_can_disable_profile_phase_prediction() {
        let now = 50_000_000;
        let mut registry = InvocationRegistry::new();
        registry.upsert_with_profile(
            InvocationMeta {
                id: 1,
                tgid: 101,
                deadline_ns: 200_000_000,
                estimated_duration_ns: 100_000_000,
                slo_class: SloClass::LatencyCritical,
                is_cold_start: false,
                profile_id: Some("pipeline".to_string()),
                created_at_ns: 0,
            },
            Some(ResourceProfile {
                phase_sequence: Some(vec![cosmos_metadata_model::PhaseSequenceEntry {
                    kind: "CpuBound".to_string(),
                    duration_pct: 100,
                }]),
                ..ResourceProfile::default()
            }),
        );

        let mut engine =
            CoordinationEngine::with_phase_prediction(Duration::from_millis(100), false);
        engine.tick(&mut registry, now);
        let state = registry.lookup_tgid_state(101).unwrap();

        assert_eq!(state.phase_ctx.phase, PhaseKind::Unknown);
    }

    fn remove_test_cgroup(path: &PathBuf) {
        let _ = fs::remove_dir(path);
    }

    fn assign_pid(cgroup_path: &PathBuf, pid: u32) {
        fs::write(cgroup_path.join("cgroup.procs"), format!("{pid}\n")).unwrap();
    }

    fn spawn_waiting_shell(cmd: &str, gate: &str) -> Child {
        Command::new("bash")
            .arg("-lc")
            .arg(format!(
                "while [ ! -f {gate} ]; do sleep 0.01; done; exec {cmd}"
            ))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    }

    #[test]
    fn phase_tracker_classifies_cpu_phase() {
        let mut tracker = PhaseTracker::new();
        let tgid = 42;
        let first = crate::registry::ResourceSnapshot {
            timestamp_ns: 0,
            ..Default::default()
        };
        let second = crate::registry::ResourceSnapshot {
            timestamp_ns: 100_000_000,
            cpu_usage_usec: 80_000,
            ..Default::default()
        };
        assert_eq!(tracker.update(tgid, first, 0), PhaseKind::Unknown);
        assert_eq!(
            tracker.update(tgid, second, 100_000_000),
            PhaseKind::CpuBound
        );
        assert_eq!(tracker.phase_of(tgid), PhaseKind::CpuBound);
    }

    #[test]
    #[ignore = "requires writable cgroup v2 and root execution"]
    fn live_phase_tracker_detects_cpu_memory_and_io() {
        let cgroup_path =
            create_test_cgroup(&format!("cosmos-phase-test-live-{}", std::process::id()))
                .expect("create cgroup under /sys/fs/cgroup/cosmos-policy");
        let gate = format!("/tmp/cosmos-phase-gate-{}", std::process::id());
        let _ = fs::remove_file(&gate);

        let result = (|| {
            let mut coordinator = PhaseCoordinator::new(Duration::from_millis(100));

            let mut cpu = spawn_waiting_shell(
                "python3 -c 'import time\nend=time.time()+3\nx=0\nwhile time.time()<end: x+=sum(i*i for i in range(20000))'",
                &gate,
            );
            assign_pid(&cgroup_path, cpu.id());
            fs::write(&gate, b"go").unwrap();
            let cpu_state = state_for(cpu.id(), 1, cgroup_path.clone());
            let cpu_phase = wait_for_cgroup_phase(
                &mut coordinator,
                &cpu_state,
                Instant::now() + Duration::from_secs(5),
                &[PhaseKind::CpuBound],
            );
            let _ = cpu.wait();
            assert_eq!(cpu_phase, Some(PhaseKind::CpuBound));

            let _ = fs::remove_file(&gate);
            let mut memory = spawn_waiting_shell(
                "python3 -c 'import time\nchunks=[]\nend=time.time()+3\nwhile time.time()<end:\n b=bytearray(16*1024*1024)\n for i in range(0, len(b), 4096): b[i]=1\n chunks.append(b)\n time.sleep(0.05)\ntime.sleep(1)'",
                &gate,
            );
            assign_pid(&cgroup_path, memory.id());
            fs::write(&gate, b"go").unwrap();
            let memory_state = state_for(memory.id(), 2, cgroup_path.clone());
            let memory_phase = wait_for_cgroup_phase(
                &mut coordinator,
                &memory_state,
                Instant::now() + Duration::from_secs(5),
                &[PhaseKind::MemoryBound],
            );
            let _ = memory.wait();
            assert_eq!(memory_phase, Some(PhaseKind::MemoryBound));

            let _ = fs::remove_file(&gate);
            let tmpfile = format!(
                "{}/target/cosmos-phase-io-{}",
                env!("CARGO_MANIFEST_DIR"),
                std::process::id()
            );
            let mut io = spawn_waiting_shell(
                &format!(
                    "python3 -c 'import os,time\nfd=os.open(\"{tmpfile}\", os.O_CREAT|os.O_TRUNC|os.O_WRONLY|os.O_SYNC, 0o644)\nbuf=b\"x\"*(1024*1024)\nend=time.time()+4\nwhile time.time()<end: os.write(fd, buf)\nos.close(fd)'"
                ),
                &gate,
            );
            assign_pid(&cgroup_path, io.id());
            fs::write(&gate, b"go").unwrap();
            let io_state = state_for(io.id(), 3, cgroup_path.clone());
            let io_phase = wait_for_cgroup_phase(
                &mut coordinator,
                &io_state,
                Instant::now() + Duration::from_secs(5),
                &[PhaseKind::IoBound, PhaseKind::Mixed],
            );
            let _ = io.wait();
            let _ = fs::remove_file(&tmpfile);
            assert!(matches!(
                io_phase,
                Some(PhaseKind::IoBound) | Some(PhaseKind::Mixed)
            ));
        })();

        let _ = fs::remove_file(&gate);
        remove_test_cgroup(&cgroup_path);
        result
    }
}
