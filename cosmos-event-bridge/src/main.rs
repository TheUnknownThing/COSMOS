mod metadata_writer;

use anyhow::{Context, Result};
use clap::Parser;
use cosmos_metadata_model::{ProfileHints, ProfileId};
use serde::Deserialize;
use std::collections::hash_map::Entry;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[derive(Parser)]
#[command(name = "cosmos-event-bridge")]
#[command(
    about = "COSMOS event bridge — consumes OpenWhisk invocation events, writes metadata to scheduler"
)]
struct Args {
    #[arg(short, long, default_value = "9731")]
    port: u16,

    #[arg(long, default_value = "9732")]
    metadata_port: u16,
}

#[derive(Debug, Deserialize)]
struct StartEvent {
    activation_id: String,
    container_id: String,
    timeout_ms: u64,
    #[serde(default)]
    estimated_duration_ms: Option<u64>,
    slo_class: Option<u32>,
    action_name: String,
    kind: String,
    cold_start: bool,
    #[serde(default)]
    profile_id: Option<ProfileId>,
    #[serde(default)]
    profile_hints: Option<ProfileHints>,
}

#[derive(Debug, Deserialize)]
struct EndEvent {
    activation_id: String,
    container_id: String,
}

#[derive(Debug, Deserialize)]
struct LocalStartEvent {
    activation_id: String,
    tgid: u32,
    timeout_ms: u64,
    #[serde(default)]
    estimated_duration_ms: Option<u64>,
    slo_class: Option<u32>,
    action_name: String,
    kind: String,
    cold_start: bool,
    #[serde(default)]
    profile_id: Option<ProfileId>,
    #[serde(default)]
    profile_hints: Option<ProfileHints>,
}

#[derive(Debug, Deserialize)]
struct LocalEndEvent {
    activation_id: String,
    tgid: u32,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum CosmosEvent {
    #[serde(rename = "start")]
    Start(StartEvent),
    #[serde(rename = "end")]
    End(EndEvent),
    #[serde(rename = "local_start")]
    LocalStart(LocalStartEvent),
    #[serde(rename = "local_end")]
    LocalEnd(LocalEndEvent),
}

struct ContainerState {
    tgids: Vec<u32>,
    refcount: usize,
}

struct BridgeState {
    containers: HashMap<String, ContainerState>,
}

struct StartResolution {
    container_id: String,
    tgids: Vec<u32>,
    timeout_ms: u64,
    slo_class: u32,
    deadline_ns: u64,
}

fn hash_activation_id(id: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    id.hash(&mut hasher);
    hasher.finish()
}

fn docker_inspect_pid(container_id: &str) -> Result<u32> {
    let output = Command::new("docker")
        .args(["inspect", "--format", "{{.State.Pid}}", container_id])
        .output()
        .context("failed to run docker inspect")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("docker inspect failed: {}", stderr);
    }

    let pid_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let pid: u32 = pid_str
        .parse()
        .with_context(|| format!("invalid PID from docker inspect: '{}'", pid_str))?;
    if pid == 0 {
        anyhow::bail!("container '{}' has no running init PID", container_id);
    }
    Ok(pid)
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == value;
    }
    let mut rest = value;
    let anchored_start = !pattern.starts_with('*');
    let anchored_end = !pattern.ends_with('*');
    for (index, part) in pattern
        .split('*')
        .filter(|part| !part.is_empty())
        .enumerate()
    {
        if index == 0 && anchored_start {
            if !rest.starts_with(part) {
                return false;
            }
            rest = &rest[part.len()..];
            continue;
        }
        let Some(pos) = rest.find(part) else {
            return false;
        };
        rest = &rest[pos + part.len()..];
    }
    !anchored_end || rest.is_empty()
}

fn resolve_container_id(container_id: &str) -> Result<String> {
    if !container_id.contains('*') {
        return Ok(container_id.to_string());
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let output = Command::new("docker")
            .args(["ps", "--format", "{{.Names}}"])
            .output()
            .context("failed to run docker ps")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("docker ps failed: {}", stderr);
        }
        let mut matches: Vec<String> = String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|name| wildcard_match(container_id, name))
            .map(str::to_string)
            .collect();
        if let Some(name) = matches.drain(..).next() {
            return Ok(name);
        }
        if Instant::now() >= deadline {
            anyhow::bail!("no running Docker container matches '{}'", container_id);
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn read_child_pids(pid: u32) -> Vec<u32> {
    let path = format!("/proc/{}/task/{}/children", pid, pid);
    let Ok(children) = fs::read_to_string(path) else {
        return Vec::new();
    };
    children
        .split_whitespace()
        .filter_map(|pid| pid.parse::<u32>().ok())
        .collect()
}

fn collect_descendant_tgids(root: u32) -> Vec<u32> {
    let mut seen = BTreeSet::new();
    let mut stack = vec![root];

    while let Some(pid) = stack.pop() {
        if !seen.insert(pid) {
            continue;
        }
        stack.extend(read_child_pids(pid));
    }

    seen.into_iter().filter(|pid| *pid != 0).collect()
}

fn container_tgids(root: u32) -> Vec<u32> {
    let deadline = std::time::Instant::now() + Duration::from_millis(500);

    loop {
        let tgids = collect_descendant_tgids(root);
        if tgids.len() > 1 {
            return tgids;
        }
        if std::time::Instant::now() >= deadline {
            return tgids;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn slo_class_from_timeout(timeout_ms: u64) -> u32 {
    match timeout_ms {
        t if t <= 1000 => 0,
        t if t <= 30000 => 1,
        _ => 2,
    }
}

fn resolve_slo_class(timeout_ms: u64, explicit: Option<u32>) -> u32 {
    match explicit {
        Some(v @ 0..=2) => v,
        _ => slo_class_from_timeout(timeout_ms),
    }
}

fn compute_estimated_duration_ns(
    timeout_ms: u64,
    estimated_duration_ms: Option<u64>,
    profile_hints: Option<&ProfileHints>,
) -> u64 {
    estimated_duration_ms
        .map(|value| value.saturating_mul(1_000_000))
        .or_else(|| profile_hints.and_then(|hints| hints.estimated_duration_ns))
        .unwrap_or_else(|| timeout_ms.saturating_mul(1_000_000))
}

fn monotonic_now_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts);
    }
    (ts.tv_sec as u64) * 1_000_000_000 + (ts.tv_nsec as u64)
}

fn prepare_start(ev: &StartEvent) -> Result<StartResolution> {
    let container_id = resolve_container_id(&ev.container_id)?;
    let tgid = docker_inspect_pid(&container_id)?;
    let tgids = container_tgids(tgid);
    if tgids.is_empty() {
        anyhow::bail!("container '{}' produced no valid tgids", container_id);
    }
    let deadline_ns = monotonic_now_ns() + ev.timeout_ms * 1_000_000;
    let estimated_duration_ns = compute_estimated_duration_ns(
        ev.timeout_ms,
        ev.estimated_duration_ms,
        ev.profile_hints.as_ref(),
    );
    let slo_class = resolve_slo_class(ev.timeout_ms, ev.slo_class);
    let invocation_id = hash_activation_id(&ev.activation_id);

    for tgid in &tgids {
        metadata_writer::write_meta(
            *tgid,
            deadline_ns,
            estimated_duration_ns,
            slo_class,
            if ev.cold_start { 1 } else { 0 },
            invocation_id,
            ev.profile_id.as_deref(),
            ev.profile_hints.as_ref(),
        )
        .with_context(|| format!("failed to write metadata for tgid={}", tgid))?;
    }

    Ok(StartResolution {
        container_id,
        tgids,
        timeout_ms: ev.timeout_ms,
        slo_class,
        deadline_ns,
    })
}

impl BridgeState {
    fn new() -> Self {
        Self {
            containers: HashMap::new(),
        }
    }

    fn record_start(&mut self, container_key: &str, tgids: &[u32]) {
        match self.containers.entry(container_key.to_string()) {
            Entry::Occupied(mut e) => {
                let state = e.get_mut();
                for tgid in tgids.iter().copied() {
                    if !state.tgids.contains(&tgid) {
                        state.tgids.push(tgid);
                    }
                }
                state.refcount += 1;
            }
            Entry::Vacant(e) => {
                e.insert(ContainerState {
                    tgids: tgids.to_vec(),
                    refcount: 1,
                });
            }
        }
    }

    fn take_end_tgids(&mut self, ev: &EndEvent) -> Option<Vec<u32>> {
        let should_remove = match self.containers.entry(ev.container_id.clone()) {
            Entry::Occupied(mut e) => {
                let state = e.get_mut();
                if state.refcount > 0 {
                    state.refcount -= 1;
                }
                if state.refcount == 0 {
                    true
                } else {
                    eprintln!(
                        "COSMOS end: activation={} container={} tgids={:?} (refcount={})",
                        ev.activation_id, ev.container_id, state.tgids, state.refcount
                    );
                    false
                }
            }
            Entry::Vacant(_) => {
                eprintln!(
                    "COSMOS end: activation={} container={} (unknown container)",
                    ev.activation_id, ev.container_id
                );
                return None;
            }
        };
        if !should_remove {
            return None;
        }
        self.containers
            .remove(&ev.container_id)
            .map(|state| state.tgids)
    }
}

fn handle_local_start(ev: &LocalStartEvent) -> Result<()> {
    let deadline_ns = monotonic_now_ns() + ev.timeout_ms * 1_000_000;
    let estimated_duration_ns = compute_estimated_duration_ns(
        ev.timeout_ms,
        ev.estimated_duration_ms,
        ev.profile_hints.as_ref(),
    );
    let slo_class = resolve_slo_class(ev.timeout_ms, ev.slo_class);
    let invocation_id = hash_activation_id(&ev.activation_id);

    metadata_writer::write_meta(
        ev.tgid,
        deadline_ns,
        estimated_duration_ns,
        slo_class,
        if ev.cold_start { 1 } else { 0 },
        invocation_id,
        ev.profile_id.as_deref(),
        ev.profile_hints.as_ref(),
    )
    .with_context(|| format!("failed to write metadata for local tgid={}", ev.tgid))?;

    eprintln!(
        "COSMOS local_start: activation={} tgid={} action={} kind={} timeout_ms={} slo={} cold={} deadline_ns={}",
        ev.activation_id, ev.tgid,
        ev.action_name, ev.kind, ev.timeout_ms, slo_class, ev.cold_start, deadline_ns
    );
    Ok(())
}

fn handle_local_end(ev: &LocalEndEvent) -> Result<()> {
    metadata_writer::delete_meta(ev.tgid)
        .with_context(|| format!("failed to delete metadata for local tgid={}", ev.tgid))?;
    eprintln!(
        "COSMOS local_end: activation={} tgid={} (deleted)",
        ev.activation_id, ev.tgid
    );
    Ok(())
}

async fn handle_connection(
    stream: TcpStream,
    peer: std::net::SocketAddr,
    state: std::sync::Arc<std::sync::Mutex<BridgeState>>,
) {
    eprintln!("connection from {}", peer);

    let (reader, mut writer) = stream.into_split();
    let buf_reader = BufReader::new(reader);
    let mut lines = buf_reader.lines();

    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if line.trim().is_empty() {
                    continue;
                }
                let mut ok = true;
                match serde_json::from_str::<CosmosEvent>(&line) {
                    Ok(CosmosEvent::Start(ev)) => match prepare_start(&ev) {
                        Ok(resolved) => {
                            {
                                let mut state = state.lock().expect("bridge state mutex poisoned");
                                state.record_start(&ev.container_id, &resolved.tgids);
                            }
                            eprintln!(
                                    "COSMOS start: activation={} container={} tgids={:?} action={} kind={} timeout_ms={} slo={} cold={} deadline_ns={}",
                                    ev.activation_id,
                                    resolved.container_id,
                                    resolved.tgids,
                                    ev.action_name,
                                    ev.kind,
                                    resolved.timeout_ms,
                                    resolved.slo_class,
                                    ev.cold_start,
                                    resolved.deadline_ns
                                );
                        }
                        Err(e) => {
                            eprintln!("error handling start event: {:#}", e);
                            ok = false;
                        }
                    },
                    Ok(CosmosEvent::End(ev)) => {
                        let tgids = {
                            let mut state = state.lock().expect("bridge state mutex poisoned");
                            state.take_end_tgids(&ev)
                        };
                        if let Some(tgids) = tgids {
                            for tgid in &tgids {
                                if let Err(e) =
                                    metadata_writer::delete_meta(*tgid).with_context(|| {
                                        format!("failed to delete metadata for tgid={}", tgid)
                                    })
                                {
                                    eprintln!("error handling end event: {:#}", e);
                                    ok = false;
                                }
                            }
                            eprintln!(
                                "COSMOS end: activation={} container={} tgids={:?} (deleted — refcount=0)",
                                ev.activation_id, ev.container_id, tgids
                            );
                        }
                    }
                    Ok(CosmosEvent::LocalStart(ev)) => {
                        if let Err(e) = handle_local_start(&ev) {
                            eprintln!("error handling local_start event: {:#}", e);
                            ok = false;
                        }
                    }
                    Ok(CosmosEvent::LocalEnd(ev)) => {
                        if let Err(e) = handle_local_end(&ev) {
                            eprintln!("error handling local_end event: {:#}", e);
                            ok = false;
                        }
                    }
                    Err(e) => {
                        eprintln!("failed to parse event: {} (line={})", e, line);
                        ok = false;
                    }
                }
                let response: &[u8] = if ok { b"ok\n" } else { b"error\n" };
                if let Err(e) = writer.write_all(response).await {
                    eprintln!("write error to {}: {}", peer, e);
                    break;
                }
            }
            Ok(None) => {
                eprintln!("connection from {} closed", peer);
                break;
            }
            Err(e) => {
                eprintln!("read error from {}: {}", peer, e);
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let addr = format!("127.0.0.1:{}", args.port);

    // Set the metadata port for the writer
    metadata_writer::set_metadata_port(args.metadata_port);

    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind to {}", addr))?;

    eprintln!("COSMOS event bridge listening on {}", addr);

    let state = std::sync::Arc::new(std::sync::Mutex::new(BridgeState::new()));

    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .context("failed to accept connection")?;

        let state = std::sync::Arc::clone(&state);
        tokio::spawn(async move {
            handle_connection(stream, peer, state).await;
        });
    }
}
