mod bpf_writer;

use anyhow::{Context, Result};
use bpf_writer::InvocationMeta;
use clap::Parser;
use serde::Deserialize;
use std::collections::hash_map::Entry;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[derive(Parser)]
#[command(name = "cosmos-event-bridge")]
#[command(
    about = "COSMOS event bridge — consumes OpenWhisk invocation events, writes BPF metadata"
)]
struct Args {
    #[arg(short, long, default_value = "9731")]
    port: u16,
}

#[derive(Debug, Deserialize)]
struct StartEvent {
    activation_id: String,
    container_id: String,
    timeout_ms: u64,
    slo_class: Option<u32>,
    action_name: String,
    kind: String,
    cold_start: bool,
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
    slo_class: Option<u32>,
    action_name: String,
    kind: String,
    cold_start: bool,
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
    Ok(pid)
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

    seen.into_iter().collect()
}

fn container_tgids(root: u32) -> Vec<u32> {
    let mut tgids = Vec::new();

    for _ in 0..5 {
        tgids = collect_descendant_tgids(root);
        if tgids.len() > 1 {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }

    tgids
}

fn slo_class_from_timeout(timeout_ms: u64) -> u32 {
    match timeout_ms {
        t if t <= 100 => 0,
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

impl BridgeState {
    fn new() -> Self {
        Self {
            containers: HashMap::new(),
        }
    }

    fn handle_start(&mut self, ev: &StartEvent) -> Result<()> {
        let tgid = docker_inspect_pid(&ev.container_id)?;
        let tgids = container_tgids(tgid);
        let deadline_ns = monotonic_now_ns() + ev.timeout_ms * 1_000_000;
        let slo_class = resolve_slo_class(ev.timeout_ms, ev.slo_class);
        let invocation_id = hash_activation_id(&ev.activation_id);

        let meta = InvocationMeta {
            deadline_ns,
            slo_class,
            is_cold_start: if ev.cold_start { 1 } else { 0 },
            invocation_id,
        };

        for tgid in &tgids {
            bpf_writer::write_meta(*tgid, &meta)
                .with_context(|| format!("failed to write BPF map for tgid={}", tgid))?;
        }

        match self.containers.entry(ev.container_id.clone()) {
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
                    tgids: tgids.clone(),
                    refcount: 1,
                });
            }
        }

        eprintln!(
            "COSMOS start: activation={} container={} tgids={:?} action={} kind={} timeout_ms={} slo={} cold={} deadline_ns={}",
            ev.activation_id, ev.container_id, tgids,
            ev.action_name, ev.kind, ev.timeout_ms, slo_class, ev.cold_start, deadline_ns
        );
        Ok(())
    }

    fn handle_end(&mut self, ev: &EndEvent) -> Result<()> {
        let should_delete = match self.containers.entry(ev.container_id.clone()) {
            Entry::Occupied(mut e) => {
                let state = e.get_mut();
                if state.refcount > 0 {
                    state.refcount -= 1;
                }
                if state.refcount == 0 {
                    Some(state.tgids.clone())
                } else {
                    eprintln!(
                        "COSMOS end: activation={} container={} tgids={:?} (refcount={})",
                        ev.activation_id, ev.container_id, state.tgids, state.refcount
                    );
                    None
                }
            }
            Entry::Vacant(_) => {
                eprintln!(
                    "COSMOS end: activation={} container={} (unknown container)",
                    ev.activation_id, ev.container_id
                );
                None
            }
        };

        if let Some(tgids) = should_delete {
            for tgid in &tgids {
                bpf_writer::delete_meta(*tgid)
                    .with_context(|| format!("failed to delete BPF map for tgid={}", tgid))?;
            }
            self.containers.remove(&ev.container_id);
            eprintln!(
                "COSMOS end: activation={} container={} tgids={:?} (deleted — refcount=0)",
                ev.activation_id, ev.container_id, tgids
            );
        }
        Ok(())
    }
}

fn handle_local_start(ev: &LocalStartEvent) -> Result<()> {
    let deadline_ns = monotonic_now_ns() + ev.timeout_ms * 1_000_000;
    let slo_class = resolve_slo_class(ev.timeout_ms, ev.slo_class);
    let invocation_id = hash_activation_id(&ev.activation_id);

    let meta = InvocationMeta {
        deadline_ns,
        slo_class,
        is_cold_start: if ev.cold_start { 1 } else { 0 },
        invocation_id,
    };

    bpf_writer::write_meta(ev.tgid, &meta)
        .with_context(|| format!("failed to write BPF map for local tgid={}", ev.tgid))?;

    eprintln!(
        "COSMOS local_start: activation={} tgid={} action={} kind={} timeout_ms={} slo={} cold={} deadline_ns={}",
        ev.activation_id, ev.tgid,
        ev.action_name, ev.kind, ev.timeout_ms, slo_class, ev.cold_start, deadline_ns
    );
    Ok(())
}

fn handle_local_end(ev: &LocalEndEvent) -> Result<()> {
    bpf_writer::delete_meta(ev.tgid)
        .with_context(|| format!("failed to delete BPF map for local tgid={}", ev.tgid))?;
    eprintln!(
        "COSMOS local_end: activation={} tgid={} (deleted)",
        ev.activation_id, ev.tgid
    );
    Ok(())
}

async fn handle_connection(stream: TcpStream, peer: SocketAddr, state: Arc<Mutex<BridgeState>>) {
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
                    Ok(CosmosEvent::Start(ev)) => {
                        let mut state = state.lock().expect("bridge state mutex poisoned");
                        if let Err(e) = state.handle_start(&ev) {
                            eprintln!("error handling start event: {:#}", e);
                            ok = false;
                        }
                    }
                    Ok(CosmosEvent::End(ev)) => {
                        let mut state = state.lock().expect("bridge state mutex poisoned");
                        if let Err(e) = state.handle_end(&ev) {
                            eprintln!("error handling end event: {:#}", e);
                            ok = false;
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

    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind to {}", addr))?;

    eprintln!("COSMOS event bridge listening on {}", addr);

    let state = Arc::new(Mutex::new(BridgeState::new()));

    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .context("failed to accept connection")?;

        let state = Arc::clone(&state);
        tokio::spawn(async move {
            handle_connection(stream, peer, state).await;
        });
    }
}
