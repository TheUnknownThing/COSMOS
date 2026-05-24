mod bpf_writer;

use anyhow::{Context, Result};
use bpf_writer::InvocationMeta;
use clap::Parser;
use serde::Deserialize;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::process::Command;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;

#[derive(Parser)]
#[command(name = "cosmos-event-bridge")]
#[command(about = "COSMOS event bridge — consumes OpenWhisk invocation events, writes BPF metadata")]
struct Args {
    #[arg(short, long, default_value = "9731")]
    port: u16,
}

#[derive(Debug, Deserialize)]
struct StartEvent {
    activation_id: String,
    container_id: String,
    timeout_ms: u64,
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
#[serde(tag = "type")]
enum CosmosEvent {
    #[serde(rename = "start")]
    Start(StartEvent),
    #[serde(rename = "end")]
    End(EndEvent),
}

struct ContainerState {
    tgid: u32,
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
        .args([
            "inspect",
            "--format",
            "{{.State.Pid}}",
            container_id,
        ])
        .output()
        .context("failed to run docker inspect")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("docker inspect failed: {}", stderr);
    }

    let pid_str = String::from_utf8_lossy(&output.stdout)
        .trim()
        .to_string();
    let pid: u32 = pid_str
        .parse()
        .with_context(|| format!("invalid PID from docker inspect: '{}'", pid_str))?;
    Ok(pid)
}

fn slo_class_from_timeout(timeout_ms: u64) -> u32 {
    match timeout_ms {
        t if t <= 500 => 0,
        t if t <= 30000 => 1,
        _ => 2,
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
        let deadline_ns = monotonic_now_ns() + ev.timeout_ms * 1_000_000;
        let slo_class = slo_class_from_timeout(ev.timeout_ms);
        let invocation_id = hash_activation_id(&ev.activation_id);

        let meta = InvocationMeta {
            deadline_ns,
            slo_class,
            is_cold_start: if ev.cold_start { 1 } else { 0 },
            invocation_id,
        };

        bpf_writer::write_meta(tgid, &meta)
            .with_context(|| format!("failed to write BPF map for tgid={}", tgid))?;

        match self.containers.entry(ev.container_id.clone()) {
            Entry::Occupied(mut e) => {
                e.get_mut().refcount += 1;
            }
            Entry::Vacant(e) => {
                e.insert(ContainerState {
                    tgid,
                    refcount: 1,
                });
            }
        }

        eprintln!(
            "COSMOS start: activation={} container={} tgid={} action={} kind={} timeout_ms={} slo={} cold={} deadline_ns={}",
            ev.activation_id, ev.container_id, tgid,
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
                    Some(state.tgid)
                } else {
                    eprintln!(
                        "COSMOS end: activation={} container={} tgid={} (refcount={})",
                        ev.activation_id, ev.container_id, state.tgid, state.refcount
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

        if let Some(tgid) = should_delete {
            bpf_writer::delete_meta(tgid)
                .with_context(|| format!("failed to delete BPF map for tgid={}", tgid))?;
            self.containers.remove(&ev.container_id);
            eprintln!(
                "COSMOS end: activation={} container={} tgid={} (deleted — refcount=0)",
                ev.activation_id, ev.container_id, tgid
            );
        }
        Ok(())
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

    let mut state = BridgeState::new();

    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .context("failed to accept connection")?;

        eprintln!("connection from {}", peer);

        let (reader, _writer) = stream.into_split();
        let buf_reader = BufReader::new(reader);
        let mut lines = buf_reader.lines();

        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<CosmosEvent>(&line) {
                        Ok(CosmosEvent::Start(ev)) => {
                            if let Err(e) = state.handle_start(&ev) {
                                eprintln!("error handling start event: {:#}", e);
                            }
                        }
                        Ok(CosmosEvent::End(ev)) => {
                            if let Err(e) = state.handle_end(&ev) {
                                eprintln!("error handling end event: {:#}", e);
                            }
                        }
                        Err(e) => {
                            eprintln!("failed to parse event: {} (line={})", e, line);
                        }
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
}
