// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

//! TCP metadata ingestion endpoint for the COSMOS scheduler.
//!
//! Accepts JSON metadata events from the cosmos-event-bridge, writes
//! them to the InvocationRegistry, and sets the 1-bit has_invocation
//! BPF hint only when metadata changes scheduling behavior.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::ffi::CString;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

use crate::registry::{InvocationMeta, ResourceProfile, SloClass};
use crate::RegistryHandle;

const HAS_INVOCATION_MAP_PATH: &str = "/sys/fs/bpf/cosmos/has_invocation";
const BPF_OBJ_GET: i32 = 7;
const BPF_MAP_UPDATE_ELEM: i32 = 2;
const BPF_MAP_DELETE_ELEM: i32 = 3;
const BPF_ATTR_SZ: usize = 64;

static HAS_INVOCATION_FD: OnceLock<i32> = OnceLock::new();

#[derive(Debug, Deserialize)]
struct MetadataWrite {
    tgid: u32,
    deadline_ns: u64,
    #[serde(default)]
    estimated_duration_ns: u64,
    slo_class: u32,
    is_cold_start: u32,
    invocation_id: u64,
    #[serde(default)]
    profile_hints: Option<ProfileHintsWire>,
}

#[derive(Debug, Deserialize)]
struct MetadataDelete {
    tgid: u32,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum MetadataCommand {
    Write(MetadataWrite),
    Delete(MetadataDelete),
}

#[derive(Debug, Deserialize)]
struct ProfileHintsWire {
    #[serde(default)]
    cpu_intensity: Option<f64>,
    #[serde(default)]
    memory_bytes: Option<u64>,
    #[serde(default)]
    working_set_bytes: Option<u64>,
    #[serde(default)]
    io_weight: Option<u64>,
    #[serde(default)]
    io_bandwidth_bytes_per_sec: Option<u64>,
    #[serde(default)]
    network_bandwidth_bytes_per_sec: Option<u64>,
}

impl From<ProfileHintsWire> for ResourceProfile {
    fn from(value: ProfileHintsWire) -> Self {
        Self {
            cpu_intensity: value.cpu_intensity,
            memory_bytes: value.memory_bytes,
            working_set_bytes: value.working_set_bytes,
            io_weight: value.io_weight,
            io_bandwidth_bytes_per_sec: value.io_bandwidth_bytes_per_sec,
            network_bandwidth_bytes_per_sec: value.network_bandwidth_bytes_per_sec,
        }
    }
}

unsafe fn sys_bpf(cmd: i32, attr: *const u8, size: u32) -> i64 {
    libc::syscall(libc::SYS_bpf, cmd, attr, size)
}

fn open_pinned_map(path: &str) -> Result<i32> {
    let cpath = CString::new(path)?;
    let mut attr = [0u8; BPF_ATTR_SZ];

    unsafe {
        let ptr = attr.as_mut_ptr();
        (ptr as *mut u64).write_unaligned(cpath.as_ptr() as u64);
        (ptr.add(8) as *mut u32).write_unaligned(0);
        (ptr.add(12) as *mut u32).write_unaligned(0);
    }

    let fd = unsafe { sys_bpf(BPF_OBJ_GET, attr.as_ptr(), BPF_ATTR_SZ as u32) };
    if fd < 0 {
        let err = std::io::Error::last_os_error();
        anyhow::bail!("failed to open pinned BPF map {}: {}", path, err);
    }
    Ok(fd as i32)
}

fn open_pinned_map_with_retry(path: &str, timeout: Duration) -> Result<i32> {
    let deadline = Instant::now() + timeout;
    loop {
        match open_pinned_map(path) {
            Ok(fd) => return Ok(fd),
            Err(err) if Instant::now() < deadline => {
                let _ = err;
                thread::sleep(Duration::from_millis(25));
            }
            Err(err) => return Err(err),
        }
    }
}

fn write_has_invocation(map_fd: i32, tgid: u32, val: u8) {
    let key: u32 = tgid;
    let value: u8 = val;
    let mut attr = [0u8; BPF_ATTR_SZ];

    unsafe {
        let ptr = attr.as_mut_ptr();
        (ptr as *mut u32).write_unaligned(map_fd as u32);
        (ptr.add(8) as *mut u64).write_unaligned(&key as *const u32 as u64);
        (ptr.add(16) as *mut u64).write_unaligned(&value as *const u8 as u64);
        (ptr.add(24) as *mut u64).write_unaligned(0);
    }

    let ret = unsafe { sys_bpf(BPF_MAP_UPDATE_ELEM, attr.as_ptr(), BPF_ATTR_SZ as u32) };
    if ret < 0 {
        let err = std::io::Error::last_os_error();
        eprintln!(
            "BPF_MAP_UPDATE_ELEM failed for has_invocation tgid={}: {}",
            tgid, err
        );
    }
}

fn delete_has_invocation(map_fd: i32, tgid: u32) {
    let key: u32 = tgid;
    let mut attr = [0u8; BPF_ATTR_SZ];

    unsafe {
        let ptr = attr.as_mut_ptr();
        (ptr as *mut u32).write_unaligned(map_fd as u32);
        (ptr.add(8) as *mut u64).write_unaligned(&key as *const u32 as u64);
    }

    let ret = unsafe { sys_bpf(BPF_MAP_DELETE_ELEM, attr.as_ptr(), BPF_ATTR_SZ as u32) };
    if ret < 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::ENOENT) {
            eprintln!(
                "BPF_MAP_DELETE_ELEM failed for has_invocation tgid={}: {}",
                tgid, err
            );
        }
    }
}

fn should_force_userspace(slo_class: SloClass) -> bool {
    matches!(slo_class, SloClass::LatencyCritical | SloClass::Batch)
}

/// Delete the 1-bit has_invocation BPF hint for a tgid.
/// Can be called from any thread once the map is pinned.
pub fn delete_invocation_hint(tgid: u32) {
    if let Some(&fd) = HAS_INVOCATION_FD.get() {
        delete_has_invocation(fd, tgid);
    }
}

/// Spawn a metadata ingestion thread that listens on a TCP port.
pub fn spawn_metadata_listener(registry: RegistryHandle, port: u16) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        if let Err(e) = run_metadata_listener(registry, port) {
            eprintln!("COSMOS metadata listener error: {:#}", e);
        }
    })
}

fn run_metadata_listener(registry: RegistryHandle, port: u16) -> Result<()> {
    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr)
        .with_context(|| format!("failed to bind metadata listener to {}", addr))?;

    eprintln!("COSMOS metadata listener on {}", addr);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if HAS_INVOCATION_FD.get().is_none() {
                    let fd =
                        open_pinned_map_with_retry(HAS_INVOCATION_MAP_PATH, Duration::from_secs(5))
                            .with_context(|| {
                                format!("failed to open {}", HAS_INVOCATION_MAP_PATH)
                            })?;
                    let _ = HAS_INVOCATION_FD.set(fd);
                }
                let reg = registry.clone();
                thread::spawn(move || {
                    handle_metadata_connection(stream, reg);
                });
            }
            Err(e) => {
                eprintln!("metadata listener accept error: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_metadata_connection(stream: TcpStream, registry: RegistryHandle) {
    let mut stream = stream;

    loop {
        let mut line = String::new();
        {
            let mut reader = BufReader::new(&stream);
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {}
                Err(_) => break,
            };
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        match serde_json::from_str::<MetadataCommand>(trimmed) {
            Ok(MetadataCommand::Write(cmd)) => {
                let slo_class = match cmd.slo_class {
                    0 => SloClass::LatencyCritical,
                    1 => SloClass::Standard,
                    2 => SloClass::Batch,
                    _ => SloClass::None,
                };

                let now = crate::monotonic_now_ns();
                {
                    let mut reg = registry.write().unwrap();
                    let meta = InvocationMeta {
                        id: cmd.invocation_id,
                        tgid: cmd.tgid,
                        deadline_ns: cmd.deadline_ns,
                        estimated_duration_ns: cmd.estimated_duration_ns,
                        slo_class,
                        is_cold_start: cmd.is_cold_start != 0,
                        created_at_ns: now,
                    };
                    reg.upsert_with_profile(meta, cmd.profile_hints.map(Into::into));
                }

                if let Some(&fd) = HAS_INVOCATION_FD.get() {
                    if should_force_userspace(slo_class) {
                        write_has_invocation(fd, cmd.tgid, 1);
                    } else {
                        delete_has_invocation(fd, cmd.tgid);
                    }
                }

                let _ = stream.write_all(b"ok\n");
            }
            Ok(MetadataCommand::Delete(cmd)) => {
                {
                    let mut reg = registry.write().unwrap();
                    reg.mark_completed_by_tgid(cmd.tgid, crate::monotonic_now_ns());
                }

                if let Some(&fd) = HAS_INVOCATION_FD.get() {
                    delete_has_invocation(fd, cmd.tgid);
                }

                let _ = stream.write_all(b"ok\n");
            }
            Err(e) => {
                eprintln!("metadata parse error: {} (line: {})", e, trimmed);
                let _ = stream.write_all(b"error\n");
            }
        }
    }
}
