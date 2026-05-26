// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

//! TCP metadata ingestion endpoint for the COSMOS scheduler.
//!
//! Accepts JSON metadata events from the cosmos-event-bridge, writes
//! them to the InvocationRegistry, and sets the 1-bit has_invocation
//! BPF hint so the kernel knows to route tasks to userspace.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::ffi::CString;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use crate::registry::{InvocationMeta, SloClass};
use crate::RegistryHandle;

const HAS_INVOCATION_MAP_PATH: &str = "/sys/fs/bpf/cosmos/has_invocation";
const BPF_OBJ_GET: i32 = 7;
const BPF_MAP_UPDATE_ELEM: i32 = 2;
const BPF_MAP_DELETE_ELEM: i32 = 3;
const BPF_ATTR_SZ: usize = 64;

#[derive(Debug, Deserialize)]
struct MetadataWrite {
    tgid: u32,
    deadline_ns: u64,
    slo_class: u32,
    is_cold_start: u32,
    invocation_id: u64,
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
        eprintln!("BPF_MAP_UPDATE_ELEM failed for has_invocation tgid={}: {}", tgid, err);
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
            eprintln!("BPF_MAP_DELETE_ELEM failed for has_invocation tgid={}: {}", tgid, err);
        }
    }
}

/// Spawn a metadata ingestion thread that listens on a TCP port.
pub fn spawn_metadata_listener(
    registry: RegistryHandle,
    port: u16,
) -> thread::JoinHandle<()> {
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

    let map_fd = open_pinned_map(HAS_INVOCATION_MAP_PATH)
        .with_context(|| format!("failed to open {}", HAS_INVOCATION_MAP_PATH))?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let reg = registry.clone();
                let fd = map_fd;
                thread::spawn(move || {
                    handle_metadata_connection(stream, reg, fd);
                });
            }
            Err(e) => {
                eprintln!("metadata listener accept error: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_metadata_connection(stream: TcpStream, registry: RegistryHandle, map_fd: i32) {
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
        } // reader dropped here

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
                        slo_class,
                        is_cold_start: cmd.is_cold_start != 0,
                        created_at_ns: now,
                    };
                    reg.upsert(meta);
                }

                write_has_invocation(map_fd, cmd.tgid, 1);

                let _ = stream.write_all(b"ok\n");
            }
            Ok(MetadataCommand::Delete(cmd)) => {
                {
                    let mut reg = registry.write().unwrap();
                    reg.remove_by_tgid(cmd.tgid);
                }

                delete_has_invocation(map_fd, cmd.tgid);

                let _ = stream.write_all(b"ok\n");
            }
            Err(e) => {
                eprintln!("metadata parse error: {} (line: {})", e, trimmed);
                let _ = stream.write_all(b"error\n");
            }
        }
    }
}
