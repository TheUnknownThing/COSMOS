// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

//! TCP metadata ingestion endpoint for the COSMOS scheduler.
//!
//! Accepts JSON metadata events from the cosmos-event-bridge, writes
//! them to the InvocationRegistry, and sets the 1-bit has_invocation
//! BPF hint only when metadata changes scheduling behavior.

use anyhow::{Context, Result};
use cosmos_metadata_model::{MetadataCommand, ProfileCatalogFile, ProfileId};
use std::collections::HashMap;
use std::ffi::CString;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::{Arc, OnceLock};
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

#[derive(Debug, Clone, Default)]
pub struct ProfileCatalog {
    profiles: HashMap<ProfileId, ResourceProfile>,
}

pub type ProfileCatalogHandle = Arc<ProfileCatalog>;

impl ProfileCatalog {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let Some(path) = path else {
            return Ok(Self::default());
        };
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read profile catalog {}", path.display()))?;
        let file: ProfileCatalogFile = serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse profile catalog {}", path.display()))?;
        if file.version != 1 {
            anyhow::bail!(
                "unsupported profile catalog version {} in {}",
                file.version,
                path.display()
            );
        }
        Ok(Self {
            profiles: file.profiles.into_iter().collect(),
        })
    }

    fn resolve(&self, profile_id: &str) -> Option<&ResourceProfile> {
        self.profiles.get(profile_id)
    }
}

pub fn load_profile_catalog(path: Option<&Path>) -> Result<ProfileCatalogHandle> {
    Ok(Arc::new(ProfileCatalog::load(path)?))
}

fn resolve_profile(
    catalog: &ProfileCatalog,
    profile_id: Option<&str>,
    inline_hints: Option<&ResourceProfile>,
) -> Option<ResourceProfile> {
    let baseline = profile_id.and_then(|profile_id| match catalog.resolve(profile_id) {
        Some(profile) => Some(profile.clone()),
        None => {
            eprintln!(
                "COSMOS metadata warning: unknown profile_id={} - using inline hints only",
                profile_id
            );
            None
        }
    });
    ResourceProfile::merged(baseline.as_ref(), inline_hints)
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
pub fn spawn_metadata_listener(
    registry: RegistryHandle,
    port: u16,
    profile_catalog: ProfileCatalogHandle,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        if let Err(e) = run_metadata_listener(registry, port, profile_catalog) {
            eprintln!("COSMOS metadata listener error: {:#}", e);
        }
    })
}

fn run_metadata_listener(
    registry: RegistryHandle,
    port: u16,
    profile_catalog: ProfileCatalogHandle,
) -> Result<()> {
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
                let profile_catalog = profile_catalog.clone();
                thread::spawn(move || {
                    handle_metadata_connection(stream, reg, profile_catalog);
                });
            }
            Err(e) => {
                eprintln!("metadata listener accept error: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_metadata_connection(
    stream: TcpStream,
    registry: RegistryHandle,
    profile_catalog: ProfileCatalogHandle,
) {
    let mut reader = BufReader::new(stream);

    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        };

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
                let profile = resolve_profile(
                    &profile_catalog,
                    cmd.profile_id.as_deref(),
                    cmd.profile_hints.as_ref(),
                );
                {
                    let mut reg = registry.write().unwrap();
                    let meta = InvocationMeta {
                        id: cmd.invocation_id,
                        tgid: cmd.tgid,
                        deadline_ns: cmd.deadline_ns,
                        estimated_duration_ns: cmd.estimated_duration_ns,
                        slo_class,
                        is_cold_start: cmd.is_cold_start != 0,
                        profile_id: cmd.profile_id.clone(),
                        created_at_ns: now,
                    };
                    reg.upsert_with_profile(meta, profile);
                }

                if let Some(&fd) = HAS_INVOCATION_FD.get() {
                    if should_force_userspace(slo_class) {
                        write_has_invocation(fd, cmd.tgid, 1);
                    } else {
                        delete_has_invocation(fd, cmd.tgid);
                    }
                }

                let _ = reader.get_mut().write_all(b"ok\n");
            }
            Ok(MetadataCommand::Delete(cmd)) => {
                {
                    let mut reg = registry.write().unwrap();
                    reg.mark_completed_by_tgid(cmd.tgid, crate::monotonic_now_ns());
                }

                if let Some(&fd) = HAS_INVOCATION_FD.get() {
                    delete_has_invocation(fd, cmd.tgid);
                }

                let _ = reader.get_mut().write_all(b"ok\n");
            }
            Err(e) => {
                eprintln!("metadata parse error: {} (line: {})", e, trimmed);
                let _ = reader.get_mut().write_all(b"error\n");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::InvocationRegistry;
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::path::PathBuf;
    use std::sync::{Arc, RwLock};

    fn profile(memory_bytes: u64, io_weight: u64) -> ResourceProfile {
        ResourceProfile {
            memory_bytes: Some(memory_bytes),
            io_weight: Some(io_weight),
            ..ResourceProfile::default()
        }
    }

    #[test]
    fn resolve_profile_uses_catalog_baseline() {
        let mut profiles = HashMap::new();
        profiles.insert("memory_heavy".to_string(), profile(256, 300));
        let catalog = ProfileCatalog { profiles };

        let resolved = resolve_profile(&catalog, Some("memory_heavy"), None).unwrap();
        assert_eq!(resolved.memory_bytes, Some(256));
        assert_eq!(resolved.io_weight, Some(300));
    }

    #[test]
    fn resolve_profile_applies_inline_overrides() {
        let mut profiles = HashMap::new();
        profiles.insert("memory_heavy".to_string(), profile(256, 300));
        let catalog = ProfileCatalog { profiles };

        let resolved = resolve_profile(
            &catalog,
            Some("memory_heavy"),
            Some(&ResourceProfile {
                io_weight: Some(900),
                network_bandwidth_bytes_per_sec: Some(10_000),
                ..ResourceProfile::default()
            }),
        )
        .unwrap();

        assert_eq!(resolved.memory_bytes, Some(256));
        assert_eq!(resolved.io_weight, Some(900));
        assert_eq!(resolved.network_bandwidth_bytes_per_sec, Some(10_000));
    }

    #[test]
    fn resolve_profile_falls_back_to_inline_for_unknown_id() {
        let catalog = ProfileCatalog::default();
        let resolved = resolve_profile(
            &catalog,
            Some("unknown"),
            Some(&ResourceProfile {
                io_weight: Some(123),
                ..ResourceProfile::default()
            }),
        )
        .unwrap();
        assert_eq!(resolved.io_weight, Some(123));
    }

    #[test]
    fn profile_catalog_load_rejects_unsupported_version() {
        let path = PathBuf::from(format!(
            "/tmp/cosmos-profile-catalog-{}-{}.json",
            std::process::id(),
            crate::monotonic_now_ns()
        ));
        fs::write(&path, r#"{"version":2,"profiles":{}}"#).unwrap();
        let result = ProfileCatalog::load(Some(path.as_path()));
        let _ = fs::remove_file(&path);
        assert!(result.is_err());
    }

    #[test]
    fn persistent_connection_processes_buffered_commands() {
        let registry: RegistryHandle = Arc::new(RwLock::new(InvocationRegistry::new()));
        let profile_catalog: ProfileCatalogHandle = Arc::new(ProfileCatalog::default());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server_registry = registry.clone();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            handle_metadata_connection(stream, server_registry, profile_catalog);
        });

        let mut client = TcpStream::connect(addr).unwrap();
        client
            .write_all(
                b"{\"tgid\":101,\"deadline_ns\":1000,\"estimated_duration_ns\":500,\"slo_class\":1,\"is_cold_start\":0,\"invocation_id\":9001}\n\
                  {\"tgid\":202,\"deadline_ns\":2000,\"estimated_duration_ns\":750,\"slo_class\":2,\"is_cold_start\":1,\"invocation_id\":9002}\n",
            )
            .unwrap();
        client.shutdown(Shutdown::Write).unwrap();

        let mut responses = BufReader::new(client);
        let mut line = String::new();
        responses.read_line(&mut line).unwrap();
        assert_eq!(line, "ok\n");
        line.clear();
        responses.read_line(&mut line).unwrap();
        assert_eq!(line, "ok\n");

        server.join().unwrap();

        let reg = registry.read().unwrap();
        assert_eq!(reg.lookup_tgid(101), Some(9001));
        assert_eq!(reg.lookup_tgid(202), Some(9002));
        assert_eq!(reg.lookup_tgid_meta(202).unwrap().deadline_ns, 2000);
    }
}
