//! TCP client for writing metadata to the COSMOS scheduler's metadata endpoint.
//!
//! Replaces the old direct-BPF write path. This bridge now connects to the
//! scheduler's metadata TCP endpoint, which writes to the registry and sets the
//! has_invocation BPF hint.

use anyhow::{Context, Result};
use cosmos_metadata_model::{MetadataDelete, MetadataWrite, ProfileHints};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

static METADATA_PORT: OnceLock<u16> = OnceLock::new();

pub fn set_metadata_port(port: u16) {
    let _ = METADATA_PORT.set(port);
}

fn get_addr() -> String {
    let port = METADATA_PORT.get().copied().unwrap_or(9732);
    format!("127.0.0.1:{}", port)
}

pub fn write_meta(
    tgid: u32,
    deadline_ns: u64,
    estimated_duration_ns: u64,
    slo_class: u32,
    is_cold_start: u32,
    invocation_id: u64,
    profile_id: Option<&str>,
    profile_hints: Option<&ProfileHints>,
) -> Result<()> {
    retry_metadata_op(|| {
        write_meta_once(
            tgid,
            deadline_ns,
            estimated_duration_ns,
            slo_class,
            is_cold_start,
            invocation_id,
            profile_id,
            profile_hints,
        )
    })
}

fn write_meta_once(
    tgid: u32,
    deadline_ns: u64,
    estimated_duration_ns: u64,
    slo_class: u32,
    is_cold_start: u32,
    invocation_id: u64,
    profile_id: Option<&str>,
    profile_hints: Option<&ProfileHints>,
) -> Result<()> {
    let addr = get_addr();
    let mut stream = TcpStream::connect(&addr)
        .with_context(|| format!("failed to connect to metadata endpoint at {}", addr))?;

    let json = MetadataWrite {
        tgid,
        deadline_ns,
        estimated_duration_ns,
        slo_class,
        is_cold_start,
        invocation_id,
        profile_id: profile_id.map(str::to_owned),
        profile_hints: profile_hints.cloned(),
    };

    stream
        .write_all(serde_json::to_string(&json)?.as_bytes())
        .context("failed to write metadata to scheduler")?;
    stream.write_all(b"\n").context("failed to write newline")?;

    let mut response = String::new();
    let mut reader = BufReader::new(&stream);
    reader
        .read_line(&mut response)
        .context("failed to read response")?;

    if response.trim() != "ok" {
        anyhow::bail!("unexpected response: {}", response.trim());
    }

    Ok(())
}

pub fn delete_meta(tgid: u32) -> Result<()> {
    retry_metadata_op(|| delete_meta_once(tgid))
}

fn delete_meta_once(tgid: u32) -> Result<()> {
    let addr = get_addr();
    let mut stream = TcpStream::connect(&addr)
        .with_context(|| format!("failed to connect to metadata endpoint at {}", addr))?;

    let json = MetadataDelete { tgid };

    stream
        .write_all(serde_json::to_string(&json)?.as_bytes())
        .context("failed to write delete to scheduler")?;
    stream.write_all(b"\n").context("failed to write newline")?;

    let mut response = String::new();
    let mut reader = BufReader::new(&stream);
    reader
        .read_line(&mut response)
        .context("failed to read response")?;

    if response.trim() != "ok" {
        anyhow::bail!("unexpected response: {}", response.trim());
    }

    Ok(())
}

fn retry_metadata_op<F>(mut op: F) -> Result<()>
where
    F: FnMut() -> Result<()>,
{
    let mut last_err = None;
    for attempt in 0..5 {
        match op() {
            Ok(()) => return Ok(()),
            Err(err) => {
                last_err = Some(err);
                if attempt < 4 {
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }
    Err(last_err.expect("metadata retry loop always records an error"))
}
