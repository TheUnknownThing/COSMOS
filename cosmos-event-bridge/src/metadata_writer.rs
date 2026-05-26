//! TCP client for writing metadata to the COSMOS scheduler's metadata endpoint.
//!
//! Replaces the old bpf_writer.rs that wrote directly to pinned BPF maps
//! via raw bpf() syscalls. Now connects to the scheduler's metadata TCP
//! endpoint, which writes to the Registry and sets the has_invocation BPF hint.

use anyhow::{Context, Result};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::OnceLock;

static METADATA_PORT: OnceLock<u16> = OnceLock::new();

pub fn set_metadata_port(port: u16) {
    let _ = METADATA_PORT.set(port);
}

fn get_addr() -> String {
    let port = METADATA_PORT.get().copied().unwrap_or(9732);
    format!("127.0.0.1:{}", port)
}

pub fn write_meta(tgid: u32, deadline_ns: u64, slo_class: u32, is_cold_start: u32, invocation_id: u64) -> Result<()> {
    let addr = get_addr();
    let mut stream = TcpStream::connect(&addr)
        .with_context(|| format!("failed to connect to metadata endpoint at {}", addr))?;

    let json = serde_json::json!({
        "tgid": tgid,
        "deadline_ns": deadline_ns,
        "slo_class": slo_class,
        "is_cold_start": is_cold_start,
        "invocation_id": invocation_id,
    });

    stream.write_all(json.to_string().as_bytes())
        .context("failed to write metadata to scheduler")?;
    stream.write_all(b"\n")
        .context("failed to write newline")?;

    let mut response = String::new();
    let mut reader = BufReader::new(&stream);
    reader.read_line(&mut response)
        .context("failed to read response")?;

    if response.trim() != "ok" {
        anyhow::bail!("unexpected response: {}", response.trim());
    }

    Ok(())
}

pub fn delete_meta(tgid: u32) -> Result<()> {
    let addr = get_addr();
    let mut stream = TcpStream::connect(&addr)
        .with_context(|| format!("failed to connect to metadata endpoint at {}", addr))?;

    let json = serde_json::json!({
        "tgid": tgid,
    });

    stream.write_all(json.to_string().as_bytes())
        .context("failed to write delete to scheduler")?;
    stream.write_all(b"\n")
        .context("failed to write newline")?;

    let mut response = String::new();
    let mut reader = BufReader::new(&stream);
    reader.read_line(&mut response)
        .context("failed to read response")?;

    if response.trim() != "ok" {
        anyhow::bail!("unexpected response: {}", response.trim());
    }

    Ok(())
}
