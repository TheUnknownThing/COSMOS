//! TCP client for writing metadata to the COSMOS scheduler's metadata endpoint.
//!
//! Replaces the old direct-BPF write path. This bridge now connects to the
//! scheduler's metadata TCP endpoint, which writes to the registry and sets the
//! has_invocation BPF hint.

use anyhow::{Context, Result};
use cosmos_metadata_model::{MetadataCommand, MetadataDelete, MetadataWrite, ProfileHints};
use std::cell::RefCell;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

static METADATA_PORT: OnceLock<u16> = OnceLock::new();

thread_local! {
    static METADATA_CONN: RefCell<Option<MetadataConnection>> = const { RefCell::new(None) };
}

struct MetadataConnection {
    addr: String,
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl MetadataConnection {
    fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .with_context(|| format!("failed to connect to metadata endpoint at {}", addr))?;
        stream
            .set_nodelay(true)
            .context("failed to enable TCP_NODELAY for metadata endpoint")?;
        let reader = BufReader::new(
            stream
                .try_clone()
                .context("failed to clone metadata endpoint stream")?,
        );
        Ok(Self {
            addr: addr.to_string(),
            stream,
            reader,
        })
    }

    fn send(&mut self, command: &MetadataCommand) -> Result<()> {
        self.stream
            .write_all(serde_json::to_string(command)?.as_bytes())
            .context("failed to write metadata to scheduler")?;
        self.stream
            .write_all(b"\n")
            .context("failed to write metadata newline")?;
        self.stream
            .flush()
            .context("failed to flush metadata command")?;

        let mut response = String::new();
        self.reader
            .read_line(&mut response)
            .context("failed to read metadata response")?;

        if response.trim() != "ok" {
            anyhow::bail!("unexpected metadata response: {}", response.trim());
        }

        Ok(())
    }
}

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
    let command = MetadataCommand::Write(MetadataWrite {
        tgid,
        deadline_ns,
        estimated_duration_ns,
        slo_class,
        is_cold_start,
        invocation_id,
        profile_id: profile_id.map(str::to_owned),
        profile_hints: profile_hints.cloned(),
    });
    retry_metadata_op(|| send_command_once(&command))
}

pub fn delete_meta(tgid: u32) -> Result<()> {
    let command = MetadataCommand::Delete(MetadataDelete { tgid });
    retry_metadata_op(|| send_command_once(&command))
}

fn send_command_once(command: &MetadataCommand) -> Result<()> {
    let addr = get_addr();
    METADATA_CONN.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot
            .as_ref()
            .map(|conn| conn.addr.as_str() != addr.as_str())
            .unwrap_or(true)
        {
            *slot = Some(MetadataConnection::connect(&addr)?);
        }

        let result = slot
            .as_mut()
            .expect("metadata connection initialized")
            .send(command);
        if result.is_err() {
            *slot = None;
        }
        result
    })
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
