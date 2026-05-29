// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};

use crate::registry::InvocationMeta;

#[derive(Debug, Clone)]
pub struct TraceCollectorConfig {
    pub root: PathBuf,
    pub sample_interval: Duration,
}

#[derive(Clone)]
pub struct TraceCollectorHandle {
    tx: Sender<TraceCommand>,
}

struct TraceStart {
    meta: InvocationMeta,
    profile_id: String,
    cgroup_path: PathBuf,
}

enum TraceCommand {
    Start(TraceStart),
    Finish { tgid: u32, timestamp_ns: u64 },
}

#[derive(Debug)]
struct ActiveTrace {
    run_dir: PathBuf,
    cgroup_path: PathBuf,
    activation_id: u64,
    tgid: u32,
}

#[derive(Debug, Serialize)]
struct RuntimeTraceMeta {
    profile_id: String,
    invocation_id: u64,
    tgid: u32,
    created_at_ns: u64,
    deadline_ns: u64,
    estimated_duration_ns: u64,
    warmth: String,
    cgroup_path: String,
}

impl TraceCollectorHandle {
    pub fn spawn(config: TraceCollectorConfig) -> Self {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || run_collector(rx, config));
        Self { tx }
    }

    pub fn start_invocation(
        &self,
        meta: &InvocationMeta,
        profile_id: &str,
        cgroup_path: PathBuf,
    ) -> Result<()> {
        self.tx
            .send(TraceCommand::Start(TraceStart {
                meta: meta.clone(),
                profile_id: profile_id.to_string(),
                cgroup_path,
            }))
            .context("start runtime trace")
    }

    pub fn finish_invocation(&self, tgid: u32, timestamp_ns: u64) -> Result<()> {
        self.tx
            .send(TraceCommand::Finish { tgid, timestamp_ns })
            .context("finish runtime trace")
    }
}

fn run_collector(rx: Receiver<TraceCommand>, config: TraceCollectorConfig) {
    if let Err(err) = fs::create_dir_all(&config.root) {
        eprintln!(
            "runtime trace collector disabled: create {} failed: {err:#}",
            config.root.display()
        );
        return;
    }

    let mut active = HashMap::<u32, ActiveTrace>::new();
    loop {
        match rx.recv_timeout(config.sample_interval) {
            Ok(command) => {
                handle_command(command, &config.root, &mut active);
                while let Ok(command) = rx.try_recv() {
                    handle_command(command, &config.root, &mut active);
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }

        if active.is_empty() {
            continue;
        }
        let timestamp_ns = crate::monotonic_now_ns();
        for trace in active.values() {
            let _ = sample_trace(trace, timestamp_ns);
        }
    }
}

fn handle_command(command: TraceCommand, root: &Path, active: &mut HashMap<u32, ActiveTrace>) {
    match command {
        TraceCommand::Start(start) => match create_trace(root, start) {
            Ok(trace) => {
                active.insert(trace.tgid, trace);
            }
            Err(err) => eprintln!("runtime trace start failed: {err:#}"),
        },
        TraceCommand::Finish { tgid, timestamp_ns } => {
            let Some(trace) = active.remove(&tgid) else {
                return;
            };
            if let Err(err) = sample_trace(&trace, timestamp_ns) {
                eprintln!("runtime trace final sample failed: {err:#}");
            }
            if let Err(err) = append_event(
                &trace.run_dir,
                &json!({
                    "event": "invocation_finished",
                    "invocation_id": trace.activation_id,
                    "tgid": trace.tgid,
                    "timestamp_ns": timestamp_ns,
                    "cgroup_path": trace.cgroup_path.display().to_string(),
                }),
            ) {
                eprintln!("runtime trace finish event failed: {err:#}");
            }
        }
    }
}

fn create_trace(root: &Path, start: TraceStart) -> Result<ActiveTrace> {
    let profile_dir = root.join(sanitize_path_component(&start.profile_id));
    fs::create_dir_all(&profile_dir)
        .with_context(|| format!("create {}", profile_dir.display()))?;
    let run_dir = profile_dir.join(format!("{}-{}", start.meta.created_at_ns, start.meta.id));
    fs::create_dir_all(&run_dir).with_context(|| format!("create {}", run_dir.display()))?;
    initialize_trace_files(&run_dir)?;

    let warmth = if start.meta.is_cold_start {
        "cold"
    } else {
        "warm"
    };
    let meta = RuntimeTraceMeta {
        profile_id: start.profile_id,
        invocation_id: start.meta.id,
        tgid: start.meta.tgid,
        created_at_ns: start.meta.created_at_ns,
        deadline_ns: start.meta.deadline_ns,
        estimated_duration_ns: start.meta.estimated_duration_ns,
        warmth: warmth.to_string(),
        cgroup_path: start.cgroup_path.display().to_string(),
    };
    fs::write(
        run_dir.join("run_meta.json"),
        serde_json::to_vec_pretty(&meta).context("serialize runtime trace meta")?,
    )
    .with_context(|| format!("write {}", run_dir.join("run_meta.json").display()))?;
    append_event(
        &run_dir,
        &json!({
            "event": "invocation_started",
            "invocation_id": start.meta.id,
            "tgid": start.meta.tgid,
            "timestamp_ns": start.meta.created_at_ns,
            "cgroup_path": start.cgroup_path.display().to_string(),
        }),
    )?;

    let trace = ActiveTrace {
        run_dir,
        cgroup_path: start.cgroup_path,
        activation_id: start.meta.id,
        tgid: start.meta.tgid,
    };
    sample_trace(&trace, start.meta.created_at_ns)?;
    Ok(trace)
}

fn initialize_trace_files(run_dir: &Path) -> Result<()> {
    write_if_missing(&run_dir.join("events.jsonl"), "")?;
    write_if_missing(
        &run_dir.join("cgroup_cpu.csv"),
        "timestamp_ns,usage_usec,user_usec,system_usec,nr_periods,nr_throttled,throttled_usec\n",
    )?;
    write_if_missing(
        &run_dir.join("cgroup_memory.csv"),
        "timestamp_ns,current_bytes,peak_bytes,anon_bytes,file_bytes,pgfault,pgmajfault,oom,oom_kill\n",
    )?;
    write_if_missing(
        &run_dir.join("cgroup_io.csv"),
        "timestamp_ns,rbytes,wbytes,rios,wios,dbytes,dios\n",
    )?;
    write_if_missing(
        &run_dir.join("cgroup_pressure.csv"),
        "timestamp_ns,resource,scope,avg10,avg60,avg300,total\n",
    )?;
    Ok(())
}

fn sample_trace(trace: &ActiveTrace, timestamp_ns: u64) -> Result<()> {
    let cpu = read_key_values(&trace.cgroup_path.join("cpu.stat"));
    append_line(
        &trace.run_dir.join("cgroup_cpu.csv"),
        &format!(
            "{},{},{},{},{},{},{}\n",
            timestamp_ns,
            value(&cpu, "usage_usec"),
            value(&cpu, "user_usec"),
            value(&cpu, "system_usec"),
            value(&cpu, "nr_periods"),
            value(&cpu, "nr_throttled"),
            value(&cpu, "throttled_usec")
        ),
    )?;

    let memory_stat = read_key_values(&trace.cgroup_path.join("memory.stat"));
    let memory_events = read_key_values(&trace.cgroup_path.join("memory.events"));
    append_line(
        &trace.run_dir.join("cgroup_memory.csv"),
        &format!(
            "{},{},{},{},{},{},{},{},{}\n",
            timestamp_ns,
            read_u64(&trace.cgroup_path.join("memory.current")),
            read_u64(&trace.cgroup_path.join("memory.peak")),
            value(&memory_stat, "anon"),
            value(&memory_stat, "file"),
            value(&memory_stat, "pgfault"),
            value(&memory_stat, "pgmajfault"),
            value(&memory_events, "oom"),
            value(&memory_events, "oom_kill")
        ),
    )?;

    let io = read_io_stat(&trace.cgroup_path.join("io.stat"));
    append_line(
        &trace.run_dir.join("cgroup_io.csv"),
        &format!(
            "{},{},{},{},{},{},{}\n",
            timestamp_ns,
            value(&io, "rbytes"),
            value(&io, "wbytes"),
            value(&io, "rios"),
            value(&io, "wios"),
            value(&io, "dbytes"),
            value(&io, "dios")
        ),
    )?;

    for resource in ["cpu", "memory", "io"] {
        append_pressure(&trace.run_dir, &trace.cgroup_path, resource, timestamp_ns)?;
    }
    Ok(())
}

fn append_pressure(run_dir: &Path, cgroup_path: &Path, resource: &str, ts: u64) -> Result<()> {
    let path = cgroup_path.join(format!("{resource}.pressure"));
    let Ok(file) = File::open(path) else {
        return Ok(());
    };
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        append_pressure_line(run_dir, resource, ts, &line)?;
    }
    Ok(())
}

fn append_pressure_line(run_dir: &Path, resource: &str, ts: u64, line: &str) -> Result<()> {
    let mut parts = line.split_whitespace();
    let Some(scope) = parts.next() else {
        return Ok(());
    };
    let mut values = BTreeMap::new();
    for token in parts {
        if let Some((key, raw)) = token.split_once('=') {
            values.insert(key, raw);
        }
    }
    append_line(
        &run_dir.join("cgroup_pressure.csv"),
        &format!(
            "{ts},{resource},{scope},{},{},{},{}\n",
            values.get("avg10").copied().unwrap_or("0"),
            values.get("avg60").copied().unwrap_or("0"),
            values.get("avg300").copied().unwrap_or("0"),
            values.get("total").copied().unwrap_or("0")
        ),
    )
}

fn append_event(run_dir: &Path, event: &Value) -> Result<()> {
    append_line(
        &run_dir.join("events.jsonl"),
        &format!(
            "{}\n",
            serde_json::to_string(event).context("serialize trace event")?
        ),
    )
}

fn append_line(path: &Path, line: &str) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    file.write_all(line.as_bytes())
        .with_context(|| format!("append {}", path.display()))
}

fn write_if_missing(path: &Path, contents: &str) -> Result<()> {
    if !path.exists() {
        fs::write(path, contents).with_context(|| format!("write {}", path.display()))?;
    }
    Ok(())
}

fn sanitize_path_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "unknown".to_string()
    } else {
        out
    }
}

fn read_key_values(path: &Path) -> BTreeMap<String, u64> {
    let Ok(text) = fs::read_to_string(path) else {
        return BTreeMap::new();
    };
    text.lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let key = parts.next()?;
            let value = parts.next()?.parse::<u64>().ok()?;
            Some((key.to_string(), value))
        })
        .collect()
}

fn read_io_stat(path: &Path) -> BTreeMap<String, u64> {
    let Ok(text) = fs::read_to_string(path) else {
        return BTreeMap::new();
    };
    let mut totals = BTreeMap::new();
    for line in text.lines() {
        for field in line.split_whitespace().skip(1) {
            let mut parts = field.split('=');
            let Some(key) = parts.next() else {
                continue;
            };
            let Some(raw_value) = parts.next() else {
                continue;
            };
            if let Ok(value) = raw_value.parse::<u64>() {
                *totals.entry(key.to_string()).or_insert(0) += value;
            }
        }
    }
    totals
}

fn read_u64(path: &Path) -> u64 {
    fs::read_to_string(path)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

fn value(map: &BTreeMap<String, u64>, key: &str) -> u64 {
    map.get(key).copied().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use crate::registry::SloClass;

    fn write_pressure(path: &Path, value: u64) {
        fs::write(
            path,
            format!("some avg10=0.00 avg60=0.00 avg300=0.00 total={value}\n"),
        )
        .unwrap();
    }

    #[test]
    fn collector_writes_trace_files() {
        let dir = tempfile::tempdir().unwrap();
        let cgroup = dir.path().join("cg");
        fs::create_dir(&cgroup).unwrap();
        fs::write(
            cgroup.join("cpu.stat"),
            "usage_usec 1\nuser_usec 1\nsystem_usec 0\nnr_periods 1\nnr_throttled 0\nthrottled_usec 0\n",
        )
        .unwrap();
        fs::write(
            cgroup.join("memory.stat"),
            "anon 1\nfile 2\npgfault 3\npgmajfault 0\n",
        )
        .unwrap();
        fs::write(cgroup.join("memory.events"), "oom 0\noom_kill 0\n").unwrap();
        fs::write(cgroup.join("memory.current"), "64\n").unwrap();
        fs::write(cgroup.join("memory.peak"), "128\n").unwrap();
        fs::write(
            cgroup.join("io.stat"),
            "8:0 rbytes=1 wbytes=2 rios=3 wios=4 dbytes=0 dios=0\n",
        )
        .unwrap();
        write_pressure(&cgroup.join("cpu.pressure"), 1);
        write_pressure(&cgroup.join("memory.pressure"), 2);
        write_pressure(&cgroup.join("io.pressure"), 3);

        let handle = TraceCollectorHandle::spawn(TraceCollectorConfig {
            root: dir.path().join("traces"),
            sample_interval: Duration::from_millis(5),
        });
        let meta = InvocationMeta {
            id: 42,
            tgid: 4242,
            deadline_ns: 1000,
            estimated_duration_ns: 2000,
            slo_class: SloClass::Standard,
            is_cold_start: false,
            profile_id: Some("demo".to_string()),
            created_at_ns: 10,
        };
        handle
            .start_invocation(&meta, "demo", cgroup.clone())
            .unwrap();
        thread::sleep(Duration::from_millis(20));
        handle.finish_invocation(4242, 50).unwrap();
        drop(handle);
        thread::sleep(Duration::from_millis(20));

        let run_dir = dir.path().join("traces").join("demo").join("10-42");
        assert!(run_dir.join("run_meta.json").exists());
        assert!(run_dir.join("events.jsonl").exists());
        assert!(run_dir.join("cgroup_cpu.csv").exists());
        assert!(run_dir.join("cgroup_memory.csv").exists());
        assert!(run_dir.join("cgroup_io.csv").exists());
        assert!(run_dir.join("cgroup_pressure.csv").exists());
        let events = fs::read_to_string(run_dir.join("events.jsonl")).unwrap();
        assert!(events.contains("invocation_started"));
        assert!(events.contains("invocation_finished"));
    }
}
