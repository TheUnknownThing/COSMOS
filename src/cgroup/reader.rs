// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::Result;

use crate::registry::{ResourcePressureTotals, ResourceSnapshot};

#[derive(Debug, Default)]
pub struct CgroupReader;

impl CgroupReader {
    pub fn new() -> Self {
        Self
    }

    pub fn read(&self, cgroup_path: &Path, timestamp_ns: u64) -> Result<ResourceSnapshot> {
        let cpu = read_key_values(&cgroup_path.join("cpu.stat"));
        let memory_stat = read_key_values(&cgroup_path.join("memory.stat"));
        let io = read_io_stat(&cgroup_path.join("io.stat"));

        Ok(ResourceSnapshot {
            timestamp_ns,
            cpu_usage_usec: value(&cpu, "usage_usec"),
            cpu_user_usec: value(&cpu, "user_usec"),
            cpu_system_usec: value(&cpu, "system_usec"),
            memory_current_bytes: read_u64(&cgroup_path.join("memory.current")),
            memory_peak_bytes: read_u64(&cgroup_path.join("memory.peak")),
            memory_anon_bytes: value(&memory_stat, "anon"),
            memory_file_bytes: value(&memory_stat, "file"),
            memory_pgfault: value(&memory_stat, "pgfault"),
            memory_pgmajfault: value(&memory_stat, "pgmajfault"),
            io_read_bytes: value(&io, "rbytes"),
            io_write_bytes: value(&io, "wbytes"),
            io_read_ios: value(&io, "rios"),
            io_write_ios: value(&io, "wios"),
            io_discard_bytes: value(&io, "dbytes"),
            io_discard_ios: value(&io, "dios"),
            cpu_pressure: read_pressure_totals(&cgroup_path.join("cpu.pressure")),
            memory_pressure: read_pressure_totals(&cgroup_path.join("memory.pressure")),
            io_pressure: read_pressure_totals(&cgroup_path.join("io.pressure")),
        })
    }
}

fn read_key_values(path: &Path) -> HashMap<String, u64> {
    let Ok(text) = fs::read_to_string(path) else {
        return HashMap::new();
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

fn read_io_stat(path: &Path) -> HashMap<String, u64> {
    let Ok(text) = fs::read_to_string(path) else {
        return HashMap::new();
    };
    let mut totals = HashMap::new();
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

fn read_pressure_totals(path: &Path) -> ResourcePressureTotals {
    let Ok(text) = fs::read_to_string(path) else {
        return ResourcePressureTotals::default();
    };
    let mut totals = ResourcePressureTotals::default();
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        let Some(scope) = parts.next() else {
            continue;
        };
        let total = parts
            .find_map(|field| {
                field
                    .strip_prefix("total=")
                    .and_then(|raw| raw.parse().ok())
            })
            .unwrap_or(0);
        match scope {
            "some" => totals.some_total = total,
            "full" => totals.full_total = total,
            _ => {}
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

fn value(map: &HashMap<String, u64>, key: &str) -> u64 {
    map.get(key).copied().unwrap_or(0)
}
