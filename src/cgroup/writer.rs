// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

const MIN_MEMORY_HIGH_BYTES: u64 = 16 * 1024 * 1024;
const CGROUP_ROOT_ENV: &str = "COSMOS_CGROUP_POLICY_ROOT";
const DEFAULT_CGROUP_ROOT: &str = "/sys/fs/cgroup/cosmos-policy";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CgroupDevice {
    pub major: u32,
    pub minor: u32,
}

impl CgroupDevice {
    pub fn parse(raw: &str) -> Option<Self> {
        let (major, minor) = raw.split_once(':')?;
        Some(Self {
            major: major.parse().ok()?,
            minor: minor.parse().ok()?,
        })
    }

    fn control_prefix(self) -> String {
        format!("{}:{}", self.major, self.minor)
    }
}

#[derive(Debug, Clone)]
pub struct CgroupWriter {
    root: PathBuf,
    default_io_device: Option<CgroupDevice>,
    min_memory_high_bytes: u64,
}

impl Default for CgroupWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl CgroupWriter {
    pub fn new() -> Self {
        Self {
            root: env::var_os(CGROUP_ROOT_ENV)
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(DEFAULT_CGROUP_ROOT)),
            default_io_device: None,
            min_memory_high_bytes: MIN_MEMORY_HIGH_BYTES,
        }
    }

    #[allow(dead_code)]
    pub fn with_root(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            ..Self::new()
        }
    }

    #[allow(dead_code)]
    pub fn with_default_io_device(mut self, device: CgroupDevice) -> Self {
        self.default_io_device = Some(device);
        self
    }

    pub fn set_memory_high(&self, cgroup_path: &Path, bytes: Option<u64>) -> Result<()> {
        if let Some(bytes) = bytes {
            if bytes < self.min_memory_high_bytes {
                bail!(
                    "refusing memory.high={} below safety floor {}",
                    bytes,
                    self.min_memory_high_bytes
                );
            }
        }
        self.write_control(
            cgroup_path,
            "memory.high",
            bytes
                .map(|bytes| bytes.to_string())
                .unwrap_or_else(|| "max".to_string()),
        )
    }

    pub fn set_memory_min(&self, cgroup_path: &Path, bytes: Option<u64>) -> Result<()> {
        self.write_control(
            cgroup_path,
            "memory.min",
            bytes
                .map(|bytes| bytes.to_string())
                .unwrap_or_else(|| "0".to_string()),
        )
    }

    pub fn set_io_weight(&self, cgroup_path: &Path, weight: u64) -> Result<()> {
        if !(1..=10_000).contains(&weight) {
            bail!(
                "refusing io.weight={} outside cgroup v2 range 1..=10000",
                weight
            );
        }
        self.write_control(cgroup_path, "io.weight", weight.to_string())
    }

    pub fn set_io_latency(
        &self,
        cgroup_path: &Path,
        device: CgroupDevice,
        target_us: Option<u64>,
    ) -> Result<()> {
        let target_us = target_us.unwrap_or(0);
        self.write_control(
            cgroup_path,
            "io.latency",
            format!("{} target={}", device.control_prefix(), target_us),
        )
    }

    pub fn set_io_max(
        &self,
        cgroup_path: &Path,
        device: CgroupDevice,
        read_bps: Option<u64>,
        write_bps: Option<u64>,
    ) -> Result<()> {
        let rbps = read_bps
            .map(|bps| bps.to_string())
            .unwrap_or_else(|| "max".to_string());
        let wbps = write_bps
            .map(|bps| bps.to_string())
            .unwrap_or_else(|| "max".to_string());
        self.write_control(
            cgroup_path,
            "io.max",
            format!("{} rbps={} wbps={}", device.control_prefix(), rbps, wbps),
        )
    }

    pub fn io_devices(&self, cgroup_path: &Path) -> Vec<CgroupDevice> {
        let mut devices = BTreeSet::new();
        if let Ok(text) = fs::read_to_string(cgroup_path.join("io.stat")) {
            for line in text.lines() {
                if let Some(device) = line.split_whitespace().next().and_then(CgroupDevice::parse) {
                    devices.insert(device);
                }
            }
        }
        if devices.is_empty() {
            if let Some(device) = self.default_io_device {
                devices.insert(device);
            }
        }
        devices.into_iter().collect()
    }

    pub fn manages(&self, cgroup_path: &Path) -> bool {
        self.validate_cgroup_path(cgroup_path).is_ok()
    }

    fn write_control(
        &self,
        cgroup_path: &Path,
        control_file: &str,
        value: impl AsRef<str>,
    ) -> Result<()> {
        let path = self.validate_cgroup_path(cgroup_path)?.join(control_file);
        let value = value.as_ref();
        fs::write(&path, format!("{value}\n"))
            .with_context(|| format!("write {} to {}", value, path.display()))
    }

    fn validate_cgroup_path(&self, cgroup_path: &Path) -> Result<PathBuf> {
        let root = fs::canonicalize(&self.root)
            .with_context(|| format!("canonicalize cgroup root {}", self.root.display()))?;
        let path = fs::canonicalize(cgroup_path)
            .with_context(|| format!("canonicalize cgroup path {}", cgroup_path.display()))?;
        if !path.starts_with(&root) {
            bail!(
                "refusing to write outside cgroup root: {} is not under {}",
                path.display(),
                root.display()
            );
        }
        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cosmos-cgroup-writer-{name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn test_cgroup(root: &Path) -> PathBuf {
        let cgroup = root.join("cg");
        fs::create_dir_all(&cgroup).unwrap();
        for file in [
            "memory.high",
            "memory.min",
            "io.weight",
            "io.latency",
            "io.max",
            "io.stat",
        ] {
            fs::write(cgroup.join(file), b"").unwrap();
        }
        cgroup
    }

    #[test]
    fn writes_memory_and_io_controls_under_root() {
        let root = temp_root("writes");
        let cgroup = test_cgroup(&root);
        fs::write(cgroup.join("io.stat"), "8:0 rbytes=1 wbytes=2\n").unwrap();
        let writer = CgroupWriter::with_root(&root);

        writer
            .set_memory_high(&cgroup, Some(64 * 1024 * 1024))
            .unwrap();
        writer
            .set_memory_min(&cgroup, Some(32 * 1024 * 1024))
            .unwrap();
        writer.set_io_weight(&cgroup, 750).unwrap();
        writer
            .set_io_latency(&cgroup, CgroupDevice { major: 8, minor: 0 }, Some(2_500))
            .unwrap();
        writer
            .set_io_max(
                &cgroup,
                CgroupDevice { major: 8, minor: 0 },
                Some(10_000),
                None,
            )
            .unwrap();

        assert_eq!(
            fs::read_to_string(cgroup.join("memory.high")).unwrap(),
            "67108864\n"
        );
        assert_eq!(
            fs::read_to_string(cgroup.join("memory.min")).unwrap(),
            "33554432\n"
        );
        assert_eq!(
            fs::read_to_string(cgroup.join("io.weight")).unwrap(),
            "750\n"
        );
        assert_eq!(
            fs::read_to_string(cgroup.join("io.latency")).unwrap(),
            "8:0 target=2500\n"
        );
        assert_eq!(
            fs::read_to_string(cgroup.join("io.max")).unwrap(),
            "8:0 rbps=10000 wbps=max\n"
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn refuses_unsafe_values_and_paths() {
        let root = temp_root("guards");
        let cgroup = test_cgroup(&root);
        let outside = temp_root("outside");
        fs::write(outside.join("memory.high"), b"").unwrap();
        let writer = CgroupWriter::with_root(&root);

        assert!(writer.set_memory_high(&cgroup, Some(1)).is_err());
        assert!(writer.set_io_weight(&cgroup, 0).is_err());
        assert!(writer
            .set_memory_high(&outside, Some(64 * 1024 * 1024))
            .is_err());

        fs::remove_dir_all(root).unwrap();
        fs::remove_dir_all(outside).unwrap();
    }

    #[test]
    fn discovers_io_devices_from_io_stat() {
        let root = temp_root("devices");
        let cgroup = test_cgroup(&root);
        fs::write(
            cgroup.join("io.stat"),
            "8:16 rbytes=1 wbytes=2\n8:0 rbytes=3 wbytes=4\n",
        )
        .unwrap();
        let writer = CgroupWriter::with_root(&root);
        assert_eq!(
            writer.io_devices(&cgroup),
            vec![
                CgroupDevice { major: 8, minor: 0 },
                CgroupDevice {
                    major: 8,
                    minor: 16
                }
            ]
        );
        fs::remove_dir_all(root).unwrap();
    }
}
