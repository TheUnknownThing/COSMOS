// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

#[derive(Debug, Default)]
pub struct CgroupResolver;

impl CgroupResolver {
    pub fn new() -> Self {
        Self
    }

    pub fn resolve(&self, tgid: u32) -> Result<(PathBuf, u64)> {
        let text = fs::read_to_string(format!("/proc/{tgid}/cgroup"))
            .with_context(|| format!("read /proc/{tgid}/cgroup"))?;
        for line in text.lines() {
            if let Some(path) = line.strip_prefix("0::") {
                let cgroup_path = Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/'));
                let cgroup_id = fs::metadata(&cgroup_path)
                    .with_context(|| format!("stat {}", cgroup_path.display()))?
                    .ino();
                return Ok((cgroup_path, cgroup_id));
            }
        }
        bail!("cgroup v2 path not found for pid {tgid}");
    }
}
