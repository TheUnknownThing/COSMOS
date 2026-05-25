use anyhow::Result;
use std::ffi::CString;

const BPF_MAP_PATH: &str = "/sys/fs/bpf/cosmos/invocation_meta";

// BPF commands
const BPF_OBJ_GET: i32 = 7;
const BPF_MAP_UPDATE_ELEM: i32 = 2;
const BPF_MAP_DELETE_ELEM: i32 = 3;

// BPF_ATTR size: 120 bytes on x86_64, but we only use first 32
const BPF_ATTR_SZ: usize = 64;

#[repr(C)]
pub struct InvocationMeta {
    pub deadline_ns: u64,
    pub slo_class: u32,
    pub is_cold_start: u32,
    pub invocation_id: u64,
}

unsafe fn sys_bpf(cmd: i32, attr: *const u8, size: u32) -> i64 {
    libc::syscall(libc::SYS_bpf, cmd, attr, size)
}

fn open_pinned_map() -> Result<i32> {
    let path = CString::new(BPF_MAP_PATH)?;
    let mut attr = [0u8; BPF_ATTR_SZ];

    // bpf_attr.obj_get: pathname at offset 0, bpf_fd at offset 8, file_flags at offset 12
    unsafe {
        let ptr = attr.as_mut_ptr();
        (ptr as *mut u64).write_unaligned(path.as_ptr() as u64);
        (ptr.add(8) as *mut u32).write_unaligned(0); // bpf_fd = 0
        (ptr.add(12) as *mut u32).write_unaligned(0); // file_flags = 0
    }

    let fd = unsafe { sys_bpf(BPF_OBJ_GET, attr.as_ptr(), BPF_ATTR_SZ as u32) };
    if fd < 0 {
        let err = std::io::Error::last_os_error();
        anyhow::bail!("failed to open pinned BPF map {}: {}", BPF_MAP_PATH, err);
    }
    Ok(fd as i32)
}

pub fn write_meta(tgid: u32, meta: &InvocationMeta) -> Result<()> {
    let map_fd = open_pinned_map()?;
    let key: u32 = tgid;

    let mut attr = [0u8; BPF_ATTR_SZ];

    // bpf_attr.map: map_fd at offset 0, key at offset 8, value at offset 16, flags at offset 24
    unsafe {
        let ptr = attr.as_mut_ptr();
        (ptr as *mut u32).write_unaligned(map_fd as u32);
        (ptr.add(8) as *mut u64).write_unaligned(&key as *const u32 as u64);
        (ptr.add(16) as *mut u64).write_unaligned(meta as *const InvocationMeta as u64);
        (ptr.add(24) as *mut u64).write_unaligned(0); // flags = BPF_ANY
    }

    let ret = unsafe { sys_bpf(BPF_MAP_UPDATE_ELEM, attr.as_ptr(), BPF_ATTR_SZ as u32) };

    unsafe {
        libc::close(map_fd);
    }

    if ret < 0 {
        let err = std::io::Error::last_os_error();
        anyhow::bail!("BPF_MAP_UPDATE_ELEM failed for tgid={}: {}", tgid, err);
    }
    Ok(())
}

pub fn delete_meta(tgid: u32) -> Result<()> {
    let map_fd = open_pinned_map()?;
    let key: u32 = tgid;

    let mut attr = [0u8; BPF_ATTR_SZ];

    // bpf_attr.map: map_fd at offset 0, key at offset 8
    unsafe {
        let ptr = attr.as_mut_ptr();
        (ptr as *mut u32).write_unaligned(map_fd as u32);
        (ptr.add(8) as *mut u64).write_unaligned(&key as *const u32 as u64);
    }

    let ret = unsafe { sys_bpf(BPF_MAP_DELETE_ELEM, attr.as_ptr(), BPF_ATTR_SZ as u32) };

    unsafe {
        libc::close(map_fd);
    }

    if ret < 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ENOENT) {
            return Ok(());
        }
        anyhow::bail!("BPF_MAP_DELETE_ELEM failed for tgid={}: {}", tgid, err);
    }
    Ok(())
}
