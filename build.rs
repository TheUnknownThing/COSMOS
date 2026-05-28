// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    scx_rustland_core::RustLandBuilder::new()
        .unwrap()
        .build()
        .unwrap();

    println!("cargo:rerun-if-changed=src/network/tc.bpf.c");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let out = out_dir.join("cosmos_net_tc.bpf.o");
    let status = Command::new("clang")
        .args([
            "-O2",
            "-g",
            "-target",
            "bpf",
            "-D__TARGET_ARCH_x86",
            "-Wall",
            "-Werror",
            "-c",
            "src/network/tc.bpf.c",
            "-o",
        ])
        .arg(&out)
        .status()
        .expect("failed to execute clang for cosmos network tc eBPF");
    if !status.success() {
        panic!("failed to compile src/network/tc.bpf.c");
    }
}
