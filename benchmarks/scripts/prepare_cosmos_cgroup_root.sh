#!/usr/bin/env bash
set -euo pipefail

ROOT="${COSMOS_CGROUP_POLICY_ROOT:-/sys/fs/cgroup/cosmos-policy}"

if [[ "$(id -u)" -ne 0 ]]; then
  exec sudo -E "$0" "$@"
fi

mkdir -p "$ROOT"

if [[ -f "$ROOT/cgroup.controllers" && -f "$ROOT/cgroup.subtree_control" ]]; then
  for ctl in cpu io memory cpuset pids; do
    if grep -qw "$ctl" "$ROOT/cgroup.controllers" &&
       ! grep -qw "$ctl" "$ROOT/cgroup.subtree_control"; then
      echo "+$ctl" > "$ROOT/cgroup.subtree_control" || true
    fi
  done
fi

test_dir="$ROOT/cosmos-write-test"
rmdir "$test_dir" 2>/dev/null || true
mkdir "$test_dir"
echo 67108864 > "$test_dir/memory.high"
echo 750 > "$test_dir/io.weight"
echo max > "$test_dir/memory.high"
echo 100 > "$test_dir/io.weight"
rmdir "$test_dir"

echo "COSMOS cgroup root ready: $ROOT"
