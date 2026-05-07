#!/usr/bin/env bash
set -euo pipefail

BASE="${COSMOS_CGROUP_POLICY_ROOT:-/sys/fs/cgroup/cosmos-policy}"
CONTROLLERS=(cpu cpuset io memory pids)

usage() {
  cat <<'USAGE'
Usage:
  cosmos-cgroup-policy create NAME [options]
  cosmos-cgroup-policy set NAME [options]
  cosmos-cgroup-policy run NAME -- COMMAND [ARGS...]
  cosmos-cgroup-policy add-pid NAME PID
  cosmos-cgroup-policy show NAME
  cosmos-cgroup-policy list
  cosmos-cgroup-policy delete NAME [--kill]

Options for create/set:
  --mem BYTES|K|M|G|max          Set memory.max
  --mem-high BYTES|K|M|G|max     Set memory.high
  --swap BYTES|K|M|G|max         Set memory.swap.max
  --cpu VALUE                    Set cpu.max, e.g. "max 100000" or "50000 100000"
  --cpu-weight VALUE             Set cpu.weight
  --cpus LIST                    Set cpuset.cpus, e.g. "0-15,32-47"
  --io-weight VALUE              Set io.weight
  --pids VALUE|max               Set pids.max
  --set FILE=VALUE               Set an arbitrary cgroup control file

Examples:
  sudo cosmos-cgroup-policy create test --mem 1G --swap 0 --cpu "50000 100000"
  sudo cosmos-cgroup-policy run test -- stress-ng --cpu 4 --timeout 10s
  sudo cosmos-cgroup-policy show test
  sudo cosmos-cgroup-policy delete test --kill
USAGE
}

die() {
  echo "cosmos-cgroup-policy: $*" >&2
  exit 1
}

need_cgroup_v2() {
  [[ -f /sys/fs/cgroup/cgroup.controllers ]] || die "cgroup v2 is not mounted at /sys/fs/cgroup"
}

valid_name() {
  [[ "$1" =~ ^[A-Za-z0-9_.-]+$ ]] || die "invalid policy name '$1' (use letters, digits, '.', '_', '-')"
}

policy_path() {
  valid_name "$1"
  printf '%s/%s\n' "$BASE" "$1"
}

enable_controllers() {
  local path="$1" available ctrl
  [[ -f "$path/cgroup.controllers" ]] || return 0
  available="$(<"$path/cgroup.controllers")"
  for ctrl in "${CONTROLLERS[@]}"; do
    if [[ " $available " == *" $ctrl "* ]]; then
      echo "+$ctrl" >"$path/cgroup.subtree_control" 2>/dev/null || true
    fi
  done
}

init_cpuset() {
  local path="$1" root="/sys/fs/cgroup"
  if [[ -w "$path/cpuset.cpus" && -f "$root/cpuset.cpus.effective" && ! -s "$path/cpuset.cpus" ]]; then
    cat "$root/cpuset.cpus.effective" >"$path/cpuset.cpus" 2>/dev/null || true
  fi
  if [[ -w "$path/cpuset.mems" && -f "$root/cpuset.mems.effective" && ! -s "$path/cpuset.mems" ]]; then
    cat "$root/cpuset.mems.effective" >"$path/cpuset.mems" 2>/dev/null || true
  fi
}

ensure_parent() {
  need_cgroup_v2
  mkdir -p "$BASE"
  enable_controllers /sys/fs/cgroup
  init_cpuset "$BASE"
  enable_controllers "$BASE"
}

parse_bytes() {
  local value="$1" number suffix
  [[ "$value" == "max" ]] && {
    echo max
    return
  }
  [[ "$value" =~ ^([0-9]+)([KkMmGgTt]?)$ ]] || die "invalid byte value '$value'"
  number="${BASH_REMATCH[1]}"
  suffix="${BASH_REMATCH[2]}"
  case "$suffix" in
    K|k) echo $((number * 1024)) ;;
    M|m) echo $((number * 1024 * 1024)) ;;
    G|g) echo $((number * 1024 * 1024 * 1024)) ;;
    T|t) echo $((number * 1024 * 1024 * 1024 * 1024)) ;;
    *) echo "$number" ;;
  esac
}

write_control() {
  local cg="$1" file="$2" value="$3"
  [[ -e "$cg/$file" ]] || die "$file is not available in $cg"
  printf '%s\n' "$value" >"$cg/$file" || die "failed to write $file=$value"
}

apply_options() {
  local cg="$1"
  shift
  while (($#)); do
    case "$1" in
      --mem)
        (($# >= 2)) || die "--mem requires a value"
        write_control "$cg" memory.max "$(parse_bytes "$2")"
        shift 2
        ;;
      --mem-high)
        (($# >= 2)) || die "--mem-high requires a value"
        write_control "$cg" memory.high "$(parse_bytes "$2")"
        shift 2
        ;;
      --swap)
        (($# >= 2)) || die "--swap requires a value"
        write_control "$cg" memory.swap.max "$(parse_bytes "$2")"
        shift 2
        ;;
      --cpu)
        (($# >= 2)) || die "--cpu requires a value"
        write_control "$cg" cpu.max "$2"
        shift 2
        ;;
      --cpu-weight)
        (($# >= 2)) || die "--cpu-weight requires a value"
        write_control "$cg" cpu.weight "$2"
        shift 2
        ;;
      --cpus)
        (($# >= 2)) || die "--cpus requires a value"
        init_cpuset "$cg"
        write_control "$cg" cpuset.cpus "$2"
        shift 2
        ;;
      --io-weight)
        (($# >= 2)) || die "--io-weight requires a value"
        write_control "$cg" io.weight "$2"
        shift 2
        ;;
      --pids)
        (($# >= 2)) || die "--pids requires a value"
        write_control "$cg" pids.max "$2"
        shift 2
        ;;
      --set)
        (($# >= 2)) || die "--set requires FILE=VALUE"
        [[ "$2" == *=* ]] || die "--set requires FILE=VALUE"
        write_control "$cg" "${2%%=*}" "${2#*=}"
        shift 2
        ;;
      *)
        die "unknown option '$1'"
        ;;
    esac
  done
}

create_policy() {
  (($# >= 1)) || die "create requires NAME"
  local name="$1" cg
  shift
  ensure_parent
  cg="$(policy_path "$name")"
  mkdir -p "$cg"
  init_cpuset "$cg"
  apply_options "$cg" "$@"
  echo "$cg"
}

set_policy() {
  (($# >= 1)) || die "set requires NAME"
  local cg
  cg="$(policy_path "$1")"
  shift
  [[ -d "$cg" ]] || die "policy does not exist: $cg"
  apply_options "$cg" "$@"
}

show_policy() {
  (($# == 1)) || die "show requires NAME"
  local cg f
  cg="$(policy_path "$1")"
  [[ -d "$cg" ]] || die "policy does not exist: $cg"
  echo "path $cg"
  for f in cgroup.procs cpu.max cpu.weight cpuset.cpus cpuset.cpus.effective io.weight memory.current memory.high memory.max memory.swap.max pids.current pids.max; do
    [[ -e "$cg/$f" ]] && printf '%-22s %s\n' "$f" "$(tr '\n' ' ' <"$cg/$f")"
  done
}

list_policies() {
  ensure_parent
  find "$BASE" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | sort
}

run_policy() {
  (($# >= 3)) || die "run requires NAME -- COMMAND"
  local name="$1" cg tmp pid rc
  shift
  [[ "$1" == "--" ]] || die "run requires -- before command"
  shift
  cg="$(policy_path "$name")"
  [[ -d "$cg" ]] || die "policy does not exist: $cg"

  tmp="$(mktemp -d "${TMPDIR:-/tmp}/cosmos-cgroup-policy.XXXXXX")"
  (
    printf '%s\n' "$BASHPID" >"$tmp/pid"
    while [[ ! -e "$tmp/go" ]]; do
      sleep 0.001
    done
    exec "$@"
  ) &
  pid=$!

  while [[ ! -s "$tmp/pid" ]]; do
    kill -0 "$pid" 2>/dev/null || die "command exited before cgroup assignment"
    sleep 0.001
  done
  pid="$(<"$tmp/pid")"

  if ! echo "$pid" >"$cg/cgroup.procs"; then
    kill "$pid" 2>/dev/null || true
    rm -rf "$tmp"
    die "failed to move pid $pid into $cg"
  fi
  touch "$tmp/go"
  set +e
  wait "$pid"
  rc=$?
  set -e
  rm -rf "$tmp"
  exit "$rc"
}

add_pid() {
  (($# == 2)) || die "add-pid requires NAME PID"
  local cg
  cg="$(policy_path "$1")"
  [[ -d "$cg" ]] || die "policy does not exist: $cg"
  echo "$2" >"$cg/cgroup.procs"
}

delete_policy() {
  (($# >= 1)) || die "delete requires NAME"
  local cg kill_first=0 pid
  cg="$(policy_path "$1")"
  shift
  while (($#)); do
    case "$1" in
      --kill) kill_first=1; shift ;;
      *) die "unknown option '$1'" ;;
    esac
  done
  [[ -d "$cg" ]] || die "policy does not exist: $cg"
  if ((kill_first)); then
    while read -r pid; do
      [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
    done <"$cg/cgroup.procs"
    sleep 0.2
  fi
  rmdir "$cg"
}

cmd="${1:-}"
[[ -n "$cmd" ]] || {
  usage
  exit 2
}
shift

case "$cmd" in
  create) create_policy "$@" ;;
  set) set_policy "$@" ;;
  show) show_policy "$@" ;;
  list) list_policies "$@" ;;
  run) run_policy "$@" ;;
  add-pid) add_pid "$@" ;;
  delete) delete_policy "$@" ;;
  -h|--help|help) usage ;;
  *) die "unknown command '$cmd'" ;;
esac
