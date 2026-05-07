#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  cosmos-net-policy show [IFACE]
  cosmos-net-policy clear IFACE
  cosmos-net-policy netem IFACE NETEM_ARGS...
  cosmos-net-policy rate IFACE RATE [BURST] [LATENCY]

Examples:
  cosmos-net-policy show docker0
  sudo cosmos-net-policy netem docker0 delay 20ms loss 0.1%
  sudo cosmos-net-policy rate docker0 100mbit
  sudo cosmos-net-policy clear docker0
USAGE
}

die() {
  echo "cosmos-net-policy: $*" >&2
  exit 1
}

iface_exists() {
  ip link show dev "$1" >/dev/null 2>&1 || die "interface does not exist: $1"
}

cmd="${1:-}"
[[ -n "$cmd" ]] || {
  usage
  exit 2
}
shift

case "$cmd" in
  show)
    if (($# == 0)); then
      ip -br link
      tc qdisc show
    elif (($# == 1)); then
      iface_exists "$1"
      ip -br link show dev "$1"
      tc -s qdisc show dev "$1"
    else
      die "show accepts at most one interface"
    fi
    ;;
  clear)
    (($# == 1)) || die "clear requires IFACE"
    iface_exists "$1"
    tc qdisc del dev "$1" root 2>/dev/null || true
    tc qdisc del dev "$1" ingress 2>/dev/null || true
    ;;
  netem)
    (($# >= 2)) || die "netem requires IFACE and NETEM_ARGS"
    iface="$1"
    shift
    iface_exists "$iface"
    tc qdisc replace dev "$iface" root netem "$@"
    ;;
  rate)
    (($# >= 2 && $# <= 4)) || die "rate requires IFACE RATE [BURST] [LATENCY]"
    iface="$1"
    rate="$2"
    burst="${3:-32kbit}"
    latency="${4:-400ms}"
    iface_exists "$iface"
    tc qdisc replace dev "$iface" root tbf rate "$rate" burst "$burst" latency "$latency"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    die "unknown command '$cmd'"
    ;;
esac
