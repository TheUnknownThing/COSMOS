#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
OUT_DIR="${OUT_DIR:-$ROOT/benchmarks/runs}"
REPETITIONS="${REPETITIONS:-1}"
SAMPLE_MS="${SAMPLE_MS:-100}"
DURATION_S="${DURATION_S:-60}"
CPU_WORKERS="${CPU_WORKERS:-$(nproc)}"
MEM_WORKERS="${MEM_WORKERS:-}"
MEM_BYTES_PER_WORKER="${MEM_BYTES_PER_WORKER:-2G}"
FIO_JOBS="${FIO_JOBS:-16}"
FIO_SIZE="${FIO_SIZE:-2G}"
FIO_BS="${FIO_BS:-1M}"
NET_PARALLEL="${NET_PARALLEL:-32}"
NET_PORT="${NET_PORT:-55201}"
RUN_OPENWHISK_LOAD="${RUN_OPENWHISK_LOAD:-1}"
OW_REQUESTS="${OW_REQUESTS:-512}"
OW_CONCURRENCY="${OW_CONCURRENCY:-128}"
OW_BURN_MS="${OW_BURN_MS:-50}"

if [[ -z "$MEM_WORKERS" ]]; then
  if (( CPU_WORKERS >= 32 )); then
    MEM_WORKERS=32
  else
    MEM_WORKERS="$CPU_WORKERS"
  fi
fi

cd "$ROOT"
cargo build -p cosmos-bench-profiler
cargo run -p cosmos-bench-profiler -- preflight --strict

run_command_workload() {
  local name="$1"
  local label="$2"
  shift 2
  sudo -E "$ROOT/target/debug/cosmos-bench-profiler" standalone \
    --out-dir "$OUT_DIR" \
    --name "$name" \
    --workload command \
    --workload-label "$label" \
    --input "capacity" \
    --sample-ms "$SAMPLE_MS" \
    -- "$@"
}

for rep in $(seq 1 "$REPETITIONS"); do
  run_command_workload \
    "capacity-cpu-${CPU_WORKERS}w-r${rep}" \
    "capacity-cpu-${CPU_WORKERS}w" \
    stress-ng \
    --cpu "$CPU_WORKERS" \
    --cpu-method matrixprod \
    --timeout "${DURATION_S}s" \
    --metrics-brief

  run_command_workload \
    "capacity-memory-${MEM_WORKERS}w-r${rep}" \
    "capacity-memory-${MEM_WORKERS}w-${MEM_BYTES_PER_WORKER}" \
    stress-ng \
    --vm "$MEM_WORKERS" \
    --vm-bytes "$MEM_BYTES_PER_WORKER" \
    --vm-keep \
    --timeout "${DURATION_S}s" \
    --metrics-brief

  run_command_workload \
    "capacity-fio-${FIO_JOBS}j-r${rep}" \
    "capacity-fio-${FIO_JOBS}j-${FIO_SIZE}" \
    bash -lc \
    'fio --name=cosmos-capacity --directory="$COSMOS_BENCH_TMPDIR" --rw=randrw --rwmixread=70 --bs="$0" --size="$1" --numjobs="$2" --time_based --runtime="$3" --iodepth=16 --direct=1 --group_reporting --unlink=1' \
    "$FIO_BS" "$FIO_SIZE" "$FIO_JOBS" "$DURATION_S"

  run_command_workload \
    "capacity-network-${NET_PARALLEL}p-r${rep}" \
    "capacity-network-${NET_PARALLEL}p" \
    bash -lc \
    'pkill -f "iperf3 -s -1 -p $0" >/dev/null 2>&1 || true; iperf3 -s -1 -p "$0" >/tmp/cosmos-iperf3-server.log 2>&1 & server=$!; sleep 1; iperf3 -c 127.0.0.1 -p "$0" -P "$1" -t "$2"; wait "$server"' \
    "$NET_PORT" "$NET_PARALLEL" "$DURATION_S"
done

if [[ "$RUN_OPENWHISK_LOAD" == "1" ]]; then
  if [[ -f /local/benchmarks/openwhisk-host.env ]]; then
    # shellcheck disable=SC1091
    . /local/benchmarks/openwhisk-host.env
  fi
  if [[ -n "${OPENWHISK_PID:-}" ]] && kill -0 "$OPENWHISK_PID" 2>/dev/null; then
    sudo docker ps -aq --filter name=wsk0_ | xargs -r sudo docker rm -f >/dev/null
    REQUESTS="$OW_REQUESTS" \
      CONCURRENCY="$OW_CONCURRENCY" \
      BURN_MS="$OW_BURN_MS" \
      WARMUP_REQUESTS=1 \
      OUT_DIR="$OUT_DIR" \
      ACTION=cosmos_capacity_load \
      "$ROOT/benchmarks/profiler/scripts/run_openwhisk_concurrent_load.sh" || true
  else
    echo "Skipping OpenWhisk concurrent load: OpenWhisk is not running." >&2
  fi
fi

cargo run -p cosmos-bench-profiler -- profile-db \
  --runs-dir "$OUT_DIR" \
  --out "$ROOT/benchmarks/profile_db.json" \
  --strict
