#!/usr/bin/env bash
set -u -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
if [[ -f /etc/profile.d/cosmos-benchmark.sh ]]; then
  # shellcheck disable=SC1091
  source /etc/profile.d/cosmos-benchmark.sh
fi

COSMOS_PREFIX="${COSMOS_PREFIX:-/usr/local/cosmos}"
COSMOS_HOME="${COSMOS_HOME:-$ROOT}"
OUT_DIR="${OUT_DIR:-$COSMOS_PREFIX/benchmarks/runs}"
RUN_DIR="${RUN_DIR:-$OUT_DIR/sebs-openwhisk-cold-warm-$(date +%Y%m%d-%H%M%S)}"
LOG_DIR="$RUN_DIR/logs"
RESULTS="$RUN_DIR/results.tsv"
OPENWHISK_LOG="${OPENWHISK_LOG:-$COSMOS_PREFIX/benchmarks/logs/openwhisk-standalone.log}"
WARM_REPETITIONS="${WARM_REPETITIONS:-5}"
TOTAL_REPETITIONS="$((WARM_REPETITIONS + 1))"
INPUTS_RAW="${INPUTS:-test small large}"
START_OPENWHISK="${START_OPENWHISK:-1}"
STOP_OPENWHISK_ON_EXIT="${STOP_OPENWHISK_ON_EXIT:-$START_OPENWHISK}"

mkdir -p "$LOG_DIR"
printf 'timestamp\tworkload\tinput\tlanguage\tversion\trepetitions\texpected_cold\texpected_warm\tstatus\tseconds\tcold_count\twarm_count\tfailures\tsebs_output\tlog\n' >"$RESULTS"

declare -a CELLS=(
  '010.sleep nodejs 20'
  '020.network-benchmark python 3.11'
  '030.clock-synchronization python 3.11'
  '040.server-reply nodejs 20'
  '110.dynamic-html nodejs 20'
  '120.uploader nodejs 20'
  '130.crud-api nodejs 20'
  '210.thumbnailer nodejs 20'
  '220.video-processing python 3.11'
  '311.compression nodejs 20'
  '411.image-recognition python 3.11'
  '501.graph-pagerank python 3.11'
  '502.graph-mst python 3.11'
  '503.graph-bfs python 3.11'
  '504.dna-visualisation python 3.11'
)
read -r -a INPUTS_ARRAY <<<"$INPUTS_RAW"

cleanup() {
  docker ps -aq --filter name=wsk0_ | xargs -r docker rm -f >/dev/null 2>&1 || true
  if [[ "$STOP_OPENWHISK_ON_EXIT" == "1" ]]; then
    cosmos-openwhisk-stop >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

cd "$COSMOS_HOME"
if [[ "$START_OPENWHISK" == "1" ]]; then
  cosmos-openwhisk-stop >/dev/null 2>&1 || true
  cosmos-openwhisk-start >"$RUN_DIR/openwhisk-start.log" 2>&1
fi
if [[ -f "$COSMOS_PREFIX/benchmarks/openwhisk-host.env" ]]; then
  # shellcheck disable=SC1091
  source "$COSMOS_PREFIX/benchmarks/openwhisk-host.env"
fi
wsk --apihost "${OPENWHISK_APIHOST:-}" --auth "${OPENWHISK_AUTH:-}" property get >"$RUN_DIR/wsk-property.log" 2>&1 || true

if [[ -f "$OPENWHISK_LOG" ]]; then
  OW_START_LINE="$(($(wc -l <"$OPENWHISK_LOG") + 1))"
else
  OW_START_LINE=1
fi

parse_experiment() {
  local log_file="$1"
  local output_dir=""
  output_dir="$(sed -n 's/.*Created experiment output at //p' "$log_file" | tail -1)"
  if [[ -z "$output_dir" || ! -f "$output_dir/experiments.json" ]]; then
    printf '\t0\t0\t0'
    return
  fi
  python3 - "$output_dir/experiments.json" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    data = json.load(handle)

cold = 0
warm = 0
failures = 0
for invocations in data.get("_invocations", {}).values():
    for invocation in invocations.values():
        stats = invocation.get("stats", {})
        if stats.get("failure"):
            failures += 1
        if stats.get("cold_start") is True:
            cold += 1
        elif stats.get("cold_start") is False:
            warm += 1
print(f"{sys.argv[1].rsplit('/', 1)[0]}\t{cold}\t{warm}\t{failures}")
PY
}

echo "[$(date -Is)] cold/warm SeBS OpenWhisk matrix started run_dir=$RUN_DIR warm_repetitions=$WARM_REPETITIONS" | tee -a "$RUN_DIR/driver.log"

for input in "${INPUTS_ARRAY[@]}"; do
  for cell in "${CELLS[@]}"; do
    read -r workload lang version <<<"$cell"
    safe="${workload//./_}_${input}_${lang}${version}"
    safe="${safe//[^A-Za-z0-9_.-]/_}"
    log="$LOG_DIR/${safe}.log"
    start=$(date +%s)
    ts=$(date -Is)
    echo "[$ts] START workload=$workload input=$input lang=$lang version=$version repetitions=$TOTAL_REPETITIONS" | tee -a "$RUN_DIR/driver.log"

    docker ps -aq --filter name=wsk0_ | xargs -r docker rm -f >/dev/null 2>&1 || true
    if REPETITIONS="$TOTAL_REPETITIONS" cosmos-sebs-openwhisk-invoke "$workload" "$input" "$lang" "$version" >"$log" 2>&1; then
      status=ok
    else
      status=fail
    fi

    end=$(date +%s)
    parsed="$(parse_experiment "$log")"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t1\t%s\t%s\t%s\t%s\t%s\n' \
      "$(date -Is)" "$workload" "$input" "$lang" "$version" "$TOTAL_REPETITIONS" "$WARM_REPETITIONS" \
      "$status" "$((end - start))" "$parsed" "$log" >>"$RESULTS"
    echo "[$(date -Is)] END status=$status seconds=$((end - start)) log=$log" | tee -a "$RUN_DIR/driver.log"

    docker ps -aq --filter name=wsk0_ | xargs -r docker rm -f >/dev/null 2>&1 || true
  done
done

if [[ -f "$OPENWHISK_LOG" ]]; then
  tail -n +"$OW_START_LINE" "$OPENWHISK_LOG" >"$RUN_DIR/openwhisk-lifecycle.log" || true
  "$ROOT/benchmarks/profiler/scripts/parse_openwhisk_lifecycle.py" \
    "$RUN_DIR/openwhisk-lifecycle.log" \
    --tsv "$RUN_DIR/openwhisk_lifecycle.tsv" \
    --summary-json "$RUN_DIR/openwhisk_lifecycle_summary.json" || true
fi

echo "[$(date -Is)] cold/warm SeBS OpenWhisk matrix finished run_dir=$RUN_DIR" | tee -a "$RUN_DIR/driver.log"
