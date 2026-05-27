#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
PROFILER="${PROFILER:-${CARGO_TARGET_DIR:-$ROOT/target}/debug/cosmos-bench-profiler}"
OUT_DIR="${OUT_DIR:-$ROOT/benchmarks/runs}"
REPETITIONS="${REPETITIONS:-1}"
SAMPLE_MS="${SAMPLE_MS:-50}"
INPUT_SIZE="${INPUT_SIZE:-test}"
MODE="${MODE:-burst}"
CONCURRENCY="${CONCURRENCY:-1}"
RATE="${RATE:-10}"
DURATION_S="${DURATION_S:-30}"

NODE_RUNNER="$ROOT/benchmarks/profiler/scripts/sebs_local_node_runner.js"
PYTHON_RUNNER="$ROOT/benchmarks/profiler/scripts/sebs_local_python_runner.py"
SEBS="$ROOT/benchmarks/third_party/serverless-benchmarks/benchmarks"

tmpdir="$(mktemp -d /tmp/cosmos-sebs-standalone.XXXXXX)"
cleanup() {
  rm -rf "$tmpdir"
}
trap cleanup EXIT

mode_args=()
case "$MODE" in
  burst)
    mode_args=(--mode burst --concurrency "$CONCURRENCY" --repetitions "$REPETITIONS")
    ;;
  continuous)
    mode_args=(--mode continuous --rate "$RATE" --concurrency "$CONCURRENCY" --duration-s "$DURATION_S")
    ;;
  throughput)
    mode_args=(--mode throughput --concurrency "$CONCURRENCY" --duration-s "$DURATION_S")
    ;;
  *)
    echo "unknown MODE: $MODE (expected burst, continuous, or throughput)" >&2
    exit 2
    ;;
esac

write_sleep_input() {
  local out="$1"
  case "$INPUT_SIZE" in
    test) printf '%s\n' '{"sleep":1}' >"$out" ;;
    small) printf '%s\n' '{"sleep":100}' >"$out" ;;
    large) printf '%s\n' '{"sleep":1000}' >"$out" ;;
    *) printf '%s\n' "unknown INPUT_SIZE for sleep: $INPUT_SIZE" >&2; return 2 ;;
  esac
}

write_dynamic_html_input() {
  local out="$1"
  case "$INPUT_SIZE" in
    test) printf '%s\n' '{"username":"testname","random_len":10}' >"$out" ;;
    small) printf '%s\n' '{"username":"testname","random_len":1000}' >"$out" ;;
    large) printf '%s\n' '{"username":"testname","random_len":100000}' >"$out" ;;
    *) printf '%s\n' "unknown INPUT_SIZE for dynamic-html: $INPUT_SIZE" >&2; return 2 ;;
  esac
}

ensure_node_deps() {
  local dir="$1"
  local name="$2"
  NODE_PATH_FOR_RUN=""
  if [[ -f "$dir/package.json" ]]; then
    local depdir="$tmpdir/node-deps/$name"
    mkdir -p "$depdir"
    cp "$dir/package.json" "$depdir/package.json"
    if [[ -f "$dir/package-lock.json" ]]; then
      cp "$dir/package-lock.json" "$depdir/package-lock.json"
    fi
    (cd "$depdir" && npm install --omit=dev)
    NODE_PATH_FOR_RUN="$depdir/node_modules"
  fi
}

ensure_python_deps() {
  local dir="$1"
  if [[ -f "$dir/requirements.txt" ]]; then
    pip3 install --quiet -r "$dir/requirements.txt" 2>/dev/null || true
  fi
}

cd "$ROOT"
cargo build -p cosmos-bench-profiler

echo "[$(date -Is)] SeBS standalone suite mode=$MODE concurrency=$CONCURRENCY input=$INPUT_SIZE" >&2

# ---- 010.sleep ----

for lang in nodejs python; do
  for rep in $(seq 1 "$REPETITIONS"); do
    sleep_input="$tmpdir/sleep-${lang}-${rep}.json"
    write_sleep_input "$sleep_input"
    if [[ "$lang" == "nodejs" ]]; then
      runner="$NODE_RUNNER"
      func="$SEBS/000.microbenchmarks/010.sleep/nodejs/function.js"
    else
      runner="$PYTHON_RUNNER"
      func="$SEBS/000.microbenchmarks/010.sleep/python/function.py"
      ensure_python_deps "$(dirname "$func")"
    fi
    sudo -E "$PROFILER" standalone \
      --out-dir "$OUT_DIR" \
      --name "sebs-010-sleep-${lang}-${MODE}-r${rep}" \
      --workload command \
      --workload-label "sebs-010.sleep-${lang}" \
      --input "$INPUT_SIZE" \
      --sample-ms "$SAMPLE_MS" \
      "${mode_args[@]}" \
      -- "$runner" "$func" "$sleep_input"
  done
done

# ---- 110.dynamic-html ----

for lang in nodejs python; do
  for rep in $(seq 1 "$REPETITIONS"); do
    dynamic_input="$tmpdir/dynamic-html-${lang}-${rep}.json"
    write_dynamic_html_input "$dynamic_input"
    if [[ "$lang" == "nodejs" ]]; then
      ensure_node_deps "$SEBS/100.webapps/110.dynamic-html/nodejs" "110.dynamic-html-nodejs"
      runner="$NODE_RUNNER"
      func="$SEBS/100.webapps/110.dynamic-html/nodejs/function.js"
      sudo -E "$PROFILER" standalone \
        --out-dir "$OUT_DIR" \
        --name "sebs-110-dynamic-html-${lang}-${MODE}-r${rep}" \
        --workload command \
        --workload-label "sebs-110.dynamic-html-${lang}" \
        --input "$INPUT_SIZE" \
        --sample-ms "$SAMPLE_MS" \
        "${mode_args[@]}" \
        -- env NODE_PATH="$NODE_PATH_FOR_RUN" "$runner" "$func" "$dynamic_input"
    else
      ensure_python_deps "$(dirname "$SEBS/100.webapps/110.dynamic-html/python/function.py")"
      runner="$PYTHON_RUNNER"
      func="$SEBS/100.webapps/110.dynamic-html/python/function.py"
      sudo -E "$PROFILER" standalone \
        --out-dir "$OUT_DIR" \
        --name "sebs-110-dynamic-html-${lang}-${MODE}-r${rep}" \
        --workload command \
        --workload-label "sebs-110.dynamic-html-${lang}" \
        --input "$INPUT_SIZE" \
        --sample-ms "$SAMPLE_MS" \
        "${mode_args[@]}" \
        -- "$runner" "$func" "$dynamic_input"
    fi
  done
done

echo "[$(date -Is)] SeBS standalone suite finished" >&2

# Build profile DB from all runs
if [[ -d "$OUT_DIR" ]]; then
  cargo run -p cosmos-bench-profiler -- profile-db \
    --runs-dir "$OUT_DIR" \
    --out "$ROOT/benchmarks/profile_db.json" \
    --strict || true
fi
