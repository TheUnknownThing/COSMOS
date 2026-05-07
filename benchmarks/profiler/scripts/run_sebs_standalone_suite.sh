#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
OUT_DIR="${OUT_DIR:-$ROOT/benchmarks/runs}"
REPETITIONS="${REPETITIONS:-1}"
SAMPLE_MS="${SAMPLE_MS:-50}"
INPUT_SIZE="${INPUT_SIZE:-test}"
RUNNER="$ROOT/benchmarks/profiler/scripts/sebs_local_node_runner.js"
SEBS="$ROOT/benchmarks/third_party/serverless-benchmarks/benchmarks"

tmpdir="$(mktemp -d /tmp/cosmos-sebs-standalone.XXXXXX)"
cleanup() {
  rm -rf "$tmpdir"
}
trap cleanup EXIT

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

cd "$ROOT"
cargo build -p cosmos-bench-profiler
cargo run -p cosmos-bench-profiler -- preflight

for rep in $(seq 1 "$REPETITIONS"); do
  sleep_input="$tmpdir/sleep-${rep}.json"
  write_sleep_input "$sleep_input"
  sudo "$ROOT/target/debug/cosmos-bench-profiler" standalone \
    --out-dir "$OUT_DIR" \
    --name "sebs-010-sleep-nodejs-r${rep}" \
    --workload command \
    --workload-label sebs-010.sleep-nodejs \
    --input "$INPUT_SIZE" \
    --sample-ms "$SAMPLE_MS" \
    -- "$RUNNER" \
    "$SEBS/000.microbenchmarks/010.sleep/nodejs/function.js" \
    "$sleep_input"

  dynamic_input="$tmpdir/dynamic-html-${rep}.json"
  write_dynamic_html_input "$dynamic_input"
  ensure_node_deps "$SEBS/100.webapps/110.dynamic-html/nodejs" "110.dynamic-html-nodejs"
  sudo "$ROOT/target/debug/cosmos-bench-profiler" standalone \
    --out-dir "$OUT_DIR" \
    --name "sebs-110-dynamic-html-nodejs-r${rep}" \
    --workload command \
    --workload-label sebs-110.dynamic-html-nodejs \
    --input "$INPUT_SIZE" \
    --sample-ms "$SAMPLE_MS" \
    -- env NODE_PATH="$NODE_PATH_FOR_RUN" "$RUNNER" \
    "$SEBS/100.webapps/110.dynamic-html/nodejs/function.js" \
    "$dynamic_input"
done

cargo run -p cosmos-bench-profiler -- profile-db \
  --runs-dir "$OUT_DIR" \
  --out "$ROOT/benchmarks/profile_db.json" \
  --strict
