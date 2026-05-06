#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
OUT_DIR="${OUT_DIR:-$ROOT/benchmarks/runs}"
REPETITIONS="${REPETITIONS:-1}"
DURATION_MS="${DURATION_MS:-1000}"
SAMPLE_MS="${SAMPLE_MS:-50}"

cd "$ROOT"
cargo build -p cosmos-bench-profiler
cargo run -p cosmos-bench-profiler -- preflight --strict

for workload in cpu memory io network; do
  for rep in $(seq 1 "$REPETITIONS"); do
    sudo "$ROOT/target/debug/cosmos-bench-profiler" standalone \
      --out-dir "$OUT_DIR" \
      --name "standalone-${workload}-suite-r${rep}" \
      --workload "$workload" \
      --duration-ms "$DURATION_MS" \
      --sample-ms "$SAMPLE_MS"
  done
done

cargo run -p cosmos-bench-profiler -- profile-db \
  --runs-dir "$OUT_DIR" \
  --out "$ROOT/benchmarks/profile_db.json" \
  --strict
