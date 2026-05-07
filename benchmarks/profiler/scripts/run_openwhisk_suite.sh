#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
OUT_DIR="${OUT_DIR:-$ROOT/benchmarks/runs}"
PROFILER="${PROFILER:-${CARGO_TARGET_DIR:-$ROOT/target}/debug/cosmos-bench-profiler}"
COSMOS_PREFIX="${COSMOS_PREFIX:-/usr/local/cosmos}"
JAVA_HOME="${JAVA_HOME:-/usr}"
PATH="$PATH:$JAVA_HOME/bin"
OW_DIR="$ROOT/benchmarks/third_party/openwhisk"
OW_LOG="${OW_LOG:-$COSMOS_PREFIX/benchmarks/logs/openwhisk-suite-standalone.log}"
ACTION="${ACTION:-cosmos_bench_suite_$$}"
REPETITIONS="${REPETITIONS:-1}"
SFS_WAS_ACTIVE=0
OW_PID=""

cleanup() {
  if [[ -n "$OW_PID" ]] && kill -0 "$OW_PID" 2>/dev/null; then
    kill "$OW_PID" 2>/dev/null || true
    wait "$OW_PID" 2>/dev/null || true
  fi
  docker ps -aq --filter name=wsk0_ --filter name=whisk- | xargs -r docker rm -f >/dev/null 2>&1 || true
  (cd "$OW_DIR" && JAVA_HOME="$JAVA_HOME" PATH="$PATH" ./gradlew --stop >/dev/null 2>&1 || true)
  if [[ "$SFS_WAS_ACTIVE" == "1" ]]; then
    sudo systemctl start sfs.service >/dev/null 2>&1 || sudo systemctl start sfs >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

cd "$ROOT"
cargo build -p cosmos-bench-profiler
cargo run -p cosmos-bench-profiler -- preflight --strict

if systemctl is-active --quiet sfs.service 2>/dev/null || systemctl is-active --quiet sfs 2>/dev/null; then
  SFS_WAS_ACTIVE=1
  sudo systemctl stop sfs.service >/dev/null 2>&1 || sudo systemctl stop sfs >/dev/null 2>&1 || true
fi

cd "$OW_DIR"
"$JAVA_HOME/bin/java" -version >/dev/null
./gradlew :core:standalone:build -x test

HOST_IP="$(ip -4 route get 8.8.8.8 | awk '/src/{for(i=1;i<=NF;i++) if($i=="src") print $(i+1)}' | head -1)"
"$JAVA_HOME/bin/java" \
  -Dwhisk.standalone.host.name="$HOST_IP" \
  -Dwhisk.standalone.host.ip="$HOST_IP" \
  -Dwhisk.standalone.host.internal="$HOST_IP" \
  -jar bin/openwhisk-standalone.jar \
  -c "$ROOT/benchmarks/profiler/configs/openwhisk-host-network.conf" \
  -m "$ROOT/benchmarks/profiler/configs/runtimes-no-prewarm.json" \
  --no-ui --dev-mode --data-dir "$COSMOS_PREFIX/benchmarks/openwhisk-suite-home" >"$OW_LOG" 2>&1 &
OW_PID="$!"

for _ in $(seq 1 90); do
  if curl -fsS "http://$HOST_IP:3233/api/v1" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
curl -fsS "http://$HOST_IP:3233/api/v1" >/dev/null

AUTH="${OPENWHISK_AUTH:-23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP}"
wsk property set --apihost "http://$HOST_IP:3233" --auth "$AUTH" >/dev/null
ACTION_FILE="$(mktemp /tmp/cosmos-openwhisk-action.XXXXXX.js)"
PARAM_FILE="$(mktemp /tmp/cosmos-openwhisk-params.XXXXXX.json)"
cat >"$ACTION_FILE" <<'JS'
function main(args) {
  return { ok: true, name: args.name || "benchmark" };
}
exports.main = main;
JS
printf '%s\n' '{"name":"benchmark"}' >"$PARAM_FILE"

cd "$ROOT"
for rep in $(seq 1 "$REPETITIONS"); do
  sudo -E env PATH="$PATH" "$PROFILER" open-whisk \
    --out-dir "$OUT_DIR" \
    --name "openwhisk-suite-r${rep}" \
    --action "$ACTION" \
    --file "$ACTION_FILE" \
    --kind nodejs:20 \
    --insecure \
    --apihost "http://$HOST_IP:3233" \
    --auth "$AUTH" \
    --invoke-http \
    --param-file "$PARAM_FILE" \
    --warmth warm
done

cargo run -p cosmos-bench-profiler -- profile-db \
  --runs-dir "$OUT_DIR" \
  --out "$ROOT/benchmarks/profile_db.json" \
  --strict
