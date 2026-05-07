#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
OUT_DIR="${OUT_DIR:-$ROOT/benchmarks/runs}"
ACTION="${ACTION:-cosmos_concurrent_load}"
KIND="${KIND:-nodejs:20}"
REQUESTS="${REQUESTS:-256}"
CONCURRENCY="${CONCURRENCY:-64}"
ACTION_CONCURRENCY="${ACTION_CONCURRENCY:-$REQUESTS}"
BURN_MS="${BURN_MS:-25}"
TIMEOUT_S="${TIMEOUT_S:-120}"
WARMUP_REQUESTS="${WARMUP_REQUESTS:-1}"
COSMOS_PREFIX="${COSMOS_PREFIX:-/usr/local/cosmos}"
OPENWHISK_ENV_FILE="${OPENWHISK_ENV_FILE:-$COSMOS_PREFIX/benchmarks/openwhisk-host.env}"
if [[ -f "$OPENWHISK_ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  . "$OPENWHISK_ENV_FILE"
fi
APIHOST="${APIHOST:-${OPENWHISK_APIHOST:-}}"
AUTH="${AUTH:-${OPENWHISK_AUTH:-}}"
WSK_INSECURE="${WSK_INSECURE:-1}"
LOAD_INSECURE="${LOAD_INSECURE:-0}"

cd "$ROOT"

if [[ -z "$APIHOST" ]]; then
  APIHOST="$(wsk property get --apihost | awk '{print $3}')"
fi
if [[ -z "$AUTH" ]]; then
  AUTH="$(wsk property get --auth | awk '{print $3}')"
fi
if [[ -z "$APIHOST" || -z "$AUTH" ]]; then
  echo "OpenWhisk API host/auth are required. Set APIHOST/AUTH or configure wsk properties." >&2
  exit 2
fi

ACTION_FILE="$(mktemp "${TMPDIR:-/tmp}/cosmos-openwhisk-concurrent-action.XXXXXX.js")"
PARAM_FILE="$(mktemp "${TMPDIR:-/tmp}/cosmos-openwhisk-concurrent-params.XXXXXX.json")"
cleanup() {
  rm -f "$ACTION_FILE" "$PARAM_FILE"
}
trap cleanup EXIT

cat >"$ACTION_FILE" <<'JS'
function main(args) {
  const burnMs = Number(args.burn_ms || 25);
  const requestId = args.request_id || "";
  const start = Date.now();
  let value = 0;
  while (Date.now() - start < burnMs) {
    value = (value + Math.sqrt(value + 1.25)) % 1000003;
  }
  return {
    ok: true,
    request_id: requestId,
    burn_ms: burnMs,
    value: value
  };
}
exports.main = main;
JS

printf '{"burn_ms":%s}\n' "$BURN_MS" >"$PARAM_FILE"

wsk_args=(--apihost "$APIHOST" --auth "$AUTH")
if [[ "$WSK_INSECURE" == "1" ]]; then
  wsk_args=(-i "${wsk_args[@]}")
fi
wsk "${wsk_args[@]}" action update "$ACTION" "$ACTION_FILE" --kind "$KIND" --concurrency "$ACTION_CONCURRENCY" >/dev/null

load_args=(
  "$ROOT/benchmarks/profiler/scripts/openwhisk_concurrent_load.py"
  --apihost "$APIHOST"
  --auth "$AUTH"
  --action "$ACTION"
  --out-dir "$OUT_DIR"
  --requests "$REQUESTS"
  --concurrency "$CONCURRENCY"
  --timeout-s "$TIMEOUT_S"
  --warmup-requests "$WARMUP_REQUESTS"
  --param-file "$PARAM_FILE"
)
if [[ "$LOAD_INSECURE" == "1" ]]; then
  load_args+=(--insecure)
fi

python3 "${load_args[@]}"
