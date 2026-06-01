#!/usr/bin/env bash
set -euo pipefail

COSMOS_ROOT="${COSMOS_ROOT:-/opt/COSMOS}"
SEBS_ROOT="${SEBS_ROOT:-$COSMOS_ROOT/benchmarks/third_party/serverless-benchmarks}"
SEBS_VENV="${SEBS_VENV:-/opt/sebs-venv}"
SEBS_STATE="${SEBS_STATE:-/opt/sebs}"
STORAGE_BASE="$SEBS_STATE/storage-base.json"
STORAGE_JSON="$SEBS_STATE/storage.json"
OPENWHISK_CONFIG="$SEBS_STATE/openwhisk.json"

HOST_IP="${SEBS_STORAGE_HOST_IP:-$(ip -4 route get 8.8.8.8 | awk '/src/{for(i=1;i<=NF;i++) if($i=="src") print $(i+1)}' | head -1)}"
MINIO_PORT="${SEBS_MINIO_PORT:-9011}"
SCYLLA_PORT="${SEBS_SCYLLA_PORT:-9012}"
REGISTRY_HOST="${SEBS_REGISTRY_HOST:-localhost}"
REGISTRY_PORT="${SEBS_REGISTRY_PORT:-5000}"
REGISTRY_ADDR="$REGISTRY_HOST:$REGISTRY_PORT"

sudo mkdir -p "$SEBS_STATE" "$SEBS_VENV" "$SEBS_STATE/minio-volume"
sudo chown -R "$(id -u):$(id -g)" "$SEBS_STATE" "$SEBS_VENV"

DOCKER=(docker)
if ! "${DOCKER[@]}" info >/dev/null 2>&1 && sudo docker info >/dev/null 2>&1; then
  DOCKER=(sudo docker)
fi

if ! "${DOCKER[@]}" info >/dev/null 2>&1; then
  echo "docker is not accessible by this user; re-login after docker group changes or run from a docker-enabled account" >&2
  exit 1
fi

if [[ ! -x "$SEBS_VENV/bin/sebs" ]]; then
  python3 -m venv "$SEBS_VENV"
  "$SEBS_VENV/bin/python" -m pip install -U pip setuptools wheel
  "$SEBS_VENV/bin/python" -m pip install -e "$SEBS_ROOT"
fi

if ! "${DOCKER[@]}" ps --format '{{.Names}}' | grep -qx cosmos-sebs-registry; then
  "${DOCKER[@]}" rm -f cosmos-sebs-registry >/dev/null 2>&1 || true
  "${DOCKER[@]}" run -d \
    --restart unless-stopped \
    --name cosmos-sebs-registry \
    -p "$REGISTRY_PORT:5000" \
    registry:2 >/dev/null
fi

cat > "$STORAGE_BASE" <<JSON
{
  "object": {
    "type": "minio",
    "minio": {
      "mapped_port": $MINIO_PORT,
      "version": "RELEASE.2024-07-16T23-46-41Z",
      "data_volume": "$SEBS_STATE/minio-volume"
    }
  },
  "nosql": {
    "type": "scylladb",
    "scylladb": {
      "mapped_port": $SCYLLA_PORT,
      "version": "6.0",
      "cpus": 1,
      "memory": "750",
      "data_volume": "cosmos-sebs-scylladb-volume"
    }
  }
}
JSON

minio_live() {
  curl -fsS "http://$HOST_IP:$MINIO_PORT/minio/health/live" >/dev/null 2>&1
}

scylla_live() {
  curl -fsS "http://$HOST_IP:$SCYLLA_PORT" >/dev/null 2>&1
}

if [[ ! -s "$STORAGE_JSON" ]] || ! minio_live || ! scylla_live; then
  "$SEBS_VENV/bin/sebs" storage start all "$STORAGE_BASE" \
    --output-json "$STORAGE_JSON" \
    --no-remove-containers
fi

tmp="$(mktemp)"
jq \
  --arg minio "$HOST_IP:$MINIO_PORT" \
  --arg scylla "$HOST_IP:$SCYLLA_PORT" \
  '.object.minio.address = $minio | .nosql.scylladb.address = $scylla' \
  "$STORAGE_JSON" > "$tmp"
mv "$tmp" "$STORAGE_JSON"

jq \
  --slurpfile storage "$STORAGE_JSON" \
  '.experiments.update_storage = true
   | .deployment.openwhisk.wskExec = "/usr/local/bin/wsk"
   | .deployment.openwhisk.wskBypassSecurity = "true"
   | .deployment.openwhisk.docker_registry.registry = "'"$REGISTRY_ADDR"'"
   | .deployment.openwhisk.docker_registry.username = ""
   | .deployment.openwhisk.docker_registry.password = ""
   | .deployment.openwhisk.storage = $storage[0]' \
  "$SEBS_ROOT/configs/openwhisk.json" > "$OPENWHISK_CONFIG"

minio_live
scylla_live

echo "SeBS storage ready:"
echo "  storage: $STORAGE_JSON"
echo "  config:  $OPENWHISK_CONFIG"
echo "  registry: $REGISTRY_ADDR"
