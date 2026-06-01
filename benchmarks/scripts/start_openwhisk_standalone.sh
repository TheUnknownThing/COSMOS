#!/usr/bin/env bash
set -euo pipefail

AUTH="${OPENWHISK_AUTH:-23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP}"
IMAGE="${OPENWHISK_STANDALONE_IMAGE:-openwhisk/standalone:nightly}"
CONTAINER="${OPENWHISK_STANDALONE_CONTAINER:-cosmos-ow-controller}"
BASE_DIR="${OPENWHISK_HOME:-/opt/OpenWhisk}"
BIN_DIR="$BASE_DIR/bin"
DATA_DIR="$BASE_DIR/data"
ACTION_MEMORY_MAX="${OPENWHISK_ACTION_MEMORY_MAX:-2048m}"
ACTION_MEMORY_STD="${OPENWHISK_ACTION_MEMORY_STD:-256m}"
INVOKER_USER_MEMORY="${OPENWHISK_INVOKER_USER_MEMORY:-4096m}"
INVOKES_PER_MINUTE="${OPENWHISK_INVOKES_PER_MINUTE:-6000}"
CONCURRENT_INVOKES="${OPENWHISK_CONCURRENT_INVOKES:-512}"
TRIGGERS_PER_MINUTE="${OPENWHISK_TRIGGERS_PER_MINUTE:-6000}"
export WSK_CONFIG_FILE="${WSK_CONFIG_FILE:-$BASE_DIR/wskprops}"

sudo mkdir -p "$BIN_DIR" "$DATA_DIR"
sudo chown -R "$(id -u):$(id -g)" "$BASE_DIR"

DOCKER=(docker)
if ! "${DOCKER[@]}" info >/dev/null 2>&1 && sudo docker info >/dev/null 2>&1; then
  DOCKER=(sudo docker)
fi

if ! "${DOCKER[@]}" info >/dev/null 2>&1; then
  sudo systemctl stop docker.socket docker.service >/dev/null 2>&1 || true
  sudo rm -f /run/docker.sock /var/run/docker.sock
  sudo nohup dockerd --host=unix:///var/run/docker.sock >/tmp/cosmos-dockerd.log 2>&1 &
  for _ in $(seq 1 30); do
    if docker info >/dev/null 2>&1; then
      DOCKER=(docker)
      break
    fi
    if sudo docker info >/dev/null 2>&1; then
      DOCKER=(sudo docker)
      break
    fi
    sleep 1
  done
fi
"${DOCKER[@]}" info >/dev/null

if [[ ! -x "$BIN_DIR/docker-static" ]]; then
  tmp="$(mktemp -d)"
  curl -fsSL -o "$tmp/docker.tgz" \
    https://download.docker.com/linux/static/stable/x86_64/docker-29.0.0.tgz
  tar -xzf "$tmp/docker.tgz" -C "$tmp" docker/docker
  install -m 0755 "$tmp/docker/docker" "$BIN_DIR/docker-static"
  rm -rf "$tmp"
fi

if [[ ! -x "$BIN_DIR/wsk" ]]; then
  tmp="$(mktemp -d)"
  curl -fsSL -o "$tmp/wsk.tgz" \
    https://github.com/apache/openwhisk-cli/releases/download/latest/OpenWhisk_CLI-latest-linux-amd64.tgz
  tar -xzf "$tmp/wsk.tgz" -C "$tmp"
  install -m 0755 "$tmp/wsk" "$BIN_DIR/wsk"
  rm -rf "$tmp"
fi

sudo tee /usr/local/bin/wsk >/dev/null <<'SH'
#!/usr/bin/env bash
export WSK_CONFIG_FILE="${WSK_CONFIG_FILE:-/opt/OpenWhisk/wskprops}"
exec /opt/OpenWhisk/bin/wsk "$@"
SH
sudo chmod 0755 /usr/local/bin/wsk

HOST_IP="${OPENWHISK_HOST_IP:-$(ip -4 route get 8.8.8.8 | awk '/src/{for(i=1;i<=NF;i++) if($i=="src") print $(i+1)}' | head -1)}"

"${DOCKER[@]}" rm -f "$CONTAINER" >/dev/null 2>&1 || true
"${DOCKER[@]}" run -d \
  --name "$CONTAINER" \
  --hostname openwhisk \
  --add-host "openwhisk:$HOST_IP" \
  --network host \
  -e DOCKER_HOST=unix:///var/run/docker.sock \
  -e JVM_EXTRA_ARGS="-Dwhisk.memory.max=$ACTION_MEMORY_MAX -Dwhisk.memory.std=$ACTION_MEMORY_STD -Dwhisk.namespace-default-limit.memory.max=$ACTION_MEMORY_MAX -Dwhisk.container-pool.user-memory=$INVOKER_USER_MEMORY -Dwhisk.config.limits-actions-invokes-perMinute=$INVOKES_PER_MINUTE -Dwhisk.config.limits-actions-invokes-concurrent=$CONCURRENT_INVOKES -Dwhisk.config.limits-triggers-fires-perMinute=$TRIGGERS_PER_MINUTE" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$BIN_DIR/docker-static:/usr/bin/docker:ro" \
  -v "$DATA_DIR:/data" \
  "$IMAGE" >/dev/null

for _ in $(seq 1 180); do
  if curl -fsS "http://$HOST_IP:3233/api/v1" >/dev/null 2>&1; then
    "$BIN_DIR/wsk" property set --apihost "http://$HOST_IP:3233" --auth "$AUTH" >/dev/null
    echo "OpenWhisk ready: http://$HOST_IP:3233"
    exit 0
  fi
  if ! "${DOCKER[@]}" ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    "${DOCKER[@]}" logs --tail 200 "$CONTAINER" >&2 || true
    exit 1
  fi
  sleep 2
done

"${DOCKER[@]}" logs --tail 200 "$CONTAINER" >&2 || true
echo "timed out waiting for OpenWhisk" >&2
exit 1
