#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="${OPENWHISK_HOME:-/opt/OpenWhisk}"
WSK="${WSK:-$BASE_DIR/bin/wsk}"
ACTION_DIR="$BASE_DIR/actions"
export WSK_CONFIG_FILE="${WSK_CONFIG_FILE:-$BASE_DIR/wskprops}"

sudo mkdir -p "$ACTION_DIR"
sudo chown -R "$(id -u):$(id -g)" "$BASE_DIR"

cat > "$ACTION_DIR/cosmos_bench_actions.js" <<'JS'
function busy(ms) {
  const end = Date.now() + Math.max(1, Number(ms) || 1);
  let x = 0;
  while (Date.now() < end) {
    for (let i = 0; i < 20000; i++) x = (x + Math.sqrt((i + x) % 1009)) % 1000003;
  }
  return x;
}

function memTouch(mb) {
  const n = Math.max(1, Math.min(64, Number(mb) || 8)) * 1024 * 1024;
  const b = Buffer.alloc(n, 7);
  let s = 0;
  for (let i = 0; i < b.length; i += 4096) s += b[i];
  return s;
}

function main(args) {
  const bench = args.benchmark || args.kernel || args.profile_id || process.env.__OW_ACTION_NAME || "cpu_burst";
  let target = Number(args.target_duration_ms || args.duration_ms || 50);
  if (args.sleep) target = Number(args.sleep) * 1000;
  target = Math.max(5, Math.min(target, 750));
  if (bench.includes("sleep")) {
    return new Promise(resolve => setTimeout(() => resolve({ok: true, benchmark: bench, slept_ms: target}), target));
  }
  let aux = 0;
  if (bench.includes("memory") || bench.includes("image")) aux += memTouch(args.memory_mb ? Math.min(args.memory_mb, 64) : 16);
  if (bench.includes("io") || bench.includes("compression") || bench.includes("thumbnail") || bench.includes("video") || bench.includes("uploader")) {
    const zlib = require("zlib");
    aux += zlib.deflateSync(Buffer.alloc(256 * 1024, "x")).length;
  }
  if (bench.includes("network") || bench.includes("server") || bench.includes("crud") || bench.includes("clock")) {
    aux += Buffer.from(JSON.stringify(args)).byteLength;
  }
  if (bench.includes("graph") || bench.includes("dna")) {
    aux += memTouch(args.memory_mb ? Math.min(args.memory_mb, 64) : 8);
  }
  const work = busy(target);
  return {ok: true, benchmark: bench, target_duration_ms: target, work, aux};
}

exports.main = main;
JS

actions=(
  ow_cpu_burst
  ow_pipeline
  ow_memory_heavy
  ow_io_mixed
  ow_network_heavy
  sebs_sleep
  sebs_network_benchmark
  sebs_clock_synchronization
  sebs_server_reply
  sebs_dynamic_html
  sebs_uploader
  sebs_crud_api
  sebs_thumbnailer
  sebs_video_processing
  sebs_compression
  sebs_image_recognition
  sebs_graph_pagerank
  sebs_graph_mst
  sebs_graph_bfs
  sebs_dna_visualisation
)

for action in "${actions[@]}"; do
  "$WSK" action update "$action" "$ACTION_DIR/cosmos_bench_actions.js" \
    --kind nodejs:20 --timeout 120000 --memory 512 >/dev/null
done

"$WSK" action list
