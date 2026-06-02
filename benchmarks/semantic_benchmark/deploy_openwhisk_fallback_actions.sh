#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KERNEL_DIR="$SCRIPT_DIR/kernels"
ACTION_DIR="$SCRIPT_DIR/openwhisk"
WSK="${WSK:-wsk}"
PACKAGE="${TMPDIR:-/tmp}/cosmos-semantic-kernel-action.zip"

make -C "$KERNEL_DIR" static >/dev/null

rm -f "$PACKAGE"
cp "$ACTION_DIR/semantic_kernel_action.js" "$ACTION_DIR/index.js"
for static_kernel in "$KERNEL_DIR"/semantic_*_static; do
  kernel_name="$(basename "$static_kernel" _static)"
  cp "$static_kernel" "$ACTION_DIR/$kernel_name"
  chmod 755 "$ACTION_DIR/$kernel_name"
done
python3 - "$ACTION_DIR" "$PACKAGE" <<'PY'
import sys
import zipfile
from pathlib import Path

action_dir = Path(sys.argv[1])
package = Path(sys.argv[2])
with zipfile.ZipFile(package, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    entries = [("index.js", 0o644)]
    entries.extend((path.name, 0o755) for path in sorted(action_dir.glob("semantic_*")))
    for name, mode in entries:
        info = zipfile.ZipInfo(name)
        info.external_attr = (mode & 0xFFFF) << 16
        zf.writestr(info, (action_dir / name).read_bytes())
PY
rm -f "$ACTION_DIR/index.js"
find "$ACTION_DIR" -maxdepth 1 -type f -name 'semantic_*' ! -name 'semantic_kernel_action.js' -delete

actions=(
  noop-dispatch-controllable
  passive-wait-controllable
  db-network-wait-controllable
  local-file-io-controllable
  memory-touch-controllable
  cpu-loop-controllable
  mixed-pipeline-controllable
  workflow-fanout-controllable
  cpu-spin-controllable
  memory-scan-controllable
  storage-io-controllable
  network-transfer-controllable
  balanced-pipeline-controllable
)

for action in "${actions[@]}"; do
  "$WSK" action update "$action" "$PACKAGE" \
    --kind nodejs:20 \
    --timeout 120000 \
    --memory 512 >/dev/null
done

"$WSK" action list | grep -E 'noop-dispatch-controllable|passive-wait-controllable|db-network-wait-controllable|local-file-io-controllable|memory-touch-controllable|cpu-loop-controllable|mixed-pipeline-controllable|workflow-fanout-controllable|cpu-spin-controllable|memory-scan-controllable|storage-io-controllable|network-transfer-controllable|balanced-pipeline-controllable'
