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
cp "$KERNEL_DIR/semantic_kernel_static" "$ACTION_DIR/semantic_kernel"
chmod 755 "$ACTION_DIR/semantic_kernel"
python3 - "$ACTION_DIR" "$PACKAGE" <<'PY'
import sys
import zipfile
from pathlib import Path

action_dir = Path(sys.argv[1])
package = Path(sys.argv[2])
with zipfile.ZipFile(package, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for name, mode in (("index.js", 0o644), ("semantic_kernel", 0o755)):
        info = zipfile.ZipInfo(name)
        info.external_attr = (mode & 0xFFFF) << 16
        zf.writestr(info, (action_dir / name).read_bytes())
PY
rm -f "$ACTION_DIR/index.js" "$ACTION_DIR/semantic_kernel"

actions=(
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

"$WSK" action list | grep -E 'cpu-spin-controllable|memory-scan-controllable|storage-io-controllable|network-transfer-controllable|balanced-pipeline-controllable'
