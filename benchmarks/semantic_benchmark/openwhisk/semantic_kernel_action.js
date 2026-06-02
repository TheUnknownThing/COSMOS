const fs = require("fs");
const path = require("path");

const ACTION_TO_KERNEL = {
  "noop-dispatch-controllable": "semantic_control",
  "passive-wait-controllable": "semantic_passive_wait",
  "db-network-wait-controllable": "semantic_network_wait",
  "local-file-io-controllable": "semantic_local_io",
  "memory-touch-controllable": "semantic_memory_touch",
  "cpu-loop-controllable": "semantic_cpu_loop",
  "mixed-pipeline-controllable": "semantic_mixed_pipeline",
  "workflow-fanout-controllable": "semantic_workflow_fanout",
  "cpu-spin-controllable": "semantic_cpu_spin",
  "memory-scan-controllable": "semantic_memory_scan",
  "storage-io-controllable": "semantic_storage_io",
  "network-transfer-controllable": "semantic_network_transfer",
  "balanced-pipeline-controllable": "semantic_balanced_pipeline",
};

function actionName() {
  const raw = process.env.__OW_ACTION_NAME || "";
  return raw.split("/").filter(Boolean).pop() || "";
}

function kernelFor(args) {
  if (args.kernel) {
    const kernel = String(args.kernel);
    return ACTION_TO_KERNEL[kernel] || kernel;
  }
  if (args.duration_realization && ACTION_TO_KERNEL[args.duration_realization]) {
    return ACTION_TO_KERNEL[args.duration_realization];
  }
  if (args.payload && args.payload.duration_realization && ACTION_TO_KERNEL[args.payload.duration_realization]) {
    return ACTION_TO_KERNEL[args.payload.duration_realization];
  }
  const name = actionName();
  if (ACTION_TO_KERNEL[name]) return ACTION_TO_KERNEL[name];
  const workload = String(args.workload || "");
  if (ACTION_TO_KERNEL[workload]) return ACTION_TO_KERNEL[workload];
  throw new Error(`cannot infer semantic kernel for action=${name}`);
}

function targetUs(args) {
  const payload = args.payload || args.sebs_payload || {};
  const knobs = args.resource_knobs || payload.resource_knobs || {};
  const rawMs =
    args.target_duration_ms ||
    args.duration_ms ||
    payload.target_duration_ms ||
    knobs.target_duration_ms ||
    50;
  const ms = Math.max(1, Math.floor(Number(rawMs) || 50));
  return ms * 1000;
}

function extraArgs(args) {
  const payload = args.payload || args.sebs_payload || {};
  const knobs = args.resource_knobs || payload.resource_knobs || {};
  const result = [];
  if (knobs.working_set_size) {
    result.push("--working-set", String(Math.max(1, Math.floor(Number(knobs.working_set_size)))));
  }
  if (knobs.transfer_size) {
    result.push("--transfer-size", String(Math.max(1, Math.floor(Number(knobs.transfer_size)))));
  }
  if (knobs.fanout) {
    result.push("--fanout", String(Math.max(1, Math.floor(Number(knobs.fanout)))));
  }
  return result;
}

function main(args) {
  const kernel = path.join(__dirname, kernelFor(args || {}));
  fs.chmodSync(kernel, 0o755);
  const target = targetUs(args || {});
  const command = ["--target-us", String(target), ...extraArgs(args || {})];
  const started = Date.now();
  const child = require("child_process").spawnSync(kernel, command, {
    encoding: "utf8",
    timeout: Math.max(30000, Math.ceil(target / 1000) + 30000),
  });
  if (child.status !== 0) {
    throw new Error(child.stderr || `kernel failed with status ${child.status}`);
  }
  const kernelResult = JSON.parse(child.stdout);
  return {
    ok: true,
    benchmark: actionName() || path.basename(kernel),
    executable: path.basename(kernel),
    target_duration_ms: Math.round(target / 1000),
    elapsed_ms: Date.now() - started,
    kernel: kernelResult,
  };
}

exports.main = main;
