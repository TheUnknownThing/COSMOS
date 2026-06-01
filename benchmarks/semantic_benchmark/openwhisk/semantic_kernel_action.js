const { execFileSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const ACTION_TO_MODE = {
  "cpu-spin-controllable": "cpu",
  "memory-scan-controllable": "memory",
  "storage-io-controllable": "io",
  "network-transfer-controllable": "network",
  "balanced-pipeline-controllable": "balanced",
};

function actionName() {
  const raw = process.env.__OW_ACTION_NAME || "";
  return raw.split("/").filter(Boolean).pop() || "";
}

function modeFor(args) {
  if (args.mode) return String(args.mode);
  if (args.duration_realization && ACTION_TO_MODE[args.duration_realization]) {
    return ACTION_TO_MODE[args.duration_realization];
  }
  if (args.payload && args.payload.duration_realization && ACTION_TO_MODE[args.payload.duration_realization]) {
    return ACTION_TO_MODE[args.payload.duration_realization];
  }
  const name = actionName();
  if (ACTION_TO_MODE[name]) return ACTION_TO_MODE[name];
  const workload = String(args.workload || args.kernel || "");
  if (ACTION_TO_MODE[workload]) return ACTION_TO_MODE[workload];
  throw new Error(`cannot infer semantic kernel mode for action=${name}`);
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
  return result;
}

function main(args) {
  const kernel = path.join(__dirname, "semantic_kernel");
  fs.chmodSync(kernel, 0o755);
  const mode = modeFor(args || {});
  const target = targetUs(args || {});
  const command = ["--mode", mode, "--target-us", String(target), ...extraArgs(args || {})];
  const started = Date.now();
  const stdout = execFileSync(kernel, command, {
    encoding: "utf8",
    timeout: Math.max(30000, Math.ceil(target / 1000) + 30000),
  });
  const kernelResult = JSON.parse(stdout);
  return {
    ok: true,
    benchmark: actionName() || `semantic-kernel-${mode}`,
    mode,
    target_duration_ms: Math.round(target / 1000),
    elapsed_ms: Date.now() - started,
    kernel: kernelResult,
  };
}

exports.main = main;
