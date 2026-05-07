#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

async function main() {
  const [, , functionPathRaw, inputPathRaw] = process.argv;
  if (!functionPathRaw || !inputPathRaw) {
    throw new Error("usage: sebs_local_node_runner.js <function.js> <input.json>");
  }

  const functionPath = path.resolve(functionPathRaw);
  const inputPath = path.resolve(inputPathRaw);
  const input = JSON.parse(fs.readFileSync(inputPath, "utf8"));

  process.chdir(path.dirname(functionPath));
  const moduleExports = require(functionPath);
  const handler = moduleExports.handler || moduleExports.main;
  if (typeof handler !== "function") {
    throw new Error(`${functionPath} exports neither handler nor main`);
  }

  const output = await handler(input);
  process.stdout.write(`${JSON.stringify(output)}\n`);
}

main().catch((err) => {
  process.stderr.write(`${err.stack || err.message}\n`);
  process.exit(1);
});
