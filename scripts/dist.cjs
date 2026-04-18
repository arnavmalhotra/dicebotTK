#!/usr/bin/env node
const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

const projectRoot = path.resolve(__dirname, "..");
const buildWorkerScript = path.join(projectRoot, "scripts", "build-worker.cjs");
const electronBuilderCli = path.join(projectRoot, "node_modules", "electron-builder", "cli.js");
const electronCacheDir = path.join(projectRoot, "build", "electron-cache");
const electronBuilderCacheDir = path.join(projectRoot, "build", "electron-builder-cache");

run();

function run() {
  const target = process.argv[2] || defaultTarget();

  runNodeScript(buildWorkerScript);
  fs.mkdirSync(electronCacheDir, { recursive: true });
  fs.mkdirSync(electronBuilderCacheDir, { recursive: true });

  if (!fs.existsSync(electronBuilderCli)) {
    console.error("electron-builder is not installed. Run `npm install` first.");
    process.exit(1);
  }

  const args = resolveBuilderArgs(target);
  const publish = process.env.GH_TOKEN ? "always" : "never";
  const result = spawnSync(process.execPath, [electronBuilderCli, "--publish", publish, ...args], {
    cwd: projectRoot,
    stdio: "inherit",
    env: {
      ...process.env,
      ELECTRON_CACHE: electronCacheDir,
      ELECTRON_BUILDER_CACHE: electronBuilderCacheDir,
    },
  });

  if (result.error) { console.error(`Failed to launch electron-builder: ${result.error.message}`); process.exit(1); }
  process.exit(result.status || 0);
}

function runNodeScript(scriptPath) {
  const result = spawnSync(process.execPath, [scriptPath], { cwd: projectRoot, stdio: "inherit", env: process.env });
  if (result.error) { console.error(`Failed: ${result.error.message}`); process.exit(1); }
  if (result.status !== 0) process.exit(result.status || 1);
}

function defaultTarget() {
  if (process.platform === "darwin") return "mac";
  if (process.platform === "win32") return "win";
  return "dir";
}

function resolveBuilderArgs(target) {
  if (target === "mac") return ["--mac", "dmg", "zip"];
  if (target === "win") return ["--win", "nsis"];
  if (target === "dir") return ["--dir"];
  console.error(`Unsupported target: ${target}`);
  process.exit(1);
}
