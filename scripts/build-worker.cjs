#!/usr/bin/env node
const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

const projectRoot = path.resolve(__dirname, "..");
const workerEntry = path.join(projectRoot, "python", "worker.py");
const workerDistDir = path.join(projectRoot, "build", "worker");
const pyinstallerRoot = path.join(projectRoot, "build", "pyinstaller");
const pyinstallerConfigDir = path.join(pyinstallerRoot, "config");

buildWorker();

function buildWorker() {
  const python = resolvePythonInvocation();

  fs.rmSync(workerDistDir, { recursive: true, force: true });
  fs.rmSync(pyinstallerRoot, { recursive: true, force: true });
  fs.mkdirSync(path.join(pyinstallerRoot, "work"), { recursive: true });
  fs.mkdirSync(path.join(pyinstallerRoot, "spec"), { recursive: true });
  fs.mkdirSync(pyinstallerConfigDir, { recursive: true });

  const recaptchaCtx = path.join(projectRoot, "python", "recaptcha_context.json");
  const addData = fs.existsSync(recaptchaCtx)
    ? [
        "--add-data",
        process.platform === "win32"
          ? `${recaptchaCtx};.`
          : `${recaptchaCtx}:.`,
      ]
    : [];

  const args = [
    ...python.prefix,
    "-m",
    "PyInstaller",
    "--noconfirm",
    "--clean",
    "--onedir",
    "--name",
    "worker",
    "--distpath",
    workerDistDir,
    "--workpath",
    path.join(pyinstallerRoot, "work"),
    "--specpath",
    path.join(pyinstallerRoot, "spec"),
    "--collect-all", "undetected_chromedriver",
    "--collect-all", "selenium",
    "--collect-all", "tzdata",
    "--collect-all", "pytz",
    "--collect-all", "certifi",
    "--collect-all", "curl_cffi",
    "--collect-all", "openpyxl",
    "--hidden-import", "curl_cffi",
    "--hidden-import", "aycd_inbox_api_client",
    ...addData,
    workerEntry,
  ];

  const result = spawnSync(python.command, args, {
    cwd: projectRoot,
    stdio: "inherit",
    env: { ...process.env, PYINSTALLER_CONFIG_DIR: pyinstallerConfigDir, PYTHONUTF8: "1" },
  });

  if (result.error) {
    console.error(`Failed to start PyInstaller: ${result.error.message}`);
    process.exit(1);
  }
  if (result.status !== 0) {
    console.error("PyInstaller failed. Install deps with:");
    console.error("  python -m pip install -r python/requirements-build.txt");
    process.exit(result.status || 1);
  }

  const builtWorker = getBuiltWorkerPath();
  if (!fs.existsSync(builtWorker)) {
    console.error(`PyInstaller finished, but the worker binary was not found at ${builtWorker}`);
    process.exit(1);
  }
  console.log(`Built worker: ${builtWorker}`);
}

function resolvePythonInvocation() {
  const explicitPython = resolveExplicitPython();
  const candidates =
    process.platform === "win32"
      ? [
          { command: path.join(projectRoot, ".venv", "Scripts", "python.exe"), prefix: [] },
          ...(explicitPython ? [{ command: explicitPython, prefix: [] }] : []),
          { command: "python", prefix: [] },
          { command: "py", prefix: ["-3"] },
        ]
      : [
          { command: path.join(projectRoot, ".venv", "bin", "python"), prefix: [] },
          ...(explicitPython ? [{ command: explicitPython, prefix: [] }] : []),
          { command: "python3", prefix: [] },
          { command: "python", prefix: [] },
        ];

  for (const c of candidates) {
    if (path.isAbsolute(c.command) && !fs.existsSync(c.command)) continue;
    const probe = spawnSync(c.command, [...c.prefix, "--version"], { stdio: "ignore", env: process.env });
    if (!probe.error && probe.status === 0) return c;
  }
  console.error("Could not find a usable Python interpreter.");
  process.exit(1);
}

function resolveExplicitPython() {
  const candidates = [
    process.env.DICEBOT_PYTHON,
    process.env.PYTHON,
    process.env.pythonLocation
      ? process.platform === "win32"
        ? path.join(process.env.pythonLocation, "python.exe")
        : path.join(process.env.pythonLocation, "bin", "python")
      : null,
  ];
  for (const c of candidates) {
    if (typeof c === "string" && c.trim()) return c.trim();
  }
  return null;
}

function getBuiltWorkerPath() {
  const exe = process.platform === "win32" ? "worker.exe" : "worker";
  return path.join(workerDistDir, "worker", exe);
}
