const { app, BrowserWindow, ipcMain, dialog, shell } = require("electron");
const { spawn } = require("node:child_process");
const path = require("node:path");
const fs = require("node:fs");
const readline = require("node:readline");
const { Readable } = require("node:stream");
const { pipeline } = require("node:stream/promises");

let mainWindow = null;
let pyProc = null;
let pyReady = false;
const pending = new Map();
let nextId = 1;
const readyWaiters = [];
let updateCheckTimer = null;
let updateCheckInFlight = false;
let pendingInstallerPath = null;
let currentDownloadPromise = null;
let updateState = {
  status: "idle",
  currentVersion: app.getVersion(),
  latestVersion: "",
  mode: process.platform,
  downloadUrl: "",
  extraDownloadUrl: "",
  releasePageUrl: "",
  error: "",
};

const UPDATE_CHECK_INTERVAL_MS = 6 * 60 * 60 * 1000;
const UPDATE_CONFIG = {
  appId: "dicebotticketkings",
  productName: "DiceBotTicketKings",
  manifestUrl: "https://dl.tieroneonly.com/latest/DiceBotTicketKings-update.json",
  windowsUrl: "https://dl.tieroneonly.com/latest/DiceBotTicketKings-win.exe",
  macDmgUrl: "https://dl.tieroneonly.com/latest/DiceBotTicketKings-mac.dmg",
  macZipUrl: "https://dl.tieroneonly.com/latest/DiceBotTicketKings-mac.zip",
};

function setUpdateState(patch) {
  updateState = {
    ...updateState,
    ...patch,
    currentVersion: app.getVersion(),
  };
  if (mainWindow && !mainWindow.isDestroyed() && mainWindow.webContents) {
    mainWindow.webContents.send("update:event", updateState);
  }
}

function normalizeVersion(value) {
  return String(value || "").trim().replace(/^v/i, "");
}

function compareVersions(a, b) {
  const left = normalizeVersion(a).split(".").map((part) => parseInt(part, 10) || 0);
  const right = normalizeVersion(b).split(".").map((part) => parseInt(part, 10) || 0);
  const len = Math.max(left.length, right.length);
  for (let i = 0; i < len; i += 1) {
    const diff = (left[i] || 0) - (right[i] || 0);
    if (diff !== 0) return diff;
  }
  return 0;
}

async function fetchUpdateManifest() {
  const response = await fetch(UPDATE_CONFIG.manifestUrl, {
    headers: {
      Accept: "application/json",
      "User-Agent": `${UPDATE_CONFIG.productName}/${app.getVersion()}`,
    },
  });
  if (!response.ok) {
    throw new Error(`Update manifest request failed (${response.status})`);
  }
  const data = await response.json();
  return {
    version: normalizeVersion(data.version || data.release_tag || ""),
    releasePageUrl: data.release_page_url || "",
    windowsUrl: data.windows?.url || data.windows_url || UPDATE_CONFIG.windowsUrl,
    macDmgUrl: data.mac?.dmg_url || data.mac_dmg_url || UPDATE_CONFIG.macDmgUrl,
    macZipUrl: data.mac?.zip_url || data.mac_zip_url || UPDATE_CONFIG.macZipUrl,
  };
}

async function downloadWindowsInstaller(version, downloadUrl) {
  const updatesDir = path.join(app.getPath("userData"), "updates");
  fs.mkdirSync(updatesDir, { recursive: true });
  const targetPath = path.join(updatesDir, `${UPDATE_CONFIG.appId}-${normalizeVersion(version)}-win.exe`);
  const tempPath = `${targetPath}.download`;
  if (fs.existsSync(targetPath)) {
    return targetPath;
  }

  const response = await fetch(downloadUrl, {
    headers: {
      "User-Agent": `${UPDATE_CONFIG.productName}/${app.getVersion()}`,
    },
  });
  if (!response.ok || !response.body) {
    throw new Error(`Installer download failed (${response.status})`);
  }

  await pipeline(Readable.fromWeb(response.body), fs.createWriteStream(tempPath));
  fs.renameSync(tempPath, targetPath);
  return targetPath;
}

async function checkForAppUpdates() {
  if (updateCheckInFlight) return;
  updateCheckInFlight = true;
  setUpdateState({ status: "checking", error: "" });
  try {
    const release = await fetchUpdateManifest();
    if (!release.version || compareVersions(release.version, app.getVersion()) <= 0) {
      setUpdateState({
        status: "idle",
        latestVersion: release.version || "",
        releasePageUrl: release.releasePageUrl || "",
        downloadUrl: "",
        extraDownloadUrl: "",
        error: "",
      });
      return;
    }

    if (process.platform === "win32") {
      const baseState = {
        latestVersion: release.version,
        releasePageUrl: release.releasePageUrl || "",
        mode: "windows",
        downloadUrl: release.windowsUrl || UPDATE_CONFIG.windowsUrl,
        extraDownloadUrl: "",
        error: "",
      };
      if (!app.isPackaged) {
        setUpdateState({ ...baseState, status: "available" });
        return;
      }
      setUpdateState({ ...baseState, status: "downloading" });
      currentDownloadPromise = downloadWindowsInstaller(release.version, baseState.downloadUrl);
      const installerPath = await currentDownloadPromise;
      pendingInstallerPath = installerPath;
      setUpdateState({ ...baseState, status: "downloaded" });
      return;
    }

    if (process.platform === "darwin") {
      setUpdateState({
        status: "available",
        latestVersion: release.version,
        releasePageUrl: release.releasePageUrl || "",
        mode: "mac",
        downloadUrl: release.macDmgUrl || UPDATE_CONFIG.macDmgUrl,
        extraDownloadUrl: release.macZipUrl || UPDATE_CONFIG.macZipUrl,
        error: "",
      });
      return;
    }

    setUpdateState({
      status: "available",
      latestVersion: release.version,
      releasePageUrl: release.releasePageUrl || "",
      mode: process.platform,
      downloadUrl: release.releasePageUrl || "",
      extraDownloadUrl: "",
      error: "",
    });
  } catch (err) {
    console.error("[update] check failed:", err);
    if (updateState.latestVersion) {
      setUpdateState({
        status: "available",
        error: err.message || "Update check failed",
      });
    } else {
      setUpdateState({
        status: "idle",
        error: "",
      });
    }
  } finally {
    currentDownloadPromise = null;
    updateCheckInFlight = false;
  }
}

function scheduleUpdateChecks() {
  checkForAppUpdates();
  if (updateCheckTimer) clearInterval(updateCheckTimer);
  updateCheckTimer = setInterval(checkForAppUpdates, UPDATE_CHECK_INTERVAL_MS);
}

async function installDownloadedUpdate() {
  if (process.platform !== "win32") {
    throw new Error("Install is only available on Windows");
  }
  if (currentDownloadPromise) {
    await currentDownloadPromise;
  }
  if (!pendingInstallerPath || !fs.existsSync(pendingInstallerPath)) {
    throw new Error("No downloaded update is ready yet");
  }

  app.once("will-quit", () => {
    try {
      const child = spawn(pendingInstallerPath, [], {
        detached: true,
        stdio: "ignore",
      });
      child.unref();
    } catch (err) {
      console.error("[update] failed to launch installer:", err);
    }
  });
  app.quit();
}

// ── Python worker location ────────────────────────────────────────────────
function resolveWorker() {
  if (app.isPackaged) {
    const exeName = process.platform === "win32" ? "worker.exe" : "worker";
    return {
      command: path.join(process.resourcesPath, "bin", "worker", exeName),
      args: [],
      shell: false,
    };
  }

  const explicit =
    process.env.DICEBOT_PYTHON ||
    process.env.PYTHON ||
    (process.platform === "win32"
      ? path.join(__dirname, "..", ".venv", "Scripts", "python.exe")
      : path.join(__dirname, "..", ".venv", "bin", "python"));

  const workerScript = path.join(__dirname, "..", "python", "worker.py");
  const python = fs.existsSync(explicit)
    ? explicit
    : process.platform === "win32"
    ? "python"
    : "python3";
  return { command: python, args: [workerScript], shell: false };
}

function startPython() {
  const spec = resolveWorker();
  // In packaged builds __dirname lives inside app.asar — joining ".." gives a
  // virtual path that doesn't exist on disk, which Windows rejects as cwd.
  // Use the worker's own real directory under resourcesPath instead.
  const cwd = app.isPackaged
    ? path.dirname(spec.command)
    : path.join(__dirname, "..");
  pyProc = spawn(spec.command, spec.args, {
    cwd,
    env: { ...process.env, PYTHONUTF8: "1", PYTHONUNBUFFERED: "1" },
    stdio: ["pipe", "pipe", "pipe"],
  });

  pyProc.on("error", (err) => {
    console.error("[worker] spawn error:", err);
    if (mainWindow) mainWindow.webContents.send("worker:log", { level: "error", message: `Worker spawn failed: ${err.message}` });
  });

  pyProc.on("exit", (code, signal) => {
    console.error(`[worker] exited code=${code} signal=${signal}`);
    pyReady = false;
    pending.forEach(({ reject }) => reject(new Error("worker exited")));
    pending.clear();
  });

  const stdoutRl = readline.createInterface({ input: pyProc.stdout });
  stdoutRl.on("line", (line) => {
    if (!line) return;
    let msg;
    try {
      msg = JSON.parse(line);
    } catch {
      console.error("[worker] non-JSON:", line);
      return;
    }
    if (msg.type === "ready") {
      pyReady = true;
      readyWaiters.splice(0).forEach((fn) => fn());
      return;
    }
    if (typeof msg.id === "number") {
      const entry = pending.get(msg.id);
      if (entry) {
        pending.delete(msg.id);
        if (msg.ok) entry.resolve(msg.data);
        else entry.reject(new Error(msg.error || "worker error"));
      }
      return;
    }
    // Event to renderer
    if (mainWindow) mainWindow.webContents.send("worker:event", msg);
  });

  pyProc.stderr.on("data", (buf) => {
    const text = buf.toString();
    console.error("[worker stderr]", text);
    if (mainWindow) mainWindow.webContents.send("worker:log", { level: "error", message: text });
  });
}

function waitForReady() {
  if (pyReady) return Promise.resolve();
  return new Promise((resolve) => readyWaiters.push(resolve));
}

async function rpc(method, params = {}) {
  await waitForReady();
  if (!pyProc || pyProc.killed) throw new Error("worker not running");
  const id = nextId++;
  const payload = JSON.stringify({ id, method, params }) + "\n";
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    pyProc.stdin.write(payload, (err) => {
      if (err) {
        pending.delete(id);
        reject(err);
      }
    });
  });
}

// ── Window ────────────────────────────────────────────────────────────────
function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1280,
    height: 820,
    minWidth: 1100,
    minHeight: 700,
    title: "DiceBotTK",
    backgroundColor: "#000000",
    webPreferences: {
      preload: path.join(__dirname, "preload.js"),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });
  mainWindow.setMenuBarVisibility(false);
  mainWindow.loadFile(path.join(__dirname, "index.html"));
  mainWindow.webContents.on("did-finish-load", () => {
    mainWindow?.webContents.send("update:event", updateState);
  });
  mainWindow.on("closed", () => {
    mainWindow = null;
  });
}

// ── IPC wiring ────────────────────────────────────────────────────────────
function registerIpc() {
  const passthrough = [
    ["db:get-groups", "db.get_groups"],
    ["db:create-group", "db.create_group"],
    ["db:delete-group", "db.delete_group"],
    ["db:rename-group", "db.rename_group"],
    ["db:get-accounts", "db.get_accounts"],
    ["db:get-account", "db.get_account"],
    ["db:add-account", "db.add_account"],
    ["db:update-account", "db.update_account"],
    ["db:delete-account", "db.delete_account"],
    ["db:assign-group", "db.assign_group"],
    ["db:get-stats", "db.get_stats"],
    ["db:get-accounts-needing-auth", "db.get_accounts_needing_auth"],
    ["db:get-accounts-with-valid-session", "db.get_accounts_with_valid_session"],
    ["db:get-session", "db.get_session"],
    ["db:get-tasks", "db.get_tasks"],
    ["db:get-task", "db.get_task"],
    ["db:create-task", "db.create_task"],
    ["db:update-task", "db.update_task"],
    ["db:delete-task", "db.delete_task"],
    ["db:get-inventory", "db.get_inventory"],
    ["db:delete-inventory-item", "db.delete_inventory_item"],
    ["auth:login-one", "auth.login_one"],
    ["auth:farm", "auth.farm"],
    ["cart:run", "cart.run"],
    ["task:run", "task.run"],
    ["task:stop", "task.stop"],
    ["session:stop", "session.stop"],
    ["session:approve", "session.approve"],
    ["session:set-otp", "session.set_otp"],
  ];
  for (const [channel, method] of passthrough) {
    ipcMain.handle(channel, async (_evt, params) => {
      try {
        const data = await rpc(method, params || {});
        return { ok: true, data };
      } catch (err) {
        return { ok: false, error: err.message };
      }
    });
  }

  ipcMain.handle("db:import-file", async () => {
    const { canceled, filePaths } = await dialog.showOpenDialog(mainWindow, {
      title: "Import accounts CSV/XLSX",
      properties: ["openFile"],
      filters: [
        { name: "Accounts", extensions: ["csv", "xlsx"] },
        { name: "All files", extensions: ["*"] },
      ],
    });
    if (canceled || !filePaths?.length) return { ok: false, error: "Cancelled" };
    try {
      const data = await rpc("db.import_file", { file_path: filePaths[0] });
      return { ok: true, data };
    } catch (err) {
      return { ok: false, error: err.message };
    }
  });

  ipcMain.handle("db:import-tasks-file", async () => {
    const { canceled, filePaths } = await dialog.showOpenDialog(mainWindow, {
      title: "Import tasks CSV/XLSX",
      properties: ["openFile"],
      filters: [
        { name: "Tasks", extensions: ["csv", "xlsx"] },
        { name: "All files", extensions: ["*"] },
      ],
    });
    if (canceled || !filePaths?.length) return { ok: false, error: "Cancelled" };
    try {
      const data = await rpc("db.import_tasks_file", { file_path: filePaths[0] });
      return { ok: true, data };
    } catch (err) {
      return { ok: false, error: err.message };
    }
  });

  ipcMain.handle("shell:open-external", async (_evt, url) => {
    try {
      await shell.openExternal(url);
      return { ok: true };
    } catch (err) {
      return { ok: false, error: err.message };
    }
  });

  ipcMain.handle("dialog:save-sample", async () => {
    const { canceled, filePath } = await dialog.showSaveDialog(mainWindow, {
      title: "Save sample CSV",
      defaultPath: "sample_accounts.csv",
      filters: [{ name: "CSV", extensions: ["csv"] }],
    });
    if (canceled || !filePath) return { ok: false };
    const header =
      "phone,email,card_number,exp_month,exp_year,cvc,billing_name,billing_email,billing_phone,billing_postal,billing_country,proxy,aycd_key,aycd_email,imap_email,imap_password,imap_host\n";
    const sample =
      "+14155550100,sample@example.com,4242424242424242,12,2030,123,Sample User,sample@example.com,+14155550100,94103,US,user:pass@proxy.example:8000,,,sample@example.com,app-password-here,imap.gmail.com\n";
    fs.writeFileSync(filePath, header + sample);
    return { ok: true, filePath };
  });

  ipcMain.handle("update:get-state", async () => ({ ...updateState }));
  ipcMain.handle("update:install", async () => {
    try {
      await installDownloadedUpdate();
      return { ok: true };
    } catch (err) {
      return { ok: false, error: err.message };
    }
  });
}

// ── App lifecycle ─────────────────────────────────────────────────────────
app.whenReady().then(() => {
  startPython();
  registerIpc();
  createWindow();
  scheduleUpdateChecks();
  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on("window-all-closed", () => {
  if (updateCheckTimer) clearInterval(updateCheckTimer);
  if (pyProc && !pyProc.killed) {
    try { pyProc.stdin.end(); } catch {}
    try { pyProc.kill(); } catch {}
  }
  if (process.platform !== "darwin") app.quit();
});

app.on("before-quit", () => {
  if (pyProc && !pyProc.killed) {
    try { pyProc.kill(); } catch {}
  }
});
