const { app, BrowserWindow, ipcMain, dialog, shell } = require("electron");
const { spawn } = require("node:child_process");
const path = require("node:path");
const fs = require("node:fs");
const readline = require("node:readline");

let mainWindow = null;
let pyProc = null;
let pyReady = false;
const pending = new Map();
let nextId = 1;
const readyWaiters = [];

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
  pyProc = spawn(spec.command, spec.args, {
    cwd: path.join(__dirname, ".."),
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
      "phone,email,card_number,exp_month,exp_year,cvc,billing_name,billing_email,billing_phone,billing_postal,billing_country,proxy,aycd_key\n";
    const sample =
      "+14155550100,sample@example.com,4242424242424242,12,2030,123,Sample User,sample@example.com,+14155550100,94103,US,user:pass@proxy.example:8000,\n";
    fs.writeFileSync(filePath, header + sample);
    return { ok: true, filePath };
  });
}

// ── App lifecycle ─────────────────────────────────────────────────────────
app.whenReady().then(() => {
  startPython();
  registerIpc();
  createWindow();
  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on("window-all-closed", () => {
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
