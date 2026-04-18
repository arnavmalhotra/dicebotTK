const { contextBridge, ipcRenderer } = require("electron");

const invoke = (ch) => (params) => ipcRenderer.invoke(ch, params);

contextBridge.exposeInMainWorld("api", {
  // DB — accounts / groups / sessions
  getGroups: invoke("db:get-groups"),
  createGroup: (name) => ipcRenderer.invoke("db:create-group", { name }),
  deleteGroup: (group_id) => ipcRenderer.invoke("db:delete-group", { group_id }),
  renameGroup: (group_id, name) => ipcRenderer.invoke("db:rename-group", { group_id, name }),
  getAccounts: (group_id) => ipcRenderer.invoke("db:get-accounts", { group_id }),
  getAccount: (account_id) => ipcRenderer.invoke("db:get-account", { account_id }),
  addAccount: (fields) => ipcRenderer.invoke("db:add-account", fields),
  updateAccount: (account_id, fields) => ipcRenderer.invoke("db:update-account", { account_id, ...fields }),
  deleteAccount: (account_id) => ipcRenderer.invoke("db:delete-account", { account_id }),
  assignGroup: (account_ids, group_id) => ipcRenderer.invoke("db:assign-group", { account_ids, group_id }),
  importFile: () => ipcRenderer.invoke("db:import-file"),
  getStats: invoke("db:get-stats"),
  getAccountsNeedingAuth: invoke("db:get-accounts-needing-auth"),
  getAccountsWithValidSession: (group_id) =>
    ipcRenderer.invoke("db:get-accounts-with-valid-session", { group_id }),
  getSession: (account_id) => ipcRenderer.invoke("db:get-session", { account_id }),

  // DB — tasks
  getTasks: invoke("db:get-tasks"),
  getTask: (task_id) => ipcRenderer.invoke("db:get-task", { task_id }),
  createTask: (fields) => ipcRenderer.invoke("db:create-task", fields),
  updateTask: (task_id, fields) => ipcRenderer.invoke("db:update-task", { task_id, ...fields }),
  deleteTask: (task_id) => ipcRenderer.invoke("db:delete-task", { task_id }),

  // Auth / cart / task runners
  authLoginOne: (account) => ipcRenderer.invoke("auth:login-one", { account }),
  authFarm: (accounts) => ipcRenderer.invoke("auth:farm", { accounts }),
  cartRun: (payload) => ipcRenderer.invoke("cart:run", payload),
  taskRun: (payload) => ipcRenderer.invoke("task:run", payload),
  taskStop: (task_id) => ipcRenderer.invoke("task:stop", { task_id }),
  sessionStop: (session_id) => ipcRenderer.invoke("session:stop", { session_id }),
  sessionApprove: (session_id) => ipcRenderer.invoke("session:approve", { session_id }),
  sessionSetOtp: (session_id, code) => ipcRenderer.invoke("session:set-otp", { session_id, code }),

  // Shell helpers
  openExternal: (url) => ipcRenderer.invoke("shell:open-external", url),
  saveSample: invoke("dialog:save-sample"),

  // Events
  onEvent: (cb) => {
    const listener = (_e, msg) => cb(msg);
    ipcRenderer.on("worker:event", listener);
    return () => ipcRenderer.removeListener("worker:event", listener);
  },
  onWorkerLog: (cb) => {
    const listener = (_e, msg) => cb(msg);
    ipcRenderer.on("worker:log", listener);
    return () => ipcRenderer.removeListener("worker:log", listener);
  },
});
