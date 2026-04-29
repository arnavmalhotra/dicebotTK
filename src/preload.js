const { contextBridge, ipcRenderer } = require("electron");

const invoke = (ch) => (params) => ipcRenderer.invoke(ch, params);

contextBridge.exposeInMainWorld("api", {
  // DB
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

  // Payment pools (managed list of card labels)
  getPaymentPools: invoke("db:get-payment-pools"),
  createPaymentPool: (name) => ipcRenderer.invoke("db:create-payment-pool", { name }),
  renamePaymentPool: (pool_id, name) => ipcRenderer.invoke("db:rename-payment-pool", { pool_id, name }),
  deletePaymentPool: (pool_id) => ipcRenderer.invoke("db:delete-payment-pool", { pool_id }),

  // Payment cards (cards belong to a pool, assigned to accounts)
  getPaymentCards: invoke("db:get-payment-cards"),
  getPaymentCard: (card_id) => ipcRenderer.invoke("db:get-payment-card", { card_id }),
  addPaymentCard: (fields) => ipcRenderer.invoke("db:add-payment-card", fields),
  updatePaymentCard: (card_id, fields) => ipcRenderer.invoke("db:update-payment-card", { card_id, ...fields }),
  deletePaymentCard: (card_id) => ipcRenderer.invoke("db:delete-payment-card", { card_id }),
  assignCard: (account_id, card_id) => ipcRenderer.invoke("db:assign-card", { account_id, card_id }),
  unassignCard: (account_id, card_id) => ipcRenderer.invoke("db:unassign-card", { account_id, card_id }),
  getCardLabels: invoke("db:get-card-labels"),
  getAssignedCardsForAccount: (account_id) => ipcRenderer.invoke("db:get-assigned-cards-for-account", { account_id }),
  bulkAccountCardsByLabel: (account_ids, label) => ipcRenderer.invoke("db:bulk-account-cards-by-label", { account_ids, label }),
  bulkAddPaymentCards: (rows) => ipcRenderer.invoke("db:bulk-add-payment-cards", { rows }),

  // Code pools (drop access / presale codes)
  getCodePools: invoke("db:get-code-pools"),
  createCodePool: (name) => ipcRenderer.invoke("db:create-code-pool", { name }),
  renameCodePool: (pool_id, name) => ipcRenderer.invoke("db:rename-code-pool", { pool_id, name }),
  deleteCodePool: (pool_id) => ipcRenderer.invoke("db:delete-code-pool", { pool_id }),
  getCodePoolCodes: (pool_id) => ipcRenderer.invoke("db:get-code-pool-codes", { pool_id }),
  addCodePoolCodes: (pool_id, codes) => ipcRenderer.invoke("db:add-code-pool-codes", { pool_id, codes }),
  deleteCodePoolCode: (code_id) => ipcRenderer.invoke("db:delete-code-pool-code", { code_id }),
  clearCodePool: (pool_id) => ipcRenderer.invoke("db:clear-code-pool", { pool_id }),
  drawCodesFromPool: (pool_id, count) => ipcRenderer.invoke("db:draw-codes-from-pool", { pool_id, count }),

  importFile: () => ipcRenderer.invoke("db:import-file"),
  getStats: invoke("db:get-stats"),
  getAccountsNeedingAuth: invoke("db:get-accounts-needing-auth"),
  getAccountsWithValidSession: (group_id) =>
    ipcRenderer.invoke("db:get-accounts-with-valid-session", { group_id }),
  getSession: (account_id) => ipcRenderer.invoke("db:get-session", { account_id }),
  getInventoryItems: invoke("db:get-inventory-items"),
  deleteInventoryItem: (item_id) => ipcRenderer.invoke("db:delete-inventory-item", { item_id }),

  // Tasks
  getTasks: invoke("db:get-tasks"),
  getTask: (task_id) => ipcRenderer.invoke("db:get-task", { task_id }),
  createTask: (fields) => ipcRenderer.invoke("db:create-task", fields),
  updateTask: (task_id, fields) => ipcRenderer.invoke("db:update-task", { task_id, ...fields }),
  deleteTask: (task_id) => ipcRenderer.invoke("db:delete-task", { task_id }),
  importTasksFile: (file_path) => ipcRenderer.invoke("db:import-tasks-file", { file_path }),
  taskRun: (payload) => ipcRenderer.invoke("task:run", payload),
  taskStop: (task_id) => ipcRenderer.invoke("task:stop", { task_id }),

  // Auth / cart
  authLoginOne: (account) => ipcRenderer.invoke("auth:login-one", { account }),
  authOpenProfile: (account) => ipcRenderer.invoke("auth:open-profile", { account }),
  authManualLoginOne: (account) => ipcRenderer.invoke("auth:manual-login-one", { account }),
  authFarm: (payload) => ipcRenderer.invoke("auth:farm", payload),
  authRefreshState: (payload) => ipcRenderer.invoke("auth:refresh-state", payload || {}),
  cartRun: (payload) => ipcRenderer.invoke("cart:run", payload),
  eventPreview: (event_url) => ipcRenderer.invoke("event:preview", { event_url }),
  sessionStop: (session_id) => ipcRenderer.invoke("session:stop", { session_id }),
  sessionApprove: (session_id) => ipcRenderer.invoke("session:approve", { session_id }),
  sessionSetOtp: (session_id, code) => ipcRenderer.invoke("session:set-otp", { session_id, code }),

  // Shell helpers
  openExternal: (url) => ipcRenderer.invoke("shell:open-external", url),
  saveSample: invoke("dialog:save-sample"),
  getAppVersion: invoke("app:get-version"),
  getUpdateState: invoke("update:get-state"),
  installUpdate: invoke("update:install"),

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
  onUpdateEvent: (cb) => {
    const listener = (_e, msg) => cb(msg);
    ipcRenderer.on("update:event", listener);
    return () => ipcRenderer.removeListener("update:event", listener);
  },
});
