"use strict";

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

const state = {
  accounts: [],
  groups: [],
  activeGroup: null,
  selected: new Set(),
  search: "",
  inventory: [],
  inventorySearch: "",
  tasks: [],
  selectedTasks: new Set(),
  taskSearch: "",
  carts: new Map(), // session_id -> cart info
  payment: { cards: [], pools: [], assignDropdownCardId: null },
  codePools: { pools: [], expanded: new Set(), codesByPool: new Map() },
  authFarm: { sessionId: null, running: false, accountStatus: new Map(), attempts: [] },
  authRefresh: {
    sessionId: null,
    running: false,
    total: 0,
    checked: 0,
    valid: 0,
    revoked: 0,
    skipped: 0,
    message: "Session badges are cached. Use Refresh auth state for a live check.",
  },
  update: null,
  settings: loadSettings(),
  activeModalType: null,
  bulkApprovalBusy: false,
};

function loadSettings() {
  try {
    return JSON.parse(localStorage.getItem("dicebot.settings") || "{}");
  } catch {
    return {};
  }
}
function saveSettings(patch) {
  state.settings = { ...state.settings, ...patch };
  localStorage.setItem("dicebot.settings", JSON.stringify(state.settings));
}

function formatVersionLabel(version) {
  const clean = String(version || "").replace(/^v/i, "").trim();
  return clean ? `v${clean}` : "";
}

function setUpdateState(nextState) {
  state.update = nextState || null;
  renderUpdateBanner();
}

function renderUpdateBanner() {
  const root = $("#updateBanner");
  if (!root) return;
  const update = state.update;
  if (!update || !update.status || update.status === "idle" || update.status === "checking") {
    root.hidden = true;
    root.innerHTML = "";
    return;
  }

  const versionLabel = formatVersionLabel(update.latestVersion);
  const links = [];
  let title = "";
  let message = "";
  let actionsHtml = "";

  if (update.mode === "mac") {
    title = `${versionLabel || "A new version"} is available`;
    message = "A newer macOS build is ready. Copy one of the links below to download it.";
    if (update.downloadUrl) links.push({ label: "DMG", value: update.downloadUrl });
    if (update.extraDownloadUrl) links.push({ label: "ZIP", value: update.extraDownloadUrl });
  } else if (update.mode === "windows" && update.status === "downloading") {
    title = `${versionLabel || "A new version"} is available`;
    message = "Downloading the Windows installer in the background now. You will be able to install it from here once the download finishes.";
    if (update.downloadUrl) links.push({ label: "Installer link", value: update.downloadUrl });
  } else if (update.mode === "windows" && update.status === "downloaded") {
    title = `${versionLabel || "A new version"} is ready`;
    message = "The update has finished downloading. Click Install update to close DiceBot and launch the new installer.";
    if (update.downloadUrl) links.push({ label: "Installer link", value: update.downloadUrl });
    actionsHtml = `<button class="btn btn-primary" id="installUpdateBtn">Install update</button>`;
  } else if (update.mode === "windows") {
    title = `${versionLabel || "A new version"} is available`;
    message = "A newer Windows build is available.";
    if (update.error) {
      message += ` Automatic download failed: ${update.error}`;
    } else if (!update.downloadUrl) {
      message += " Download it manually from the link below.";
    }
    if (update.downloadUrl) links.push({ label: "Installer link", value: update.downloadUrl });
  } else {
    title = `${versionLabel || "A new version"} is available`;
    message = update.error || "A newer build is available.";
    if (update.downloadUrl) links.push({ label: "Download link", value: update.downloadUrl });
  }

  root.innerHTML = `
    <div class="update-banner-content">
      <div class="update-banner-title">${escapeHtml(title)}</div>
      <div class="update-banner-text">${escapeHtml(message)}</div>
      ${links.length ? `
        <div class="update-banner-links">
          ${links.map((link) => `
            <div class="update-link-block">
              <div class="update-link-label">${escapeHtml(link.label)}</div>
              <textarea class="update-link-input" readonly rows="2">${link.value}</textarea>
            </div>
          `).join("")}
        </div>
      ` : ""}
    </div>
    ${actionsHtml ? `<div class="update-banner-actions">${actionsHtml}</div>` : ""}
  `;
  root.hidden = false;

  const installBtn = $("#installUpdateBtn");
  if (installBtn) {
    installBtn.addEventListener("click", async () => {
      installBtn.disabled = true;
      const res = await api.installUpdate();
      if (!res.ok) {
        installBtn.disabled = false;
        alert(res.error || "Could not launch the installer.");
      }
    });
  }
}

function pickLaunchAccounts(accounts, count) {
  const pool = [...(accounts || [])].sort((a, b) => (Number(a.id) || 0) - (Number(b.id) || 0));
  if (!pool.length) return [];
  const limit = Math.max(1, Math.min(pool.length, parseInt(count, 10) || pool.length));
  const start = ((Number(state.settings.profileLaunchCursor) || 0) % pool.length + pool.length) % pool.length;
  const chosen = [];
  for (let i = 0; i < limit; i += 1) {
    chosen.push(pool[(start + i) % pool.length]);
  }
  saveSettings({ profileLaunchCursor: (start + limit) % pool.length });
  return chosen;
}

function parseLineList(raw) {
  return String(raw || "")
    .split(/\r?\n|,/)
    .map((value) => value.trim())
    .filter(Boolean);
}

function parseOptionalFloat(raw) {
  const value = String(raw ?? "").trim();
  if (!value) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function sanitizePriceRules(raw) {
  const out = [];
  for (const item of Array.isArray(raw) ? raw : []) {
    if (!item || typeof item !== "object") continue;
    const rawQty = parseInt(item.quantity, 10);
    const quantity = Number.isFinite(rawQty) && rawQty > 0 ? rawQty : 1;
    const minPrice = Number(item.min_price);
    const maxPrice = Number(item.max_price);
    const rule = { quantity };
    if (Number.isFinite(minPrice) && minPrice > 0) rule.min_price = minPrice;
    if (Number.isFinite(maxPrice) && maxPrice > 0) rule.max_price = maxPrice;
    out.push(rule);
  }
  return out;
}

function renderPriceRuleRow(rule = { quantity: "", min_price: "", max_price: "" }) {
  const qty = rule.quantity === "" || rule.quantity == null ? "" : String(rule.quantity);
  const minPrice = rule.min_price === "" || rule.min_price == null ? "" : String(rule.min_price);
  const maxPrice = rule.max_price === "" || rule.max_price == null ? "" : String(rule.max_price);
  return `
    <div class="price-rule-row" style="display:flex;gap:8px;align-items:flex-end;margin-bottom:6px;">
      <label style="flex:1;"><span>Buy (qty)</span><input class="price-rule-qty" type="text" inputmode="numeric" pattern="[0-9]*" value="${escapeHtml(qty)}" placeholder="1"/></label>
      <label style="flex:1;"><span>min $</span><input class="price-rule-min" type="text" inputmode="decimal" pattern="[0-9]*\.?[0-9]*" value="${escapeHtml(minPrice)}" placeholder="any"/></label>
      <label style="flex:1;"><span>max $</span><input class="price-rule-max" type="text" inputmode="decimal" pattern="[0-9]*\.?[0-9]*" value="${escapeHtml(maxPrice)}" placeholder="any"/></label>
      <button type="button" class="btn btn-ghost btn-sm price-rule-remove" title="Remove rule">✕</button>
    </div>
  `;
}

function mountPriceRuleList(listEl, addBtn, initialRules) {
  if (!listEl) return () => [];
  const rules = sanitizePriceRules(initialRules);
  listEl.innerHTML = rules.length
    ? rules.map(renderPriceRuleRow).join("")
    : renderPriceRuleRow();
  const bind = () => {
    listEl.querySelectorAll(".price-rule-remove").forEach((btn) => {
      btn.onclick = () => {
        if (listEl.querySelectorAll(".price-rule-row").length <= 1) {
          btn.closest(".price-rule-row").querySelectorAll("input").forEach((i) => { i.value = ""; });
          return;
        }
        btn.closest(".price-rule-row")?.remove();
      };
    });
  };
  bind();
  addBtn?.addEventListener("click", () => {
    listEl.insertAdjacentHTML("beforeend", renderPriceRuleRow());
    bind();
  });
  return () => {
    const collected = [];
    listEl.querySelectorAll(".price-rule-row").forEach((row) => {
      const rawQty = parseInt(row.querySelector(".price-rule-qty")?.value, 10);
      const quantity = Number.isFinite(rawQty) && rawQty > 0 ? rawQty : 1;
      const minPrice = parseOptionalFloat(row.querySelector(".price-rule-min")?.value);
      const maxPrice = parseOptionalFloat(row.querySelector(".price-rule-max")?.value);
      const rule = { quantity };
      if (minPrice != null && minPrice > 0) rule.min_price = minPrice;
      if (maxPrice != null && maxPrice > 0) rule.max_price = maxPrice;
      collected.push(rule);
    });
    return collected;
  };
}

function summarizePriceRules(rules) {
  const list = sanitizePriceRules(rules);
  if (!list.length) return "No auto-buy rules";
  const r = list[0];
  let cond;
  if (r.min_price != null && r.max_price != null) {
    cond = `if $${r.min_price.toFixed(2)}–$${r.max_price.toFixed(2)}`;
  } else if (r.max_price != null) {
    cond = `if ≤$${r.max_price.toFixed(2)}`;
  } else if (r.min_price != null) {
    cond = `if ≥$${r.min_price.toFixed(2)}`;
  } else {
    cond = "(any price)";
  }
  const first = `Buy ${r.quantity} ${cond}`;
  return list.length === 1 ? first : `${first} +${list.length - 1}`;
}

function pluralize(count, noun) {
  return `${count} ${noun}${count === 1 ? "" : "s"}`;
}

function authRefreshSummary(valid, revoked, skipped) {
  const parts = [pluralize(valid, "valid"), pluralize(revoked, "revoked")];
  if (skipped) parts.push(`${pluralize(skipped, "skipped")} (left cached)`);
  return parts.join(", ");
}

function authProxyPoolValue({ persist = false, normalize = false } = {}) {
  const input = $("#authProxyPool");
  const fallback = Array.isArray(state.settings.authProxyPool)
    ? state.settings.authProxyPool.join("\n")
    : String(state.settings.authProxyPool || "");
  const proxies = [...new Set(parseLineList(input ? input.value : fallback))];
  if (normalize && input) input.value = proxies.join("\n");
  if (persist) saveSettings({ authProxyPool: proxies });
  renderAuthProxyPoolStatus(proxies.length);
  return proxies;
}

function renderAuthProxyPoolStatus(count = null) {
  const status = $("#authProxyPoolStatus");
  if (!status) return;
  const total = count == null ? authProxyPoolValue().length : count;
  status.textContent = `${total} ${total === 1 ? "proxy" : "proxies"} saved. Unauthenticated accounts borrow from this pool and keep the proxy only after a successful auth.`;
}

function renderAuthRefreshUi() {
  const btn = $("#refreshAuthStateBtn");
  if (btn) {
    btn.disabled = state.authRefresh.running || state.authFarm.running;
    btn.textContent = state.authRefresh.running ? "Refreshing…" : "Refresh auth state";
  }
  const status = $("#refreshAuthStateStatus");
  if (status) status.textContent = state.authRefresh.message;
}

function accountHasUsableSession(account) {
  return ["active", "expiring", "ok", "warn", "warning"].includes(String(account?.session_status || "").toLowerCase());
}

// ── Page switching ────────────────────────────────────────────────────────
$$("#navTabs .nav-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    $$("#navTabs .nav-btn").forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    const page = btn.dataset.page;
    $$(".page").forEach((p) => p.classList.remove("active"));
    $(`#page-${page}`).classList.add("active");
    if (page === "accounts") refreshAccounts();
    if (page === "auth") refreshAuthFarm();
    if (page === "dashboard") refreshDashboard();
    if (page === "inventory") refreshInventory();
    if (page === "profiles") refreshPaymentCards();
    if (page === "codepools") refreshCodePools();
    if (page === "tasks") refreshTasks();
  });
});

// ── Settings form ─────────────────────────────────────────────────────────
let _collectDefaultRules = () => [];
function hydrateSettingsForm() {
  $("#capsolverKey").value = state.settings.capsolverKey || "";
  $("#twocaptchaKey").value = state.settings.twocaptchaKey || "";
  $("#captchafunKey").value = state.settings.captchafunKey || "";
  $("#aycdKey").value = state.settings.aycdKey || "";
  $("#approvalWebhookUrl").value = state.settings.approvalWebhookUrl || "";
  $("#approvalPollUrl").value = state.settings.approvalPollUrl || "";
  $("#approvalSecret").value = state.settings.approvalSecret || "";
  _collectDefaultRules = mountPriceRuleList(
    $("#defaultRulesList"),
    $("#defaultRulesAdd"),
    state.settings.defaultPriceRules,
  );
}

const authProxyPoolInput = $("#authProxyPool");
if (authProxyPoolInput) {
  authProxyPoolInput.value = Array.isArray(state.settings.authProxyPool)
    ? state.settings.authProxyPool.join("\n")
    : String(state.settings.authProxyPool || "");
  authProxyPoolInput.addEventListener("input", () => authProxyPoolValue());
  authProxyPoolInput.addEventListener("blur", () => authProxyPoolValue({ persist: true, normalize: true }));
  renderAuthProxyPoolStatus();
}

$("#saveSettingsBtn").addEventListener("click", () => {
  saveSettings({
    capsolverKey: $("#capsolverKey").value.trim(),
    twocaptchaKey: $("#twocaptchaKey").value.trim(),
    captchafunKey: $("#captchafunKey").value.trim(),
    aycdKey: $("#aycdKey").value.trim(),
    approvalWebhookUrl: $("#approvalWebhookUrl").value.trim(),
    approvalPollUrl: $("#approvalPollUrl").value.trim(),
    approvalSecret: $("#approvalSecret").value.trim(),
    defaultPriceRules: _collectDefaultRules(),
    authProxyPool: authProxyPoolValue({ normalize: true }),
  });
  const el = $("#settingsStatus");
  el.textContent = "Saved.";
  setTimeout(() => (el.textContent = "—"), 2000);
});

// ── Accounts ──────────────────────────────────────────────────────────────
async function refreshAccounts() {
  const [groupsRes, accountsRes] = await Promise.all([
    api.getGroups(),
    api.getAccounts(state.activeGroup),
  ]);
  if (groupsRes.ok) state.groups = groupsRes.data || [];
  if (accountsRes.ok) state.accounts = accountsRes.data || [];
  renderGroupChips();
  renderAccountsTable();
}

function renderGroupChips() {
  const bar = $("#groupChips");
  bar.innerHTML = "";
  const allChip = document.createElement("button");
  allChip.className = "chip" + (state.activeGroup == null ? " active" : "");
  allChip.textContent = "All";
  allChip.addEventListener("click", () => { state.activeGroup = null; refreshAccounts(); });
  bar.appendChild(allChip);
  for (const g of state.groups) {
    const c = document.createElement("button");
    c.className = "chip" + (state.activeGroup === g.id ? " active" : "");
    c.textContent = g.name;
    c.addEventListener("click", () => { state.activeGroup = g.id; refreshAccounts(); });
    bar.appendChild(c);
  }
  const add = document.createElement("button");
  add.className = "chip";
  add.textContent = "+ New group";
  add.addEventListener("click", openNewGroupModal);
  bar.appendChild(add);
}

function openNewGroupModal() {
  openModal({
    title: "New group",
    bodyHtml: `
      <div class="form-grid">
        <label><span>Group name</span><input id="g_name" autofocus placeholder="e.g. Miami drop" /></label>
        <div id="g_error" class="status-line" style="color:var(--danger);display:none;"></div>
      </div>
    `,
    footerHtml: `<button class="btn btn-ghost" data-close>Cancel</button>
                 <button class="btn btn-primary" id="g_save">Create</button>`,
    onMount: () => {
      const input = $("#g_name");
      input?.focus();
      const submit = async () => {
        const name = input.value.trim();
        if (!name) { $("#g_error").textContent = "Name required."; $("#g_error").style.display = "block"; return; }
        const r = await api.createGroup(name);
        if (r.ok) { closeModal(); refreshAccounts(); }
        else { $("#g_error").textContent = r.error || "Failed to create group."; $("#g_error").style.display = "block"; }
      };
      $("#g_save").addEventListener("click", submit);
      input.addEventListener("keydown", (e) => { if (e.key === "Enter") submit(); });
    },
  });
}

function renderAccountsTable() {
  const body = $("#accountsBody");
  const search = state.search.toLowerCase();
  const rows = state.accounts.filter((a) => {
    if (!search) return true;
    return (a.phone || "").toLowerCase().includes(search) ||
           (a.email || "").toLowerCase().includes(search);
  });
  body.innerHTML = "";
  if (!rows.length) { $("#accountsEmpty").hidden = false; return; }
  $("#accountsEmpty").hidden = true;

  for (const a of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><input type="checkbox" data-id="${a.id}" ${state.selected.has(a.id) ? "checked" : ""} /></td>
      <td>${escapeHtml(a.phone || "")}</td>
      <td>${escapeHtml(a.email || "")}</td>
      <td>${escapeHtml(a.group_name || "—")}</td>
      <td>${a.proxy ? "<span class=\"muted\">set</span>" : "<span class=\"muted\">—</span>"}</td>
      <td><span class="session-dot ${sessionDotClass(a.session_status)}"></span>${a.session_status || "none"}</td>
      <td>${a.session_expires_in || "—"}</td>
      <td class="col-actions">
        <button class="btn-icon" data-action="edit" data-id="${a.id}" title="Edit account">✎</button>
        <button class="btn-icon" data-action="open" data-id="${a.id}" title="Open account">↗</button>
        <button class="btn-icon" data-action="auth" data-id="${a.id}" title="Run auth">🔑</button>
        <button class="btn-icon" data-action="manual-auth" data-id="${a.id}" title="Manual auth (type phone yourself)">✋</button>
        <button class="btn-icon" data-action="delete" data-id="${a.id}" title="Delete account">🗑</button>
      </td>
    `;
    body.appendChild(tr);
  }

  body.querySelectorAll("input[type=checkbox]").forEach((cb) => {
    cb.addEventListener("change", () => {
      const id = Number(cb.dataset.id);
      if (cb.checked) state.selected.add(id); else state.selected.delete(id);
      renderBulkBar();
    });
  });
  body.querySelectorAll("[data-action]").forEach((btn) => {
    btn.addEventListener("click", () => handleAccountAction(btn.dataset.action, Number(btn.dataset.id)));
  });
  renderBulkBar();
}

function renderBulkBar() {
  const bar = $("#bulkBar");
  const n = state.selected.size;
  bar.hidden = n === 0;
  if (n) $("#bulkCount").textContent = `${n} selected`;
}

$("#refreshAuthStateBtn").addEventListener("click", async () => {
  if (state.authRefresh.running) return;
  if (state.authFarm.running) {
    state.authRefresh.message = "Stop Auth Farm before refreshing auth state.";
    renderAuthRefreshUi();
    return;
  }
  const res = await api.authRefreshState();
  if (!res.ok) {
    state.authRefresh.message = `Could not start auth refresh: ${res.error || "Unknown error"}`;
    renderAuthRefreshUi();
    appendFarmLog(`Auth state refresh failed to start: ${res.error || "Unknown error"}`, "error");
    return;
  }
  const total = Number(res.data?.total || 0);
  state.authRefresh = {
    sessionId: res.data.session_id,
    running: true,
    total,
    checked: 0,
    valid: 0,
    revoked: 0,
    skipped: 0,
    message: total
      ? `Refreshing auth state for ${pluralize(total, "cached session")}…`
      : "Refreshing auth state…",
  };
  renderAuthRefreshUi();
  appendFarmLog(
    total
      ? `Refreshing auth state for ${pluralize(total, "cached session")}.`
      : "Refreshing auth state.",
    "info",
  );
});

function sessionDotClass(status) {
  if (status === "ok" || status === "active") return "ok";
  if (status === "warn" || status === "warning" || status === "expiring") return "warn";
  if (status === "expired") return "expired";
  return "none";
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;" }[c]));
}

async function handleAccountAction(action, id) {
  if (action === "delete") {
    if (!confirm("Delete this account?")) return;
    const r = await api.deleteAccount(id);
    if (r.ok) refreshAccounts();
    return;
  }
  if (action === "edit") {
    const res = await api.getAccount(id);
    if (res.ok) openAccountModal(res.data);
    return;
  }
  if (action === "auth") {
    const res = await api.getAccount(id);
    if (!res.ok) return;
    const r = await api.authLoginOne({ ...res.data });
    if (r.ok) appendFarmLog(`Auth started for ${res.data.phone} (session ${r.data.session_id})`);
    else appendFarmLog(`Failed to start auth for ${res.data.phone}: ${r.error || "Unknown error"}`, "error");
    return;
  }
  if (action === "manual-auth") {
    const res = await api.getAccount(id);
    if (!res.ok) return;
    const phone = res.data.phone || "";
    alert(
      `Manual auth started for:\n\n  ${phone}\n\n` +
      `A Chrome window will open. Type the phone number yourself and submit. ` +
      `DiceBot will detect the OTP screen and fill the code automatically.`
    );
    const r = await api.authManualLoginOne({ ...res.data });
    if (r.ok) {
      appendFarmLog(`Manual auth started for ${phone} — enter the phone in Chrome yourself.`);
      state.authFarm.accountStatus.set(id, "running");
      refreshAuthFarm();
    } else {
      appendFarmLog(`Failed to start manual auth for ${phone}: ${r.error || "Unknown error"}`, "error");
    }
    return;
  }
  if (action === "open") {
    const res = await api.getAccount(id);
    if (!res.ok) return;
    const r = await api.authOpenProfile({ ...res.data });
    if (r.ok) {
      appendFarmLog(`Opening account profile for ${res.data.phone} (session ${r.data.session_id})`);
      state.authFarm.accountStatus.set(id, "running");
      refreshAuthFarm();
      if ($("#page-accounts").classList.contains("active")) refreshAccounts();
    }
    else appendFarmLog(`Failed to open ${res.data.phone}: ${r.error || "Unknown error"}`, "error");
  }
}

$("#accountSearch").addEventListener("input", (e) => {
  state.search = e.target.value;
  renderAccountsTable();
});
$("#inventorySearch")?.addEventListener("input", (e) => {
  state.inventorySearch = e.target.value;
  renderInventoryTable();
});
$("#selectAll").addEventListener("change", (e) => {
  if (e.target.checked) state.accounts.forEach((a) => state.selected.add(a.id));
  else state.selected.clear();
  renderAccountsTable();
});

$("#bulkClearBtn").addEventListener("click", () => {
  state.selected.clear();
  renderAccountsTable();
});

$("#bulkDeleteBtn").addEventListener("click", async () => {
  const ids = [...state.selected];
  if (!ids.length) return;
  if (!confirm(`Delete ${ids.length} account(s)?`)) return;
  await Promise.all(ids.map((id) => api.deleteAccount(id)));
  state.selected.clear();
  refreshAccounts();
});

$("#bulkAuthBtn").addEventListener("click", async () => {
  const ids = [...state.selected];
  if (!ids.length) return;
  const accts = await Promise.all(ids.map((id) => api.getAccount(id)));
  const queue = accts.filter((r) => r.ok).map((r) => r.data);
  const concurrency = authFarmConcurrencyValue({ persist: true, normalize: true });
  const authProxyPool = authProxyPoolValue({ persist: true, normalize: true });
  const r = await api.authFarm({ accounts: queue, concurrency, auth_proxy_pool: authProxyPool });
  if (r.ok) {
    state.authFarm.accountStatus.clear();
    state.authFarm.sessionId = r.data.session_id;
    state.authFarm.running = true;
    $("#startFarmBtn").hidden = true;
    $("#stopFarmBtn").hidden = false;
    renderAuthRefreshUi();
    appendFarmLog(`Farm started — ${queue.length} selected accounts · concurrency ${concurrency} · proxy pool ${authProxyPool.length}`);
    $$("#navTabs .nav-btn").forEach((b) => b.classList.remove("active"));
    $('#navTabs .nav-btn[data-page="auth"]').classList.add("active");
    $$(".page").forEach((p) => p.classList.remove("active"));
    $("#page-auth").classList.add("active");
    refreshAuthFarm();
  } else {
    alert("Farm failed to start: " + r.error);
  }
});

$("#bulkAssignBtn").addEventListener("click", openBulkAssignModal);

async function openBulkAssignModal() {
  const ids = [...state.selected];
  if (!ids.length) return;
  const gRes = await api.getGroups();
  const groups = gRes.ok ? gRes.data || [] : [];
  const rows = [
    `<button type="button" class="chip" data-assign-gid="">No group</button>`,
    ...groups.map((g) => `<button type="button" class="chip" data-assign-gid="${g.id}">${escapeHtml(g.name)}</button>`),
  ].join("");
  openModal({
    title: `Assign ${ids.length} account(s) to group`,
    bodyHtml: `
      <p class="muted" style="font-size:12px;margin:0 0 10px;">Pick a group — or create a new one.</p>
      <div class="group-chips" style="flex-wrap:wrap;">${rows}</div>
      <hr style="border:none;border-top:1px solid var(--border);margin:16px 0;" />
      <div class="form-grid">
        <label><span>Create and assign new group</span>
          <div style="display:flex;gap:8px;">
            <input id="ga_new" placeholder="Group name" />
            <button type="button" class="btn btn-primary" id="ga_create">Create</button>
          </div>
        </label>
      </div>
    `,
    footerHtml: `<button class="btn btn-ghost" data-close>Cancel</button>`,
    onMount: () => {
      document.querySelectorAll("[data-assign-gid]").forEach((btn) => {
        btn.addEventListener("click", async () => {
          const raw = btn.dataset.assignGid;
          const gid = raw === "" ? null : Number(raw);
          const r = await api.assignGroup(ids, gid);
          if (r.ok) { closeModal(); state.selected.clear(); refreshAccounts(); }
          else alert("Assign failed: " + r.error);
        });
      });
      $("#ga_create").addEventListener("click", async () => {
        const name = $("#ga_new").value.trim();
        if (!name) return;
        const cg = await api.createGroup(name);
        if (!cg.ok) { alert("Create failed: " + cg.error); return; }
        const r = await api.assignGroup(ids, cg.data.id);
        if (r.ok) { closeModal(); state.selected.clear(); refreshAccounts(); }
        else alert("Assign failed: " + r.error);
      });
    },
  });
}

$("#addAccountBtn").addEventListener("click", () => openAccountModal(null));
$("#importBtn").addEventListener("click", async () => {
  const r = await api.importFile();
  if (r.ok) { alert(`Imported ${r.data.count} accounts.`); refreshAccounts(); }
  else if (r.error !== "Cancelled") alert("Import failed: " + r.error);
});
$("#showSampleBtn").addEventListener("click", showSampleModal);

function showSampleModal() {
  openModal({
    title: "Sample CSV format",
    bodyHtml: `
      <p class="muted">Expected columns (header row):</p>
      <pre style="background:var(--bg-0);padding:12px;border-radius:8px;font-size:11px;overflow:auto;">phone,email,card_number,exp_month,exp_year,cvc,billing_name,billing_email,billing_phone,billing_postal,billing_country,proxy,aycd_key,gmail_email,gmail_app_password</pre>
      <p class="muted" style="margin-top:10px;">All fields except <code>phone</code> are optional. For Gmail OTP support, use <code>gmail_email</code> and a Gmail app password. XLSX with the same header is also supported.</p>
    `,
    footerHtml: `<button class="btn btn-ghost" data-close>Close</button>
                 <button class="btn btn-primary" id="dlSampleBtn">Download CSV</button>`,
    onMount: () => {
      $("#dlSampleBtn").addEventListener("click", async () => {
        const r = await api.saveSample();
        if (r.ok) closeModal();
      });
    },
  });
}

function openAccountModal(account) {
  const isEdit = !!account;
  const a = account || {};
  openModal({
    title: isEdit ? "Edit account" : "Add account",
    bodyHtml: `
      <div class="form-grid">
        <label><span>Phone (+country)</span><input id="f_phone" value="${escapeHtml(a.phone || "")}" /></label>
        <label><span>Email</span><input id="f_email" value="${escapeHtml(a.email || "")}" /></label>
        <label><span>Proxy</span><input id="f_proxy" value="${escapeHtml(a.proxy || "")}" placeholder="user:pass@host:port" /></label>
        <label><span>AYCD key (per-account)</span><input id="f_aycd" value="${escapeHtml(a.aycd_key || "")}" /></label>
        <label><span>Gmail inbox email</span><input id="f_imap_email" value="${escapeHtml(a.imap_email || "")}" placeholder="leave blank to reuse account email" /></label>
        <label><span>Gmail app password</span><input id="f_imap_password" type="password" value="${escapeHtml(a.imap_password || "")}" placeholder="16-character app password" /></label>
        <div class="muted" style="font-size:12px;grid-column:1 / -1;">Gmail only. If AYCD does not return a code, Auth Farm can fall back to Gmail IMAP using this inbox and app password.</div>
        <label><span>Card number</span><input id="f_card" value="${escapeHtml(a.card_number || "")}" /></label>
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;">
          <label><span>MM</span><input id="f_em" value="${escapeHtml(a.card_exp_month || "")}" /></label>
          <label><span>YYYY</span><input id="f_ey" value="${escapeHtml(a.card_exp_year || "")}" /></label>
          <label><span>CVC</span><input id="f_cvc" value="${escapeHtml(a.card_cvv || "")}" /></label>
        </div>
        <label><span>Billing name</span><input id="f_bname" value="${escapeHtml(a.billing_name || "")}" /></label>
        <label><span>Billing postal / country</span>
          <div style="display:grid;grid-template-columns:2fr 1fr;gap:8px;">
            <input id="f_bp" value="${escapeHtml(a.billing_postal || "")}" />
            <input id="f_bc" value="${escapeHtml(a.billing_country || "US")}" />
          </div>
        </label>
      </div>
    `,
    footerHtml: `<button class="btn btn-ghost" data-close>Cancel</button>
                 <button class="btn btn-primary" id="saveAccountBtn">${isEdit ? "Save" : "Add"}</button>`,
    onMount: () => {
      $("#saveAccountBtn").addEventListener("click", async () => {
        const fields = {
          phone: $("#f_phone").value.trim(),
          email: $("#f_email").value.trim(),
          proxy: $("#f_proxy").value.trim(),
          aycd_key: $("#f_aycd").value.trim(),
          imap_email: $("#f_imap_email").value.trim(),
          imap_password: $("#f_imap_password").value.trim(),
          card_number: $("#f_card").value.trim(),
          exp_month: $("#f_em").value.trim(),
          exp_year: $("#f_ey").value.trim(),
          cvc: $("#f_cvc").value.trim(),
          billing_name: $("#f_bname").value.trim(),
          billing_postal: $("#f_bp").value.trim(),
          billing_country: $("#f_bc").value.trim(),
        };
        if (!fields.phone) { alert("Phone is required."); return; }
        const r = isEdit
          ? await api.updateAccount(a.id, fields)
          : await api.addAccount(fields);
        if (r.ok) { closeModal(); refreshAccounts(); }
        else alert("Save failed: " + r.error);
      });
    },
  });
}

// ── Payment Cards (DiceBotNew-style) ──────────────────────────────────────
function maskCardNumber(num) {
  const digits = String(num || "").replace(/\D/g, "");
  if (!digits) return "—";
  if (digits.length <= 4) return digits;
  return `•••• ${digits.slice(-4)}`;
}

function parseCardCsv(text) {
  const lines = String(text || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length) return { rows: [], errors: ["Paste at least a header row and one data row."] };
  const splitRow = (line) => {
    const out = [];
    let cur = "";
    let inQuotes = false;
    for (const ch of line) {
      if (ch === '"') { inQuotes = !inQuotes; continue; }
      if (ch === "," && !inQuotes) { out.push(cur); cur = ""; continue; }
      cur += ch;
    }
    out.push(cur);
    return out.map((s) => s.trim());
  };
  const headers = splitRow(lines[0]).map((h) => h.toLowerCase());
  const rows = [];
  const errors = [];
  for (let i = 1; i < lines.length; i += 1) {
    const cells = splitRow(lines[i]);
    const row = {};
    headers.forEach((h, idx) => { if (h) row[h] = cells[idx] != null ? cells[idx] : ""; });
    if (!Object.values(row).some(Boolean)) continue;
    rows.push(row);
  }
  if (!headers.includes("card_number")) errors.push("Missing required 'card_number' column.");
  return { rows, errors };
}

async function refreshPaymentCards() {
  const [cardsRes, poolsRes] = await Promise.all([api.getPaymentCards(), api.getPaymentPools()]);
  state.payment.cards = cardsRes.ok ? (cardsRes.data || []) : [];
  state.payment.pools = poolsRes.ok ? (poolsRes.data || []) : [];
  renderCardsList();
}

function openManagePoolsModal() {
  const renderBody = () => {
    const pools = state.payment.pools || [];
    const rowsHtml = pools.length
      ? pools.map((p) => `
          <div class="queue-item">
            <div>
              <div>${escapeHtml(p.name)}</div>
              <div class="muted" style="font-size:11px;">${p.card_count || 0} card${p.card_count === 1 ? "" : "s"}</div>
            </div>
            <div style="display:flex;gap:4px;">
              <button class="btn-icon" data-pool-action="rename" data-pool-id="${p.id}" title="Rename">✎</button>
              <button class="btn-icon" data-pool-action="delete" data-pool-id="${p.id}" title="Delete">🗑</button>
            </div>
          </div>
        `).join("")
      : `<div class="empty-state">No pools yet. Create one below.</div>`;
    return `
      <div class="queue-list" id="mp_list" style="max-height:280px;overflow:auto;margin-bottom:12px;">${rowsHtml}</div>
      <div style="display:flex;gap:8px;">
        <input id="mp_new_name" placeholder="New pool name (e.g. Spring batch)" style="flex:1;" />
        <button type="button" class="btn btn-primary" id="mp_new_btn">+ Create</button>
      </div>
    `;
  };
  openModal({
    title: "Manage card pools",
    bodyHtml: renderBody(),
    footerHtml: `<button class="btn btn-ghost" data-close>Close</button>`,
    onMount: () => {
      const wireUp = () => {
        document.querySelectorAll("#mp_list [data-pool-action]").forEach((btn) => {
          btn.addEventListener("click", async () => {
            const pid = Number(btn.dataset.poolId);
            const pool = state.payment.pools.find((p) => p.id === pid);
            if (!pool) return;
            const action = btn.dataset.poolAction;
            if (action === "rename") {
              const nx = prompt(`Rename pool "${pool.name}" to:`, pool.name);
              if (nx == null || nx.trim() === "" || nx.trim() === pool.name) return;
              const r = await api.renamePaymentPool(pid, nx.trim());
              if (!r.ok) { alert("Rename failed: " + r.error); return; }
              await refreshPaymentCards();
              $("#modalBody").innerHTML = renderBody();
              wireUp();
            } else if (action === "delete") {
              if (!confirm(`Delete pool "${pool.name}"? Its ${pool.card_count || 0} card${pool.card_count === 1 ? "" : "s"} will lose their pool label (cards stay, just unlabeled).`)) return;
              const r = await api.deletePaymentPool(pid);
              if (!r.ok) { alert("Delete failed: " + r.error); return; }
              await refreshPaymentCards();
              $("#modalBody").innerHTML = renderBody();
              wireUp();
            }
          });
        });
        const create = async () => {
          const name = $("#mp_new_name").value.trim();
          if (!name) return;
          const r = await api.createPaymentPool(name);
          if (!r.ok) { alert("Create failed: " + r.error); return; }
          await refreshPaymentCards();
          $("#modalBody").innerHTML = renderBody();
          wireUp();
        };
        $("#mp_new_btn").addEventListener("click", create);
        $("#mp_new_name").addEventListener("keydown", (e) => { if (e.key === "Enter") create(); });
      };
      wireUp();
    },
  });
}

function renderCardsList() {
  const list = $("#cardsList");
  const titleEl = $("#cardsPanelTitle");
  if (!list) return;
  const cards = state.payment.cards || [];
  if (titleEl) titleEl.textContent = `${cards.length} card${cards.length === 1 ? "" : "s"}`;
  if (!cards.length) {
    list.innerHTML = '<div class="empty-state">No cards yet. Click "+ Add card" to add one.</div>';
    return;
  }
  list.innerHTML = cards.map((c) => {
    const assigned = c.assigned_accounts || [];
    const chips = assigned.length
      ? assigned.map((a) => `<span class="badge badge-muted" style="margin-right:4px;font-size:11px;">${escapeHtml(a.account_phone || a.account_name || "?")}</span>`).join("")
      : `<span class="muted" style="font-size:11px;">No accounts assigned</span>`;
    const isOpen = state.payment.assignDropdownCardId === c.id;
    return `
      <div class="panel" style="margin-bottom:10px;padding:14px;">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px;">
          <div style="min-width:0;flex:1;">
            <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
              <strong>${escapeHtml(c.label || "(no label)")}</strong>
              <span class="badge badge-muted" style="font-size:11px;">${escapeHtml(maskCardNumber(c.card_number))}</span>
              <span class="muted" style="font-size:12px;">${escapeHtml(`${c.card_exp_month || "??"}/${c.card_exp_year || "??"}`)}</span>
              ${c.billing_postal ? `<span class="muted" style="font-size:12px;">postal ${escapeHtml(c.billing_postal)}</span>` : ""}
            </div>
            <div style="margin-top:8px;">${chips}</div>
          </div>
          <div style="display:flex;gap:6px;flex-shrink:0;position:relative;">
            <button class="btn btn-ghost btn-sm" data-card-action="toggle-assign" data-card-id="${c.id}">Assign</button>
            <button class="btn btn-ghost btn-sm" data-card-action="edit" data-card-id="${c.id}">Edit</button>
            <button class="btn btn-ghost btn-sm" data-card-action="delete" data-card-id="${c.id}">Delete</button>
          </div>
        </div>
        ${isOpen ? `<div id="assignPanel-${c.id}" style="margin-top:12px;padding:10px;background:var(--bg-0);border-radius:6px;max-height:280px;overflow:auto;"></div>` : ""}
      </div>
    `;
  }).join("");

  list.querySelectorAll("[data-card-action]").forEach((btn) => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const cid = Number(btn.dataset.cardId);
      const card = cards.find((c) => c.id === cid);
      if (!card) return;
      const action = btn.dataset.cardAction;
      if (action === "toggle-assign") {
        state.payment.assignDropdownCardId = state.payment.assignDropdownCardId === cid ? null : cid;
        renderCardsList();
        if (state.payment.assignDropdownCardId === cid) {
          await mountAssignPanel(card);
        }
      } else if (action === "edit") {
        openEditCardModal(card);
      } else if (action === "delete") {
        deleteCard(card);
      }
    });
  });
}

async function mountAssignPanel(card) {
  const panel = document.getElementById(`assignPanel-${card.id}`);
  if (!panel) return;
  const acctsRes = await api.getAccounts(null);
  const accounts = acctsRes.ok ? (acctsRes.data || []) : [];
  const assignedIds = new Set((card.assigned_accounts || []).map((a) => Number(a.account_id)));
  if (!accounts.length) {
    panel.innerHTML = `<div class="muted" style="font-size:12px;">No accounts. Import accounts first.</div>`;
    return;
  }
  panel.innerHTML = accounts.map((a) => {
    const isAssigned = assignedIds.has(a.id);
    return `
      <button type="button" class="assign-row" data-account-id="${a.id}" data-is-assigned="${isAssigned}"
        style="display:flex;align-items:center;gap:10px;width:100%;padding:6px 8px;background:transparent;border:none;color:var(--text);text-align:left;cursor:pointer;border-radius:4px;">
        <span style="display:inline-block;width:16px;height:16px;border-radius:3px;border:1px solid var(--border);background:${isAssigned ? "var(--accent)" : "transparent"};text-align:center;line-height:14px;font-size:12px;">${isAssigned ? "✓" : ""}</span>
        <span>${escapeHtml(a.phone || a.name)}</span>
        <span class="muted" style="font-size:11px;margin-left:auto;">${escapeHtml(a.email || "")}</span>
      </button>
    `;
  }).join("");
  panel.querySelectorAll(".assign-row").forEach((row) => {
    row.addEventListener("mouseenter", () => { row.style.background = "var(--bg-1)"; });
    row.addEventListener("mouseleave", () => { row.style.background = "transparent"; });
    row.addEventListener("click", async (e) => {
      e.stopPropagation();
      const aid = Number(row.dataset.accountId);
      const isAssigned = row.dataset.isAssigned === "true";
      const r = isAssigned ? await api.unassignCard(aid, card.id) : await api.assignCard(aid, card.id);
      if (!r.ok) { alert("Assign failed: " + r.error); return; }
      await refreshPaymentCards();
      // Re-mount the panel for the same card if it's still the open one
      if (state.payment.assignDropdownCardId === card.id) {
        const updated = state.payment.cards.find((c) => c.id === card.id);
        if (updated) await mountAssignPanel(updated);
      }
    });
  });
}

document.addEventListener("click", (e) => {
  if (state.payment.assignDropdownCardId == null) return;
  if (e.target.closest("[data-card-action]") || e.target.closest("[id^='assignPanel-']")) return;
  state.payment.assignDropdownCardId = null;
  renderCardsList();
});

function cardImportSampleHeader() {
  return "label,card_number,exp_month,exp_year,cvc,billing_name,billing_email,billing_phone,billing_postal,billing_country";
}

function openBulkImportCardsModal() {
  openModal({
    title: "Bulk import cards",
    bodyHtml: `
      <p class="muted" style="font-size:12px;margin:0 0 8px;">Paste a CSV. Each row creates one new card. The <code>label</code> column names a pool — pools that don't exist yet are created automatically. Assign cards to accounts after import.</p>
      <pre style="background:var(--bg-0);padding:10px;border-radius:6px;font-size:11px;overflow:auto;margin:0 0 10px;">${escapeHtml(cardImportSampleHeader())}</pre>
      <textarea id="bi_text" rows="10" style="width:100%;font-family:monospace;font-size:12px;" placeholder="${escapeHtml(cardImportSampleHeader())}"></textarea>
      <div id="bi_status" class="status-line muted" style="margin-top:8px;">Paste rows above to import.</div>
    `,
    footerHtml: `<button class="btn btn-ghost" data-close>Cancel</button>
                 <button class="btn btn-primary" id="bi_import">Import</button>`,
    onMount: () => {
      $("#bi_import").addEventListener("click", async () => {
        const text = $("#bi_text").value;
        const { rows, errors } = parseCardCsv(text);
        const status = $("#bi_status");
        if (errors.length) {
          status.style.color = "var(--danger)";
          status.textContent = errors.join(" ");
          return;
        }
        if (!rows.length) {
          status.style.color = "var(--danger)";
          status.textContent = "No data rows parsed.";
          return;
        }
        status.style.color = "";
        status.textContent = `Importing ${rows.length} row${rows.length === 1 ? "" : "s"}…`;
        const r = await api.bulkAddPaymentCards(rows);
        if (!r.ok) { status.style.color = "var(--danger)"; status.textContent = "Import failed: " + r.error; return; }
        const data = r.data || {};
        const parts = [`${data.added || 0} added`];
        if (data.errors?.length) parts.push(`${data.errors.length} error${data.errors.length === 1 ? "" : "s"}`);
        status.style.color = data.errors?.length ? "var(--warn)" : "";
        let detail = "";
        if (data.errors?.length) detail += `\nErrors: ${data.errors.slice(0, 5).map((e) => `row ${e.row}: ${e.error}`).join("; ")}`;
        status.textContent = parts.join(" · ") + detail;
        await refreshPaymentCards();
      });
    },
  });
}

function poolSelectHtml(idAttr, selectedPoolId) {
  const pools = state.payment.pools || [];
  if (!pools.length) {
    return `<select id="${idAttr}" disabled><option>No pools yet — click "Manage card pools" first</option></select>`;
  }
  const opts = pools.map((p) => `<option value="${p.id}" ${selectedPoolId === p.id ? "selected" : ""}>${escapeHtml(p.name)}</option>`).join("");
  return `<select id="${idAttr}">${opts}</select>`;
}

function openAddCardModal() {
  if (!(state.payment.pools || []).length) {
    alert(`No pools yet. Click "Manage card pools" to create at least one pool first.`);
    return;
  }
  openModal({
    title: "Add card",
    bodyHtml: `
      <div class="form-grid">
        <label><span>Pool</span>${poolSelectHtml("ac_pool", state.payment.pools[0]?.id)}</label>
        <label><span>Card number</span><input id="ac_card" autofocus /></label>
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;">
          <label><span>MM</span><input id="ac_em" /></label>
          <label><span>YYYY</span><input id="ac_ey" /></label>
          <label><span>CVC</span><input id="ac_cvc" /></label>
        </div>
        <label><span>Billing name</span><input id="ac_bname" /></label>
        <label><span>Billing email</span><input id="ac_bemail" /></label>
        <label><span>Billing phone</span><input id="ac_bphone" /></label>
        <label><span>Billing postal / country</span>
          <div style="display:grid;grid-template-columns:2fr 1fr;gap:8px;">
            <input id="ac_bp" />
            <input id="ac_bc" value="US" />
          </div>
        </label>
      </div>
    `,
    footerHtml: `<button class="btn btn-ghost" data-close>Cancel</button>
                 <button class="btn btn-primary" id="ac_save">Save</button>`,
    onMount: () => {
      $("#ac_save").addEventListener("click", async () => {
        const poolId = Number($("#ac_pool").value);
        if (!poolId) { alert("Pick a pool."); return; }
        const fields = {
          pool_id: poolId,
          card_number: $("#ac_card").value.trim(),
          exp_month: $("#ac_em").value.trim(),
          exp_year: $("#ac_ey").value.trim(),
          cvc: $("#ac_cvc").value.trim(),
          billing_name: $("#ac_bname").value.trim(),
          billing_email: $("#ac_bemail").value.trim(),
          billing_phone: $("#ac_bphone").value.trim(),
          billing_postal: $("#ac_bp").value.trim(),
          billing_country: $("#ac_bc").value.trim() || "US",
        };
        if (!fields.card_number) { alert("Card number is required."); return; }
        const r = await api.addPaymentCard(fields);
        if (r.ok) { closeModal(); await refreshPaymentCards(); }
        else alert("Save failed: " + r.error);
      });
    },
  });
}

function openEditCardModal(card) {
  openModal({
    title: "Edit card",
    bodyHtml: `
      <div class="form-grid">
        <label><span>Pool</span>${poolSelectHtml("ec_pool", card.pool_id)}</label>
        <label><span>Card number</span><input id="ec_card" value="${escapeHtml(card.card_number || "")}" /></label>
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;">
          <label><span>MM</span><input id="ec_em" value="${escapeHtml(card.card_exp_month || "")}" /></label>
          <label><span>YYYY</span><input id="ec_ey" value="${escapeHtml(card.card_exp_year || "")}" /></label>
          <label><span>CVC</span><input id="ec_cvc" value="${escapeHtml(card.card_cvv || "")}" /></label>
        </div>
        <label><span>Billing name</span><input id="ec_bname" value="${escapeHtml(card.billing_name || "")}" /></label>
        <label><span>Billing email</span><input id="ec_bemail" value="${escapeHtml(card.billing_email || "")}" /></label>
        <label><span>Billing phone</span><input id="ec_bphone" value="${escapeHtml(card.billing_phone || "")}" /></label>
        <label><span>Billing postal / country</span>
          <div style="display:grid;grid-template-columns:2fr 1fr;gap:8px;">
            <input id="ec_bp" value="${escapeHtml(card.billing_postal || "")}" />
            <input id="ec_bc" value="${escapeHtml(card.billing_country || "US")}" />
          </div>
        </label>
      </div>
    `,
    footerHtml: `<button class="btn btn-ghost" data-close>Cancel</button>
                 <button class="btn btn-primary" id="ec_save">Save</button>`,
    onMount: () => {
      $("#ec_save").addEventListener("click", async () => {
        const poolSelect = $("#ec_pool");
        const fields = {
          card_number: $("#ec_card").value.trim(),
          card_exp_month: $("#ec_em").value.trim(),
          card_exp_year: $("#ec_ey").value.trim(),
          card_cvv: $("#ec_cvc").value.trim(),
          billing_name: $("#ec_bname").value.trim(),
          billing_email: $("#ec_bemail").value.trim(),
          billing_phone: $("#ec_bphone").value.trim(),
          billing_postal: $("#ec_bp").value.trim(),
          billing_country: $("#ec_bc").value.trim() || "US",
        };
        if (poolSelect && !poolSelect.disabled) {
          fields.pool_id = Number(poolSelect.value);
        }
        const r = await api.updatePaymentCard(card.id, fields);
        if (r.ok) { closeModal(); await refreshPaymentCards(); }
        else alert("Save failed: " + r.error);
      });
    },
  });
}

async function deleteCard(card) {
  if (!confirm(`Delete card "${card.label || maskCardNumber(card.card_number)}"? This also removes all assignments.`)) return;
  const r = await api.deletePaymentCard(card.id);
  if (r.ok) await refreshPaymentCards();
  else alert("Delete failed: " + r.error);
}

$("#addCardBtn")?.addEventListener("click", openAddCardModal);
$("#bulkImportCardsBtn")?.addEventListener("click", openBulkImportCardsModal);
$("#manageCardPoolsBtn")?.addEventListener("click", async () => {
  await refreshPaymentCards();
  openManagePoolsModal();
});

// ── Code Pools ────────────────────────────────────────────────────────────
async function refreshCodePools() {
  const r = await api.getCodePools();
  state.codePools.pools = r.ok ? (r.data || []) : [];
  // drop expanded entries that no longer exist
  const validIds = new Set(state.codePools.pools.map((p) => p.id));
  for (const id of Array.from(state.codePools.expanded)) {
    if (!validIds.has(id)) state.codePools.expanded.delete(id);
  }
  // load codes for any expanded pools
  await Promise.all(
    Array.from(state.codePools.expanded).map(async (pid) => {
      const cr = await api.getCodePoolCodes(pid);
      state.codePools.codesByPool.set(pid, cr.ok ? (cr.data || []) : []);
    }),
  );
  renderCodePoolsList();
}

function renderCodePoolsList() {
  const list = $("#codePoolsList");
  const titleEl = $("#codePoolsPanelTitle");
  if (!list) return;
  const pools = state.codePools.pools || [];
  if (titleEl) titleEl.textContent = `${pools.length} code pool${pools.length === 1 ? "" : "s"}`;
  if (!pools.length) {
    list.innerHTML = '<div class="empty-state">No code pools yet. Click "+ New code pool" to create one.</div>';
    return;
  }
  list.innerHTML = pools.map((p) => {
    const isOpen = state.codePools.expanded.has(p.id);
    const codes = state.codePools.codesByPool.get(p.id) || [];
    const codesPanel = isOpen ? `
      <div style="margin-top:12px;padding:12px;background:var(--bg-0);border-radius:6px;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
          <div class="muted" style="font-size:12px;">${codes.length} code${codes.length === 1 ? "" : "s"} in pool</div>
          <div style="display:flex;gap:6px;">
            <button class="btn btn-ghost btn-sm" data-codepool-action="add" data-pool-id="${p.id}">+ Add codes</button>
            <button class="btn btn-ghost btn-sm" data-codepool-action="clear" data-pool-id="${p.id}" ${codes.length ? "" : "disabled"}>Clear all</button>
          </div>
        </div>
        ${codes.length
          ? `<div style="max-height:260px;overflow:auto;border:1px solid var(--border);border-radius:6px;">
              ${codes.map((c) => `
                <div style="display:flex;justify-content:space-between;align-items:center;padding:6px 10px;border-bottom:1px solid var(--border);">
                  <code style="font-size:12px;">${escapeHtml(c.code)}</code>
                  <button class="btn-icon" data-codepool-action="delete-code" data-code-id="${c.id}" title="Remove this code">🗑</button>
                </div>
              `).join("")}
            </div>`
          : `<div class="muted" style="font-size:12px;">No codes yet. Click "+ Add codes" to paste a batch.</div>`}
      </div>
    ` : "";
    return `
      <div class="panel" style="margin-bottom:10px;padding:14px;">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px;">
          <div style="min-width:0;flex:1;">
            <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
              <strong>${escapeHtml(p.name)}</strong>
              <span class="badge badge-muted" style="font-size:11px;">${p.code_count || 0} code${p.code_count === 1 ? "" : "s"}</span>
            </div>
          </div>
          <div style="display:flex;gap:6px;flex-shrink:0;">
            <button class="btn btn-ghost btn-sm" data-codepool-action="toggle" data-pool-id="${p.id}">${isOpen ? "Hide codes" : "View codes"}</button>
            <button class="btn btn-ghost btn-sm" data-codepool-action="rename" data-pool-id="${p.id}">Rename</button>
            <button class="btn btn-ghost btn-sm" data-codepool-action="delete" data-pool-id="${p.id}">Delete</button>
          </div>
        </div>
        ${codesPanel}
      </div>
    `;
  }).join("");

  list.querySelectorAll("[data-codepool-action]").forEach((btn) => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const action = btn.dataset.codepoolAction;
      if (action === "delete-code") {
        const codeId = Number(btn.dataset.codeId);
        const r = await api.deleteCodePoolCode(codeId);
        if (!r.ok) { alert("Delete failed: " + r.error); return; }
        await refreshCodePools();
        return;
      }
      const pid = Number(btn.dataset.poolId);
      const pool = state.codePools.pools.find((p) => p.id === pid);
      if (!pool) return;
      if (action === "toggle") {
        if (state.codePools.expanded.has(pid)) {
          state.codePools.expanded.delete(pid);
        } else {
          state.codePools.expanded.add(pid);
          const cr = await api.getCodePoolCodes(pid);
          state.codePools.codesByPool.set(pid, cr.ok ? (cr.data || []) : []);
        }
        renderCodePoolsList();
      } else if (action === "rename") {
        const nx = prompt(`Rename code pool "${pool.name}" to:`, pool.name);
        if (nx == null || nx.trim() === "" || nx.trim() === pool.name) return;
        const r = await api.renameCodePool(pid, nx.trim());
        if (!r.ok) { alert("Rename failed: " + r.error); return; }
        await refreshCodePools();
      } else if (action === "delete") {
        if (!confirm(`Delete code pool "${pool.name}"? Its ${pool.code_count || 0} code${pool.code_count === 1 ? "" : "s"} will be removed.`)) return;
        const r = await api.deleteCodePool(pid);
        if (!r.ok) { alert("Delete failed: " + r.error); return; }
        state.codePools.expanded.delete(pid);
        state.codePools.codesByPool.delete(pid);
        await refreshCodePools();
      } else if (action === "add") {
        openAddCodesToPoolModal(pool);
      } else if (action === "clear") {
        if (!confirm(`Clear all codes from pool "${pool.name}"? This cannot be undone.`)) return;
        const r = await api.clearCodePool(pid);
        if (!r.ok) { alert("Clear failed: " + r.error); return; }
        state.codePools.codesByPool.set(pid, []);
        await refreshCodePools();
      }
    });
  });
}

function openCreateCodePoolModal() {
  openModal({
    title: "New code pool",
    bodyHtml: `
      <div class="form-grid">
        <label><span>Pool name</span><input id="cp_new_name" autofocus placeholder="e.g. Spring presale codes" /></label>
        <label><span>Codes (optional — one per line)</span>
          <textarea id="cp_new_codes" rows="8" placeholder="ACCESS-1&#10;ACCESS-2&#10;…"></textarea>
        </label>
        <p class="muted" style="font-size:12px;margin:0;">You can also paste codes later from the pool's "+ Add codes" button.</p>
      </div>
    `,
    footerHtml: `<button class="btn btn-ghost" data-close>Cancel</button>
                 <button class="btn btn-primary" id="cp_new_save">Create</button>`,
    onMount: () => {
      $("#cp_new_save").addEventListener("click", async () => {
        const name = $("#cp_new_name").value.trim();
        if (!name) { alert("Pool name is required."); return; }
        const created = await api.createCodePool(name);
        if (!created.ok) { alert("Create failed: " + created.error); return; }
        const pid = created.data?.id;
        const codes = parseLineList($("#cp_new_codes").value);
        if (pid && codes.length) {
          const r = await api.addCodePoolCodes(pid, codes);
          if (!r.ok) { alert("Codes import failed: " + r.error); }
        }
        closeModal();
        await refreshCodePools();
      });
    },
  });
}

function openAddCodesToPoolModal(pool) {
  openModal({
    title: `Add codes to "${pool.name}"`,
    bodyHtml: `
      <p class="muted" style="font-size:12px;margin:0 0 8px;">Paste one code per line (or comma-separated). Duplicates inside this pool are skipped.</p>
      <textarea id="cp_codes_text" rows="12" style="width:100%;font-family:monospace;font-size:12px;" placeholder="ACCESS-1&#10;ACCESS-2&#10;…" autofocus></textarea>
      <div id="cp_codes_status" class="status-line muted" style="margin-top:8px;">Paste codes above to add them.</div>
    `,
    footerHtml: `<button class="btn btn-ghost" data-close>Cancel</button>
                 <button class="btn btn-primary" id="cp_codes_save">Add codes</button>`,
    onMount: () => {
      $("#cp_codes_save").addEventListener("click", async () => {
        const codes = parseLineList($("#cp_codes_text").value);
        const status = $("#cp_codes_status");
        if (!codes.length) {
          status.style.color = "var(--danger)";
          status.textContent = "No codes parsed.";
          return;
        }
        status.style.color = "";
        status.textContent = `Adding ${codes.length} code${codes.length === 1 ? "" : "s"}…`;
        const r = await api.addCodePoolCodes(pool.id, codes);
        if (!r.ok) { status.style.color = "var(--danger)"; status.textContent = "Add failed: " + r.error; return; }
        const data = r.data || {};
        const parts = [`${data.added || 0} added`];
        if (data.skipped) parts.push(`${data.skipped} duplicate${data.skipped === 1 ? "" : "s"} skipped`);
        status.textContent = parts.join(" · ");
        // Refresh codes list for the pool while leaving the modal open is fine,
        // but most users will just close. Refresh pool list anyway.
        const cr = await api.getCodePoolCodes(pool.id);
        state.codePools.codesByPool.set(pool.id, cr.ok ? (cr.data || []) : []);
        state.codePools.expanded.add(pool.id);
        await refreshCodePools();
      });
    },
  });
}

$("#addCodePoolBtn")?.addEventListener("click", openCreateCodePoolModal);

// ── Auth Farm ─────────────────────────────────────────────────────────────
function authFarmConcurrencyValue({ persist = false, normalize = false } = {}) {
  const input = $("#farmConcurrency");
  const raw = String(input?.value ?? state.settings.authFarmConcurrency ?? "1").trim();
  const parsed = parseInt(raw, 10);
  const value = Number.isFinite(parsed) && parsed > 0 ? parsed : 1;
  if (normalize && input) input.value = String(value);
  if (persist) saveSettings({ authFarmConcurrency: value });
  return value;
}

$("#farmConcurrency").value = String(state.settings.authFarmConcurrency ?? 1);
$("#farmConcurrency").addEventListener("input", () => {
  const raw = String($("#farmConcurrency").value ?? "").trim();
  if (!raw) return;
  const parsed = parseInt(raw, 10);
  if (Number.isFinite(parsed) && parsed > 0) saveSettings({ authFarmConcurrency: parsed });
});
$("#farmConcurrency").addEventListener("blur", () => {
  authFarmConcurrencyValue({ persist: true, normalize: true });
});

async function refreshAuthFarm() {
  const [stats, pending] = await Promise.all([
    api.getStats(),
    api.getAccountsNeedingAuth(),
  ]);
  if (stats.ok && stats.data) {
    $("#farmTotal").textContent = stats.data.total_accounts ?? "—";
    $("#farmActive").textContent = stats.data.active_sessions ?? "—";
    $("#farmExpiring").textContent = stats.data.expiring_sessions ?? "—";
    $("#farmNeeds").textContent = stats.data.no_session ?? "—";
  }
  renderAuthQueue(pending.ok ? pending.data : []);
}
function renderAuthQueue(accounts) {
  const q = $("#farmQueue");
  if (!accounts?.length) { q.innerHTML = '<div class="empty-state">All accounts authenticated.</div>'; return; }
  q.innerHTML = "";
  for (const a of accounts) {
    const row = document.createElement("div");
    row.className = "queue-item";
    const status = state.authFarm.accountStatus.get(a.id) || "pending";
    row.innerHTML = `
      <div>
        <div>${escapeHtml(a.phone || "")}</div>
        <div class="muted" style="font-size:11px;">${escapeHtml(a.email || "")}</div>
      </div>
      <span class="queue-status badge-${statusBadge(status)}">${status}</span>
    `;
    q.appendChild(row);
  }
}
function statusBadge(s) {
  if (s === "ok") return "ok";
  if (s === "fail" || s === "error") return "danger";
  if (s === "running") return "warn";
  return "muted";
}
$("#refreshAuthBtn").addEventListener("click", refreshAuthFarm);
$("#startFarmBtn").addEventListener("click", async () => {
  const pending = await api.getAccountsNeedingAuth();
  if (!pending.ok || !pending.data?.length) { appendFarmLog("No accounts need auth.", "info"); return; }
  const concurrency = authFarmConcurrencyValue({ persist: true, normalize: true });
  const authProxyPool = authProxyPoolValue({ persist: true, normalize: true });
  const r = await api.authFarm({ accounts: pending.data, concurrency, auth_proxy_pool: authProxyPool });
  if (r.ok) {
    state.authFarm.accountStatus.clear();
    state.authFarm.sessionId = r.data.session_id;
    state.authFarm.running = true;
    $("#startFarmBtn").hidden = true;
    $("#stopFarmBtn").hidden = false;
    renderAuthRefreshUi();
    appendFarmLog(`Farm started — ${pending.data.length} accounts in queue · concurrency ${concurrency} · proxy pool ${authProxyPool.length}`);
  } else {
    appendFarmLog("Farm failed to start: " + r.error, "error");
  }
});
$("#stopFarmBtn").addEventListener("click", async () => {
  if (state.authFarm.sessionId) await api.sessionStop(state.authFarm.sessionId);
});

$("#exportProxyCsvBtn")?.addEventListener("click", () => exportProxyStats("csv"));
$("#exportProxyJsonBtn")?.addEventListener("click", () => exportProxyStats("json"));
$("#clearProxyStatsBtn")?.addEventListener("click", clearProxyStats);

$("#exportInventoryBtn")?.addEventListener("click", () => exportInventory("csv"));
$("#exportInventoryJsonBtn")?.addEventListener("click", () => exportInventory("json"));
$("#exportTasksBtn")?.addEventListener("click", exportTasks);
$("#exportAccountsBtn")?.addEventListener("click", exportAccounts);

function appendFarmLog(message, level = "info") {
  const pane = $("#farmLog");
  const line = document.createElement("div");
  line.className = `log-line ${level}`;
  const ts = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  line.innerHTML = `<span class="log-ts">${ts}</span>${escapeHtml(message)}`;
  pane.appendChild(line);
  pane.scrollTop = pane.scrollHeight;
}

// ── Proxy-success tracking (Auth Farm) ────────────────────────────────────

function recordAuthAttempt(msg) {
  if (!msg || !msg.account_id) return;
  const list = state.authFarm.attempts;
  const idx = list.findIndex((r) => r.account_id === msg.account_id && r.session_id === state.authFarm.sessionId && r.status === "running");
  if (msg.status === "running") {
    list.push({
      session_id: state.authFarm.sessionId,
      account_id: msg.account_id,
      phone: msg.phone || "",
      email: msg.email || "",
      proxy: msg.proxy || "",
      proxy_origin: msg.proxy_origin || "",
      started_at: msg.started_at || (Date.now() / 1000),
      finished_at: null,
      duration_s: null,
      status: "running",
      error: "",
    });
  } else if (idx >= 0) {
    Object.assign(list[idx], {
      status: msg.status,
      error: msg.error || "",
      finished_at: msg.finished_at ?? (Date.now() / 1000),
      duration_s: msg.duration_s ?? (((msg.finished_at || (Date.now() / 1000)) - list[idx].started_at) || 0),
      proxy: msg.proxy || list[idx].proxy,
      proxy_origin: msg.proxy_origin || list[idx].proxy_origin,
    });
  } else {
    list.push({
      session_id: state.authFarm.sessionId,
      account_id: msg.account_id,
      phone: msg.phone || "",
      email: msg.email || "",
      proxy: msg.proxy || "",
      proxy_origin: msg.proxy_origin || "",
      started_at: msg.started_at || (Date.now() / 1000),
      finished_at: msg.finished_at ?? (Date.now() / 1000),
      duration_s: msg.duration_s ?? 0,
      status: msg.status,
      error: msg.error || "",
    });
  }
  if (list.length > 5000) list.splice(0, list.length - 5000);
  renderProxyStats();
}

function renderProxyStats() {
  const tbody = $("#proxyStatsBody");
  if (!tbody) return;
  const rows = [...state.authFarm.attempts].reverse();
  const summary = $("#proxyStatsSummary");
  if (summary) {
    const ok = rows.filter((r) => r.status === "ok").length;
    const fail = rows.filter((r) => r.status === "fail").length;
    const running = rows.filter((r) => r.status === "running").length;
    summary.textContent = `${rows.length} attempts · ${ok} ok · ${fail} fail · ${running} running`;
  }
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No farm attempts yet.</td></tr>';
    return;
  }
  tbody.innerHTML = "";
  for (const r of rows) {
    const tr = document.createElement("tr");
    const ts = r.started_at ? new Date(r.started_at * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "—";
    const dur = r.duration_s != null ? `${Number(r.duration_s).toFixed(1)}s` : "—";
    const proxyDisplay = r.proxy ? `${escapeHtml(r.proxy)}${r.proxy_origin ? ` <span class="muted" style="font-size:11px;">(${escapeHtml(r.proxy_origin)})</span>` : ""}` : '<span class="muted">none</span>';
    const statusClass = r.status === "ok" ? "badge-ok" : r.status === "fail" ? "badge-danger" : "badge-warn";
    tr.innerHTML = `
      <td>${escapeHtml(ts)}</td>
      <td>
        <div>${escapeHtml(r.phone || "")}</div>
        <div class="muted" style="font-size:11px;">${escapeHtml(r.email || "")}</div>
      </td>
      <td>${proxyDisplay}</td>
      <td><span class="badge ${statusClass}">${escapeHtml(r.status)}</span></td>
      <td>${escapeHtml(dur)}</td>
      <td class="muted" style="font-size:11px;">${escapeHtml(r.error || "")}</td>
    `;
    tbody.appendChild(tr);
  }
}

function downloadFile(filename, content, mime = "text/plain") {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  setTimeout(() => { URL.revokeObjectURL(url); a.remove(); }, 0);
}

function toCsv(rows, columns) {
  const header = columns.map((c) => csvCell(c.label)).join(",");
  const body = rows.map((r) => columns.map((c) => csvCell(typeof c.get === "function" ? c.get(r) : r[c.key])).join(","));
  return [header, ...body].join("\n");
}

function csvCell(v) {
  if (v == null) return "";
  const s = String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function exportProxyStats(format) {
  const rows = state.authFarm.attempts;
  if (!rows.length) { alert("No farm attempts to export yet."); return; }
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  if (format === "json") {
    downloadFile(`auth-farm-${stamp}.json`, JSON.stringify(rows, null, 2), "application/json");
  } else {
    const cols = [
      { key: "started_at", label: "started_at_iso", get: (r) => r.started_at ? new Date(r.started_at * 1000).toISOString() : "" },
      { key: "finished_at", label: "finished_at_iso", get: (r) => r.finished_at ? new Date(r.finished_at * 1000).toISOString() : "" },
      { key: "duration_s", label: "duration_s" },
      { key: "account_id", label: "account_id" },
      { key: "phone", label: "phone" },
      { key: "email", label: "email" },
      { key: "proxy", label: "proxy" },
      { key: "proxy_origin", label: "proxy_origin" },
      { key: "status", label: "status" },
      { key: "error", label: "error" },
      { key: "session_id", label: "session_id" },
    ];
    downloadFile(`auth-farm-${stamp}.csv`, toCsv(rows, cols), "text/csv");
  }
}

function clearProxyStats() {
  if (!confirm("Clear the farm-attempt history?")) return;
  state.authFarm.attempts = [];
  renderProxyStats();
}

// ── Cart-run exports ──────────────────────────────────────────────────────

function _cartFilenameStem(cart) {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const phone = (cart.account_phone || cart.account_id || "cart").toString().replace(/[^A-Za-z0-9_+-]/g, "_");
  return `cart-${phone}-${stamp}`;
}

function exportCartLogs(sessionId, format = "txt") {
  const cart = state.carts.get(sessionId);
  if (!cart) { alert("Cart session not found."); return; }
  const logs = cart.logs || [];
  if (!logs.length) { alert("No log lines captured for this cart yet."); return; }
  const stem = _cartFilenameStem(cart);
  if (format === "json") {
    downloadFile(`${stem}-logs.json`, JSON.stringify(logs, null, 2), "application/json");
    return;
  }
  const text = logs.map((l) => `[${l.ts || ""}] [${l.level || "info"}] ${l.msg || ""}`).join("\n");
  downloadFile(`${stem}-logs.txt`, text, "text/plain");
}

function exportInventory(format = "csv") {
  const items = state.inventory || [];
  if (!items.length) { alert("Inventory is empty."); return; }
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  if (format === "json") {
    downloadFile(`inventory-${stamp}.json`, JSON.stringify(items, null, 2), "application/json");
    return;
  }
  const cols = [
    { key: "purchased_at", label: "purchased_at" },
    { key: "purchase_id", label: "purchase_id" },
    { key: "account_phone", label: "account_phone" },
    { key: "account_name", label: "account_name" },
    { key: "event_name", label: "event_name" },
    { key: "event_date", label: "event_date" },
    { key: "event_venue", label: "event_venue" },
    { key: "event_url", label: "event_url" },
    { key: "ticket_name", label: "ticket_name" },
    { key: "ticket_currency", label: "ticket_currency" },
    { key: "ticket_price", label: "ticket_price" },
    { key: "quantity", label: "quantity" },
    { key: "total_price", label: "total_price" },
    { key: "purchase_status", label: "purchase_status" },
  ];
  downloadFile(`inventory-${stamp}.csv`, toCsv(items, cols), "text/csv");
}

function exportTasks() {
  const tasks = state.tasks || [];
  if (!tasks.length) { alert("No tasks to export."); return; }
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const cols = [
    { key: "id", label: "id" },
    { key: "account_id", label: "account_id" },
    { key: "account_phone", label: "account_phone" },
    { key: "account_email", label: "account_email" },
    { key: "event_url", label: "event_url" },
    { key: "ticket_tier", label: "ticket_tier" },
    { key: "min_price", label: "min_price" },
    { key: "max_price", label: "max_price" },
    { key: "quantity", label: "quantity" },
    { key: "presale_code", label: "presale_code" },
    { key: "mode", label: "mode" },
    { key: "scheduled_at", label: "scheduled_at" },
    { key: "scheduled_tz", label: "scheduled_tz" },
    { key: "status", label: "status" },
    { key: "last_error", label: "last_error" },
    { key: "created_at", label: "created_at" },
  ];
  downloadFile(`tasks-${stamp}.csv`, toCsv(tasks, cols), "text/csv");
}

function exportAccounts() {
  const accounts = state.accounts || [];
  if (!accounts.length) { alert("No accounts to export."); return; }
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const cols = [
    { key: "id", label: "id" },
    { key: "phone", label: "phone" },
    { key: "email", label: "email" },
    { key: "group_name", label: "group" },
    { key: "proxy", label: "proxy" },
    { key: "billing_country", label: "billing_country" },
    { key: "session_status", label: "session_status" },
    { key: "session_expires_in", label: "session_expires_in" },
    { key: "session_saved_at", label: "session_saved_at_unix", get: (r) => r.session_saved_at || "" },
    { key: "card_label", label: "card_label", get: (r) => r.card_label || "" },
    { key: "aycd_key", label: "aycd_key" },
    { key: "imap_email", label: "imap_email" },
    { key: "billing_name", label: "billing_name" },
    { key: "billing_email", label: "billing_email" },
    { key: "billing_phone", label: "billing_phone" },
    { key: "billing_postal", label: "billing_postal" },
  ];
  downloadFile(`accounts-${stamp}.csv`, toCsv(accounts, cols), "text/csv");
}

function exportCartEvents(sessionId, format = "csv") {
  const cart = state.carts.get(sessionId);
  if (!cart) { alert("Cart session not found."); return; }
  const events = cart.events || [];
  if (!events.length) { alert("No structured events captured for this cart yet."); return; }
  const stem = _cartFilenameStem(cart);
  if (format === "json") {
    downloadFile(`${stem}-events.json`, JSON.stringify(events, null, 2), "application/json");
    return;
  }
  const cols = [
    { key: "ts", label: "ts_iso", get: (r) => r.ts ? new Date(r.ts * 1000).toISOString() : "" },
    { key: "status", label: "status" },
    { key: "event_url", label: "event_url" },
    { key: "event_name", label: "event_name" },
    { key: "ticket_name", label: "ticket_name" },
    { key: "ticket_price", label: "ticket_price" },
    { key: "ticket_currency", label: "ticket_currency" },
    { key: "quantity", label: "quantity" },
    { key: "requested_quantity", label: "requested_quantity" },
    { key: "total_price", label: "total_price" },
    { key: "purchase_id", label: "purchase_id" },
    { key: "approval_id", label: "approval_id" },
    { key: "scheduled_at", label: "scheduled_at" },
    { key: "scheduled_tz", label: "scheduled_tz" },
    { key: "quantity_warning", label: "quantity_warning" },
  ];
  downloadFile(`${stem}-events.csv`, toCsv(events, cols), "text/csv");
}
// ── Cart helpers ──────────────────────────────────────────────────────────
function tierPriceLabel(tier) {
  return `${priceSymbol(tier.currency)}${(Number(tier.price_cents || 0) / 100).toFixed(2)}`;
}

function tierIsActionable(tier) {
  const status = String(tier?.status || "").toLowerCase();
  const secondary = String(tier?.secondary_status || "").toLowerCase();
  // Dice frequently leaves status="on-sale" while flagging sold-out via
  // secondary_status. Block on either field reporting a non-purchasable state.
  const blocked = new Set([
    "sold-out", "soldout",
    "off-sale", "offsale",
    "not-on-sale",
    "ended", "expired", "closed",
    "unavailable",
    "waitlist",
  ]);
  if (blocked.has(status) || blocked.has(secondary)) return false;
  return status === "on-sale" || Boolean(tier?.has_reserve_token);
}

function formatTierBadges(tier) {
  const badges = [];
  if (tier.status) badges.push(`<span class="tier-badge">${escapeHtml(tier.status)}</span>`);
  if (tier.secondary_status) badges.push(`<span class="tier-badge">${escapeHtml(tier.secondary_status)}</span>`);
  if (tier.price_tier_name) badges.push(`<span class="tier-badge">${escapeHtml(tier.price_tier_name)}</span>`);
  if (tier.price_tier_index != null) badges.push(`<span class="tier-badge">Tier ${escapeHtml(String(tier.price_tier_index))}</span>`);
  if (tier.has_reserve_token) badges.push('<span class="tier-badge">Reserve token ready</span>');
  return badges.join("");
}

async function launchCartRuns(accounts, common, options = {}) {
  const { perProfileCodes = null } = options;
  const failures = [];
  const splitMode = String(common.fire_mode || "").toLowerCase() === "split";
  const splitCutoff = splitMode ? Math.floor(accounts.length / 2) : 0;
  await Promise.all(accounts.map(async (account, idx) => {
    const perTask = { ...common, account };
    if (splitMode) {
      perTask.pre_drop_fire_window_enabled = idx < splitCutoff;
    }
    if (perProfileCodes) {
      const code = perProfileCodes.get(account.id);
      perTask.presale_code = code || "";
    }
    const res = await api.cartRun(perTask);
    if (res.ok) {
      state.carts.set(res.data.session_id, {
        session_id: res.data.session_id,
        account_phone: account.phone,
        account_id: account.id,
        status: "starting",
      });
    } else {
      failures.push(`${account.phone}: ${res.error}`);
    }
  }));
  renderCartGrid();
  return { failures };
}

function normalizeDiceEventInput(value) {
  const raw = String(value || "").trim().replace(/\s+/g, "").replace(/[,\s]+$/g, "");
  if (!raw) return "";
  if (/^[a-f0-9]{24}$/i.test(raw)) return `https://dice.fm/event/${raw.toLowerCase()}`;
  if (!raw.includes("/") && !raw.includes(".")) return `https://dice.fm/event/${raw}`;
  const withScheme = raw.startsWith("//")
    ? `https:${raw}`
    : (/^[a-z][a-z0-9+.-]*:\/\//i.test(raw) ? raw : `https://${raw}`);
  try {
    const url = new URL(withScheme);
    const parts = url.pathname.split("/").filter(Boolean);
    let slug = "";
    for (let i = 0; i < parts.length; i += 1) {
      if ((parts[i] === "event" || parts[i] === "events") && parts[i + 1]) {
        slug = parts[i + 1];
        break;
      }
    }
    if (!slug && parts.length && !["event", "events"].includes(parts[parts.length - 1])) {
      slug = parts[parts.length - 1];
    }
    return slug ? `https://dice.fm/event/${slug}` : "";
  } catch (_) {
    return "";
  }
}



// ── Dashboard / carts ─────────────────────────────────────────────────────
async function refreshDashboard() {
  const stats = await api.getStats();
  if (stats.ok && stats.data) {
    $("#statTotal").textContent = stats.data.total_accounts ?? "—";
    $("#statValid").textContent = stats.data.active_sessions ?? "—";
    $("#statExpiring").textContent = stats.data.expiring_sessions ?? "—";
    $("#statNeedsAuth").textContent = stats.data.no_session ?? "—";
  }
  renderCartGrid();
}

function formatCurrencyAmount(amount, currency = "USD") {
  const value = Number(amount);
  if (!Number.isFinite(value)) return "—";
  try {
    return new Intl.NumberFormat(undefined, {
      style: "currency",
      currency: (currency || "USD").toUpperCase(),
      maximumFractionDigits: 2,
    }).format(value);
  } catch {
    return `${priceSymbol(currency)}${value.toFixed(2)}`;
  }
}

function formatEventDateLabel(iso) {
  if (!iso) return "Date TBA";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

async function refreshInventory() {
  const res = await api.getInventoryItems();
  if (!res.ok) return;
  state.inventory = res.data || [];
  renderInventory();
}

function renderInventory() {
  const items = state.inventory || [];
  const tickets = items.reduce((sum, item) => sum + Math.max(1, Number(item.quantity || 1)), 0);
  const spent = items.reduce((sum, item) => sum + Number(item.total_price || 0), 0);
  const uniqueEvents = new Set(items.map((item) => item.event_url || `${item.event_name}|${item.event_date}`)).size;
  const currencies = [...new Set(items.map((item) => (item.ticket_currency || "USD").toUpperCase()))];

  $("#inventoryCount").textContent = String(items.length);
  $("#inventoryTickets").textContent = String(tickets);
  $("#inventorySpent").textContent = currencies.length <= 1
    ? formatCurrencyAmount(spent, currencies[0] || "USD")
    : "Mixed";
  $("#inventoryEvents").textContent = String(uniqueEvents);

  renderInventoryTable();
}

function renderInventoryTable() {
  const body = $("#inventoryBody");
  if (!body) return;
  const term = (state.inventorySearch || "").trim().toLowerCase();
  const rows = (state.inventory || []).filter((item) => {
    if (!term) return true;
    return [
      item.account_phone,
      item.account_name,
      item.event_name,
      item.event_venue,
      item.ticket_name,
      item.purchase_id,
    ].some((value) => String(value || "").toLowerCase().includes(term));
  });

  body.innerHTML = "";
  $("#inventoryEmpty").hidden = rows.length > 0;
  if (!rows.length) return;

  for (const item of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(formatPurchasedTime(item.purchased_at))}</td>
      <td>
        <div>${escapeHtml(item.account_phone || item.account_name || "—")}</div>
        <div class="muted">${escapeHtml(item.account_name || "—")}</div>
      </td>
      <td>
        <div>${escapeHtml(item.event_name || "—")}</div>
        <div class="muted">${escapeHtml(item.event_venue || "Venue TBA")}</div>
        <div class="muted">${escapeHtml(formatEventDateLabel(item.event_date))}</div>
      </td>
      <td>
        <div>${escapeHtml(item.ticket_name || "—")}</div>
        <div class="muted">${escapeHtml(item.ticket_type_id || "")}</div>
      </td>
      <td>${escapeHtml(String(item.quantity || 1))}</td>
      <td>${escapeHtml(formatCurrencyAmount(item.ticket_price, item.ticket_currency))}</td>
      <td>${escapeHtml(formatCurrencyAmount(item.total_price, item.ticket_currency))}</td>
      <td><span class="muted">${escapeHtml(item.purchase_id || "—")}</span></td>
      <td class="col-actions">
        ${item.event_url ? `<button class="btn-icon" data-inventory-action="open" data-url="${escapeHtml(item.event_url)}">↗</button>` : ""}
      </td>
    `;
    body.appendChild(tr);
  }

  body.querySelectorAll("[data-inventory-action='open']").forEach((btn) => {
    btn.addEventListener("click", async () => {
      await api.openExternal(btn.dataset.url);
    });
  });
}

function renderCartGrid() {
  const grid = $("#cartGrid");
  if (state.carts.size === 0) {
    grid.innerHTML = '<div class="empty-state">No carts running. Click "Start cart run" to reserve tickets.</div>';
    updateCartApprovalBar();
    return;
  }
  grid.innerHTML = "";
  for (const cart of state.carts.values()) {
    grid.appendChild(renderCartCard(cart));
  }
  updateCartApprovalBar();
}

function approvableCarts() {
  return [...state.carts.values()]
    .filter((cart) => cart.status === "reserved" && !cart.approval_local_sent)
    .sort((a, b) => {
      const aExpiry = Number(a.ttl_until || 0) || Number.POSITIVE_INFINITY;
      const bExpiry = Number(b.ttl_until || 0) || Number.POSITIVE_INFINITY;
      if (aExpiry !== bExpiry) return aExpiry - bExpiry;
      return String(a.session_id || "").localeCompare(String(b.session_id || ""));
    });
}

function readApproveCountInput() {
  const input = $("#approveCountInput");
  if (!input) return null;
  const raw = String(input.value ?? "").trim();
  if (!raw) return null;
  const parsed = parseInt(raw, 10);
  if (!Number.isFinite(parsed)) return null;
  return Math.max(1, parsed);
}

function syncApproveCountButtonLabel() {
  const input = $("#approveCountInput");
  const btn = $("#approveCountBtn");
  if (!input || !btn) return;
  const reserved = approvableCarts().length;
  const value = readApproveCountInput();
  const displayCount = Math.min(value ?? 1, Math.max(reserved, 1));
  const label = `Approve the next ${displayCount} reserved cart${displayCount === 1 ? "" : "s"}`;
  btn.textContent = "Approve next";
  btn.title = label;
  btn.setAttribute("aria-label", label);
}

function normalizeApproveCountInput() {
  const input = $("#approveCountInput");
  if (!input) return 1;
  const reserved = approvableCarts().length;
  const max = Math.max(reserved, 1);
  const next = Math.min(readApproveCountInput() ?? 1, max);
  input.value = String(next);
  return next;
}

function updateCartApprovalBar() {
  const bar = $("#cartApprovalBar");
  const summary = $("#cartApprovalSummary");
  const dismissOneBtn = $("#dismissOneBtn");
  const dismissAllBtn = $("#dismissAllBtn");
  const approveOneBtn = $("#approveOneBtn");
  const approveCountInput = $("#approveCountInput");
  const approveCountBtn = $("#approveCountBtn");
  const approveAllBtn = $("#approveAllBtn");
  if (
    !bar || !summary || !dismissOneBtn || !dismissAllBtn
    || !approveOneBtn || !approveCountInput || !approveCountBtn || !approveAllBtn
  ) return;

  const reserved = approvableCarts();
  const count = reserved.length;
  const nextExpiry = reserved[0]?.ttl_until;
  const nextExpiryText = nextExpiry && Number.isFinite(Number(nextExpiry))
    ? ` · next expires ${Math.max(0, Math.ceil(Number(nextExpiry) - Date.now() / 1000))}s`
    : "";

  bar.hidden = count === 0;
  summary.textContent = count === 0
    ? "0 carts awaiting approval"
    : `${count} cart${count === 1 ? "" : "s"} awaiting approval${nextExpiryText}`;

  const max = Math.max(count, 1);
  const current = readApproveCountInput();
  approveCountInput.max = String(max);
  if (current != null && current > max && count > 0) approveCountInput.value = String(max);

  const disabled = count === 0 || state.bulkApprovalBusy;
  dismissOneBtn.disabled = disabled;
  dismissAllBtn.disabled = disabled;
  approveOneBtn.disabled = disabled;
  approveCountInput.disabled = disabled;
  approveCountBtn.disabled = disabled;
  approveAllBtn.disabled = disabled;
  syncApproveCountButtonLabel();
}

async function approveCartSession(sessionId, { statusEl = null } = {}) {
  const cart = state.carts.get(sessionId);
  if (!cart) return { ok: false, error: "Cart not found" };
  cart.approval_local_sent = true;
  state.carts.set(sessionId, cart);
  if (statusEl) statusEl.textContent = "approval sent — completing purchase…";
  renderCartGrid();

  const res = await api.sessionApprove(sessionId);
  if (!res.ok) {
    const current = state.carts.get(sessionId);
    if (current) {
      current.approval_local_sent = false;
      state.carts.set(sessionId, current);
    }
    if (statusEl) statusEl.textContent = "approve failed: " + res.error;
    renderCartGrid();
  }
  return res;
}

async function dismissCartSession(sessionId) {
  const cart = state.carts.get(sessionId);
  if (!cart) return { ok: false, error: "Cart not found" };
  const res = await api.sessionStop(sessionId);
  if (res.ok) {
    state.carts.delete(sessionId);
    renderCartGrid();
  }
  return res;
}

async function approveCartBatch(count) {
  const queue = approvableCarts().slice(0, Math.max(1, count));
  if (!queue.length) return;
  state.bulkApprovalBusy = true;
  updateCartApprovalBar();
  const results = await Promise.all(queue.map(async (cart) => ({
    cart,
    res: await approveCartSession(cart.session_id),
  })));
  state.bulkApprovalBusy = false;
  updateCartApprovalBar();
  const failures = results
    .filter(({ res }) => !res.ok)
    .map(({ cart, res }) => `${cart.account_phone || cart.session_id}: ${res.error}`);
  if (failures.length) {
    alert("Some approvals failed:\n" + failures.join("\n"));
  }
}

async function dismissCartBatch(count) {
  const queue = approvableCarts().slice(0, Math.max(1, count));
  if (!queue.length) return;
  state.bulkApprovalBusy = true;
  updateCartApprovalBar();
  const results = await Promise.all(queue.map(async (cart) => ({
    cart,
    res: await dismissCartSession(cart.session_id),
  })));
  state.bulkApprovalBusy = false;
  updateCartApprovalBar();
  const failures = results
    .filter(({ res }) => !res.ok)
    .map(({ cart, res }) => `${cart.account_phone || cart.session_id}: ${res.error}`);
  if (failures.length) {
    alert("Some dismissals failed:\n" + failures.join("\n"));
  }
}

function renderCartCard(cart) {
  const el = document.createElement("div");
  el.className = "cart-card";
  el.dataset.sid = cart.session_id;
  const quantity = Math.max(1, parseInt(cart.quantity, 10) || 1);
  const explicitTotal = Number(cart.total_price);
  const unitPrice = Number(cart.ticket_price);
  const displayTotal = Number.isFinite(explicitTotal) && explicitTotal > 0
    ? explicitTotal
    : Number.isFinite(unitPrice) && unitPrice > 0
      ? unitPrice * quantity
      : NaN;
  const price = Number.isFinite(displayTotal) && displayTotal > 0
    ? formatCurrencyAmount(displayTotal, cart.ticket_currency)
    : "—";
  const scheduleLine = cart.scheduled_at
    ? `${cart.status === "armed" ? "Drop" : "Scheduled"}: ${cart.scheduled_at.replace("T", " ")}${cart.scheduled_tz ? ` ${cart.scheduled_tz}` : ""}`
    : "";
  const countdownLabel = cart.status === "armed" ? "ARMED" : "--:--";
  const canApprove = cart.status === "reserved" && !cart.approval_local_sent;
  const approvalLine = cart.status === "reserved"
    ? (
      cart.approval_local_sent
        ? "Approval sent — waiting for checkout"
        : cart.approval_channel === "webhook"
          ? `Awaiting app or webhook approval${cart.approval_id ? ` · ${cart.approval_id}` : ""}`
          : "Awaiting in-app approval"
    )
    : "";
  const logLines = (cart.logs || [])
    .slice(-200)
    .map((l) => `<div class="log-line ${l.level || "info"}"><span class="log-ts">${l.ts}</span>${escapeHtml(l.msg)}</div>`)
    .join("");
  el.innerHTML = `
    <div class="cart-card-top">
      <div>
        <div class="cart-account">${escapeHtml(cart.account_phone || "cart")}</div>
        <div class="cart-ticket">${escapeHtml(cart.event_name || "…")}<br>${escapeHtml(cart.ticket_name || "")}</div>
      </div>
      <div class="cart-price">${price}</div>
    </div>
    <div class="cart-countdown" data-countdown="${cart.ttl_until || 0}">${escapeHtml(countdownLabel)}</div>
    <div class="cart-actions">
      <button class="btn btn-ghost btn-sm" data-cart-action="logs">Logs</button>
      <button class="btn btn-ghost btn-sm" data-cart-action="export-logs" title="Export log lines">Export logs</button>
      <button class="btn btn-ghost btn-sm" data-cart-action="export-events" title="Export structured cart events">Export events</button>
      <button class="btn btn-ghost btn-sm" data-cart-action="decline">Decline</button>
      <button class="btn btn-primary btn-sm" data-cart-action="approve" ${canApprove ? "" : "disabled"}>Approve</button>
    </div>
    <div class="muted" style="font-size:11px;" data-cart-status>${escapeHtml(cart.status || "reserved")}${approvalLine ? `<br>${escapeHtml(approvalLine)}` : ""}${scheduleLine ? `<br>${escapeHtml(scheduleLine)}` : ""}</div>
    <div class="cart-log-pane log-pane" ${cart.showLogs ? "" : "hidden"}>${logLines}</div>
  `;
  el.querySelector('[data-cart-action="logs"]').addEventListener("click", () => {
    const c = state.carts.get(cart.session_id);
    if (c) { c.showLogs = !c.showLogs; state.carts.set(cart.session_id, c); renderCartGrid(); }
  });
  el.querySelector('[data-cart-action="export-logs"]').addEventListener("click", () => {
    exportCartLogs(cart.session_id);
  });
  el.querySelector('[data-cart-action="export-events"]').addEventListener("click", () => {
    exportCartEvents(cart.session_id);
  });
  el.querySelector('[data-cart-action="decline"]').addEventListener("click", async () => {
    await api.sessionStop(cart.session_id);
    state.carts.delete(cart.session_id);
    renderCartGrid();
  });
  el.querySelector('[data-cart-action="approve"]').addEventListener("click", async () => {
    const card = el.querySelector('[data-cart-status]');
    await approveCartSession(cart.session_id, { statusEl: card });
  });
  return el;
}

function browserTz() {
  try { return Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC"; }
  catch { return "UTC"; }
}

const COMMON_TIMEZONES = [
  { value: "America/New_York", label: "New York (ET)" },
  { value: "America/Chicago", label: "Chicago (CT)" },
  { value: "America/Denver", label: "Denver (MT)" },
  { value: "America/Los_Angeles", label: "Los Angeles (PT)" },
  { value: "Europe/London", label: "London" },
  { value: "Europe/Paris", label: "Paris / CET" },
  { value: "Asia/Tokyo", label: "Tokyo" },
  { value: "Australia/Sydney", label: "Sydney" },
  { value: "UTC", label: "UTC" },
];

function tzSelectOptions(selected) {
  const active = selected || browserTz();
  const zones = [...COMMON_TIMEZONES];
  if (active && !zones.some((z) => z.value === active)) {
    zones.unshift({ value: active, label: `${active} (local)` });
  }
  return zones
    .map((z) => `<option value="${escapeHtml(z.value)}" ${z.value === active ? "selected" : ""}>${escapeHtml(z.label)}</option>`)
    .join("");
}

function priceSymbol(ccy) {
  const c = (ccy || "USD").toUpperCase();
  return { USD: "$", GBP: "£", EUR: "€", CAD: "C$", AUD: "A$" }[c] || `${c} `;
}

$("#newCartBtn").addEventListener("click", () => openCartModal());
$("#dismissOneBtn")?.addEventListener("click", async () => {
  await dismissCartBatch(1);
});
$("#dismissAllBtn")?.addEventListener("click", async () => {
  await dismissCartBatch(approvableCarts().length);
});
$("#approveOneBtn")?.addEventListener("click", async () => {
  await approveCartBatch(1);
});
$("#approveCountBtn")?.addEventListener("click", async () => {
  const count = normalizeApproveCountInput();
  await approveCartBatch(count);
});
$("#approveAllBtn")?.addEventListener("click", async () => {
  await approveCartBatch(approvableCarts().length);
});
$("#approveCountInput")?.addEventListener("input", syncApproveCountButtonLabel);
$("#approveCountInput")?.addEventListener("blur", () => {
  normalizeApproveCountInput();
  syncApproveCountButtonLabel();
});

async function openCartModal(options = {}) {
  const {
    title = "Start cart run",
    submitLabel = "Start",
    defaults = {},
    lockEventUrl = false,
    lockTier = false,
    lockMode = false,
    hidePriceRange = false,
  } = options;
  const acctsRes = await api.getAccountsWithValidSession(null);
  let accounts = acctsRes.ok ? acctsRes.data || [] : [];
  const groupsRes = await api.getGroups();
  const groups = groupsRes.ok ? (groupsRes.data || []) : [];
  const labelsRes = await api.getCardLabels();
  const cardLabels = labelsRes.ok ? (labelsRes.data || []) : [];
  const codePoolsRes = await api.getCodePools();
  const codePools = codePoolsRes.ok ? (codePoolsRes.data || []) : [];
  const presetEventUrl = defaults.event_url || "";
  const presetTier = defaults.ticket_tier || "";
  const presetMode = defaults.mode || "manual";
  const presetCode = defaults.presale_code || "";
  const presetProfilesRaw = parseInt(defaults.max_profiles, 10);
  const presetProfiles = Number.isFinite(presetProfilesRaw) && presetProfilesRaw > 0 ? presetProfilesRaw : null;
  const presetStrategyRaw = String(defaults.tier_strategy || "").toLowerCase();
  const presetStrategy = ["cheapest", "most_expensive"].includes(presetStrategyRaw)
    ? presetStrategyRaw
    : "cheapest";
  const presetFireMode = (() => {
    const explicit = String(defaults.fire_mode || "").toLowerCase();
    if (["predrop", "ondrop", "split"].includes(explicit)) return explicit;
    return defaults.pre_drop_fire_window_enabled === true ? "predrop" : "ondrop";
  })();
  const presetKeywords = (() => {
    if (typeof defaults.tier_keywords === "string") return defaults.tier_keywords;
    if (Array.isArray(defaults.tier_keywords)) return defaults.tier_keywords.join(" ");
    return presetTier || "";
  })();
  const presetTaskMin = defaults.target_min_price != null ? String(defaults.target_min_price) : "";
  const presetTaskMax = defaults.target_max_price != null ? String(defaults.target_max_price) : "";
  const presetTierInfo = defaults.selected_tier || null;
  const tierInfoHtml = lockTier && presetTierInfo
    ? `
      <div class="monitor-meta">
        <div class="monitor-meta-item">
          <div class="monitor-meta-label">Selected tier</div>
          <div class="monitor-meta-value">${escapeHtml(presetTierInfo.name || presetTier || "—")}</div>
        </div>
        <div class="monitor-meta-item">
          <div class="monitor-meta-label">Price</div>
          <div class="monitor-meta-value">${escapeHtml(tierPriceLabel(presetTierInfo))}</div>
        </div>
        <div class="monitor-meta-item">
          <div class="monitor-meta-label">Status</div>
          <div class="monitor-meta-value">${escapeHtml(presetTierInfo.status || "—")}</div>
        </div>
        <div class="monitor-meta-item">
          <div class="monitor-meta-label">Max order</div>
          <div class="monitor-meta-value">${escapeHtml(String(presetTierInfo.max_per_order || 1))}</div>
        </div>
      </div>
    `
    : "";
  const tierInputHtml = lockTier
    ? ""
    : `
      <div>
        <div style="margin-bottom:6px;">Tier selection</div>
        <div class="chip-row" id="c_strategy_chips" style="display:flex;gap:6px;flex-wrap:wrap;">
          <button type="button" class="chip${presetStrategy === "cheapest" ? " active" : ""}" data-strategy="cheapest">Cheapest available</button>
          <button type="button" class="chip${presetStrategy === "most_expensive" ? " active" : ""}" data-strategy="most_expensive">Most expensive available</button>
        </div>
        <label style="margin-top:6px;display:block;">
          <span>Keyword filters (optional)</span>
          <input id="c_keywords" value="${escapeHtml(presetKeywords)}" placeholder="stage vip -parking -resale" />
        </label>
        <p class="muted" style="font-size:12px; margin:4px 0 0;">Tokens included = positive (any match keeps the tier). Tokens prefixed with <code>-</code> = negative (any match excludes the tier). Match against tier name, description, and tier label. Example: <code>stage vip -parking</code> keeps tiers mentioning stage or vip but drops parking passes.</p>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:6px;">
          <label><span>Min price (optional)</span><input id="c_task_min" type="text" inputmode="decimal" pattern="[0-9]*\.?[0-9]*" value="${escapeHtml(presetTaskMin)}" placeholder="any" /></label>
          <label><span>Max price (optional)</span><input id="c_task_max" type="text" inputmode="decimal" pattern="[0-9]*\.?[0-9]*" value="${escapeHtml(presetTaskMax)}" placeholder="any" /></label>
        </div>
        <p class="muted" style="font-size:12px; margin:4px 0 0;">Filters narrow the candidate pool first; the strategy then picks from what remains. On tier-fallback, the same strategy picks the next-best tier from the still-eligible pool.</p>
      </div>
    `;
  const rulesHtml = `
    <div>
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
        <span>Auto-buy rules</span>
        <button type="button" class="btn btn-ghost btn-sm" id="c_rules_add">+ Add rule</button>
      </div>
      <div id="c_rules_list"></div>
      <p class="muted" style="font-size:12px; margin:4px 0 0;">Each rule: buy N tickets if the tier price is between $min and $max. All three fields are optional — qty defaults to 1, blank min/max means no bound. Evaluated in order; first matching rule wins. Leave the list empty to buy whatever the strategy picks at any price.</p>
    </div>
  `;
  const tierBlock = `${tierInfoHtml}${tierInputHtml}${rulesHtml}`;
  openModal({
    title,
    bodyHtml: `
      <div class="form-grid">
        <label><span>Event URL</span>
          <div style="display:flex;gap:6px;">
            <input id="c_url" style="flex:1;" placeholder="https://dice.fm/event/..." value="${escapeHtml(presetEventUrl)}" ${lockEventUrl ? "readonly" : ""} />
            <button type="button" class="btn btn-ghost btn-sm" id="c_fetch_tiers">Fetch tiers</button>
          </div>
        </label>
        <div class="tier-pick" id="c_tier_pick_wrap" hidden>
          <div class="tier-pick-head">
            <div>
              <div class="tier-pick-title" id="c_tier_pick_label">Acceptable tiers</div>
              <div class="tier-pick-count" id="c_tier_pick_count"></div>
            </div>
            <div class="tier-pick-tools" id="c_tier_pick_tools">
              <button type="button" class="btn btn-ghost btn-sm" id="c_tier_pick_all">Select all</button>
              <button type="button" class="btn btn-ghost btn-sm" id="c_tier_pick_none">Clear</button>
            </div>
          </div>
          <div class="tier-pick-list" id="c_tier_pick_list"></div>
          <div class="tier-pick-notice" id="c_tier_pick_hint">Picked tiers override the keyword filter — keywords below are ignored when at least one tier is checked.</div>
        </div>
        <label><span>Account group</span>
          <select id="c_group">
            <option value="">All groups</option>
            ${groups.map((g) => `<option value="${g.id}">${escapeHtml(g.name)}</option>`).join("")}
          </select>
        </label>
        <div class="profile-pool">
          <div class="profile-pool-label">Valid profile pool</div>
          <div class="profile-pool-value" id="c_pool_value">${accounts.length} profile${accounts.length === 1 ? "" : "s"} ready</div>
          <div class="profile-pool-hint" id="c_pool_hint">
            ${accounts.length
              ? "Launches rotate through your valid session pool so the same profiles are not always used first."
              : "No accounts with a valid session are available. Run Auth Farm first."}
          </div>
        </div>
        ${tierBlock}
        <div class="profile-launcher">
          <input id="c_profiles" type="hidden" value="${Math.min(Math.max(accounts.length, 1), presetProfiles || Math.max(accounts.length, 1))}" />
          <div class="profile-launcher-top">
            <div>
              <div class="profile-launcher-label">Profiles to launch</div>
              <div class="profile-launcher-value" id="c_profiles_value">All available</div>
            </div>
            <div class="profile-launcher-stepper">
              <button type="button" class="btn btn-ghost btn-sm" id="c_profiles_down">-</button>
              <button type="button" class="btn btn-ghost btn-sm" id="c_profiles_up">+</button>
            </div>
          </div>
          <input id="c_profiles_range" class="profile-launcher-range" type="range" min="1" max="${Math.max(accounts.length, 1)}" value="${Math.min(Math.max(accounts.length, 1), presetProfiles || Math.max(accounts.length, 1))}" />
          <div class="profile-launcher-footer">
            <div class="profile-launcher-hint" id="c_profiles_hint">Launching all available profiles</div>
            <div class="profile-launcher-presets" id="c_profiles_presets"></div>
          </div>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">
          <label><span>Scheduled drop (leave blank to run immediately)</span>
            <input id="c_sched" type="datetime-local" step="1" />
          </label>
          <label><span>Timezone</span>
            <select id="c_tz">${tzSelectOptions("")}</select>
          </label>
        </div>
        <p class="muted" style="font-size:11px; margin:-6px 0 0;">At T-3s the cart starts polling tiers; it fires the moment a reserve token appears at drop.</p>
        <label><span>Fire timing</span>
          <select id="c_fire_mode">
            <option value="predrop" ${presetFireMode === "predrop" ? "selected" : ""}>Predrop ATC (fire within 4 min of drop)</option>
            <option value="ondrop" ${presetFireMode === "ondrop" ? "selected" : ""}>On-drop ATC (hold until drop instant)</option>
            <option value="split" ${presetFireMode === "split" ? "selected" : ""}>Split — half predrop, half on-drop</option>
          </select>
          <p class="muted" style="font-size:11px; margin:4px 0 0;">Predrop: fire as soon as a reserve token is available within 4 min of drop. On-drop: hold strictly until the scheduled instant. Split: half the launched profiles use predrop, the other half on-drop.</p>
        </label>
        <label><span>Card label</span>
          <select id="c_card_label">
            <option value="">Use account default card</option>
            ${cardLabels.map((lbl) => `<option value="${escapeHtml(lbl)}">${escapeHtml(lbl)}</option>`).join("")}
          </select>
        </label>
        <p class="muted" style="font-size:11px; margin:-6px 0 0;">When a label is picked, each account uses its assigned card with that label. Manage cards under the Cards tab.</p>
        <label><span>Mode</span>
          <select id="c_mode" ${lockMode ? "disabled" : ""}>
            <option value="manual" ${presetMode === "manual" ? "selected" : ""}>Reserve only (manual approve)</option>
            <option value="auto" ${presetMode === "auto" ? "selected" : ""}>Reserve + auto-checkout</option>
          </select>
        </label>
        <div>
          <div style="margin-bottom:6px;">Access code (optional — for locked / presale events)</div>
          <div class="chip-row" id="c_code_mode_chips" style="display:flex;gap:6px;flex-wrap:wrap;">
            <button type="button" class="chip active" data-code-mode="none">No code</button>
            <button type="button" class="chip" data-code-mode="single">Single code</button>
            <button type="button" class="chip" data-code-mode="pool">Use code pool</button>
          </div>
          <div id="c_code_single_wrap" style="margin-top:6px;display:none;">
            <input id="c_code" placeholder="PRESALE2026" value="${escapeHtml(presetCode)}" />
          </div>
          <div id="c_code_pool_wrap" style="margin-top:6px;display:none;">
            ${codePools.length
              ? `<select id="c_code_pool">
                   ${codePools.map((p) => `<option value="${p.id}">${escapeHtml(p.name)} (${p.code_count || 0} code${p.code_count === 1 ? "" : "s"})</option>`).join("")}
                 </select>
                 <p class="muted" style="font-size:12px; margin:4px 0 0;">When the drop launches, one code is randomly assigned to each running profile. If the pool has fewer codes than profiles, profiles without a code will run with no access code.</p>`
              : `<p class="muted" style="font-size:12px; margin:0;">No code pools yet. Create one from the <strong>Code Pools</strong> tab first.</p>`}
          </div>
        </div>
      </div>
    `,
    footerHtml: `<button class="btn btn-ghost" data-close>Cancel</button>
                 <button class="btn btn-primary" id="startCartBtn">${escapeHtml(submitLabel)}</button>`,
    onMount: () => {
      const profilesInput = $("#c_profiles");
      const profilesRange = $("#c_profiles_range");
      const profilesValue = $("#c_profiles_value");
      const profilesHint = $("#c_profiles_hint");
      const profilesPresets = $("#c_profiles_presets");
      const profilesDown = $("#c_profiles_down");
      const profilesUp = $("#c_profiles_up");

      const tierPickWrap = $("#c_tier_pick_wrap");
      const tierPickList = $("#c_tier_pick_list");
      const tierPickHint = $("#c_tier_pick_hint");
      const tierPickLabel = $("#c_tier_pick_label");
      const tierPickCount = $("#c_tier_pick_count");
      const tierPickTools = $("#c_tier_pick_tools");
      const fetchTiersBtn = $("#c_fetch_tiers");
      let previewTiers = [];
      let previewLocked = false;
      let lastPreviewedUrl = "";

      const updateTierPickCount = () => {
        if (!tierPickCount) return;
        if (previewLocked || !previewTiers.length) { tierPickCount.textContent = ""; return; }
        const total = previewTiers.length;
        const checked = tierPickList?.querySelectorAll("input[type=checkbox]:checked").length || 0;
        tierPickCount.textContent = `${checked} of ${total} selected`;
      };

      const showPanel = ({ title, hint, hintWarn = false, showTools = false, html = "" }) => {
        if (!tierPickWrap) return;
        tierPickWrap.hidden = false;
        if (tierPickLabel) tierPickLabel.textContent = title;
        if (tierPickHint) {
          tierPickHint.textContent = hint;
          tierPickHint.classList.toggle("warn", Boolean(hintWarn));
        }
        if (tierPickTools) tierPickTools.style.display = showTools ? "" : "none";
        if (tierPickList) tierPickList.innerHTML = html;
        updateTierPickCount();
      };

      const renderTierPicker = () => {
        if (previewLocked) {
          showPanel({
            title: "Locked event",
            hint: "This event requires an access code. Tier prefetch isn't possible until the code unlocks the listing — use the keyword filter / strategy below.",
          });
          return;
        }
        if (!previewTiers.length) {
          if (tierPickWrap) tierPickWrap.hidden = true;
          return;
        }
        const html = previewTiers.map((tier) => {
          const id = String(tier.id || "");
          const price = tierPriceLabel(tier);
          const status = String(tier.status || "");
          const actionable = tierIsActionable(tier);
          const meta = [];
          if (status) meta.push(`<span>${escapeHtml(status)}</span>`);
          if (tier.has_reserve_token) meta.push(`<span class="tier-pick-meta-dot">reserve token ready</span>`);
          if (tier.max_per_order) meta.push(`<span class="tier-pick-meta-dot">max ${escapeHtml(String(tier.max_per_order))}</span>`);
          return `<label class="tier-pick-row${actionable ? " checked" : " disabled"}">
            <input type="checkbox" data-tier-id="${escapeHtml(id)}" ${actionable ? "checked" : ""} ${actionable ? "" : "disabled"} />
            <div class="tier-pick-body">
              <div class="tier-pick-name">${escapeHtml(tier.name || "Unnamed tier")}</div>
              <div class="tier-pick-meta">${meta.join("")}</div>
            </div>
            <div class="tier-pick-price">${escapeHtml(price)}</div>
          </label>`;
        }).join("");
        showPanel({
          title: "Acceptable tiers",
          hint: "Picked tiers override the keyword filter — keywords below are ignored when at least one tier is checked.",
          showTools: true,
          html,
        });
      };

      const fetchTiers = async () => {
        const url = normalizeDiceEventInput($("#c_url").value);
        if (!url) {
          alert("Enter a full DICE event URL first.");
          return;
        }
        if (!fetchTiersBtn) return;
        const prevLabel = fetchTiersBtn.textContent;
        fetchTiersBtn.disabled = true;
        fetchTiersBtn.textContent = "Fetching…";
        try {
          const res = await api.eventPreview(url);
          if (!res?.ok) throw new Error(res?.error || "preview failed");
          const data = res.data || {};
          previewLocked = Boolean(data.event_locked);
          previewTiers = Array.isArray(data.tiers) ? data.tiers : [];
          lastPreviewedUrl = url;
          if (!previewLocked && !previewTiers.length) {
            showPanel({
              title: "No tiers found",
              hint: data.tiers_error
                ? `Dice didn't return any tiers (${data.tiers_error}). Falling back to the keyword filter / strategy below.`
                : "Dice didn't return any tiers for this event yet — falling back to the keyword filter / strategy below.",
              hintWarn: true,
            });
          } else {
            renderTierPicker();
          }
        } catch (err) {
          previewTiers = [];
          previewLocked = false;
          showPanel({
            title: "Prefetch failed",
            hint: `Couldn't prefetch tiers: ${err.message || err}. The cart will still launch using the keyword filter / strategy below.`,
            hintWarn: true,
          });
        } finally {
          fetchTiersBtn.disabled = false;
          fetchTiersBtn.textContent = prevLabel;
        }
      };

      fetchTiersBtn?.addEventListener("click", fetchTiers);
      $("#c_url")?.addEventListener("input", () => {
        if (lastPreviewedUrl && normalizeDiceEventInput($("#c_url").value) !== lastPreviewedUrl) {
          previewTiers = [];
          previewLocked = false;
          lastPreviewedUrl = "";
          if (tierPickWrap) tierPickWrap.hidden = true;
        }
      });
      tierPickList?.addEventListener("change", (e) => {
        if (e.target?.matches?.("input[type=checkbox]")) {
          const row = e.target.closest(".tier-pick-row");
          if (row && !row.classList.contains("disabled")) row.classList.toggle("checked", e.target.checked);
          updateTierPickCount();
        }
      });
      $("#c_tier_pick_all")?.addEventListener("click", () => {
        tierPickList?.querySelectorAll("input[type=checkbox]").forEach((cb) => {
          if (cb.disabled) return;
          cb.checked = true;
          cb.closest(".tier-pick-row")?.classList.add("checked");
        });
        updateTierPickCount();
      });
      $("#c_tier_pick_none")?.addEventListener("click", () => {
        tierPickList?.querySelectorAll("input[type=checkbox]").forEach((cb) => {
          cb.checked = false;
          cb.closest(".tier-pick-row")?.classList.remove("checked");
        });
        updateTierPickCount();
      });
      const collectAllowedTierIds = () => {
        if (previewLocked || !previewTiers.length) return [];
        return Array.from(tierPickList?.querySelectorAll("input[type=checkbox]:checked") || [])
          .map((cb) => cb.dataset.tierId)
          .filter(Boolean);
      };

      const seedCartRules = (() => {
        const fromDefaults = defaults.price_rules;
        if (Array.isArray(fromDefaults) && fromDefaults.length) return fromDefaults;
        if (defaults.target_max_price != null) {
          const seed = { quantity: Math.max(1, parseInt(defaults.quantity, 10) || 1), max_price: Number(defaults.target_max_price) };
          if (defaults.target_min_price != null) seed.min_price = Number(defaults.target_min_price);
          return [seed];
        }
        if (Array.isArray(state.settings.defaultPriceRules)) return state.settings.defaultPriceRules;
        return [];
      })();
      const collectCartRules = mountPriceRuleList($("#c_rules_list"), $("#c_rules_add"), seedCartRules);

      let activeStrategy = presetStrategy;
      const strategyChips = $("#c_strategy_chips");
      const updateStrategyUi = () => {
        if (!strategyChips) return;
        strategyChips.querySelectorAll("[data-strategy]").forEach((btn) => {
          btn.classList.toggle("active", btn.dataset.strategy === activeStrategy);
        });
      };
      strategyChips?.querySelectorAll("[data-strategy]").forEach((btn) => {
        btn.addEventListener("click", () => {
          activeStrategy = btn.dataset.strategy;
          updateStrategyUi();
        });
      });
      updateStrategyUi();

      let codeMode = presetCode ? "single" : "none";
      const codeModeChips = $("#c_code_mode_chips");
      const codeSingleWrap = $("#c_code_single_wrap");
      const codePoolWrap = $("#c_code_pool_wrap");
      const updateCodeModeUi = () => {
        codeModeChips?.querySelectorAll("[data-code-mode]").forEach((btn) => {
          btn.classList.toggle("active", btn.dataset.codeMode === codeMode);
        });
        if (codeSingleWrap) codeSingleWrap.style.display = codeMode === "single" ? "" : "none";
        if (codePoolWrap) codePoolWrap.style.display = codeMode === "pool" ? "" : "none";
      };
      codeModeChips?.querySelectorAll("[data-code-mode]").forEach((btn) => {
        btn.addEventListener("click", () => {
          if (btn.dataset.codeMode === "pool" && !codePools.length) {
            alert('No code pools yet. Create one from the "Code Pools" tab first.');
            return;
          }
          codeMode = btn.dataset.codeMode;
          updateCodeModeUi();
        });
      });
      updateCodeModeUi();

      const selectedCount = () => accounts.length;
      const clampProfileCount = (value, maxCount) => {
        if (!maxCount) return 1;
        return Math.max(1, Math.min(maxCount, parseInt(value, 10) || 1));
      };
      const buildProfilePresets = (maxCount) => {
        if (!profilesPresets) return;
        if (!maxCount) {
          profilesPresets.innerHTML = "";
          return;
        }
        const half = Math.max(1, Math.ceil(maxCount / 2));
        const options = [
          { label: "1", value: 1 },
          ...(maxCount >= 3 ? [{ label: "3", value: Math.min(3, maxCount) }] : []),
          ...(maxCount >= 4 ? [{ label: "Half", value: half }] : []),
          { label: "All", value: maxCount },
        ];
        const seen = new Set();
        profilesPresets.innerHTML = options
          .filter((item) => {
            const key = `${item.label}:${item.value}`;
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
          })
          .map((item) => `<button type="button" class="chip" data-profile-preset="${item.value}">${escapeHtml(item.label)}</button>`)
          .join("");
        profilesPresets.querySelectorAll("[data-profile-preset]").forEach((btn) => {
          btn.addEventListener("click", () => {
            setProfilesValue(Number(btn.dataset.profilePreset), true);
          });
        });
      };
      const renderProfilesUi = (count, maxCount) => {
        if (profilesInput) profilesInput.value = String(count);
        if (profilesRange) {
          profilesRange.max = String(Math.max(maxCount, 1));
          profilesRange.value = String(clampProfileCount(count, maxCount));
          profilesRange.disabled = maxCount < 2;
        }
        if (profilesValue) {
          profilesValue.textContent = maxCount <= 0
            ? "No profiles available"
            : count === maxCount
              ? (maxCount === 1 ? "1 profile" : `All ${maxCount} available`)
              : `${count} profile${count === 1 ? "" : "s"}`;
        }
        if (profilesHint) {
          profilesHint.textContent = maxCount <= 0
            ? "No valid profiles available for this launch."
            : count === maxCount
              ? (maxCount === 1 ? "Launching the available profile." : `Launching all ${maxCount} available profiles.`)
              : `Launching ${count} of ${maxCount} available profiles.`;
        }
        profilesDown.disabled = maxCount <= 0 || count <= 1;
        profilesUp.disabled = maxCount <= 0 || count >= maxCount;
        profilesPresets?.querySelectorAll("[data-profile-preset]").forEach((btn) => {
          btn.classList.toggle("active", Number(btn.dataset.profilePreset) === count);
        });
      };
      const setProfilesValue = (nextValue, markTouched = false) => {
        const maxCount = selectedCount();
        const next = clampProfileCount(nextValue, maxCount);
        if (markTouched && profilesInput) profilesInput.dataset.touched = "1";
        renderProfilesUi(next, maxCount);
      };
      const syncProfiles = (force = false) => {
        const maxCount = selectedCount();
        buildProfilePresets(maxCount);
        if (!profilesInput) return;
        const current = parseInt(profilesInput.value, 10) || 0;
        const preferredCount = Math.min(presetProfiles || maxCount, maxCount);
        const next = force || !profilesInput.dataset.touched || current > maxCount || current < 1
          ? preferredCount
          : current;
        renderProfilesUi(Math.max(1, next), maxCount);
      };

      profilesRange?.addEventListener("input", () => {
        setProfilesValue(profilesRange.value, true);
      });
      profilesDown?.addEventListener("click", () => {
        setProfilesValue((parseInt(profilesInput?.value || "1", 10) || 1) - 1, true);
      });
      profilesUp?.addEventListener("click", () => {
        setProfilesValue((parseInt(profilesInput?.value || "1", 10) || 1) + 1, true);
      });
      $("#c_group")?.addEventListener("change", async (e) => {
        const raw = e.target.value;
        const gid = raw === "" ? null : Number(raw);
        const r = await api.getAccountsWithValidSession(gid);
        accounts = r.ok ? (r.data || []) : [];
        const poolValueEl = $("#c_pool_value");
        const poolHintEl = $("#c_pool_hint");
        if (poolValueEl) poolValueEl.textContent = `${accounts.length} profile${accounts.length === 1 ? "" : "s"} ready`;
        if (poolHintEl) poolHintEl.textContent = accounts.length
          ? "Launches rotate through your valid session pool so the same profiles are not always used first."
          : "No accounts with a valid session in this group. Pick a different group or run Auth Farm first.";
        if (profilesInput) profilesInput.dataset.touched = "";
        syncProfiles(true);
      });
      syncProfiles(true);
      $("#startCartBtn").addEventListener("click", async () => {
        if (!accounts.length) { alert("No accounts with a valid session. Run Auth Farm first."); return; }
        const eventUrl = normalizeDiceEventInput($("#c_url").value);
        if (!eventUrl) {
          alert("Enter a full DICE event URL with the event slug, not just https://dice.fm/event/.");
          return;
        }
        const profilesToUse = Math.max(1, Math.min(accounts.length, parseInt($("#c_profiles").value, 10) || accounts.length));
        let chosen = pickLaunchAccounts(accounts, profilesToUse);
        const scheduledAt = $("#c_sched").value.trim();
        const cartRules = collectCartRules();

        const cardLabel = $("#c_card_label")?.value || "";
        if (cardLabel) {
          const lookupRes = await api.bulkAccountCardsByLabel(chosen.map((a) => a.id), cardLabel);
          const cardMap = lookupRes.ok ? (lookupRes.data || {}) : {};
          const missing = chosen.filter((a) => !cardMap[a.id] && !cardMap[String(a.id)]);
          if (missing.length === chosen.length) {
            alert(`None of the selected accounts have an assigned card with label "${cardLabel}". Assign cards under the Cards tab first.`);
            return;
          }
          if (missing.length) {
            const phones = missing.slice(0, 8).map((a) => a.phone).join(", ");
            const more = missing.length > 8 ? `, +${missing.length - 8} more` : "";
            const ok = confirm(
              `${missing.length} of ${chosen.length} accounts don't have an assigned card with label "${cardLabel}":\n${phones}${more}\n\nContinue with the ${chosen.length - missing.length} that do?`
            );
            if (!ok) return;
            chosen = chosen.filter((a) => cardMap[a.id] || cardMap[String(a.id)]);
          }
          chosen = chosen.map((a) => {
            const card = cardMap[a.id] || cardMap[String(a.id)];
            if (!card) return a;
            return {
              ...a,
              card_number: card.card_number || a.card_number,
              card_exp_month: card.card_exp_month || a.card_exp_month,
              card_exp_year: card.card_exp_year || a.card_exp_year,
              card_cvv: card.card_cvv || a.card_cvv,
              billing_name: card.billing_name || a.billing_name,
              billing_email: card.billing_email || a.billing_email,
              billing_phone: card.billing_phone || a.billing_phone,
              billing_postal: card.billing_postal || a.billing_postal,
              billing_country: card.billing_country || a.billing_country || "US",
            };
          });
        }

        const launchStrategy = lockTier ? "cheapest" : activeStrategy;
        const launchTierName = lockTier ? (presetTier || null) : null;
        const launchAllowedTierIds = lockTier ? [] : collectAllowedTierIds();
        const launchKeywords = lockTier ? "" : ($("#c_keywords")?.value.trim() || "");
        const launchTaskMin = lockTier ? null : (parseOptionalFloat($("#c_task_min")?.value) ?? null);
        const launchTaskMax = lockTier ? null : (parseOptionalFloat($("#c_task_max")?.value) ?? null);
        if (!lockTier && previewTiers.length && !previewLocked && !launchAllowedTierIds.length) {
          alert("You fetched tiers but didn't keep any checked. Pick at least one acceptable tier or click Clear → Cancel and use keyword filtering instead.");
          return;
        }
        const fireModeRaw = String($("#c_fire_mode")?.value || "predrop").toLowerCase();
        const fireMode = ["predrop", "ondrop", "split"].includes(fireModeRaw) ? fireModeRaw : "predrop";
        const fireWindowEnabled = fireMode !== "ondrop";

        const singleCode = codeMode === "single" ? ($("#c_code")?.value.trim() || "") : "";
        const codePoolId = codeMode === "pool"
          ? Number($("#c_code_pool")?.value || 0) || null
          : null;
        if (codeMode === "pool" && !codePoolId) {
          alert("Pick a code pool or switch to a different access-code mode.");
          return;
        }

        let perProfileCodes = null;
        if (codePoolId) {
          const draw = await api.drawCodesFromPool(codePoolId, chosen.length);
          const drawnCodes = draw.ok ? (draw.data || []) : [];
          if (!drawnCodes.length) {
            alert("The selected code pool has no codes. Add codes to the pool from the Code Pools tab first.");
            return;
          }
          if (drawnCodes.length < chosen.length) {
            const ok = confirm(
              `Code pool has only ${drawnCodes.length} code${drawnCodes.length === 1 ? "" : "s"} but ${chosen.length} profiles will launch. ` +
              `${chosen.length - drawnCodes.length} profile${chosen.length - drawnCodes.length === 1 ? "" : "s"} will run with no access code. Continue?`,
            );
            if (!ok) return;
          }
          perProfileCodes = new Map();
          chosen.forEach((acct, idx) => {
            const code = drawnCodes[idx];
            if (code) perProfileCodes.set(acct.id, code);
          });
        }

        const common = {
          event_url: eventUrl,
          presale_code: singleCode,
          price_rules: cartRules,
          ticket_tier: launchTierName,
          tier_strategy: launchStrategy,
          tier_keywords: launchKeywords || null,
          allowed_tier_ids: launchAllowedTierIds,
          target_min_price: launchTaskMin,
          target_max_price: launchTaskMax,
          ticket_type_id: defaults.ticket_type_id || null,
          quantity: 1,
          scheduled_at: scheduledAt || "",
          scheduled_tz: scheduledAt ? $("#c_tz").value : "",
          pre_drop_fire_window_enabled: fireWindowEnabled,
          fire_mode: fireMode,
          mode: lockMode ? presetMode : $("#c_mode").value,
          capsolver_key: state.settings.capsolverKey || null,
          twocaptcha_key: state.settings.twocaptchaKey || null,
          approval_webhook_url: state.settings.approvalWebhookUrl || null,
          approval_poll_url: state.settings.approvalPollUrl || null,
          approval_secret: state.settings.approvalSecret || null,
          approval_poll_interval_seconds: 2,
        };
        closeModal();
        const { failures } = await launchCartRuns(chosen, common, { perProfileCodes });
        if (failures.length) alert("Some carts failed:\n" + failures.join("\n"));
      });
    },
  });
}

// ── Modal helpers ─────────────────────────────────────────────────────────
const modalBackdrop = $("#modalBackdrop");
function openModal({ title, bodyHtml, footerHtml, onMount, type = "generic" }) {
  state.activeModalType = type;
  $("#modalTitle").textContent = title;
  $("#modalBody").innerHTML = bodyHtml || "";
  $("#modalFooter").innerHTML = footerHtml || "";
  modalBackdrop.hidden = false;
  modalBackdrop.querySelectorAll("[data-close]").forEach((btn) => btn.addEventListener("click", closeModal));
  if (onMount) onMount();
}
function closeModal() {
  modalBackdrop.hidden = true;
  state.activeModalType = null;
}

function formatPurchasedTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}
$("#modalClose").addEventListener("click", closeModal);
modalBackdrop.addEventListener("click", (e) => { if (e.target === modalBackdrop) closeModal(); });

// ── Worker event stream ───────────────────────────────────────────────────
const _otpPrompted = new Set();
async function promptManualOtp(msg) {
  const sid = msg.session_id;
  if (!sid || _otpPrompted.has(sid)) return;
  _otpPrompted.add(sid);
  try {
    const reason = msg.reason || "Automatic OTP lookup did not return a code";
    const timeout = msg.timeout_seconds ? Math.round(Number(msg.timeout_seconds) / 60) : null;
    const hint = timeout ? ` You have about ${timeout} minute(s).` : "";
    const raw = window.prompt(
      `${reason}.\n\nEnter the OTP code from your email/SMS and press OK.${hint}`,
      "",
    );
    const code = (raw || "").trim();
    if (!code) {
      appendFarmLog("Manual OTP cancelled — session will time out.", "warning");
      return;
    }
    const r = await api.sessionSetOtp(sid, code);
    if (r.ok) appendFarmLog(`Manual OTP submitted (${code}).`);
    else appendFarmLog(`Failed to submit manual OTP: ${r.error || "unknown"}`, "error");
  } finally {
    setTimeout(() => _otpPrompted.delete(sid), 2000);
  }
}

api.onEvent((msg) => {
  if (msg.type === "log") {
    if (msg.session_id && state.carts.has(msg.session_id)) {
      const c = state.carts.get(msg.session_id);
      c.logs = c.logs || [];
      const ts = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
      c.logs.push({ ts, msg: msg.message || "", level: msg.level || "info" });
      if (c.logs.length > 400) c.logs.splice(0, c.logs.length - 400);
      state.carts.set(msg.session_id, c);
      if ($("#page-dashboard").classList.contains("active") && c.showLogs) renderCartGrid();
      return;
    }
    appendFarmLog(msg.message || "", msg.level || "info");
    return;
  }
  if (msg.type === "auth_update") {
    state.authFarm.accountStatus.set(msg.account_id, msg.status);
    recordAuthAttempt(msg);
    refreshAuthFarm();
    if ($("#page-accounts").classList.contains("active")) refreshAccounts();
    if ($("#page-dashboard").classList.contains("active")) refreshDashboard();
    return;
  }
  if (msg.type === "await_otp") {
    if (msg.status === "waiting") promptManualOtp(msg);
    return;
  }
  if (msg.type === "auth_state_update") {
    if (state.authRefresh.sessionId === msg.session_id) {
      state.authRefresh.total = Number(msg.total ?? state.authRefresh.total ?? 0);
      state.authRefresh.checked = Number(msg.checked ?? state.authRefresh.checked ?? 0);
      state.authRefresh.valid = Number(msg.valid ?? state.authRefresh.valid ?? 0);
      state.authRefresh.revoked = Number(msg.revoked ?? state.authRefresh.revoked ?? 0);
      state.authRefresh.skipped = Number(msg.skipped ?? state.authRefresh.skipped ?? 0);
      if (msg.status === "checking") {
        state.authRefresh.message = `Checking ${state.authRefresh.checked + 1} of ${Math.max(state.authRefresh.total, 1)} cached sessions… ${authRefreshSummary(state.authRefresh.valid, state.authRefresh.revoked, state.authRefresh.skipped)}`;
      } else {
        state.authRefresh.message = `Checked ${state.authRefresh.checked} of ${Math.max(state.authRefresh.total, 1)} cached sessions… ${authRefreshSummary(state.authRefresh.valid, state.authRefresh.revoked, state.authRefresh.skipped)}`;
      }
      renderAuthRefreshUi();
      if (msg.status === "invalid") {
        refreshAuthFarm();
        if ($("#page-accounts").classList.contains("active")) refreshAccounts();
        if ($("#page-dashboard").classList.contains("active")) refreshDashboard();
      }
    }
    return;
  }
  if (msg.type === "cart_update") {
    const sid = msg.session_id;
    const existing = state.carts.get(sid) || { session_id: sid };
    const previousApprovalId = existing.approval_id;
    Object.assign(existing, msg);
    if (msg.ttl) existing.ttl_until = Date.now() / 1000 + msg.ttl;
    if (msg.status !== "reserved") {
      existing.approval_local_sent = false;
    } else if (msg.approval_id && msg.approval_id !== previousApprovalId) {
      existing.approval_local_sent = false;
    }
    existing.events = existing.events || [];
    existing.events.push({ ts: Date.now() / 1000, ...msg });
    if (existing.events.length > 500) existing.events.splice(0, existing.events.length - 500);
    state.carts.set(sid, existing);
    if (msg.status === "purchased") refreshInventory();
    if ($("#page-dashboard").classList.contains("active")) renderCartGrid();
    return;
  }
  if (msg.type === "done") {
    if (state.authFarm.sessionId === msg.session_id) {
      state.authFarm.running = false;
      state.authFarm.sessionId = null;
      $("#startFarmBtn").hidden = false;
      $("#stopFarmBtn").hidden = true;
      renderAuthRefreshUi();
      appendFarmLog(msg.ok ? "Farm finished." : `Farm failed: ${msg.error || ""}`, msg.ok ? "info" : "error");
      refreshAuthFarm();
    }
    if (state.authRefresh.sessionId === msg.session_id) {
      const total = Number(msg.total ?? state.authRefresh.total ?? 0);
      const valid = Number(msg.valid ?? state.authRefresh.valid ?? 0);
      const revoked = Number(msg.revoked ?? state.authRefresh.revoked ?? 0);
      const skipped = Number(msg.skipped ?? state.authRefresh.skipped ?? 0);
      const ts = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
      const summary = total
        ? authRefreshSummary(valid, revoked, skipped)
        : "No cached sessions to validate";
      state.authRefresh = {
        sessionId: null,
        running: false,
        total: 0,
        checked: 0,
        valid: 0,
        revoked: 0,
        skipped: 0,
        message: msg.ok
          ? `Last refresh ${ts}: ${summary}.`
          : `Auth state refresh failed: ${msg.error || "Unknown error"}`,
      };
      renderAuthRefreshUi();
      appendFarmLog(
        msg.ok
          ? `Auth state refresh finished — ${summary}.`
          : `Auth state refresh failed: ${msg.error || "Unknown error"}`,
        msg.ok ? "info" : "error",
      );
      refreshAuthFarm();
      if ($("#page-accounts").classList.contains("active")) refreshAccounts();
      if ($("#page-dashboard").classList.contains("active")) refreshDashboard();
    }
    if (state.carts.has(msg.session_id)) {
      const c = state.carts.get(msg.session_id);
      c.status = msg.ok ? "done" : `failed: ${msg.error || ""}`;
      state.carts.set(msg.session_id, c);
      renderCartGrid();
    }
  }
});
api.onWorkerLog((msg) => appendFarmLog(msg.message || "", msg.level || "info"));
api.onUpdateEvent((msg) => setUpdateState(msg));

// ── Countdown ticker ──────────────────────────────────────────────────────
setInterval(() => {
  $$(".cart-countdown").forEach((el) => {
    const until = Number(el.dataset.countdown);
    if (!until) return;
    const remaining = Math.max(0, until - Date.now() / 1000);
    const mm = Math.floor(remaining / 60);
    const ss = Math.floor(remaining % 60);
    el.textContent = `${String(mm).padStart(2, "0")}:${String(ss).padStart(2, "0")}`;
    el.classList.toggle("warn", remaining < 60 && remaining >= 15);
    el.classList.toggle("danger", remaining < 15);
  });
}, 500);

// ── Tasks page ────────────────────────────────────────────────────────────
async function refreshTasks() {
  const r = await api.getTasks();
  state.tasks = r.ok ? r.data || [] : [];
  // Drop selections for tasks that no longer exist.
  const known = new Set(state.tasks.map((t) => t.id));
  for (const id of [...state.selectedTasks]) if (!known.has(id)) state.selectedTasks.delete(id);
  renderTasks();
}

function renderTasks() {
  const grid = $("#taskGrid");
  const search = state.taskSearch.toLowerCase();
  const rows = state.tasks.filter((t) => {
    if (!search) return true;
    return (t.account_phone || "").toLowerCase().includes(search) ||
           (t.event_url || "").toLowerCase().includes(search);
  });
  if (!rows.length) {
    grid.innerHTML = '<div class="empty-state">No tasks yet. Click "Add task" to create one.</div>';
    renderTaskBulkBar();
    return;
  }
  grid.innerHTML = "";
  for (const t of rows) grid.appendChild(renderTaskCard(t));
  renderTaskBulkBar();
  const all = $("#selectAllTasks");
  if (all) all.checked = rows.length > 0 && rows.every((t) => state.selectedTasks.has(t.id));
}

function renderTaskCard(t) {
  const el = document.createElement("div");
  const selected = state.selectedTasks.has(t.id);
  const running = t.status === "running";
  el.className = "task-card" + (selected ? " selected" : "") + (running ? " running" : "");
  const sessionOk = t.session_status === "active" || t.session_status === "expiring";
  const priceRange = (t.min_price != null || t.max_price != null)
    ? `${t.min_price ?? "?"} – ${t.max_price ?? "?"}`
    : "any price";
  const url = (t.event_url || "").trim();
  const urlShort = url ? url.replace(/^https?:\/\//, "").slice(0, 60) : "— no link —";
  el.innerHTML = `
    <div class="task-card-top">
      <div style="display:flex; gap:10px; align-items:flex-start; min-width:0;">
        <input type="checkbox" class="task-sel" data-id="${t.id}" ${selected ? "checked" : ""} style="margin-top:3px;" />
        <div style="min-width:0;">
          <div class="task-account">${escapeHtml(t.account_phone || "")} <span class="session-dot ${sessionDotClass(t.session_status)}"></span></div>
          <div class="task-meta">${escapeHtml(urlShort)}</div>
        </div>
      </div>
      <span class="task-status-pill ${running ? "running" : t.status}">${escapeHtml(t.status || "idle")}</span>
    </div>
    <div class="task-meta">
      <div>Tier: ${escapeHtml(t.ticket_tier || "any")} · Qty: ${escapeHtml(String(t.quantity || 1))} · ${escapeHtml(t.mode || "auto")}</div>
      <div>Price: ${escapeHtml(priceRange)}${t.presale_code ? ` · Presale: ${escapeHtml(t.presale_code)}` : ""}</div>
      ${t.scheduled_at ? `<div>Drop: ${escapeHtml(t.scheduled_at.replace("T"," "))} ${escapeHtml(t.scheduled_tz || "")}</div>` : ""}
      ${t.last_error ? `<div style="color:#fff; margin-top:4px;">Last error: ${escapeHtml(t.last_error)}</div>` : ""}
      ${!sessionOk ? `<div style="color:var(--text-muted); margin-top:4px;">No valid session — run auth before starting</div>` : ""}
    </div>
    <div class="task-actions">
      <button class="btn btn-primary btn-sm" data-task-action="start" ${running || !sessionOk ? "disabled" : ""}>Start</button>
      <button class="btn btn-danger btn-sm" data-task-action="stop" ${running ? "" : "disabled"}>Stop</button>
      <button class="btn btn-ghost btn-sm" data-task-action="edit">Edit</button>
      <button class="btn btn-ghost btn-sm" data-task-action="delete">Delete</button>
    </div>
  `;
  el.querySelector(".task-sel").addEventListener("change", (e) => {
    if (e.target.checked) state.selectedTasks.add(t.id);
    else state.selectedTasks.delete(t.id);
    renderTasks();
  });
  el.querySelectorAll("[data-task-action]").forEach((btn) => {
    btn.addEventListener("click", () => handleTaskAction(btn.dataset.taskAction, t));
  });
  return el;
}

function renderTaskBulkBar() {
  const bar = $("#taskBulkBar");
  const n = state.selectedTasks.size;
  bar.hidden = n === 0;
  if (n) $("#taskBulkCount").textContent = `${n} selected`;
}

async function handleTaskAction(action, task) {
  if (action === "start") return startTasks([task.id]);
  if (action === "stop") return stopTasks([task.id]);
  if (action === "edit") return openTaskModal(task);
  if (action === "delete") {
    if (!confirm("Delete this task?")) return;
    const r = await api.deleteTask(task.id);
    if (r.ok) refreshTasks();
    else alert("Delete failed: " + r.error);
  }
}

async function startTasks(ids) {
  if (!ids.length) return;
  const common = {
    capsolver_key: state.settings.capsolverKey || null,
    twocaptcha_key: state.settings.twocaptchaKey || null,
  };
  const failures = [];
  await Promise.all(ids.map(async (tid) => {
    const r = await api.taskRun({ task_id: tid, ...common });
    if (!r.ok) failures.push(`#${tid}: ${r.error}`);
  }));
  refreshTasks();
  if (failures.length) alert("Some tasks did not start:\n" + failures.join("\n"));
}

async function stopTasks(ids) {
  if (!ids.length) return;
  await Promise.all(ids.map((tid) => api.taskStop(tid)));
  refreshTasks();
}

$("#addTaskBtn").addEventListener("click", () => openTaskModal(null));
$("#refreshTasksBtn").addEventListener("click", refreshTasks);
$("#importTasksBtn").addEventListener("click", async () => {
  const r = await api.importTasksFile();
  if (r.ok) {
    showImportResultModal({
      title: "Task import result",
      summary: [
        `Created: ${r.data.created}`,
        `Skipped: ${r.data.skipped}`,
      ],
      log: r.data.log || [],
      columns: ["row", "outcome", "email", "event_url", "reason"],
    });
    refreshTasks();
  } else if (r.error !== "Cancelled") {
    alert("Import failed: " + r.error);
  }
});
$("#taskSearch").addEventListener("input", (e) => { state.taskSearch = e.target.value; renderTasks(); });

$("#selectAllTasks").addEventListener("change", (e) => {
  if (e.target.checked) state.tasks.forEach((t) => state.selectedTasks.add(t.id));
  else state.selectedTasks.clear();
  renderTasks();
});
$("#taskBulkClearBtn").addEventListener("click", () => { state.selectedTasks.clear(); renderTasks(); });
$("#bulkStartTasksBtn").addEventListener("click", () => startTasks([...state.selectedTasks]));
$("#bulkStopTasksBtn").addEventListener("click", () => stopTasks([...state.selectedTasks]));
$("#bulkDeleteTasksBtn").addEventListener("click", async () => {
  const ids = [...state.selectedTasks];
  if (!ids.length) return;
  if (!confirm(`Delete ${ids.length} task(s)?`)) return;
  await Promise.all(ids.map((id) => api.deleteTask(id)));
  state.selectedTasks.clear();
  refreshTasks();
});
$("#bulkEditTasksBtn").addEventListener("click", () => openBulkEditTasksModal());

async function openTaskModal(task) {
  const isEdit = !!task;
  let accountOptions = "";
  if (!isEdit) {
    const r = await api.getAccounts(null);
    const accts = r.ok ? r.data || [] : [];
    accountOptions = accts
      .map((a) => `<option value="${a.id}">${escapeHtml(a.phone || "")} — ${escapeHtml(a.email || "")}</option>`)
      .join("");
  }
  const t = task || {};
  openModal({
    title: isEdit ? `Edit task — ${escapeHtml(t.account_phone || "")}` : "Add task",
    bodyHtml: `
      <div class="form-grid">
        ${isEdit ? "" : `<label><span>Account</span><select id="t_account">${accountOptions}</select></label>`}
        <label><span>Event URL (link)</span><input id="t_url" value="${escapeHtml(t.event_url || "")}" placeholder="https://dice.fm/event/..." /></label>
        <label><span>Presale code (optional)</span><input id="t_code" value="${escapeHtml(t.presale_code || "")}" /></label>
        <label><span>Preferred tier (fuzzy)</span><input id="t_tier" value="${escapeHtml(t.ticket_tier || state.settings.defaultTier || "")}" /></label>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px;">
          <label><span>Min price</span><input id="t_min" type="number" step="0.01" value="${t.min_price ?? state.settings.defaultMinPrice ?? ""}" /></label>
          <label><span>Max price</span><input id="t_max" type="number" step="0.01" value="${t.max_price ?? state.settings.defaultMaxPrice ?? ""}" /></label>
        </div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px;">
          <label><span>Quantity</span><input id="t_qty" type="number" min="1" step="1" value="${t.quantity ?? 1}" /></label>
          <label><span>Mode</span>
            <select id="t_mode">
              <option value="auto" ${t.mode !== "manual" ? "selected" : ""}>Auto checkout</option>
              <option value="manual" ${t.mode === "manual" ? "selected" : ""}>Reserve only (manual)</option>
            </select>
          </label>
        </div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px;">
          <label><span>Scheduled drop (leave blank to run immediately)</span>
            <input id="t_sched" type="datetime-local" step="1" value="${escapeHtml(t.scheduled_at || "")}" />
          </label>
          <label><span>Timezone</span>
            <select id="t_tz">${tzSelectOptions(t.scheduled_tz || "")}</select>
          </label>
        </div>
        <p class="muted" style="font-size:11px; margin:-6px 0 0;">At T-3s the task starts polling tiers; the cart fires at the exact scheduled instant.</p>
      </div>
    `,
    footerHtml: `<button class="btn btn-ghost" data-close>Cancel</button>
                 <button class="btn btn-primary" id="t_save">${isEdit ? "Save" : "Create"}</button>`,
    onMount: () => {
      $("#t_save").addEventListener("click", async () => {
        const fields = {
          event_url: $("#t_url").value.trim(),
          presale_code: $("#t_code").value.trim(),
          ticket_tier: $("#t_tier").value.trim(),
          min_price: $("#t_min").value,
          max_price: $("#t_max").value,
          quantity: parseInt($("#t_qty").value, 10) || 1,
          mode: $("#t_mode").value,
          scheduled_at: $("#t_sched").value.trim(),
          scheduled_tz: $("#t_sched").value.trim() ? $("#t_tz").value : "",
        };
        let r;
        if (isEdit) {
          r = await api.updateTask(t.id, fields);
        } else {
          const aid = parseInt($("#t_account").value, 10);
          if (!aid) { alert("Pick an account."); return; }
          r = await api.createTask({ account_id: aid, ...fields });
        }
        if (r.ok) { closeModal(); refreshTasks(); }
        else alert("Save failed: " + r.error);
      });
    },
  });
}

function openBulkEditTasksModal() {
  const ids = [...state.selectedTasks];
  if (!ids.length) return;
  openModal({
    title: `Bulk edit ${ids.length} task(s)`,
    bodyHtml: `
      <p class="muted" style="font-size:12px; margin:0 0 10px;">Only filled fields will be applied; blank fields keep their current value.</p>
      <div class="form-grid">
        <label><span>Event URL (link)</span><input id="bt_url" placeholder="leave blank to keep" /></label>
        <label><span>Presale code</span><input id="bt_code" placeholder="leave blank to keep" /></label>
        <label><span>Preferred tier</span><input id="bt_tier" placeholder="leave blank to keep" /></label>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px;">
          <label><span>Min price</span><input id="bt_min" type="number" step="0.01" /></label>
          <label><span>Max price</span><input id="bt_max" type="number" step="0.01" /></label>
        </div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px;">
          <label><span>Quantity</span><input id="bt_qty" type="number" min="1" step="1" /></label>
          <label><span>Mode</span>
            <select id="bt_mode">
              <option value="">(keep)</option>
              <option value="auto">Auto checkout</option>
              <option value="manual">Reserve only (manual)</option>
            </select>
          </label>
        </div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px;">
          <label><span>Scheduled drop</span><input id="bt_sched" type="datetime-local" step="1" /></label>
          <label><span>Timezone</span><select id="bt_tz"><option value="">(keep)</option>${tzSelectOptions("")}</select></label>
        </div>
        <p class="muted" style="font-size:11px; margin:-6px 0 0;">Clear-schedule: type the literal word <code>clear</code> into "Scheduled drop" to remove it on every selected task.</p>
      </div>
    `,
    footerHtml: `<button class="btn btn-ghost" data-close>Cancel</button>
                 <button class="btn btn-primary" id="bt_save">Apply to ${ids.length}</button>`,
    onMount: () => {
      $("#bt_save").addEventListener("click", async () => {
        const patches = {};
        const url = $("#bt_url").value.trim(); if (url) patches.event_url = url;
        const code = $("#bt_code").value.trim(); if (code) patches.presale_code = code;
        const tier = $("#bt_tier").value.trim(); if (tier) patches.ticket_tier = tier;
        const min = $("#bt_min").value; if (min !== "") patches.min_price = min;
        const max = $("#bt_max").value; if (max !== "") patches.max_price = max;
        const qty = $("#bt_qty").value; if (qty !== "") patches.quantity = parseInt(qty, 10) || 1;
        const mode = $("#bt_mode").value; if (mode) patches.mode = mode;
        const schedRaw = $("#bt_sched").value.trim();
        const tzRaw = $("#bt_tz").value;
        let clearSched = false;
        if (schedRaw.toLowerCase() === "clear") {
          clearSched = true;
        } else if (schedRaw) {
          patches.scheduled_at = schedRaw;
          patches.scheduled_tz = tzRaw || browserTz();
        } else if (tzRaw) {
          patches.scheduled_tz = tzRaw;
        }
        if (!clearSched && !Object.keys(patches).length) { closeModal(); return; }

        // Merge each existing task with patches, then update.
        const byId = new Map(state.tasks.map((t) => [t.id, t]));
        await Promise.all(ids.map((id) => {
          const cur = byId.get(id);
          if (!cur) return Promise.resolve();
          const merged = {
            event_url: patches.event_url ?? cur.event_url ?? "",
            presale_code: patches.presale_code ?? cur.presale_code ?? "",
            ticket_tier: patches.ticket_tier ?? cur.ticket_tier ?? "",
            min_price: patches.min_price ?? cur.min_price ?? "",
            max_price: patches.max_price ?? cur.max_price ?? "",
            quantity: patches.quantity ?? cur.quantity ?? 1,
            mode: patches.mode ?? cur.mode ?? "auto",
            scheduled_at: clearSched ? "" : (patches.scheduled_at ?? cur.scheduled_at ?? ""),
            scheduled_tz: clearSched ? "" : (patches.scheduled_tz ?? cur.scheduled_tz ?? ""),
          };
          return api.updateTask(id, merged);
        }));
        closeModal();
        refreshTasks();
      });
    },
  });
}

// ── Init ──────────────────────────────────────────────────────────────────
hydrateSettingsForm();
refreshDashboard();
refreshInventory();
refreshTasks();
api.getUpdateState().then((res) => {
  if (res.ok) setUpdateState(res.data);
});
api.getAppVersion().then((version) => {
  const tag = $("#versionTag");
  if (tag && version) tag.textContent = `v${String(version).replace(/^v/i, "")}`;
});
renderAuthRefreshUi();
