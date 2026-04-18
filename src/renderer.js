"use strict";

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

const state = {
  accounts: [],
  groups: [],
  activeGroup: null,
  selected: new Set(),
  search: "",
  carts: new Map(), // session_id -> cart info
  tasks: [],
  selectedTasks: new Set(),
  taskSearch: "",
  authFarm: { sessionId: null, running: false, accountStatus: new Map() },
  settings: loadSettings(),
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

// ── Page switching ────────────────────────────────────────────────────────
$$("#navTabs .nav-btn").forEach((btn) => {
  btn.addEventListener("click", () => gotoPage(btn.dataset.page));
});

function gotoPage(page) {
  $$("#navTabs .nav-btn").forEach((b) => b.classList.toggle("active", b.dataset.page === page));
  $$(".page").forEach((p) => p.classList.toggle("active", p.id === `page-${page}`));
  if (page === "accounts") refreshAccounts();
  if (page === "auth") refreshAuthFarm();
  if (page === "dashboard") refreshDashboard();
  if (page === "tasks") refreshTasks();
}

$("#gotoGuideBtn")?.addEventListener("click", () => gotoPage("guide"));

// ── Settings form ─────────────────────────────────────────────────────────
function hydrateSettingsForm() {
  $("#capsolverKey").value = state.settings.capsolverKey || "";
  $("#twocaptchaKey").value = state.settings.twocaptchaKey || "";
  $("#captchafunKey").value = state.settings.captchafunKey || "";
  $("#aycdKey").value = state.settings.aycdKey || "";
  $("#defaultMinPrice").value = state.settings.defaultMinPrice ?? "";
  $("#defaultMaxPrice").value = state.settings.defaultMaxPrice ?? "";
  $("#defaultTier").value = state.settings.defaultTier || "";
}
$("#saveSettingsBtn").addEventListener("click", () => {
  saveSettings({
    capsolverKey: $("#capsolverKey").value.trim(),
    twocaptchaKey: $("#twocaptchaKey").value.trim(),
    captchafunKey: $("#captchafunKey").value.trim(),
    aycdKey: $("#aycdKey").value.trim(),
    defaultMinPrice: parseFloat($("#defaultMinPrice").value) || null,
    defaultMaxPrice: parseFloat($("#defaultMaxPrice").value) || null,
    defaultTier: $("#defaultTier").value.trim(),
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
        <div id="g_error" class="status-line" style="color:#fff;display:none;"></div>
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
        <button class="btn-icon" data-action="edit" data-id="${a.id}">✎</button>
        <button class="btn-icon" data-action="auth" data-id="${a.id}">🔑</button>
        <button class="btn-icon" data-action="delete" data-id="${a.id}">🗑</button>
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
  }
}

$("#accountSearch").addEventListener("input", (e) => {
  state.search = e.target.value;
  renderAccountsTable();
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
  const r = await api.authFarm(queue);
  if (r.ok) {
    state.authFarm.sessionId = r.data.session_id;
    state.authFarm.running = true;
    appendFarmLog(`Farm started — ${queue.length} selected accounts`);
    gotoPage("auth");
  } else {
    alert("Farm failed to start: " + r.error);
  }
});

$("#bulkMakeTasksBtn").addEventListener("click", async () => {
  const ids = [...state.selected];
  if (!ids.length) return;
  let created = 0;
  for (const aid of ids) {
    const r = await api.createTask({ account_id: aid, mode: "auto", quantity: 1 });
    if (r.ok) created += 1;
  }
  state.selected.clear();
  refreshAccounts();
  alert(`Created ${created} task(s). Edit them on the Tasks page.`);
  gotoPage("tasks");
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
      <pre style="background:var(--bg-0);padding:12px;border-radius:8px;font-size:11px;overflow:auto;">phone,email,card_number,exp_month,exp_year,cvc,billing_name,billing_email,billing_phone,billing_postal,billing_country,proxy,aycd_key,imap_email,imap_password,imap_host</pre>
      <p class="muted" style="margin-top:10px;">All fields except <code>phone</code> are optional. XLSX with the same header is also supported.</p>
      <p class="muted" style="margin-top:8px;">Per-account IMAP is used to read the Dice.fm OTP email. Leave <code>imap_host</code> blank to default to <code>imap.gmail.com</code>. Gmail needs an app password, not your regular login.</p>
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
        <label><span>IMAP email</span><input id="f_imap_email" value="${escapeHtml(a.imap_email || "")}" placeholder="otp-inbox@example.com" /></label>
        <label><span>IMAP password</span><input id="f_imap_pw" type="password" value="${escapeHtml(a.imap_password || "")}" placeholder="app password" /></label>
        <label><span>IMAP host</span><input id="f_imap_host" value="${escapeHtml(a.imap_host || "")}" placeholder="imap.gmail.com" /></label>
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
          imap_password: $("#f_imap_pw").value,
          imap_host: $("#f_imap_host").value.trim(),
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

// ── Auth Farm ─────────────────────────────────────────────────────────────
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
  const r = await api.authFarm(pending.data);
  if (r.ok) {
    state.authFarm.sessionId = r.data.session_id;
    state.authFarm.running = true;
    $("#startFarmBtn").hidden = true;
    $("#stopFarmBtn").hidden = false;
    appendFarmLog(`Farm started — ${pending.data.length} accounts in queue`);
  } else {
    appendFarmLog("Farm failed to start: " + r.error, "error");
  }
});
$("#stopFarmBtn").addEventListener("click", async () => {
  if (state.authFarm.sessionId) await api.sessionStop(state.authFarm.sessionId);
});

function appendFarmLog(message, level = "info") {
  const pane = $("#farmLog");
  const line = document.createElement("div");
  line.className = `log-line ${level}`;
  const ts = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  line.innerHTML = `<span class="log-ts">${ts}</span>${escapeHtml(message)}`;
  pane.appendChild(line);
  pane.scrollTop = pane.scrollHeight;
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

function renderCartGrid() {
  const grid = $("#cartGrid");
  if (state.carts.size === 0) {
    grid.innerHTML = '<div class="empty-state">No carts running. Start tasks from the Tasks page or kick off a one-off run.</div>';
    return;
  }
  grid.innerHTML = "";
  for (const cart of state.carts.values()) {
    grid.appendChild(renderCartCard(cart));
  }
}

function renderCartCard(cart) {
  const el = document.createElement("div");
  el.className = "cart-card";
  el.dataset.sid = cart.session_id;
  const priceNum = Number(cart.ticket_price);
  const price = Number.isFinite(priceNum) && priceNum > 0
    ? `${priceSymbol(cart.ticket_currency)}${priceNum.toFixed(2)}`
    : "—";
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
    <div class="cart-countdown" data-countdown="${cart.ttl_until || 0}">--:--</div>
    <div class="cart-actions">
      <button class="btn btn-ghost btn-sm" data-cart-action="logs">Logs</button>
      <button class="btn btn-ghost btn-sm" data-cart-action="decline">Decline</button>
      <button class="btn btn-primary btn-sm" data-cart-action="approve">Approve</button>
    </div>
    <div class="muted" style="font-size:11px;" data-cart-status>${escapeHtml(cart.status || "reserved")}</div>
    <div class="cart-log-pane log-pane" ${cart.showLogs ? "" : "hidden"}>${logLines}</div>
  `;
  el.querySelector('[data-cart-action="logs"]').addEventListener("click", () => {
    const c = state.carts.get(cart.session_id);
    if (c) { c.showLogs = !c.showLogs; state.carts.set(cart.session_id, c); renderCartGrid(); }
  });
  el.querySelector('[data-cart-action="decline"]').addEventListener("click", async () => {
    await api.sessionStop(cart.session_id);
    state.carts.delete(cart.session_id);
    renderCartGrid();
  });
  el.querySelector('[data-cart-action="approve"]').addEventListener("click", async () => {
    const card = el.querySelector('[data-cart-status]');
    card.textContent = "approved — completing purchase…";
    const r = await api.sessionApprove(cart.session_id);
    if (!r.ok) card.textContent = "approve failed: " + r.error;
  });
  return el;
}

function priceSymbol(ccy) {
  const c = (ccy || "USD").toUpperCase();
  return { USD: "$", GBP: "£", EUR: "€", CAD: "C$", AUD: "A$" }[c] || `${c} `;
}

$("#newCartBtn").addEventListener("click", openCartModal);

async function openCartModal() {
  const [acctsRes, groupsRes] = await Promise.all([
    api.getAccountsWithValidSession(null),
    api.getGroups(),
  ]);
  const accounts = acctsRes.ok ? acctsRes.data || [] : [];
  const groups = groupsRes.ok ? groupsRes.data || [] : [];
  const rows = accounts
    .map((a) => `
      <label class="cart-acct-row" data-group="${a.group_id ?? ""}">
        <input type="checkbox" class="c_acct_cb" value="${a.id}" data-group="${a.group_id ?? ""}" checked />
        <span>${escapeHtml(a.phone)}</span>
        <span class="muted">${escapeHtml(a.group_name || "—")}</span>
      </label>`)
    .join("");
  const groupChipsHtml = [
    ...groups.map((g) => {
      const count = accounts.filter((a) => a.group_id === g.id).length;
      return `<button type="button" class="chip" data-group-chip="${g.id}">${escapeHtml(g.name)} (${count})</button>`;
    }),
    `<button type="button" class="chip" data-group-chip="none">No group</button>`,
  ].join("");
  openModal({
    title: "Start cart run",
    bodyHtml: `
      <div class="form-grid">
        <label><span>Event URL</span><input id="c_url" placeholder="https://dice.fm/event/..." /></label>
        <label><span>Presale code (optional)</span><input id="c_code" placeholder="" /></label>
        <div>
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
            <span class="muted" style="font-size:12px;">Accounts with valid session (${accounts.length})</span>
            <div style="display:flex;gap:8px;">
              <button type="button" class="btn btn-ghost btn-sm" id="c_all">Select all</button>
              <button type="button" class="btn btn-ghost btn-sm" id="c_none">Clear</button>
            </div>
          </div>
          <div class="group-chips" id="c_group_chips" style="margin-bottom:8px;">${groupChipsHtml}</div>
          <div id="c_acct_list" class="cart-acct-list">${rows || '<div class="empty-state">No accounts with a valid session. Run Auth Farm first.</div>'}</div>
        </div>
        <label><span>Preferred tier (fuzzy match)</span><input id="c_tier" value="${escapeHtml(state.settings.defaultTier || "")}" /></label>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">
          <label><span>Min price</span><input id="c_min" type="number" step="0.01" value="${state.settings.defaultMinPrice ?? ""}" /></label>
          <label><span>Max price</span><input id="c_max" type="number" step="0.01" value="${state.settings.defaultMaxPrice ?? ""}" /></label>
        </div>
        <label><span>Mode</span>
          <select id="c_mode">
            <option value="manual">Reserve only (manual approve)</option>
            <option value="auto">Reserve + auto-checkout</option>
          </select>
        </label>
      </div>
    `,
    footerHtml: `<button class="btn btn-ghost" data-close>Cancel</button>
                 <button class="btn btn-primary" id="startCartBtn">Start</button>`,
    onMount: () => {
      $("#c_all")?.addEventListener("click", () => $$(".c_acct_cb").forEach((cb) => (cb.checked = true)));
      $("#c_none")?.addEventListener("click", () => $$(".c_acct_cb").forEach((cb) => (cb.checked = false)));
      $$("#c_group_chips .chip").forEach((chip) => {
        chip.addEventListener("click", () => {
          chip.classList.toggle("active");
          const activeGroups = $$("#c_group_chips .chip.active").map((c) => c.dataset.groupChip);
          if (activeGroups.length === 0) {
            $$(".c_acct_cb").forEach((cb) => (cb.checked = true));
            return;
          }
          $$(".c_acct_cb").forEach((cb) => {
            const g = cb.dataset.group;
            const matchesNone = activeGroups.includes("none") && g === "";
            const matchesGroup = activeGroups.includes(g);
            cb.checked = matchesNone || matchesGroup;
          });
        });
      });
      $("#startCartBtn").addEventListener("click", async () => {
        const selectedIds = $$(".c_acct_cb").filter((cb) => cb.checked).map((cb) => Number(cb.value));
        const selected = accounts.filter((a) => selectedIds.includes(a.id));
        if (!selected.length) { alert("Select at least one account."); return; }
        const common = {
          event_url: $("#c_url").value.trim(),
          presale_code: $("#c_code").value.trim() || null,
          target_min_price: parseFloat($("#c_min").value) || null,
          target_max_price: parseFloat($("#c_max").value) || null,
          ticket_tier: $("#c_tier").value.trim() || null,
          mode: $("#c_mode").value,
          capsolver_key: state.settings.capsolverKey || null,
          twocaptcha_key: state.settings.twocaptchaKey || null,
        };
        closeModal();
        const failures = [];
        await Promise.all(selected.map(async (account) => {
          const r = await api.cartRun({ ...common, account });
          if (r.ok) {
            state.carts.set(r.data.session_id, {
              session_id: r.data.session_id,
              account_phone: account.phone,
              account_id: account.id,
              status: "starting",
            });
          } else {
            failures.push(`${account.phone}: ${r.error}`);
          }
        }));
        renderCartGrid();
        if (failures.length) alert("Some carts failed:\n" + failures.join("\n"));
      });
    },
  });
}

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
        if (!Object.keys(patches).length) { closeModal(); return; }

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
          };
          return api.updateTask(id, merged);
        }));
        closeModal();
        refreshTasks();
      });
    },
  });
}

// ── Modal helpers ─────────────────────────────────────────────────────────
const modalBackdrop = $("#modalBackdrop");
function openModal({ title, bodyHtml, footerHtml, onMount }) {
  $("#modalTitle").textContent = title;
  $("#modalBody").innerHTML = bodyHtml || "";
  $("#modalFooter").innerHTML = footerHtml || "";
  modalBackdrop.hidden = false;
  modalBackdrop.querySelectorAll("[data-close]").forEach((btn) => btn.addEventListener("click", closeModal));
  if (onMount) onMount();
}
function closeModal() { modalBackdrop.hidden = true; }
$("#modalClose").addEventListener("click", closeModal);
modalBackdrop.addEventListener("click", (e) => { if (e.target === modalBackdrop) closeModal(); });

// ── Worker event stream ───────────────────────────────────────────────────
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
    }
    appendFarmLog(msg.message || "", msg.level || "info");
    return;
  }
  if (msg.type === "auth_update") {
    state.authFarm.accountStatus.set(msg.account_id, msg.status);
    refreshAuthFarm();
    return;
  }
  if (msg.type === "task_update") {
    if ($("#page-tasks").classList.contains("active")) refreshTasks();
    return;
  }
  if (msg.type === "cart_update") {
    const sid = msg.session_id;
    const existing = state.carts.get(sid) || { session_id: sid };
    Object.assign(existing, msg);
    if (msg.ttl) existing.ttl_until = Date.now() / 1000 + msg.ttl;
    state.carts.set(sid, existing);
    if ($("#page-dashboard").classList.contains("active")) renderCartGrid();
    return;
  }
  if (msg.type === "done") {
    if (state.authFarm.sessionId === msg.session_id) {
      state.authFarm.running = false;
      state.authFarm.sessionId = null;
      $("#startFarmBtn").hidden = false;
      $("#stopFarmBtn").hidden = true;
      appendFarmLog(msg.ok ? "Farm finished." : `Farm failed: ${msg.error || ""}`, msg.ok ? "info" : "error");
      refreshAuthFarm();
    }
    if (state.carts.has(msg.session_id)) {
      const c = state.carts.get(msg.session_id);
      c.status = msg.ok ? "done" : `failed: ${msg.error || ""}`;
      state.carts.set(msg.session_id, c);
      renderCartGrid();
    }
    if ($("#page-tasks").classList.contains("active")) refreshTasks();
  }
});
api.onWorkerLog((msg) => appendFarmLog(msg.message || "", msg.level || "info"));

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

// ── Init ──────────────────────────────────────────────────────────────────
hydrateSettingsForm();
refreshDashboard();
