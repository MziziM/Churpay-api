(function () {
  "use strict";

  const TOKEN_KEY = "churpay.admin.token";
  const THEME_KEY = "churpay.admin.theme";
  const LAST_ACTIVITY_KEY = "churpay.admin.lastActivityAt";
  const INACTIVITY_TIMEOUT_MS = 60 * 1000;
  const ACTIVITY_EVENTS = ["pointerdown", "click", "keydown", "touchstart", "input", "wheel"];

  const state = {
    token: "",
    profile: null,
    church: null,
    funds: [],
    totals: [],
    dashboardTransactions: [],
    chartDays: 14,
    txRows: [],
    txMeta: { limit: 25, offset: 0, count: 0, returned: 0 },
    txFilters: {
      search: "",
      fundId: "",
      channel: "",
      status: "",
      from: "",
      to: "",
      limit: 25,
    },
    members: [],
    memberMeta: { count: 0, returned: 0, limit: 50, offset: 0 },
    memberFilters: {
      search: "",
      role: "",
      limit: 50,
    },
    currentTab: "dashboard",
    qr: null,
    sidebarOpen: false,
  };

  const TAB_TITLE = {
    dashboard: "Dashboard",
    transactions: "Transactions",
    funds: "Funds",
    qr: "QR Codes",
    members: "Members",
    settings: "Settings",
  };

  const $ = (id) => document.getElementById(id);
  const $$ = (selector) => Array.from(document.querySelectorAll(selector));

  const el = {
    loadingScreen: $("loadingScreen"),
    authView: $("authView"),
    appView: $("appView"),
    authError: $("authError"),
    loginForm: $("loginForm"),
    identifierInput: $("identifierInput"),
    passwordInput: $("passwordInput"),
    loginBtn: $("loginBtn"),

    toastContainer: $("toastContainer"),
    confirmDialog: $("confirmDialog"),
    confirmTitle: $("confirmTitle"),
    confirmBody: $("confirmBody"),
    confirmOkBtn: $("confirmOkBtn"),
    confirmCancelBtn: $("confirmCancelBtn"),

    navTabs: $("navTabs"),
    sidebar: $("sidebar"),
    sidebarOverlay: $("sidebarOverlay"),
    sidebarToggleBtn: $("sidebarToggleBtn"),
    pageKicker: $("pageKicker"),
    pageTitle: $("pageTitle"),
    churchName: $("churchName"),
    adminIdentity: $("adminIdentity"),
    refreshBtn: $("refreshBtn"),
    logoutBtn: $("logoutBtn"),
    statusInline: $("statusInline"),
    themeToggleBtn: $("themeToggleBtn"),

    statTodayTotal: $("statTodayTotal"),
    statWeekTotal: $("statWeekTotal"),
    statMonthTotal: $("statMonthTotal"),
    statDonors: $("statDonors"),
    statTransactions: $("statTransactions"),
    chartRangeSelect: $("chartRangeSelect"),
    chartSkeleton: $("chartSkeleton"),
    donationChart: $("donationChart"),
    dashboardRecentBody: $("dashboardRecentBody"),
    dashboardTotalsBody: $("dashboardTotalsBody"),

    txSearchInput: $("txSearchInput"),
    txFundSelect: $("txFundSelect"),
    txChannelSelect: $("txChannelSelect"),
    txStatusSelect: $("txStatusSelect"),
    txFromInput: $("txFromInput"),
    txToInput: $("txToInput"),
    txLimitInput: $("txLimitInput"),
    applyTxFiltersBtn: $("applyTxFiltersBtn"),
    resetTxFiltersBtn: $("resetTxFiltersBtn"),
    exportTxBtn: $("exportTxBtn"),
    txMeta: $("txMeta"),
    transactionsBody: $("transactionsBody"),
    txPrevBtn: $("txPrevBtn"),
    txNextBtn: $("txNextBtn"),
    txPageLabel: $("txPageLabel"),

    refreshFundsBtn: $("refreshFundsBtn"),
    createFundForm: $("createFundForm"),
    fundNameInput: $("fundNameInput"),
    fundCodeInput: $("fundCodeInput"),
    fundActiveInput: $("fundActiveInput"),
    createFundBtn: $("createFundBtn"),
    fundsBody: $("fundsBody"),

    qrForm: $("qrForm"),
    qrFundSelect: $("qrFundSelect"),
    qrAmountInput: $("qrAmountInput"),
    generateQrBtn: $("generateQrBtn"),
    qrCard: $("qrCard"),
    qrPayloadValue: $("qrPayloadValue"),
    qrDeepLink: $("qrDeepLink"),
    qrWebLink: $("qrWebLink"),
    copyPayloadBtn: $("copyPayloadBtn"),
    copyWebLinkBtn: $("copyWebLinkBtn"),
    shareQrBtn: $("shareQrBtn"),
    downloadQrBtn: $("downloadQrBtn"),

    memberSearchInput: $("memberSearchInput"),
    memberRoleSelect: $("memberRoleSelect"),
    memberLimitInput: $("memberLimitInput"),
    applyMemberFiltersBtn: $("applyMemberFiltersBtn"),
    refreshMembersBtn: $("refreshMembersBtn"),
    memberMeta: $("memberMeta"),
    membersBody: $("membersBody"),

    churchSummary: $("churchSummary"),
    churchForm: $("churchForm"),
    churchNameInput: $("churchNameInput"),
    joinCodeInput: $("joinCodeInput"),
    saveChurchBtn: $("saveChurchBtn"),
    adminProfileForm: $("adminProfileForm"),
    adminFullNameInput: $("adminFullNameInput"),
    adminPhoneInput: $("adminPhoneInput"),
    adminEmailInput: $("adminEmailInput"),
    adminPasswordInput: $("adminPasswordInput"),
    saveAdminProfileBtn: $("saveAdminProfileBtn"),
  };

  let inactivityTimerId = 0;
  let inactivityWatchdogId = 0;
  let inactivityListening = false;
  let lastActivityAt = Date.now();

  function readSharedLastActivity() {
    const raw = Number(window.localStorage.getItem(LAST_ACTIVITY_KEY) || 0);
    if (!Number.isFinite(raw) || raw <= 0) return lastActivityAt;
    return Math.max(lastActivityAt, raw);
  }

  function writeSharedLastActivity(timestamp) {
    const ts = Number(timestamp || Date.now());
    lastActivityAt = ts;
    try {
      window.localStorage.setItem(LAST_ACTIVITY_KEY, String(ts));
    } catch (_err) {
      // Ignore storage write issues; local timer still works.
    }
  }

  function parseJsonSafe(text) {
    try {
      return text ? JSON.parse(text) : null;
    } catch (_err) {
      return null;
    }
  }

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function formatMoney(value) {
    const n = Number(value || 0);
    if (!Number.isFinite(n)) return "R 0.00";
    return `R ${n.toFixed(2)}`;
  }

  function formatDate(value) {
    if (!value) return "-";
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return String(value);
    return d.toLocaleString();
  }

  function formatDateShort(value) {
    if (!value) return "-";
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return "-";
    return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
  }

  function buildQuery(params) {
    const sp = new URLSearchParams();
    Object.keys(params || {}).forEach((k) => {
      const v = params[k];
      if (v === null || typeof v === "undefined" || v === "") return;
      sp.set(k, String(v));
    });
    const qs = sp.toString();
    return qs ? `?${qs}` : "";
  }

  function showLoading(show) {
    if (!el.loadingScreen) return;
    if (show) {
      el.loadingScreen.classList.remove("hidden");
      el.loadingScreen.style.opacity = "1";
      return;
    }

    el.loadingScreen.style.opacity = "0";
    window.setTimeout(() => el.loadingScreen.classList.add("hidden"), 180);
  }

  function showAuth(show) {
    el.authView.classList.toggle("hidden", !show);
    el.appView.classList.toggle("hidden", show);
    if (show) setSidebarOpen(false);
  }

  function setBusy(button, busy, busyLabel, idleLabel) {
    if (!button) return;
    button.disabled = !!busy;
    if (busyLabel && idleLabel) {
      button.textContent = busy ? busyLabel : idleLabel;
    }
  }

  function setToken(token) {
    state.token = token || "";
    if (state.token) {
      window.localStorage.setItem(TOKEN_KEY, state.token);
    } else {
      window.localStorage.removeItem(TOKEN_KEY);
    }
  }

  function clearInactivityTimer() {
    if (!inactivityTimerId) return;
    window.clearTimeout(inactivityTimerId);
    inactivityTimerId = 0;
  }

  function clearInactivityWatchdog() {
    if (!inactivityWatchdogId) return;
    window.clearInterval(inactivityWatchdogId);
    inactivityWatchdogId = 0;
  }

  function stopInactivityWatch() {
    clearInactivityTimer();
    clearInactivityWatchdog();
    if (!inactivityListening) return;
    ACTIVITY_EVENTS.forEach((eventName) => {
      window.removeEventListener(eventName, onUserActivity);
    });
    document.removeEventListener("visibilitychange", onVisibilityChange);
    window.removeEventListener("focus", onUserActivity);
    window.removeEventListener("storage", onStorageActivity);
    inactivityListening = false;
  }

  function scheduleInactivityTimer() {
    clearInactivityTimer();
    if (!state.token) return;
    const elapsed = Date.now() - readSharedLastActivity();
    const remaining = Math.max(0, INACTIVITY_TIMEOUT_MS - elapsed);
    if (remaining <= 0) {
      void onLogout({ silent: true, reason: "You were logged out after 1 minute of inactivity." });
      return;
    }
    inactivityTimerId = window.setTimeout(() => {
      void onLogout({ silent: true, reason: "You were logged out after 1 minute of inactivity." });
    }, remaining);
  }

  function onUserActivity() {
    if (!state.token) return;
    writeSharedLastActivity(Date.now());
    scheduleInactivityTimer();
  }

  function onStorageActivity(event) {
    if (!state.token) return;
    if (!event) return;
    if (event.key === TOKEN_KEY && !event.newValue) {
      void onLogout({ silent: true, reason: "You were signed out in another tab." });
      return;
    }
    if (event.key !== LAST_ACTIVITY_KEY) return;
    const shared = Number(event.newValue || 0);
    if (Number.isFinite(shared) && shared > 0) {
      lastActivityAt = Math.max(lastActivityAt, shared);
      scheduleInactivityTimer();
    }
  }

  function onVisibilityChange() {
    if (!state.token) return;
    if (document.hidden) {
      clearInactivityTimer();
      return;
    }
    const elapsed = Date.now() - lastActivityAt;
    if (elapsed >= INACTIVITY_TIMEOUT_MS) {
      void onLogout({ silent: true, reason: "You were logged out after 1 minute of inactivity." });
      return;
    }
    scheduleInactivityTimer();
  }

  function startInactivityWatch() {
    if (!state.token) return;
    writeSharedLastActivity(Date.now());
    if (!inactivityListening) {
      ACTIVITY_EVENTS.forEach((eventName) => {
        window.addEventListener(eventName, onUserActivity, { passive: true });
      });
      document.addEventListener("visibilitychange", onVisibilityChange);
      window.addEventListener("focus", onUserActivity, { passive: true });
      window.addEventListener("storage", onStorageActivity);
      inactivityListening = true;
    }
    scheduleInactivityTimer();
    clearInactivityWatchdog();
    inactivityWatchdogId = window.setInterval(() => {
      if (!state.token) return;
      const elapsed = Date.now() - readSharedLastActivity();
      if (elapsed >= INACTIVITY_TIMEOUT_MS) {
        void onLogout({ silent: true, reason: "You were logged out after 1 minute of inactivity." });
      }
    }, 5000);
  }

  function toast(message, type = "info", timeout = 3200) {
    if (!message) return;
    const node = document.createElement("div");
    node.className = `toast ${type}`;
    node.textContent = message;
    el.toastContainer.appendChild(node);
    window.setTimeout(() => {
      node.style.opacity = "0";
      node.style.transform = "translateY(-6px)";
      window.setTimeout(() => node.remove(), 180);
    }, timeout);
  }

  function showInlineStatus(message, kind = "info") {
    if (!message) {
      el.statusInline.className = "status-inline hidden";
      el.statusInline.textContent = "";
      return;
    }
    el.statusInline.className = `status-inline ${kind}`;
    el.statusInline.textContent = message;
  }

  function showAuthError(message) {
    if (!message) {
      el.authError.classList.add("hidden");
      el.authError.textContent = "";
      return;
    }
    el.authError.classList.remove("hidden");
    el.authError.textContent = message;
  }

  function applyTheme(theme) {
    const normalized = theme === "light" ? "light" : "dark";
    document.documentElement.setAttribute("data-theme", normalized);
    window.localStorage.setItem(THEME_KEY, normalized);
    el.themeToggleBtn.textContent = normalized === "dark" ? "Switch to light mode" : "Switch to dark mode";
  }

  function currentTheme() {
    return document.documentElement.getAttribute("data-theme") || "dark";
  }

  function setSidebarOpen(open) {
    state.sidebarOpen = !!open;
    if (state.sidebarOpen) {
      el.appView.classList.add("sidebar-open");
    } else {
      el.appView.classList.remove("sidebar-open");
    }
    if (el.sidebarToggleBtn) {
      el.sidebarToggleBtn.setAttribute("aria-expanded", state.sidebarOpen ? "true" : "false");
    }
  }

  function installBrandLogoFallback() {
    const logos = Array.from(document.querySelectorAll("img[data-logo]"));
    logos.forEach((img) => {
      const sources = [
        img.getAttribute("src"),
        "/assets/brand/churpay-logo-500x250.png",
        "/assets/brand/churpay-logo.png",
        "/assets/churpay-logo.png",
        "/assets/brand/churpay-mark.svg",
        "/favicon.png",
      ].filter(Boolean);
      const uniqueSources = Array.from(new Set(sources));
      img.dataset.logoSources = JSON.stringify(uniqueSources);
      img.dataset.logoSourceIndex = "0";

      img.addEventListener("error", () => {
        const list = JSON.parse(img.dataset.logoSources || "[]");
        const current = Number(img.dataset.logoSourceIndex || "0");
        const next = current + 1;
        if (next < list.length) {
          img.dataset.logoSourceIndex = String(next);
          img.src = list[next];
          return;
        }
        img.style.visibility = "hidden";
      });
    });
  }

  function parseTransactionStatus(tx) {
    if (tx && typeof tx.status === "string" && tx.status) {
      return tx.status.toUpperCase();
    }
    if (tx && tx.provider) {
      if (["payfast", "manual", "simulated"].includes(String(tx.provider).toLowerCase())) return "PAID";
    }
    return "PAID";
  }

  function statusBadgeClass(status) {
    const s = String(status || "").toLowerCase();
    if (s === "paid" || s === "complete") return "paid";
    if (s === "pending") return "pending";
    return "failed";
  }

  function isAdminRole(role) {
    return role === "admin" || role === "super";
  }

  function ensureAdminProfile(profile) {
    if (!profile) throw new Error("Profile missing");
    if (!isAdminRole(profile.role)) {
      throw new Error("Admin role required to access portal");
    }
  }

  async function apiRequest(path, options = {}) {
    const headers = Object.assign({}, options.headers || {});
    if (state.token) headers.Authorization = `Bearer ${state.token}`;

    let body;
    if (typeof options.body !== "undefined") {
      headers["Content-Type"] = "application/json";
      body = JSON.stringify(options.body);
    }

    const res = await fetch(path, {
      method: options.method || "GET",
      headers,
      body,
    });

    const text = await res.text();
    if (options.raw) {
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      return text;
    }

    const json = parseJsonSafe(text);
    if (!res.ok) {
      const msg = (json && (json.error || json.message)) || `HTTP ${res.status}`;
      const err = new Error(msg);
      err.status = res.status;
      err.payload = json || text;
      throw err;
    }

    return json;
  }

  async function confirmAction({ title, body, okLabel = "Confirm" }) {
    if (!el.confirmDialog || typeof el.confirmDialog.showModal !== "function") {
      return window.confirm(body || title || "Are you sure?");
    }

    return new Promise((resolve) => {
      el.confirmTitle.textContent = title || "Confirm action";
      el.confirmBody.textContent = body || "Are you sure?";
      el.confirmOkBtn.textContent = okLabel;

      const onClose = () => {
        const result = el.confirmDialog.returnValue === "ok";
        el.confirmDialog.removeEventListener("close", onClose);
        resolve(result);
      };

      el.confirmDialog.addEventListener("close", onClose);
      el.confirmDialog.showModal();
    });
  }

  function renderSkeletonRows(tbody, cols, rows = 4) {
    if (!tbody) return;
    const colsSafe = Math.max(1, Number(cols || 1));
    const rowsSafe = Math.max(1, Number(rows || 1));
    let html = "";
    for (let i = 0; i < rowsSafe; i += 1) {
      html += `<tr class="skeleton-row">${"<td>&nbsp;</td>".repeat(colsSafe)}</tr>`;
    }
    tbody.innerHTML = html;
  }

  function renderEmpty(tbody, cols, message, ctaLabel, ctaId) {
    if (!tbody) return;
    const button = ctaLabel && ctaId ? `<button id="${ctaId}" class="btn ghost" type="button">${escapeHtml(ctaLabel)}</button>` : "";
    tbody.innerHTML = `
      <tr>
        <td colspan="${Number(cols || 1)}">
          <div class="empty-state">${escapeHtml(message || "No records found.")} ${button}</div>
        </td>
      </tr>
    `;
  }

  function switchTab(tabName) {
    state.currentTab = tabName;
    $$(".nav-link[data-tab]").forEach((btn) => {
      const active = btn.getAttribute("data-tab") === tabName;
      btn.classList.toggle("active", active);
    });

    $$(".panel[id^='panel-']").forEach((panel) => {
      panel.classList.toggle("hidden", panel.id !== `panel-${tabName}`);
    });

    el.pageTitle.textContent = TAB_TITLE[tabName] || "Admin";
    el.pageKicker.textContent = tabName === "dashboard" ? "Admin Portal" : "Control Center";
    setSidebarOpen(false);
  }

  async function loginAdmin(identifier, password) {
    const res = await fetch("/api/auth/login/admin", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ identifier, password }),
    });

    const text = await res.text();
    const json = parseJsonSafe(text);
    if (!res.ok) {
      throw new Error((json && json.error) || "Login failed");
    }
    if (!json || !json.token) throw new Error("Login failed");
    return json;
  }

  async function loadProfile() {
    const data = await apiRequest("/api/auth/me");
    const profile = data && (data.profile || data.member);
    ensureAdminProfile(profile);
    state.profile = profile;

    el.adminIdentity.textContent = `${profile.fullName || profile.email || profile.phone || "Admin"} (${profile.role})`;
    el.adminFullNameInput.value = profile.fullName || "";
    el.adminPhoneInput.value = profile.phone || "";
    el.adminEmailInput.value = profile.email || "";

    if (profile.churchName) {
      el.churchName.textContent = profile.churchName;
    } else {
      el.churchName.textContent = "No church linked";
    }
  }

  async function loadChurch() {
    try {
      const data = await apiRequest("/api/auth/church/me");
      state.church = data && data.church ? data.church : null;
    } catch (err) {
      if (err.status === 404) {
        state.church = null;
      } else {
        throw err;
      }
    }

    if (state.church) {
      el.churchSummary.textContent = `Current: ${state.church.name} | Join code: ${state.church.joinCode || "-"}`;
      el.churchNameInput.value = state.church.name || "";
      el.joinCodeInput.value = state.church.joinCode || "";
      el.saveChurchBtn.textContent = "Update church";
      el.churchName.textContent = state.church.name || el.churchName.textContent;
    } else {
      el.churchSummary.textContent = "No church linked yet. Create one below.";
      el.churchNameInput.value = "";
      el.joinCodeInput.value = "";
      el.saveChurchBtn.textContent = "Create church";
    }
  }

  function updateFundSelects() {
    const targets = [el.txFundSelect, el.qrFundSelect];
    targets.forEach((select) => {
      if (!select) return;
      const current = select.value;
      const isQr = select === el.qrFundSelect;
      select.innerHTML = isQr ? '<option value="">Select a fund</option>' : '<option value="">All funds</option>';
      state.funds.forEach((fund) => {
        const option = document.createElement("option");
        option.value = fund.id;
        option.textContent = `${fund.name} (${fund.code})`;
        select.appendChild(option);
      });
      if (current) select.value = current;
    });
  }

  async function loadFunds() {
    renderSkeletonRows(el.fundsBody, 5, 4);
    const data = await apiRequest("/api/funds" + buildQuery({ includeInactive: 1, churchId: "me" }));
    state.funds = (data && data.funds) || [];

    if (!state.funds.length) {
      renderEmpty(el.fundsBody, 5, "No funds created yet.", "Create first fund", "focusCreateFundBtn");
      window.setTimeout(() => {
        const trigger = $("focusCreateFundBtn");
        if (trigger) trigger.addEventListener("click", () => el.fundNameInput.focus(), { once: true });
      }, 0);
      updateFundSelects();
      return;
    }

    el.fundsBody.innerHTML = state.funds
      .map((fund) => {
        const active = !!fund.active;
        const status = active ? "active" : "inactive";
        const created = fund.createdAt || fund.created_at || "";
        return `
          <tr>
            <td>${escapeHtml(fund.name || "-")}</td>
            <td>${escapeHtml(fund.code || "-")}</td>
            <td><span class="badge ${status}">${active ? "Active" : "Inactive"}</span></td>
            <td>${escapeHtml(formatDate(created))}</td>
            <td class="actions-cell">
              <button class="btn ghost" data-action="rename" data-id="${escapeHtml(fund.id)}" type="button">Rename</button>
              <button class="btn ghost" data-action="toggle" data-id="${escapeHtml(fund.id)}" type="button">${active ? "Disable" : "Enable"}</button>
            </td>
          </tr>
        `;
      })
      .join("");

    updateFundSelects();
  }

  async function loadTotals() {
    const data = await apiRequest("/api/admin/dashboard/totals");
    state.totals = (data && data.totals) || [];
    if (!state.totals.length) {
      renderEmpty(el.dashboardTotalsBody, 3, "No totals yet.");
      return;
    }

    el.dashboardTotalsBody.innerHTML = state.totals
      .map((row) => {
        return `
          <tr>
            <td>${escapeHtml(row.name || "-")}</td>
            <td>${escapeHtml(row.code || "-")}</td>
            <td>${escapeHtml(formatMoney(row.total))}</td>
          </tr>
        `;
      })
      .join("");
  }

  function dashboardDateRange(days) {
    const end = new Date();
    const start = new Date();
    start.setDate(end.getDate() - (Number(days || 14) - 1));
    return { start, end };
  }

  function txQueryFromFilters(filters, offsetOverride) {
    const limit = Math.max(1, Math.min(200, Number(filters.limit || 25)));
    return {
      search: filters.search || "",
      fundId: filters.fundId || "",
      channel: filters.channel || "",
      status: filters.status || "",
      from: filters.from || "",
      to: filters.to || "",
      limit,
      offset: Math.max(0, Number(typeof offsetOverride === "number" ? offsetOverride : state.txMeta.offset || 0)),
    };
  }

  async function loadDashboardTransactions() {
    const range = dashboardDateRange(state.chartDays);
    const data = await apiRequest(
      "/api/admin/dashboard/transactions/recent" +
        buildQuery({
          from: range.start.toISOString().slice(0, 10),
          to: range.end.toISOString().slice(0, 10),
          limit: 1000,
          offset: 0,
        })
    );

    state.dashboardTransactions = (data && data.transactions) || [];
    return data;
  }

  function calculateDashboardStats() {
    const tx = state.dashboardTransactions.slice();
    const now = new Date();

    const todayStart = new Date(now);
    todayStart.setHours(0, 0, 0, 0);

    const weekStart = new Date(now);
    weekStart.setDate(now.getDate() - 6);
    weekStart.setHours(0, 0, 0, 0);

    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);

    let todayTotal = 0;
    let weekTotal = 0;
    let monthTotal = 0;
    const donors = new Set();

    tx.forEach((row) => {
      const amount = Number(row.amount || 0);
      const created = new Date(row.createdAt || row.created_at || 0);
      if (!Number.isFinite(amount) || Number.isNaN(created.getTime())) return;

      if (created >= todayStart) todayTotal += amount;
      if (created >= weekStart) weekTotal += amount;
      if (created >= monthStart) monthTotal += amount;

      const donorKey = row.memberPhone || row.memberName || row.memberEmail || "";
      if (donorKey) donors.add(String(donorKey));
    });

    return {
      todayTotal,
      weekTotal,
      monthTotal,
      donorCount: donors.size,
      txCount: tx.length,
    };
  }

  function renderDashboardStats() {
    const stats = calculateDashboardStats();
    el.statTodayTotal.textContent = formatMoney(stats.todayTotal);
    el.statWeekTotal.textContent = formatMoney(stats.weekTotal);
    el.statMonthTotal.textContent = formatMoney(stats.monthTotal);
    el.statDonors.textContent = String(stats.donorCount);
    el.statTransactions.textContent = String(stats.txCount);
  }

  function renderDashboardRecent() {
    const rows = state.dashboardTransactions.slice(0, 10);
    if (!rows.length) {
      renderEmpty(el.dashboardRecentBody, 4, "No recent transactions yet.");
      return;
    }

    el.dashboardRecentBody.innerHTML = rows
      .map((tx) => {
        return `
          <tr>
            <td>${escapeHtml(tx.reference || "-")}</td>
            <td>${escapeHtml(formatMoney(tx.amount))}</td>
            <td>${escapeHtml(tx.fundName || tx.fundCode || "-")}</td>
            <td>${escapeHtml(formatDate(tx.createdAt || tx.created_at))}</td>
          </tr>
        `;
      })
      .join("");
  }

  function renderChart() {
    const days = Number(state.chartDays || 14);
    const range = dashboardDateRange(days);

    const daily = new Map();
    const labels = [];
    const cursor = new Date(range.start);

    while (cursor <= range.end) {
      const key = cursor.toISOString().slice(0, 10);
      labels.push(key);
      daily.set(key, 0);
      cursor.setDate(cursor.getDate() + 1);
    }

    state.dashboardTransactions.forEach((tx) => {
      const created = new Date(tx.createdAt || tx.created_at || "");
      if (Number.isNaN(created.getTime())) return;
      const key = created.toISOString().slice(0, 10);
      if (!daily.has(key)) return;
      const amount = Number(tx.amount || 0);
      daily.set(key, (daily.get(key) || 0) + (Number.isFinite(amount) ? amount : 0));
    });

    const values = labels.map((k) => daily.get(k) || 0);
    const max = Math.max(...values, 1);

    el.chartSkeleton.classList.add("hidden");
    el.donationChart.classList.remove("hidden");
    el.donationChart.style.setProperty("--bars", String(labels.length));

    el.donationChart.innerHTML = labels
      .map((key) => {
        const value = daily.get(key) || 0;
        const pct = Math.max(4, (value / max) * 100);
        const date = new Date(key + "T00:00:00");
        return `
          <div class="chart-bar" style="height:${pct}%" data-value="${escapeHtml(formatMoney(value))}">
            <span>${escapeHtml(formatDateShort(date))}</span>
          </div>
        `;
      })
      .join("");
  }

  async function loadDashboard() {
    renderSkeletonRows(el.dashboardRecentBody, 4, 5);
    renderSkeletonRows(el.dashboardTotalsBody, 3, 4);
    el.chartSkeleton.classList.remove("hidden");
    el.donationChart.classList.add("hidden");

    await Promise.all([loadTotals(), loadDashboardTransactions()]);
    renderDashboardStats();
    renderDashboardRecent();
    renderChart();
  }

  async function loadTransactions() {
    renderSkeletonRows(el.transactionsBody, 7, 7);

    const query = txQueryFromFilters(state.txFilters);
    const data = await apiRequest("/api/admin/dashboard/transactions/recent" + buildQuery(query));
    const rows = (data && data.transactions) || [];
    const meta = (data && data.meta) || { count: rows.length, returned: rows.length, limit: query.limit, offset: query.offset };

    state.txRows = rows;
    state.txMeta = {
      count: Number(meta.count || rows.length),
      returned: Number(meta.returned || rows.length),
      limit: Number(meta.limit || query.limit),
      offset: Number(meta.offset || query.offset),
    };

    if (!rows.length) {
      renderEmpty(el.transactionsBody, 7, "No transactions match your filters.", "Clear filters", "clearTxFiltersBtn");
      window.setTimeout(() => {
        const node = $("clearTxFiltersBtn");
        if (node) node.addEventListener("click", resetTransactionFilters, { once: true });
      }, 0);
    } else {
      el.transactionsBody.innerHTML = rows
        .map((tx) => {
          const status = parseTransactionStatus(tx);
          return `
            <tr>
              <td>${escapeHtml(tx.reference || "-")}</td>
              <td>${escapeHtml(formatMoney(tx.amount))}</td>
              <td>${escapeHtml(tx.fundName || tx.fundCode || "-")}</td>
              <td>${escapeHtml(tx.memberName || tx.memberPhone || "-")}</td>
              <td>${escapeHtml(tx.channel || tx.provider || "-")}</td>
              <td><span class="badge ${statusBadgeClass(status)}">${escapeHtml(status)}</span></td>
              <td>${escapeHtml(formatDate(tx.createdAt || tx.created_at))}</td>
            </tr>
          `;
        })
        .join("");
    }

    const page = Math.floor(state.txMeta.offset / Math.max(1, state.txMeta.limit)) + 1;
    const shownTo = state.txMeta.offset + state.txRows.length;
    if (state.txMeta.count === 0) {
      el.txMeta.textContent = "No transactions found for current filters.";
    } else {
      const fromLabel = state.txMeta.offset + 1;
      el.txMeta.textContent = `Showing ${fromLabel}-${shownTo} of ${state.txMeta.count} matching transactions`;
    }
    el.txPageLabel.textContent = `Page ${page}`;
    el.txPrevBtn.disabled = state.txMeta.offset <= 0;
    el.txNextBtn.disabled = state.txMeta.offset + state.txRows.length >= state.txMeta.count;
  }

  async function exportTransactions() {
    const query = txQueryFromFilters(state.txFilters, 0);
    query.limit = Math.max(query.limit, 5000);
    delete query.offset;

    const res = await fetch("/api/admin/dashboard/transactions/export" + buildQuery(query), {
      headers: { Authorization: `Bearer ${state.token}` },
    });

    const csv = await res.text();
    if (!res.ok) {
      const json = parseJsonSafe(csv);
      throw new Error((json && json.error) || "Export failed");
    }

    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `transactions-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }

  async function createFund(payload) {
    return apiRequest("/api/funds", {
      method: "POST",
      body: payload,
    });
  }

  async function patchFund(fundId, payload) {
    return apiRequest(`/api/funds/${encodeURIComponent(fundId)}`, {
      method: "PATCH",
      body: payload,
    });
  }

  async function onCreateFundSubmit(event) {
    event.preventDefault();
    setBusy(el.createFundBtn, true, "Creating...", "Create fund");
    try {
      const name = (el.fundNameInput.value || "").trim();
      const code = (el.fundCodeInput.value || "").trim();
      const active = !!el.fundActiveInput.checked;
      if (!name) throw new Error("Fund name is required");

      await createFund({ name, code: code || undefined, active });
      el.fundNameInput.value = "";
      el.fundCodeInput.value = "";
      el.fundActiveInput.checked = true;
      await Promise.all([loadFunds(), loadTotals(), loadDashboardTransactions()]);
      renderDashboardStats();
      renderDashboardRecent();
      toast("Fund created.", "success");
    } catch (err) {
      toast(err.message || "Could not create fund.", "error");
    } finally {
      setBusy(el.createFundBtn, false, "Creating...", "Create fund");
    }
  }

  async function onFundsAction(event) {
    const button = event.target.closest("button[data-action]");
    if (!button) return;

    const action = button.getAttribute("data-action");
    const fundId = button.getAttribute("data-id");
    if (!action || !fundId) return;

    const fund = state.funds.find((f) => f.id === fundId);
    if (!fund) return;

    try {
      if (action === "rename") {
        const newName = window.prompt("New fund name", fund.name || "");
        if (!newName) return;
        await patchFund(fundId, { name: newName.trim() });
        toast("Fund renamed.", "success");
      }

      if (action === "toggle") {
        const nextActive = !fund.active;
        const confirmed = await confirmAction({
          title: nextActive ? "Enable fund" : "Disable fund",
          body: nextActive
            ? `Enable ${fund.name} and allow donations again?`
            : `Disable ${fund.name}? Donors will not see it in active funds.`,
          okLabel: nextActive ? "Enable" : "Disable",
        });
        if (!confirmed) return;
        await patchFund(fundId, { active: nextActive });
        toast(nextActive ? "Fund enabled." : "Fund disabled.", "success");
      }

      await Promise.all([loadFunds(), loadTotals()]);
    } catch (err) {
      toast(err.message || "Fund update failed.", "error");
    }
  }

  function renderQrCard(data) {
    if (!data) {
      state.qr = null;
      el.qrCard.className = "qr-card empty-state";
      el.qrCard.innerHTML = "<p>Generate a QR code to start accepting donations quickly.</p>";
      el.qrPayloadValue.value = "";
      el.qrDeepLink.value = "";
      el.qrWebLink.value = "";
      el.downloadQrBtn.removeAttribute("href");
      return;
    }

    state.qr = data;

    const payloadObject = data.qrPayload || data.qr?.payload || {};
    const payloadString = typeof payloadObject === "string" ? payloadObject : JSON.stringify(payloadObject, null, 2);
    const qrRaw = data.qr?.value || payloadString;
    const qrImage = `https://api.qrserver.com/v1/create-qr-code/?size=420x420&data=${encodeURIComponent(qrRaw)}`;

    el.qrCard.className = "qr-card";
    el.qrCard.innerHTML = `<img src="${escapeHtml(qrImage)}" alt="Donation QR" />`;
    el.qrPayloadValue.value = payloadString;
    el.qrDeepLink.value = data.deepLink || "";
    el.qrWebLink.value = data.webLink || "";
    el.downloadQrBtn.href = qrImage;
  }

  async function onGenerateQr(event) {
    event.preventDefault();
    setBusy(el.generateQrBtn, true, "Generating...", "Generate QR");

    try {
      const fundId = (el.qrFundSelect.value || "").trim();
      const amount = (el.qrAmountInput.value || "").trim();
      if (!fundId) throw new Error("Select a fund first");

      const data = await apiRequest("/api/churches/me/qr" + buildQuery({ fundId, amount }));
      renderQrCard(data);
      toast("QR generated.", "success");
    } catch (err) {
      toast(err.message || "QR generation failed.", "error");
    } finally {
      setBusy(el.generateQrBtn, false, "Generating...", "Generate QR");
    }
  }

  async function copyToClipboard(text, successMessage) {
    if (!text) {
      toast("Nothing to copy yet.", "error");
      return;
    }
    try {
      await navigator.clipboard.writeText(text);
      toast(successMessage || "Copied.", "success");
    } catch (_err) {
      toast("Copy failed. Please copy manually.", "error");
    }
  }

  async function shareQrLink() {
    const link = el.qrWebLink.value || el.qrDeepLink.value;
    if (!link) {
      toast("Generate QR first.", "error");
      return;
    }

    if (navigator.share) {
      try {
        await navigator.share({ title: "Churpay donation link", url: link });
        return;
      } catch (_err) {
        // Fallback to clipboard below.
      }
    }

    await copyToClipboard(link, "Link copied for sharing.");
  }

  async function loadMembers() {
    renderSkeletonRows(el.membersBody, 5, 5);

    const query = {
      search: state.memberFilters.search || "",
      role: state.memberFilters.role || "",
      limit: Math.max(1, Math.min(200, Number(state.memberFilters.limit || 50))),
      offset: 0,
    };

    try {
      const data = await apiRequest("/api/admin/members" + buildQuery(query));
      const rows = (data && data.members) || [];
      const meta = (data && data.meta) || { count: rows.length, returned: rows.length, limit: query.limit, offset: 0 };
      state.members = rows;
      state.memberMeta = meta;

      if (!rows.length) {
        renderEmpty(el.membersBody, 5, "No members found for current filters.");
      } else {
        el.membersBody.innerHTML = rows
          .map((member) => {
            const roleClass = member.role === "admin" ? "paid" : "pending";
            return `
              <tr>
                <td>${escapeHtml(member.fullName || "-")}</td>
                <td>${escapeHtml(member.phone || "-")}</td>
                <td>${escapeHtml(member.email || "-")}</td>
                <td><span class="badge ${roleClass}">${escapeHtml(member.role || "member")}</span></td>
                <td>${escapeHtml(formatDate(member.createdAt || member.created_at))}</td>
              </tr>
            `;
          })
          .join("");
      }

      el.memberMeta.textContent = `Showing ${rows.length} of ${Number(meta.count || rows.length)} members`;
    } catch (err) {
      if (err.status === 404) {
        renderEmpty(el.membersBody, 5, "Members endpoint is not enabled yet on this deploy.");
        el.memberMeta.textContent = "Members endpoint unavailable.";
        return;
      }
      throw err;
    }
  }

  async function onSaveChurch(event) {
    event.preventDefault();
    const busyLabel = state.church ? "Updating..." : "Creating...";
    setBusy(el.saveChurchBtn, true, busyLabel, state.church ? "Update church" : "Create church");

    try {
      const name = (el.churchNameInput.value || "").trim();
      const joinCode = (el.joinCodeInput.value || "").trim().toUpperCase();
      if (!name) throw new Error("Church name is required");

      const body = { name };
      if (joinCode) body.joinCode = joinCode;

      if (state.church) {
        await apiRequest("/api/auth/church/me", { method: "PATCH", body });
      } else {
        await apiRequest("/api/auth/church/me", { method: "POST", body });
      }

      await Promise.all([loadChurch(), loadProfile()]);
      toast("Church profile saved.", "success");
    } catch (err) {
      toast(err.message || "Could not save church.", "error");
    } finally {
      setBusy(el.saveChurchBtn, false, busyLabel, state.church ? "Update church" : "Create church");
    }
  }

  async function onSaveAdminProfile(event) {
    event.preventDefault();
    setBusy(el.saveAdminProfileBtn, true, "Updating...", "Update profile");

    try {
      const payload = {
        fullName: (el.adminFullNameInput.value || "").trim(),
        phone: (el.adminPhoneInput.value || "").trim(),
        email: (el.adminEmailInput.value || "").trim(),
      };

      const password = (el.adminPasswordInput.value || "").trim();
      if (password) payload.password = password;

      Object.keys(payload).forEach((k) => {
        if (payload[k] === "") delete payload[k];
      });

      if (!Object.keys(payload).length) {
        throw new Error("No profile changes supplied");
      }

      await apiRequest("/api/auth/profile/me", {
        method: "PATCH",
        body: payload,
      });

      el.adminPasswordInput.value = "";
      await loadProfile();
      toast("Profile updated.", "success");
    } catch (err) {
      toast(err.message || "Could not update profile.", "error");
    } finally {
      setBusy(el.saveAdminProfileBtn, false, "Updating...", "Update profile");
    }
  }

  function readTxFiltersFromInputs() {
    state.txFilters.search = (el.txSearchInput.value || "").trim();
    state.txFilters.fundId = el.txFundSelect.value || "";
    state.txFilters.channel = el.txChannelSelect.value || "";
    state.txFilters.status = el.txStatusSelect.value || "";
    state.txFilters.from = el.txFromInput.value || "";
    state.txFilters.to = el.txToInput.value || "";
    state.txFilters.limit = Math.max(1, Math.min(200, Number(el.txLimitInput.value || 25)));
  }

  function writeTxFiltersToInputs() {
    el.txSearchInput.value = state.txFilters.search;
    el.txFundSelect.value = state.txFilters.fundId;
    el.txChannelSelect.value = state.txFilters.channel;
    el.txStatusSelect.value = state.txFilters.status;
    el.txFromInput.value = state.txFilters.from;
    el.txToInput.value = state.txFilters.to;
    el.txLimitInput.value = String(state.txFilters.limit || 25);
  }

  function resetTransactionFilters() {
    state.txFilters = {
      search: "",
      fundId: "",
      channel: "",
      status: "",
      from: "",
      to: "",
      limit: 25,
    };
    state.txMeta.offset = 0;
    writeTxFiltersToInputs();
  }

  function readMemberFilters() {
    state.memberFilters.search = (el.memberSearchInput.value || "").trim();
    state.memberFilters.role = el.memberRoleSelect.value || "";
    state.memberFilters.limit = Math.max(1, Math.min(200, Number(el.memberLimitInput.value || 50)));
  }

  async function refreshAll() {
    showInlineStatus("Refreshing portal data...", "info");
    await Promise.all([loadProfile(), loadChurch(), loadFunds(), loadDashboard(), loadTransactions(), loadMembers()]);
    showInlineStatus("Portal refreshed.", "info");
    window.setTimeout(() => showInlineStatus(""), 1600);
  }

  async function onLogout(options = {}) {
    const reason = options?.reason || "";
    const silent = !!options?.silent;
    stopInactivityWatch();
    setToken("");
    state.profile = null;
    state.church = null;
    state.funds = [];
    state.totals = [];
    state.dashboardTransactions = [];
    state.txRows = [];
    state.members = [];
    renderQrCard(null);
    showAuth(true);
    showInlineStatus("");
    showAuthError("");
    if (reason) {
      toast(reason, "info", 4200);
    } else if (!silent) {
      toast("Signed out.", "info");
    }
  }

  async function bootstrapPortal() {
    await Promise.all([loadProfile(), loadChurch(), loadFunds()]);
    await Promise.all([loadDashboard(), loadTransactions(), loadMembers()]);
  }

  async function onLoginSubmit(event) {
    event.preventDefault();
    showAuthError("");
    setBusy(el.loginBtn, true, "Signing in...", "Sign in");

    try {
      const identifier = (el.identifierInput.value || "").trim();
      const password = el.passwordInput.value || "";
      if (!identifier || !password) throw new Error("Phone/email and password are required");

      const data = await loginAdmin(identifier, password);
      setToken(data.token);
      showAuth(false);
      startInactivityWatch();
      showLoading(true);

      await bootstrapPortal();
      switchTab("dashboard");

      toast("Welcome back.", "success");
    } catch (err) {
      setToken("");
      showAuth(true);
      showAuthError(err.message || "Sign-in failed");
      toast(err.message || "Sign-in failed", "error");
    } finally {
      setBusy(el.loginBtn, false, "Signing in...", "Sign in");
      showLoading(false);
    }
  }

  function bindNavigation() {
    $$(".nav-link[data-tab]").forEach((node) => {
      node.addEventListener("click", () => {
        const tab = node.getAttribute("data-tab") || "dashboard";
        switchTab(tab);
      });
    });
  }

  function bindEvents() {
    bindNavigation();

    if (el.sidebarToggleBtn) {
      el.sidebarToggleBtn.addEventListener("click", () => {
        setSidebarOpen(!state.sidebarOpen);
      });
    }

    if (el.sidebarOverlay) {
      el.sidebarOverlay.addEventListener("click", () => setSidebarOpen(false));
    }

    window.addEventListener("resize", () => {
      setSidebarOpen(false);
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && state.sidebarOpen) {
        setSidebarOpen(false);
      }
    });

    el.loginForm.addEventListener("submit", onLoginSubmit);
    el.logoutBtn.addEventListener("click", onLogout);

    el.refreshBtn.addEventListener("click", async () => {
      try {
        setBusy(el.refreshBtn, true, "Refreshing...", "Refresh");
        await refreshAll();
      } catch (err) {
        showInlineStatus(err.message || "Refresh failed", "error");
        toast(err.message || "Refresh failed", "error");
      } finally {
        setBusy(el.refreshBtn, false, "Refreshing...", "Refresh");
      }
    });

    el.themeToggleBtn.addEventListener("click", () => {
      const next = currentTheme() === "dark" ? "light" : "dark";
      applyTheme(next);
    });

    el.chartRangeSelect.addEventListener("change", async () => {
      state.chartDays = Number(el.chartRangeSelect.value || 14);
      try {
        await loadDashboard();
      } catch (err) {
        toast(err.message || "Could not refresh chart", "error");
      }
    });

    el.applyTxFiltersBtn.addEventListener("click", async () => {
      try {
        readTxFiltersFromInputs();
        state.txMeta.offset = 0;
        await loadTransactions();
      } catch (err) {
        toast(err.message || "Could not apply filters", "error");
      }
    });

    el.resetTxFiltersBtn.addEventListener("click", async () => {
      try {
        resetTransactionFilters();
        await loadTransactions();
      } catch (err) {
        toast(err.message || "Could not reset filters", "error");
      }
    });

    el.txPrevBtn.addEventListener("click", async () => {
      if (state.txMeta.offset <= 0) return;
      try {
        state.txMeta.offset = Math.max(0, state.txMeta.offset - state.txMeta.limit);
        await loadTransactions();
      } catch (err) {
        toast(err.message || "Failed loading previous page", "error");
      }
    });

    el.txNextBtn.addEventListener("click", async () => {
      if (state.txRows.length < state.txMeta.limit) return;
      try {
        state.txMeta.offset += state.txMeta.limit;
        await loadTransactions();
      } catch (err) {
        toast(err.message || "Failed loading next page", "error");
      }
    });

    el.exportTxBtn.addEventListener("click", async () => {
      try {
        setBusy(el.exportTxBtn, true, "Exporting...", "Export CSV");
        readTxFiltersFromInputs();
        await exportTransactions();
        toast("CSV download started.", "success");
      } catch (err) {
        toast(err.message || "CSV export failed", "error");
      } finally {
        setBusy(el.exportTxBtn, false, "Exporting...", "Export CSV");
      }
    });

    el.createFundForm.addEventListener("submit", onCreateFundSubmit);

    el.refreshFundsBtn.addEventListener("click", async () => {
      try {
        await loadFunds();
        toast("Funds refreshed.", "info");
      } catch (err) {
        toast(err.message || "Could not refresh funds", "error");
      }
    });

    el.fundsBody.addEventListener("click", onFundsAction);

    el.qrForm.addEventListener("submit", onGenerateQr);
    el.copyPayloadBtn.addEventListener("click", () => copyToClipboard(el.qrPayloadValue.value, "QR payload copied."));
    el.copyWebLinkBtn.addEventListener("click", () => copyToClipboard(el.qrWebLink.value, "Web link copied."));
    el.shareQrBtn.addEventListener("click", shareQrLink);

    el.applyMemberFiltersBtn.addEventListener("click", async () => {
      try {
        readMemberFilters();
        await loadMembers();
      } catch (err) {
        toast(err.message || "Could not load members", "error");
      }
    });

    el.refreshMembersBtn.addEventListener("click", async () => {
      try {
        await loadMembers();
      } catch (err) {
        toast(err.message || "Could not refresh members", "error");
      }
    });

    el.churchForm.addEventListener("submit", onSaveChurch);
    el.adminProfileForm.addEventListener("submit", onSaveAdminProfile);
  }

  async function init() {
    installBrandLogoFallback();
    bindEvents();
    setSidebarOpen(false);

    const preferredTheme = window.localStorage.getItem(THEME_KEY) || "dark";
    applyTheme(preferredTheme);

    showLoading(true);

    const storedToken = window.localStorage.getItem(TOKEN_KEY);
    if (!storedToken) {
      stopInactivityWatch();
      showAuth(true);
      switchTab("dashboard");
      showLoading(false);
      return;
    }

    try {
      setToken(storedToken);
      showAuth(false);
      startInactivityWatch();
      switchTab("dashboard");
      await bootstrapPortal();
      toast("Session restored.", "info", 1800);
    } catch (err) {
      stopInactivityWatch();
      setToken("");
      showAuth(true);
      showAuthError("Session expired. Please sign in again.");
      toast(err.message || "Session expired. Please sign in again.", "error");
    } finally {
      showLoading(false);
    }
  }

  init();
})();
