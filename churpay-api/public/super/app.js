(function () {
  "use strict";

  const TOKEN_KEY = "churpay.super.token";
  const THEME_KEY = "churpay.admin.theme";
  const LAST_ACTIVITY_KEY = "churpay.super.lastActivityAt";
  const INACTIVITY_TIMEOUT_MS = 60 * 1000;
  const ACTIVITY_EVENTS = ["pointerdown", "click", "keydown", "touchstart", "input", "wheel"];

  const TAB_PATHS = {
    dashboard: "/super/dashboard",
    churches: "/super/churches",
    transactions: "/super/transactions",
    funds: "/super/funds",
    members: "/super/members",
    settings: "/super/settings",
    audit: "/super/audit-logs",
  };

  const TAB_TITLES = {
    dashboard: "Dashboard",
    churches: "Churches",
    transactions: "Transactions",
    funds: "Funds",
    members: "Members",
    settings: "Settings",
    audit: "Audit Logs",
  };

  const state = {
    token: "",
    profile: null,
    currentTab: "dashboard",
    selectedChurchId: "",
    churchDialogMode: "create",
    churchDialogId: "",
    churches: [],
    churchRows: [],
    funds: [],
    loaded: {
      dashboard: false,
      churches: false,
      transactions: false,
      funds: false,
      members: false,
      settings: false,
      audit: false,
    },
    txFilters: {
      churchId: "",
      fundId: "",
      provider: "",
      status: "",
      search: "",
      from: "",
      to: "",
      limit: 25,
      offset: 0,
    },
  };

  const $ = (id) => document.getElementById(id);
  const $$ = (selector) => Array.from(document.querySelectorAll(selector));

  const el = {
    loadingScreen: $("loadingScreen"),
    toastContainer: $("toastContainer"),
    appView: $("appView"),
    navTabs: $("navTabs"),
    pageTitle: $("pageTitle"),
    pageKicker: $("pageKicker"),
    superName: $("superName"),
    superMeta: $("superMeta"),
    refreshBtn: $("refreshBtn"),
    logoutBtn: $("logoutBtn"),
    statusInline: $("statusInline"),
    themeToggleBtn: $("themeToggleBtn"),

    sidebar: $("sidebar"),
    sidebarOverlay: $("sidebarOverlay"),
    sidebarToggleBtn: $("sidebarToggleBtn"),

    dashChurchSelect: $("dashChurchSelect"),
    dashFromInput: $("dashFromInput"),
    dashToInput: $("dashToInput"),
    applyDashFiltersBtn: $("applyDashFiltersBtn"),
    statTodayTotal: $("statTodayTotal"),
    statWeekTotal: $("statWeekTotal"),
    statMonthTotal: $("statMonthTotal"),
    statTotalFeesCollected: $("statTotalFeesCollected"),
    statTotalSuperadminCut: $("statTotalSuperadminCut"),
    statNetPlatformRevenue: $("statNetPlatformRevenue"),
    statChurches: $("statChurches"),
    statFunds: $("statFunds"),
    statFailed: $("statFailed"),
    dashboardRecentBody: $("dashboardRecentBody"),

    openCreateChurchBtn: $("openCreateChurchBtn"),
    churchSearchInput: $("churchSearchInput"),
    churchLimitInput: $("churchLimitInput"),
    applyChurchFiltersBtn: $("applyChurchFiltersBtn"),
    churchesBody: $("churchesBody"),
    churchDetailsMeta: $("churchDetailsMeta"),
    churchDetailFundsBody: $("churchDetailFundsBody"),
    churchDetailAdminsBody: $("churchDetailAdminsBody"),
    churchDetailTransactionsBody: $("churchDetailTransactionsBody"),

    churchDialog: $("churchDialog"),
    churchDialogForm: $("churchDialogForm"),
    churchDialogTitle: $("churchDialogTitle"),
    churchDialogName: $("churchDialogName"),
    churchDialogJoinCode: $("churchDialogJoinCode"),
    churchDialogActive: $("churchDialogActive"),
    churchDialogSaveBtn: $("churchDialogSaveBtn"),

    txChurchSelect: $("txChurchSelect"),
    txFundSelect: $("txFundSelect"),
    txProviderSelect: $("txProviderSelect"),
    txStatusSelect: $("txStatusSelect"),
    txSearchInput: $("txSearchInput"),
    txFromInput: $("txFromInput"),
    txToInput: $("txToInput"),
    txLimitInput: $("txLimitInput"),
    applyTxFiltersBtn: $("applyTxFiltersBtn"),
    exportTxBtn: $("exportTxBtn"),
    txMeta: $("txMeta"),
    transactionsBody: $("transactionsBody"),

    fundChurchSelect: $("fundChurchSelect"),
    fundSearchInput: $("fundSearchInput"),
    applyFundFiltersBtn: $("applyFundFiltersBtn"),
    fundsBody: $("fundsBody"),

    qrDialog: $("qrDialog"),
    qrChurchName: $("qrChurchName"),
    qrFundName: $("qrFundName"),
    qrPayload: $("qrPayload"),
    qrDeepLink: $("qrDeepLink"),
    qrWebLink: $("qrWebLink"),
    copyQrPayloadBtn: $("copyQrPayloadBtn"),
    copyQrWebBtn: $("copyQrWebBtn"),

    memberSearchInput: $("memberSearchInput"),
    memberRoleSelect: $("memberRoleSelect"),
    memberChurchSelect: $("memberChurchSelect"),
    applyMemberFiltersBtn: $("applyMemberFiltersBtn"),
    membersBody: $("membersBody"),

    settingsEnvironment: $("settingsEnvironment"),
    settingsWebhook: $("settingsWebhook"),
    settingsRateLimits: $("settingsRateLimits"),
    settingsMaintenance: $("settingsMaintenance"),

    auditBody: $("auditBody"),
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
      .replace(/\"/g, "&quot;")
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

  function buildQuery(params) {
    const sp = new URLSearchParams();
    Object.keys(params || {}).forEach((k) => {
      const v = params[k];
      if (v === "" || v === null || typeof v === "undefined") return;
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

  function showInlineStatus(message, kind = "info") {
    if (!message) {
      el.statusInline.className = "status-inline hidden";
      el.statusInline.textContent = "";
      return;
    }
    el.statusInline.className = `status-inline ${kind}`;
    el.statusInline.textContent = message;
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
      performLogout("You were logged out after 1 minute of inactivity.");
      return;
    }
    inactivityTimerId = window.setTimeout(() => {
      performLogout("You were logged out after 1 minute of inactivity.");
    }, remaining);
  }

  function onUserActivity() {
    if (!state.token) return;
    writeSharedLastActivity(Date.now());
    scheduleInactivityTimer();
  }

  function onStorageActivity(event) {
    if (!state.token || !event) return;
    if (event.key === TOKEN_KEY && !event.newValue) {
      performLogout("You were signed out in another tab.");
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
      performLogout("You were logged out after 1 minute of inactivity.");
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
        performLogout("You were logged out after 1 minute of inactivity.");
      }
    }, 5000);
  }

  function performLogout(reason = "", redirect = "/super/login/") {
    stopInactivityWatch();
    state.token = "";
    window.localStorage.removeItem(TOKEN_KEY);
    if (reason) {
      toast(reason, "info", 4200);
      window.setTimeout(() => window.location.assign(redirect), 200);
      return;
    }
    window.location.assign(redirect);
  }

  function toast(message, type = "info", timeout = 3000) {
    if (!message || !el.toastContainer) return;
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

  function setSidebarOpen(open) {
    if (!el.appView) return;
    el.appView.classList.toggle("sidebar-open", !!open);
    if (el.sidebarToggleBtn) {
      el.sidebarToggleBtn.setAttribute("aria-expanded", open ? "true" : "false");
    }
  }

  function applyTheme(theme) {
    const normalized = theme === "light" ? "light" : "dark";
    document.documentElement.setAttribute("data-theme", normalized);
    window.localStorage.setItem(THEME_KEY, normalized);
    if (el.themeToggleBtn) {
      el.themeToggleBtn.textContent = normalized === "dark" ? "Switch to light mode" : "Switch to dark mode";
    }
  }

  function currentTheme() {
    return document.documentElement.getAttribute("data-theme") || "dark";
  }

  function installLogoFallback() {
    const images = Array.from(document.querySelectorAll("img"));
    images.forEach((img) => {
      const src = img.getAttribute("src") || "";
      if (!src.includes("/assets/brand/")) return;
      const fallback = [
        src,
        "/assets/brand/churpay-logo-500x250.png",
        "/assets/brand/churpay-logo.png",
        "/assets/churpay-logo.png",
        "/assets/brand/churpay-mark.svg",
        "/favicon.png",
      ];
      const list = Array.from(new Set(fallback));
      let index = 0;
      img.addEventListener("error", () => {
        index += 1;
        if (index < list.length) {
          img.src = list[index];
          return;
        }
        img.style.visibility = "hidden";
      });
    });
  }

  function parseContentDispositionFilename(headerValue) {
    const raw = String(headerValue || "");
    const match = raw.match(/filename\*=UTF-8''([^;]+)|filename=\"?([^\";]+)\"?/i);
    const encoded = match && (match[1] || match[2]);
    if (!encoded) return "export.csv";
    try {
      return decodeURIComponent(encoded);
    } catch (_err) {
      return encoded;
    }
  }

  function pathToTab(pathname) {
    const path = pathname || "/super/";
    const clean = path.replace(/\/+$/, "") || "/super";
    if (clean === "/super" || clean === "/super/dashboard") return { tab: "dashboard", churchId: "" };
    if (clean === "/super/churches") return { tab: "churches", churchId: "" };
    if (clean.startsWith("/super/churches/")) {
      const churchId = decodeURIComponent(clean.split("/super/churches/")[1] || "").trim();
      return { tab: "churches", churchId };
    }
    if (clean === "/super/transactions") return { tab: "transactions", churchId: "" };
    if (clean === "/super/funds") return { tab: "funds", churchId: "" };
    if (clean === "/super/members") return { tab: "members", churchId: "" };
    if (clean === "/super/settings") return { tab: "settings", churchId: "" };
    if (clean === "/super/audit-logs") return { tab: "audit", churchId: "" };
    return { tab: "dashboard", churchId: "" };
  }

  function tabToPath(tab, churchId) {
    if (tab === "churches" && churchId) return `/super/churches/${encodeURIComponent(churchId)}`;
    return TAB_PATHS[tab] || "/super/dashboard";
  }

  async function apiRequest(path, options = {}) {
    const headers = Object.assign({}, options.headers || {});
    if (state.token) headers.Authorization = `Bearer ${state.token}`;

    let body;
    if (typeof options.body !== "undefined") {
      headers["Content-Type"] = "application/json";
      body = JSON.stringify(options.body);
    }

    const response = await fetch(path, {
      method: options.method || "GET",
      headers,
      body,
    });

    if (options.raw) {
      if (!response.ok) {
        const text = await response.text();
        const json = parseJsonSafe(text);
        const msg = (json && (json.error || json.message)) || `HTTP ${response.status}`;
        const err = new Error(msg);
        err.status = response.status;
        throw err;
      }
      return response;
    }

    const text = await response.text();
    const json = parseJsonSafe(text);

    if (!response.ok) {
      const msg = (json && (json.error || json.message)) || `HTTP ${response.status}`;
      const err = new Error(msg);
      err.status = response.status;
      err.payload = json || text;
      throw err;
    }

    return json;
  }

  function renderSkeletonRows(tbody, cols, rows = 4) {
    if (!tbody) return;
    const colCount = Math.max(1, Number(cols || 1));
    const rowCount = Math.max(1, Number(rows || 1));
    let html = "";
    for (let i = 0; i < rowCount; i += 1) {
      html += `<tr class=\"skeleton-row\">${"<td>&nbsp;</td>".repeat(colCount)}</tr>`;
    }
    tbody.innerHTML = html;
  }

  function renderEmpty(tbody, cols, message) {
    if (!tbody) return;
    tbody.innerHTML = `
      <tr>
        <td colspan="${Number(cols || 1)}">
          <div class="empty-state">${escapeHtml(message || "No records found.")}</div>
        </td>
      </tr>
    `;
  }

  function statusBadgeClass(status) {
    const s = String(status || "").toLowerCase();
    if (s === "paid" || s === "complete" || s === "active") return "paid";
    if (s === "pending") return "pending";
    return "failed";
  }

  function setPageMeta(tab) {
    if (el.pageTitle) el.pageTitle.textContent = TAB_TITLES[tab] || "Dashboard";
    if (el.pageKicker) el.pageKicker.textContent = tab === "dashboard" ? "Super Admin Portal" : "Platform Control";
  }

  function activateTab(tab, pushHistory = true) {
    state.currentTab = tab;
    $$(".nav-link[data-tab]").forEach((btn) => {
      btn.classList.toggle("active", btn.getAttribute("data-tab") === tab);
    });
    $$(".panel[id^='panel-']").forEach((panel) => {
      panel.classList.toggle("hidden", panel.id !== `panel-${tab}`);
    });

    setPageMeta(tab);
    setSidebarOpen(false);

    if (pushHistory) {
      const nextPath = tabToPath(tab, state.selectedChurchId);
      if (window.location.pathname !== nextPath) {
        window.history.pushState({ tab, churchId: state.selectedChurchId }, "", nextPath);
      }
    }

    void ensureTabLoaded(tab);
  }

  async function ensureAuth() {
    state.token = window.localStorage.getItem(TOKEN_KEY) || "";
    if (!state.token) {
      window.location.assign("/super/login/");
      return false;
    }

    try {
      const me = await apiRequest("/api/super/me");
      state.profile = me.profile || null;
      if (el.superName) el.superName.textContent = state.profile?.fullName || "Super Admin";
      if (el.superMeta) el.superMeta.textContent = state.profile?.email || "Platform-wide control";
      startInactivityWatch();
      return true;
    } catch (err) {
      if (err.status === 403) {
        performLogout("", "/admin/");
      } else {
        performLogout();
      }
      return false;
    }
  }

  async function loadChurchOptions() {
    const data = await apiRequest("/api/super/churches" + buildQuery({ limit: 200, offset: 0 }));
    state.churches = Array.isArray(data.churches) ? data.churches : [];

    const churchSelectTargets = [
      el.dashChurchSelect,
      el.txChurchSelect,
      el.fundChurchSelect,
      el.memberChurchSelect,
    ];

    churchSelectTargets.forEach((select) => {
      if (!select) return;
      const current = select.value;
      select.innerHTML = '<option value="">All churches</option>';
      state.churches.forEach((church) => {
        const option = document.createElement("option");
        option.value = church.id;
        option.textContent = church.name;
        select.appendChild(option);
      });
      if (current) select.value = current;
    });
  }

  async function loadFundsOptions() {
    const data = await apiRequest("/api/super/funds" + buildQuery({ limit: 300, offset: 0 }));
    state.funds = Array.isArray(data.funds) ? data.funds : [];

    const targets = [el.txFundSelect];
    targets.forEach((select) => {
      if (!select) return;
      const current = select.value;
      select.innerHTML = '<option value="">All funds</option>';
      state.funds.forEach((fund) => {
        const option = document.createElement("option");
        option.value = fund.id;
        option.textContent = `${fund.name} (${fund.churchName || "-"})`;
        select.appendChild(option);
      });
      if (current) select.value = current;
    });
  }

  async function loadDashboard() {
    const query = {
      churchId: el.dashChurchSelect?.value || "",
      from: el.dashFromInput?.value || "",
      to: el.dashToInput?.value || "",
    };

    renderSkeletonRows(el.dashboardRecentBody, 6, 9);
    const data = await apiRequest("/api/super/dashboard/summary" + buildQuery(query));

    const summary = data.summary || {};
    el.statTodayTotal.textContent = formatMoney(summary.todayTotal);
    el.statWeekTotal.textContent = formatMoney(summary.weekTotal);
    el.statMonthTotal.textContent = formatMoney(summary.monthTotal);
    if (el.statTotalFeesCollected) el.statTotalFeesCollected.textContent = formatMoney(summary.totalFeesCollected);
    if (el.statTotalSuperadminCut) el.statTotalSuperadminCut.textContent = formatMoney(summary.totalSuperadminCut);
    if (el.statNetPlatformRevenue) el.statNetPlatformRevenue.textContent = formatMoney(summary.netPlatformRevenue);
    el.statChurches.textContent = String(summary.totalChurches || 0);
    el.statFunds.textContent = String(summary.activeFunds || 0);
    el.statFailed.textContent = String(summary.failedPayments || 0);

    if (Array.isArray(data.churches) && data.churches.length) {
      state.churches = data.churches;
      await populateChurchSelectsFromState();
    }

    const rows = Array.isArray(data.recentTransactions) ? data.recentTransactions : [];
    if (!rows.length) {
      renderEmpty(el.dashboardRecentBody, 9, "No transactions found for this filter.");
      return;
    }

    el.dashboardRecentBody.innerHTML = rows
      .map((row) => {
        const status = String(row.status || "PAID").toUpperCase();
        return `
          <tr>
            <td>${escapeHtml(row.reference || "-")}</td>
            <td>${escapeHtml(formatMoney(row.amount))}</td>
            <td>${escapeHtml(formatMoney(row.platformFeeAmount))}</td>
            <td>${escapeHtml(formatMoney(row.amountGross || row.amount))}</td>
            <td>${escapeHtml(formatMoney(row.superadminCutAmount))}</td>
            <td>${escapeHtml(row.churchName || "-")}</td>
            <td>${escapeHtml(row.fundName || "-")}</td>
            <td><span class="badge ${statusBadgeClass(status)}">${escapeHtml(status)}</span></td>
            <td>${escapeHtml(formatDate(row.createdAt))}</td>
          </tr>
        `;
      })
      .join("");
  }

  async function populateChurchSelectsFromState() {
    const churchSelectTargets = [
      el.dashChurchSelect,
      el.txChurchSelect,
      el.fundChurchSelect,
      el.memberChurchSelect,
    ];

    churchSelectTargets.forEach((select) => {
      if (!select) return;
      const current = select.value;
      select.innerHTML = '<option value="">All churches</option>';
      state.churches.forEach((church) => {
        const option = document.createElement("option");
        option.value = church.id;
        option.textContent = church.name;
        select.appendChild(option);
      });
      if (current) select.value = current;
    });
  }

  function selectedChurchById(id) {
    return state.churchRows.find((church) => church.id === id) || state.churches.find((church) => church.id === id) || null;
  }

  async function loadChurches() {
    renderSkeletonRows(el.churchesBody, 5, 6);
    const data = await apiRequest(
      "/api/super/churches" +
        buildQuery({
          search: el.churchSearchInput?.value || "",
          limit: Number(el.churchLimitInput?.value || 50),
          offset: 0,
        })
    );

    const rows = Array.isArray(data.churches) ? data.churches : [];
    state.churchRows = rows;

    if (!rows.length) {
      renderEmpty(el.churchesBody, 5, "No churches found.");
      return;
    }

    el.churchesBody.innerHTML = rows
      .map((church) => {
        const active = !!church.active;
        return `
          <tr>
            <td>${escapeHtml(church.name || "-")}</td>
            <td>${escapeHtml(church.joinCode || "-")}</td>
            <td><span class="badge ${active ? "active" : "inactive"}">${active ? "Active" : "Disabled"}</span></td>
            <td>${escapeHtml(formatDate(church.createdAt))}</td>
            <td class="actions-cell">
              <button class="btn ghost" type="button" data-action="view" data-id="${escapeHtml(church.id)}">View</button>
              <button class="btn ghost" type="button" data-action="edit" data-id="${escapeHtml(church.id)}">Edit</button>
              <button class="btn ghost" type="button" data-action="toggle" data-id="${escapeHtml(church.id)}" data-active="${active ? "1" : "0"}">${active ? "Disable" : "Enable"}</button>
            </td>
          </tr>
        `;
      })
      .join("");
  }

  function openChurchDialog(church) {
    state.churchDialogMode = church ? "edit" : "create";
    state.churchDialogId = church ? church.id : "";

    el.churchDialogTitle.textContent = church ? "Edit church" : "Create church";
    el.churchDialogSaveBtn.textContent = church ? "Update church" : "Create church";
    el.churchDialogName.value = church?.name || "";
    el.churchDialogJoinCode.value = church?.joinCode || "";
    el.churchDialogActive.checked = church ? !!church.active : true;

    if (el.churchDialog && typeof el.churchDialog.showModal === "function") {
      el.churchDialog.showModal();
    }
  }

  async function saveChurchFromDialog() {
    const name = String(el.churchDialogName.value || "").trim();
    const joinCode = String(el.churchDialogJoinCode.value || "").trim();
    const active = !!el.churchDialogActive.checked;

    if (!name) {
      toast("Church name is required", "error");
      return;
    }

    const body = { name };
    if (joinCode) body.joinCode = joinCode;
    if (state.churchDialogMode === "edit") body.active = active;

    if (state.churchDialogMode === "create") {
      const created = await apiRequest("/api/super/churches", { method: "POST", body });
      const createdChurch = created?.church;
      if (createdChurch?.id) state.selectedChurchId = createdChurch.id;
      toast("Church created", "success");
    } else {
      await apiRequest(`/api/super/churches/${encodeURIComponent(state.churchDialogId)}`, {
        method: "PATCH",
        body,
      });
      state.selectedChurchId = state.churchDialogId;
      toast("Church updated", "success");
    }

    el.churchDialog.close("ok");
    await loadChurches();
    await loadChurchDetail(state.selectedChurchId);
  }

  async function loadChurchDetail(churchId) {
    if (!churchId) {
      el.churchDetailsMeta.textContent = "Select a church to view details.";
      renderEmpty(el.churchDetailFundsBody, 4, "No church selected.");
      renderEmpty(el.churchDetailAdminsBody, 4, "No church selected.");
      renderEmpty(el.churchDetailTransactionsBody, 5, "No church selected.");
      return;
    }

    renderSkeletonRows(el.churchDetailFundsBody, 4, 3);
    renderSkeletonRows(el.churchDetailAdminsBody, 4, 3);
    renderSkeletonRows(el.churchDetailTransactionsBody, 5, 4);

    const data = await apiRequest(`/api/super/churches/${encodeURIComponent(churchId)}`);
    const church = data.church || selectedChurchById(churchId);

    if (church) {
      el.churchDetailsMeta.textContent = `${church.name} | Join code: ${church.joinCode || "-"} | Status: ${church.active ? "Active" : "Disabled"}`;
    } else {
      el.churchDetailsMeta.textContent = "Church details loaded.";
    }

    const funds = Array.isArray(data.funds) ? data.funds : [];
    if (!funds.length) {
      renderEmpty(el.churchDetailFundsBody, 4, "No funds found for this church.");
    } else {
      el.churchDetailFundsBody.innerHTML = funds
        .map((fund) => {
          return `
            <tr>
              <td>${escapeHtml(fund.name || "-")}</td>
              <td>${escapeHtml(fund.code || "-")}</td>
              <td><span class="badge ${fund.active ? "active" : "inactive"}">${fund.active ? "Active" : "Disabled"}</span></td>
              <td>${escapeHtml(formatDate(fund.createdAt))}</td>
            </tr>
          `;
        })
        .join("");
    }

    const admins = Array.isArray(data.admins) ? data.admins : [];
    if (!admins.length) {
      renderEmpty(el.churchDetailAdminsBody, 4, "No church admins found.");
    } else {
      el.churchDetailAdminsBody.innerHTML = admins
        .map((member) => {
          return `
            <tr>
              <td>${escapeHtml(member.fullName || "-")}</td>
              <td>${escapeHtml(member.phone || "-")}</td>
              <td>${escapeHtml(member.email || "-")}</td>
              <td><span class="badge ${member.role === "super" ? "pending" : "active"}">${escapeHtml(String(member.role || "member").toUpperCase())}</span></td>
            </tr>
          `;
        })
        .join("");
    }

    const txs = Array.isArray(data.transactions) ? data.transactions : [];
    if (!txs.length) {
      renderEmpty(el.churchDetailTransactionsBody, 5, "No transactions for this church yet.");
    } else {
      el.churchDetailTransactionsBody.innerHTML = txs
        .slice(0, 50)
        .map((tx) => {
          const status = String(tx.status || "PAID").toUpperCase();
          return `
            <tr>
              <td>${escapeHtml(tx.reference || "-")}</td>
              <td>${escapeHtml(formatMoney(tx.amount))}</td>
              <td>${escapeHtml(tx.fundName || "-")}</td>
              <td><span class="badge ${statusBadgeClass(status)}">${escapeHtml(status)}</span></td>
              <td>${escapeHtml(formatDate(tx.createdAt))}</td>
            </tr>
          `;
        })
        .join("");
    }

    state.selectedChurchId = churchId;
    if (state.currentTab === "churches") {
      const path = tabToPath("churches", churchId);
      if (window.location.pathname !== path) {
        window.history.pushState({ tab: "churches", churchId }, "", path);
      }
    }
  }

  async function toggleChurchStatus(churchId, currentlyActive) {
    const row = selectedChurchById(churchId);
    const willActivate = !currentlyActive;
    const confirmed = window.confirm(
      `${willActivate ? "Enable" : "Disable"} church${row?.name ? ` \"${row.name}\"` : ""}?`
    );
    if (!confirmed) return;

    await apiRequest(`/api/super/churches/${encodeURIComponent(churchId)}`, {
      method: "PATCH",
      body: { active: willActivate },
    });
    toast(`Church ${willActivate ? "enabled" : "disabled"}`, "success");
    await loadChurches();
    if (state.selectedChurchId === churchId) {
      await loadChurchDetail(churchId);
    }
  }

  function currentTxFilters() {
    return {
      churchId: el.txChurchSelect?.value || "",
      fundId: el.txFundSelect?.value || "",
      provider: el.txProviderSelect?.value || "",
      status: el.txStatusSelect?.value || "",
      search: el.txSearchInput?.value || "",
      from: el.txFromInput?.value || "",
      to: el.txToInput?.value || "",
      limit: Math.max(1, Math.min(200, Number(el.txLimitInput?.value || 25))),
      offset: 0,
    };
  }

  async function loadTransactions() {
    const query = currentTxFilters();
    state.txFilters = query;

    renderSkeletonRows(el.transactionsBody, 8, 11);
    const data = await apiRequest("/api/super/transactions" + buildQuery(query));
    const rows = Array.isArray(data.transactions) ? data.transactions : [];
    const meta = data.meta || { count: 0, returned: rows.length, limit: query.limit, offset: query.offset };

    el.txMeta.textContent = `Showing ${meta.returned || rows.length} of ${meta.count || 0}`;

    if (!rows.length) {
      renderEmpty(el.transactionsBody, 11, "No transactions found.");
      return;
    }

    el.transactionsBody.innerHTML = rows
      .map((tx) => {
        const status = String(tx.status || "PAID").toUpperCase();
        return `
          <tr>
            <td>${escapeHtml(tx.reference || "-")}</td>
            <td>${escapeHtml(formatMoney(tx.amount))}</td>
            <td>${escapeHtml(formatMoney(tx.platformFeeAmount))}</td>
            <td>${escapeHtml(formatMoney(tx.amountGross || tx.amount))}</td>
            <td>${escapeHtml(formatMoney(tx.superadminCutAmount))}</td>
            <td>${escapeHtml(tx.churchName || "-")}</td>
            <td>${escapeHtml(tx.fundName || "-")}</td>
            <td>${escapeHtml(tx.memberName || tx.memberPhone || "-")}</td>
            <td>${escapeHtml((tx.provider || tx.channel || "-").toUpperCase())}</td>
            <td><span class="badge ${statusBadgeClass(status)}">${escapeHtml(status)}</span></td>
            <td>${escapeHtml(formatDate(tx.createdAt))}</td>
          </tr>
        `;
      })
      .join("");
  }

  async function exportTransactions() {
    const query = Object.assign({}, state.txFilters, { limit: 10000, offset: 0 });
    const response = await apiRequest("/api/super/transactions/export" + buildQuery(query), { raw: true });
    const blob = await response.blob();
    const filename = parseContentDispositionFilename(response.headers.get("content-disposition"));
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename || "super-transactions.csv";
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
    toast("CSV export downloaded", "success");
  }

  async function loadFunds() {
    renderSkeletonRows(el.fundsBody, 6, 6);
    const query = {
      churchId: el.fundChurchSelect?.value || "",
      search: el.fundSearchInput?.value || "",
      limit: 200,
      offset: 0,
    };

    const data = await apiRequest("/api/super/funds" + buildQuery(query));
    const rows = Array.isArray(data.funds) ? data.funds : [];
    state.funds = rows;

    if (!rows.length) {
      renderEmpty(el.fundsBody, 6, "No funds found.");
      return;
    }

    el.fundsBody.innerHTML = rows
      .map((fund) => {
        return `
          <tr>
            <td>${escapeHtml(fund.name || "-")}</td>
            <td>${escapeHtml(fund.code || "-")}</td>
            <td>${escapeHtml(fund.churchName || "-")}</td>
            <td><span class="badge ${fund.active ? "active" : "inactive"}">${fund.active ? "Active" : "Inactive"}</span></td>
            <td>${escapeHtml(formatDate(fund.createdAt))}</td>
            <td class="actions-cell">
              <button class="btn ghost" type="button" data-fund-action="qr" data-id="${escapeHtml(fund.id)}">View QR</button>
              <button class="btn ghost" type="button" data-fund-action="edit" data-id="${escapeHtml(fund.id)}">Edit</button>
              <button class="btn ghost" type="button" data-fund-action="toggle" data-id="${escapeHtml(fund.id)}" data-active="${fund.active ? "1" : "0"}">${fund.active ? "Disable" : "Enable"}</button>
            </td>
          </tr>
        `;
      })
      .join("");
  }

  function findFund(id) {
    return state.funds.find((fund) => fund.id === id) || null;
  }

  async function openFundQr(fundId) {
    const fund = findFund(fundId);
    if (!fund) {
      toast("Fund not found", "error");
      return;
    }

    const data = await apiRequest(
      "/api/super/qr" +
        buildQuery({
          churchId: fund.churchId,
          fundId: fund.id,
        })
    );

    el.qrChurchName.value = data?.church?.name || fund.churchName || "";
    el.qrFundName.value = data?.fund?.name || fund.name || "";
    el.qrPayload.value = JSON.stringify(data?.qrPayload || {}, null, 2);
    el.qrDeepLink.value = data?.deepLink || "";
    el.qrWebLink.value = data?.webLink || "";

    if (el.qrDialog && typeof el.qrDialog.showModal === "function") {
      el.qrDialog.showModal();
    }
  }

  async function editFund(fundId) {
    const fund = findFund(fundId);
    if (!fund) {
      toast("Fund not found", "error");
      return;
    }

    const name = window.prompt("Fund name", fund.name || "");
    if (name === null) return;
    const trimmedName = String(name).trim();
    if (!trimmedName) {
      toast("Fund name is required", "error");
      return;
    }

    const code = window.prompt("Fund code", fund.code || "");
    if (code === null) return;
    const trimmedCode = String(code).trim().toLowerCase();
    if (!trimmedCode) {
      toast("Fund code is required", "error");
      return;
    }

    await apiRequest(`/api/super/funds/${encodeURIComponent(fundId)}`, {
      method: "PATCH",
      body: { name: trimmedName, code: trimmedCode },
    });
    toast("Fund updated", "success");
    await loadFunds();
  }

  async function toggleFund(fundId, currentlyActive) {
    const fund = findFund(fundId);
    if (!fund) {
      toast("Fund not found", "error");
      return;
    }

    const willActivate = !currentlyActive;
    const confirmed = window.confirm(
      `${willActivate ? "Enable" : "Disable"} fund \"${fund.name || fund.code || fund.id}\"?`
    );
    if (!confirmed) return;

    await apiRequest(`/api/super/funds/${encodeURIComponent(fundId)}`, {
      method: "PATCH",
      body: { active: willActivate },
    });
    toast(`Fund ${willActivate ? "enabled" : "disabled"}`, "success");
    await loadFunds();
  }

  async function loadMembers() {
    renderSkeletonRows(el.membersBody, 7, 6);
    const query = {
      search: el.memberSearchInput?.value || "",
      role: el.memberRoleSelect?.value || "",
      churchId: el.memberChurchSelect?.value || "",
      limit: 100,
      offset: 0,
    };

    const data = await apiRequest("/api/super/members" + buildQuery(query));
    const rows = Array.isArray(data.members) ? data.members : [];

    if (!rows.length) {
      renderEmpty(el.membersBody, 7, "No members found.");
      return;
    }

    el.membersBody.innerHTML = rows
      .map((member) => {
        const role = String(member.role || "member").toUpperCase();
        return `
          <tr>
            <td>${escapeHtml(member.fullName || "-")}</td>
            <td>${escapeHtml(member.phone || "-")}</td>
            <td>${escapeHtml(member.email || "-")}</td>
            <td>${escapeHtml(member.churchName || "-")}</td>
            <td><span class="badge ${role === "MEMBER" ? "pending" : "active"}">${escapeHtml(role)}</span></td>
            <td>${escapeHtml(formatDate(member.createdAt))}</td>
            <td><button class="btn ghost" type="button" data-member-action="reset" data-id="${escapeHtml(member.id)}">Reset password</button></td>
          </tr>
        `;
      })
      .join("");
  }

  async function loadSettings() {
    const data = await apiRequest("/api/super/settings");
    const settings = data.settings || {};
    const rate = settings.rateLimits || {};

    el.settingsEnvironment.textContent = settings.environment || "-";
    el.settingsWebhook.textContent = settings.webhooks?.payfastNotifyUrl || "Not configured";
    el.settingsRateLimits.textContent = `Global: ${rate.globalMax || "-"} / ${rate.globalWindowMs || "-"}ms | Auth: ${rate.authMax || "-"} / ${rate.authWindowMs || "-"}ms`;
    el.settingsMaintenance.checked = !!settings.maintenanceMode;
  }

  async function loadAuditLogs() {
    renderSkeletonRows(el.auditBody, 3, 3);
    const data = await apiRequest("/api/super/audit-logs");
    const rows = Array.isArray(data.logs) ? data.logs : [];

    if (!rows.length) {
      renderEmpty(el.auditBody, 3, "No audit logs available.");
      return;
    }

    el.auditBody.innerHTML = rows
      .map((row) => {
        return `
          <tr>
            <td>${escapeHtml(formatDate(row.createdAt))}</td>
            <td>${escapeHtml(row.actor || "-")}</td>
            <td>${escapeHtml(row.action || "-")}</td>
          </tr>
        `;
      })
      .join("");
  }

  async function ensureTabLoaded(tab) {
    try {
      if (tab === "dashboard") {
        await loadDashboard();
        state.loaded.dashboard = true;
        return;
      }

      if (tab === "churches") {
        await loadChurches();
        if (state.selectedChurchId) {
          await loadChurchDetail(state.selectedChurchId);
        } else {
          await loadChurchDetail("");
        }
        state.loaded.churches = true;
        return;
      }

      if (tab === "transactions") {
        await loadFundsOptions();
        await loadTransactions();
        state.loaded.transactions = true;
        return;
      }

      if (tab === "funds") {
        await loadFunds();
        state.loaded.funds = true;
        return;
      }

      if (tab === "members") {
        await loadMembers();
        state.loaded.members = true;
        return;
      }

      if (tab === "settings") {
        await loadSettings();
        state.loaded.settings = true;
        return;
      }

      if (tab === "audit") {
        await loadAuditLogs();
        state.loaded.audit = true;
      }
    } catch (err) {
      if (err.status === 401) {
        performLogout();
        return;
      }
      if (err.status === 403) {
        performLogout("", "/admin/");
        return;
      }
      console.error(`[super/${tab}] load error`, err?.message || err, err?.stack);
      showInlineStatus(err?.message || "Failed to load data", "error");
      toast(err?.message || "Request failed", "error");
    }
  }

  function bindEvents() {
    el.themeToggleBtn?.addEventListener("click", () => {
      applyTheme(currentTheme() === "dark" ? "light" : "dark");
    });

    el.sidebarToggleBtn?.addEventListener("click", () => {
      const open = !(el.appView && el.appView.classList.contains("sidebar-open"));
      setSidebarOpen(open);
    });

    el.sidebarOverlay?.addEventListener("click", () => setSidebarOpen(false));

    el.navTabs?.addEventListener("click", (event) => {
      const target = event.target.closest(".nav-link[data-tab]");
      if (!target) return;
      const tab = target.getAttribute("data-tab");
      if (!tab) return;
      activateTab(tab, true);
    });

    el.refreshBtn?.addEventListener("click", () => {
      void ensureTabLoaded(state.currentTab);
    });

    el.logoutBtn?.addEventListener("click", () => {
      performLogout();
    });

    el.applyDashFiltersBtn?.addEventListener("click", () => {
      void loadDashboard();
    });

    el.openCreateChurchBtn?.addEventListener("click", () => openChurchDialog(null));
    el.applyChurchFiltersBtn?.addEventListener("click", () => {
      void loadChurches();
    });

    el.churchesBody?.addEventListener("click", (event) => {
      const actionEl = event.target.closest("button[data-action][data-id]");
      if (!actionEl) return;
      const action = actionEl.getAttribute("data-action");
      const id = actionEl.getAttribute("data-id");
      if (!id) return;

      if (action === "view") {
        state.selectedChurchId = id;
        void loadChurchDetail(id);
        return;
      }

      if (action === "edit") {
        const church = selectedChurchById(id);
        openChurchDialog(church);
        return;
      }

      if (action === "toggle") {
        const active = actionEl.getAttribute("data-active") === "1";
        void toggleChurchStatus(id, active);
      }
    });

    el.churchDialogForm?.addEventListener("submit", (event) => {
      event.preventDefault();
      const submitter = event.submitter;
      if (!submitter || submitter.value !== "ok") {
        el.churchDialog?.close("cancel");
        return;
      }
      void saveChurchFromDialog();
    });

    el.applyTxFiltersBtn?.addEventListener("click", () => {
      void loadTransactions();
    });

    el.exportTxBtn?.addEventListener("click", () => {
      void exportTransactions();
    });

    el.applyFundFiltersBtn?.addEventListener("click", () => {
      void loadFunds();
    });

    el.fundsBody?.addEventListener("click", (event) => {
      const actionEl = event.target.closest("button[data-fund-action][data-id]");
      if (!actionEl) return;
      const action = actionEl.getAttribute("data-fund-action");
      const id = actionEl.getAttribute("data-id");
      if (!id) return;

      if (action === "qr") {
        void openFundQr(id);
        return;
      }

      if (action === "edit") {
        void editFund(id);
        return;
      }

      if (action === "toggle") {
        const active = actionEl.getAttribute("data-active") === "1";
        void toggleFund(id, active);
      }
    });

    el.copyQrPayloadBtn?.addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(el.qrPayload.value || "");
        toast("QR payload copied", "success");
      } catch (_err) {
        toast("Unable to copy payload", "error");
      }
    });

    el.copyQrWebBtn?.addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(el.qrWebLink.value || "");
        toast("Web link copied", "success");
      } catch (_err) {
        toast("Unable to copy web link", "error");
      }
    });

    el.applyMemberFiltersBtn?.addEventListener("click", () => {
      void loadMembers();
    });

    el.membersBody?.addEventListener("click", (event) => {
      const actionEl = event.target.closest("button[data-member-action]");
      if (!actionEl) return;
      const action = actionEl.getAttribute("data-member-action");
      if (action === "reset") {
        toast("Password reset workflow coming soon", "info");
      }
    });

    window.addEventListener("popstate", () => {
      const parsed = pathToTab(window.location.pathname);
      state.selectedChurchId = parsed.churchId || "";
      activateTab(parsed.tab, false);
    });
  }

  async function init() {
    showLoading(true);
    installLogoFallback();
    applyTheme(window.localStorage.getItem(THEME_KEY) || "dark");
    bindEvents();

    const authed = await ensureAuth();
    if (!authed) return;

    el.appView.classList.remove("hidden");

    try {
      await loadChurchOptions();
      await loadFundsOptions();

      const parsed = pathToTab(window.location.pathname);
      state.selectedChurchId = parsed.churchId || "";
      activateTab(parsed.tab, false);
      showInlineStatus("");
    } catch (err) {
      console.error("[super/init] error", err?.message || err, err?.stack);
      showInlineStatus(err?.message || "Failed to initialize super portal", "error");
    } finally {
      showLoading(false);
    }
  }

  init();
})();
