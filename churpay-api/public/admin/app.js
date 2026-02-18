(function () {
  "use strict";

  const TOKEN_KEY = "churpay.admin.token";
  const THEME_KEY = "churpay.admin.theme";
  const LAST_ACTIVITY_KEY = "churpay.admin.lastActivityAt";
  const INACTIVITY_TIMEOUT_MS = 15 * 60 * 1000;
  const INACTIVITY_WARNING_BEFORE_MS = 60 * 1000;
  const ACTIVITY_EVENTS = ["pointerdown", "click", "keydown", "touchstart", "input", "wheel"];
  const SYSTEM_THEME_QUERY = "(prefers-color-scheme: dark)";

  const state = {
    token: "",
    profile: null,
    allowedTabs: [],
    portalSettings: { accountantTabs: [] },
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
    statementFilters: {
      from: "",
      to: "",
      allStatuses: false,
    },
    authTwoFactor: null,
    payfastStatus: null,
  };

  const TAB_TITLE = {
    dashboard: "Dashboard",
    transactions: "Transactions",
    statements: "Statements",
    funds: "Funds",
    qr: "QR Codes",
    members: "Members",
    settings: "Settings",
  };

  const ADMIN_PORTAL_TABS = Object.keys(TAB_TITLE);
  const DEFAULT_ACCOUNTANT_TABS = ["dashboard", "transactions", "statements"];
  const ACCOUNTANT_TAB_LABELS = {
    dashboard: "Dashboard",
    transactions: "Transactions",
    statements: "Statements",
    funds: "Funds",
    qr: "QR codes",
    members: "Members",
  };

  const $ = (id) => document.getElementById(id);
  const $$ = (selector) => Array.from(document.querySelectorAll(selector));

  const el = {
    loadingScreen: $("loadingScreen"),
    authView: $("authView"),
    appView: $("appView"),
    authError: $("authError"),
    loginForm: $("loginForm"),
    credentialFields: $("credentialFields"),
    identifierInput: $("identifierInput"),
    passwordInput: $("passwordInput"),
    twoFactorFields: $("twoFactorFields"),
    twoFactorHint: $("twoFactorHint"),
    twoFactorCodeInput: $("twoFactorCodeInput"),
    twoFactorBackBtn: $("twoFactorBackBtn"),
    loginBtn: $("loginBtn"),

    toastContainer: $("toastContainer"),
    confirmDialog: $("confirmDialog"),
    confirmTitle: $("confirmTitle"),
    confirmBody: $("confirmBody"),
    confirmOkBtn: $("confirmOkBtn"),
    confirmCancelBtn: $("confirmCancelBtn"),
    promptDialog: $("promptDialog"),
    promptTitle: $("promptTitle"),
    promptBody: $("promptBody"),
    promptLabel: $("promptLabel"),
    promptInput: $("promptInput"),
    promptOkBtn: $("promptOkBtn"),
    promptCancelBtn: $("promptCancelBtn"),

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

    statementFromInput: $("statementFromInput"),
    statementToInput: $("statementToInput"),
    statementAllStatusesInput: $("statementAllStatusesInput"),
    loadStatementBtn: $("loadStatementBtn"),
    openStatementPrintBtn: $("openStatementPrintBtn"),
    downloadStatementBtn: $("downloadStatementBtn"),
    statementDonationTotal: $("statementDonationTotal"),
    statementFeeTotal: $("statementFeeTotal"),
    statementPayfastFeeTotal: $("statementPayfastFeeTotal"),
    statementNetReceivedTotal: $("statementNetReceivedTotal"),
    statementTotalCharged: $("statementTotalCharged"),
    statementTxCount: $("statementTxCount"),
    statementMeta: $("statementMeta"),
    statementByFundBody: $("statementByFundBody"),
    statementByMethodBody: $("statementByMethodBody"),

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

    accountantAccessCard: $("accountantAccessCard"),
    accountantAccessMeta: $("accountantAccessMeta"),
    saveAccountantAccessBtn: $("saveAccountantAccessBtn"),
    accTabDashboard: $("accTabDashboard"),
    accTabTransactions: $("accTabTransactions"),
    accTabStatements: $("accTabStatements"),
    accTabFunds: $("accTabFunds"),
    accTabQr: $("accTabQr"),
    accTabMembers: $("accTabMembers"),

    payfastSetupCard: $("payfastSetupCard"),
    payfastStatusBadge: $("payfastStatusBadge"),
    payfastStatusMeta: $("payfastStatusMeta"),
    openPayfastConnectBtn: $("openPayfastConnectBtn"),
    refreshPayfastStatusBtn: $("refreshPayfastStatusBtn"),

    payfastConnectDialog: $("payfastConnectDialog"),
    payfastConnectError: $("payfastConnectError"),
    payfastMaskText: $("payfastMaskText"),
    payfastMerchantIdInput: $("payfastMerchantIdInput"),
    payfastMerchantKeyInput: $("payfastMerchantKeyInput"),
    payfastPassphraseInput: $("payfastPassphraseInput"),
    payfastModalCloseBtn: $("payfastModalCloseBtn"),
    payfastDisconnectBtn: $("payfastDisconnectBtn"),
    payfastConnectSubmitBtn: $("payfastConnectSubmitBtn"),
  };

  let inactivityTimerId = 0;
  let inactivityWarningTimerId = 0;
  let inactivityWatchdogId = 0;
  let inactivityListening = false;
  let lastActivityAt = Date.now();
  const systemThemeMedia = window.matchMedia ? window.matchMedia(SYSTEM_THEME_QUERY) : null;
  let systemThemeListening = false;

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

  function formatPayerDisplay(row) {
    const paymentSource = String(row?.paymentSource || "DIRECT_APP").toUpperCase();
    const payerType = String(row?.payerType || "member").toLowerCase();
    const payer = row?.memberName || row?.memberPhone || row?.memberEmail || "-";
    const beneficiary = row?.onBehalfOfMemberName || row?.onBehalfOfMemberPhone || row?.onBehalfOfMemberEmail || "-";
    if (paymentSource === "SHARE_LINK" || payerType === "on_behalf") {
      return `Paid for ${beneficiary} (payer: ${payer})`;
    }
    if (payerType === "visitor") return `Visitor: ${payer}`;
    return payer;
  }

  function formatMethodDisplay(row) {
    const provider = String(row?.provider || "").trim().toLowerCase();
    if (!provider) return "-";
    if (provider === "payfast") return "PAYFAST";
    if (provider === "cash") return "CASH";
    return provider.toUpperCase();
  }

  function formatChannelDisplay(row) {
    const channel = String(row?.channel || "").trim().toLowerCase();
    if (!channel) return "-";
    return channel.toUpperCase();
  }

  function needsCashApproval(row) {
    const provider = String(row?.provider || "").trim().toLowerCase();
    if (provider !== "cash") return false;
    const verified = !!row?.cashVerifiedByAdmin;
    const status = parseTransactionStatus(row);
    return !verified && (status === "PREPARED" || status === "RECORDED");
  }

  function renderTransactionActions(row) {
    const paymentIntentId = row?.paymentIntentId || row?.payment_intent_id || "";
    if (!paymentIntentId) return `<span class="muted">-</span>`;
    if (!needsCashApproval(row)) return `<span class="muted">-</span>`;

    return `
      <div class="actions-cell">
        <button class="btn primary" data-action="cash-confirm" data-id="${escapeHtml(paymentIntentId)}" type="button">Confirm cash</button>
        <button class="btn danger" data-action="cash-reject" data-id="${escapeHtml(paymentIntentId)}" type="button">Reject</button>
      </div>
    `.trim();
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
    if (show) {
      setSidebarOpen(false);
      setAuthStep(null);
    }
  }

  function setAuthStep(twoFactorPayload = null) {
    const challengeId = String(twoFactorPayload?.challengeId || "").trim();
    const active = !!challengeId;
    state.authTwoFactor = active
      ? {
          challengeId,
          emailMasked: String(twoFactorPayload?.emailMasked || "").trim(),
        }
      : null;

    if (el.credentialFields) el.credentialFields.classList.toggle("hidden", active);
    if (el.twoFactorFields) el.twoFactorFields.classList.toggle("hidden", !active);
    if (el.identifierInput) el.identifierInput.required = !active;
    if (el.passwordInput) el.passwordInput.required = !active;
    if (el.twoFactorCodeInput) {
      el.twoFactorCodeInput.required = active;
      if (!active) el.twoFactorCodeInput.value = "";
    }
    if (el.twoFactorHint) {
      const masked = state.authTwoFactor?.emailMasked || "your email";
      el.twoFactorHint.textContent = active
        ? `Enter the 6-digit sign-in code sent to ${masked}.`
        : "Enter the 6-digit sign-in code.";
    }
    if (el.loginBtn) {
      el.loginBtn.textContent = active ? "Verify code" : "Sign in";
    }
    if (active && el.twoFactorCodeInput) {
      window.setTimeout(() => el.twoFactorCodeInput.focus(), 0);
    }
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

  function clearInactivityWarningTimer() {
    if (!inactivityWarningTimerId) return;
    window.clearTimeout(inactivityWarningTimerId);
    inactivityWarningTimerId = 0;
  }

  function clearInactivityWatchdog() {
    if (!inactivityWatchdogId) return;
    window.clearInterval(inactivityWatchdogId);
    inactivityWatchdogId = 0;
  }

  function stopInactivityWatch() {
    clearInactivityTimer();
    clearInactivityWarningTimer();
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
    clearInactivityWarningTimer();
    if (!state.token) return;
    const elapsed = Date.now() - readSharedLastActivity();
    const remaining = Math.max(0, INACTIVITY_TIMEOUT_MS - elapsed);
    if (remaining <= 0) {
      void onLogout({ silent: true, reason: "You were logged out after 15 minutes of inactivity." });
      return;
    }

    const warningDelay = remaining - INACTIVITY_WARNING_BEFORE_MS;
    if (warningDelay > 0) {
      inactivityWarningTimerId = window.setTimeout(() => {
        if (!state.token) return;
        if (document.hidden) return;
        const active = document.activeElement;
        const tag = active ? String(active.tagName || "").toLowerCase() : "";
        const midForm =
          tag === "input" ||
          tag === "textarea" ||
          tag === "select" ||
          !!(active && active.isContentEditable) ||
          !!document.querySelector("dialog[open]");
        if (!midForm) return;

        const keep = window.confirm("You’ve been inactive. Stay signed in?");
        if (keep) {
          onUserActivity();
          return;
        }
        void onLogout({ silent: true, reason: "You were logged out after 15 minutes of inactivity." });
      }, warningDelay);
    }

    inactivityTimerId = window.setTimeout(() => {
      void onLogout({ silent: true, reason: "You were logged out after 15 minutes of inactivity." });
    }, remaining);
  }

  function onUserActivity() {
    if (!state.token) return;
    writeSharedLastActivity(Date.now());
    scheduleInactivityTimer();
  }

  function onStorageActivity(event) {
    if (!event) return;
    if (event.key === THEME_KEY) {
      applyTheme(event.newValue || "system");
      return;
    }
    if (!state.token) return;
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
      clearInactivityWarningTimer();
      return;
    }
    const elapsed = Date.now() - lastActivityAt;
    if (elapsed >= INACTIVITY_TIMEOUT_MS) {
      void onLogout({ silent: true, reason: "You were logged out after 15 minutes of inactivity." });
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
        void onLogout({ silent: true, reason: "You were logged out after 15 minutes of inactivity." });
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

  function normalizeThemePreference(theme) {
    if (theme === "light" || theme === "dark" || theme === "system") return theme;
    return "system";
  }

  function systemTheme() {
    if (!systemThemeMedia) return "dark";
    return systemThemeMedia.matches ? "dark" : "light";
  }

  function resolveTheme(themePreference) {
    const preference = normalizeThemePreference(themePreference);
    if (preference === "system") return systemTheme();
    return preference;
  }

  function currentThemePreference() {
    return normalizeThemePreference(window.localStorage.getItem(THEME_KEY) || "system");
  }

  function renderThemeToggleLabel(preference, resolvedTheme) {
    if (!el.themeToggleBtn) return;
    if (preference === "system") {
      el.themeToggleBtn.textContent = `Device mode: ${resolvedTheme === "dark" ? "Dark" : "Light"}`;
      return;
    }
    el.themeToggleBtn.textContent = `Locked: ${resolvedTheme === "dark" ? "Dark" : "Light"} (use device mode)`;
  }

  function applyTheme(themePreference) {
    const preference = normalizeThemePreference(themePreference);
    const resolvedTheme = resolveTheme(preference);
    document.documentElement.setAttribute("data-theme", resolvedTheme);
    window.localStorage.setItem(THEME_KEY, preference);
    renderThemeToggleLabel(preference, resolvedTheme);
  }

  function startSystemThemeSync() {
    if (!systemThemeMedia || systemThemeListening) return;
    const onSystemThemeChange = () => {
      if (currentThemePreference() !== "system") return;
      applyTheme("system");
    };
    if (typeof systemThemeMedia.addEventListener === "function") {
      systemThemeMedia.addEventListener("change", onSystemThemeChange);
    } else if (typeof systemThemeMedia.addListener === "function") {
      systemThemeMedia.addListener(onSystemThemeChange);
    }
    systemThemeListening = true;
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
        "/assets/brand/churpay-logo.svg",
        "/assets/brand/churpay-logo-500x250.png",
        "/assets/brand/churpay-logo.png",
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
    if (["paid", "complete", "confirmed"].includes(s)) return "paid";
    if (["pending", "prepared", "recorded"].includes(s)) return "pending";
    if (["failed", "cancelled", "rejected"].includes(s)) return "failed";
    return "failed";
  }

  function isStaffRole(role) {
    const r = String(role || "").toLowerCase();
    return r === "admin" || r === "accountant" || r === "super";
  }

  function isChurchAdminRole(role) {
    const r = String(role || "").toLowerCase();
    return r === "admin" || r === "super";
  }

  function ensureAdminProfile(profile) {
    if (!profile) throw new Error("Profile missing");
    if (!isStaffRole(profile.role)) {
      throw new Error("Staff role required to access portal");
    }
  }

  function normalizeAllowedTabs(tabs) {
    const list = Array.isArray(tabs) ? tabs : [];
    const seen = new Set();
    const out = [];
    list.forEach((raw) => {
      const key = String(raw || "").trim().toLowerCase();
      if (!key) return;
      if (!ADMIN_PORTAL_TABS.includes(key)) return;
      if (seen.has(key)) return;
      seen.add(key);
      out.push(key);
    });
    return out;
  }

  function setAllowedTabs(tabs) {
    const normalized = normalizeAllowedTabs(tabs);
    state.allowedTabs = normalized.length ? normalized : ADMIN_PORTAL_TABS.slice();
  }

  function firstAllowedTab() {
    return (state.allowedTabs && state.allowedTabs[0]) || "dashboard";
  }

  function isTabAllowed(tabName) {
    const key = String(tabName || "").trim().toLowerCase();
    if (!key) return false;
    if (!state.allowedTabs || !state.allowedTabs.length) return true;
    return state.allowedTabs.includes(key);
  }

  function applyTabVisibility() {
    const allowed = new Set(state.allowedTabs || []);
    $$(".nav-link[data-tab]").forEach((btn) => {
      const tab = String(btn.getAttribute("data-tab") || "").trim().toLowerCase();
      const visible = !tab || allowed.has(tab);
      btn.classList.toggle("hidden", !visible);
    });

    const desired = isTabAllowed(state.currentTab) ? state.currentTab : firstAllowedTab();
    switchTab(desired);
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

  async function promptAction({
    title,
    body,
    label = "Value",
    placeholder = "",
    value = "",
    okLabel = "Submit",
    cancelLabel = "Cancel",
    okVariant = "primary", // "primary" | "danger"
    inputType = "text",
  }) {
    if (!el.promptDialog || typeof el.promptDialog.showModal !== "function") {
      const typed = window.prompt(body || title || "Enter value", value);
      return typed === null ? null : String(typed);
    }

    return new Promise((resolve) => {
      el.promptTitle.textContent = title || "Enter details";
      el.promptBody.textContent = body || "Please enter a value.";
      el.promptLabel.textContent = label || "Value";
      el.promptInput.type = inputType || "text";
      el.promptInput.placeholder = placeholder || "";
      el.promptInput.value = value == null ? "" : String(value);
      el.promptCancelBtn.textContent = cancelLabel || "Cancel";
      el.promptOkBtn.textContent = okLabel || "Submit";
      el.promptOkBtn.className = `btn ${okVariant === "danger" ? "danger" : "primary"}`;

      const onEnter = (event) => {
        if (event.key !== "Enter") return;
        event.preventDefault();
        el.promptOkBtn.click();
      };

      const onClose = () => {
        const accepted = el.promptDialog.returnValue === "ok";
        const typed = accepted ? String(el.promptInput.value || "") : null;
        el.promptInput.removeEventListener("keydown", onEnter);
        el.promptDialog.removeEventListener("close", onClose);
        resolve(typed);
      };

      el.promptInput.addEventListener("keydown", onEnter);
      el.promptDialog.addEventListener("close", onClose);
      el.promptDialog.showModal();
      window.setTimeout(() => el.promptInput.focus(), 20);
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
    const resolved = isTabAllowed(tabName) ? tabName : firstAllowedTab();
    state.currentTab = resolved;
    $$(".nav-link[data-tab]").forEach((btn) => {
      const active = btn.getAttribute("data-tab") === resolved;
      btn.classList.toggle("active", active);
    });

    $$(".panel[id^='panel-']").forEach((panel) => {
      panel.classList.toggle("hidden", panel.id !== `panel-${resolved}`);
    });

    el.pageTitle.textContent = TAB_TITLE[resolved] || "Admin";
    el.pageKicker.textContent = resolved === "dashboard" ? "Admin Portal" : "Control Center";
    setSidebarOpen(false);

    if (resolved === "statements") {
      if (el.statementFromInput && !el.statementFromInput.value) el.statementFromInput.value = isoStartOfMonthLocal();
      if (el.statementToInput && !el.statementToInput.value) el.statementToInput.value = isoTodayLocal();
      loadStatementSummary().catch((err) => toast(err.message || "Could not load statement", "error"));
    }
    if (resolved === "settings" && isChurchAdminRole(state.profile?.role)) {
      loadPayfastStatus().catch((err) => toast(err.message || "Could not load PayFast status", "error"));
    }
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
    if (!json || (!json.token && !json.requiresTwoFactor)) throw new Error("Login failed");
    return json;
  }

  async function verifyAdminTwoFactor(challengeId, code) {
    const res = await fetch("/api/auth/login/admin/verify-2fa", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ challengeId, code }),
    });

    const text = await res.text();
    const json = parseJsonSafe(text);
    if (!res.ok) {
      throw new Error((json && json.error) || "Two-factor verification failed");
    }
    if (!json || !json.token) throw new Error("Two-factor verification failed");
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

    const canManageFunds = isChurchAdminRole(profile.role);
    if (el.createFundForm) el.createFundForm.classList.toggle("hidden", !canManageFunds);
  }

  function accountantCheckboxMap() {
    return {
      dashboard: el.accTabDashboard,
      transactions: el.accTabTransactions,
      statements: el.accTabStatements,
      funds: el.accTabFunds,
      qr: el.accTabQr,
      members: el.accTabMembers,
    };
  }

  function setAccountantTabsInUi(tabs) {
    const normalized = normalizeAllowedTabs(tabs).filter((t) => t !== "settings");
    const effective = normalized.length ? normalized : DEFAULT_ACCOUNTANT_TABS.slice();
    const map = accountantCheckboxMap();
    Object.keys(map).forEach((key) => {
      const box = map[key];
      if (!box) return;
      box.checked = effective.includes(key);
    });

    if (el.accountantAccessMeta) {
      el.accountantAccessMeta.textContent = normalized.length
        ? "Saved settings apply immediately for new accountant sessions."
        : "No custom settings saved yet. Showing default access.";
    }
  }

  function readAccountantTabsFromUi() {
    const map = accountantCheckboxMap();
    const selected = [];
    Object.keys(map).forEach((key) => {
      const box = map[key];
      if (box && box.checked) selected.push(key);
    });
    return selected;
  }

  function showAccountantAccessCard(show) {
    if (!el.accountantAccessCard) return;
    el.accountantAccessCard.classList.toggle("hidden", !show);
  }

  function showPayfastSetupCard(show) {
    if (!el.payfastSetupCard) return;
    el.payfastSetupCard.classList.toggle("hidden", !show);
  }

  function setPayfastConnectError(message = "") {
    if (!el.payfastConnectError) return;
    const text = String(message || "").trim();
    el.payfastConnectError.textContent = text;
    el.payfastConnectError.classList.toggle("hidden", !text);
  }

  function renderPayfastStatus(status) {
    state.payfastStatus = status || null;
    if (!el.payfastStatusBadge || !el.payfastStatusMeta) return;

    const connected = !!status?.connected;
    el.payfastStatusBadge.className = `badge ${connected ? "active" : "inactive"}`;
    el.payfastStatusBadge.textContent = connected ? "Connected" : "Not connected";

    if (connected) {
      const connectedAt = status?.connectedAt ? formatDate(status.connectedAt) : "unknown date";
      const merchantIdMasked = status?.merchantIdMasked || "";
      const merchantKeyMasked = status?.merchantKeyMasked || "";
      const parts = [`Connected ${connectedAt}.`];
      if (merchantIdMasked) parts.push(`Merchant ID ${merchantIdMasked}.`);
      if (merchantKeyMasked) parts.push(`Merchant key ${merchantKeyMasked}.`);
      el.payfastStatusMeta.textContent = parts.join(" ");
    } else {
      const lastAttempt = status?.lastAttemptError
        ? ` Last attempt failed: ${status.lastAttemptError}`
        : "";
      el.payfastStatusMeta.textContent =
        "Activate church-level PayFast credentials to start receiving payments directly." + lastAttempt;
    }

    if (el.payfastMaskText) {
      const masked = status?.merchantKeyMasked || "";
      if (masked) {
        el.payfastMaskText.textContent = `Stored merchant key: ${masked}`;
        el.payfastMaskText.classList.remove("hidden");
      } else {
        el.payfastMaskText.textContent = "";
        el.payfastMaskText.classList.add("hidden");
      }
    }

    if (el.payfastDisconnectBtn) {
      el.payfastDisconnectBtn.disabled = !connected;
    }
  }

  async function loadPayfastStatus() {
    if (!isChurchAdminRole(state.profile?.role)) {
      showPayfastSetupCard(false);
      return;
    }
    showPayfastSetupCard(true);
    try {
      const data = await apiRequest("/api/churches/payfast/status");
      renderPayfastStatus(data || {});
    } catch (err) {
      const message = String(err?.message || "");
      if (message.toLowerCase().includes("join a church")) {
        showPayfastSetupCard(false);
        return;
      }
      renderPayfastStatus({ connected: false, lastAttemptError: message || "Could not load PayFast status." });
    }
  }

  function openPayfastConnectDialog() {
    if (!el.payfastConnectDialog) return;
    setPayfastConnectError("");
    if (el.payfastMerchantIdInput) el.payfastMerchantIdInput.value = "";
    if (el.payfastMerchantKeyInput) el.payfastMerchantKeyInput.value = "";
    if (el.payfastPassphraseInput) el.payfastPassphraseInput.value = "";
    if (typeof el.payfastConnectDialog.showModal === "function") {
      el.payfastConnectDialog.showModal();
      window.setTimeout(() => el.payfastMerchantIdInput?.focus(), 20);
    }
  }

  function closePayfastConnectDialog() {
    if (!el.payfastConnectDialog) return;
    if (el.payfastConnectDialog.open && typeof el.payfastConnectDialog.close === "function") {
      el.payfastConnectDialog.close();
    }
    setPayfastConnectError("");
  }

  async function onPayfastConnectSubmit() {
    if (!isChurchAdminRole(state.profile?.role)) {
      toast("Only church admins can connect PayFast.", "error");
      return;
    }

    const merchantId = String(el.payfastMerchantIdInput?.value || "").trim();
    const merchantKey = String(el.payfastMerchantKeyInput?.value || "").trim();
    const passphrase = String(el.payfastPassphraseInput?.value || "").trim();

    if (!merchantId || !merchantKey) {
      setPayfastConnectError("Merchant ID and Merchant Key are required.");
      return;
    }

    setPayfastConnectError("");
    setBusy(el.payfastConnectSubmitBtn, true, "Testing...", "Test & Connect");
    try {
      await apiRequest("/api/churches/payfast/connect", {
        method: "POST",
        body: { merchantId, merchantKey, passphrase },
      });
      await loadPayfastStatus();
      toast("PayFast connected for this church.", "success");
      closePayfastConnectDialog();
    } catch (err) {
      const message = err?.message || "Could not connect PayFast.";
      setPayfastConnectError(message);
      toast(message, "error");
    } finally {
      setBusy(el.payfastConnectSubmitBtn, false, "Testing...", "Test & Connect");
    }
  }

  async function onPayfastDisconnect() {
    if (!isChurchAdminRole(state.profile?.role)) {
      toast("Only church admins can disconnect PayFast.", "error");
      return;
    }
    const confirmed = await confirmAction({
      title: "Disconnect PayFast",
      body: "Disconnect this church PayFast account? New payment checkouts will stop until reconnected.",
      okLabel: "Disconnect",
      okVariant: "danger",
    });
    if (!confirmed) return;

    setPayfastConnectError("");
    setBusy(el.payfastDisconnectBtn, true, "Disconnecting...", "Disconnect");
    try {
      await apiRequest("/api/churches/payfast/disconnect", { method: "POST", body: {} });
      await loadPayfastStatus();
      toast("PayFast disconnected.", "info");
      closePayfastConnectDialog();
    } catch (err) {
      const message = err?.message || "Could not disconnect PayFast.";
      setPayfastConnectError(message);
      toast(message, "error");
    } finally {
      setBusy(el.payfastDisconnectBtn, false, "Disconnecting...", "Disconnect");
    }
  }

  async function loadPortalSettings() {
    try {
      const data = await apiRequest("/api/admin/portal-settings");
      setAllowedTabs(data?.allowedTabs || []);
      state.portalSettings = data?.settings || { accountantTabs: [] };

      applyTabVisibility();

      const canEdit = isChurchAdminRole(state.profile?.role);
      showAccountantAccessCard(canEdit);
      showPayfastSetupCard(canEdit);
      if (canEdit) {
        setAccountantTabsInUi(state.portalSettings?.accountantTabs || []);
      }
    } catch (err) {
      // Fallback for early bootstrap or older deploys.
      setAllowedTabs(ADMIN_PORTAL_TABS);
      state.portalSettings = { accountantTabs: [] };
      applyTabVisibility();

      // If church isn't linked yet, hide the config card to avoid confusion.
      const msg = String(err?.message || "");
      if (msg.toLowerCase().includes("join a church")) {
        showAccountantAccessCard(false);
        showPayfastSetupCard(false);
      }
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
      if (isChurchAdminRole(state.profile?.role)) {
        renderEmpty(el.fundsBody, 5, "No funds created yet.", "Create first fund", "focusCreateFundBtn");
        window.setTimeout(() => {
          const trigger = $("focusCreateFundBtn");
          if (trigger) trigger.addEventListener("click", () => el.fundNameInput.focus(), { once: true });
        }, 0);
      } else {
        renderEmpty(el.fundsBody, 5, "No funds created yet.");
      }
      updateFundSelects();
      return;
    }

    const canManageFunds = isChurchAdminRole(state.profile?.role);
    el.fundsBody.innerHTML = state.funds
      .map((fund) => {
        const active = !!fund.active;
        const status = active ? "active" : "inactive";
        const created = fund.createdAt || fund.created_at || "";
        const actions = canManageFunds
          ? `
              <button class="btn ghost" data-action="rename" data-id="${escapeHtml(fund.id)}" type="button">Rename</button>
              <button class="btn ghost" data-action="toggle" data-id="${escapeHtml(fund.id)}" type="button">${active ? "Disable" : "Enable"}</button>
            `
          : `<span class="meta-line">Read-only</span>`;
        return `
          <tr>
            <td>${escapeHtml(fund.name || "-")}</td>
            <td>${escapeHtml(fund.code || "-")}</td>
            <td><span class="badge ${status}">${active ? "Active" : "Inactive"}</span></td>
            <td>${escapeHtml(formatDate(created))}</td>
            <td class="actions-cell">
              ${actions}
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
      const status = parseTransactionStatus(row);
      if (!(status === "PAID" || status === "CONFIRMED")) return;

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
    const rows = state.dashboardTransactions
      .filter((tx) => {
        const status = parseTransactionStatus(tx);
        return status === "PAID" || status === "CONFIRMED";
      })
      .slice(0, 10);
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
      const status = parseTransactionStatus(tx);
      if (!(status === "PAID" || status === "CONFIRMED")) return;
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
    renderSkeletonRows(el.transactionsBody, 9, 7);

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
      renderEmpty(el.transactionsBody, 9, "No transactions match your filters.", "Clear filters", "clearTxFiltersBtn");
      window.setTimeout(() => {
        const node = $("clearTxFiltersBtn");
        if (node) node.addEventListener("click", resetTransactionFilters, { once: true });
      }, 0);
    } else {
      el.transactionsBody.innerHTML = rows
        .map((tx) => {
          const status = parseTransactionStatus(tx);
          const method = formatMethodDisplay(tx);
          const channel = formatChannelDisplay(tx);
          const actions = renderTransactionActions(tx);
          return `
            <tr>
              <td>${escapeHtml(tx.reference || "-")}</td>
              <td>${escapeHtml(formatMoney(tx.amount))}</td>
              <td>${escapeHtml(tx.fundName || tx.fundCode || "-")}</td>
              <td>${escapeHtml(formatPayerDisplay(tx))}</td>
              <td>${escapeHtml(method)}</td>
              <td>${escapeHtml(channel)}</td>
              <td><span class="badge ${statusBadgeClass(status)}">${escapeHtml(status)}</span></td>
              <td>${escapeHtml(formatDate(tx.createdAt || tx.created_at))}</td>
              <td>${actions}</td>
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

  function isoTodayLocal() {
    const d = new Date();
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}`;
  }

  function isoStartOfMonthLocal() {
    const d = new Date();
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    return `${yyyy}-${mm}-01`;
  }

  function readStatementFiltersFromInputs() {
    state.statementFilters.from = el.statementFromInput ? (el.statementFromInput.value || "") : "";
    state.statementFilters.to = el.statementToInput ? (el.statementToInput.value || "") : "";
    state.statementFilters.allStatuses = !!(el.statementAllStatusesInput && el.statementAllStatusesInput.checked);
  }

  function statementQueryFromFilters(filters) {
    const q = {};
    if (filters?.from) q.from = filters.from;
    if (filters?.to) q.to = filters.to;
    if (filters?.allStatuses) q.allStatuses = "1";
    return q;
  }

  function renderStatementTables(payload) {
    const byFund = payload?.breakdown?.byFund || [];
    const byMethod = payload?.breakdown?.byMethod || [];

    if (el.statementByFundBody) {
      if (!byFund.length) {
        renderEmpty(el.statementByFundBody, 7, "No statement records for this period.");
      } else {
        el.statementByFundBody.innerHTML = byFund
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.fundName || row.fundCode || "-")}</td>
              <td>${escapeHtml(formatMoney(row.donationTotal))}</td>
              <td>${escapeHtml(formatMoney(row.feeTotal))}</td>
              <td>${escapeHtml(formatMoney(row.payfastFeeTotal))}</td>
              <td>${escapeHtml(formatMoney(row.netReceivedTotal))}</td>
              <td>${escapeHtml(formatMoney(row.totalCharged))}</td>
              <td>${escapeHtml(String(row.transactionCount || 0))}</td>
            </tr>
          `)
          .join("");
      }
    }

    if (el.statementByMethodBody) {
      if (!byMethod.length) {
        renderEmpty(el.statementByMethodBody, 7, "No statement records for this period.");
      } else {
        el.statementByMethodBody.innerHTML = byMethod
          .map((row) => `
            <tr>
              <td>${escapeHtml(String(row.provider || "-").toUpperCase())}</td>
              <td>${escapeHtml(formatMoney(row.donationTotal))}</td>
              <td>${escapeHtml(formatMoney(row.feeTotal))}</td>
              <td>${escapeHtml(formatMoney(row.payfastFeeTotal))}</td>
              <td>${escapeHtml(formatMoney(row.netReceivedTotal))}</td>
              <td>${escapeHtml(formatMoney(row.totalCharged))}</td>
              <td>${escapeHtml(String(row.transactionCount || 0))}</td>
            </tr>
          `)
          .join("");
      }
    }
  }

  async function loadStatementSummary() {
    if (el.statementMeta) el.statementMeta.textContent = "Loading statement...";
    renderSkeletonRows(el.statementByFundBody, 7, 4);
    renderSkeletonRows(el.statementByMethodBody, 7, 4);

    readStatementFiltersFromInputs();
    const query = statementQueryFromFilters(state.statementFilters);
    const data = await apiRequest("/api/admin/statements/summary" + buildQuery(query));

    const summary = data?.summary || {};
    if (el.statementDonationTotal) el.statementDonationTotal.textContent = formatMoney(summary.donationTotal || 0);
    if (el.statementFeeTotal) el.statementFeeTotal.textContent = formatMoney(summary.feeTotal || 0);
    if (el.statementPayfastFeeTotal) el.statementPayfastFeeTotal.textContent = formatMoney(summary.payfastFeeTotal || 0);
    if (el.statementNetReceivedTotal) el.statementNetReceivedTotal.textContent = formatMoney(summary.netReceivedTotal || 0);
    if (el.statementTotalCharged) el.statementTotalCharged.textContent = formatMoney(summary.totalCharged || 0);
    if (el.statementTxCount) el.statementTxCount.textContent = String(summary.transactionCount || 0);

    const meta = data?.meta || {};
    const fromLabel = meta.from || state.statementFilters.from || "-";
    const toLabel = meta.to || state.statementFilters.to || "-";
    const statusHint = meta.defaultStatuses
      ? `Finalized statuses: ${meta.defaultStatuses.join(", ")}`
      : (state.statementFilters.allStatuses ? "All statuses included" : "Status filter applied");
    if (el.statementMeta) el.statementMeta.textContent = `Period: ${fromLabel} to ${toLabel}. ${statusHint}.`;

    renderStatementTables(data);
  }

  async function downloadStatementCsv() {
    readStatementFiltersFromInputs();
    const query = statementQueryFromFilters(state.statementFilters);
    const res = await fetch("/api/admin/statements/export" + buildQuery(query), {
      headers: { Authorization: `Bearer ${state.token}` },
    });

    const csv = await res.text();
    if (!res.ok) {
      const json = parseJsonSafe(csv);
      throw new Error((json && json.error) || "Statement export failed");
    }

    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    const from = state.statementFilters.from || "from";
    const to = state.statementFilters.to || "to";
    a.download = `statement-${from}-to-${to}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }

  async function openStatementPrintView() {
    readStatementFiltersFromInputs();
    const query = statementQueryFromFilters(state.statementFilters);

    const res = await fetch("/api/admin/statements/print" + buildQuery(query), {
      headers: { Authorization: `Bearer ${state.token}`, Accept: "text/html" },
    });
    const html = await res.text();
    if (!res.ok) {
      const json = parseJsonSafe(html);
      throw new Error((json && json.error) || "Could not open printable statement");
    }

    const blob = new Blob([html], { type: "text/html;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const popup = window.open(url, "_blank", "noopener,noreferrer");
    if (!popup) {
      URL.revokeObjectURL(url);
      throw new Error("Popup blocked. Allow popups to open the printable statement.");
    }
    window.setTimeout(() => URL.revokeObjectURL(url), 60 * 1000);
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

    if (!isChurchAdminRole(state.profile?.role)) {
      toast("Read-only access. Ask an admin to manage funds.", "info");
      return;
    }

    const action = button.getAttribute("data-action");
    const fundId = button.getAttribute("data-id");
    if (!action || !fundId) return;

    const fund = state.funds.find((f) => f.id === fundId);
    if (!fund) return;

    try {
      if (action === "rename") {
        const newName = await promptAction({
          title: "Rename fund",
          body: `Enter a new name for ${fund.name || "this fund"}.`,
          label: "Fund name",
          placeholder: "e.g. Building Fund",
          value: fund.name || "",
          okLabel: "Save",
          cancelLabel: "Cancel",
          okVariant: "primary",
          inputType: "text",
        });
        if (newName === null) return;
        const trimmed = String(newName || "").trim();
        if (!trimmed) return;
        await patchFund(fundId, { name: trimmed });
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

  async function confirmCashGiving(paymentIntentId) {
    const id = String(paymentIntentId || "").trim();
    if (!id) return;

    const confirmed = await confirmAction({
      title: "Confirm cash record",
      body: "Confirm this cash giving record? This marks it as CONFIRMED for statements and reconciliation.",
      okLabel: "Confirm",
    });
    if (!confirmed) return;

    await apiRequest(`/api/admin/cash-givings/${encodeURIComponent(id)}/confirm`, { method: "POST", body: {} });
    toast("Cash record confirmed.", "success");
    await Promise.all([loadTotals(), loadDashboardTransactions(), loadTransactions()]);
  }

  async function rejectCashGiving(paymentIntentId) {
    const id = String(paymentIntentId || "").trim();
    if (!id) return;

    const note = await promptAction({
      title: "Reject cash record",
      body: "Add a short reason. This will be shown to the member.",
      label: "Reason",
      placeholder: "e.g. Amount doesn't match, missing proof, wrong fund…",
      value: "",
      okLabel: "Reject",
      cancelLabel: "Cancel",
      okVariant: "danger",
      inputType: "text",
    });
    if (note === null) return;
    const trimmed = String(note || "").trim();
    if (!trimmed) {
      toast("A reason is required to reject.", "error");
      return;
    }

    const confirmed = await confirmAction({
      title: "Reject cash record",
      body: "Reject this cash record? It will be marked as REJECTED.",
      okLabel: "Reject",
    });
    if (!confirmed) return;

    await apiRequest(`/api/admin/cash-givings/${encodeURIComponent(id)}/reject`, { method: "POST", body: { note: trimmed } });
    toast("Cash record rejected.", "success");
    await Promise.all([loadTotals(), loadDashboardTransactions(), loadTransactions()]);
  }

  async function onTransactionsAction(event) {
    const button = event.target.closest("button[data-action]");
    if (!button) return;

    const action = String(button.getAttribute("data-action") || "").trim();
    const id = String(button.getAttribute("data-id") || "").trim();
    if (!action || !id) return;

    try {
      if (action === "cash-confirm") {
        await confirmCashGiving(id);
      }
      if (action === "cash-reject") {
        await rejectCashGiving(id);
      }
    } catch (err) {
      toast(err?.message || "Action failed", "error");
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
    renderSkeletonRows(el.membersBody, 5, 6);

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
        renderEmpty(el.membersBody, 6, "No members found for current filters.");
      } else {
        const canEditRoles = isChurchAdminRole(state.profile?.role);
        el.membersBody.innerHTML = rows
          .map((member) => {
            const roleLower = String(member.role || "member").toLowerCase();
            const roleClass = roleLower === "admin" ? "paid" : roleLower === "accountant" ? "pending" : "pending";
            const dateOfBirth =
              typeof member.dateOfBirth === "string"
                ? member.dateOfBirth
                : (typeof member.date_of_birth === "string" ? member.date_of_birth : "");
            const roleCell = canEditRoles
              ? `
                <select class="role-inline-select" data-member-id="${escapeHtml(member.id)}" data-member-name="${escapeHtml(member.fullName || member.email || member.phone || "member")}" data-current-role="${escapeHtml(roleLower)}" ${member.id === state.profile?.id ? "disabled" : ""}>
                  <option value="admin" ${roleLower === "admin" ? "selected" : ""}>Admin</option>
                  <option value="accountant" ${roleLower === "accountant" ? "selected" : ""}>Accountant</option>
                  <option value="member" ${roleLower === "member" ? "selected" : ""}>Member</option>
                </select>
              `
              : `<span class="badge ${roleClass}">${escapeHtml(roleLower)}</span>`;
            return `
              <tr>
                <td>${escapeHtml(member.fullName || "-")}</td>
                <td>${escapeHtml(member.phone || "-")}</td>
                <td>${escapeHtml(member.email || "-")}</td>
                <td>${escapeHtml(dateOfBirth || "-")}</td>
                <td>${roleCell}</td>
                <td>${escapeHtml(formatDate(member.createdAt || member.created_at))}</td>
              </tr>
            `;
          })
          .join("");

        if (canEditRoles) {
          $$(".role-inline-select").forEach((select) => {
            select.addEventListener(
              "change",
              async (event) => {
                const node = event.target;
                const memberId = node.getAttribute("data-member-id") || "";
                const memberName = node.getAttribute("data-member-name") || "member";
                const previousRole = node.getAttribute("data-current-role") || "";
                const nextRole = String(node.value || "").toLowerCase();
                if (!memberId || !nextRole) return;

                const confirmed = await confirmAction({
                  title: "Change member role",
                  body: `Set ${memberName} to ${nextRole.toUpperCase()}?`,
                  okLabel: "Change role",
                });
                if (!confirmed) {
                  node.value = previousRole || "member";
                  return;
                }

                node.disabled = true;
                try {
                  await apiRequest(`/api/admin/members/${encodeURIComponent(memberId)}/role`, {
                    method: "PATCH",
                    body: { role: nextRole },
                  });
                  node.setAttribute("data-current-role", nextRole);
                  toast(`Role updated for ${memberName}.`, "success");
                } catch (err) {
                  node.value = previousRole || "member";
                  toast(err.message || "Could not update role.", "error");
                } finally {
                  node.disabled = false;
                }
              },
              { passive: true }
            );
          });
        }
      }

      el.memberMeta.textContent = `Showing ${rows.length} of ${Number(meta.count || rows.length)} members`;
    } catch (err) {
      if (err.status === 404) {
        renderEmpty(el.membersBody, 6, "Members endpoint is not enabled yet on this deploy.");
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
    await Promise.all([loadProfile(), loadChurch(), loadFunds()]);
    await loadPortalSettings();
    if (isChurchAdminRole(state.profile?.role)) {
      await loadPayfastStatus();
    }

    const tasks = [];
    if (isTabAllowed("dashboard")) tasks.push(loadDashboard());
    if (isTabAllowed("transactions")) tasks.push(loadTransactions());
    if (isTabAllowed("members")) tasks.push(loadMembers());
    await Promise.all(tasks);
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
    state.statementFilters = { from: "", to: "", allStatuses: false };
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
    await loadPortalSettings();
    if (isChurchAdminRole(state.profile?.role)) {
      await loadPayfastStatus();
    }

    const tasks = [];
    if (isTabAllowed("dashboard")) tasks.push(loadDashboard());
    if (isTabAllowed("transactions")) tasks.push(loadTransactions());
    if (isTabAllowed("members")) tasks.push(loadMembers());
    await Promise.all(tasks);
  }

  async function onLoginSubmit(event) {
    event.preventDefault();
    showAuthError("");
    const verifyingTwoFactor = !!state.authTwoFactor;
    setBusy(
      el.loginBtn,
      true,
      verifyingTwoFactor ? "Verifying..." : "Signing in...",
      verifyingTwoFactor ? "Verify code" : "Sign in"
    );

    try {
      let data = null;

      if (verifyingTwoFactor) {
        const challengeId = state.authTwoFactor?.challengeId || "";
        const code = String(el.twoFactorCodeInput?.value || "").trim();
        if (!challengeId) throw new Error("Two-factor challenge is missing. Please sign in again.");
        if (!code) throw new Error("Two-factor code is required.");
        data = await verifyAdminTwoFactor(challengeId, code);
      } else {
        const identifier = (el.identifierInput.value || "").trim();
        const password = el.passwordInput.value || "";
        if (!identifier || !password) throw new Error("Phone/email and password are required");

        data = await loginAdmin(identifier, password);
        if (data && data.requiresTwoFactor) {
          setAuthStep(data.twoFactor || {});
          showAuthError(`Verification code sent to ${data?.twoFactor?.emailMasked || "your email"}.`);
          toast("Enter your 6-digit sign-in code to continue.", "info");
          return;
        }
      }

      if (!data?.token) throw new Error("Sign-in failed");
      setToken(data.token);
      setAuthStep(null);
      showAuth(false);
      startInactivityWatch();
      showLoading(true);

      await bootstrapPortal();
      switchTab(firstAllowedTab());

      toast("Welcome back.", "success");
    } catch (err) {
      setToken("");
      showAuthError(err.message || "Sign-in failed");
      toast(err.message || "Sign-in failed", "error");
    } finally {
      setBusy(
        el.loginBtn,
        false,
        state.authTwoFactor ? "Verifying..." : "Signing in...",
        state.authTwoFactor ? "Verify code" : "Sign in"
      );
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
    if (el.twoFactorBackBtn) {
      el.twoFactorBackBtn.addEventListener("click", () => {
        setAuthStep(null);
        showAuthError("");
      });
    }
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
      const preference = currentThemePreference();
      if (preference === "system") {
        const next = currentTheme() === "dark" ? "light" : "dark";
        applyTheme(next);
        return;
      }
      applyTheme("system");
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

    if (el.transactionsBody) {
      el.transactionsBody.addEventListener("click", onTransactionsAction);
    }

    if (el.loadStatementBtn) {
      el.loadStatementBtn.addEventListener("click", async () => {
        try {
          setBusy(el.loadStatementBtn, true, "Loading...", "Load statement");
          await loadStatementSummary();
        } catch (err) {
          toast(err.message || "Could not load statement", "error");
        } finally {
          setBusy(el.loadStatementBtn, false, "Loading...", "Load statement");
        }
      });
    }

    if (el.downloadStatementBtn) {
      el.downloadStatementBtn.addEventListener("click", async () => {
        try {
          setBusy(el.downloadStatementBtn, true, "Preparing...", "Download CSV");
          await downloadStatementCsv();
          toast("Statement download started.", "success");
        } catch (err) {
          toast(err.message || "Could not download statement", "error");
        } finally {
          setBusy(el.downloadStatementBtn, false, "Preparing...", "Download CSV");
        }
      });
    }

    if (el.openStatementPrintBtn) {
      el.openStatementPrintBtn.addEventListener("click", async () => {
        try {
          setBusy(el.openStatementPrintBtn, true, "Opening...", "Open printable (PDF)");
          await openStatementPrintView();
          toast("Printable statement opened. Use Print to save as PDF.", "success", 4200);
        } catch (err) {
          toast(err.message || "Could not open printable statement", "error");
        } finally {
          setBusy(el.openStatementPrintBtn, false, "Opening...", "Open printable (PDF)");
        }
      });
    }

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

    if (el.saveAccountantAccessBtn) {
      el.saveAccountantAccessBtn.addEventListener("click", async () => {
        if (!isChurchAdminRole(state.profile?.role)) {
          toast("Only admins can change accountant access.", "error");
          return;
        }

        const selectedTabs = normalizeAllowedTabs(readAccountantTabsFromUi()).filter((tab) => tab !== "settings");
        if (!selectedTabs.length) {
          toast("Select at least one tab for accountant access.", "error");
          return;
        }

        try {
          setBusy(el.saveAccountantAccessBtn, true, "Saving...", "Save access");
          const data = await apiRequest("/api/admin/portal-settings", {
            method: "PATCH",
            body: { accountantTabs: selectedTabs },
          });
          state.portalSettings.accountantTabs = (data && data.settings && data.settings.accountantTabs) || selectedTabs;
          setAccountantTabsInUi(state.portalSettings.accountantTabs);
          toast("Accountant access saved.", "success");
        } catch (err) {
          toast(err.message || "Could not save accountant access.", "error");
        } finally {
          setBusy(el.saveAccountantAccessBtn, false, "Saving...", "Save access");
        }
      });
    }

    if (el.openPayfastConnectBtn) {
      el.openPayfastConnectBtn.addEventListener("click", () => {
        if (!isChurchAdminRole(state.profile?.role)) {
          toast("Only church admins can connect PayFast.", "error");
          return;
        }
        openPayfastConnectDialog();
      });
    }

    if (el.refreshPayfastStatusBtn) {
      el.refreshPayfastStatusBtn.addEventListener("click", async () => {
        try {
          setBusy(el.refreshPayfastStatusBtn, true, "Refreshing...", "Refresh status");
          await loadPayfastStatus();
          toast("PayFast status refreshed.", "info", 1600);
        } catch (err) {
          toast(err.message || "Could not refresh PayFast status.", "error");
        } finally {
          setBusy(el.refreshPayfastStatusBtn, false, "Refreshing...", "Refresh status");
        }
      });
    }

    if (el.payfastModalCloseBtn) {
      el.payfastModalCloseBtn.addEventListener("click", () => closePayfastConnectDialog());
    }
    if (el.payfastConnectSubmitBtn) {
      el.payfastConnectSubmitBtn.addEventListener("click", () => {
        onPayfastConnectSubmit().catch((err) => toast(err.message || "Could not connect PayFast.", "error"));
      });
    }
    if (el.payfastDisconnectBtn) {
      el.payfastDisconnectBtn.addEventListener("click", () => {
        onPayfastDisconnect().catch((err) => toast(err.message || "Could not disconnect PayFast.", "error"));
      });
    }
  }

  async function init() {
    installBrandLogoFallback();
    bindEvents();
    setSidebarOpen(false);

    const preferredTheme = window.localStorage.getItem(THEME_KEY) || "system";
    applyTheme(preferredTheme);
    startSystemThemeSync();

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
