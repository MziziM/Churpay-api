(function () {
  "use strict";

  const TOKEN_KEY = "churpay.super.token";
  const THEME_KEY = "churpay.admin.theme";
  const LAST_ACTIVITY_KEY = "churpay.super.lastActivityAt";
  const INACTIVITY_TIMEOUT_MS = 15 * 60 * 1000;
  const INACTIVITY_WARNING_BEFORE_MS = 60 * 1000;
  const ACTIVITY_EVENTS = ["pointerdown", "click", "keydown", "touchstart", "input", "wheel"];
  const SYSTEM_THEME_QUERY = "(prefers-color-scheme: dark)";

  const TAB_PATHS = {
    dashboard: "/super/dashboard",
    churches: "/super/churches",
    onboarding: "/super/onboarding",
    jobs: "/super/jobs",
    transactions: "/super/transactions",
    funds: "/super/funds",
    members: "/super/members",
    settings: "/super/settings",
    audit: "/super/audit-logs",
  };

  const TAB_TITLES = {
    dashboard: "Dashboard",
    churches: "Churches",
    onboarding: "Onboarding",
    jobs: "Jobs",
    transactions: "Transactions",
    funds: "Funds",
    members: "Members",
    settings: "Settings",
    audit: "Audit Logs",
  };

  const ACCOUNTANT_CONFIGURABLE_TABS = ["dashboard", "transactions", "statements", "funds", "qr", "members"];
  const ACCOUNTANT_DEFAULT_TABS = ["dashboard", "transactions", "statements"];

  const state = {
    token: "",
    profile: null,
    currentTab: "dashboard",
    selectedChurchId: "",
    selectedChurchDetail: null,
    churchBankDialogChurchId: "",
    churchBankDialogChurchName: "",
    churchBankAccountsDraft: [],
    churchDialogMode: "create",
    churchDialogId: "",
    churches: [],
    churchRows: [],
    memberRows: [],
    memberActionsMemberId: "",
    memberEditMemberId: "",
    memberEditOriginal: null,
    onboardingRows: [],
    selectedOnboardingDetail: null,
    onboardingEditDraft: null,
    jobsRows: [],
    selectedOnboardingId: "",
    selectedJobId: "",
    previewObjectUrl: "",
    funds: [],
    legalDocuments: [],
    legalDocEditingKey: "",
    loaded: {
      dashboard: false,
      churches: false,
      onboarding: false,
      jobs: false,
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
    onboardingFilters: {
      status: "",
      search: "",
      limit: 50,
      offset: 0,
    },
    jobsFilters: {
      status: "",
      churchId: "",
      search: "",
      limit: 50,
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
    onboardingPendingBadge: $("onboardingPendingBadge"),
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
    statTotalProcessed: $("statTotalProcessed"),
    statTotalPayfastFees: $("statTotalPayfastFees"),
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
    churchBankAccountsMeta: $("churchBankAccountsMeta"),
    churchBankAccountsBody: $("churchBankAccountsBody"),
    editChurchBankAccountsBtn: $("editChurchBankAccountsBtn"),
    churchAccountantAccessSection: $("churchAccountantAccessSection"),
    churchAccTabDashboard: $("churchAccTabDashboard"),
    churchAccTabTransactions: $("churchAccTabTransactions"),
    churchAccTabStatements: $("churchAccTabStatements"),
    churchAccTabFunds: $("churchAccTabFunds"),
    churchAccTabQr: $("churchAccTabQr"),
    churchAccTabMembers: $("churchAccTabMembers"),
    saveChurchAccountantAccessBtn: $("saveChurchAccountantAccessBtn"),
    churchAccountantAccessMeta: $("churchAccountantAccessMeta"),

    onboardingStatusSelect: $("onboardingStatusSelect"),
    onboardingSearchInput: $("onboardingSearchInput"),
    onboardingLimitInput: $("onboardingLimitInput"),
    applyOnboardingFiltersBtn: $("applyOnboardingFiltersBtn"),
    onboardingMeta: $("onboardingMeta"),
    onboardingBody: $("onboardingBody"),
    onboardingDetailsMeta: $("onboardingDetailsMeta"),
    onboardingDetailsChurch: $("onboardingDetailsChurch"),
    onboardingDetailsAdmin: $("onboardingDetailsAdmin"),
    onboardingDownloadCipcBtn: $("onboardingDownloadCipcBtn"),
    onboardingDownloadBankBtn: $("onboardingDownloadBankBtn"),
    onboardingEditBtn: $("onboardingEditBtn"),
    onboardingReplaceCipcBtn: $("onboardingReplaceCipcBtn"),
    onboardingReplaceBankBtn: $("onboardingReplaceBankBtn"),
    onboardingApproveBtn: $("onboardingApproveBtn"),
    onboardingRejectBtn: $("onboardingRejectBtn"),
    onboardingDeleteBtn: $("onboardingDeleteBtn"),
    onboardingActionMeta: $("onboardingActionMeta"),
    onboardingReviewNoteInput: $("onboardingReviewNoteInput"),
    documentPreviewDialog: $("documentPreviewDialog"),
    documentPreviewTitle: $("documentPreviewTitle"),
    documentPreviewMeta: $("documentPreviewMeta"),
    documentPreviewContainer: $("documentPreviewContainer"),
    documentPreviewOpenNewTab: $("documentPreviewOpenNewTab"),
    documentPreviewDownloadLink: $("documentPreviewDownloadLink"),

    openCreateJobBtn: $("openCreateJobBtn"),
    jobsStatusSelect: $("jobsStatusSelect"),
    jobsChurchSelect: $("jobsChurchSelect"),
    jobsSearchInput: $("jobsSearchInput"),
    jobsLimitInput: $("jobsLimitInput"),
    applyJobsFiltersBtn: $("applyJobsFiltersBtn"),
    jobsMeta: $("jobsMeta"),
    jobsBody: $("jobsBody"),
    jobForm: $("jobForm"),
    jobFormHeading: $("jobFormHeading"),
    jobIdInput: $("jobIdInput"),
    jobTitleInput: $("jobTitleInput"),
    jobChurchSelect: $("jobChurchSelect"),
    jobStatusSelect: $("jobStatusSelect"),
    jobEmploymentTypeSelect: $("jobEmploymentTypeSelect"),
    jobLocationInput: $("jobLocationInput"),
    jobDepartmentInput: $("jobDepartmentInput"),
    jobSummaryInput: $("jobSummaryInput"),
    jobDescriptionInput: $("jobDescriptionInput"),
    jobRequirementsInput: $("jobRequirementsInput"),
    jobApplicationUrlInput: $("jobApplicationUrlInput"),
    jobApplicationEmailInput: $("jobApplicationEmailInput"),
    jobExpiresAtInput: $("jobExpiresAtInput"),
    saveJobBtn: $("saveJobBtn"),
    resetJobFormBtn: $("resetJobFormBtn"),
    deleteJobBtn: $("deleteJobBtn"),
    jobFormMeta: $("jobFormMeta"),

    churchDialog: $("churchDialog"),
    churchDialogForm: $("churchDialogForm"),
    churchDialogTitle: $("churchDialogTitle"),
    churchDialogName: $("churchDialogName"),
    churchDialogJoinCode: $("churchDialogJoinCode"),
    churchDialogActive: $("churchDialogActive"),
    churchDialogSaveBtn: $("churchDialogSaveBtn"),

    churchBankDialog: $("churchBankDialog"),
    churchBankDialogForm: $("churchBankDialogForm"),
    churchBankDialogTitle: $("churchBankDialogTitle"),
    churchBankDialogMeta: $("churchBankDialogMeta"),
    churchBankAccountsList: $("churchBankAccountsList"),
    churchBankAddAccountBtn: $("churchBankAddAccountBtn"),
    churchBankSaveBtn: $("churchBankSaveBtn"),

    onboardingEditDialog: $("onboardingEditDialog"),
    onboardingEditDialogForm: $("onboardingEditDialogForm"),
    onboardingEditDialogTitle: $("onboardingEditDialogTitle"),
    onboardingEditDialogMeta: $("onboardingEditDialogMeta"),
    onboardingEditChurchName: $("onboardingEditChurchName"),
    onboardingEditJoinCode: $("onboardingEditJoinCode"),
    onboardingEditAdminName: $("onboardingEditAdminName"),
    onboardingEditAdminPhone: $("onboardingEditAdminPhone"),
    onboardingEditAdminEmail: $("onboardingEditAdminEmail"),
    onboardingBankAccountsList: $("onboardingBankAccountsList"),
    onboardingAddBankAccountBtn: $("onboardingAddBankAccountBtn"),
    onboardingEditSaveBtn: $("onboardingEditSaveBtn"),

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
    memberActionsDialog: $("memberActionsDialog"),
    memberActionsTitle: $("memberActionsTitle"),
    memberActionsMeta: $("memberActionsMeta"),
    memberActionsRoleBadge: $("memberActionsRoleBadge"),
    memberActionsEditBtn: $("memberActionsEditBtn"),
    memberActionsRoleBtn: $("memberActionsRoleBtn"),
    memberActionsDobBtn: $("memberActionsDobBtn"),
    memberActionsResetBtn: $("memberActionsResetBtn"),
    memberActionsDeleteBtn: $("memberActionsDeleteBtn"),
    memberRoleDialog: $("memberRoleDialog"),
    memberRoleDialogForm: $("memberRoleDialogForm"),
    memberRoleDialogTitle: $("memberRoleDialogTitle"),
    memberRoleDialogMeta: $("memberRoleDialogMeta"),
    memberRoleDialogSelect: $("memberRoleDialogSelect"),
    memberRoleDialogSaveBtn: $("memberRoleDialogSaveBtn"),
    memberDobDialog: $("memberDobDialog"),
    memberDobDialogForm: $("memberDobDialogForm"),
    memberDobDialogTitle: $("memberDobDialogTitle"),
    memberDobDialogMeta: $("memberDobDialogMeta"),
    memberDobDialogInput: $("memberDobDialogInput"),
    memberDobDialogSaveBtn: $("memberDobDialogSaveBtn"),
    memberDobDialogClearBtn: $("memberDobDialogClearBtn"),
    memberEditDialog: $("memberEditDialog"),
    memberEditDialogForm: $("memberEditDialogForm"),
    memberEditDialogTitle: $("memberEditDialogTitle"),
    memberEditDialogMeta: $("memberEditDialogMeta"),
    memberEditRoleChip: $("memberEditRoleChip"),
    memberEditFullName: $("memberEditFullName"),
    memberEditPhone: $("memberEditPhone"),
    memberEditEmail: $("memberEditEmail"),
    memberEditChurchJoinCode: $("memberEditChurchJoinCode"),
    memberEditRoleReadOnly: $("memberEditRoleReadOnly"),
    memberEditClearChurchBtn: $("memberEditClearChurchBtn"),
    memberEditSaveBtn: $("memberEditSaveBtn"),

    settingsEnvironment: $("settingsEnvironment"),
    settingsWebhook: $("settingsWebhook"),
    settingsRateLimits: $("settingsRateLimits"),
    settingsMaintenance: $("settingsMaintenance"),
    legalDocsMeta: $("legalDocsMeta"),
    legalDocsBody: $("legalDocsBody"),
    legalDocDialog: $("legalDocDialog"),
    legalDocDialogForm: $("legalDocDialogForm"),
    legalDocDialogTitle: $("legalDocDialogTitle"),
    legalDocDialogMeta: $("legalDocDialogMeta"),
    legalDocKeyInput: $("legalDocKeyInput"),
    legalDocTitleInput: $("legalDocTitleInput"),
    legalDocBodyInput: $("legalDocBodyInput"),
    legalDocDialogSaveBtn: $("legalDocDialogSaveBtn"),

    auditBody: $("auditBody"),
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

  function formatDateOfBirth(value) {
    if (!value) return "-";
    const raw = String(value).trim();
    if (!raw) return "-";
    const match = raw.match(/^(\d{4}-\d{2}-\d{2})/);
    if (match) return match[1];
    const d = new Date(raw);
    if (Number.isNaN(d.getTime())) return raw;
    return d.toISOString().slice(0, 10);
  }

  function formatInactiveFor(secondsValue) {
    const seconds = Number(secondsValue || 0);
    if (!Number.isFinite(seconds) || seconds <= 0) return "Active now";

    const days = seconds / 86400;
    if (days < 1) return "<1 day";
    if (days < 30) return `${Math.floor(days)} day${Math.floor(days) === 1 ? "" : "s"}`;

    const months = days / 30.4375;
    if (months < 12) return `${Math.floor(months)} month${Math.floor(months) === 1 ? "" : "s"}`;

    const years = days / 365.25;
    if (years < 10) return `${years.toFixed(1)} years`;
    return `${Math.floor(years)} years`;
  }

  function inactivityBadgeClass(secondsValue) {
    const seconds = Number(secondsValue || 0);
    if (!Number.isFinite(seconds) || seconds <= 0) return "active";
    const days = seconds / 86400;
    if (days < 365) return "pending";
    return "inactive";
  }

  function formatBytes(value) {
    const bytes = Number(value || 0);
    if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
    if (bytes < 1024) return `${bytes} B`;
    const kb = bytes / 1024;
    if (kb < 1024) return `${kb.toFixed(1)} KB`;
    const mb = kb / 1024;
    if (mb < 1024) return `${mb.toFixed(1)} MB`;
    const gb = mb / 1024;
    return `${gb.toFixed(2)} GB`;
  }

  function ensurePrimaryAccount(accounts) {
    const list = Array.isArray(accounts) ? accounts : [];
    const primaryCount = list.reduce((acc, row) => acc + (row && row.isPrimary ? 1 : 0), 0);
    if (primaryCount === 1) return list;
    if (!list.length) return list;
    // Ensure exactly 1 primary.
    return list.map((row, idx) => ({ ...row, isPrimary: idx === 0 }));
  }

  function cloneBankAccounts(accounts) {
    const list = Array.isArray(accounts) ? accounts : [];
    return ensurePrimaryAccount(
      list.map((row) => ({
        bankName: String(row?.bankName || ""),
        accountName: String(row?.accountName || ""),
        accountNumber: String(row?.accountNumber || ""),
        branchCode: String(row?.branchCode || ""),
        accountType: String(row?.accountType || ""),
        isPrimary: !!row?.isPrimary,
      }))
    );
  }

  function renderBankAccountsEditor(container, accounts, options = {}) {
    if (!container) return;
    const list = Array.isArray(accounts) ? accounts : [];
    const primaryGroup = String(options.primaryGroup || "primaryBankAccount");
    const allowRemove = list.length > 1;

    container.innerHTML = list
      .map((acct, idx) => {
        const primaryLabel = acct?.isPrimary ? `<span class="badge active">Primary</span>` : "";
        const removeBtn = allowRemove
          ? `<button type="button" class="btn danger ghost" data-bank-action="remove" data-bank-index="${idx}">Remove</button>`
          : "";
        return `
          <div class="card" style="padding:14px;box-shadow:none;">
            <div class="card-head" style="gap:10px;align-items:center;justify-content:space-between;">
              <div style="display:flex;align-items:center;gap:10px;">
                <h4 style="margin:0;">Account ${idx + 1}</h4>
                ${primaryLabel}
              </div>
              ${removeBtn}
            </div>
            <div class="form-grid">
              <label class="field">
                <span>Bank name</span>
                <input type="text" data-bank-field="bankName" data-bank-index="${idx}" value="${escapeHtml(acct?.bankName || "")}" placeholder="e.g. Capitec" required />
              </label>
              <label class="field">
                <span>Account name</span>
                <input type="text" data-bank-field="accountName" data-bank-index="${idx}" value="${escapeHtml(acct?.accountName || "")}" placeholder="Account holder name" required />
              </label>
              <label class="field">
                <span>Account number</span>
                <input type="text" inputmode="numeric" data-bank-field="accountNumber" data-bank-index="${idx}" value="${escapeHtml(acct?.accountNumber || "")}" placeholder="Digits only" required />
              </label>
              <label class="field">
                <span>Branch code (optional)</span>
                <input type="text" inputmode="numeric" data-bank-field="branchCode" data-bank-index="${idx}" value="${escapeHtml(acct?.branchCode || "")}" placeholder="e.g. 470010" />
              </label>
              <label class="field">
                <span>Account type (optional)</span>
                <input type="text" data-bank-field="accountType" data-bank-index="${idx}" value="${escapeHtml(acct?.accountType || "")}" placeholder="Cheque / Savings" />
              </label>
              <label class="field field-checkbox">
                <input type="radio" name="${escapeHtml(primaryGroup)}" data-bank-action="primary" data-bank-index="${idx}" ${acct?.isPrimary ? "checked" : ""} />
                <span>Primary</span>
              </label>
            </div>
          </div>
        `;
      })
      .join("");
  }

  function fileToPayload(file, { label = "Document", maxBytes = 10 * 1024 * 1024 } = {}) {
    return new Promise((resolve, reject) => {
      if (!(file instanceof File) || !file.size) return reject(new Error(`Missing required ${label.toLowerCase()}`));
      if (file.size > maxBytes) {
        return reject(new Error(`${label} must be ${(maxBytes / (1024 * 1024)).toFixed(0)}MB or smaller`));
      }

      const reader = new FileReader();
      reader.onload = () => {
        const dataUrl = typeof reader.result === "string" ? reader.result : "";
        const match = dataUrl.match(/^data:([^;]+);base64,(.+)$/i);
        if (!match) return reject(new Error("Invalid document encoding"));
        return resolve({
          filename: file.name,
          mimeType: match[1],
          base64: match[2],
        });
      };
      reader.onerror = () => reject(new Error("Could not read selected file"));
      reader.readAsDataURL(file);
    });
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

  function normalizeAccountantTabs(value) {
    const list = Array.isArray(value) ? value : [];
    const out = [];
    const seen = new Set();

    for (const raw of list) {
      const key = String(raw || "").trim().toLowerCase();
      if (!key) continue;
      if (!ACCOUNTANT_CONFIGURABLE_TABS.includes(key)) continue;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(key);
    }

    return out;
  }

  function setChurchAccountantTabsInUi(value) {
    const normalized = normalizeAccountantTabs(value);
    const effective = normalized.length ? normalized : ACCOUNTANT_DEFAULT_TABS.slice();
    const enabled = new Set(effective);

    if (el.churchAccTabDashboard) el.churchAccTabDashboard.checked = enabled.has("dashboard");
    if (el.churchAccTabTransactions) el.churchAccTabTransactions.checked = enabled.has("transactions");
    if (el.churchAccTabStatements) el.churchAccTabStatements.checked = enabled.has("statements");
    if (el.churchAccTabFunds) el.churchAccTabFunds.checked = enabled.has("funds");
    if (el.churchAccTabQr) el.churchAccTabQr.checked = enabled.has("qr");
    if (el.churchAccTabMembers) el.churchAccTabMembers.checked = enabled.has("members");
  }

  function readChurchAccountantTabsFromUi() {
    const chosen = [];
    if (el.churchAccTabDashboard?.checked) chosen.push("dashboard");
    if (el.churchAccTabTransactions?.checked) chosen.push("transactions");
    if (el.churchAccTabStatements?.checked) chosen.push("statements");
    if (el.churchAccTabFunds?.checked) chosen.push("funds");
    if (el.churchAccTabQr?.checked) chosen.push("qr");
    if (el.churchAccTabMembers?.checked) chosen.push("members");
    return normalizeAccountantTabs(chosen);
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
      performLogout("You were logged out after 15 minutes of inactivity.");
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
        performLogout("You were logged out after 15 minutes of inactivity.");
      }, warningDelay);
    }

    inactivityTimerId = window.setTimeout(() => {
      performLogout("You were logged out after 15 minutes of inactivity.");
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
      clearInactivityWarningTimer();
      return;
    }
    const elapsed = Date.now() - lastActivityAt;
    if (elapsed >= INACTIVITY_TIMEOUT_MS) {
      performLogout("You were logged out after 15 minutes of inactivity.");
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
        performLogout("You were logged out after 15 minutes of inactivity.");
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
    if (el.themeToggleBtn) {
      if (preference === "system") {
        el.themeToggleBtn.textContent = `Device mode: ${resolvedTheme === "dark" ? "Dark" : "Light"}`;
        return;
      }
      el.themeToggleBtn.textContent = `Locked: ${resolvedTheme === "dark" ? "Dark" : "Light"} (use device mode)`;
    }
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

  function installLogoFallback() {
    const images = Array.from(document.querySelectorAll("img"));
    images.forEach((img) => {
      const src = img.getAttribute("src") || "";
      if (!src.includes("/assets/brand/")) return;
      const fallback = [
        src,
        "/assets/brand/churpay-logo.svg",
        "/assets/brand/churpay-logo-500x250.png",
        "/assets/brand/churpay-logo.png",
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

  function revokePreviewObjectUrl() {
    if (!state.previewObjectUrl) return;
    try {
      URL.revokeObjectURL(state.previewObjectUrl);
    } catch (_err) {
      // ignore
    }
    state.previewObjectUrl = "";
  }

  function closeDocumentPreviewDialog() {
    revokePreviewObjectUrl();
    if (el.documentPreviewContainer) el.documentPreviewContainer.innerHTML = "";
    if (el.documentPreviewMeta) el.documentPreviewMeta.textContent = "";
    if (el.documentPreviewOpenNewTab) el.documentPreviewOpenNewTab.removeAttribute("href");
    if (el.documentPreviewDownloadLink) el.documentPreviewDownloadLink.removeAttribute("href");
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
    if (clean === "/super/onboarding") return { tab: "onboarding", churchId: "" };
    if (clean === "/super/jobs") return { tab: "jobs", churchId: "" };
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
    await populateChurchSelectsFromState();
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
    if (el.statTotalProcessed) el.statTotalProcessed.textContent = formatMoney(summary.totalProcessed);
    if (el.statTotalPayfastFees) el.statTotalPayfastFees.textContent = formatMoney(summary.totalPayfastFees);
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
    function fillChurchSelect(select, firstOptionLabel) {
      if (!select) return;
      const current = select.value;
      select.innerHTML = `<option value="">${escapeHtml(firstOptionLabel)}</option>`;
      state.churches.forEach((church) => {
        const option = document.createElement("option");
        option.value = church.id;
        option.textContent = church.name;
        select.appendChild(option);
      });
      if (current) select.value = current;
    }

    const churchSelectTargets = [
      el.dashChurchSelect,
      el.txChurchSelect,
      el.fundChurchSelect,
      el.memberChurchSelect,
      el.jobsChurchSelect,
    ];
    churchSelectTargets.forEach((select) => fillChurchSelect(select, "All churches"));
    fillChurchSelect(el.jobChurchSelect, "All churches / platform-wide");
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
        const deleteDisabled = active ? 'disabled title="Disable this church before deleting"' : "";
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
              <button class="btn danger" type="button" data-action="delete" data-id="${escapeHtml(church.id)}" ${deleteDisabled}>Delete</button>
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
      renderEmpty(el.churchBankAccountsBody, 6, "No church selected.");
      if (el.churchBankAccountsMeta) el.churchBankAccountsMeta.textContent = "Select a church to view bank accounts.";
      if (el.editChurchBankAccountsBtn) el.editChurchBankAccountsBtn.disabled = true;
      el.churchAccountantAccessSection?.classList.add("hidden");
      if (el.churchAccountantAccessMeta) el.churchAccountantAccessMeta.textContent = "Select a church to configure access.";
      return;
    }

    renderSkeletonRows(el.churchDetailFundsBody, 4, 3);
    renderSkeletonRows(el.churchDetailAdminsBody, 4, 3);
    renderSkeletonRows(el.churchDetailTransactionsBody, 5, 4);
    renderSkeletonRows(el.churchBankAccountsBody, 6, 2);

    const data = await apiRequest(`/api/super/churches/${encodeURIComponent(churchId)}`);
    state.selectedChurchDetail = data || null;
    const church = data.church || selectedChurchById(churchId);

    if (church) {
      el.churchDetailsMeta.textContent = `${church.name} | Join code: ${church.joinCode || "-"} | Status: ${church.active ? "Active" : "Disabled"}`;
    } else {
      el.churchDetailsMeta.textContent = "Church details loaded.";
    }
    if (el.editChurchBankAccountsBtn) el.editChurchBankAccountsBtn.disabled = false;

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

    const bankAccounts = Array.isArray(data.bankAccounts) ? data.bankAccounts : [];
    if (el.churchBankAccountsMeta) {
      el.churchBankAccountsMeta.textContent = bankAccounts.length
        ? `${bankAccounts.length} bank account${bankAccounts.length === 1 ? "" : "s"} on file`
        : "No bank accounts saved yet.";
    }
    if (!bankAccounts.length) {
      renderEmpty(el.churchBankAccountsBody, 6, "No bank accounts found for this church.");
    } else {
      el.churchBankAccountsBody.innerHTML = bankAccounts
        .map((acct) => {
          return `
            <tr>
              <td>${escapeHtml(acct.bankName || "-")}</td>
              <td>${escapeHtml(acct.accountName || "-")}</td>
              <td>${escapeHtml(acct.accountNumber || "-")}</td>
              <td>${escapeHtml(acct.branchCode || "-")}</td>
              <td>${escapeHtml(acct.accountType || "-")}</td>
              <td>${acct.isPrimary ? '<span class="badge active">Yes</span>' : '<span class="badge pending">No</span>'}</td>
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

    el.churchAccountantAccessSection?.classList.remove("hidden");
    const portalSettings = church?.adminPortalSettings && typeof church.adminPortalSettings === "object" ? church.adminPortalSettings : {};
    const configuredTabs = portalSettings?.accountantTabs || [];
    setChurchAccountantTabsInUi(configuredTabs);
    if (el.churchAccountantAccessMeta) {
      const normalized = normalizeAccountantTabs(configuredTabs);
      el.churchAccountantAccessMeta.textContent = normalized.length
        ? "Accountant access is customized for this church."
        : "Using default accountant tabs (dashboard, transactions, statements). Save to override.";
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

  async function deleteOnboardingRequest(requestId) {
    if (!requestId) {
      toast("Select an onboarding request first", "error");
      return;
    }

    const row = state.onboardingRows.find((r) => r && r.id === requestId) || null;
    const status = String(row?.verificationStatus || "").toLowerCase();
    if (status !== "rejected") {
      toast("Only rejected onboarding requests can be deleted", "error");
      return;
    }

    const label = row?.churchName ? `"${row.churchName}"` : requestId;
    const confirmed = window.confirm(`Delete rejected onboarding request ${label}?\n\nThis cannot be undone.`);
    if (!confirmed) return;

    await apiRequest(`/api/super/church-onboarding/${encodeURIComponent(requestId)}?confirm=${encodeURIComponent(requestId)}`, {
      method: "DELETE",
    });

    toast("Onboarding request deleted", "success");
    if (state.selectedOnboardingId === requestId) {
      state.selectedOnboardingId = "";
      if (el.onboardingDetailsMeta) el.onboardingDetailsMeta.textContent = "Select a request to view details.";
      if (el.onboardingDetailsChurch) el.onboardingDetailsChurch.innerHTML = "";
      if (el.onboardingDetailsAdmin) el.onboardingDetailsAdmin.innerHTML = "";
      if (el.onboardingReviewNoteInput) el.onboardingReviewNoteInput.value = "";
      if (el.onboardingActionMeta) el.onboardingActionMeta.textContent = "Approval creates/promotes the church admin account.";
    }

    await loadOnboardingRequests();
    await refreshOnboardingPendingBadge();
  }

  async function deleteChurch(churchId) {
    const row = selectedChurchById(churchId);
    if (!row) {
      toast("Church not found in current list", "error");
      return;
    }
    if (row.active) {
      toast("Disable the church before deleting it", "error");
      return;
    }

    const confirmed = window.confirm(
      `Permanently delete church \"${row.name || row.joinCode || churchId}\"?\n\nThis can only succeed if the church has no transaction/payment history.\nIf it has history, please keep it disabled instead.`
    );
    if (!confirmed) return;

    try {
      await apiRequest(`/api/super/churches/${encodeURIComponent(churchId)}`, { method: "DELETE" });
      toast("Church deleted", "success");

      if (state.selectedChurchId === churchId) {
        state.selectedChurchId = "";
        if (el.churchDetailsMeta) el.churchDetailsMeta.textContent = "Select a church to view details.";
        renderEmpty(el.churchDetailFundsBody, 4, "Select a church to view funds.");
        renderEmpty(el.churchDetailAdminsBody, 4, "Select a church to view admins.");
        renderEmpty(el.churchDetailTransactionsBody, 5, "Select a church to view transactions.");
      }

      await loadChurches();
    } catch (err) {
      if (err?.status === 409) {
        const typed = window.prompt(
          `Delete failed: ${err?.message || "This church has history."}\n\n` +
            `If this church is TEST DATA and you want to permanently purge ALL its data (members, funds, payments, transactions), type PURGE and press OK.\n\n` +
            `This requires DATA_PURGE_ENABLED=true on the server.`
        );
        if (String(typed || "").trim().toUpperCase() === "PURGE") {
          try {
            await apiRequest(
              `/api/super/churches/${encodeURIComponent(churchId)}/purge?confirm=${encodeURIComponent(churchId)}`,
              { method: "DELETE" }
            );
            toast("Church purged permanently", "success");

            if (state.selectedChurchId === churchId) {
              state.selectedChurchId = "";
              if (el.churchDetailsMeta) el.churchDetailsMeta.textContent = "Select a church to view details.";
              renderEmpty(el.churchDetailFundsBody, 4, "Select a church to view funds.");
              renderEmpty(el.churchDetailAdminsBody, 4, "Select a church to view admins.");
              renderEmpty(el.churchDetailTransactionsBody, 5, "Select a church to view transactions.");
            }

            await loadChurches();
            return;
          } catch (purgeErr) {
            toast(purgeErr?.message || "Purge failed", "error");
            return;
          }
        }
      }
      toast(err?.message || "Delete failed", "error");
    }
  }

  function onboardingBadgeClass(status) {
    const normalized = String(status || "").toLowerCase();
    if (normalized === "approved") return "active";
    if (normalized === "rejected") return "failed";
    return "pending";
  }

  function onboardingStatusText(status) {
    const normalized = String(status || "").toLowerCase();
    if (!normalized) return "PENDING";
    return normalized.toUpperCase();
  }

  function sanitizePendingCount(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric) || numeric < 0) return 0;
    return Math.floor(numeric);
  }

  function renderOnboardingPendingBadge(count) {
    if (!el.onboardingPendingBadge) return;
    const pendingCount = sanitizePendingCount(count);
    const label =
      pendingCount === 1 ? "1 pending onboarding request" : `${pendingCount} pending onboarding requests`;

    el.onboardingPendingBadge.textContent = pendingCount > 99 ? "99+" : String(pendingCount);
    el.onboardingPendingBadge.classList.toggle("hidden", pendingCount <= 0);
    el.onboardingPendingBadge.setAttribute("title", label);

    const onboardingNav = el.navTabs?.querySelector(".nav-link[data-tab='onboarding']");
    if (onboardingNav) {
      onboardingNav.setAttribute("aria-label", pendingCount > 0 ? `Onboarding, ${label}` : "Onboarding");
    }
  }

  async function refreshOnboardingPendingBadge(options = {}) {
    const fallbackCount = options.pendingCount;
    if (Number.isFinite(Number(fallbackCount))) {
      renderOnboardingPendingBadge(fallbackCount);
      return;
    }

    try {
      const data = await apiRequest(
        "/api/super/church-onboarding" +
          buildQuery({
            status: "pending",
            limit: 1,
            offset: 0,
          })
      );
      const pendingCount = Number(data?.meta?.count ?? data?.requests?.length ?? 0);
      renderOnboardingPendingBadge(pendingCount);
    } catch (err) {
      console.error("[super/onboarding-badge] load error", err?.message || err, err?.stack);
    }
  }

  function currentOnboardingFilters() {
    return {
      status: el.onboardingStatusSelect?.value || "",
      search: el.onboardingSearchInput?.value || "",
      limit: Math.max(1, Math.min(200, Number(el.onboardingLimitInput?.value || 50))),
      offset: 0,
    };
  }

  function findOnboardingRequest(requestId) {
    return state.onboardingRows.find((item) => item.id === requestId) || null;
  }

  function renderOnboardingDetailPlaceholder(message) {
    if (el.onboardingDetailsMeta) el.onboardingDetailsMeta.textContent = message || "Select a request to view details.";
    if (el.onboardingDetailsChurch) {
      el.onboardingDetailsChurch.innerHTML = `<p class="meta-line">${escapeHtml(message || "No request selected.")}</p>`;
    }
    if (el.onboardingDetailsAdmin) {
      el.onboardingDetailsAdmin.innerHTML = `<p class="meta-line">${escapeHtml(message || "No request selected.")}</p>`;
    }
    if (el.onboardingActionMeta) {
      el.onboardingActionMeta.textContent = "Approval creates/promotes the church admin account.";
    }
    if (el.onboardingReviewNoteInput) {
      el.onboardingReviewNoteInput.value = "";
    }
    if (el.onboardingDownloadCipcBtn) el.onboardingDownloadCipcBtn.disabled = true;
    if (el.onboardingDownloadBankBtn) el.onboardingDownloadBankBtn.disabled = true;
    if (el.onboardingEditBtn) el.onboardingEditBtn.disabled = true;
    if (el.onboardingReplaceCipcBtn) el.onboardingReplaceCipcBtn.disabled = true;
    if (el.onboardingReplaceBankBtn) el.onboardingReplaceBankBtn.disabled = true;
    if (el.onboardingApproveBtn) el.onboardingApproveBtn.disabled = true;
    if (el.onboardingRejectBtn) el.onboardingRejectBtn.disabled = true;
    if (el.onboardingDeleteBtn) el.onboardingDeleteBtn.disabled = true;
  }

  function renderOnboardingDetail(request) {
    if (!request) {
      renderOnboardingDetailPlaceholder("Select a request to view details.");
      return;
    }

    const status = onboardingStatusText(request.verificationStatus);
    const verifiedEmail = request.adminEmailVerified ? "Yes" : "No";
    const cipcName = request.cipcFilename || "-";
    const bankName = request.bankConfirmationFilename || "-";
    const cipcBytes = request.cipcBytes ? ` (${formatBytes(request.cipcBytes)})` : "";
    const bankBytes = request.bankConfirmationBytes ? ` (${formatBytes(request.bankConfirmationBytes)})` : "";
    const currentNote = String(request.verificationNote || "");
    const bankAccounts = Array.isArray(request.bankAccounts) ? request.bankAccounts : [];

    if (el.onboardingDetailsMeta) {
      el.onboardingDetailsMeta.textContent =
        `${request.churchName || "-"} | ${status} | submitted ${formatDate(request.createdAt)}`;
    }

    if (el.onboardingDetailsChurch) {
      const bankAccountsHtml = bankAccounts.length
        ? bankAccounts
            .map((acct, idx) => {
              const labelParts = [
                acct.bankName ? String(acct.bankName) : "",
                acct.accountName ? String(acct.accountName) : "",
                acct.accountNumber ? String(acct.accountNumber) : "",
                acct.branchCode ? `Branch: ${String(acct.branchCode)}` : "",
                acct.accountType ? `Type: ${String(acct.accountType)}` : "",
              ].filter(Boolean);
              const suffix = acct.isPrimary ? " (primary)" : "";
              const title = labelParts.join(" | ");
              return `<p class="meta-line"><strong>Account ${idx + 1}:</strong> ${escapeHtml(title)}${escapeHtml(suffix)}</p>`;
            })
            .join("")
        : `<p class="meta-line"><strong>Bank accounts:</strong> -</p>`;

      el.onboardingDetailsChurch.innerHTML = `
        <p class="meta-line"><strong>Name:</strong> ${escapeHtml(request.churchName || "-")}</p>
        <p class="meta-line"><strong>Requested join code:</strong> ${escapeHtml(request.requestedJoinCode || "-")}</p>
        <p class="meta-line"><strong>Approved church ID:</strong> ${escapeHtml(request.approvedChurchId || "-")}</p>
        ${bankAccountsHtml}
        <p class="meta-line"><strong>CIPC document:</strong> ${escapeHtml(cipcName)}${escapeHtml(cipcBytes)}</p>
        <p class="meta-line"><strong>Bank letter:</strong> ${escapeHtml(bankName)}${escapeHtml(bankBytes)}</p>
      `;
    }

    if (el.onboardingDetailsAdmin) {
      el.onboardingDetailsAdmin.innerHTML = `
        <p class="meta-line"><strong>Full name:</strong> ${escapeHtml(request.adminFullName || "-")}</p>
        <p class="meta-line"><strong>Phone:</strong> ${escapeHtml(request.adminPhone || "-")}</p>
        <p class="meta-line"><strong>Email:</strong> ${escapeHtml(request.adminEmail || "-")}</p>
        <p class="meta-line"><strong>Email verified:</strong> ${escapeHtml(verifiedEmail)}</p>
        <p class="meta-line"><strong>Verified by:</strong> ${escapeHtml(request.verifiedBy || "-")}</p>
        <p class="meta-line"><strong>Verified at:</strong> ${escapeHtml(formatDate(request.verifiedAt))}</p>
        <p class="meta-line"><strong>Current review note:</strong> ${escapeHtml(currentNote || "-")}</p>
      `;
    }

    if (el.onboardingActionMeta) {
      el.onboardingActionMeta.textContent = request.adminEmailVerified
        ? "Email verified. You can approve or reject this request."
        : "Admin email is not verified yet. Approval will be blocked.";
    }

    if (el.onboardingReviewNoteInput) {
      el.onboardingReviewNoteInput.value = currentNote;
    }
    const isPending = String(request.verificationStatus || "").toLowerCase() === "pending";
    const isRejected = String(request.verificationStatus || "").toLowerCase() === "rejected";
    const hasCipc = !!request.cipcFilename || Number(request.cipcBytes || 0) > 0;
    const hasBank = !!request.bankConfirmationFilename || Number(request.bankConfirmationBytes || 0) > 0;
    if (el.onboardingDownloadCipcBtn) el.onboardingDownloadCipcBtn.disabled = !hasCipc;
    if (el.onboardingDownloadBankBtn) el.onboardingDownloadBankBtn.disabled = !hasBank;
    if (el.onboardingEditBtn) el.onboardingEditBtn.disabled = false;
    if (el.onboardingReplaceCipcBtn) el.onboardingReplaceCipcBtn.disabled = false;
    if (el.onboardingReplaceBankBtn) el.onboardingReplaceBankBtn.disabled = false;
    if (el.onboardingApproveBtn) el.onboardingApproveBtn.disabled = !(isPending && request.adminEmailVerified);
    if (el.onboardingRejectBtn) el.onboardingRejectBtn.disabled = !isPending;
    if (el.onboardingDeleteBtn) el.onboardingDeleteBtn.disabled = !isRejected;
  }

  async function loadOnboardingRequests() {
    renderSkeletonRows(el.onboardingBody, 7, 9);
    const query = currentOnboardingFilters();
    const isPendingFilter = String(query.status || "").toLowerCase() === "pending";
    state.onboardingFilters = query;

    const data = await apiRequest("/api/super/church-onboarding" + buildQuery(query));
    const rows = Array.isArray(data.requests) ? data.requests : [];
    const meta = data.meta || { count: 0, returned: rows.length, limit: query.limit, offset: query.offset };
    state.onboardingRows = rows;

    if (el.onboardingMeta) {
      el.onboardingMeta.textContent = `Showing ${meta.returned || rows.length} of ${meta.count || 0}`;
    }

    if (!rows.length) {
      renderEmpty(el.onboardingBody, 9, "No onboarding requests found.");
      state.selectedOnboardingId = "";
      renderOnboardingDetailPlaceholder("No onboarding requests found.");
      await refreshOnboardingPendingBadge({
        pendingCount: isPendingFilter ? Number(meta.count || 0) : undefined,
      });
      return;
    }

    el.onboardingBody.innerHTML = rows
      .map((request) => {
        const status = onboardingStatusText(request.verificationStatus);
        const statusClass = onboardingBadgeClass(request.verificationStatus);
        const rejected = String(request.verificationStatus || "").toLowerCase() === "rejected";
        const cipcReady = !!request.cipcFilename || Number(request.cipcBytes || 0) > 0;
        const bankReady = !!request.bankConfirmationFilename || Number(request.bankConfirmationBytes || 0) > 0;
        const notePreview = String(request.verificationNote || "").trim();
        const shortNote = notePreview.length > 64 ? `${notePreview.slice(0, 64)}...` : notePreview || "-";
        return `
          <tr>
            <td>${escapeHtml(request.churchName || "-")}</td>
            <td>${escapeHtml(request.requestedJoinCode || "-")}</td>
            <td>${escapeHtml(request.adminFullName || "-")}</td>
            <td><span class="badge ${request.adminEmailVerified ? "active" : "pending"}">${request.adminEmailVerified ? "Verified" : "Pending"}</span></td>
            <td>
              <span class="badge ${cipcReady ? "active" : "pending"}">CIPC ${cipcReady ? "Yes" : "No"}</span>
              <span class="badge ${bankReady ? "active" : "pending"}">Bank ${bankReady ? "Yes" : "No"}</span>
            </td>
            <td><span class="badge ${statusClass}">${escapeHtml(status)}</span></td>
            <td>${escapeHtml(formatDate(request.createdAt))}</td>
            <td title="${escapeHtml(notePreview || "")}">${escapeHtml(shortNote)}</td>
            <td class="actions-cell">
              <button class="btn ghost" type="button" data-onboarding-action="view" data-id="${escapeHtml(request.id)}">View</button>
              <button class="btn ghost" type="button" data-onboarding-action="cipc" data-id="${escapeHtml(request.id)}">CIPC</button>
              <button class="btn ghost" type="button" data-onboarding-action="bank" data-id="${escapeHtml(request.id)}">Bank</button>
              ${rejected ? `<button class="btn danger ghost" type="button" data-onboarding-action="delete" data-id="${escapeHtml(request.id)}">Delete</button>` : ""}
            </td>
          </tr>
        `;
      })
      .join("");

    if (!state.selectedOnboardingId || !findOnboardingRequest(state.selectedOnboardingId)) {
      state.selectedOnboardingId = rows[0].id;
    }
    await loadOnboardingDetail(state.selectedOnboardingId);
    await refreshOnboardingPendingBadge({
      pendingCount: isPendingFilter ? Number(meta.count || rows.length) : undefined,
    });
  }

  async function loadOnboardingDetail(requestId) {
    if (!requestId) {
      state.selectedOnboardingId = "";
      state.selectedOnboardingDetail = null;
      renderOnboardingDetailPlaceholder("Select a request to view details.");
      return;
    }
    const data = await apiRequest(`/api/super/church-onboarding/${encodeURIComponent(requestId)}`);
    const request = data.request || null;
    state.selectedOnboardingId = request ? request.id : requestId;
    state.selectedOnboardingDetail = request;
    renderOnboardingDetail(request);
  }

  async function previewOnboardingDocument(requestId, documentType) {
    if (!requestId) {
      toast("Select an onboarding request first", "error");
      return;
    }

    const response = await apiRequest(
      `/api/super/church-onboarding/${encodeURIComponent(requestId)}/documents/${encodeURIComponent(documentType)}?inline=1`,
      { raw: true }
    );
    const blob = await response.blob();
    const fallbackName = documentType === "cipc" ? "cipc-document" : "bank-confirmation";
    const filename = parseContentDispositionFilename(response.headers.get("content-disposition")) || `${fallbackName}.bin`;
    const mime = String(response.headers.get("content-type") || blob.type || "").toLowerCase();

    revokePreviewObjectUrl();
    state.previewObjectUrl = URL.createObjectURL(blob);

    if (el.documentPreviewTitle) {
      const label = documentType === "cipc" ? "CIPC document" : "Bank confirmation letter";
      el.documentPreviewTitle.textContent = `Preview: ${label}`;
    }
    if (el.documentPreviewMeta) {
      el.documentPreviewMeta.textContent = `${filename} | ${mime || "unknown type"} | ${formatBytes(blob.size)}`;
    }
    if (el.documentPreviewOpenNewTab) {
      el.documentPreviewOpenNewTab.href = state.previewObjectUrl;
    }
    if (el.documentPreviewDownloadLink) {
      el.documentPreviewDownloadLink.href = state.previewObjectUrl;
      el.documentPreviewDownloadLink.download = filename;
    }

    if (el.documentPreviewContainer) {
      if (mime.includes("pdf")) {
        el.documentPreviewContainer.innerHTML = `<iframe title="Document preview" src="${state.previewObjectUrl}" style="width:100%;height:70vh;border:0;"></iframe>`;
      } else if (mime.startsWith("image/")) {
        el.documentPreviewContainer.innerHTML = `<div style="padding:12px;text-align:center;"><img alt="Document preview" src="${state.previewObjectUrl}" style="max-width:100%;max-height:70vh;object-fit:contain;" /></div>`;
      } else {
        el.documentPreviewContainer.innerHTML = `<div class="meta-line" style="padding:14px;">Inline preview is not available for this file type. Use Open in new tab or Download.</div>`;
      }
    }

    if (el.documentPreviewDialog && typeof el.documentPreviewDialog.showModal === "function") {
      el.documentPreviewDialog.showModal();
    }
  }

  function blankBankAccount(isPrimary = false) {
    return {
      bankName: "",
      accountName: "",
      accountNumber: "",
      branchCode: "",
      accountType: "",
      isPrimary: !!isPrimary,
    };
  }

  function openChurchBankDialog() {
    const churchId = String(state.selectedChurchId || "").trim();
    if (!churchId) {
      toast("Select a church first", "error");
      return;
    }

    const churchName = state.selectedChurchDetail?.church?.name || selectedChurchById(churchId)?.name || "Church";
    const existing = Array.isArray(state.selectedChurchDetail?.bankAccounts)
      ? state.selectedChurchDetail.bankAccounts
      : [];

    state.churchBankDialogChurchId = churchId;
    state.churchBankDialogChurchName = churchName;
    state.churchBankAccountsDraft = cloneBankAccounts(existing);
    if (!state.churchBankAccountsDraft.length) state.churchBankAccountsDraft = [blankBankAccount(true)];

    if (el.churchBankDialogTitle) el.churchBankDialogTitle.textContent = `Edit bank accounts: ${churchName}`;
    if (el.churchBankDialogMeta) {
      el.churchBankDialogMeta.textContent = "Bank accounts are used for payouts. Choose one primary account.";
    }

    renderBankAccountsEditor(el.churchBankAccountsList, state.churchBankAccountsDraft, {
      primaryGroup: "churchBankPrimary",
    });

    if (el.churchBankDialog && typeof el.churchBankDialog.showModal === "function") {
      el.churchBankDialog.showModal();
    }
  }

  async function saveChurchBankAccountsFromDialog() {
    const churchId = String(state.churchBankDialogChurchId || "").trim();
    if (!churchId) throw new Error("Missing church");

    const body = {
      bankAccounts: ensurePrimaryAccount(state.churchBankAccountsDraft || []),
    };
    await apiRequest(`/api/super/churches/${encodeURIComponent(churchId)}/bank-accounts`, {
      method: "PUT",
      body,
    });
    toast("Bank accounts updated", "success");
    await loadChurchDetail(churchId);
  }

  function openOnboardingEditDialog() {
    const request = state.selectedOnboardingDetail;
    if (!request?.id) {
      toast("Select an onboarding request first", "error");
      return;
    }

    state.onboardingEditDraft = {
      requestId: request.id,
      bankAccounts: cloneBankAccounts(Array.isArray(request.bankAccounts) ? request.bankAccounts : []),
    };
    if (!state.onboardingEditDraft.bankAccounts.length) state.onboardingEditDraft.bankAccounts = [blankBankAccount(true)];

    if (el.onboardingEditDialogTitle) el.onboardingEditDialogTitle.textContent = "Edit onboarding request";
    if (el.onboardingEditDialogMeta) {
      el.onboardingEditDialogMeta.textContent =
        "Updating the admin email will reset email verification and require a new code to be sent.";
    }

    if (el.onboardingEditChurchName) el.onboardingEditChurchName.value = request.churchName || "";
    if (el.onboardingEditJoinCode) el.onboardingEditJoinCode.value = String(request.requestedJoinCode || "").toUpperCase();
    if (el.onboardingEditAdminName) el.onboardingEditAdminName.value = request.adminFullName || "";
    if (el.onboardingEditAdminPhone) el.onboardingEditAdminPhone.value = request.adminPhone || "";
    if (el.onboardingEditAdminEmail) el.onboardingEditAdminEmail.value = request.adminEmail || "";

    renderBankAccountsEditor(el.onboardingBankAccountsList, state.onboardingEditDraft.bankAccounts, {
      primaryGroup: "onboardingBankPrimary",
    });

    if (el.onboardingEditDialog && typeof el.onboardingEditDialog.showModal === "function") {
      el.onboardingEditDialog.showModal();
    }
  }

  async function saveOnboardingEditsFromDialog() {
    const draft = state.onboardingEditDraft;
    const requestId = String(draft?.requestId || "").trim();
    if (!requestId) throw new Error("Missing onboarding request");

    const churchName = String(el.onboardingEditChurchName?.value || "").trim();
    const requestedJoinCode = String(el.onboardingEditJoinCode?.value || "").trim().toUpperCase();
    const adminFullName = String(el.onboardingEditAdminName?.value || "").trim();
    const adminPhone = String(el.onboardingEditAdminPhone?.value || "").trim();
    const adminEmail = String(el.onboardingEditAdminEmail?.value || "").trim();

    const body = {
      churchName,
      requestedJoinCode,
      adminFullName,
      adminPhone,
      adminEmail,
      bankAccounts: ensurePrimaryAccount(draft?.bankAccounts || []),
    };

    await apiRequest(`/api/super/church-onboarding/${encodeURIComponent(requestId)}`, {
      method: "PATCH",
      body,
    });
    toast("Onboarding request updated", "success");
    await loadOnboardingRequests();
    await loadOnboardingDetail(requestId);
  }

  async function replaceOnboardingDocumentFromPicker(documentType) {
    const requestId = String(state.selectedOnboardingId || "").trim();
    if (!requestId) {
      toast("Select an onboarding request first", "error");
      return;
    }
    const normalizedType = String(documentType || "").toLowerCase();
    if (!["cipc", "bank"].includes(normalizedType)) return;

    const input = document.createElement("input");
    input.type = "file";
    input.accept = ".pdf,image/jpeg,image/png,image/webp";
    input.onchange = () => {
      const file = input.files && input.files[0];
      if (!file) return;

      void (async () => {
        try {
          const label = normalizedType === "cipc" ? "CIPC document" : "Bank confirmation letter";
          const payload = await fileToPayload(file, { label, maxBytes: 10 * 1024 * 1024 });
          await apiRequest(
            `/api/super/church-onboarding/${encodeURIComponent(requestId)}/documents/${encodeURIComponent(normalizedType)}`,
            { method: "PUT", body: payload }
          );
          toast(`${label} updated`, "success");
          await loadOnboardingRequests();
          await loadOnboardingDetail(requestId);
        } catch (err) {
          toast(err?.message || "Upload failed", "error");
        }
      })();
    };
    input.click();
  }

  async function approveOnboardingRequest(requestId) {
    if (!requestId) {
      toast("Select an onboarding request first", "error");
      return;
    }

    const note = String(el.onboardingReviewNoteInput?.value || "").trim();
    const data = await apiRequest(`/api/super/church-onboarding/${encodeURIComponent(requestId)}/approve`, {
      method: "POST",
      body: { verificationNote: note },
    });

    const tempPassword = data?.admin?.temporaryPassword || "";
    if (tempPassword) {
      window.alert(`Onboarding approved.\nTemporary admin password:\n${tempPassword}\n\nSave it securely now.`);
    } else {
      toast("Onboarding approved", "success");
    }

    await loadOnboardingRequests();
    await loadOnboardingDetail(requestId);
  }

  async function rejectOnboardingRequest(requestId) {
    if (!requestId) {
      toast("Select an onboarding request first", "error");
      return;
    }

    const trimmedReason = String(el.onboardingReviewNoteInput?.value || "").trim();
    if (!trimmedReason) {
      toast("Review comment is required when rejecting", "error");
      el.onboardingReviewNoteInput?.focus();
      return;
    }

    await apiRequest(`/api/super/church-onboarding/${encodeURIComponent(requestId)}/reject`, {
      method: "POST",
      body: { verificationNote: trimmedReason },
    });

    toast("Onboarding request rejected", "success");
    await loadOnboardingRequests();
    await loadOnboardingDetail(requestId);
  }

  function formatDateTimeLocalInput(value) {
    if (!value) return "";
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return "";
    const pad = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }

  function jobsStatusBadgeClass(status) {
    const s = String(status || "").toUpperCase();
    if (s === "PUBLISHED") return "active";
    if (s === "DRAFT") return "pending";
    return "inactive";
  }

  function selectedJobById(jobId) {
    return state.jobsRows.find((job) => job.id === jobId) || null;
  }

  function setJobFormMeta(message, kind = "info") {
    if (!el.jobFormMeta) return;
    el.jobFormMeta.className = `meta-line${kind === "error" ? " error" : ""}`;
    el.jobFormMeta.textContent = message || "Provide either application URL or application email.";
  }

  function resetJobForm() {
    state.selectedJobId = "";
    if (el.jobIdInput) el.jobIdInput.value = "";
    if (el.jobFormHeading) el.jobFormHeading.textContent = "Create job advert";
    if (el.saveJobBtn) el.saveJobBtn.textContent = "Save advert";
    if (el.deleteJobBtn) el.deleteJobBtn.disabled = true;

    if (el.jobTitleInput) el.jobTitleInput.value = "";
    if (el.jobChurchSelect) el.jobChurchSelect.value = "";
    if (el.jobStatusSelect) el.jobStatusSelect.value = "DRAFT";
    if (el.jobEmploymentTypeSelect) el.jobEmploymentTypeSelect.value = "FULL_TIME";
    if (el.jobLocationInput) el.jobLocationInput.value = "South Africa";
    if (el.jobDepartmentInput) el.jobDepartmentInput.value = "";
    if (el.jobSummaryInput) el.jobSummaryInput.value = "";
    if (el.jobDescriptionInput) el.jobDescriptionInput.value = "";
    if (el.jobRequirementsInput) el.jobRequirementsInput.value = "";
    if (el.jobApplicationUrlInput) el.jobApplicationUrlInput.value = "";
    if (el.jobApplicationEmailInput) el.jobApplicationEmailInput.value = "";
    if (el.jobExpiresAtInput) el.jobExpiresAtInput.value = "";
    setJobFormMeta("");
  }

  function fillJobForm(job) {
    if (!job) return;
    state.selectedJobId = job.id;
    if (el.jobIdInput) el.jobIdInput.value = job.id;
    if (el.jobFormHeading) el.jobFormHeading.textContent = "Edit job advert";
    if (el.saveJobBtn) el.saveJobBtn.textContent = "Save changes";
    if (el.deleteJobBtn) el.deleteJobBtn.disabled = false;

    if (el.jobTitleInput) el.jobTitleInput.value = job.title || "";
    if (el.jobChurchSelect) el.jobChurchSelect.value = job.churchId || "";
    if (el.jobStatusSelect) el.jobStatusSelect.value = String(job.status || "DRAFT").toUpperCase();
    if (el.jobEmploymentTypeSelect) el.jobEmploymentTypeSelect.value = String(job.employmentType || "FULL_TIME").toUpperCase();
    if (el.jobLocationInput) el.jobLocationInput.value = job.location || "South Africa";
    if (el.jobDepartmentInput) el.jobDepartmentInput.value = job.department || "";
    if (el.jobSummaryInput) el.jobSummaryInput.value = job.summary || "";
    if (el.jobDescriptionInput) el.jobDescriptionInput.value = job.description || "";
    if (el.jobRequirementsInput) el.jobRequirementsInput.value = job.requirements || "";
    if (el.jobApplicationUrlInput) el.jobApplicationUrlInput.value = job.applicationUrl || "";
    if (el.jobApplicationEmailInput) el.jobApplicationEmailInput.value = job.applicationEmail || "";
    if (el.jobExpiresAtInput) el.jobExpiresAtInput.value = formatDateTimeLocalInput(job.expiresAt);
    setJobFormMeta(`Editing ${job.title || "job advert"}`);
  }

  function currentJobsFilters() {
    return {
      status: String(el.jobsStatusSelect?.value || "").toUpperCase(),
      churchId: el.jobsChurchSelect?.value || "",
      search: el.jobsSearchInput?.value || "",
      limit: Math.max(1, Math.min(200, Number(el.jobsLimitInput?.value || 50))),
      offset: 0,
    };
  }

  async function loadJobs() {
    const query = currentJobsFilters();
    state.jobsFilters = query;

    renderSkeletonRows(el.jobsBody, 8, 6);
    const data = await apiRequest("/api/super/jobs" + buildQuery(query));
    const rows = Array.isArray(data.jobs) ? data.jobs : [];
    const meta = data.meta || { count: 0, returned: rows.length };
    state.jobsRows = rows;

    if (el.jobsMeta) {
      el.jobsMeta.textContent = `Showing ${meta.returned || rows.length} of ${meta.count || 0}`;
    }

    if (!rows.length) {
      renderEmpty(el.jobsBody, 8, "No job adverts found.");
      return;
    }

    el.jobsBody.innerHTML = rows
      .map((job) => {
        const status = String(job.status || "DRAFT").toUpperCase();
        const nextAction =
          status === "DRAFT"
            ? { status: "PUBLISHED", label: "Publish" }
            : status === "PUBLISHED"
            ? { status: "CLOSED", label: "Close" }
            : { status: "DRAFT", label: "Move to draft" };

        const applyDisplay = job.applicationUrl
          ? `<a href="${escapeHtml(job.applicationUrl)}" target="_blank" rel="noopener noreferrer">Link</a>`
          : escapeHtml(job.applicationEmail || "-");

        return `
          <tr>
            <td><strong>${escapeHtml(job.title || "-")}</strong></td>
            <td>${escapeHtml(job.churchName || "All churches")}</td>
            <td>${escapeHtml(String(job.employmentType || "").replaceAll("_", " ") || "-")}</td>
            <td><span class="badge ${jobsStatusBadgeClass(status)}">${escapeHtml(status)}</span></td>
            <td>${escapeHtml(formatDate(job.publishedAt))}</td>
            <td>${escapeHtml(formatDate(job.expiresAt))}</td>
            <td>${applyDisplay}</td>
            <td class="actions-cell">
              <button class="btn ghost" type="button" data-job-action="edit" data-id="${escapeHtml(job.id)}">Edit</button>
              <button class="btn ghost" type="button" data-job-action="status" data-id="${escapeHtml(job.id)}" data-next-status="${escapeHtml(nextAction.status)}">${escapeHtml(nextAction.label)}</button>
              <button class="btn danger ghost" type="button" data-job-action="delete" data-id="${escapeHtml(job.id)}">Delete</button>
            </td>
          </tr>
        `;
      })
      .join("");

    if (state.selectedJobId) {
      const selected = selectedJobById(state.selectedJobId);
      if (selected) fillJobForm(selected);
      else resetJobForm();
    }
  }

  async function saveJobFromForm() {
    const jobId = String(el.jobIdInput?.value || "").trim();
    const payload = {
      title: String(el.jobTitleInput?.value || "").trim(),
      churchId: String(el.jobChurchSelect?.value || "").trim(),
      status: String(el.jobStatusSelect?.value || "DRAFT").trim().toUpperCase(),
      employmentType: String(el.jobEmploymentTypeSelect?.value || "FULL_TIME").trim().toUpperCase(),
      location: String(el.jobLocationInput?.value || "").trim(),
      department: String(el.jobDepartmentInput?.value || "").trim(),
      summary: String(el.jobSummaryInput?.value || "").trim(),
      description: String(el.jobDescriptionInput?.value || "").trim(),
      requirements: String(el.jobRequirementsInput?.value || "").trim(),
      applicationUrl: String(el.jobApplicationUrlInput?.value || "").trim(),
      applicationEmail: String(el.jobApplicationEmailInput?.value || "").trim(),
      expiresAt: String(el.jobExpiresAtInput?.value || "").trim(),
    };

    if (!payload.title) {
      setJobFormMeta("Title is required.", "error");
      el.jobTitleInput?.focus();
      return;
    }
    if (!payload.description) {
      setJobFormMeta("Description is required.", "error");
      el.jobDescriptionInput?.focus();
      return;
    }
    if (!payload.applicationUrl && !payload.applicationEmail) {
      setJobFormMeta("Provide application URL or application email.", "error");
      el.jobApplicationUrlInput?.focus();
      return;
    }

    const body = {
      title: payload.title,
      churchId: payload.churchId || null,
      status: payload.status,
      employmentType: payload.employmentType,
      location: payload.location || "South Africa",
      department: payload.department || null,
      summary: payload.summary || null,
      description: payload.description,
      requirements: payload.requirements || null,
      applicationUrl: payload.applicationUrl || null,
      applicationEmail: payload.applicationEmail || null,
      expiresAt: payload.expiresAt || null,
    };

    const isEdit = !!jobId;
    const path = isEdit ? `/api/super/jobs/${encodeURIComponent(jobId)}` : "/api/super/jobs";
    const method = isEdit ? "PATCH" : "POST";
    const data = await apiRequest(path, { method, body });
    const saved = data?.job || null;

    toast(isEdit ? "Job advert updated" : "Job advert created", "success");
    await loadJobs();
    if (saved) fillJobForm(saved);
    else resetJobForm();
  }

  async function updateJobStatus(jobId, status) {
    const targetStatus = String(status || "").toUpperCase();
    if (!jobId || !targetStatus) return;
    await apiRequest(`/api/super/jobs/${encodeURIComponent(jobId)}`, {
      method: "PATCH",
      body: { status: targetStatus },
    });
    toast(`Job moved to ${targetStatus.toLowerCase()}`, "success");
    await loadJobs();
    const refreshed = selectedJobById(jobId);
    if (refreshed && state.selectedJobId === jobId) fillJobForm(refreshed);
  }

  async function deleteJobAdvert(jobId) {
    if (!jobId) return;
    const job = selectedJobById(jobId);
    const label = job?.title || jobId;
    const confirmed = window.confirm(`Delete job advert "${label}"? This cannot be undone.`);
    if (!confirmed) return;

    await apiRequest(`/api/super/jobs/${encodeURIComponent(jobId)}`, { method: "DELETE" });
    toast("Job advert deleted", "success");
    if (state.selectedJobId === jobId) resetJobForm();
    await loadJobs();
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
            <td>${escapeHtml(formatPayerDisplay(tx))}</td>
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

  async function requestMemberPasswordReset(memberId) {
    const id = String(memberId || "").trim();
    if (!id) return;

    const confirmed = window.confirm("Send a password reset code to this member by email?");
    if (!confirmed) return;

    try {
      const result = await apiRequest(`/api/super/members/${encodeURIComponent(id)}/password-reset`, { method: "POST" });
      toast(`Reset email sent to ${result.email || "member"} (${result.provider || "email"})`, "success");
    } catch (err) {
      toast(err?.message || "Failed to send reset email", "error");
    }
  }

  async function saveMemberDateOfBirth(memberId, dateOfBirthValue) {
    const id = String(memberId || "").trim();
    if (!id) return;

    const trimmed = String(dateOfBirthValue || "").trim();
    const body = { dateOfBirth: trimmed || null };

    const result = await apiRequest(`/api/super/members/${encodeURIComponent(id)}/date-of-birth`, {
      method: "PATCH",
      body,
    });
    const savedDob = formatDateOfBirth(result?.member?.dateOfBirth);
    toast(`Date of birth updated: ${savedDob}`, "success");
    await loadMembers();
  }

  function openMemberDateOfBirthDialog(member) {
    if (!el.memberDobDialog || typeof el.memberDobDialog.showModal !== "function") {
      toast("DOB dialog is not available in this browser.", "error");
      return;
    }

    const id = String(member?.id || "").trim();
    if (!id) {
      toast("Invalid member selected.", "error");
      return;
    }

    const fullName = String(member?.fullName || "Member").trim() || "Member";
    const rawDateOfBirth =
      typeof member.dateOfBirth === "string"
        ? member.dateOfBirth
        : (typeof member.date_of_birth === "string" ? member.date_of_birth : "");
    const normalizedDob = formatDateOfBirth(rawDateOfBirth);

    state.memberActionsMemberId = id;
    if (el.memberDobDialogTitle) el.memberDobDialogTitle.textContent = `Edit DOB: ${fullName}`;
    if (el.memberDobDialogMeta) el.memberDobDialogMeta.textContent = "Use date picker or clear to remove DOB.";
    if (el.memberDobDialogInput) {
      el.memberDobDialogInput.value = normalizedDob && normalizedDob !== "-" ? normalizedDob : "";
    }
    if (el.memberDobDialogSaveBtn) {
      el.memberDobDialogSaveBtn.disabled = false;
      el.memberDobDialogSaveBtn.textContent = "Save DOB";
    }
    el.memberDobDialog.showModal();
    if (el.memberDobDialogInput) {
      setTimeout(() => el.memberDobDialogInput.focus(), 20);
    }
  }

  async function saveMemberRole(memberId, roleValue) {
    const id = String(memberId || "").trim();
    if (!id) return;

    const role = String(roleValue || "")
      .trim()
      .toLowerCase();
    if (!["member", "admin", "accountant"].includes(role)) {
      throw new Error("Role must be member, admin, or accountant.");
    }

    const result = await apiRequest(`/api/super/members/${encodeURIComponent(id)}/role`, {
      method: "PATCH",
      body: { role },
    });
    toast(`Role updated to ${String(result?.member?.role || role).toUpperCase()}`, "success");
    await loadMembers();
  }

  function openMemberRoleDialog(member) {
    if (!el.memberRoleDialog || typeof el.memberRoleDialog.showModal !== "function") {
      toast("Role dialog is not available in this browser.", "error");
      return;
    }

    const id = String(member?.id || "").trim();
    if (!id) {
      toast("Invalid member selected.", "error");
      return;
    }

    const fullName = String(member?.fullName || "Member").trim() || "Member";
    const role = String(member?.role || "member").trim().toLowerCase() || "member";

    state.memberActionsMemberId = id;
    if (el.memberRoleDialogTitle) el.memberRoleDialogTitle.textContent = `Change role: ${fullName}`;
    if (el.memberRoleDialogMeta) el.memberRoleDialogMeta.textContent = "Choose one role and save.";
    if (el.memberRoleDialogSelect) el.memberRoleDialogSelect.value = role;
    if (el.memberRoleDialogSaveBtn) {
      el.memberRoleDialogSaveBtn.disabled = false;
      el.memberRoleDialogSaveBtn.textContent = "Save role";
    }
    el.memberRoleDialog.showModal();
    if (el.memberRoleDialogSelect) {
      setTimeout(() => el.memberRoleDialogSelect.focus(), 20);
    }
  }

  function memberById(memberId) {
    const id = String(memberId || "").trim();
    if (!id) return null;
    return state.memberRows.find((member) => member?.id === id) || null;
  }

  function clearMemberActionsDialogState() {
    state.memberActionsMemberId = "";
    if (el.memberActionsDeleteBtn) {
      el.memberActionsDeleteBtn.disabled = true;
      el.memberActionsDeleteBtn.setAttribute("title", "Select a member first.");
    }
  }

  function openMemberActionsDialog(member) {
    if (!el.memberActionsDialog || typeof el.memberActionsDialog.showModal !== "function") {
      toast("Member actions dialog is not available in this browser.", "error");
      return;
    }

    const id = String(member?.id || "").trim();
    if (!id) {
      toast("Invalid member selected.", "error");
      return;
    }

    const fullName = String(member?.fullName || "Member").trim() || "Member";
    const role = String(member?.role || "member").trim().toLowerCase() || "member";
    const roleLabel = role.toUpperCase();
    const phone = String(member?.phone || "").trim() || "-";
    const email = String(member?.email || "").trim() || "-";
    const inactiveLabel = formatInactiveFor(member?.inactiveSeconds || 0);
    const inactiveYears = Number(member?.inactiveYears || 0);
    const canDeleteForInactivity = role === "member" && inactiveYears >= 1;

    state.memberActionsMemberId = id;

    if (el.memberActionsTitle) el.memberActionsTitle.textContent = fullName;
    if (el.memberActionsMeta) {
      el.memberActionsMeta.textContent = `Phone: ${phone} | Email: ${email} | Inactive: ${inactiveLabel}`;
    }
    if (el.memberActionsRoleBadge) {
      el.memberActionsRoleBadge.textContent = roleLabel;
      el.memberActionsRoleBadge.dataset.role = role;
    }
    if (el.memberActionsDeleteBtn) {
      el.memberActionsDeleteBtn.disabled = !canDeleteForInactivity;
      if (canDeleteForInactivity) {
        el.memberActionsDeleteBtn.removeAttribute("title");
      } else if (role !== "member") {
        el.memberActionsDeleteBtn.setAttribute("title", "Only role=member can be deleted.");
      } else {
        el.memberActionsDeleteBtn.setAttribute("title", "Member must be inactive for at least 1 year.");
      }
    }

    el.memberActionsDialog.showModal();
  }

  function resetMemberEditDialogState() {
    state.memberEditMemberId = "";
    state.memberEditOriginal = null;
    if (el.memberEditDialogForm && typeof el.memberEditDialogForm.reset === "function") {
      el.memberEditDialogForm.reset();
    }
    if (el.memberEditSaveBtn) {
      el.memberEditSaveBtn.disabled = false;
      el.memberEditSaveBtn.textContent = "Save changes";
    }
    if (el.memberEditClearChurchBtn) {
      el.memberEditClearChurchBtn.disabled = false;
      el.memberEditClearChurchBtn.removeAttribute("title");
    }
  }

  function openMemberEditDialog(member) {
    if (!el.memberEditDialog || typeof el.memberEditDialog.showModal !== "function") {
      toast("Edit dialog is not available in this browser.", "error");
      return;
    }

    const id = String(member?.id || "").trim();
    if (!id) {
      toast("Invalid member selected.", "error");
      return;
    }

    const fullName = String(member?.fullName || "").trim();
    const phone = String(member?.phone || "").trim();
    const email = String(member?.email || "").trim();
    const churchJoinCode = String(member?.churchJoinCode || "").trim();
    const churchName = String(member?.churchName || "").trim();
    const role = String(member?.role || "member").trim().toLowerCase();
    const roleLabel = role ? role.toUpperCase() : "MEMBER";

    state.memberEditMemberId = id;
    state.memberEditOriginal = {
      fullName,
      phone,
      email,
      churchJoinCode,
      role,
    };

    if (el.memberEditDialogTitle) {
      el.memberEditDialogTitle.textContent = `Edit ${fullName || "member"}`;
    }
    if (el.memberEditDialogMeta) {
      if (churchName) {
        el.memberEditDialogMeta.textContent = `Current church: ${churchName}${churchJoinCode ? ` (${churchJoinCode})` : ""}`;
      } else {
        el.memberEditDialogMeta.textContent = "No church currently assigned.";
      }
    }
    if (el.memberEditRoleChip) {
      el.memberEditRoleChip.textContent = roleLabel;
      el.memberEditRoleChip.dataset.role = role || "member";
    }

    if (el.memberEditFullName) el.memberEditFullName.value = fullName;
    if (el.memberEditPhone) el.memberEditPhone.value = phone;
    if (el.memberEditEmail) el.memberEditEmail.value = email;
    if (el.memberEditChurchJoinCode) el.memberEditChurchJoinCode.value = churchJoinCode;
    if (el.memberEditRoleReadOnly) el.memberEditRoleReadOnly.value = roleLabel;

    const roleRequiresChurch = role === "admin" || role === "accountant";
    if (el.memberEditClearChurchBtn) {
      el.memberEditClearChurchBtn.disabled = roleRequiresChurch;
      if (roleRequiresChurch) {
        el.memberEditClearChurchBtn.title = "Admin/accountant must remain assigned to a church.";
      } else {
        el.memberEditClearChurchBtn.removeAttribute("title");
      }
    }

    el.memberEditDialog.showModal();
    if (el.memberEditFullName) {
      setTimeout(() => {
        el.memberEditFullName.focus();
        el.memberEditFullName.select();
      }, 20);
    }
  }

  async function saveMemberDetailsFromDialog() {
    const id = String(state.memberEditMemberId || "").trim();
    const original = state.memberEditOriginal || {};
    if (!id) {
      toast("No member selected.", "error");
      return;
    }

    const fullName = String(el.memberEditFullName?.value || "").trim();
    const phone = String(el.memberEditPhone?.value || "").trim();
    const email = String(el.memberEditEmail?.value || "").trim();
    const joinCodeInput = String(el.memberEditChurchJoinCode?.value || "").trim();

    if (!fullName) {
      toast("Full name is required.", "error");
      return;
    }

    const body = {};
    if (fullName !== String(original.fullName || "")) body.fullName = fullName;
    if (phone !== String(original.phone || "")) body.phone = phone || null;

    const originalEmailLower = String(original.email || "").trim().toLowerCase();
    const emailLower = email.toLowerCase();
    if (emailLower !== originalEmailLower) body.email = email || null;

    if (joinCodeInput) {
      const normalizedJoinCode = joinCodeInput.toUpperCase();
      if (normalizedJoinCode === "NONE") {
        body.churchJoinCode = null;
      } else if (normalizedJoinCode !== String(original.churchJoinCode || "").toUpperCase()) {
        body.churchJoinCode = normalizedJoinCode;
      }
    }

    if (!Object.keys(body).length) {
      toast("No changes detected.", "info");
      return;
    }

    if (el.memberEditSaveBtn) {
      el.memberEditSaveBtn.disabled = true;
      el.memberEditSaveBtn.textContent = "Saving...";
    }

    try {
      await apiRequest(`/api/super/members/${encodeURIComponent(id)}`, {
        method: "PATCH",
        body,
      });
      toast("Member details updated", "success");
      el.memberEditDialog?.close("ok");
      await loadMembers();
    } catch (err) {
      toast(err?.message || "Failed to update member details", "error");
    } finally {
      if (el.memberEditSaveBtn) {
        el.memberEditSaveBtn.disabled = false;
        el.memberEditSaveBtn.textContent = "Save changes";
      }
    }
  }

  async function deleteInactiveMember(member) {
    const id = String(member?.id || "").trim();
    if (!id) return;

    const role = String(member?.role || "member").trim().toLowerCase();
    const fullName = String(member?.fullName || "this member").trim();
    const inactiveYears = Number(member?.inactiveYears || 0);
    const inactiveLabel = formatInactiveFor(member?.inactiveSeconds || 0);

    if (role !== "member") {
      toast("Only role=member can be deleted here.", "error");
      return;
    }
    if (!Number.isFinite(inactiveYears) || inactiveYears < 1) {
      toast("Member must be inactive for at least 1 year before deletion.", "error");
      return;
    }

    const confirmed = window.confirm(
      `Delete ${fullName}?\n\nThis action is only allowed for inactive members with no payment history.\nInactive for: ${inactiveLabel}`
    );
    if (!confirmed) return;

    try {
      await apiRequest(`/api/super/members/${encodeURIComponent(id)}?minInactiveYears=1`, {
        method: "DELETE",
      });
      toast("Inactive member deleted", "success");
      await loadMembers();
    } catch (err) {
      toast(err?.message || "Failed to delete member", "error");
    }
  }

  async function loadMembers() {
    renderSkeletonRows(el.membersBody, 10, 8);
    const query = {
      search: el.memberSearchInput?.value || "",
      role: el.memberRoleSelect?.value || "",
      churchId: el.memberChurchSelect?.value || "",
      limit: 100,
      offset: 0,
    };

    const data = await apiRequest("/api/super/members" + buildQuery(query));
    const rows = Array.isArray(data.members) ? data.members : [];
    state.memberRows = rows;

    if (!rows.length) {
      renderEmpty(el.membersBody, 10, "No members found.");
      return;
    }

    el.membersBody.innerHTML = rows
      .map((member) => {
        const role = String(member.role || "member").toUpperCase();
        const rawDateOfBirth =
          typeof member.dateOfBirth === "string"
            ? member.dateOfBirth
            : (typeof member.date_of_birth === "string" ? member.date_of_birth : "");
        const dateOfBirth = formatDateOfBirth(rawDateOfBirth);
        const lastActiveAt = member.lastActiveAt || member.lastSeenAt || null;
        const inactiveSeconds = Number(member.inactiveSeconds || 0);
        const inactiveYears = Number(member.inactiveYears || 0);
        const canDeleteForInactivity = String(member.role || "").toLowerCase() === "member" && inactiveYears >= 1;
        const deleteDisabledTitle =
          String(member.role || "").toLowerCase() !== "member"
            ? "Only role=member can be deleted."
            : "Member must be inactive for at least 1 year.";
        return `
          <tr>
            <td>${escapeHtml(member.fullName || "-")}</td>
            <td>${escapeHtml(member.phone || "-")}</td>
            <td>${escapeHtml(member.email || "-")}</td>
            <td>${escapeHtml(dateOfBirth || "-")}</td>
            <td>${escapeHtml(member.churchName || "-")}</td>
            <td><span class="badge ${role === "MEMBER" ? "pending" : "active"}">${escapeHtml(role)}</span></td>
            <td>${escapeHtml(lastActiveAt ? formatDate(lastActiveAt) : "-")}</td>
            <td><span class="badge ${inactivityBadgeClass(inactiveSeconds)}">${escapeHtml(formatInactiveFor(inactiveSeconds))}</span></td>
            <td>${escapeHtml(formatDate(member.createdAt))}</td>
            <td class="actions-cell member-actions-cell">
              <button
                class="btn ghost member-manage-btn"
                type="button"
                data-member-action="manage"
                data-id="${escapeHtml(member.id)}"
                data-can-delete="${canDeleteForInactivity ? "1" : "0"}"
                title="${canDeleteForInactivity ? "Open member actions" : escapeHtml(deleteDisabledTitle)}"
              >
                Manage
              </button>
            </td>
          </tr>
        `;
      })
      .join("");
  }

  async function loadLegalDocuments() {
    if (el.legalDocsMeta) el.legalDocsMeta.textContent = "Loading legal documents...";
    if (el.legalDocsBody) renderSkeletonRows(el.legalDocsBody, 3, 5);

    const data = await apiRequest("/api/super/legal-documents");
    const docs = Array.isArray(data.documents) ? data.documents : [];
    state.legalDocuments = docs;

    if (el.legalDocsMeta) {
      el.legalDocsMeta.textContent = docs.length ? `${docs.length} documents` : "No legal documents configured yet.";
    }

    if (!el.legalDocsBody) return;
    if (!docs.length) {
      renderEmpty(el.legalDocsBody, 5, "No legal documents found.");
      return;
    }

    el.legalDocsBody.innerHTML = docs
      .map((doc) => {
        return `
          <tr>
            <td><code>${escapeHtml(doc.key || "-")}</code></td>
            <td>${escapeHtml(doc.title || "-")}</td>
            <td>${escapeHtml(String(doc.version || "-"))}</td>
            <td>${escapeHtml(formatDate(doc.updatedAt))}</td>
            <td class="actions-cell">
              <button class="btn ghost" type="button" data-legal-action="edit" data-key="${escapeHtml(doc.key)}">Edit</button>
            </td>
          </tr>
        `;
      })
      .join("");
  }

  async function openLegalDocDialog(docKey) {
    const key = String(docKey || "").trim();
    if (!key) return;
    state.legalDocEditingKey = key;

    let documentData = null;
    try {
      const data = await apiRequest(`/api/super/legal-documents/${encodeURIComponent(key)}`);
      documentData = data.document || null;
    } catch (err) {
      // Allow creating/upserting new docs, but warn if not found.
      if (err?.status !== 404) throw err;
    }

    el.legalDocDialogTitle.textContent = `Edit: ${key}`;
    el.legalDocKeyInput.value = key;
    el.legalDocTitleInput.value = documentData?.title || "";
    el.legalDocBodyInput.value = documentData?.body || "";

    const metaParts = [];
    if (documentData?.version) metaParts.push(`Version ${documentData.version}`);
    if (documentData?.updatedAt) metaParts.push(`Updated ${formatDate(documentData.updatedAt)}`);
    if (documentData?.updatedBy) metaParts.push(`By ${documentData.updatedBy}`);
    el.legalDocDialogMeta.textContent = metaParts.length
      ? metaParts.join(" | ")
      : "Create or update website terms, privacy, and fee disclosures.";

    if (el.legalDocDialog && typeof el.legalDocDialog.showModal === "function") {
      el.legalDocDialog.showModal();
    }
  }

  async function saveLegalDocFromDialog() {
    const key = String(el.legalDocKeyInput?.value || state.legalDocEditingKey || "").trim();
    const title = String(el.legalDocTitleInput?.value || "").trim();
    const body = String(el.legalDocBodyInput?.value || "").trim();

    if (!key) {
      toast("Missing document key", "error");
      return;
    }
    if (!title) {
      toast("Title is required", "error");
      return;
    }
    if (!body) {
      toast("Body is required", "error");
      return;
    }

    await apiRequest(`/api/super/legal-documents/${encodeURIComponent(key)}`, {
      method: "PATCH",
      body: { title, body },
    });

    toast("Legal document saved", "success");
    el.legalDocDialog?.close("ok");
    await loadLegalDocuments();
  }

  async function loadSettings() {
    const data = await apiRequest("/api/super/settings");
    const settings = data.settings || {};
    const rate = settings.rateLimits || {};

    el.settingsEnvironment.textContent = settings.environment || "-";
    el.settingsWebhook.textContent = settings.webhooks?.payfastNotifyUrl || "Not configured";
    el.settingsRateLimits.textContent = `Global: ${rate.globalMax || "-"} / ${rate.globalWindowMs || "-"}ms | Auth: ${rate.authMax || "-"} / ${rate.authWindowMs || "-"}ms`;
    el.settingsMaintenance.checked = !!settings.maintenanceMode;

    await loadLegalDocuments();
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

      if (tab === "onboarding") {
        await loadOnboardingRequests();
        state.loaded.onboarding = true;
        return;
      }

      if (tab === "jobs") {
        await loadJobs();
        if (!state.selectedJobId) resetJobForm();
        state.loaded.jobs = true;
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
      const preference = currentThemePreference();
      if (preference === "system") {
        applyTheme(currentTheme() === "dark" ? "light" : "dark");
        return;
      }
      applyTheme("system");
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
      void refreshOnboardingPendingBadge();
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
        return;
      }

      if (action === "delete") {
        void deleteChurch(id);
      }
    });

    el.saveChurchAccountantAccessBtn?.addEventListener("click", () => {
      const churchId = String(state.selectedChurchId || "").trim();
      if (!churchId) {
        toast("Select a church first", "error");
        return;
      }

      const accountantTabs = readChurchAccountantTabsFromUi();
      if (!accountantTabs.length) {
        toast("Select at least one tab", "error");
        return;
      }

      void (async () => {
        try {
          await apiRequest(`/api/super/churches/${encodeURIComponent(churchId)}/admin-portal-settings`, {
            method: "PATCH",
            body: { accountantTabs },
          });
          toast("Accountant access saved", "success");
          await loadChurchDetail(churchId);
        } catch (err) {
          toast(err?.message || "Failed to save access", "error");
        }
      })();
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

    el.editChurchBankAccountsBtn?.addEventListener("click", () => {
      openChurchBankDialog();
    });

    el.churchBankDialogForm?.addEventListener("submit", (event) => {
      event.preventDefault();
      const submitter = event.submitter;
      if (!submitter || submitter.value !== "ok") {
        el.churchBankDialog?.close("cancel");
        return;
      }
      void (async () => {
        try {
          await saveChurchBankAccountsFromDialog();
          el.churchBankDialog?.close("ok");
        } catch (err) {
          toast(err?.message || "Failed to save bank accounts", "error");
        }
      })();
    });

    el.churchBankAddAccountBtn?.addEventListener("click", () => {
      if (!Array.isArray(state.churchBankAccountsDraft)) state.churchBankAccountsDraft = [];
      if (state.churchBankAccountsDraft.length >= 5) {
        toast("Maximum of 5 bank accounts", "error");
        return;
      }
      state.churchBankAccountsDraft.push(blankBankAccount(false));
      state.churchBankAccountsDraft = ensurePrimaryAccount(state.churchBankAccountsDraft);
      renderBankAccountsEditor(el.churchBankAccountsList, state.churchBankAccountsDraft, {
        primaryGroup: "churchBankPrimary",
      });
    });

    el.churchBankAccountsList?.addEventListener("input", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const field = target.getAttribute("data-bank-field");
      const indexRaw = target.getAttribute("data-bank-index");
      if (!field || indexRaw == null) return;
      const idx = Number(indexRaw);
      if (!Number.isFinite(idx) || idx < 0) return;
      if (!Array.isArray(state.churchBankAccountsDraft) || !state.churchBankAccountsDraft[idx]) return;
      const value = String(target.value || "");
      state.churchBankAccountsDraft[idx] = { ...state.churchBankAccountsDraft[idx], [field]: value };
    });

    el.churchBankAccountsList?.addEventListener("click", (event) => {
      const removeBtn = event.target.closest("button[data-bank-action='remove'][data-bank-index]");
      if (removeBtn) {
        const idx = Number(removeBtn.getAttribute("data-bank-index"));
        if (!Number.isFinite(idx) || idx < 0) return;
        if (!Array.isArray(state.churchBankAccountsDraft)) return;
        state.churchBankAccountsDraft.splice(idx, 1);
        if (!state.churchBankAccountsDraft.length) state.churchBankAccountsDraft = [blankBankAccount(true)];
        state.churchBankAccountsDraft = ensurePrimaryAccount(state.churchBankAccountsDraft);
        renderBankAccountsEditor(el.churchBankAccountsList, state.churchBankAccountsDraft, {
          primaryGroup: "churchBankPrimary",
        });
        return;
      }

      const primaryRadio = event.target.closest("input[data-bank-action='primary'][data-bank-index]");
      if (primaryRadio) {
        const idx = Number(primaryRadio.getAttribute("data-bank-index"));
        if (!Number.isFinite(idx) || idx < 0) return;
        if (!Array.isArray(state.churchBankAccountsDraft)) return;
        state.churchBankAccountsDraft = state.churchBankAccountsDraft.map((row, rowIdx) => ({
          ...row,
          isPrimary: rowIdx === idx,
        }));
        renderBankAccountsEditor(el.churchBankAccountsList, state.churchBankAccountsDraft, {
          primaryGroup: "churchBankPrimary",
        });
      }
    });

    el.applyOnboardingFiltersBtn?.addEventListener("click", () => {
      void loadOnboardingRequests();
    });

    el.onboardingBody?.addEventListener("click", (event) => {
      const actionEl = event.target.closest("button[data-onboarding-action][data-id]");
      if (!actionEl) return;
      const requestId = actionEl.getAttribute("data-id");
      const action = actionEl.getAttribute("data-onboarding-action");
      if (!requestId || !action) return;

      if (action === "view") {
        void loadOnboardingDetail(requestId);
        return;
      }
      if (action === "cipc") {
        void previewOnboardingDocument(requestId, "cipc");
        return;
      }
      if (action === "bank") {
        void previewOnboardingDocument(requestId, "bank");
        return;
      }
      if (action === "delete") {
        void deleteOnboardingRequest(requestId);
      }
    });

    el.onboardingDownloadCipcBtn?.addEventListener("click", () => {
      void previewOnboardingDocument(state.selectedOnboardingId, "cipc");
    });

    el.onboardingDownloadBankBtn?.addEventListener("click", () => {
      void previewOnboardingDocument(state.selectedOnboardingId, "bank");
    });

    el.onboardingEditBtn?.addEventListener("click", () => {
      openOnboardingEditDialog();
    });

    el.onboardingReplaceCipcBtn?.addEventListener("click", () => {
      void replaceOnboardingDocumentFromPicker("cipc");
    });

    el.onboardingReplaceBankBtn?.addEventListener("click", () => {
      void replaceOnboardingDocumentFromPicker("bank");
    });

    el.onboardingEditDialogForm?.addEventListener("submit", (event) => {
      event.preventDefault();
      const submitter = event.submitter;
      if (!submitter || submitter.value !== "ok") {
        el.onboardingEditDialog?.close("cancel");
        return;
      }
      void (async () => {
        try {
          await saveOnboardingEditsFromDialog();
          el.onboardingEditDialog?.close("ok");
        } catch (err) {
          toast(err?.message || "Failed to save onboarding changes", "error");
        }
      })();
    });

    el.onboardingAddBankAccountBtn?.addEventListener("click", () => {
      if (!state.onboardingEditDraft) state.onboardingEditDraft = { requestId: "", bankAccounts: [] };
      if (!Array.isArray(state.onboardingEditDraft.bankAccounts)) state.onboardingEditDraft.bankAccounts = [];
      if (state.onboardingEditDraft.bankAccounts.length >= 5) {
        toast("Maximum of 5 bank accounts", "error");
        return;
      }
      state.onboardingEditDraft.bankAccounts.push(blankBankAccount(false));
      state.onboardingEditDraft.bankAccounts = ensurePrimaryAccount(state.onboardingEditDraft.bankAccounts);
      renderBankAccountsEditor(el.onboardingBankAccountsList, state.onboardingEditDraft.bankAccounts, {
        primaryGroup: "onboardingBankPrimary",
      });
    });

    el.onboardingBankAccountsList?.addEventListener("input", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const field = target.getAttribute("data-bank-field");
      const indexRaw = target.getAttribute("data-bank-index");
      if (!field || indexRaw == null) return;
      const idx = Number(indexRaw);
      if (!Number.isFinite(idx) || idx < 0) return;
      if (!state.onboardingEditDraft || !Array.isArray(state.onboardingEditDraft.bankAccounts)) return;
      if (!state.onboardingEditDraft.bankAccounts[idx]) return;
      const value = String(target.value || "");
      state.onboardingEditDraft.bankAccounts[idx] = { ...state.onboardingEditDraft.bankAccounts[idx], [field]: value };
    });

    el.onboardingBankAccountsList?.addEventListener("click", (event) => {
      const removeBtn = event.target.closest("button[data-bank-action='remove'][data-bank-index]");
      if (removeBtn) {
        const idx = Number(removeBtn.getAttribute("data-bank-index"));
        if (!Number.isFinite(idx) || idx < 0) return;
        if (!state.onboardingEditDraft || !Array.isArray(state.onboardingEditDraft.bankAccounts)) return;
        state.onboardingEditDraft.bankAccounts.splice(idx, 1);
        if (!state.onboardingEditDraft.bankAccounts.length) state.onboardingEditDraft.bankAccounts = [blankBankAccount(true)];
        state.onboardingEditDraft.bankAccounts = ensurePrimaryAccount(state.onboardingEditDraft.bankAccounts);
        renderBankAccountsEditor(el.onboardingBankAccountsList, state.onboardingEditDraft.bankAccounts, {
          primaryGroup: "onboardingBankPrimary",
        });
        return;
      }

      const primaryRadio = event.target.closest("input[data-bank-action='primary'][data-bank-index]");
      if (primaryRadio) {
        const idx = Number(primaryRadio.getAttribute("data-bank-index"));
        if (!Number.isFinite(idx) || idx < 0) return;
        if (!state.onboardingEditDraft || !Array.isArray(state.onboardingEditDraft.bankAccounts)) return;
        state.onboardingEditDraft.bankAccounts = state.onboardingEditDraft.bankAccounts.map((row, rowIdx) => ({
          ...row,
          isPrimary: rowIdx === idx,
        }));
        renderBankAccountsEditor(el.onboardingBankAccountsList, state.onboardingEditDraft.bankAccounts, {
          primaryGroup: "onboardingBankPrimary",
        });
      }
    });

    el.onboardingApproveBtn?.addEventListener("click", () => {
      void approveOnboardingRequest(state.selectedOnboardingId);
    });

    el.onboardingRejectBtn?.addEventListener("click", () => {
      void rejectOnboardingRequest(state.selectedOnboardingId);
    });

    el.onboardingDeleteBtn?.addEventListener("click", () => {
      void deleteOnboardingRequest(state.selectedOnboardingId);
    });

    el.openCreateJobBtn?.addEventListener("click", () => {
      resetJobForm();
      el.jobTitleInput?.focus();
    });

    el.applyJobsFiltersBtn?.addEventListener("click", () => {
      void loadJobs();
    });

    el.jobsBody?.addEventListener("click", (event) => {
      const actionEl = event.target.closest("button[data-job-action][data-id]");
      if (!actionEl) return;
      const action = actionEl.getAttribute("data-job-action");
      const id = actionEl.getAttribute("data-id");
      if (!action || !id) return;

      if (action === "edit") {
        const job = selectedJobById(id);
        if (!job) {
          toast("Job advert not found", "error");
          return;
        }
        fillJobForm(job);
        return;
      }

      if (action === "status") {
        const nextStatus = actionEl.getAttribute("data-next-status");
        void updateJobStatus(id, nextStatus);
        return;
      }

      if (action === "delete") {
        void deleteJobAdvert(id);
      }
    });

    el.jobForm?.addEventListener("submit", (event) => {
      event.preventDefault();
      void (async () => {
        try {
          await saveJobFromForm();
        } catch (err) {
          console.error("[super/jobs] save error", err?.message || err, err?.stack);
          setJobFormMeta(err?.message || "Could not save job advert.", "error");
          toast(err?.message || "Could not save job advert", "error");
        }
      })();
    });

    el.resetJobFormBtn?.addEventListener("click", () => {
      resetJobForm();
    });

    el.deleteJobBtn?.addEventListener("click", () => {
      if (!state.selectedJobId) {
        toast("Select a job advert first", "error");
        return;
      }
      void deleteJobAdvert(state.selectedJobId);
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

    el.documentPreviewDialog?.addEventListener("close", () => {
      closeDocumentPreviewDialog();
    });

    el.memberEditClearChurchBtn?.addEventListener("click", () => {
      if (el.memberEditClearChurchBtn.disabled) return;
      if (el.memberEditChurchJoinCode) {
        el.memberEditChurchJoinCode.value = "NONE";
        el.memberEditChurchJoinCode.focus();
        el.memberEditChurchJoinCode.select();
      }
    });

    el.memberEditDialogForm?.addEventListener("submit", (event) => {
      event.preventDefault();
      const submitter = event.submitter;
      if (!submitter || submitter.value !== "ok") {
        el.memberEditDialog?.close("cancel");
        return;
      }
      void saveMemberDetailsFromDialog();
    });

    el.memberEditDialog?.addEventListener("close", () => {
      resetMemberEditDialogState();
    });

    el.memberActionsEditBtn?.addEventListener("click", () => {
      const member = memberById(state.memberActionsMemberId);
      if (!member) {
        toast("Member not found in current list. Refresh and try again.", "error");
        return;
      }
      el.memberActionsDialog?.close("edit");
      openMemberEditDialog(member);
    });

    el.memberActionsRoleBtn?.addEventListener("click", () => {
      const member = memberById(state.memberActionsMemberId);
      if (!member) {
        toast("Member not found in current list. Refresh and try again.", "error");
        return;
      }
      el.memberActionsDialog?.close("role");
      openMemberRoleDialog(member);
    });

    el.memberActionsDobBtn?.addEventListener("click", () => {
      const member = memberById(state.memberActionsMemberId);
      if (!member) {
        toast("Member not found in current list. Refresh and try again.", "error");
        return;
      }
      el.memberActionsDialog?.close("dob");
      openMemberDateOfBirthDialog(member);
    });

    el.memberActionsResetBtn?.addEventListener("click", () => {
      const member = memberById(state.memberActionsMemberId);
      if (!member) {
        toast("Member not found in current list. Refresh and try again.", "error");
        return;
      }
      el.memberActionsDialog?.close("reset");
      void requestMemberPasswordReset(member.id);
    });

    el.memberActionsDeleteBtn?.addEventListener("click", () => {
      const member = memberById(state.memberActionsMemberId);
      if (!member) {
        toast("Member not found in current list. Refresh and try again.", "error");
        return;
      }
      el.memberActionsDialog?.close("delete");
      void deleteInactiveMember(member);
    });

    el.memberActionsDialog?.addEventListener("close", () => {
      clearMemberActionsDialogState();
    });

    el.memberRoleDialogForm?.addEventListener("submit", (event) => {
      event.preventDefault();
      const submitter = event.submitter;
      if (!submitter || submitter.value !== "ok") {
        el.memberRoleDialog?.close("cancel");
        return;
      }

      void (async () => {
        const memberId = String(state.memberActionsMemberId || "").trim();
        const role = String(el.memberRoleDialogSelect?.value || "").trim().toLowerCase();
        if (!memberId) {
          toast("No member selected.", "error");
          return;
        }
        if (el.memberRoleDialogSaveBtn) {
          el.memberRoleDialogSaveBtn.disabled = true;
          el.memberRoleDialogSaveBtn.textContent = "Saving...";
        }
        try {
          await saveMemberRole(memberId, role);
          el.memberRoleDialog?.close("ok");
        } catch (err) {
          toast(err?.message || "Failed to update role", "error");
        } finally {
          if (el.memberRoleDialogSaveBtn) {
            el.memberRoleDialogSaveBtn.disabled = false;
            el.memberRoleDialogSaveBtn.textContent = "Save role";
          }
        }
      })();
    });

    el.memberDobDialogClearBtn?.addEventListener("click", () => {
      if (el.memberDobDialogInput) el.memberDobDialogInput.value = "";
    });

    el.memberDobDialogForm?.addEventListener("submit", (event) => {
      event.preventDefault();
      const submitter = event.submitter;
      if (!submitter || submitter.value !== "ok") {
        el.memberDobDialog?.close("cancel");
        return;
      }

      void (async () => {
        const memberId = String(state.memberActionsMemberId || "").trim();
        const dobValue = String(el.memberDobDialogInput?.value || "").trim();
        if (!memberId) {
          toast("No member selected.", "error");
          return;
        }
        if (el.memberDobDialogSaveBtn) {
          el.memberDobDialogSaveBtn.disabled = true;
          el.memberDobDialogSaveBtn.textContent = "Saving...";
        }
        try {
          await saveMemberDateOfBirth(memberId, dobValue);
          el.memberDobDialog?.close("ok");
        } catch (err) {
          toast(err?.message || "Failed to update date of birth", "error");
        } finally {
          if (el.memberDobDialogSaveBtn) {
            el.memberDobDialogSaveBtn.disabled = false;
            el.memberDobDialogSaveBtn.textContent = "Save DOB";
          }
        }
      })();
    });

    el.applyMemberFiltersBtn?.addEventListener("click", () => {
      void loadMembers();
    });

    el.membersBody?.addEventListener("click", (event) => {
      const actionEl = event.target.closest("button[data-member-action]");
      if (!actionEl) return;
      const action = actionEl.getAttribute("data-member-action");
      const id = actionEl.getAttribute("data-id");
      const member = memberById(id);
      if (!member) {
        toast("Member not found in current list. Refresh and try again.", "error");
        return;
      }
      if (action === "manage") {
        openMemberActionsDialog(member);
        return;
      }
      if (action === "edit") {
        openMemberEditDialog(member);
        return;
      }
      if (action === "role") {
        openMemberRoleDialog(member);
        return;
      }
      if (action === "dob") {
        openMemberDateOfBirthDialog(member);
        return;
      }
      if (action === "reset") {
        void requestMemberPasswordReset(id);
        return;
      }
      if (action === "delete") {
        void deleteInactiveMember(member);
      }
    });

    el.legalDocsBody?.addEventListener("click", (event) => {
      const actionEl = event.target.closest("button[data-legal-action][data-key]");
      if (!actionEl) return;
      const action = actionEl.getAttribute("data-legal-action");
      const key = actionEl.getAttribute("data-key");
      if (action === "edit") {
        void (async () => {
          try {
            await openLegalDocDialog(key);
          } catch (err) {
            toast(err?.message || "Failed to load document", "error");
          }
        })();
      }
    });

    el.legalDocDialogForm?.addEventListener("submit", (event) => {
      event.preventDefault();
      const submitter = event.submitter;
      if (!submitter || submitter.value !== "ok") {
        el.legalDocDialog?.close("cancel");
        return;
      }
      void (async () => {
        try {
          await saveLegalDocFromDialog();
        } catch (err) {
          toast(err?.message || "Failed to save document", "error");
        }
      })();
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
    applyTheme(window.localStorage.getItem(THEME_KEY) || "system");
    startSystemThemeSync();
    bindEvents();

    const authed = await ensureAuth();
    if (!authed) return;

    el.appView.classList.remove("hidden");

    try {
      await loadChurchOptions();
      await loadFundsOptions();
      await refreshOnboardingPendingBadge();

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
