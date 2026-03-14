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
    financeWeeks: 12,
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
    growthSubscription: null,
    operationsView: "overview",
    operationsGroupId: "",
    operationsGroupRows: [],
    operationsServiceRows: [],
    operationsChildrenRows: [],
    operationsChildrenCheckInRows: [],
    operationsChildrenService: null,
  };

  const TAB_PATHS = {
    dashboard: "/admin/home",
    finance: "/admin/finance/giving-analytics",
    transactions: "/admin/finance",
    statements: "/admin/finance/statements",
    funds: "/admin/finance/funds",
    qr: "/admin/finance/qr",
    members: "/admin/people",
    operations: "/admin/church-life",
    communications: "/admin/comms",
    settings: "/admin/setup",
  };

  const TAB_TITLE = {
    dashboard: "Home",
    finance: "Finance",
    growth: "Growth",
    transactions: "Transactions",
    statements: "Statements",
    funds: "Funds",
    qr: "QR Codes",
    members: "People",
    operations: "Church Life",
    communications: "Comms",
    settings: "Setup",
  };

  const CHURCH_LIFE_VIEWS = [
    "overview",
    "insights",
    "services",
    "followups",
    "prayer",
    "broadcasts",
    "volunteers",
    "groups",
    "children",
    "schedules",
  ];
  const CHURCH_LIFE_VIEW_TITLE = {
    overview: "Overview",
    insights: "Insights",
    services: "Services",
    followups: "Follow-ups",
    prayer: "Prayer",
    broadcasts: "Broadcasts",
    volunteers: "Volunteers",
    groups: "Groups",
    children: "Children",
    schedules: "Schedules",
  };
  const CHURCH_LIFE_VIEW_PANEL = {
    overview: "operationsViewOverview",
    insights: "operationsViewInsights",
    services: "operationsViewServices",
    followups: "operationsViewFollowups",
    prayer: "operationsViewPrayer",
    broadcasts: "operationsViewBroadcasts",
    volunteers: "operationsViewVolunteers",
    groups: "operationsViewGroups",
    children: "operationsViewChildren",
    schedules: "operationsViewSchedules",
  };

  const ADMIN_PORTAL_TABS = Object.keys(TAB_TITLE);
  const DEFAULT_ACCOUNTANT_TABS = ["dashboard", "transactions", "statements"];
  const ACCOUNTANT_TAB_LABELS = {
    dashboard: "Home",
    transactions: "Transactions",
    statements: "Statements",
    funds: "Funds",
    qr: "QR codes",
    members: "People",
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
    workspaceDock: $("workspaceDock"),
    workspaceDockLabel: $("workspaceDockLabel"),
    workspaceDockTitle: $("workspaceDockTitle"),
    workspaceDockActions: $("workspaceDockActions"),
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
    financeWeeksSelect: $("financeWeeksSelect"),
    refreshFinanceBtn: $("refreshFinanceBtn"),
    financeMeta: $("financeMeta"),
    financeThisMonthAmount: $("financeThisMonthAmount"),
    financePreviousMonthAmount: $("financePreviousMonthAmount"),
    financeAmountChange: $("financeAmountChange"),
    financeThisMonthDonors: $("financeThisMonthDonors"),
    financeDonorChange: $("financeDonorChange"),
    financeThisMonthTransactions: $("financeThisMonthTransactions"),
    financeTransactionChange: $("financeTransactionChange"),
    financeRecurringGross: $("financeRecurringGross"),
    financeWeeklyTrendBody: $("financeWeeklyTrendBody"),
    financeRecentBody: $("financeRecentBody"),

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
    refreshOperationsBtn: $("refreshOperationsBtn"),
    operationsTitle: $("operationsTitle"),
    operationsMeta: $("operationsMeta"),
    operationsFollowupsTitle: $("operationsFollowupsTitle"),
    operationsInsightsMeta: $("operationsInsightsMeta"),
    operationsServicesMeta: $("operationsServicesMeta"),
    createServiceBtn: $("createServiceBtn"),
    registerDoorPersonBtn: $("registerDoorPersonBtn"),
    operationsFollowupsMeta: $("operationsFollowupsMeta"),
    createFollowupBtn: $("createFollowupBtn"),
    runAutoFollowupsBtn: $("runAutoFollowupsBtn"),
    operationsVisitorsTitle: $("operationsVisitorsTitle"),
    operationsAutoFollowupsMeta: $("operationsAutoFollowupsMeta"),
    operationsAutoFollowupsBody: $("operationsAutoFollowupsBody"),
    operationsPrayerMeta: $("operationsPrayerMeta"),
    operationsBroadcastsMeta: $("operationsBroadcastsMeta"),
    sendBroadcastBtn: $("sendBroadcastBtn"),
    operationsBroadcastsTitle: $("operationsBroadcastsTitle"),
    operationsBroadcastAudiencesBody: $("operationsBroadcastAudiencesBody"),
    operationsMessagingAudiencesTitle: $("operationsMessagingAudiencesTitle"),
    operationsMessagingTemplatesTitle: $("operationsMessagingTemplatesTitle"),
    operationsBroadcastTemplatesBody: $("operationsBroadcastTemplatesBody"),
    operationsVolunteersMeta: $("operationsVolunteersMeta"),
    operationsSchedulesMeta: $("operationsSchedulesMeta"),
    createGroupBtn: $("createGroupBtn"),
    operationsGroupsMeta: $("operationsGroupsMeta"),
    operationsGroupsTotal: $("operationsGroupsTotal"),
    operationsGroupsActive: $("operationsGroupsActive"),
    operationsGroupsMembers: $("operationsGroupsMembers"),
    operationsGroupsMeetings: $("operationsGroupsMeetings"),
    operationsGroupsBody: $("operationsGroupsBody"),
    operationsGroupDetailMeta: $("operationsGroupDetailMeta"),
    operationsGroupMembersBody: $("operationsGroupMembersBody"),
    operationsGroupMeetingsBody: $("operationsGroupMeetingsBody"),
    operationsChildrenMeta: $("operationsChildrenMeta"),
    addChildProfileBtn: $("addChildProfileBtn"),
    walkInChildCheckinBtn: $("walkInChildCheckinBtn"),
    operationsChildrenProfiles: $("operationsChildrenProfiles"),
    operationsChildrenHousehold: $("operationsChildrenHousehold"),
    operationsChildrenWalkIns: $("operationsChildrenWalkIns"),
    operationsChildrenOpenCheckins: $("operationsChildrenOpenCheckins"),
    operationsChildrenBody: $("operationsChildrenBody"),
    operationsChildrenCheckinsBody: $("operationsChildrenCheckinsBody"),
    operationsAttendance: $("operationsAttendance"),
    operationsServicesToday: $("operationsServicesToday"),
    operationsFollowupsDue: $("operationsFollowupsDue"),
    operationsUrgentPrayer: $("operationsUrgentPrayer"),
    operationsTrendBody: $("operationsTrendBody"),
    operationsQueueBody: $("operationsQueueBody"),
    operationsInsightsAttendees: $("operationsInsightsAttendees"),
    operationsInsightsFirstTime: $("operationsInsightsFirstTime"),
    operationsInsightsReturning: $("operationsInsightsReturning"),
    operationsInsightsAttendanceDelta: $("operationsInsightsAttendanceDelta"),
    operationsInsightsParticipation: $("operationsInsightsParticipation"),
    operationsInsightsGiving: $("operationsInsightsGiving"),
    operationsInsightsTrendBody: $("operationsInsightsTrendBody"),
    operationsInsightsRiskBody: $("operationsInsightsRiskBody"),
    operationsServicesBody: $("operationsServicesBody"),
    operationsFollowupsOpen: $("operationsFollowupsOpen"),
    operationsFollowupsUrgent: $("operationsFollowupsUrgent"),
    operationsFollowupsMine: $("operationsFollowupsMine"),
    operationsFollowupsBody: $("operationsFollowupsBody"),
    operationsPrayerNew: $("operationsPrayerNew"),
    operationsPrayerInProgress: $("operationsPrayerInProgress"),
    operationsPrayerMine: $("operationsPrayerMine"),
    operationsPrayerClosed: $("operationsPrayerClosed"),
    operationsPrayerBody: $("operationsPrayerBody"),
    operationsBroadcastsTotal: $("operationsBroadcastsTotal"),
    operationsBroadcastsAudience: $("operationsBroadcastsAudience"),
    operationsBroadcastsSent: $("operationsBroadcastsSent"),
    operationsBroadcastsFailed: $("operationsBroadcastsFailed"),
    operationsBroadcastsBody: $("operationsBroadcastsBody"),
    operationsVolunteersOpenTerms: $("operationsVolunteersOpenTerms"),
    operationsVolunteersReviewsDue: $("operationsVolunteersReviewsDue"),
    operationsVolunteersHighLoad: $("operationsVolunteersHighLoad"),
    operationsVolunteersOverload: $("operationsVolunteersOverload"),
    operationsVolunteersBody: $("operationsVolunteersBody"),
    operationsVolunteerScheduleBody: $("operationsVolunteerScheduleBody"),
    operationsVolunteerMinistriesBody: $("operationsVolunteerMinistriesBody"),
    operationsVolunteerRolesBody: $("operationsVolunteerRolesBody"),
    operationsVolunteerReviewsBody: $("operationsVolunteerReviewsBody"),
    operationsVolunteerAuditBody: $("operationsVolunteerAuditBody"),
    operationsSchedulesToday: $("operationsSchedulesToday"),
    operationsSchedulesAssignments: $("operationsSchedulesAssignments"),
    operationsSchedulesServed: $("operationsSchedulesServed"),
    operationsSchedulesNoShow: $("operationsSchedulesNoShow"),
    operationsSchedulesBody: $("operationsSchedulesBody"),

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
    growthSubscriptionCard: $("growthSubscriptionCard"),
    growthSubscriptionStatusBadge: $("growthSubscriptionStatusBadge"),
    growthSubscriptionMeta: $("growthSubscriptionMeta"),
    growthPlanSelect: $("growthPlanSelect"),
    growthPlanHint: $("growthPlanHint"),
    requestGrowthActivationBtn: $("requestGrowthActivationBtn"),
    refreshGrowthSubscriptionBtn: $("refreshGrowthSubscriptionBtn"),

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

  function formatMoneyFromCents(value) {
    const cents = Number(value || 0);
    if (!Number.isFinite(cents)) return "R 0.00";
    return formatMoney(cents / 100);
  }

  function formatCount(value) {
    const n = Number(value || 0);
    if (!Number.isFinite(n)) return "0";
    return new Intl.NumberFormat("en-ZA").format(Math.round(n));
  }

  function formatDate(value) {
    if (!value) return "-";
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return String(value);
    return d.toLocaleString();
  }

  function formatDateOnly(value) {
    if (!value) return "-";
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return String(value);
    return d.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
  }

  function formatDateShort(value) {
    if (!value) return "-";
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return "-";
    return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
  }

  function toDateInputValue(value) {
    if (!value) return "";
    const direct = String(value).trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(direct)) return direct;
    const parsed = new Date(direct);
    if (Number.isNaN(parsed.getTime())) return "";
    return parsed.toISOString().slice(0, 10);
  }

  function toDateTimeLocalValue(value) {
    if (!value) return "";
    const parsed = new Date(String(value));
    if (Number.isNaN(parsed.getTime())) return "";
    const tzOffsetMs = parsed.getTimezoneOffset() * 60000;
    return new Date(parsed.getTime() - tzOffsetMs).toISOString().slice(0, 16);
  }

  function daysRemaining(value) {
    if (!value) return null;
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return null;
    return Math.ceil((d.getTime() - Date.now()) / 86400000);
  }

  function formatPercentChange(value) {
    const n = Number(value || 0);
    if (!Number.isFinite(n)) return "n/a";
    return `${n >= 0 ? "+" : ""}${n.toFixed(1)}%`;
  }

  function labelizeToken(value, fallback = "Unknown") {
    const text = String(value || "").trim();
    if (!text) return fallback;
    return text
      .toLowerCase()
      .split(/[_\s-]+/)
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(" ");
  }

  function parsePromptDateToIsoDateTime(value) {
    const text = String(value || "").trim();
    if (!text) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return `${text}T09:00:00`;
    if (/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}$/.test(text)) return text.replace(" ", "T") + ":00";
    return "";
  }

  function formatContactLine(...values) {
    return values
      .map((value) => String(value || "").trim())
      .filter(Boolean)
      .join(" | ");
  }

  async function safeWorkspaceRequest(read, fallback) {
    try {
      return await read();
    } catch (err) {
      const statusCode = Number(err?.status || 0);
      if (statusCode === 403 || statusCode === 404 || statusCode === 429 || statusCode === 503) {
        return Object.assign({}, fallback || {}, {
          unavailable: true,
          error: err?.message || "Unavailable right now.",
        });
      }
      throw err;
    }
  }

  function selectOperationsGroup(groupId, { refresh = false } = {}) {
    state.operationsGroupId = String(groupId || "").trim();
    if (state.currentTab !== "operations" || state.operationsView !== "groups" || !state.token) return;
    loadOperationsWorkspace({ view: "groups", force: refresh }).catch((err) => {
      handlePortalLoadError(err, "Could not load groups.");
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
    return [
      "admin",
      "super",
      "accountant",
      "finance",
      "pastor",
      "volunteer",
      "usher",
      "teacher",
      "prayer_team_lead",
    ].includes(r);
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

  function canUseFinanceWorkspace() {
    if (!state.allowedTabs || !state.allowedTabs.length) return true;
    return ["growth", "transactions", "statements", "funds", "qr"].some((tab) => state.allowedTabs.includes(tab));
  }

  function canUseChurchLifeWorkspace() {
    if (!state.allowedTabs || !state.allowedTabs.length) return true;
    return state.allowedTabs.includes("operations");
  }

  function firstAllowedTab() {
    const preferred = ["dashboard", "finance", "members", "operations", "settings"];
    const match = preferred.find((tab) => isTabAllowed(tab));
    return match || (state.allowedTabs && state.allowedTabs[0]) || "dashboard";
  }

  function isTabAllowed(tabName) {
    const key = String(tabName || "").trim().toLowerCase();
    if (!key) return false;
    if (!state.allowedTabs || !state.allowedTabs.length) return true;
    if (key === "finance") return canUseFinanceWorkspace();
    if (key === "operations" || key === "communications") return canUseChurchLifeWorkspace();
    return state.allowedTabs.includes(key);
  }

  function applyTabVisibility() {
    $$(".nav-link[data-tab]").forEach((btn) => {
      const tab = String(btn.getAttribute("data-tab") || "").trim().toLowerCase();
      const visible = !tab || isTabAllowed(tab);
      btn.classList.toggle("hidden", !visible);
    });

    const desired = isTabAllowed(state.currentTab) ? state.currentTab : firstAllowedTab();
    switchTab(desired, false);
  }

  function pathToTab(pathname) {
    const path = String(pathname || "/admin/").replace(/\/+$/, "") || "/admin";
    if (path === "/admin" || path === "/admin/home") return "dashboard";
    if (path === "/admin/finance/giving-analytics") return "finance";
    if (path === "/admin/finance") return "transactions";
    if (path === "/admin/finance/statements") return "statements";
    if (path === "/admin/finance/funds") return "funds";
    if (path === "/admin/finance/qr") return "qr";
    if (path === "/admin/people") return "members";
    if (path === "/admin/church-life") return "operations";
    if (path === "/admin/comms") return "communications";
    if (path === "/admin/setup") return "settings";
    return "dashboard";
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

  function normalizeChurchLifeView(viewName) {
    const key = String(viewName || "overview").trim().toLowerCase();
    return CHURCH_LIFE_VIEWS.includes(key) ? key : "overview";
  }

  function setOperationsShellMeta(viewName = state.operationsView) {
    if (el.operationsTitle) el.operationsTitle.textContent = "Church Life";
    if (el.refreshOperationsBtn) el.refreshOperationsBtn.textContent = "Refresh Church Life";
    if (!el.operationsMeta) return;

    const view = normalizeChurchLifeView(viewName);
    if (view === "overview") {
      el.operationsMeta.textContent = "Keep live ministry work visible without turning this into a wall of broken widgets.";
      return;
    }

    el.operationsMeta.textContent = `${CHURCH_LIFE_VIEW_TITLE[view]} keeps the live ministry flow in view without leaving Church Life.`;
  }

  function syncChurchLifeViewUi() {
    const currentView = normalizeChurchLifeView(state.operationsView);
    $$("[data-operations-view]").forEach((button) => {
      const isActive = normalizeChurchLifeView(button.getAttribute("data-operations-view")) === currentView;
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-selected", isActive ? "true" : "false");
    });

    $$(".operations-view").forEach((panel) => {
      panel.classList.toggle("hidden", panel.id !== CHURCH_LIFE_VIEW_PANEL[currentView]);
    });
  }

  function setOperationsView(viewName, { refresh = false } = {}) {
    state.operationsView = normalizeChurchLifeView(viewName);
    syncChurchLifeViewUi();
    setOperationsShellMeta(state.operationsView);

    if (state.currentTab !== "operations" || !state.token) return;
    loadOperationsWorkspace({ view: state.operationsView, force: refresh }).catch((err) => {
      handlePortalLoadError(err, `Could not load ${CHURCH_LIFE_VIEW_TITLE[state.operationsView] || "Church Life"}.`);
    });
  }

  function navTabFor(tabName) {
    if (["finance", "growth", "transactions", "statements", "funds", "qr"].includes(tabName)) return "finance";
    return tabName;
  }

  function kickerForTab(tabName) {
    const navTab = navTabFor(tabName);
    if (navTab === "dashboard") return "Admin Portal";
    if (navTab === "finance") return "Giving & Payments";
    if (navTab === "members") return "People & Roles";
    if (navTab === "operations") return "Church Life";
    if (navTab === "communications") return "Comms & Follow-up";
    if (navTab === "settings") return "Church Setup";
    return "Control Center";
  }

  function workspaceDockConfigForTab(tabName) {
    const navTab = navTabFor(tabName);
    if (navTab === "operations" || navTab === "communications") return null;

    if (navTab === "finance") {
      return {
        label: "Finance flow",
        title: "One path for analytics, transactions, statements, funds, and QR tools.",
        tabs: ["finance", "transactions", "statements", "funds", "qr"],
      };
    }

    return {
      label: "Church workspaces",
      title: "Move through the church portal without repeating shortcut tiles in every panel.",
      tabs: ["finance", "members", "operations", "settings"],
    };
  }

  function renderWorkspaceDock(tabName) {
    if (!el.workspaceDock || !el.workspaceDockActions || !el.workspaceDockLabel || !el.workspaceDockTitle) return;

    const config = workspaceDockConfigForTab(tabName);
    if (!config || !Array.isArray(config.tabs) || !config.tabs.length) {
      el.workspaceDock.classList.add("hidden");
      el.workspaceDockActions.innerHTML = "";
      return;
    }

    el.workspaceDock.classList.remove("hidden");
    el.workspaceDockLabel.textContent = config.label || "Workspace flow";
    el.workspaceDockTitle.textContent = config.title || "";
    el.workspaceDockActions.innerHTML = config.tabs
      .map((tab) => {
        const isActive = tab === tabName;
        return `
          <button
            class="btn ghost workspace-tab${isActive ? " is-active" : ""}"
            type="button"
            data-jump-tab="${escapeHtml(tab)}"
            aria-pressed="${isActive ? "true" : "false"}"
          >
            ${escapeHtml(TAB_TITLE[tab] || tab)}
          </button>
        `;
      })
      .join("");
  }

  function ensureStatementDateDefaults() {
    if (el.statementFromInput && !el.statementFromInput.value) el.statementFromInput.value = isoStartOfMonthLocal();
    if (el.statementToInput && !el.statementToInput.value) el.statementToInput.value = isoTodayLocal();
  }

  function handlePortalLoadError(err, fallbackMessage) {
    if (!err) return;
    if (err.status === 401) {
      void onLogout({ silent: true, reason: "Session expired. Please sign in again." });
      return;
    }
    const message = err?.message || fallbackMessage || "Could not load this view.";
    showInlineStatus(message, "error");
    toast(message, "error");
  }

  async function loadSettingsWorkspace() {
    const tasks = [loadGrowthSubscription()];
    if (isChurchAdminRole(state.profile?.role)) {
      tasks.push(loadPayfastStatus());
    } else {
      showPayfastSetupCard(false);
    }
    await Promise.all(tasks);
  }

  async function loadCurrentTabData(tabName) {
    if (tabName === "dashboard") {
      await loadDashboard();
      return;
    }
    if (tabName === "finance") {
      await loadFinanceWorkspace();
      return;
    }
    if (tabName === "transactions") {
      await loadTransactions();
      return;
    }
    if (tabName === "statements") {
      ensureStatementDateDefaults();
      await loadStatementSummary();
      return;
    }
    if (tabName === "funds") {
      await loadFunds();
      return;
    }
    if (tabName === "qr") {
      await loadFunds();
      return;
    }
    if (tabName === "members") {
      await loadMembers();
      return;
    }
    if (tabName === "operations") {
      await loadOperationsWorkspace();
      return;
    }
    if (tabName === "communications") {
      state.operationsView = "followups";
      syncChurchLifeViewUi();
      setOperationsShellMeta("followups");
      await loadOperationsWorkspace({ view: "followups" });
      return;
    }
    if (tabName === "settings") {
      await loadSettingsWorkspace();
    }
  }

  function switchTab(tabName, pushHistory = true) {
    const requested = String(tabName || "").trim().toLowerCase();
    const resolved = isTabAllowed(requested) ? requested : firstAllowedTab();
    const activePanelTab = resolved === "communications" ? "operations" : resolved;

    if (resolved === "communications") {
      state.operationsView = "followups";
    }

    state.currentTab = resolved;
    syncChurchLifeViewUi();
    setOperationsShellMeta(state.operationsView);

    const activeNavTab = navTabFor(resolved);
    $$(".nav-link[data-tab]").forEach((btn) => {
      const active = btn.getAttribute("data-tab") === activeNavTab;
      btn.classList.toggle("active", active);
    });

    $$(".panel[id^='panel-']").forEach((panel) => {
      panel.classList.toggle("hidden", panel.id !== `panel-${activePanelTab}`);
    });

    el.pageTitle.textContent = TAB_TITLE[resolved] || "Admin";
    el.pageKicker.textContent = kickerForTab(resolved);
    renderWorkspaceDock(resolved);

    if (pushHistory) {
      const nextPath = TAB_PATHS[resolved] || TAB_PATHS.dashboard;
      if (window.location.pathname !== nextPath) {
        window.history.pushState({ tab: resolved }, "", nextPath);
      }
    }

    setSidebarOpen(false);
    showInlineStatus("");

    if (!state.token) return;

    loadCurrentTabData(resolved).catch((err) => {
      handlePortalLoadError(err, `Could not load ${TAB_TITLE[resolved] || "this view"}.`);
    });
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

  function showGrowthSubscriptionCard(show) {
    if (!el.growthSubscriptionCard) return;
    el.growthSubscriptionCard.classList.toggle("hidden", !show);
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
      state.portalSettings = Object.assign({ accountantTabs: [], churchOperations: null }, data?.settings || {}, {
        churchOperations: data?.churchOperations || null,
      });

      applyTabVisibility();

      const canEdit = isChurchAdminRole(state.profile?.role);
      showAccountantAccessCard(canEdit);
      showPayfastSetupCard(canEdit);
      showGrowthSubscriptionCard(true);
      if (canEdit) {
        setAccountantTabsInUi(state.portalSettings?.accountantTabs || []);
      }
      if (data?.churchOperations) {
        renderGrowthSubscription({ subscription: data.churchOperations, hasAccess: data.churchOperations?.hasAccess });
      }
    } catch (err) {
      // Fallback for early bootstrap or older deploys.
      setAllowedTabs(ADMIN_PORTAL_TABS);
      state.portalSettings = { accountantTabs: [], churchOperations: null };
      applyTabVisibility();

      // If church isn't linked yet, hide the config card to avoid confusion.
      const msg = String(err?.message || "");
      if (msg.toLowerCase().includes("join a church")) {
        showAccountantAccessCard(false);
        showPayfastSetupCard(false);
        showGrowthSubscriptionCard(false);
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

  function financeSummaryText(currentValue, previousValue, pct, previousLabel) {
    const current = Number(currentValue || 0);
    const previous = Number(previousValue || 0);
    if (!Number.isFinite(current) || !Number.isFinite(previous)) return "No comparison available yet.";
    if (current === 0 && previous === 0) return "No movement yet.";
    if (previous === 0 && current > 0) return `New growth vs ${previousLabel || "last month"}.`;
    return `${formatPercentChange(pct)} vs ${previousLabel || "last month"}`;
  }

  function renderFinanceWorkspace(growthData, recentData) {
    const overview = growthData?.overview || {};
    const labels = growthData?.labels || {};
    const weeklyTrend = Array.isArray(growthData?.weeklyTrend) ? growthData.weeklyTrend : [];
    const recentRows = Array.isArray(recentData?.transactions) ? recentData.transactions : [];

    if (el.financeWeeksSelect) el.financeWeeksSelect.value = String(state.financeWeeks || 12);
    if (el.financeThisMonthAmount) el.financeThisMonthAmount.textContent = formatMoney(overview.thisMonthAmount || 0);
    if (el.financePreviousMonthAmount) el.financePreviousMonthAmount.textContent = formatMoney(overview.previousMonthAmount || 0);
    if (el.financeThisMonthDonors) el.financeThisMonthDonors.textContent = formatCount(overview.thisMonthDonors || 0);
    if (el.financeThisMonthTransactions) {
      el.financeThisMonthTransactions.textContent = formatCount(overview.thisMonthTransactions || 0);
    }

    if (el.financeAmountChange) {
      el.financeAmountChange.textContent = financeSummaryText(
        overview.thisMonthAmount,
        overview.previousMonthAmount,
        overview.amountChangePct,
        labels.previousMonth
      );
    }
    if (el.financeDonorChange) {
      el.financeDonorChange.textContent = financeSummaryText(
        overview.thisMonthDonors,
        overview.previousMonthDonors,
        overview.donorChangePct,
        labels.previousMonth
      );
    }
    if (el.financeTransactionChange) {
      el.financeTransactionChange.textContent = financeSummaryText(
        overview.thisMonthTransactions,
        overview.previousMonthTransactions,
        overview.transactionChangePct,
        labels.previousMonth
      );
    }

    if (el.financeRecurringGross) {
      const recurringCount = Number(overview.activeRecurringCount || 0);
      const recurringAmount = formatMoney(overview.monthlyRecurringGross || 0);
      el.financeRecurringGross.textContent = recurringCount
        ? `Recurring monthly: ${recurringAmount} across ${formatCount(recurringCount)} active plan${
            recurringCount === 1 ? "" : "s"
          }.`
        : `Recurring monthly: ${recurringAmount}.`;
    }

    if (el.financeWeeklyTrendBody) {
      if (!weeklyTrend.length) {
        renderEmpty(
          el.financeWeeklyTrendBody,
          3,
          growthData?.unavailable
            ? growthData?.error || "Finance analytics are not available right now."
            : "No weekly giving trend yet."
        );
      } else {
        el.financeWeeklyTrendBody.innerHTML = weeklyTrend
          .map((row) => `
            <tr>
              <td>${escapeHtml(formatDateOnly(row.weekStart))}</td>
              <td>${escapeHtml(formatMoney(row.amount || 0))}</td>
              <td>${escapeHtml(formatCount(row.transactionCount || 0))}</td>
            </tr>
          `)
          .join("");
      }
    }

    if (el.financeRecentBody) {
      const recentVisible = recentRows
        .filter((row) => {
          const status = parseTransactionStatus(row);
          return status === "PAID" || status === "CONFIRMED" || status === "RECORDED" || status === "PREPARED";
        })
        .slice(0, 8);
      if (!recentVisible.length) {
        renderEmpty(
          el.financeRecentBody,
          4,
          recentData?.unavailable
            ? recentData?.error || "Recent finance activity is unavailable right now."
            : "No recent finance activity yet."
        );
      } else {
        el.financeRecentBody.innerHTML = recentVisible
          .map((row) => {
            const status = parseTransactionStatus(row);
            return `
              <tr>
                <td>${escapeHtml(row.reference || "-")}</td>
                <td>${escapeHtml(formatMoney(row.amount || 0))}</td>
                <td><span class="badge ${statusBadgeClass(status)}">${escapeHtml(status)}</span></td>
                <td>${escapeHtml(formatDate(row.createdAt || row.created_at))}</td>
              </tr>
            `;
          })
          .join("");
      }
    }

    if (el.financeMeta) {
      const parts = [];
      if (!growthData?.unavailable) {
        parts.push(
          labels.thisMonth && labels.previousMonth
            ? `${labels.thisMonth} compared with ${labels.previousMonth}.`
            : `Showing the last ${formatCount(state.financeWeeks || 12)} weeks of giving.`
        );
      } else {
        parts.push(growthData?.error || "Advanced growth analytics are unavailable right now.");
      }
      if (recentData?.unavailable) {
        parts.push("Recent transaction activity is limited on this deploy.");
      } else {
        parts.push(`${formatCount(recentRows.length)} recent finance records loaded.`);
      }
      el.financeMeta.textContent = parts.join(" ");
    }
  }

  async function loadFinanceWorkspace() {
    if (el.financeMeta) el.financeMeta.textContent = "Loading finance workspace...";
    renderSkeletonRows(el.financeWeeklyTrendBody, 3, 6);
    renderSkeletonRows(el.financeRecentBody, 4, 6);

    const [growthData, recentData] = await Promise.all([
      safeWorkspaceRequest(
        () => apiRequest("/api/admin/reports/digital-growth" + buildQuery({ weeks: state.financeWeeks || 12 })),
        { overview: {}, labels: {}, weeklyTrend: [] }
      ),
      safeWorkspaceRequest(
        () => apiRequest("/api/admin/dashboard/transactions/recent" + buildQuery({ limit: 8, offset: 0 })),
        { transactions: [], meta: { count: 0, returned: 0, limit: 8, offset: 0 } }
      ),
    ]);

    renderFinanceWorkspace(growthData, recentData);
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

  function workspaceBadgeClass(value) {
    const key = String(value || "").trim().toUpperCase();
    if (!key) return "pending";
    if (["ACTIVE", "CONFIRMED", "DONE", "SENT", "PAID", "COMPLETED", "SERVED", "LOW"].includes(key)) return "paid";
    if (["OPEN", "BOOKED", "TRIALING", "PENDING", "QUEUED", "DRAFT", "IN_PROGRESS", "ASSIGNED", "NEW", "MEDIUM", "HIGH"].includes(key)) {
      return "pending";
    }
    return "failed";
  }

  function growthPlanLabel(planCode) {
    return normalizeGrowthPlanCode(planCode) === "GROWTH_ANNUAL" ? "Growth Annual" : "Growth Monthly";
  }

  function growthPlanHint(planCode) {
    return normalizeGrowthPlanCode(planCode) === "GROWTH_ANNUAL"
      ? "Annual billing selected: R 4,990 per year."
      : "Monthly billing selected: R 499 per month.";
  }

  function normalizeGrowthPlanCode(value) {
    const key = String(value || "").trim().toUpperCase();
    if (key === "GROWTH_ANNUAL" || key === "CHURPAY_GROWTH_ANNUAL_4990") return "GROWTH_ANNUAL";
    return "GROWTH_MONTHLY";
  }

  function inferGrowthNextAction(status) {
    const key = String(status || "").trim().toUpperCase();
    if (key === "ACTIVE") return "NONE";
    if (key === "PAST_DUE" || key === "GRACE") return "UPDATE_PAYMENT";
    if (key === "SUSPENDED" || key === "CANCELED") return "RENEW";
    return "ACTIVATE";
  }

  function normalizeGrowthSubscription(raw) {
    const payload = raw && raw.subscription ? raw : { subscription: raw || null };
    const subscription = payload?.subscription && typeof payload.subscription === "object" ? payload.subscription : {};
    const status = String(payload?.status || subscription?.status || "SUSPENDED").trim().toUpperCase();
    const planCode = normalizeGrowthPlanCode(payload?.planCode || subscription?.planCode || subscription?.plan);
    return {
      hasAccess: typeof payload?.hasAccess === "boolean" ? payload.hasAccess : !!subscription?.hasAccess,
      nextAction: String(payload?.nextAction || inferGrowthNextAction(status)).trim().toUpperCase(),
      trialDaysRemaining:
        payload?.trialDaysRemaining == null ? null : Math.max(0, Number(payload.trialDaysRemaining || 0)),
      graceDaysRemaining:
        payload?.graceDaysRemaining == null ? null : Math.max(0, Number(payload.graceDaysRemaining || 0)),
      checkoutUrl: String(payload?.checkoutUrl || "").trim(),
      checkoutWarning: String(payload?.checkoutWarning || "").trim(),
      unavailable: !!payload?.unavailable,
      error: String(payload?.error || "").trim(),
      planCode,
      status,
      currentPeriodEnd: subscription?.currentPeriodEnd || subscription?.current_period_end || null,
      trialEndsAt: subscription?.trialEndsAt || subscription?.trial_ends_at || null,
      graceEndsAt: subscription?.graceEndsAt || subscription?.grace_ends_at || null,
      accessLevel: subscription?.accessLevel || subscription?.access_level || "",
      banner: subscription?.banner || "",
      note: subscription?.note || "",
    };
  }

  function growthStatusLabel(subscription) {
    const status = String(subscription?.status || "SUSPENDED").trim().toUpperCase();
    if (status === "ACTIVE") return "Active";
    if (status === "TRIALING") return "Trial";
    if (status === "PAST_DUE") return "Past due";
    if (status === "GRACE") return "Grace";
    if (status === "CANCELED") return "Canceled";
    return "Locked";
  }

  function growthStatusBadgeClass(subscription) {
    const status = String(subscription?.status || "SUSPENDED").trim().toUpperCase();
    if (status === "ACTIVE" || status === "TRIALING") return "active";
    if (status === "PAST_DUE" || status === "GRACE") return "pending";
    return "inactive";
  }

  function syncGrowthPlanHint() {
    if (!el.growthPlanHint) return;
    const planCode = normalizeGrowthPlanCode(el.growthPlanSelect?.value || state.growthSubscription?.planCode);
    el.growthPlanHint.textContent = growthPlanHint(planCode);
  }

  function renderGrowthSubscription(raw) {
    const normalized = normalizeGrowthSubscription(raw);
    state.growthSubscription = normalized;
    showGrowthSubscriptionCard(true);

    if (el.growthPlanSelect) {
      el.growthPlanSelect.value = normalized.planCode;
      el.growthPlanSelect.disabled = !isChurchAdminRole(state.profile?.role);
    }
    syncGrowthPlanHint();

    if (el.growthSubscriptionStatusBadge) {
      el.growthSubscriptionStatusBadge.className = `badge ${growthStatusBadgeClass(normalized)}`;
      el.growthSubscriptionStatusBadge.textContent = growthStatusLabel(normalized);
    }

    if (el.growthSubscriptionMeta) {
      const parts = [`Plan: ${growthPlanLabel(normalized.planCode)}.`];
      if (normalized.unavailable) {
        parts.push(normalized.error || "Growth subscription status is unavailable right now.");
      } else if (normalized.status === "ACTIVE" && normalized.currentPeriodEnd) {
        parts.push(`Active through ${formatDateOnly(normalized.currentPeriodEnd)}.`);
      } else if (normalized.status === "TRIALING") {
        parts.push(
          normalized.trialDaysRemaining != null
            ? `${formatCount(normalized.trialDaysRemaining)} day${Number(normalized.trialDaysRemaining) === 1 ? "" : "s"} left in trial.`
            : "Trial is active."
        );
      } else if ((normalized.status === "PAST_DUE" || normalized.status === "GRACE") && normalized.graceDaysRemaining != null) {
        parts.push(
          `${formatCount(normalized.graceDaysRemaining)} day${Number(normalized.graceDaysRemaining) === 1 ? "" : "s"} left before lock.`
        );
      } else if (!normalized.hasAccess) {
        parts.push("Church Life tools stay locked until activation is complete.");
      }
      if (normalized.checkoutWarning) parts.push(normalized.checkoutWarning);
      el.growthSubscriptionMeta.textContent = parts.join(" ");
    }

    if (el.requestGrowthActivationBtn) {
      const nextAction = normalized.nextAction || inferGrowthNextAction(normalized.status);
      let label = "Activate ChurPay Growth";
      if (nextAction === "PAYFAST_CHECKOUT") label = "Continue activation";
      if (nextAction === "UPDATE_PAYMENT") label = "Update Growth payment";
      if (nextAction === "RENEW") label = "Renew ChurPay Growth";
      if (nextAction === "NONE") label = "Growth is active";
      el.requestGrowthActivationBtn.textContent = label;
      el.requestGrowthActivationBtn.disabled = !isChurchAdminRole(state.profile?.role) || nextAction === "NONE";
    }
  }

  async function loadGrowthSubscription() {
    showGrowthSubscriptionCard(true);
    try {
      const data = await safeWorkspaceRequest(() => apiRequest("/api/admin/church-operations/subscription"), {
        subscription: state.portalSettings?.churchOperations || state.growthSubscription || null,
      });
      renderGrowthSubscription(data);
    } catch (err) {
      const message = String(err?.message || "");
      if (message.toLowerCase().includes("join a church")) {
        showGrowthSubscriptionCard(false);
        return;
      }
      throw err;
    }
  }

  async function requestGrowthActivation() {
    if (!isChurchAdminRole(state.profile?.role)) {
      toast("Only church admins can activate ChurPay Growth.", "error");
      return;
    }

    const button = el.requestGrowthActivationBtn;
    const previousLabel = button?.textContent || "Activate ChurPay Growth";
    if (button) {
      button.disabled = true;
      button.textContent = "Working...";
    }

    try {
      const data = await apiRequest("/api/admin/church-operations/subscription/request", {
        method: "POST",
        body: { planCode: normalizeGrowthPlanCode(el.growthPlanSelect?.value || state.growthSubscription?.planCode) },
      });
      renderGrowthSubscription(data);

      if (data?.checkoutUrl) {
        const popup = window.open(data.checkoutUrl, "_blank", "noopener,noreferrer");
        if (!popup) window.location.assign(data.checkoutUrl);
        toast("Growth checkout opened. Finish payment to unlock Church Life.", "success", 5200);
        return;
      }

      if (data?.alreadyActive) {
        toast("ChurPay Growth is already active.", "info");
      } else if (data?.trialStarted) {
        toast("ChurPay Growth trial started.", "success");
      } else if (data?.checkoutWarning) {
        toast(data.checkoutWarning, "info", 5200);
      } else {
        toast("Growth activation request saved.", "success");
      }
    } catch (err) {
      toast(err?.message || "Could not activate ChurPay Growth.", "error");
    } finally {
      if (button) {
        button.disabled = false;
        button.textContent = previousLabel;
      }
      if (state.growthSubscription) renderGrowthSubscription(state.growthSubscription);
    }
  }

  function calculatePercentDelta(currentValue, previousValue) {
    const current = Number(currentValue || 0);
    const previous = Number(previousValue || 0);
    if (!Number.isFinite(current) || !Number.isFinite(previous) || previous === 0) return null;
    return ((current - previous) / Math.abs(previous)) * 100;
  }

  function riskBadgeClass(value) {
    const key = String(value || "").trim().toUpperCase();
    if (!key) return "pending";
    if (key === "LOW") return "paid";
    if (key === "MEDIUM") return "pending";
    return "failed";
  }

  function churchLifeServiceStatus(row) {
    if (row?.published === false) return { label: "Draft", tone: "DRAFT" };
    if (row?.checkInOpen) return { label: "Check-in open", tone: "ACTIVE" };
    if (row?.isClosed) return { label: "Closed", tone: "PENDING" };
    return { label: "Published", tone: "ACTIVE" };
  }

  function isDoneLikeStatus(value) {
    const key = String(value || "").trim().toUpperCase();
    return ["DONE", "CLOSED", "COMPLETED", "CANCELLED", "CANCELED"].includes(key);
  }

  function renderOperationsOverview(overviewData, followupsData) {
    const attention = overviewData?.attention || {};
    const snapshot = overviewData?.snapshot || {};
    const trendWeeks = Array.isArray(overviewData?.trend?.weeks) ? overviewData.trend.weeks : [];
    const followups = Array.isArray(followupsData?.followups) ? followupsData.followups : [];

    if (el.operationsAttendance) el.operationsAttendance.textContent = formatCount(snapshot.attendanceThisSunday || 0);
    if (el.operationsServicesToday) el.operationsServicesToday.textContent = formatCount(snapshot.servicesToday || 0);
    if (el.operationsFollowupsDue) el.operationsFollowupsDue.textContent = formatCount(attention.followupsDue || 0);
    if (el.operationsUrgentPrayer) el.operationsUrgentPrayer.textContent = formatCount(attention.urgentPrayer || 0);

    if (el.operationsMeta) {
      if (isCommunicationsTab()) {
        const openFollowups = followups.filter((row) => !isDoneLikeStatus(row.status));
        el.operationsMeta.textContent = `${formatCount(attention.firstTimeGuests || 0)} first-time guest(s), ${formatCount(
          openFollowups.length
        )} open follow-up item(s), and outbound comms tools in one workspace.`;
        return;
      }
      const parts = [];
      if (overviewData?.unavailable) {
        parts.push(overviewData?.error || "Church Life overview is unavailable right now.");
      } else {
        parts.push(
          `Giving health: ${String(snapshot.givingHealth || "stable").toLowerCase()}. 4-week retention: ${Math.round(
            Number(overviewData?.trend?.retention4w || 0) * 100
          )}%.`
        );
        parts.push(
          `${formatCount(attention.firstTimeGuests || 0)} first-time guest(s) and ${formatCount(attention.missedThreeWeeks || 0)} people at risk.`
        );
      }
      if (followupsData?.unavailable) parts.push("Care queue is limited on this deploy.");
      el.operationsMeta.textContent = parts.join(" ");
    }

    if (el.operationsTrendBody) {
      if (!trendWeeks.length) {
        renderEmpty(
          el.operationsTrendBody,
          2,
          overviewData?.unavailable ? overviewData?.error || "Church Life trend is unavailable." : "No attendance trend yet."
        );
      } else {
        el.operationsTrendBody.innerHTML = trendWeeks
          .map((row) => `
            <tr>
              <td>${escapeHtml(formatDateOnly(row.weekStart))}</td>
              <td>${escapeHtml(formatCount(row.attendance || 0))}</td>
            </tr>
          `)
          .join("");
      }
    }

    if (el.operationsQueueBody) {
      if (!followups.length) {
        renderEmpty(
          el.operationsQueueBody,
          3,
          followupsData?.unavailable ? followupsData?.error || "Care queue is unavailable." : "No follow-ups due right now."
        );
      } else {
        el.operationsQueueBody.innerHTML = followups
          .slice(0, 8)
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.title || row.personName || row.visitorName || "Follow-up")}</td>
              <td><span class="badge ${workspaceBadgeClass(row.status)}">${escapeHtml(String(row.status || "OPEN"))}</span></td>
              <td>${escapeHtml(formatDateOnly(row.dueAt || row.createdAt))}</td>
            </tr>
          `)
          .join("");
      }
    }
  }

  function renderOperationsInsights(insightsData) {
    const overview = insightsData?.overview || {};
    const weeklyTrend = Array.isArray(insightsData?.weeklyTrend) ? insightsData.weeklyTrend : [];
    const atRiskMembers = Array.isArray(insightsData?.atRiskMembers) ? insightsData.atRiskMembers : [];
    const currentWeek = weeklyTrend.length ? weeklyTrend[weeklyTrend.length - 1] : null;
    const previousWeek = weeklyTrend.length > 1 ? weeklyTrend[weeklyTrend.length - 2] : null;
    const attendanceDelta = calculatePercentDelta(currentWeek?.uniqueAttendees || 0, previousWeek?.uniqueAttendees || 0);

    if (el.operationsInsightsAttendees) {
      el.operationsInsightsAttendees.textContent = formatCount(currentWeek?.uniqueAttendees || overview.uniqueAttendees || 0);
    }
    if (el.operationsInsightsFirstTime) {
      el.operationsInsightsFirstTime.textContent = formatCount(currentWeek?.firstTimeVisitors || overview.firstTimeVisitors || 0);
    }
    if (el.operationsInsightsReturning) {
      el.operationsInsightsReturning.textContent = formatCount(currentWeek?.returningVisitors || overview.returningVisitors || 0);
    }
    if (el.operationsInsightsAttendanceDelta) {
      el.operationsInsightsAttendanceDelta.textContent = attendanceDelta === null ? "n/a" : formatPercentChange(attendanceDelta);
    }
    if (el.operationsInsightsParticipation) {
      el.operationsInsightsParticipation.textContent = `${Number(currentWeek?.donorParticipationRatePct || 0).toFixed(1)}%`;
    }
    if (el.operationsInsightsGiving) {
      el.operationsInsightsGiving.textContent = formatMoney(currentWeek?.givingAmount || 0);
    }

    if (el.operationsInsightsMeta) {
      const parts = [];
      if (insightsData?.unavailable) {
        parts.push(insightsData?.error || "Insights are unavailable right now.");
      } else {
        parts.push(`Last ${formatCount(overview.weeks || 8)} week(s) loaded.`);
        parts.push(`Retention: ${Number(overview.retentionRatePct || 0).toFixed(1)}%.`);
        parts.push(`${formatCount(atRiskMembers.length)} at-risk member(s) flagged.`);
      }
      el.operationsInsightsMeta.textContent = parts.join(" ");
    }

    if (el.operationsInsightsTrendBody) {
      if (!weeklyTrend.length) {
        renderEmpty(
          el.operationsInsightsTrendBody,
          5,
          insightsData?.unavailable ? insightsData?.error || "Insights are unavailable." : "No weekly insight trend yet."
        );
      } else {
        el.operationsInsightsTrendBody.innerHTML = weeklyTrend
          .slice(-8)
          .reverse()
          .map((row) => `
            <tr>
              <td>${escapeHtml(formatDateOnly(row.weekStart))}</td>
              <td>${escapeHtml(formatCount(row.uniqueAttendees || 0))}</td>
              <td>${escapeHtml(formatCount(row.firstTimeVisitors || 0))}</td>
              <td>${escapeHtml(formatCount(row.donorCount || 0))}</td>
              <td>${escapeHtml(formatMoney(row.givingAmount || 0))}</td>
            </tr>
          `)
          .join("");
      }
    }

    if (el.operationsInsightsRiskBody) {
      if (!atRiskMembers.length) {
        renderEmpty(
          el.operationsInsightsRiskBody,
          4,
          insightsData?.unavailable ? insightsData?.error || "Risk signals are unavailable." : "No high-risk members flagged right now."
        );
      } else {
        el.operationsInsightsRiskBody.innerHTML = atRiskMembers
          .slice(0, 8)
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.fullName || row.memberId || "Member")}</td>
              <td>${escapeHtml(formatDateOnly(row.lastServiceDate))}</td>
              <td>${escapeHtml(`${Number(row.attendanceDropPct || 0).toFixed(0)}%`)}</td>
              <td><span class="badge ${riskBadgeClass(row.riskBand)}">${escapeHtml(String(row.riskBand || "LOW"))}</span></td>
            </tr>
          `)
          .join("");
      }
    }
  }

  function renderOperationsServices(servicesData) {
    const services = Array.isArray(servicesData?.services) ? servicesData.services : [];
    const todayIso = isoTodayLocal();
    const upcomingRows = services
      .filter((row) => String(row?.serviceDate || "") >= todayIso)
      .sort((a, b) => String(a?.serviceDate || "").localeCompare(String(b?.serviceDate || "")));
    const recentRows = services
      .filter((row) => String(row?.serviceDate || "") < todayIso)
      .sort((a, b) => String(b?.serviceDate || "").localeCompare(String(a?.serviceDate || "")));
    const orderedRows = upcomingRows.concat(recentRows);
    const openRows = services.filter((row) => row?.checkInOpen);
    state.operationsServiceRows = orderedRows;

    if (el.operationsServicesMeta) {
      const parts = [];
      if (servicesData?.unavailable) {
        parts.push(servicesData?.error || "Services are unavailable right now.");
      } else {
        parts.push(`${formatCount(upcomingRows.length)} upcoming service(s).`);
        parts.push(`${formatCount(openRows.length)} service(s) open for check-in.`);
      }
      el.operationsServicesMeta.textContent = parts.join(" ");
    }

    if (el.operationsServicesBody) {
      if (!orderedRows.length) {
        renderEmpty(
          el.operationsServicesBody,
          7,
          servicesData?.unavailable ? servicesData?.error || "Services are unavailable." : "No services available yet."
        );
      } else {
        el.operationsServicesBody.innerHTML = orderedRows
          .map((row) => {
            const serviceStatus = churchLifeServiceStatus(row);
            const connectGroupManaged = isConnectGroupServiceRow(row);
            const checkInDisabledReason =
              row?.published === false
                ? "Publish this service to enable check-in."
                : row?.isClosed
                  ? "Closed services are read-only."
                  : "";
            const actionButtons = connectGroupManaged
              ? `<span class="table-note">Managed in Groups</span>`
              : `
                <div class="actions-cell">
                  ${
                    checkInDisabledReason
                      ? ""
                      : `
                        <button class="btn ghost" type="button" data-service-action="member" data-id="${escapeHtml(row.id)}">Member</button>
                        <button class="btn ghost" type="button" data-service-action="visitor" data-id="${escapeHtml(row.id)}">Visitor</button>
                      `
                  }
                  <button class="btn ghost" type="button" data-service-action="edit" data-id="${escapeHtml(row.id)}">Edit</button>
                  <button class="btn ghost" type="button" data-service-action="stream" data-id="${escapeHtml(row.id)}">Stream</button>
                </div>
                ${checkInDisabledReason ? `<span class="table-note">${escapeHtml(checkInDisabledReason)}</span>` : ""}
              `;
            return `
              <tr>
                <td>${escapeHtml(row.serviceName || "Service")}</td>
                <td>${escapeHtml(formatDateOnly(row.serviceDate))}</td>
                <td>${escapeHtml(row.campusName || "Main / Unassigned")}</td>
                <td><span class="badge ${workspaceBadgeClass(serviceStatus.tone)}">${escapeHtml(serviceStatus.label)}</span></td>
                <td>${escapeHtml(formatCount(row.checkInsCount || 0))}</td>
                <td>${escapeHtml(formatCount(row.childrenCheckInsCount || 0))}</td>
                <td class="member-actions-cell">${actionButtons}</td>
              </tr>
            `;
          })
          .join("");
      }
    }
  }

  function renderOperationsFollowups(followupsData, autoPreviewData) {
    const followups = Array.isArray(followupsData?.followups) ? followupsData.followups : [];
    const openRows = followups.filter((row) => !isDoneLikeStatus(row.status));
    const urgentRows = openRows.filter((row) => ["URGENT", "HIGH"].includes(String(row?.priority || "").toUpperCase()));
    const mineRows = openRows.filter((row) => String(row?.assignedMemberId || "") === String(state.profile?.id || ""));
    const previewRows = [
      ...(Array.isArray(autoPreviewData?.firstTimeVisitors) ? autoPreviewData.firstTimeVisitors.map((row) => ({ rule: "First-time", ...row })) : []),
      ...(Array.isArray(autoPreviewData?.missedThreeWeeks) ? autoPreviewData.missedThreeWeeks.map((row) => ({ rule: "Missed 3 weeks", ...row })) : []),
      ...(Array.isArray(autoPreviewData?.atRiskPredicted) ? autoPreviewData.atRiskPredicted.map((row) => ({ rule: "At risk", ...row })) : []),
    ];

    if (el.operationsFollowupsOpen) el.operationsFollowupsOpen.textContent = formatCount(openRows.length);
    if (el.operationsFollowupsUrgent) el.operationsFollowupsUrgent.textContent = formatCount(urgentRows.length);
    if (el.operationsFollowupsMine) el.operationsFollowupsMine.textContent = formatCount(mineRows.length);

    if (el.operationsFollowupsMeta) {
      el.operationsFollowupsMeta.textContent = followupsData?.unavailable
        ? followupsData?.error || "Follow-ups are unavailable right now."
        : `${formatCount(followups.length)} follow-up item(s) loaded.`;
    }

    if (el.operationsFollowupsBody) {
      if (!followups.length) {
        renderEmpty(
          el.operationsFollowupsBody,
          5,
          followupsData?.unavailable
            ? followupsData?.error || "Follow-ups are unavailable."
            : "No follow-ups waiting right now."
        );
      } else {
        el.operationsFollowupsBody.innerHTML = followups
          .slice(0, 12)
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.title || row.personName || row.visitorName || "Follow-up")}</td>
              <td>${escapeHtml(row.personName || row.visitorName || row.memberId || "Member")}</td>
              <td>${escapeHtml(row.assignedMemberName || "Unassigned")}</td>
              <td><span class="badge ${workspaceBadgeClass(row.status)}">${escapeHtml(String(row.status || "OPEN"))}</span></td>
              <td>${escapeHtml(formatDate(row.dueAt || row.createdAt))}</td>
            </tr>
          `)
          .join("");
      }
    }

    if (el.operationsAutoFollowupsMeta) {
      if (autoPreviewData?.unavailable) {
        el.operationsAutoFollowupsMeta.textContent =
          autoPreviewData?.error || "Auto follow-up preview is unavailable right now.";
      } else {
        const meta = autoPreviewData?.meta || {};
        el.operationsAutoFollowupsMeta.textContent =
          `${formatCount(previewRows.length)} candidate(s) across a ${formatCount(meta.sampleLimit || previewRows.length)}-person sample.`;
      }
    }

    if (el.operationsAutoFollowupsBody) {
      if (!previewRows.length) {
        renderEmpty(
          el.operationsAutoFollowupsBody,
          4,
          autoPreviewData?.unavailable
            ? autoPreviewData?.error || "Auto follow-up preview is unavailable."
            : "No auto follow-up candidates right now."
        );
      } else {
        el.operationsAutoFollowupsBody.innerHTML = previewRows
          .slice(0, 12)
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.rule || "Rule")}</td>
              <td>${escapeHtml(row.fullName || row.memberId || "Member")}</td>
              <td>${escapeHtml(formatContactLine(row.phone, row.email) || "-")}</td>
              <td>${escapeHtml(formatDateOnly(row.firstServiceDate || row.lastServiceDate || row.predictedAt || row.scoredAt))}</td>
            </tr>
          `)
          .join("");
      }
    }
  }

  function renderOperationsPrayer(prayerData) {
    const prayerRequests = Array.isArray(prayerData?.prayerRequests) ? prayerData.prayerRequests : [];
    const newRows = prayerRequests.filter((row) => String(row?.status || "").toUpperCase() === "NEW");
    const inProgressRows = prayerRequests.filter((row) => ["ASSIGNED", "IN_PROGRESS"].includes(String(row?.status || "").toUpperCase()));
    const mineRows = prayerRequests.filter((row) => String(row?.assignedToUserId || "") === String(state.profile?.id || ""));
    const closedRows = prayerRequests.filter((row) => String(row?.status || "").toUpperCase() === "CLOSED");

    if (el.operationsPrayerNew) el.operationsPrayerNew.textContent = formatCount(newRows.length);
    if (el.operationsPrayerInProgress) el.operationsPrayerInProgress.textContent = formatCount(inProgressRows.length);
    if (el.operationsPrayerMine) el.operationsPrayerMine.textContent = formatCount(mineRows.length);
    if (el.operationsPrayerClosed) el.operationsPrayerClosed.textContent = formatCount(closedRows.length);

    if (el.operationsPrayerMeta) {
      el.operationsPrayerMeta.textContent = prayerData?.unavailable
        ? prayerData?.error || "Prayer requests are unavailable right now."
        : `${formatCount(prayerRequests.length)} prayer request(s) loaded.`;
    }

    if (el.operationsPrayerBody) {
      if (!prayerRequests.length) {
        renderEmpty(
          el.operationsPrayerBody,
          4,
          prayerData?.unavailable ? prayerData?.error || "Prayer requests are unavailable." : "No prayer requests right now."
        );
      } else {
        el.operationsPrayerBody.innerHTML = prayerRequests
          .slice(0, 12)
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.subject || row.memberName || "Prayer request")}</td>
              <td>${escapeHtml(row.memberName || row.memberId || "Member")}</td>
              <td>${escapeHtml(row.assignedToUserName || row.assignedTeam || "Unassigned")}</td>
              <td><span class="badge ${workspaceBadgeClass(row.status)}">${escapeHtml(String(row.status || "NEW"))}</span></td>
            </tr>
          `)
          .join("");
      }
    }
  }

  function renderOperationsBroadcasts(broadcastsData, audiencesData, templatesData) {
    const broadcasts = Array.isArray(broadcastsData?.broadcasts) ? broadcastsData.broadcasts : [];
    const audiences = Array.isArray(audiencesData?.audiences) ? audiencesData.audiences : [];
    const templates = Array.isArray(templatesData?.templates) ? templatesData.templates : [];
    const totalAudience = broadcasts.reduce((sum, row) => sum + Number(row?.audienceCount || 0), 0);
    const totalSent = broadcasts.reduce((sum, row) => sum + Number(row?.sentCount || 0), 0);
    const totalFailed = broadcasts.reduce((sum, row) => sum + Number(row?.failedCount || 0), 0);

    if (el.operationsBroadcastsTotal) el.operationsBroadcastsTotal.textContent = formatCount(broadcasts.length);
    if (el.operationsBroadcastsAudience) el.operationsBroadcastsAudience.textContent = formatCount(totalAudience);
    if (el.operationsBroadcastsSent) el.operationsBroadcastsSent.textContent = formatCount(totalSent);
    if (el.operationsBroadcastsFailed) el.operationsBroadcastsFailed.textContent = formatCount(totalFailed);

    if (el.operationsBroadcastsMeta) {
      el.operationsBroadcastsMeta.textContent = broadcastsData?.unavailable
        ? broadcastsData?.error || "Broadcasts are unavailable right now."
        : `${formatCount(broadcasts.length)} recent broadcast(s) loaded.`;
    }

    if (el.operationsBroadcastsBody) {
      if (!broadcasts.length) {
        renderEmpty(
          el.operationsBroadcastsBody,
          4,
          broadcastsData?.unavailable
            ? broadcastsData?.error || "Broadcasts are unavailable."
            : "No broadcast history yet."
        );
      } else {
        el.operationsBroadcastsBody.innerHTML = broadcasts
          .slice(0, 12)
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.title || "Broadcast")}</td>
              <td><span class="badge ${workspaceBadgeClass(row.status)}">${escapeHtml(String(row.status || "DRAFT"))}</span></td>
              <td>${escapeHtml(formatCount(row.audienceCount || 0))}</td>
              <td>${escapeHtml(formatDate(row.sentAt || row.createdAt))}</td>
            </tr>
          `)
          .join("");
      }
    }

    if (el.operationsBroadcastAudiencesBody) {
      if (!audiences.length) {
        renderEmpty(
          el.operationsBroadcastAudiencesBody,
          3,
          audiencesData?.unavailable
            ? audiencesData?.error || "Saved audiences are unavailable."
            : "No saved audiences yet."
        );
      } else {
        el.operationsBroadcastAudiencesBody.innerHTML = audiences
          .slice(0, 8)
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.name || "Audience")}</td>
              <td>${escapeHtml(labelizeToken(row.segmentKey, "All members"))}${row.segmentTag ? `: ${escapeHtml(row.segmentTag)}` : ""}</td>
              <td><span class="badge ${row.active === false ? "failed" : "paid"}">${escapeHtml(row.active === false ? "Inactive" : "Active")}</span></td>
            </tr>
          `)
          .join("");
      }
    }

    if (el.operationsBroadcastTemplatesBody) {
      if (!templates.length) {
        renderEmpty(
          el.operationsBroadcastTemplatesBody,
          3,
          templatesData?.unavailable
            ? templatesData?.error || "Templates are unavailable."
            : "No broadcast templates yet."
        );
      } else {
        el.operationsBroadcastTemplatesBody.innerHTML = templates
          .slice(0, 8)
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.name || row.title || "Template")}</td>
              <td>${escapeHtml(labelizeToken(row.defaultSegmentKey, "All members"))}${row.defaultSegmentTag ? `: ${escapeHtml(row.defaultSegmentTag)}` : ""}</td>
              <td><span class="badge ${row.active === false ? "failed" : "paid"}">${escapeHtml(row.active === false ? "Inactive" : "Active")}</span></td>
            </tr>
          `)
          .join("");
      }
    }
  }

  function renderOperationsVolunteers(volunteerData, schedulesData, ministriesData, rolesData, reviewsData, auditData) {
    const summary = volunteerData?.summary || {};
    const overloadMembers = Array.isArray(volunteerData?.overloadMembers) ? volunteerData.overloadMembers : [];
    const schedules = Array.isArray(schedulesData?.schedules) ? schedulesData.schedules : [];
    const ministries = Array.isArray(ministriesData?.ministries) ? ministriesData.ministries : [];
    const roles = Array.isArray(rolesData?.ministryRoles) ? rolesData.ministryRoles : [];
    const reviews = Array.isArray(reviewsData?.volunteerTerms) ? reviewsData.volunteerTerms : [];
    const auditLogs = Array.isArray(auditData?.logs) ? auditData.logs : [];
    const todayIso = isoTodayLocal();
    const todayRows = schedules.filter((row) => String(row?.scheduleDate || "") === todayIso);

    if (el.operationsVolunteersOpenTerms) el.operationsVolunteersOpenTerms.textContent = formatCount(summary.openTerms || 0);
    if (el.operationsVolunteersReviewsDue) el.operationsVolunteersReviewsDue.textContent = formatCount(summary.reviewsDue || 0);
    if (el.operationsVolunteersHighLoad) el.operationsVolunteersHighLoad.textContent = formatCount(summary.highLoadMembers || 0);
    if (el.operationsVolunteersOverload) el.operationsVolunteersOverload.textContent = formatCount(summary.overloadMembers || 0);

    if (el.operationsVolunteersMeta) {
      const parts = [];
      if (volunteerData?.unavailable) parts.push(volunteerData?.error || "Volunteer insights are unavailable right now.");
      else parts.push(`${formatCount(overloadMembers.length)} overload alert(s) loaded.`);
      if (schedulesData?.unavailable) parts.push("Today's schedule preview is unavailable.");
      else parts.push(`${formatCount(todayRows.length)} schedule(s) on today's run sheet.`);
      if (!ministriesData?.unavailable) parts.push(`${formatCount(ministries.length)} ministry setup item(s) loaded.`);
      el.operationsVolunteersMeta.textContent = parts.join(" ");
    }

    if (el.operationsVolunteersBody) {
      if (!overloadMembers.length) {
        renderEmpty(
          el.operationsVolunteersBody,
          3,
          volunteerData?.unavailable ? volunteerData?.error || "Volunteer insights are unavailable." : "No overload alerts right now."
        );
      } else {
        el.operationsVolunteersBody.innerHTML = overloadMembers
          .slice(0, 8)
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.memberName || row.memberId || "Volunteer")}</td>
              <td>${escapeHtml(formatCount(row.activeRoles || 0))}</td>
              <td><span class="badge ${Number(row.activeRoles || 0) >= 4 ? "failed" : "pending"}">${escapeHtml(
                Number(row.activeRoles || 0) >= 4 ? "Overload" : "High load"
              )}</span></td>
            </tr>
          `)
          .join("");
      }
    }

    if (el.operationsVolunteerScheduleBody) {
      if (!todayRows.length) {
        renderEmpty(
          el.operationsVolunteerScheduleBody,
          4,
          schedulesData?.unavailable ? schedulesData?.error || "Schedules are unavailable." : "No schedules today."
        );
      } else {
        el.operationsVolunteerScheduleBody.innerHTML = todayRows
          .slice(0, 8)
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.title || row.ministryName || "Schedule")}</td>
              <td>${escapeHtml(formatCount(row.assignmentCount || 0))}</td>
              <td>${escapeHtml(formatCount(row.servedCount || 0))}</td>
              <td>${escapeHtml(formatCount(row.noShowCount || 0))}</td>
            </tr>
          `)
          .join("");
      }
    }

    if (el.operationsVolunteerMinistriesBody) {
      if (!ministries.length) {
        renderEmpty(
          el.operationsVolunteerMinistriesBody,
          4,
          ministriesData?.unavailable ? ministriesData?.error || "Ministries are unavailable." : "No ministries configured yet."
        );
      } else {
        el.operationsVolunteerMinistriesBody.innerHTML = ministries
          .slice(0, 10)
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.name || "Ministry")}</td>
              <td>${escapeHtml(row.campusName || "Main / Unassigned")}</td>
              <td>${escapeHtml(formatCount(row.roleCount || 0))}</td>
              <td>${escapeHtml(formatCount(row.activeVolunteerCount || 0))}</td>
            </tr>
          `)
          .join("");
      }
    }

    if (el.operationsVolunteerRolesBody) {
      if (!roles.length) {
        renderEmpty(
          el.operationsVolunteerRolesBody,
          4,
          rolesData?.unavailable ? rolesData?.error || "Roles are unavailable." : "No ministry roles configured yet."
        );
      } else {
        el.operationsVolunteerRolesBody.innerHTML = roles
          .slice(0, 10)
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.roleName || "Role")}</td>
              <td>${escapeHtml(row.ministryName || "Ministry")}</td>
              <td>${escapeHtml(`${formatCount(row.defaultTermMonths || 0)} mo`)}</td>
              <td>${escapeHtml(formatCount(row.activeTermsCount || 0))}</td>
            </tr>
          `)
          .join("");
      }
    }

    if (el.operationsVolunteerReviewsBody) {
      if (!reviews.length) {
        renderEmpty(
          el.operationsVolunteerReviewsBody,
          4,
          reviewsData?.unavailable ? reviewsData?.error || "Reviews are unavailable." : "No volunteer reviews due right now."
        );
      } else {
        el.operationsVolunteerReviewsBody.innerHTML = reviews
          .slice(0, 10)
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.memberName || row.memberId || "Volunteer")}</td>
              <td>${escapeHtml(row.ministryRoleName || row.ministryName || "Role")}</td>
              <td>${escapeHtml(formatDateOnly(row.endDate))}</td>
              <td><span class="badge ${workspaceBadgeClass(row.status)}">${escapeHtml(String(row.status || "ACTIVE"))}</span></td>
            </tr>
          `)
          .join("");
      }
    }

    if (el.operationsVolunteerAuditBody) {
      if (!auditLogs.length) {
        renderEmpty(
          el.operationsVolunteerAuditBody,
          3,
          auditData?.unavailable ? auditData?.error || "Audit history is unavailable." : "No recent audit activity."
        );
      } else {
        el.operationsVolunteerAuditBody.innerHTML = auditLogs
          .slice(0, 10)
          .map((row) => `
            <tr>
              <td>${escapeHtml(formatDate(row.createdAt))}</td>
              <td>${escapeHtml(labelizeToken(row.action, "Action"))}</td>
              <td>${escapeHtml(row.actorName || row.actorMemberRef || "System")}</td>
            </tr>
          `)
          .join("");
      }
    }
  }

  function renderOperationsSchedules(schedulesData) {
    const schedules = Array.isArray(schedulesData?.schedules) ? schedulesData.schedules : [];
    const todayIso = isoTodayLocal();
    const todayRows = schedules.filter((row) => String(row?.scheduleDate || "") === todayIso);
    const todaySummary = todayRows.reduce(
      (acc, row) => {
        acc.assignments += Number(row?.assignmentCount || 0);
        acc.served += Number(row?.servedCount || 0);
        acc.noShow += Number(row?.noShowCount || 0);
        return acc;
      },
      { assignments: 0, served: 0, noShow: 0 }
    );

    if (el.operationsSchedulesToday) el.operationsSchedulesToday.textContent = formatCount(todayRows.length);
    if (el.operationsSchedulesAssignments) el.operationsSchedulesAssignments.textContent = formatCount(todaySummary.assignments);
    if (el.operationsSchedulesServed) el.operationsSchedulesServed.textContent = formatCount(todaySummary.served);
    if (el.operationsSchedulesNoShow) el.operationsSchedulesNoShow.textContent = formatCount(todaySummary.noShow);

    if (el.operationsSchedulesMeta) {
      el.operationsSchedulesMeta.textContent = schedulesData?.unavailable
        ? schedulesData?.error || "Schedules are unavailable right now."
        : `${formatCount(schedules.length)} schedule(s) loaded.`;
    }

    if (el.operationsSchedulesBody) {
      if (!schedules.length) {
        renderEmpty(
          el.operationsSchedulesBody,
          5,
          schedulesData?.unavailable ? schedulesData?.error || "Schedules are unavailable." : "No schedules created yet."
        );
      } else {
        el.operationsSchedulesBody.innerHTML = schedules
          .slice(0, 12)
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.title || "Schedule")}</td>
              <td>${escapeHtml(formatDateOnly(row.scheduleDate))}</td>
              <td>${escapeHtml(row.ministryName || row.serviceName || "Ministry")}</td>
              <td><span class="badge ${workspaceBadgeClass(row.status)}">${escapeHtml(String(row.status || "DRAFT"))}</span></td>
              <td>${escapeHtml(formatCount(row.assignmentCount || 0))}</td>
            </tr>
          `)
          .join("");
      }
    }
  }

  function renderOperationsGroups(groupsData, membersData, meetingsData) {
    const groups = Array.isArray(groupsData?.groups) ? groupsData.groups : [];
    state.operationsGroupRows = groups;
    const members = Array.isArray(membersData?.members) ? membersData.members : [];
    const meetings = Array.isArray(meetingsData?.meetings) ? meetingsData.meetings : [];
    const selectedGroup = groups.find((row) => String(row?.id || "") === String(state.operationsGroupId || "")) || null;
    const totalMembers = groups.reduce((sum, row) => sum + Number(row?.memberCount || 0), 0);
    const activeGroups = groups.filter((row) => row?.active !== false);

    if (el.operationsGroupsTotal) el.operationsGroupsTotal.textContent = formatCount(groups.length);
    if (el.operationsGroupsActive) el.operationsGroupsActive.textContent = formatCount(activeGroups.length);
    if (el.operationsGroupsMembers) el.operationsGroupsMembers.textContent = formatCount(totalMembers);
    if (el.operationsGroupsMeetings) el.operationsGroupsMeetings.textContent = formatCount(meetings.length);

    if (el.operationsGroupsMeta) {
      const parts = [];
      if (groupsData?.unavailable) parts.push(groupsData?.error || "Groups are unavailable right now.");
      else parts.push(`${formatCount(groups.length)} group(s) loaded.`);
      if (selectedGroup?.name) parts.push(`Selected: ${selectedGroup.name}.`);
      el.operationsGroupsMeta.textContent = parts.join(" ");
    }

    if (el.operationsGroupsBody) {
      if (!groups.length) {
        renderEmpty(
          el.operationsGroupsBody,
          5,
          groupsData?.unavailable ? groupsData?.error || "Groups are unavailable." : "No groups created yet."
        );
      } else {
        el.operationsGroupsBody.innerHTML = groups
          .slice(0, 12)
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.name || "Group")}</td>
              <td>${escapeHtml(row.leaderName || "Unassigned")}</td>
              <td>${escapeHtml(labelizeToken(row.groupType, "Group"))}</td>
              <td>${escapeHtml(formatCount(row.memberCount || 0))}</td>
              <td class="member-actions-cell">
                <div class="actions-cell">
                  <button class="btn ghost" type="button" data-group-action="open" data-id="${escapeHtml(row.id)}">Open</button>
                  <button class="btn ghost" type="button" data-group-action="toggle" data-id="${escapeHtml(row.id)}">${
                    row.active === false ? "Restore" : "Pause"
                  }</button>
                </div>
              </td>
            </tr>
          `)
          .join("");
      }
    }

    if (el.operationsGroupDetailMeta) {
      if (!selectedGroup) {
        el.operationsGroupDetailMeta.textContent = groups.length
          ? "Select a group to see members and meetings."
          : "Create a group to start organizing members and meetings.";
      } else {
        el.operationsGroupDetailMeta.textContent = formatContactLine(
          selectedGroup.name,
          selectedGroup.leaderName ? `Leader: ${selectedGroup.leaderName}` : "",
          selectedGroup.defaultLocation ? `Location: ${selectedGroup.defaultLocation}` : ""
        );
      }
    }

    if (el.operationsGroupMembersBody) {
      if (!selectedGroup) {
        renderEmpty(el.operationsGroupMembersBody, 4, "No group selected yet.");
      } else if (!members.length) {
        renderEmpty(
          el.operationsGroupMembersBody,
          4,
          membersData?.unavailable ? membersData?.error || "Group members are unavailable." : "No members added to this group yet."
        );
      } else {
        el.operationsGroupMembersBody.innerHTML = members
          .slice(0, 12)
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.fullName || row.memberId || "Member")}</td>
              <td>${escapeHtml(labelizeToken(row.role, "Member"))}</td>
              <td>${escapeHtml(formatContactLine(row.phone, row.email) || "-")}</td>
              <td>${escapeHtml(formatDateOnly(row.joinedOn || row.createdAt))}</td>
            </tr>
          `)
          .join("");
      }
    }

    if (el.operationsGroupMeetingsBody) {
      if (!selectedGroup) {
        renderEmpty(el.operationsGroupMeetingsBody, 4, "No group selected yet.");
      } else if (!meetings.length) {
        renderEmpty(
          el.operationsGroupMeetingsBody,
          4,
          meetingsData?.unavailable ? meetingsData?.error || "Meetings are unavailable." : "No meetings logged for this group yet."
        );
      } else {
        el.operationsGroupMeetingsBody.innerHTML = meetings
          .slice(0, 12)
          .map((row) => `
            <tr>
              <td>${escapeHtml(formatDateOnly(row.meetingDate))}</td>
              <td>${escapeHtml(row.location || selectedGroup?.defaultLocation || "-")}</td>
              <td><span class="badge ${workspaceBadgeClass(row.status)}">${escapeHtml(String(row.status || "SCHEDULED"))}</span></td>
              <td>${escapeHtml(formatCount(row.attendanceCount || row.presentCount || 0))}</td>
            </tr>
          `)
          .join("");
      }
    }
  }

  function renderOperationsChildren(childrenData, checkinsData) {
    const children = Array.isArray(childrenData?.children) ? childrenData.children : [];
    const checkIns = Array.isArray(checkinsData?.checkIns) ? checkinsData.checkIns : [];
    const householdCount = children.filter((row) => String(row?.source || "").toUpperCase() === "HOUSEHOLD").length;
    const walkInCount = children.filter((row) => String(row?.source || "").toUpperCase() === "WALK_IN").length;
    state.operationsChildrenRows = children;
    state.operationsChildrenCheckInRows = checkIns;
    state.operationsChildrenService = checkinsData?.service || null;

    if (el.operationsChildrenProfiles) el.operationsChildrenProfiles.textContent = formatCount(children.length);
    if (el.operationsChildrenHousehold) el.operationsChildrenHousehold.textContent = formatCount(householdCount);
    if (el.operationsChildrenWalkIns) el.operationsChildrenWalkIns.textContent = formatCount(walkInCount);
    if (el.operationsChildrenOpenCheckins) el.operationsChildrenOpenCheckins.textContent = formatCount(checkIns.length);

    if (el.operationsChildrenMeta) {
      const parts = [];
      if (childrenData?.unavailable) parts.push(childrenData?.error || "Children profiles are unavailable right now.");
      else parts.push(`${formatCount(children.length)} child profile(s) loaded.`);
      if (!checkinsData?.unavailable && checkinsData?.service?.serviceName) {
        parts.push(`Live service: ${checkinsData.service.serviceName}.`);
      }
      el.operationsChildrenMeta.textContent = parts.join(" ");
    }

    if (el.operationsChildrenBody) {
      if (!children.length) {
        renderEmpty(
          el.operationsChildrenBody,
          5,
          childrenData?.unavailable ? childrenData?.error || "Children profiles are unavailable." : "No children profiles yet."
        );
      } else {
        el.operationsChildrenBody.innerHTML = children
          .map((row) => {
            const rowKey = operationsChildRowKey(row);
            const canEdit = String(row?.source || "").toUpperCase() === "HOUSEHOLD" || isEditableWalkInChildRow(row);
            return `
              <tr>
                <td>${escapeHtml(row.childName || row.childMemberName || "Child")}</td>
                <td>${escapeHtml(row.parentMemberName || row.parentName || "-")}</td>
                <td>${escapeHtml(labelizeToken(row.source, "Unknown"))}</td>
                <td>${escapeHtml(formatDate(row.lastSeenAt || row.updatedAt || row.createdAt))}</td>
                <td class="member-actions-cell">
                  <div class="actions-cell">
                    <button
                      class="btn ghost"
                      type="button"
                      data-child-profile-action="checkin"
                      data-id="${escapeHtml(rowKey)}"
                    >Check in</button>
                    ${
                      canEdit
                        ? `
                          <button
                            class="btn ghost"
                            type="button"
                            data-child-profile-action="edit"
                            data-id="${escapeHtml(rowKey)}"
                          >Edit</button>
                        `
                        : `<span class="table-note">Use Add child profile to save this child.</span>`
                    }
                  </div>
                </td>
              </tr>
            `;
          })
          .join("");
      }
    }

    if (el.operationsChildrenCheckinsBody) {
      if (!checkIns.length) {
        renderEmpty(
          el.operationsChildrenCheckinsBody,
          5,
          checkinsData?.unavailable ? checkinsData?.error || "Children check-ins are unavailable." : "No open children check-ins right now."
        );
      } else {
        el.operationsChildrenCheckinsBody.innerHTML = checkIns
          .map((row) => `
            <tr>
              <td>${escapeHtml(row.childName || row.childMemberName || "Child")}</td>
              <td>${escapeHtml(row.parentName || row.parentMemberName || "-")}</td>
              <td>${escapeHtml(row.serviceName || checkinsData?.service?.serviceName || "-")}</td>
              <td>${escapeHtml(formatDate(row.checkedInAt || row.createdAt))}</td>
              <td class="member-actions-cell">
                <div class="actions-cell">
                  <button class="btn ghost" type="button" data-child-checkin-action="pickup" data-id="${escapeHtml(row.id)}">Pickup</button>
                </div>
              </td>
            </tr>
          `)
          .join("");
      }
    }
  }

  async function loadOperationsWorkspace({ view = state.operationsView } = {}) {
    const currentView = normalizeChurchLifeView(view);
    state.operationsView = currentView;
    syncCommunicationsEntryForView(currentView);
    syncChurchLifeViewUi();
    setOperationsShellMeta(currentView);

    if (currentView === "overview") {
      if (el.operationsMeta) {
        el.operationsMeta.textContent = "Loading Church Life overview...";
      }
      renderSkeletonRows(el.operationsTrendBody, 2, 6);
      renderSkeletonRows(el.operationsQueueBody, 3, 6);

      const [overviewData, followupsData] = await Promise.all([
        safeWorkspaceRequest(() => apiRequest("/api/admin/operations/overview"), {
          attention: {},
          snapshot: {},
          trend: { weeks: [], retention4w: 0 },
        }),
        safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/followups" + buildQuery({ limit: 8 })), {
          followups: [],
        }),
      ]);

      renderOperationsOverview(overviewData, followupsData);
      return;
    }

    if (currentView === "insights") {
      if (el.operationsInsightsMeta) el.operationsInsightsMeta.textContent = "Loading growth insights...";
      renderSkeletonRows(el.operationsInsightsTrendBody, 5, 6);
      renderSkeletonRows(el.operationsInsightsRiskBody, 4, 6);

      const insightsData = await safeWorkspaceRequest(
        () => apiRequest("/api/admin/operations/insights" + buildQuery({ weeks: 8, serviceLimit: 20 })),
        { overview: {}, weeklyTrend: [], atRiskMembers: [] }
      );
      renderOperationsInsights(insightsData);
      return;
    }

    if (currentView === "services") {
      if (el.operationsServicesMeta) el.operationsServicesMeta.textContent = "Loading services...";
      renderSkeletonRows(el.operationsServicesBody, 4, 7);

      const servicesData = await safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/services" + buildQuery({ limit: 40 })), {
        services: [],
      });
      renderOperationsServices(servicesData);
      return;
    }

    if (currentView === "followups") {
      if (el.operationsFollowupsMeta) {
        el.operationsFollowupsMeta.textContent = isCommunicationsTab() ? "Loading follow-up workspace..." : "Loading follow-ups...";
      }
      renderSkeletonRows(el.operationsFollowupsBody, 5, 6);
      renderSkeletonRows(el.operationsAutoFollowupsBody, 4, 6);

      const [followupsData, autoPreviewData] = await Promise.all([
        safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/followups" + buildQuery({ limit: 40 })), {
          followups: [],
        }),
        safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/auto-followups/preview" + buildQuery({ sampleLimit: 24 })), {
          firstTimeVisitors: [],
          missedThreeWeeks: [],
          atRiskPredicted: [],
          meta: {},
        }),
      ]);
      renderOperationsFollowups(followupsData, autoPreviewData);
      return;
    }

    if (currentView === "prayer") {
      if (el.operationsPrayerMeta) el.operationsPrayerMeta.textContent = "Loading prayer inbox...";
      renderSkeletonRows(el.operationsPrayerBody, 4, 6);

      const prayerData = await safeWorkspaceRequest(
        () => apiRequest("/api/admin/church-life/prayer-requests" + buildQuery({ limit: 40 })),
        { prayerRequests: [] }
      );
      renderOperationsPrayer(prayerData);
      return;
    }

    if (currentView === "broadcasts") {
      if (el.operationsBroadcastsMeta) {
        el.operationsBroadcastsMeta.textContent = isCommunicationsTab()
          ? "Loading broadcasts and messaging settings..."
          : "Loading broadcasts...";
      }
      renderSkeletonRows(el.operationsBroadcastsBody, 4, 6);
      renderSkeletonRows(el.operationsBroadcastAudiencesBody, 3, 5);
      renderSkeletonRows(el.operationsBroadcastTemplatesBody, 3, 5);

      const [broadcastsData, audiencesData, templatesData] = await Promise.all([
        safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/broadcasts" + buildQuery({ limit: 20 })), {
          broadcasts: [],
        }),
        safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/broadcast-audiences" + buildQuery({ limit: 20 })), {
          audiences: [],
        }),
        safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/broadcast-templates" + buildQuery({ limit: 20 })), {
          templates: [],
        }),
      ]);
      renderOperationsBroadcasts(broadcastsData, audiencesData, templatesData);
      return;
    }

    if (currentView === "volunteers") {
      if (el.operationsVolunteersMeta) el.operationsVolunteersMeta.textContent = "Loading volunteer governance...";
      renderSkeletonRows(el.operationsVolunteersBody, 3, 5);
      renderSkeletonRows(el.operationsVolunteerScheduleBody, 4, 5);
      renderSkeletonRows(el.operationsVolunteerMinistriesBody, 4, 5);
      renderSkeletonRows(el.operationsVolunteerRolesBody, 4, 5);
      renderSkeletonRows(el.operationsVolunteerReviewsBody, 4, 5);
      renderSkeletonRows(el.operationsVolunteerAuditBody, 3, 5);

      const [volunteerData, schedulesData, ministriesData, rolesData, reviewsData, auditData] = await Promise.all([
        safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/volunteer-terms/attention" + buildQuery({ days: 30 })), {
          summary: {},
          overloadMembers: [],
        }),
        safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/schedules" + buildQuery({ limit: 20 })), {
          schedules: [],
        }),
        safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/ministries" + buildQuery({ limit: 20 })), {
          ministries: [],
        }),
        safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/ministry-roles" + buildQuery({ limit: 20 })), {
          ministryRoles: [],
        }),
        safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/volunteer-terms/reviews-due" + buildQuery({ days: 45, limit: 20 })), {
          volunteerTerms: [],
        }),
        safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/audit-logs" + buildQuery({ limit: 12 })), {
          logs: [],
        }),
      ]);

      renderOperationsVolunteers(volunteerData, schedulesData, ministriesData, rolesData, reviewsData, auditData);
      return;
    }

    if (currentView === "groups") {
      if (el.operationsGroupsMeta) el.operationsGroupsMeta.textContent = "Loading groups...";
      renderSkeletonRows(el.operationsGroupsBody, 5, 6);
      renderSkeletonRows(el.operationsGroupMembersBody, 4, 6);
      renderSkeletonRows(el.operationsGroupMeetingsBody, 4, 6);

      const groupsData = await safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/groups" + buildQuery({ limit: 40 })), {
        groups: [],
      });
      const groups = Array.isArray(groupsData?.groups) ? groupsData.groups : [];
      if (!state.operationsGroupId || !groups.some((row) => String(row?.id || "") === String(state.operationsGroupId || ""))) {
        state.operationsGroupId = String(groups[0]?.id || "").trim();
      }
      let membersData = { members: [] };
      let meetingsData = { meetings: [] };
      if (state.operationsGroupId) {
        [membersData, meetingsData] = await Promise.all([
          safeWorkspaceRequest(() => apiRequest(`/api/admin/church-life/groups/${encodeURIComponent(state.operationsGroupId)}/members`), {
            members: [],
          }),
          safeWorkspaceRequest(() => apiRequest(`/api/admin/church-life/groups/${encodeURIComponent(state.operationsGroupId)}/meetings` + buildQuery({ limit: 12 })), {
            meetings: [],
          }),
        ]);
      }
      renderOperationsGroups(groupsData, membersData, meetingsData);
      return;
    }

    if (currentView === "children") {
      if (el.operationsChildrenMeta) el.operationsChildrenMeta.textContent = "Loading children workspace...";
      renderSkeletonRows(el.operationsChildrenBody, 4, 5);
      renderSkeletonRows(el.operationsChildrenCheckinsBody, 5, 6);

      const [childrenData, checkinsData] = await Promise.all([
        safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/children-household" + buildQuery({ includeAll: true, limit: 40 })), {
          children: [],
          meta: { sourceBreakdown: {} },
        }),
        safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/children-check-ins" + buildQuery({ status: "open", limit: 40 })), {
          checkIns: [],
          summary: {},
          service: null,
        }),
      ]);
      renderOperationsChildren(childrenData, checkinsData);
      return;
    }

    if (currentView === "schedules") {
      if (el.operationsSchedulesMeta) el.operationsSchedulesMeta.textContent = "Loading schedules...";
      renderSkeletonRows(el.operationsSchedulesBody, 5, 6);

      const schedulesData = await safeWorkspaceRequest(() => apiRequest("/api/admin/church-life/schedules" + buildQuery({ limit: 40 })), {
        schedules: [],
      });
      renderOperationsSchedules(schedulesData);
    }
  }

  async function createFollowupFromPrompt() {
    const title = await promptAction({
      title: "New follow-up",
      body: "Start with the follow-up title.",
      label: "Title",
      placeholder: "First-time visitor welcome call",
      okLabel: "Next",
    });
    if (title === null) return;
    const memberRef = await promptAction({
      title: "Member Ref",
      body: "Enter a member ID/phone/email if this follow-up is for an existing member, or leave blank for a visitor.",
      label: "Member ref",
      placeholder: "Optional",
      okLabel: "Next",
    });
    if (memberRef === null) return;

    let visitorName = "";
    if (!String(memberRef || "").trim()) {
      const visitor = await promptAction({
        title: "Visitor Name",
        body: "A visitor name is required when there is no member ref.",
        label: "Visitor name",
        placeholder: "Guest name",
        okLabel: "Next",
      });
      if (visitor === null) return;
      visitorName = String(visitor || "").trim();
      if (!visitorName) {
        toast("Visitor name is required if no member ref is supplied.", "error");
        return;
      }
    }

    const dueDate = await promptAction({
      title: "Due Date",
      body: "Optional. Use YYYY-MM-DD or YYYY-MM-DD HH:MM.",
      label: "Due date",
      placeholder: "2026-03-20",
      okLabel: "Create follow-up",
    });
    if (dueDate === null) return;
    const dueAt = parsePromptDateToIsoDateTime(dueDate);
    if (dueDate && dueAt === "") {
      toast("Due date must be YYYY-MM-DD or YYYY-MM-DD HH:MM.", "error");
      return;
    }

    await apiRequest("/api/admin/church-life/followups", {
      method: "POST",
      body: {
        title: String(title || "").trim(),
        followupType: "CARE",
        memberRef: String(memberRef || "").trim() || null,
        visitorName: visitorName || null,
        dueAt: dueAt || null,
      },
    });
    toast("Follow-up created.", "success");
    await loadOperationsWorkspace({ view: "followups" });
  }

  async function runAutoFollowups() {
    const confirmed = await confirmAction({
      title: "Run auto follow-ups",
      body: "This will open follow-ups for first-time visitors, members who missed 3 weeks, and predicted at-risk members.",
      okLabel: "Run now",
    });
    if (!confirmed) return;

    await apiRequest("/api/admin/church-life/auto-followups/run", {
      method: "POST",
      body: { limitPerRule: 100 },
    });
    toast("Auto follow-ups generated.", "success");
    await loadOperationsWorkspace({ view: "followups" });
  }

  async function sendBroadcastFromPrompt() {
    const title = await promptAction({
      title: "Send broadcast",
      body: "This sends a simple in-app broadcast to all members.",
      label: "Title",
      placeholder: "Weekend service reminder",
      okLabel: "Next",
    });
    if (title === null) return;
    const body = await promptAction({
      title: "Broadcast body",
      body: "Keep it short and direct.",
      label: "Message",
      placeholder: "See you at service this Sunday at 09:00.",
      okLabel: "Send broadcast",
    });
    if (body === null) return;
    if (!String(title || "").trim() || !String(body || "").trim()) {
      toast("Broadcast title and message are required.", "error");
      return;
    }

    await apiRequest("/api/admin/church-life/broadcasts", {
      method: "POST",
      body: {
        title: String(title || "").trim(),
        body: String(body || "").trim(),
        segmentKey: "ALL_MEMBERS",
      },
    });
    toast("Broadcast sent.", "success");
    await loadOperationsWorkspace({ view: "broadcasts" });
  }

  async function createGroupFromPrompt() {
    const name = await promptAction({
      title: "Create group",
      body: "Start with the group name.",
      label: "Group name",
      placeholder: "Young Adults Midweek",
      okLabel: "Next",
    });
    if (name === null) return;
    if (!String(name || "").trim()) {
      toast("Group name is required.", "error");
      return;
    }

    const location = await promptAction({
      title: "Default location",
      body: "Optional. This will be used for meetings unless you change it later.",
      label: "Location",
      placeholder: "Main hall",
      okLabel: "Create group",
    });
    if (location === null) return;

    const data = await apiRequest("/api/admin/church-life/groups", {
      method: "POST",
      body: {
        name: String(name || "").trim(),
        groupType: "SMALL_GROUP",
        defaultLocation: String(location || "").trim() || null,
      },
    });
    state.operationsGroupId = String(data?.group?.id || "").trim();
    toast("Group created.", "success");
    await loadOperationsWorkspace({ view: "groups" });
  }

  async function toggleGroupActive(groupId) {
    const current = state.operationsGroupRows.find((row) => String(row?.id || "") === String(groupId || "")) || null;
    if (!current) return;
    const nextActive = current.active === false;
    const confirmed = await confirmAction({
      title: nextActive ? "Restore group" : "Pause group",
      body: `${nextActive ? "Restore" : "Pause"} ${current.name || "this group"}?`,
      okLabel: nextActive ? "Restore" : "Pause",
    });
    if (!confirmed) return;

    await apiRequest(`/api/admin/church-life/groups/${encodeURIComponent(groupId)}`, {
      method: "PATCH",
      body: { active: nextActive },
    });
    toast(`Group ${nextActive ? "restored" : "paused"}.`, "success");
    await loadOperationsWorkspace({ view: "groups" });
  }

  async function pickupChildrenCheckIn(checkInId) {
    const confirmed = await confirmAction({
      title: "Complete pickup",
      body: "Mark this child as picked up?",
      okLabel: "Complete pickup",
    });
    if (!confirmed) return;

    await apiRequest(`/api/admin/church-life/children-check-ins/${encodeURIComponent(checkInId)}/pickup`, {
      method: "POST",
      body: {},
    });
    toast("Child pickup recorded.", "success");
    await loadOperationsWorkspace({ view: "children" });
  }

  function operationsChildRowKey(row) {
    return String(row?.selectionKey || row?.id || "").trim();
  }

  function isEditableWalkInChildRow(row) {
    return String(row?.selectionKey || "")
      .trim()
      .toLowerCase()
      .startsWith("walkin:");
  }

  function isConnectGroupServiceRow(row) {
    return String(row?.serviceCategory || "").trim().toUpperCase() === "CONNECT_GROUP";
  }

  function findOperationsServiceRow(serviceId) {
    return state.operationsServiceRows.find((row) => String(row?.id || "") === String(serviceId || "").trim()) || null;
  }

  function findOperationsChildRow(rowKey) {
    const targetKey = String(rowKey || "").trim();
    return state.operationsChildrenRows.find((row) => operationsChildRowKey(row) === targetKey) || null;
  }

  async function ensureOperationsServiceRows() {
    if (Array.isArray(state.operationsServiceRows) && state.operationsServiceRows.length) return state.operationsServiceRows;
    const data = await apiRequest("/api/admin/church-life/services" + buildQuery({ limit: 40 }));
    const rows = Array.isArray(data?.services) ? data.services : [];
    state.operationsServiceRows = rows;
    return rows;
  }

  async function pickOperationalService(preferredServiceId = "") {
    const requestedId = String(preferredServiceId || "").trim();
    const rows = await ensureOperationsServiceRows();
    if (!rows.length) return null;

    if (requestedId) {
      const requested = rows.find((row) => String(row?.id || "") === requestedId);
      if (requested) return requested;
    }

    const usableRows = rows.filter((row) => !isConnectGroupServiceRow(row));
    const publishedRows = usableRows.filter((row) => row?.published !== false);
    const todayIso = isoTodayLocal();
    const upcomingRows = publishedRows
      .filter((row) => String(row?.serviceDate || "") >= todayIso)
      .sort((left, right) => {
        const dateCompare = String(left?.serviceDate || "").localeCompare(String(right?.serviceDate || ""));
        if (dateCompare !== 0) return dateCompare;
        return String(left?.startsAt || "").localeCompare(String(right?.startsAt || ""));
      });

    return upcomingRows[0] || publishedRows[0] || usableRows[0] || rows[0] || null;
  }

  async function resolveOperationsParentMember(parentRef) {
    const lookup = String(parentRef || "").trim();
    if (!lookup) throw new Error("Parent phone, email, or member ID is required.");

    const data = await apiRequest("/api/admin/church-life/children-household" + buildQuery({ parentRef: lookup, limit: 1 }));
    const parent = data?.parent || null;
    if (!parent?.memberPk) {
      throw new Error("Parent was not found in this church. Use phone, email, or member ID.");
    }
    return parent;
  }

  async function createOrEditServiceFromPrompt(serviceId = "") {
    const current = serviceId ? findOperationsServiceRow(serviceId) : null;
    const actionLabel = current ? "Update service" : "Create service";

    const serviceName = await promptAction({
      title: current ? "Edit service" : "Add service",
      body: "Use the service name your team will recognize tomorrow.",
      label: "Service name",
      placeholder: "Sunday Celebration",
      value: current?.serviceName || "",
      okLabel: "Next",
    });
    if (serviceName === null) return;
    if (!String(serviceName || "").trim()) {
      toast("Service name is required.", "error");
      return;
    }

    const serviceDate = await promptAction({
      title: "Service date",
      body: "Pick the date for this service.",
      label: "Date",
      value: toDateInputValue(current?.serviceDate || isoTodayLocal()),
      okLabel: "Next",
      inputType: "date",
    });
    if (serviceDate === null) return;
    if (!String(serviceDate || "").trim()) {
      toast("Service date is required.", "error");
      return;
    }

    const startsAt = await promptAction({
      title: "Start time",
      body: "Optional. Leave blank if you want to add it later.",
      label: "Starts at",
      value: toDateTimeLocalValue(current?.startsAt),
      placeholder: "Optional",
      okLabel: "Next",
      inputType: "datetime-local",
    });
    if (startsAt === null) return;

    const endsAt = await promptAction({
      title: "End time",
      body: "Optional. Leave blank if you want to add it later.",
      label: "Ends at",
      value: toDateTimeLocalValue(current?.endsAt),
      placeholder: "Optional",
      okLabel: "Next",
      inputType: "datetime-local",
    });
    if (endsAt === null) return;

    const location = await promptAction({
      title: "Location",
      body: "Optional but useful for staff and children's teams.",
      label: "Location",
      placeholder: "Main auditorium",
      value: current?.location || "",
      okLabel: actionLabel,
    });
    if (location === null) return;

    await apiRequest(
      current ? `/api/admin/church-life/services/${encodeURIComponent(serviceId)}` : "/api/admin/church-life/services",
      {
        method: current ? "PATCH" : "POST",
        body: {
          serviceName: String(serviceName || "").trim(),
          serviceDate: String(serviceDate || "").trim(),
          startsAt: String(startsAt || "").trim() || null,
          endsAt: String(endsAt || "").trim() || null,
          location: String(location || "").trim() || null,
          published: current ? current.published !== false : true,
        },
      }
    );

    state.operationsServiceRows = [];
    toast(current ? "Service updated." : "Service created.", "success");
    await loadOperationsWorkspace({ view: "services" });
  }

  async function openServiceStreamLink(serviceId) {
    const data = await apiRequest(`/api/admin/church-life/services/${encodeURIComponent(serviceId)}/stream-link`);
    const link = String(data?.stream?.link || "").trim();
    if (!link) throw new Error("Stream link is not available for this service.");

    await copyToClipboard(link, "Service stream link copied.");
    const popup = window.open(link, "_blank", "noopener,noreferrer");
    if (!popup) {
      toast("Stream link copied. Paste it into a browser if a new tab did not open.", "info", 4200);
    }
  }

  async function checkInMemberToServiceFromPrompt(serviceId = "") {
    const service = await pickOperationalService(serviceId);
    if (!service?.id) throw new Error("Add and publish a service first.");
    if (isConnectGroupServiceRow(service)) throw new Error("This service is managed from Groups.");

    const memberRef = await promptAction({
      title: "Member check-in",
      body: `Check a person into ${service.serviceName || "this service"} on ${formatDateOnly(service.serviceDate)} using phone, email, or member ID.`,
      label: "Phone, email, or member ID",
      placeholder: "0710000000",
      okLabel: "Check in member",
    });
    if (memberRef === null) return;
    if (!String(memberRef || "").trim()) {
      toast("Phone, email, or member ID is required.", "error");
      return;
    }

    await apiRequest("/api/admin/church-life/check-ins/usher", {
      method: "POST",
      body: {
        serviceId: service.id,
        memberRef: String(memberRef || "").trim(),
      },
    });

    state.operationsServiceRows = [];
    toast("Member checked in.", "success");
    await loadOperationsWorkspace({ view: "services" });
  }

  async function checkInVisitorToServiceFromPrompt(serviceId = "") {
    const service = await pickOperationalService(serviceId);
    if (!service?.id) throw new Error("Add and publish a service first.");
    if (isConnectGroupServiceRow(service)) throw new Error("This service is managed from Groups.");

    const visitorName = await promptAction({
      title: "Visitor check-in",
      body: `Capture the visitor for ${service.serviceName || "this service"} on ${formatDateOnly(service.serviceDate)}.`,
      label: "Visitor name",
      placeholder: "Guest name",
      okLabel: "Next",
    });
    if (visitorName === null) return;
    if (!String(visitorName || "").trim()) {
      toast("Visitor name is required.", "error");
      return;
    }

    const visitorPhone = await promptAction({
      title: "Visitor phone",
      body: "Optional, but helpful for follow-up.",
      label: "Phone",
      placeholder: "0710000000",
      okLabel: "Next",
    });
    if (visitorPhone === null) return;

    const visitorEmail = await promptAction({
      title: "Visitor email",
      body: "Optional. Leave blank if you do not have it.",
      label: "Email",
      placeholder: "guest@example.com",
      okLabel: "Check in visitor",
      inputType: "email",
    });
    if (visitorEmail === null) return;

    await apiRequest("/api/admin/church-life/check-ins/usher", {
      method: "POST",
      body: {
        serviceId: service.id,
        checkInSubject: "VISITOR",
        visitorName: String(visitorName || "").trim(),
        visitorPhone: String(visitorPhone || "").trim() || null,
        visitorEmail: String(visitorEmail || "").trim() || null,
      },
    });

    state.operationsServiceRows = [];
    toast("Visitor checked in and added to Church Life.", "success");
    await loadOperationsWorkspace({ view: "services" });
  }

  async function registerDoorPersonFromPrompt() {
    const fullName = await promptAction({
      title: "Register person",
      body: "Create or refresh the member record before check-in.",
      label: "Full name",
      placeholder: "Member full name",
      okLabel: "Next",
    });
    if (fullName === null) return;
    if (!String(fullName || "").trim()) {
      toast("Full name is required.", "error");
      return;
    }

    const phone = await promptAction({
      title: "Phone",
      body: "Phone or email is required.",
      label: "Phone",
      placeholder: "0710000000",
      okLabel: "Next",
    });
    if (phone === null) return;

    const email = await promptAction({
      title: "Email",
      body: "Optional if phone is provided.",
      label: "Email",
      placeholder: "member@example.com",
      okLabel: "Next",
      inputType: "email",
    });
    if (email === null) return;
    if (!String(phone || "").trim() && !String(email || "").trim()) {
      toast("Phone or email is required.", "error");
      return;
    }

    const dateOfBirth = await promptAction({
      title: "Date of birth",
      body: "This is required to complete the member record.",
      label: "Date of birth",
      okLabel: "Register person",
      inputType: "date",
    });
    if (dateOfBirth === null) return;
    if (!String(dateOfBirth || "").trim()) {
      toast("Date of birth is required.", "error");
      return;
    }

    const data = await apiRequest("/api/admin/church-life/check-ins/register-member", {
      method: "POST",
      body: {
        fullName: String(fullName || "").trim(),
        phone: String(phone || "").trim() || null,
        email: String(email || "").trim() || null,
        dateOfBirth: String(dateOfBirth || "").trim(),
      },
    });

    const member = data?.member || {};
    const memberRef = String(member?.phone || phone || member?.email || email || "").trim();
    const service = await pickOperationalService();

    toast(data?.existing ? "Person updated and ready." : "Person registered.", "success");

    if (service?.id && memberRef) {
      const confirmCheckIn = await confirmAction({
        title: "Check into service now?",
        body: `Check ${member.fullName || fullName} into ${service.serviceName || "the next service"} on ${formatDateOnly(
          service.serviceDate
        )}?`,
        okLabel: "Check in now",
      });
      if (confirmCheckIn) {
        await apiRequest("/api/admin/church-life/check-ins/usher", {
          method: "POST",
          body: {
            serviceId: service.id,
            memberRef,
          },
        });
        toast("Person registered and checked in.", "success");
      }
    }

    state.operationsServiceRows = [];
    await loadOperationsWorkspace({ view: "services" });
  }

  async function addChildProfileFromPrompt() {
    const parentRef = await promptAction({
      title: "Add child profile",
      body: "Use the parent phone, email, or member ID already in ChurPay.",
      label: "Parent reference",
      placeholder: "0710000000",
      okLabel: "Next",
    });
    if (parentRef === null) return;

    const parent = await resolveOperationsParentMember(parentRef);

    const childName = await promptAction({
      title: "Child name",
      body: `Create a child profile under ${parent.fullName || "this parent"}.`,
      label: "Child name",
      placeholder: "Child full name",
      okLabel: "Next",
    });
    if (childName === null) return;
    if (!String(childName || "").trim()) {
      toast("Child name is required.", "error");
      return;
    }

    const dateOfBirth = await promptAction({
      title: "Date of birth",
      body: "Optional, but helpful for age grouping.",
      label: "Date of birth",
      okLabel: "Next",
      inputType: "date",
    });
    if (dateOfBirth === null) return;

    const schoolGrade = await promptAction({
      title: "School grade",
      body: "Optional. Leave blank if you do not have it.",
      label: "School grade",
      placeholder: "Grade 4",
      okLabel: "Add child",
    });
    if (schoolGrade === null) return;

    await apiRequest(`/api/admin/church-life/member-profiles/${encodeURIComponent(parent.memberPk)}/children`, {
      method: "POST",
      body: {
        childName: String(childName || "").trim(),
        dateOfBirth: String(dateOfBirth || "").trim() || null,
        schoolGrade: String(schoolGrade || "").trim() || null,
        relationship: "CHILD",
        shareAddress: true,
      },
    });

    toast("Child profile added.", "success");
    await loadOperationsWorkspace({ view: "children" });
  }

  async function editChildProfileFromPrompt(rowKey) {
    const row = findOperationsChildRow(rowKey);
    if (!row) throw new Error("Child profile not found.");

    if (String(row?.source || "").toUpperCase() === "WALK_IN") {
      if (!isEditableWalkInChildRow(row)) {
        throw new Error("This returning child needs a saved household profile. Use Add child profile first.");
      }

      const childName = await promptAction({
        title: "Edit walk-in child",
        body: "Update the walk-in record so tomorrow's data is clean.",
        label: "Child name",
        placeholder: "Child name",
        value: row?.childName || row?.childMemberName || "",
        okLabel: "Next",
      });
      if (childName === null) return;
      if (!String(childName || "").trim()) {
        toast("Child name is required.", "error");
        return;
      }

      const parentName = await promptAction({
        title: "Parent name",
        body: "Optional, but recommended.",
        label: "Parent name",
        placeholder: "Parent name",
        value: row?.parentMemberName || row?.parentName || "",
        okLabel: "Next",
      });
      if (parentName === null) return;

      const parentPhone = await promptAction({
        title: "Parent phone",
        body: "Optional.",
        label: "Parent phone",
        placeholder: "0710000000",
        value: row?.parentPhone || "",
        okLabel: "Next",
      });
      if (parentPhone === null) return;

      const parentEmail = await promptAction({
        title: "Parent email",
        body: "Optional.",
        label: "Parent email",
        placeholder: "parent@example.com",
        value: row?.parentEmail || "",
        okLabel: "Save child",
        inputType: "email",
      });
      if (parentEmail === null) return;

      await apiRequest(
        `/api/admin/church-life/children-household/walk-ins/${encodeURIComponent(String(row.selectionKey || "").trim())}`,
        {
          method: "PATCH",
          body: {
            childName: String(childName || "").trim(),
            parentName: String(parentName || "").trim() || null,
            parentPhone: String(parentPhone || "").trim() || null,
            parentEmail: String(parentEmail || "").trim() || null,
          },
        }
      );
    } else {
      const parentMemberPk = String(row?.parentMemberPk || row?.parentMemberPks?.[0] || "").trim();
      const childId = String(row?.id || "").trim();
      if (!parentMemberPk || !childId) {
        throw new Error("This child profile cannot be edited from the current record.");
      }

      const childName = await promptAction({
        title: "Edit child profile",
        body: `Update ${row?.childName || row?.childMemberName || "this child"} for tomorrow's service.`,
        label: "Child name",
        placeholder: "Child name",
        value: row?.childName || row?.childMemberName || "",
        okLabel: "Next",
      });
      if (childName === null) return;
      if (!String(childName || "").trim()) {
        toast("Child name is required.", "error");
        return;
      }

      const dateOfBirth = await promptAction({
        title: "Date of birth",
        body: "Optional.",
        label: "Date of birth",
        value: toDateInputValue(row?.dateOfBirth),
        okLabel: "Next",
        inputType: "date",
      });
      if (dateOfBirth === null) return;

      const schoolGrade = await promptAction({
        title: "School grade",
        body: "Optional.",
        label: "School grade",
        placeholder: "Grade 4",
        value: row?.schoolGrade || "",
        okLabel: "Save child",
      });
      if (schoolGrade === null) return;

      await apiRequest(
        `/api/admin/church-life/member-profiles/${encodeURIComponent(parentMemberPk)}/children/${encodeURIComponent(childId)}`,
        {
          method: "PATCH",
          body: {
            childName: String(childName || "").trim(),
            dateOfBirth: String(dateOfBirth || "").trim() || null,
            schoolGrade: String(schoolGrade || "").trim() || null,
          },
        }
      );
    }

    toast("Child profile updated.", "success");
    await loadOperationsWorkspace({ view: "children" });
  }

  async function checkInChildFromRow(rowKey) {
    const row = findOperationsChildRow(rowKey);
    if (!row) throw new Error("Child profile not found.");

    const service = state.operationsChildrenService?.id
      ? state.operationsChildrenService
      : await pickOperationalService();
    if (!service?.id) throw new Error("Add and publish a service first.");

    const source = String(row?.source || "").toUpperCase();
    const body = { serviceId: service.id };
    if (source === "HOUSEHOLD") {
      body.householdChildId = String(row?.id || row?.householdChildIds?.[0] || "").trim();
    } else {
      body.childName = String(row?.childName || row?.childMemberName || "").trim();
      body.parentName = String(row?.parentMemberName || row?.parentName || "").trim() || null;
      body.parentPhone = String(row?.parentPhone || "").trim() || null;
      body.parentEmail = String(row?.parentEmail || "").trim() || null;
    }

    if (!body.householdChildId && !body.childName) {
      throw new Error("This child record is missing the details needed for check-in.");
    }

    await apiRequest("/api/admin/church-life/children-check-ins", {
      method: "POST",
      body,
    });

    state.operationsServiceRows = [];
    toast("Child checked in.", "success");
    await loadOperationsWorkspace({ view: "children" });
  }

  async function walkInChildCheckinFromPrompt() {
    const service = state.operationsChildrenService?.id
      ? state.operationsChildrenService
      : await pickOperationalService();
    if (!service?.id) throw new Error("Add and publish a service first.");

    const childName = await promptAction({
      title: "Walk-in child check-in",
      body: `Capture a walk-in child for ${service.serviceName || "the next service"} on ${formatDateOnly(service.serviceDate)}.`,
      label: "Child name",
      placeholder: "Child name",
      okLabel: "Next",
    });
    if (childName === null) return;
    if (!String(childName || "").trim()) {
      toast("Child name is required.", "error");
      return;
    }

    const parentName = await promptAction({
      title: "Parent name",
      body: "Optional, but recommended.",
      label: "Parent name",
      placeholder: "Parent name",
      okLabel: "Next",
    });
    if (parentName === null) return;

    const parentPhone = await promptAction({
      title: "Parent phone",
      body: "Optional.",
      label: "Parent phone",
      placeholder: "0710000000",
      okLabel: "Next",
    });
    if (parentPhone === null) return;

    const parentEmail = await promptAction({
      title: "Parent email",
      body: "Optional.",
      label: "Parent email",
      placeholder: "parent@example.com",
      okLabel: "Check in child",
      inputType: "email",
    });
    if (parentEmail === null) return;

    await apiRequest("/api/admin/church-life/children-check-ins", {
      method: "POST",
      body: {
        serviceId: service.id,
        childName: String(childName || "").trim(),
        parentName: String(parentName || "").trim() || null,
        parentPhone: String(parentPhone || "").trim() || null,
        parentEmail: String(parentEmail || "").trim() || null,
      },
    });

    state.operationsServiceRows = [];
    toast("Walk-in child checked in.", "success");
    await loadOperationsWorkspace({ view: "children" });
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
    state.currentTab = pathToTab(window.location.pathname);
    await Promise.all([loadProfile(), loadChurch(), loadFunds()]);
    await loadPortalSettings();
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
    state.growthSubscription = null;
    state.operationsView = "overview";
    state.operationsGroupId = "";
    state.operationsGroupRows = [];
    state.statementFilters = { from: "", to: "", allStatuses: false };
    renderQrCard(null);
    syncChurchLifeViewUi();
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
    state.currentTab = pathToTab(window.location.pathname);
    await Promise.all([loadProfile(), loadChurch(), loadFunds()]);
    await loadPortalSettings();
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
        switchTab(tab, true);
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

    document.addEventListener("click", (event) => {
      const origin = event.target;
      if (!(origin instanceof Element)) return;
      const operationsTrigger = origin.closest("[data-operations-view]");
      if (operationsTrigger) {
        const view = String(operationsTrigger.getAttribute("data-operations-view") || "").trim();
        if (view) {
          setOperationsView(view);
          return;
        }
      }
      const trigger = origin.closest("[data-jump-tab]");
      if (!trigger) return;
      const tab = String(trigger.getAttribute("data-jump-tab") || "").trim();
      if (!tab) return;
      switchTab(tab, true);
    });

    el.chartRangeSelect.addEventListener("change", async () => {
      state.chartDays = Number(el.chartRangeSelect.value || 14);
      try {
        await loadDashboard();
      } catch (err) {
        toast(err.message || "Could not refresh chart", "error");
      }
    });

    if (el.financeWeeksSelect) {
      el.financeWeeksSelect.addEventListener("change", async () => {
        state.financeWeeks = Math.max(4, Math.min(52, Number(el.financeWeeksSelect.value || 12)));
        try {
          await loadFinanceWorkspace();
        } catch (err) {
          toast(err.message || "Could not refresh finance workspace", "error");
        }
      });
    }

    if (el.refreshFinanceBtn) {
      el.refreshFinanceBtn.addEventListener("click", async () => {
        try {
          setBusy(el.refreshFinanceBtn, true, "Refreshing...", "Refresh finance");
          await loadFinanceWorkspace();
        } catch (err) {
          toast(err.message || "Could not refresh finance workspace", "error");
        } finally {
          setBusy(el.refreshFinanceBtn, false, "Refreshing...", "Refresh finance");
        }
      });
    }

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

    if (el.refreshOperationsBtn) {
      el.refreshOperationsBtn.addEventListener("click", async () => {
        try {
          setBusy(el.refreshOperationsBtn, true, "Refreshing...", "Refresh Church Life");
          await loadOperationsWorkspace();
        } catch (err) {
          toast(err.message || "Could not refresh Church Life", "error");
        } finally {
          setBusy(el.refreshOperationsBtn, false, "Refreshing...", "Refresh Church Life");
        }
      });
    }

    if (el.createFollowupBtn) {
      el.createFollowupBtn.addEventListener("click", () => {
        createFollowupFromPrompt().catch((err) => toast(err?.message || "Could not create follow-up.", "error"));
      });
    }

    if (el.runAutoFollowupsBtn) {
      el.runAutoFollowupsBtn.addEventListener("click", async () => {
        try {
          setBusy(el.runAutoFollowupsBtn, true, "Running...", "Run auto follow-ups");
          await runAutoFollowups();
        } catch (err) {
          toast(err?.message || "Could not run auto follow-ups.", "error");
        } finally {
          setBusy(el.runAutoFollowupsBtn, false, "Running...", "Run auto follow-ups");
        }
      });
    }

    if (el.sendBroadcastBtn) {
      el.sendBroadcastBtn.addEventListener("click", () => {
        sendBroadcastFromPrompt().catch((err) => toast(err?.message || "Could not send broadcast.", "error"));
      });
    }

    if (el.createGroupBtn) {
      el.createGroupBtn.addEventListener("click", () => {
        createGroupFromPrompt().catch((err) => toast(err?.message || "Could not create group.", "error"));
      });
    }

    if (el.createServiceBtn) {
      el.createServiceBtn.addEventListener("click", () => {
        createOrEditServiceFromPrompt().catch((err) => toast(err?.message || "Could not save service.", "error"));
      });
    }

    if (el.registerDoorPersonBtn) {
      el.registerDoorPersonBtn.addEventListener("click", () => {
        registerDoorPersonFromPrompt().catch((err) => toast(err?.message || "Could not register person.", "error"));
      });
    }

    if (el.addChildProfileBtn) {
      el.addChildProfileBtn.addEventListener("click", () => {
        addChildProfileFromPrompt().catch((err) => toast(err?.message || "Could not add child profile.", "error"));
      });
    }

    if (el.walkInChildCheckinBtn) {
      el.walkInChildCheckinBtn.addEventListener("click", () => {
        walkInChildCheckinFromPrompt().catch((err) => toast(err?.message || "Could not check in child.", "error"));
      });
    }

    if (el.operationsGroupsBody) {
      el.operationsGroupsBody.addEventListener("click", (event) => {
        const origin = event.target;
        if (!(origin instanceof Element)) return;
        const actionEl = origin.closest("button[data-group-action][data-id]");
        if (!actionEl) return;
        const groupId = actionEl.getAttribute("data-id");
        const action = actionEl.getAttribute("data-group-action");
        if (!groupId || !action) return;
        if (action === "open") {
          state.operationsGroupId = groupId;
          void loadOperationsWorkspace({ view: "groups" });
          return;
        }
        if (action === "toggle") {
          void toggleGroupActive(groupId).catch((err) => toast(err?.message || "Could not update group.", "error"));
        }
      });
    }

    if (el.operationsServicesBody) {
      el.operationsServicesBody.addEventListener("click", (event) => {
        const origin = event.target;
        if (!(origin instanceof Element)) return;
        const actionEl = origin.closest("button[data-service-action][data-id]");
        if (!actionEl) return;
        const serviceId = actionEl.getAttribute("data-id");
        const action = actionEl.getAttribute("data-service-action");
        if (!serviceId || !action) return;
        if (action === "member") {
          void checkInMemberToServiceFromPrompt(serviceId).catch((err) => toast(err?.message || "Could not check in member.", "error"));
          return;
        }
        if (action === "visitor") {
          void checkInVisitorToServiceFromPrompt(serviceId).catch((err) => toast(err?.message || "Could not check in visitor.", "error"));
          return;
        }
        if (action === "edit") {
          void createOrEditServiceFromPrompt(serviceId).catch((err) => toast(err?.message || "Could not update service.", "error"));
          return;
        }
        if (action === "stream") {
          void openServiceStreamLink(serviceId).catch((err) => toast(err?.message || "Could not open stream link.", "error"));
        }
      });
    }

    if (el.operationsChildrenBody) {
      el.operationsChildrenBody.addEventListener("click", (event) => {
        const origin = event.target;
        if (!(origin instanceof Element)) return;
        const actionEl = origin.closest("button[data-child-profile-action][data-id]");
        if (!actionEl) return;
        const rowKey = actionEl.getAttribute("data-id");
        const action = actionEl.getAttribute("data-child-profile-action");
        if (!rowKey || !action) return;
        if (action === "checkin") {
          void checkInChildFromRow(rowKey).catch((err) => toast(err?.message || "Could not check in child.", "error"));
          return;
        }
        if (action === "edit") {
          void editChildProfileFromPrompt(rowKey).catch((err) => toast(err?.message || "Could not update child.", "error"));
        }
      });
    }

    if (el.operationsChildrenCheckinsBody) {
      el.operationsChildrenCheckinsBody.addEventListener("click", (event) => {
        const origin = event.target;
        if (!(origin instanceof Element)) return;
        const actionEl = origin.closest("button[data-child-checkin-action][data-id]");
        if (!actionEl) return;
        const checkInId = actionEl.getAttribute("data-id");
        const action = actionEl.getAttribute("data-child-checkin-action");
        if (!checkInId || action !== "pickup") return;
        void pickupChildrenCheckIn(checkInId).catch((err) => toast(err?.message || "Could not record pickup.", "error"));
      });
    }

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

    if (el.growthPlanSelect) {
      el.growthPlanSelect.addEventListener("change", () => {
        syncGrowthPlanHint();
      });
    }

    if (el.requestGrowthActivationBtn) {
      el.requestGrowthActivationBtn.addEventListener("click", () => {
        requestGrowthActivation().catch((err) => {
          toast(err?.message || "Could not activate ChurPay Growth.", "error");
        });
      });
    }

    if (el.refreshGrowthSubscriptionBtn) {
      el.refreshGrowthSubscriptionBtn.addEventListener("click", async () => {
        try {
          setBusy(el.refreshGrowthSubscriptionBtn, true, "Refreshing...", "Refresh status");
          await loadGrowthSubscription();
          toast("Growth status refreshed.", "info", 1600);
        } catch (err) {
          toast(err?.message || "Could not refresh Growth status.", "error");
        } finally {
          setBusy(el.refreshGrowthSubscriptionBtn, false, "Refreshing...", "Refresh status");
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

    window.addEventListener("popstate", () => {
      const parsed = pathToTab(window.location.pathname);
      switchTab(parsed, false);
    });
  }

  async function init() {
    installBrandLogoFallback();
    bindEvents();
    syncChurchLifeViewUi();
    setSidebarOpen(false);

    const preferredTheme = window.localStorage.getItem(THEME_KEY) || "system";
    applyTheme(preferredTheme);
    startSystemThemeSync();

    showLoading(true);

    const storedToken = window.localStorage.getItem(TOKEN_KEY);
    if (!storedToken) {
      stopInactivityWatch();
      showAuth(true);
      syncGrowthPlanHint();
      switchTab(pathToTab(window.location.pathname), false);
      showLoading(false);
      return;
    }

    try {
      setToken(storedToken);
      showAuth(false);
      startInactivityWatch();
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
