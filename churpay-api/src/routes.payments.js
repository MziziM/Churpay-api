import express from "express";
import bcrypt from "bcryptjs";
import { db } from "./db.js";
import { requireAuth, requireAdmin, requireStaff } from "./auth.js";
import { handlePayfastItn, payfastItnRawParser } from "./routes.webhooks.js";
import { createNotification } from "./notifications.js";
import {
  churchGrowthSegmentsCatalog,
  generateChurchAutoFollowups,
  listChurchBroadcastAudience,
  normalizeBroadcastSegmentKey,
  previewAutoFollowups,
  sendChurchInAppBroadcast,
} from "./church-growth-jobs.js";
import { upsertChurchDonor } from "./church-donors.js";
import { PaymentGatewayFactory } from "./payments/index.js";
import {
  connectChurchPayfastCredentials,
  disconnectChurchPayfastCredentials,
  getChurchPayfastStatus,
  normalizePayfastMode,
  recordChurchPayfastConnectAttempt,
  validatePayfastCredentialConnection,
} from "./payfast-church.js";
import {
  churchSubscriptionPlanConfig,
  computeSubscriptionAccess,
  DEFAULT_CHURCH_SUBSCRIPTION_PLAN_CODE,
  DEFAULT_CHURCH_SUBSCRIPTION_TRIAL_DAYS,
  ensureChurchTrialSubscription,
  loadChurchSubscription,
  normalizeChurchSubscriptionPlanCode,
  normalizeChurchSubscriptionRow,
} from "./church-subscriptions.js";
import crypto from "node:crypto";
import { inflateRawSync } from "node:zlib";

const router = express.Router();
const isProduction = (process.env.NODE_ENV || "").toLowerCase() === "production";
const isAdminRole = (role) => role === "admin" || role === "super";
const ADMIN_PORTAL_TABS = [
  "dashboard",
  "growth",
  "operations",
  "transactions",
  "statements",
  "funds",
  "qr",
  "members",
  "settings",
];
const ACCOUNTANT_CONFIGURABLE_TABS = ["dashboard", "transactions", "statements", "funds", "qr", "members"];
const ACCOUNTANT_DEFAULT_TABS = ["dashboard", "transactions", "statements"];
const DEFAULT_PLATFORM_FEE_FIXED = 2.5;
const DEFAULT_PLATFORM_FEE_PCT = 0.0075;
const DEFAULT_SUPERADMIN_CUT_PCT = 1.0;
const DEFAULT_CASH_RECORD_FEE_ENABLED = false;
const DEFAULT_CASH_RECORD_FEE_RATE = 0.0075;
const DEFAULT_RECURRING_GIVING_ENABLED = true;
const CHURPAY_GROWTH_SUBSCRIPTION_SOURCE = "CHURPAY_GROWTH_SUBSCRIPTION";
const CHURCH_EVENT_TICKET_SOURCE = "EVENT_TICKET";
const CHURCH_EVENT_TICKET_MAX_QUANTITY = 20;
const CHURCH_LIFE_ACCESS_REQUIRED_CODE = "CHURPAY_GROWTH_ACTIVE_REQUIRED";
const CHURCH_LIFE_CHECKIN_METHODS = new Set(["TAP", "QR", "USHER"]);
const CHURCH_LIFE_CHILD_CHECKIN_METHODS = new Set(["TEACHER", "QR", "USHER", "KIOSK", "MANUAL"]);
const CHURCH_LIFE_CHILD_CHECKOUT_METHODS = new Set(["PARENT", "TEACHER", "USHER", "KIOSK", "MANUAL"]);
const CHURCH_LIFE_PRAYER_CATEGORIES = new Set([
  "GENERAL",
  "HEALTH",
  "FAMILY",
  "FINANCIAL",
  "GRIEF",
  "MENTAL_HEALTH",
  "ADDICTION",
  "THANKSGIVING",
  "OTHER",
]);
const CHURCH_LIFE_PRAYER_SENSITIVE_CATEGORIES = new Set(["MENTAL_HEALTH", "ADDICTION", "GRIEF", "HEALTH"]);
const CHURCH_LIFE_PRAYER_VISIBILITIES = new Set(["RESTRICTED", "TEAM_ONLY", "CHURCH"]);
const CHURCH_LIFE_PRAYER_TEAMS = new Set(["PRAYER_TEAM", "CARE_TEAM", "PASTORAL"]);
const CHURCH_LIFE_PRAYER_ASSIGNMENT_ROLES = new Set(["PRAYER_TEAM", "CARE_TEAM", "PASTORAL", "PRAYER_TEAM_LEAD"]);
const CHURCH_LIFE_EVENT_STATUSES = new Set(["DRAFT", "PUBLISHED", "CANCELLED"]);
const CHURCH_EVENT_REGISTRATION_STATUSES = new Set(["REGISTERED", "CHECKED_IN", "CANCELLED"]);
const CHURCH_EVENT_REGISTRATION_ACTIVE_STATUSES = ["REGISTERED", "CHECKED_IN"];
const CHURCH_GROUP_TYPES = new Set(["MINISTRY", "SMALL_GROUP", "SERVE_TEAM", "CLASS", "OTHER"]);
const CHURCH_GROUP_MEMBER_ROLES = new Set(["LEADER", "ASSISTANT", "MEMBER", "VOLUNTEER"]);
const CHURCH_GROUP_MEETING_STATUSES = new Set(["SCHEDULED", "COMPLETED", "CANCELLED"]);
const CHURCH_GROUP_ATTENDANCE_STATUSES = new Set(["PRESENT", "ABSENT", "LATE", "EXCUSED"]);
const CHURCH_FAMILY_RELATIONSHIP_TYPES = new Set([
  "SPOUSE",
  "PARENT",
  "CHILD",
  "MOTHER",
  "FATHER",
  "SIBLING",
  "BROTHER",
  "SISTER",
  "GUARDIAN",
  "GRANDPARENT",
  "GRANDCHILD",
  "AUNT_UNCLE",
  "NIECE_NEPHEW",
  "COUSIN",
  "IN_LAW",
  "STEP_PARENT",
  "STEP_CHILD",
  "OTHER",
]);
const CHURCH_FAMILY_RECIPROCAL_TYPE = Object.freeze({
  SPOUSE: "SPOUSE",
  PARENT: "CHILD",
  CHILD: "PARENT",
  MOTHER: "CHILD",
  FATHER: "CHILD",
  SIBLING: "SIBLING",
  BROTHER: "SIBLING",
  SISTER: "SIBLING",
  GUARDIAN: "CHILD",
  GRANDPARENT: "GRANDCHILD",
  GRANDCHILD: "GRANDPARENT",
  AUNT_UNCLE: "NIECE_NEPHEW",
  NIECE_NEPHEW: "AUNT_UNCLE",
  COUSIN: "COUSIN",
  IN_LAW: "IN_LAW",
  STEP_PARENT: "STEP_CHILD",
  STEP_CHILD: "STEP_PARENT",
  OTHER: "OTHER",
});
const CHURCH_FOLLOWUP_TYPES = new Set(["VISITOR_CALL", "PRAYER", "COUNSELING", "CARE", "GENERAL"]);
const CHURCH_FOLLOWUP_STATUSES = new Set(["OPEN", "IN_PROGRESS", "CLOSED", "CANCELLED"]);
const CHURCH_FOLLOWUP_PRIORITIES = new Set(["LOW", "MEDIUM", "HIGH", "URGENT"]);
const CHURCH_FOLLOWUP_TASK_STATUSES = new Set(["TODO", "IN_PROGRESS", "DONE", "CANCELLED"]);
const CHURCH_BROADCAST_STATUSES = new Set(["DRAFT", "SENT", "PARTIAL", "FAILED"]);
const VOLUNTEER_TERM_STATUSES = new Set(["ACTIVE", "REVIEW_DUE", "ENDED", "RENEWED"]);
const VOLUNTEER_TERM_END_REASONS = new Set(["PROMOTED", "ENDED", "NON_RENEWED"]);
const VOLUNTEER_SYSTEM_RATINGS = new Set(["NORMAL", "HIGH", "OVERLOAD", "HEALTHY", "NEEDS_ATTENTION", "AT_RISK"]);
const MINISTRY_SCHEDULE_STATUSES = new Set(["DRAFT", "PUBLISHED", "CLOSED"]);
const MINISTRY_SCHEDULE_ASSIGNMENT_STATUSES = new Set(["ASSIGNED", "CONFIRMED", "DECLINED", "NO_SHOW", "SERVED"]);
const CHURCH_LIFE_AUDIT_ACTIONS = new Set([
  "PROFILE_CREATED",
  "PROFILE_UPDATED",
  "ATTENDANCE_AUTO_MARK",
  "ATTENDANCE_OVERRIDE",
  "CHECKIN_OVERRIDE",
  "ATTENDANCE_IMPORT_CSV",
  "PRAYER_ASSIGNED",
  "PRAYER_STATUS_CHANGED",
  "VOLUNTEER_TERM_CREATED",
  "VOLUNTEER_TERM_UPDATED",
  "VOLUNTEER_TERM_ENDED",
  "MINISTRY_CREATED",
  "MINISTRY_UPDATED",
  "MINISTRY_ROLE_CREATED",
  "MINISTRY_ROLE_UPDATED",
  "SCHEDULE_CREATED",
  "SCHEDULE_UPDATED",
  "SCHEDULE_ASSIGNMENT_UPDATED",
]);
const CHURCH_LIFE_AUDIT_ENTITY_TYPES = new Set([
  "MEMBER_PROFILE",
  "ATTENDANCE_RECORD",
  "CHECKIN",
  "IMPORT_BATCH",
  "PRAYER_REQUEST",
  "VOLUNTEER_TERM",
  "MINISTRY_SCHEDULE",
  "MINISTRY_ASSIGNMENT",
  "MINISTRY",
  "MINISTRY_ROLE",
]);
const CHURCH_LIFE_PERMISSION_DENIED_CODE = "CHURCH_LIFE_PERMISSION_DENIED";
const CHURCH_STAFF_ROLES = new Set([
  "super",
  "admin",
  "accountant",
  "finance",
  "pastor",
  "volunteer",
  "usher",
  "teacher",
  "prayer_team_lead",
]);
const CHURCH_ROLE_ALIASES = Object.freeze({
  accountant: "finance",
});
const CHURCH_LIFE_ROLE_TAB_DEFAULTS = Object.freeze({
  finance: ["dashboard", "transactions", "statements", "funds", "operations"],
  pastor: ["dashboard", "growth", "operations", "members"],
  prayer_team_lead: ["dashboard", "operations"],
  volunteer: ["dashboard", "operations"],
  usher: ["dashboard", "operations"],
  teacher: ["dashboard", "operations"],
});
const CHURCH_LIFE_PERMISSIONS = Object.freeze({
  super: ["*"],
  admin: ["*"],
  pastor: [
    "ops.overview.read",
    "ops.attendance.read",
    "ops.attendance.write",
    "ops.insights.read",
    "services.read",
    "services.write",
    "checkins.live.read",
    "checkins.usher.write",
    "checkins.attendance.write",
    "checkins.import.write",
    "checkins.contact.read",
    "apologies.read",
    "prayer.assignments.read",
    "prayer.assignments.write",
    "prayer.requests.read",
    "prayer.requests.write",
    "prayer.sensitive.read",
    "events.read",
    "events.write",
    "profiles.read",
    "profiles.write",
    "profiles.consent.read",
    "profiles.consent.write",
    "profiles.notes.read",
    "groups.read",
    "groups.write",
    "followups.read",
    "followups.write",
    "followups.tasks.read",
    "followups.tasks.write",
    "followups.sensitive.read",
    "audit.read",
    "broadcasts.read",
    "broadcasts.write",
    "autofollowups.preview",
    "autofollowups.run",
    "volunteers.read",
    "volunteers.write",
    "schedules.read",
    "schedules.write",
  ],
  finance: [
    "ops.overview.read",
    "ops.attendance.read",
    "ops.insights.read",
    "services.read",
    "checkins.live.read",
    "apologies.read",
    "events.read",
    "profiles.read",
    "groups.read",
    "followups.read",
    "followups.tasks.read",
    "audit.read",
    "broadcasts.read",
    "autofollowups.preview",
    "schedules.read",
  ],
  prayer_team_lead: [
    "ops.overview.read",
    "services.read",
    "prayer.requests.read",
    "prayer.assignments.read",
    "prayer.assignments.write",
    "volunteers.read",
    "schedules.read",
  ],
  volunteer: [
    "ops.overview.read",
    "ops.attendance.read",
    "services.read",
    "checkins.live.read",
    "checkins.usher.write",
    "apologies.read",
    "events.read",
    "followups.read",
    "followups.write",
    "followups.tasks.read",
    "followups.tasks.write",
    "followups.sensitive.read",
    "autofollowups.preview",
    "volunteers.read",
    "schedules.read",
  ],
  usher: [
    "services.read",
    "checkins.live.read",
    "checkins.usher.write",
    "children.household.read",
    "children.checkins.read",
    "children.checkins.write",
    "children.pickups.write",
    "children.contact.read",
    "followups.read",
    "followups.write",
    "followups.tasks.read",
    "followups.tasks.write",
    "followups.sensitive.read",
    "schedules.read",
  ],
  teacher: [
    "ops.overview.read",
    "services.read",
    "children.household.read",
    "children.checkins.read",
    "children.checkins.write",
    "children.pickups.write",
    "children.contact.read",
    "schedules.read",
  ],
});
const RECURRING_COMING_SOON_MESSAGE =
  "Recurring giving is coming soon. Please use one-time PayFast or cash for now.";
const RECURRING_FREQUENCY_CODES = new Set([1, 2, 3, 4, 5, 6]);
const RECURRING_DEFAULT_FREQUENCY = 3; // monthly
const RECURRING_DEFAULT_CYCLES = 0; // 0 = indefinite in PayFast
const DEFAULT_CHURCH_LIFE_CHILD_MAX_AGE = 12;
const DEFAULT_CHURCH_LIFE_YOUTH_MAX_AGE = 17;
const DEFAULT_CHURCH_LIFE_SENIOR_MIN_AGE = 60;
const CHURCH_MEMBER_AGE_GROUPS = new Set(["CHILD", "YOUTH", "ADULT", "SENIOR", "UNKNOWN"]);
const CHURCH_HOUSEHOLD_CHILD_GENDERS = new Set(["MALE", "FEMALE", "OTHER", "UNSPECIFIED"]);
const CHURCH_MEMBER_BAPTISM_STATUSES = new Set(["YES", "NO", "UNKNOWN"]);
const CHURCH_MEMBER_IMPORT_DUPLICATE_MODES = new Set(["skip", "update"]);
const CHURCH_MEMBER_IMPORT_MAX_ROWS = 6000;
const CHURCH_MEMBER_IMPORT_MAX_FILE_BYTES = 12 * 1024 * 1024;
const CHURCH_CAMPUS_STATUSES = new Set(["ACTIVE", "INACTIVE"]);
const EXTERNAL_GIVING_SOURCE = "EXTERNAL_GIVE";
const DONOR_ADMIN_ROLES = new Set(["super", "admin", "finance"]);
const CHURCH_MEMBER_IMPORT_TARGET_FIELDS = Object.freeze([
  { key: "fullName", label: "Full Name" },
  { key: "memberId", label: "Member ID" },
  { key: "email", label: "Email" },
  { key: "phone", label: "Phone" },
  { key: "dateOfBirth", label: "Date Of Birth" },
  { key: "joinDate", label: "Join Date" },
  { key: "householdName", label: "Household Name" },
  { key: "householdRole", label: "Household Role" },
  { key: "addressLine1", label: "Address Line 1" },
  { key: "addressLine2", label: "Address Line 2" },
  { key: "suburb", label: "Suburb" },
  { key: "city", label: "City" },
  { key: "province", label: "Province" },
  { key: "postalCode", label: "Postal Code" },
  { key: "country", label: "Country" },
  { key: "alternatePhone", label: "Alternate Phone" },
  { key: "whatsappNumber", label: "WhatsApp Number" },
  { key: "occupation", label: "Occupation" },
  { key: "emergencyContactName", label: "Emergency Contact Name" },
  { key: "emergencyContactPhone", label: "Emergency Contact Phone" },
  { key: "emergencyContactRelation", label: "Emergency Contact Relation" },
  { key: "ministryTags", label: "Ministry Tags" },
  { key: "consentData", label: "Data Consent" },
  { key: "consentContact", label: "Contact Consent" },
  { key: "notes", label: "Notes" },
  { key: "baptismStatus", label: "Baptism Status" },
]);

function normalizeAdminPortalTabs(value, { includeSettings = false } = {}) {
  const allowlist = includeSettings ? ADMIN_PORTAL_TABS : ACCOUNTANT_CONFIGURABLE_TABS;
  const list = Array.isArray(value) ? value : [];
  const seen = new Set();
  const out = [];
  for (const raw of list) {
    const key = String(raw || "").trim().toLowerCase();
    if (!key) continue;
    if (!allowlist.includes(key)) continue;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(key);
  }
  return out;
}

function normalizeChurchStaffRole(role) {
  const raw = String(role || "")
    .trim()
    .toLowerCase();
  return CHURCH_ROLE_ALIASES[raw] || raw;
}

function isChurchStaffRole(role) {
  return CHURCH_STAFF_ROLES.has(normalizeChurchStaffRole(role));
}

function getChurchLifePermissionSet(role) {
  const normalizedRole = normalizeChurchStaffRole(role);
  const list = CHURCH_LIFE_PERMISSIONS[normalizedRole] || [];
  return new Set(list);
}

function hasChurchLifePermission(accessOrRole, permission) {
  const key = String(permission || "").trim();
  if (!key) return false;
  const permissions =
    accessOrRole && accessOrRole.permissions instanceof Set
      ? accessOrRole.permissions
      : getChurchLifePermissionSet(accessOrRole);
  return permissions.has("*") || permissions.has(key);
}

function getChurchLifeAccess(req) {
  if (req.churchLifeAccess && req.churchLifeAccess.permissions instanceof Set) {
    return req.churchLifeAccess;
  }
  const role = normalizeChurchStaffRole(req.user?.role);
  const access = {
    role,
    permissions: getChurchLifePermissionSet(role),
  };
  req.churchLifeAccess = access;
  return access;
}

function requireChurchLifeStaff(req, res, next) {
  return requireAuth(req, res, () => {
    if (!isChurchStaffRole(req.user?.role)) {
      return res.status(403).json({ error: "Staff role required", code: CHURCH_LIFE_PERMISSION_DENIED_CODE });
    }
    getChurchLifeAccess(req);
    return next();
  });
}

function requireChurchLifePermission(permission, message = "Forbidden") {
  return (req, res, next) => {
    const access = getChurchLifeAccess(req);
    if (hasChurchLifePermission(access, permission)) return next();
    return res.status(403).json({
      error: message,
      code: CHURCH_LIFE_PERMISSION_DENIED_CODE,
      role: access.role,
      permission,
    });
  };
}

async function loadAdminPortalSettingsForChurch(churchId) {
  if (!churchId) return {};
  try {
    const row = await db.oneOrNone("select admin_portal_settings from churches where id=$1", [churchId]);
    const settings = row?.admin_portal_settings;
    if (!settings || typeof settings !== "object" || Array.isArray(settings)) return {};
    return settings;
  } catch (err) {
    // Older DBs may not have the column yet. Treat as default settings.
    if (err?.code === "42703" || err?.code === "42P01") return {};
    throw err;
  }
}

async function getAdminPortalAccess({ role, churchId }) {
  const normalizedRole = normalizeChurchStaffRole(role);
  const settings = await loadAdminPortalSettingsForChurch(churchId);
  const churchOperations = await loadChurchOperationsSubscription(churchId);
  const operationsEnabled = normalizedRole === "super" ? true : !!churchOperations?.hasAccess;

  if (normalizedRole === "admin" || normalizedRole === "super") {
    const allowedTabs = operationsEnabled
      ? ADMIN_PORTAL_TABS.slice()
      : ADMIN_PORTAL_TABS.filter((tab) => tab !== "operations");
    return {
      role: normalizedRole,
      allowedTabs,
      settings,
      churchOperations: { ...churchOperations, hasAccess: operationsEnabled },
    };
  }

  let allowedTabs = [];
  if (normalizedRole === "finance") {
    const configured = normalizeAdminPortalTabs(settings?.accountantTabs || []);
    const base = configured.length ? configured : ACCOUNTANT_DEFAULT_TABS.slice();
    allowedTabs = normalizeAdminPortalTabs([...base, "operations"], { includeSettings: true });
  } else if (
    normalizedRole === "pastor" ||
    normalizedRole === "volunteer" ||
    normalizedRole === "usher" ||
    normalizedRole === "teacher" ||
    normalizedRole === "prayer_team_lead"
  ) {
    allowedTabs = normalizeAdminPortalTabs(CHURCH_LIFE_ROLE_TAB_DEFAULTS[normalizedRole] || [], {
      includeSettings: true,
    });
  }

  if (!operationsEnabled) {
    allowedTabs = allowedTabs.filter((tab) => tab !== "operations");
  }
  if (!allowedTabs.length) {
    allowedTabs = ["dashboard"];
  }

  return {
    role: normalizedRole,
    allowedTabs,
    settings,
    churchOperations: { ...churchOperations, hasAccess: !!churchOperations?.hasAccess },
  };
}

function requireAdminPortalTabsAny(...tabs) {
  const required = normalizeAdminPortalTabs(tabs, { includeSettings: true });
  return async (req, res, next) => {
    try {
      const role = normalizeChurchStaffRole(req.user?.role);
      if (!isChurchStaffRole(role)) return res.status(403).json({ error: "Forbidden" });

      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const access = await getAdminPortalAccess({ role, churchId });
      const ok = required.some((tab) => access.allowedTabs.includes(tab));
      if (!ok) return res.status(403).json({ error: "Forbidden" });

      return next();
    } catch (err) {
      console.error("[admin/portal-settings] tab guard error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  };
}

function makeMpaymentId() {
  return "CP-" + crypto.randomBytes(8).toString("hex").toUpperCase();
}

function makeCashReference() {
  return "CASH-" + crypto.randomBytes(8).toString("hex").toUpperCase();
}

function makeGivingLinkToken() {
  // 32 bytes -> url-safe token (Node 20+ supports base64url encoding).
  return crypto.randomBytes(32).toString("base64url");
}

function makeRecurringReference() {
  return "SUB-" + crypto.randomBytes(8).toString("hex").toUpperCase();
}

function toCurrencyNumber(value) {
  const rounded = Math.round(Number(value) * 100) / 100;
  return Number.isFinite(rounded) ? Number(rounded.toFixed(2)) : 0;
}

function parseRecurringFrequency(raw) {
  if (typeof raw === "number" && Number.isInteger(raw) && RECURRING_FREQUENCY_CODES.has(raw)) {
    return raw;
  }

  if (typeof raw === "string" && raw.trim()) {
    const key = raw.trim().toLowerCase();
    if (/^\d+$/.test(key)) {
      const n = Number(key);
      if (RECURRING_FREQUENCY_CODES.has(n)) return n;
    }

    // PayFast frequency codes
    // 1=weekly, 2=biweekly, 3=monthly, 4=quarterly, 5=biannually, 6=annually
    const aliases = {
      weekly: 1,
      biweekly: 2,
      fortnightly: 2,
      monthly: 3,
      quarterly: 4,
      biannually: 5,
      semiannually: 5,
      annually: 6,
      yearly: 6,
    };
    if (Object.prototype.hasOwnProperty.call(aliases, key)) return aliases[key];
  }

  return null;
}

function parsePositiveInt(raw, fallback = null) {
  if (raw === null || typeof raw === "undefined" || raw === "") return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || !Number.isInteger(value)) return null;
  return value;
}

function parseIsoDateOnly(raw) {
  if (typeof raw !== "string") return null;
  const v = raw.trim();
  if (!v) return null;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(v)) return null;
  const d = new Date(`${v}T00:00:00.000Z`);
  if (Number.isNaN(d.getTime())) return null;
  return v;
}

function readFeeConfig() {
  const fixed = Number(process.env.PLATFORM_FEE_FIXED ?? DEFAULT_PLATFORM_FEE_FIXED);
  const pct = Number(process.env.PLATFORM_FEE_PCT ?? DEFAULT_PLATFORM_FEE_PCT);
  const superPct = Number(process.env.SUPERADMIN_CUT_PCT ?? DEFAULT_SUPERADMIN_CUT_PCT);
  return {
    fixed: Number.isFinite(fixed) ? fixed : DEFAULT_PLATFORM_FEE_FIXED,
    pct: Number.isFinite(pct) ? pct : DEFAULT_PLATFORM_FEE_PCT,
    superPct: Number.isFinite(superPct) ? superPct : DEFAULT_SUPERADMIN_CUT_PCT,
  };
}

function readCashFeeConfig() {
  const enabledRaw = String(process.env.CASH_RECORD_FEE_ENABLED ?? DEFAULT_CASH_RECORD_FEE_ENABLED).toLowerCase();
  const enabled = ["1", "true", "yes", "on"].includes(enabledRaw);
  const rate = Number(process.env.CASH_RECORD_FEE_RATE ?? DEFAULT_CASH_RECORD_FEE_RATE);
  return {
    enabled,
    rate: Number.isFinite(rate) && rate >= 0 ? rate : DEFAULT_CASH_RECORD_FEE_RATE,
  };
}

function readRecurringConfig() {
  const forceEnabled = parseEnvBoolean(process.env.RECURRING_GIVING_FORCE_ENABLE) === true;
  const parsed = parseEnvBoolean(process.env.RECURRING_GIVING_ENABLED);
  const baseEnabled = typeof parsed === "boolean" ? parsed : DEFAULT_RECURRING_GIVING_ENABLED;
  const enabled = forceEnabled ? true : baseEnabled;
  return { enabled, forceEnabled };
}

function normalizeChurchOperationsSubscription(row) {
  const normalized = normalizeChurchSubscriptionRow(row);
  return {
    ...normalized,
    plan: normalized.planCode,
    startedAt: normalized.trialStartsAt || normalized.currentPeriodStart || null,
    cancelledAt: normalized.canceledAt || null,
  };
}

function hasChurchOperationsAccess(subscription) {
  return computeSubscriptionAccess(subscription).hasAccess;
}

async function loadChurchOperationsSubscription(churchId) {
  return loadChurchSubscription(churchId);
}

function parseDateOnlyOrNull(value) {
  const text = String(value || "").trim();
  if (!text) return null;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return null;
  const parsed = new Date(`${text}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime())) return null;
  return text;
}

function parseNonNegativeWholeNumber(value, fieldName, { required = false } = {}) {
  const text = String(value ?? "").trim();
  if (!text) {
    if (required) return { error: `${fieldName} is required.` };
    return { value: 0 };
  }
  if (!/^\d+$/.test(text)) return { error: `${fieldName} must be a whole number.` };
  const n = Number(text);
  if (!Number.isFinite(n) || n < 0) return { error: `${fieldName} must be 0 or more.` };
  return { value: Math.trunc(n) };
}

function parseUuidOrNull(value) {
  const text = String(value ?? "").trim();
  if (!text) return null;
  return UUID_REGEX.test(text) ? text : null;
}

function normalizeAttendanceRow(row) {
  return {
    id: row?.id || null,
    serviceId: row?.serviceId || null,
    serviceName: row?.serviceName || null,
    campusId: row?.campusId || null,
    campusName: row?.campusName || null,
    campusCode: row?.campusCode || null,
    serviceDate: formatDateIsoLike(row?.serviceDate),
    totalAttendance: Number(row?.totalAttendance || 0),
    adultsCount: Number(row?.adultsCount || 0),
    youthCount: Number(row?.youthCount || 0),
    childrenCount: Number(row?.childrenCount || 0),
    firstTimeGuests: Number(row?.firstTimeGuests || 0),
    notes: row?.notes || null,
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function normalizeChurchCampusStatus(value) {
  const key = normalizeUpperToken(value);
  if (CHURCH_CAMPUS_STATUSES.has(key)) return key;
  return "ACTIVE";
}

function normalizeChurchCampusCode(value, fallback = "MAIN") {
  const raw = String(value || fallback || "MAIN").trim().toUpperCase();
  const sanitized = raw.replace(/[^A-Z0-9_-]/g, "").slice(0, 24);
  return sanitized || "MAIN";
}

async function findChurchCampusById(churchId, campusId) {
  if (!churchId || !UUID_REGEX.test(String(campusId || "").trim())) return null;
  return db.oneOrNone(
    `
    select
      id,
      church_id as "churchId",
      name,
      code,
      status,
      created_at as "createdAt",
      updated_at as "updatedAt"
    from church_campuses
    where church_id=$1 and id=$2
    limit 1
    `,
    [churchId, String(campusId).trim()]
  );
}

function normalizeChurchCampusRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    name: String(row?.name || "").trim(),
    code: normalizeChurchCampusCode(row?.code || "MAIN"),
    status: normalizeChurchCampusStatus(row?.status),
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

async function resolveChurchCampusId(churchId, campusInput, { required = false, fieldName = "campusId" } = {}) {
  const campusId = String(campusInput || "").trim();
  if (!campusId) {
    if (required) throw new Error(`${fieldName} is required.`);
    return null;
  }
  if (!UUID_REGEX.test(campusId)) throw new Error(`${fieldName} must be a UUID.`);
  const campus = await findChurchCampusById(churchId, campusId);
  if (!campus) throw new Error(`${fieldName} is not in this church.`);
  return campus.id;
}

async function readChurchAttendanceSummary(churchId, campusId = null) {
  if (!churchId) {
    return {
      servicesTracked: 0,
      avgLast8Services: 0,
      peakAttendance: 0,
      latestServiceDate: null,
      latestAttendance: 0,
    };
  }

  try {
    const row = await db.one(
      `
      with ranked as (
        select
          service_date,
          total_attendance,
          row_number() over (order by service_date desc, updated_at desc) as rn
        from church_attendance_records
        where church_id = $1
          and ($2::uuid is null or campus_id = $2::uuid)
      )
      select
        count(*)::int as "servicesTracked",
        coalesce(round(avg(total_attendance) filter (where rn <= 8), 2), 0)::numeric(10,2) as "avgLast8Services",
        coalesce(max(total_attendance), 0)::int as "peakAttendance",
        (select service_date from ranked where rn = 1) as "latestServiceDate",
        coalesce((select total_attendance from ranked where rn = 1), 0)::int as "latestAttendance"
      from ranked
      `,
      [churchId, campusId]
    );

    return {
      servicesTracked: Number(row?.servicesTracked || 0),
      avgLast8Services: Number(row?.avgLast8Services || 0),
      peakAttendance: Number(row?.peakAttendance || 0),
      latestServiceDate: row?.latestServiceDate ? formatDateIsoLike(row.latestServiceDate) : null,
      latestAttendance: Number(row?.latestAttendance || 0),
    };
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return {
        servicesTracked: 0,
        avgLast8Services: 0,
        peakAttendance: 0,
        latestServiceDate: null,
        latestAttendance: 0,
      };
    }
    throw err;
  }
}

function normalizeUpperToken(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function parseChurchLifeDate(value) {
  const text = String(value || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return null;
  const parsed = new Date(`${text}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime())) return null;
  return text;
}

function parseChurchLifeDateTimeOrNull(value) {
  if (value == null || value === "") return null;
  const parsed = new Date(String(value));
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
}

function daysUntilDateTime(value, now = new Date()) {
  if (!value) return null;
  const target = new Date(String(value));
  if (Number.isNaN(target.getTime())) return null;
  const current = now instanceof Date && !Number.isNaN(now.getTime()) ? now : new Date();
  const diffMs = target.getTime() - current.getTime();
  return Math.ceil(diffMs / (24 * 60 * 60 * 1000));
}

function parseChurchLifeLimit(value, fallback = 20, max = 100) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(Math.max(Math.trunc(n), 1), max);
}

function parseChurchLifeIntEnv(name, fallback) {
  const n = Number(process.env[name]);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function resolveChurchLifeAgeBoundaries() {
  const childMax = Math.min(Math.max(parseChurchLifeIntEnv("CHURCH_LIFE_CHILD_MAX_AGE", DEFAULT_CHURCH_LIFE_CHILD_MAX_AGE), 0), 30);
  const youthMax = Math.min(
    Math.max(parseChurchLifeIntEnv("CHURCH_LIFE_YOUTH_MAX_AGE", DEFAULT_CHURCH_LIFE_YOUTH_MAX_AGE), childMax),
    60
  );
  const seniorMin = Math.max(parseChurchLifeIntEnv("CHURCH_LIFE_SENIOR_MIN_AGE", DEFAULT_CHURCH_LIFE_SENIOR_MIN_AGE), youthMax + 1);
  return { childMax, youthMax, seniorMin };
}

const CHURCH_LIFE_AGE_BOUNDARIES = resolveChurchLifeAgeBoundaries();

function calculateAgeFromDateOfBirth(dateValue, nowValue = new Date()) {
  if (!dateValue) return null;
  const birthDate = dateValue instanceof Date ? dateValue : new Date(`${String(dateValue).slice(0, 10)}T00:00:00.000Z`);
  const nowDate = nowValue instanceof Date ? nowValue : new Date(nowValue);
  if (Number.isNaN(birthDate.getTime()) || Number.isNaN(nowDate.getTime())) return null;
  let age = nowDate.getUTCFullYear() - birthDate.getUTCFullYear();
  const monthDiff = nowDate.getUTCMonth() - birthDate.getUTCMonth();
  if (monthDiff < 0 || (monthDiff === 0 && nowDate.getUTCDate() < birthDate.getUTCDate())) {
    age -= 1;
  }
  if (!Number.isFinite(age) || age < 0 || age > 130) return null;
  return age;
}

function resolveChurchMemberAgeGroup(dateValue) {
  const age = calculateAgeFromDateOfBirth(dateValue);
  if (age === null) return "UNKNOWN";
  if (age <= CHURCH_LIFE_AGE_BOUNDARIES.childMax) return "CHILD";
  if (age <= CHURCH_LIFE_AGE_BOUNDARIES.youthMax) return "YOUTH";
  if (age >= CHURCH_LIFE_AGE_BOUNDARIES.seniorMin) return "SENIOR";
  return "ADULT";
}

function normalizeChurchHouseholdChildGender(value) {
  const key = normalizeUpperToken(value || "UNSPECIFIED");
  return CHURCH_HOUSEHOLD_CHILD_GENDERS.has(key) ? key : "UNSPECIFIED";
}

function normalizeChurchHouseholdRelationship(value) {
  return String(value || "CHILD")
    .trim()
    .slice(0, 60) || "CHILD";
}

function normalizeChurchFamilyRelationshipType(value, fallback = "OTHER") {
  const safeFallback = CHURCH_FAMILY_RELATIONSHIP_TYPES.has(String(fallback || "").toUpperCase())
    ? String(fallback || "").toUpperCase()
    : "OTHER";
  const key = normalizeUpperToken(value || safeFallback);
  return CHURCH_FAMILY_RELATIONSHIP_TYPES.has(key) ? key : safeFallback;
}

function reciprocalChurchFamilyRelationshipType(relationshipType) {
  const key = normalizeChurchFamilyRelationshipType(relationshipType, "OTHER");
  return CHURCH_FAMILY_RECIPROCAL_TYPE[key] || "OTHER";
}

function guessCsvDelimiter(sampleText) {
  const text = String(sampleText || "");
  const commaCount = (text.match(/,/g) || []).length;
  const semicolonCount = (text.match(/;/g) || []).length;
  return semicolonCount > commaCount ? ";" : ",";
}

function parseCsvLine(line, delimiter = ",") {
  const cells = [];
  let current = "";
  let quoted = false;
  const text = String(line || "");

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];
    if (char === '"') {
      const next = text[i + 1];
      if (quoted && next === '"') {
        current += '"';
        i += 1;
        continue;
      }
      quoted = !quoted;
      continue;
    }
    if (char === delimiter && !quoted) {
      cells.push(current);
      current = "";
      continue;
    }
    current += char;
  }

  cells.push(current);
  return cells.map((cell) => String(cell || "").trim());
}

function normalizeCsvHeaderToken(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

function parseChurchLifeCheckinCsv(csvText) {
  const normalized = String(csvText || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
  if (!normalized) return { rows: [], hasHeader: false };

  const lines = normalized
    .split("\n")
    .map((line) => String(line || ""))
    .filter((line) => line.trim());
  if (!lines.length) return { rows: [], hasHeader: false };

  const delimiter = guessCsvDelimiter(lines.slice(0, 3).join("\n"));
  const parsed = lines.map((line) => parseCsvLine(line, delimiter));
  const first = parsed[0] || [];
  const firstTokens = first.map(normalizeCsvHeaderToken);

  const memberHeaderKeys = new Set([
    "member",
    "memberid",
    "memberpk",
    "memberref",
    "memberuuid",
    "identifier",
    "phone",
    "email",
  ]);
  const methodHeaderKeys = new Set(["method", "checkinmethod", "checkin"]);
  const notesHeaderKeys = new Set(["notes", "note", "comment", "comments", "message", "remark", "remarks"]);
  const checkedInAtHeaderKeys = new Set(["checkedinat", "checkedat", "timestamp", "time"]);

  const hasHeader = firstTokens.some(
    (token) =>
      memberHeaderKeys.has(token) ||
      methodHeaderKeys.has(token) ||
      notesHeaderKeys.has(token) ||
      checkedInAtHeaderKeys.has(token)
  );

  const findHeaderIndex = (set) => {
    for (let i = 0; i < firstTokens.length; i += 1) {
      if (set.has(firstTokens[i])) return i;
    }
    return -1;
  };

  const memberRefIndex = hasHeader ? findHeaderIndex(memberHeaderKeys) : 0;
  const methodIndex = hasHeader ? findHeaderIndex(methodHeaderKeys) : 1;
  const notesIndex = hasHeader ? findHeaderIndex(notesHeaderKeys) : 2;
  const checkedInAtIndex = hasHeader ? findHeaderIndex(checkedInAtHeaderKeys) : -1;
  const startLine = hasHeader ? 2 : 1;

  const rows = parsed
    .slice(hasHeader ? 1 : 0)
    .map((cols, index) => ({
      lineNumber: startLine + index,
      memberRef: memberRefIndex >= 0 ? String(cols[memberRefIndex] || "").trim() : "",
      method: methodIndex >= 0 ? String(cols[methodIndex] || "").trim() : "",
      notes: notesIndex >= 0 ? String(cols[notesIndex] || "").trim() : "",
      checkedInAt: checkedInAtIndex >= 0 ? String(cols[checkedInAtIndex] || "").trim() : "",
    }))
    .filter((row) => row.memberRef || row.method || row.notes || row.checkedInAt);

  return { rows, hasHeader };
}

function normalizeChurchMemberBaptismStatus(value, fallback = "UNKNOWN") {
  const normalizedFallback = CHURCH_MEMBER_BAPTISM_STATUSES.has(normalizeUpperToken(fallback))
    ? normalizeUpperToken(fallback)
    : "UNKNOWN";
  const key = normalizeUpperToken(value);
  if (!key) return normalizedFallback;
  if (["YES", "Y", "TRUE", "1", "BAPTISED", "BAPTIZED"].includes(key)) return "YES";
  if (["NO", "N", "FALSE", "0", "NOTBAPTISED", "NOTBAPTIZED"].includes(key)) return "NO";
  if (["UNKNOWN", "UNK", "PENDING", "NA", "N/A"].includes(key)) return "UNKNOWN";
  return normalizedFallback;
}

function normalizeImportText(value, maxLen = 2000) {
  const text = String(value ?? "")
    .trim()
    .slice(0, maxLen);
  return text || null;
}

function normalizeImportEmail(value) {
  const text = String(value ?? "")
    .trim()
    .toLowerCase();
  if (!text) return null;
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(text)) return null;
  return text;
}

function normalizeImportPhone(value) {
  const text = String(value ?? "").trim();
  if (!text) return null;
  const hasPlus = text.startsWith("+");
  const digits = text.replace(/\D+/g, "");
  if (!digits) return null;
  const normalized = hasPlus ? `+${digits}` : digits;
  return normalized.slice(0, 40);
}

function normalizeImportMemberId(value) {
  const text = String(value ?? "")
    .trim()
    .replace(/\s+/g, "")
    .slice(0, 60);
  return text || null;
}

function decodeBase64Payload(input) {
  const text = String(input ?? "").trim();
  if (!text) return Buffer.alloc(0);
  const payload = text.includes(",") && text.startsWith("data:") ? text.slice(text.indexOf(",") + 1) : text;
  return Buffer.from(payload, "base64");
}

function detectMemberImportFileType(fileName, mimeType = "") {
  const name = String(fileName || "")
    .trim()
    .toLowerCase();
  const mime = String(mimeType || "")
    .trim()
    .toLowerCase();
  if (name.endsWith(".csv") || mime.includes("text/csv") || mime.includes("text/plain")) return "csv";
  if (
    name.endsWith(".xlsx") ||
    mime.includes("spreadsheetml") ||
    mime.includes("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
  ) {
    return "xlsx";
  }
  if (name.endsWith(".xls")) return "xls";
  return "unknown";
}

function normalizeImportHeaderLabel(value, index) {
  const text = String(value ?? "")
    .replace(/\s+/g, " ")
    .trim();
  if (!text) return `Column ${index + 1}`;
  return text.slice(0, 120);
}

function uniquifyImportHeaders(values) {
  const seen = new Map();
  return values.map((value, index) => {
    const base = normalizeImportHeaderLabel(value, index);
    const key = base.toLowerCase();
    const count = (seen.get(key) || 0) + 1;
    seen.set(key, count);
    return count > 1 ? `${base} (${count})` : base;
  });
}

function parseCsvTableRows(csvText) {
  const normalized = String(csvText || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
  if (!normalized) return [];
  const lines = normalized
    .split("\n")
    .map((line) => String(line || ""))
    .filter((line) => line.trim());
  if (!lines.length) return [];
  const delimiter = guessCsvDelimiter(lines.slice(0, 4).join("\n"));
  return lines.map((line) => parseCsvLine(line, delimiter));
}

function decodeXmlEntities(value) {
  return String(value || "")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&");
}

function parseXmlAttributes(fragment) {
  const out = {};
  const regex = /([A-Za-z_][\w:.-]*)\s*=\s*(['"])(.*?)\2/g;
  let match;
  while ((match = regex.exec(String(fragment || "")))) {
    out[match[1]] = decodeXmlEntities(match[3]);
  }
  return out;
}

function letterColumnToIndex(col) {
  const text = String(col || "")
    .trim()
    .toUpperCase();
  if (!text) return -1;
  let out = 0;
  for (let i = 0; i < text.length; i += 1) {
    const code = text.charCodeAt(i);
    if (code < 65 || code > 90) return -1;
    out = out * 26 + (code - 64);
  }
  return out - 1;
}

function readZipEntries(buffer) {
  if (!Buffer.isBuffer(buffer) || !buffer.length) {
    throw new Error("Spreadsheet file is empty.");
  }
  const eocdSignature = 0x06054b50;
  const centralSignature = 0x02014b50;
  const localSignature = 0x04034b50;
  const minEocdLength = 22;
  const searchStart = Math.max(0, buffer.length - 65557);
  let eocdOffset = -1;
  for (let i = buffer.length - minEocdLength; i >= searchStart; i -= 1) {
    if (buffer.readUInt32LE(i) === eocdSignature) {
      eocdOffset = i;
      break;
    }
  }
  if (eocdOffset < 0) {
    throw new Error("Invalid XLSX file: central directory not found.");
  }

  const totalEntries = buffer.readUInt16LE(eocdOffset + 10);
  const centralOffset = buffer.readUInt32LE(eocdOffset + 16);
  if (!Number.isFinite(totalEntries) || totalEntries <= 0) {
    throw new Error("XLSX file has no worksheet entries.");
  }

  const entries = new Map();
  let cursor = centralOffset;
  for (let i = 0; i < totalEntries; i += 1) {
    if (cursor + 46 > buffer.length || buffer.readUInt32LE(cursor) !== centralSignature) {
      break;
    }
    const compression = buffer.readUInt16LE(cursor + 10);
    const compressedSize = buffer.readUInt32LE(cursor + 20);
    const fileNameLength = buffer.readUInt16LE(cursor + 28);
    const extraLength = buffer.readUInt16LE(cursor + 30);
    const commentLength = buffer.readUInt16LE(cursor + 32);
    const localHeaderOffset = buffer.readUInt32LE(cursor + 42);
    const fileNameStart = cursor + 46;
    const fileNameEnd = fileNameStart + fileNameLength;
    const fileName = buffer.slice(fileNameStart, fileNameEnd).toString("utf8").replace(/\\/g, "/");
    cursor = fileNameEnd + extraLength + commentLength;
    if (!fileName || fileName.endsWith("/")) continue;

    if (localHeaderOffset + 30 > buffer.length || buffer.readUInt32LE(localHeaderOffset) !== localSignature) {
      continue;
    }
    const localNameLength = buffer.readUInt16LE(localHeaderOffset + 26);
    const localExtraLength = buffer.readUInt16LE(localHeaderOffset + 28);
    const dataStart = localHeaderOffset + 30 + localNameLength + localExtraLength;
    const dataEnd = dataStart + compressedSize;
    if (dataStart < 0 || dataEnd > buffer.length) continue;
    const compressed = buffer.slice(dataStart, dataEnd);

    let content = Buffer.alloc(0);
    if (compression === 0) {
      content = compressed;
    } else if (compression === 8) {
      content = inflateRawSync(compressed);
    } else {
      continue;
    }
    entries.set(fileName, content);
  }

  return entries;
}

function parseXlsxSheetRows(buffer, requestedSheetName = "") {
  const entries = readZipEntries(buffer);
  const workbookXml = entries.get("xl/workbook.xml");
  const workbookRelsXml = entries.get("xl/_rels/workbook.xml.rels");
  if (!workbookXml || !workbookRelsXml) {
    throw new Error("Invalid XLSX file: workbook metadata is missing.");
  }

  const workbookText = workbookXml.toString("utf8");
  const relsText = workbookRelsXml.toString("utf8");
  const sheetMatches = Array.from(workbookText.matchAll(/<sheet\b([^>]*)\/?>/g));
  if (!sheetMatches.length) {
    throw new Error("No sheets found in XLSX file.");
  }

  const relMap = new Map();
  for (const relMatch of relsText.matchAll(/<Relationship\b([^>]*)\/?>/g)) {
    const attrs = parseXmlAttributes(relMatch[1]);
    const id = String(attrs.Id || "").trim();
    const target = String(attrs.Target || "").trim();
    if (!id || !target) continue;
    relMap.set(id, target.replace(/^\/+/, ""));
  }

  const normalizedRequested = String(requestedSheetName || "")
    .trim()
    .toLowerCase();
  let targetSheet = null;
  for (const sheetMatch of sheetMatches) {
    const attrs = parseXmlAttributes(sheetMatch[1]);
    const name = String(attrs.name || "").trim();
    const rid = String(attrs["r:id"] || "").trim();
    if (!rid) continue;
    if (!targetSheet) {
      targetSheet = { name, rid };
    }
    if (normalizedRequested && name.toLowerCase() === normalizedRequested) {
      targetSheet = { name, rid };
      break;
    }
  }
  if (!targetSheet) throw new Error("Could not resolve target sheet in XLSX file.");

  const relTarget = relMap.get(targetSheet.rid);
  if (!relTarget) throw new Error("Invalid XLSX file: sheet relationship is missing.");
  const normalizedTarget = relTarget.replace(/^\.?\/*/, "");
  const fullSheetPath = normalizedTarget.startsWith("xl/") ? normalizedTarget : `xl/${normalizedTarget}`;
  const sheetXml = entries.get(fullSheetPath);
  if (!sheetXml) {
    throw new Error(`Could not read sheet data for \"${targetSheet.name || "Sheet1"}\".`);
  }

  const sharedStringsXml = entries.get("xl/sharedStrings.xml");
  const sharedStrings = [];
  if (sharedStringsXml) {
    const sharedText = sharedStringsXml.toString("utf8");
    for (const siMatch of sharedText.matchAll(/<si\b[^>]*>([\s\S]*?)<\/si>/g)) {
      const body = siMatch[1] || "";
      const tokens = Array.from(body.matchAll(/<t\b[^>]*>([\s\S]*?)<\/t>/g)).map((m) => decodeXmlEntities(m[1] || ""));
      const value = tokens.length ? tokens.join("") : decodeXmlEntities(body.replace(/<[^>]+>/g, ""));
      sharedStrings.push(value);
    }
  }

  const sheetText = sheetXml.toString("utf8");
  const rowMatches = Array.from(sheetText.matchAll(/<row\b[^>]*>([\s\S]*?)<\/row>/g));
  const rows = [];
  for (const rowMatch of rowMatches) {
    const rowBody = rowMatch[1] || "";
    const rowValues = [];
    for (const cellMatch of rowBody.matchAll(/<c\b([^>]*)>([\s\S]*?)<\/c>|<c\b([^>]*)\/>/g)) {
      const attrs = parseXmlAttributes(cellMatch[1] || cellMatch[3] || "");
      const cellBody = cellMatch[2] || "";
      const colRef = String(attrs.r || "").replace(/[0-9]/g, "");
      const colIndex = letterColumnToIndex(colRef);
      if (colIndex < 0) continue;

      const type = String(attrs.t || "").trim().toLowerCase();
      let value = "";
      if (type === "inlineStr".toLowerCase()) {
        const textMatch = cellBody.match(/<t\b[^>]*>([\s\S]*?)<\/t>/);
        value = decodeXmlEntities(textMatch?.[1] || "");
      } else {
        const valueMatch = cellBody.match(/<v\b[^>]*>([\s\S]*?)<\/v>/);
        const raw = decodeXmlEntities(valueMatch?.[1] || "");
        if (type === "s") {
          const index = Number(raw);
          value = Number.isFinite(index) && sharedStrings[index] != null ? sharedStrings[index] : "";
        } else if (type === "b") {
          value = raw === "1" ? "true" : "false";
        } else {
          value = raw;
        }
      }
      rowValues[colIndex] = String(value || "");
    }
    if (rowValues.some((cell) => String(cell || "").trim() !== "")) {
      rows.push(rowValues.map((cell) => String(cell || "").trim()));
    }
  }
  return rows;
}

function parseMemberImportFileTable({ fileName, fileBase64, mimeType, sheetName }) {
  const payload = decodeBase64Payload(fileBase64);
  if (!payload.length) {
    throw new Error("Import file payload is empty.");
  }
  if (payload.length > CHURCH_MEMBER_IMPORT_MAX_FILE_BYTES) {
    throw new Error(`Import file is too large. Max allowed size is ${Math.round(CHURCH_MEMBER_IMPORT_MAX_FILE_BYTES / 1024 / 1024)} MB.`);
  }

  const fileType = detectMemberImportFileType(fileName, mimeType);
  if (fileType === "xls") {
    throw new Error("Legacy .xls files are not supported. Save as .xlsx or .csv and retry.");
  }
  if (fileType === "unknown") {
    throw new Error("Unsupported file type. Upload a .csv or .xlsx file.");
  }

  const rows = fileType === "xlsx" ? parseXlsxSheetRows(payload, sheetName) : parseCsvTableRows(payload.toString("utf8"));
  if (!rows.length) {
    throw new Error("No rows found in the import file.");
  }

  const headers = uniquifyImportHeaders(rows[0]);
  const bodyRows = rows
    .slice(1)
    .map((cells, index) => {
      const values = {};
      headers.forEach((header, headerIndex) => {
        values[header] = String(cells?.[headerIndex] ?? "").trim();
      });
      const hasAnyValue = Object.values(values).some((value) => String(value || "").trim() !== "");
      if (!hasAnyValue) return null;
      return {
        lineNumber: index + 2,
        values,
      };
    })
    .filter(Boolean);

  return {
    fileType,
    headers,
    rows: bodyRows,
  };
}

function normalizeMemberImportHeaderToken(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

function buildMemberImportRecommendedMapping(headers = []) {
  const aliasesByField = {
    fullName: ["fullname", "name", "membername"],
    memberId: ["memberid", "id", "churchmemberid", "membernumber"],
    email: ["email", "emailaddress", "mail"],
    phone: ["phone", "phonenumber", "mobile", "cell", "cellphone", "telephone", "contactnumber"],
    dateOfBirth: ["dateofbirth", "dob", "birthdate", "birth"],
    joinDate: ["joindate", "joinedon", "datejoined", "joineddate"],
    householdName: ["householdname", "household", "familyname"],
    householdRole: ["householdrole", "familyrole", "relationship"],
    addressLine1: ["addressline1", "address1", "streetaddress", "street"],
    addressLine2: ["addressline2", "address2", "unit", "building"],
    suburb: ["suburb", "area"],
    city: ["city", "town"],
    province: ["province", "state", "region"],
    postalCode: ["postalcode", "postcode", "zipcode", "zip"],
    country: ["country", "nation"],
    alternatePhone: ["alternatephone", "altphone", "secondaryphone"],
    whatsappNumber: ["whatsappnumber", "whatsapp", "whatsappphone"],
    occupation: ["occupation", "job", "profession"],
    emergencyContactName: ["emergencycontactname", "emergencyname", "nextofkinname"],
    emergencyContactPhone: ["emergencycontactphone", "emergencyphone", "nextofkinphone"],
    emergencyContactRelation: ["emergencycontactrelation", "emergencyrelation", "nextofkinrelation"],
    ministryTags: ["ministrytags", "tags", "ministries", "groups"],
    consentData: ["consentdata", "dataconsent"],
    consentContact: ["consentcontact", "contactconsent", "marketingconsent"],
    notes: ["notes", "note", "comments", "remarks"],
    baptismStatus: ["baptismstatus", "baptized", "baptised", "baptism"],
  };

  const indexedHeaders = headers.map((header) => ({
    header,
    token: normalizeMemberImportHeaderToken(header),
  }));
  const mapping = {};

  for (const field of CHURCH_MEMBER_IMPORT_TARGET_FIELDS) {
    const aliases = aliasesByField[field.key] || [];
    const hit = indexedHeaders.find((entry) => aliases.includes(entry.token));
    mapping[field.key] = hit ? hit.header : null;
  }

  return mapping;
}

function normalizeMemberImportDuplicateMode(value) {
  const key = String(value || "")
    .trim()
    .toLowerCase();
  return CHURCH_MEMBER_IMPORT_DUPLICATE_MODES.has(key) ? key : "skip";
}

function normalizeMemberImportMapping(mappingRaw, headers = []) {
  const allowed = new Set(headers.map((header) => String(header || "")));
  const recommended = buildMemberImportRecommendedMapping(headers);
  const payload = mappingRaw && typeof mappingRaw === "object" && !Array.isArray(mappingRaw) ? mappingRaw : {};
  const mapping = {};

  for (const field of CHURCH_MEMBER_IMPORT_TARGET_FIELDS) {
    const requested = String(payload[field.key] || "").trim();
    if (requested && allowed.has(requested)) {
      mapping[field.key] = requested;
      continue;
    }
    const fallback = String(recommended[field.key] || "").trim();
    mapping[field.key] = fallback && allowed.has(fallback) ? fallback : null;
  }

  return { mapping, recommendedMapping: recommended };
}

function parseImportDateValue(value) {
  if (value == null || value === "") return { value: null, error: null };
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return { value: value.toISOString().slice(0, 10), error: null };
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    const serial = Number(value);
    if (serial > 0) {
      const millis = Math.round((serial - 25569) * 86400 * 1000);
      const parsed = new Date(millis);
      if (!Number.isNaN(parsed.getTime())) {
        return { value: parsed.toISOString().slice(0, 10), error: null };
      }
    }
  }

  const text = String(value).trim();
  if (!text) return { value: null, error: null };
  const canonical = parseChurchLifeDate(text);
  if (canonical) return { value: canonical, error: null };

  const parsed = new Date(text);
  if (!Number.isNaN(parsed.getTime())) {
    return { value: parsed.toISOString().slice(0, 10), error: null };
  }

  return { value: null, error: `Invalid date value: ${text}` };
}

function parseImportBooleanValue(value) {
  if (value == null || value === "") return undefined;
  return toBoolean(value) === true;
}

function buildMemberImportErrorCsv(rows = []) {
  const header = ["rowNumber", "action", "reason", "dedupeBy", "dedupeValue", "fullName", "email", "phone", "memberId"];
  const lines = [header.map(csvEscape).join(",")];
  for (const row of rows) {
    const out = [
      row?.rowNumber ?? "",
      row?.action || "",
      row?.reason || "",
      row?.dedupeBy || "",
      row?.dedupeValue || "",
      row?.fullName || "",
      row?.email || "",
      row?.phone || "",
      row?.memberId || "",
    ];
    lines.push(out.map(csvEscape).join(","));
  }
  return `${lines.join("\n")}\n`;
}

function mappedImportCell(row, mapping, fieldKey) {
  const header = String(mapping?.[fieldKey] || "").trim();
  if (!header) return undefined;
  if (!row || typeof row !== "object") return undefined;
  const value = row.values?.[header];
  return typeof value === "undefined" ? "" : value;
}

function normalizeImportStringList(value) {
  if (value == null) return undefined;
  const text = String(value || "").trim();
  if (!text) return [];
  return normalizeStringArray(text.split(/[;,]/g), { maxItems: 40, maxLen: 60 });
}

async function loadMemberImportExistingLookup(churchId, preparedRows) {
  const emails = [];
  const phones = [];
  const memberIds = [];
  const seenEmail = new Set();
  const seenPhone = new Set();
  const seenMemberId = new Set();

  for (const row of preparedRows) {
    if (!row || row.errors?.length) continue;
    if (row.dedupeBy === "email" && row.dedupeValue) {
      const key = String(row.dedupeValue || "").toLowerCase();
      if (!seenEmail.has(key)) {
        seenEmail.add(key);
        emails.push(key);
      }
    }
    if (row.dedupeBy === "phone" && row.dedupeValue) {
      const digits = String(row.dedupeValue || "").replace(/\D+/g, "");
      if (digits && !seenPhone.has(digits)) {
        seenPhone.add(digits);
        phones.push(digits);
      }
    }
    if (row.dedupeBy === "memberId" && row.dedupeValue) {
      const key = String(row.dedupeValue || "").toLowerCase();
      if (!seenMemberId.has(key)) {
        seenMemberId.add(key);
        memberIds.push(key);
      }
    }
  }

  if (!emails.length && !phones.length && !memberIds.length) {
    return {
      byEmail: new Map(),
      byPhone: new Map(),
      byMemberId: new Map(),
    };
  }

  const rows = await db.manyOrNone(
    `
    select
      id,
      member_id as "memberId",
      full_name as "fullName",
      phone,
      email,
      role,
      date_of_birth as "dateOfBirth"
    from members
    where church_id = $1
      and (
        (cardinality($2::text[]) > 0 and lower(coalesce(email, '')) = any($2::text[]))
        or (cardinality($3::text[]) > 0 and regexp_replace(coalesce(phone, ''), '[^0-9]+', '', 'g') = any($3::text[]))
        or (cardinality($4::text[]) > 0 and lower(coalesce(member_id, '')) = any($4::text[]))
      )
    order by updated_at desc nulls last, created_at desc nulls last
    `,
    [churchId, emails, phones, memberIds]
  );

  const byEmail = new Map();
  const byPhone = new Map();
  const byMemberId = new Map();
  for (const row of rows) {
    const email = String(row?.email || "")
      .trim()
      .toLowerCase();
    const phoneDigits = String(row?.phone || "").replace(/\D+/g, "");
    const memberId = String(row?.memberId || "")
      .trim()
      .toLowerCase();

    if (email && !byEmail.has(email)) byEmail.set(email, row);
    if (phoneDigits && !byPhone.has(phoneDigits)) byPhone.set(phoneDigits, row);
    if (memberId && !byMemberId.has(memberId)) byMemberId.set(memberId, row);
  }

  return { byEmail, byPhone, byMemberId };
}

function prepareMemberImportRows(tableRows, mapping, duplicateMode = "skip") {
  const mode = normalizeMemberImportDuplicateMode(duplicateMode);
  const results = [];
  const firstSeenByDedupeKey = new Map();

  for (const sourceRow of tableRows) {
    const warnings = [];
    const errors = [];

    const rawFullName = mappedImportCell(sourceRow, mapping, "fullName");
    const rawMemberId = mappedImportCell(sourceRow, mapping, "memberId");
    const rawEmail = mappedImportCell(sourceRow, mapping, "email");
    const rawPhone = mappedImportCell(sourceRow, mapping, "phone");
    const rawDateOfBirth = mappedImportCell(sourceRow, mapping, "dateOfBirth");
    const rawJoinDate = mappedImportCell(sourceRow, mapping, "joinDate");
    const rawBaptismStatus = mappedImportCell(sourceRow, mapping, "baptismStatus");

    const fullName = typeof rawFullName === "undefined" ? undefined : normalizeImportText(rawFullName, 160);
    const memberId = typeof rawMemberId === "undefined" ? undefined : normalizeImportMemberId(rawMemberId);

    let email = undefined;
    if (typeof rawEmail !== "undefined") {
      const cleaned = String(rawEmail || "").trim();
      if (!cleaned) email = null;
      else {
        email = normalizeImportEmail(cleaned);
        if (!email) errors.push("Invalid email address.");
      }
    }

    let phone = undefined;
    if (typeof rawPhone !== "undefined") {
      const cleaned = String(rawPhone || "").trim();
      if (!cleaned) phone = null;
      else {
        phone = normalizeImportPhone(cleaned);
        if (!phone) errors.push("Invalid phone number.");
      }
    }

    const dobParsed = typeof rawDateOfBirth === "undefined" ? { value: undefined, error: null } : parseImportDateValue(rawDateOfBirth);
    const joinParsed = typeof rawJoinDate === "undefined" ? { value: undefined, error: null } : parseImportDateValue(rawJoinDate);
    if (dobParsed.error) errors.push(`dateOfBirth: ${dobParsed.error}`);
    if (joinParsed.error) errors.push(`joinDate: ${joinParsed.error}`);

    const baptismStatus =
      typeof rawBaptismStatus === "undefined" ? undefined : normalizeChurchMemberBaptismStatus(rawBaptismStatus, "UNKNOWN");
    if (typeof rawBaptismStatus !== "undefined") {
      const rawToken = normalizeUpperToken(rawBaptismStatus || "");
      if (rawToken && !CHURCH_MEMBER_BAPTISM_STATUSES.has(rawToken)) {
        warnings.push(`Unrecognized baptism status \"${String(rawBaptismStatus)}\"; defaulted to UNKNOWN.`);
      }
    }

    const dedupeBy = email ? "email" : phone ? "phone" : memberId ? "memberId" : null;
    const dedupeValue = dedupeBy === "email" ? email : dedupeBy === "phone" ? phone : dedupeBy === "memberId" ? memberId : null;
    if (!dedupeBy || !dedupeValue) {
      errors.push("Row must include email, phone, or memberId for deduplication.");
    }
    const dedupeKey =
      dedupeBy === "email"
        ? `email:${String(dedupeValue || "").toLowerCase()}`
        : dedupeBy === "phone"
          ? `phone:${String(dedupeValue || "").replace(/\D+/g, "")}`
          : dedupeBy === "memberId"
            ? `memberId:${String(dedupeValue || "").toLowerCase()}`
            : "";

    let duplicateInFileOfLine = null;
    if (dedupeKey) {
      if (firstSeenByDedupeKey.has(dedupeKey)) {
        duplicateInFileOfLine = firstSeenByDedupeKey.get(dedupeKey);
        warnings.push(`Duplicate in uploaded file. First seen at row ${duplicateInFileOfLine}.`);
      } else {
        firstSeenByDedupeKey.set(dedupeKey, sourceRow.lineNumber);
      }
    }

    const profilePatch = {
      householdName:
        typeof mappedImportCell(sourceRow, mapping, "householdName") === "undefined"
          ? undefined
          : normalizeImportText(mappedImportCell(sourceRow, mapping, "householdName"), 120),
      householdRole:
        typeof mappedImportCell(sourceRow, mapping, "householdRole") === "undefined"
          ? undefined
          : normalizeImportText(mappedImportCell(sourceRow, mapping, "householdRole"), 80),
      addressLine1:
        typeof mappedImportCell(sourceRow, mapping, "addressLine1") === "undefined"
          ? undefined
          : normalizeImportText(mappedImportCell(sourceRow, mapping, "addressLine1"), 180),
      addressLine2:
        typeof mappedImportCell(sourceRow, mapping, "addressLine2") === "undefined"
          ? undefined
          : normalizeImportText(mappedImportCell(sourceRow, mapping, "addressLine2"), 180),
      suburb:
        typeof mappedImportCell(sourceRow, mapping, "suburb") === "undefined"
          ? undefined
          : normalizeImportText(mappedImportCell(sourceRow, mapping, "suburb"), 120),
      city:
        typeof mappedImportCell(sourceRow, mapping, "city") === "undefined"
          ? undefined
          : normalizeImportText(mappedImportCell(sourceRow, mapping, "city"), 120),
      province:
        typeof mappedImportCell(sourceRow, mapping, "province") === "undefined"
          ? undefined
          : normalizeImportText(mappedImportCell(sourceRow, mapping, "province"), 120),
      postalCode:
        typeof mappedImportCell(sourceRow, mapping, "postalCode") === "undefined"
          ? undefined
          : normalizeImportText(mappedImportCell(sourceRow, mapping, "postalCode"), 40),
      country:
        typeof mappedImportCell(sourceRow, mapping, "country") === "undefined"
          ? undefined
          : normalizeImportText(mappedImportCell(sourceRow, mapping, "country"), 120),
      alternatePhone:
        typeof mappedImportCell(sourceRow, mapping, "alternatePhone") === "undefined"
          ? undefined
          : normalizeImportPhone(mappedImportCell(sourceRow, mapping, "alternatePhone")),
      whatsappNumber:
        typeof mappedImportCell(sourceRow, mapping, "whatsappNumber") === "undefined"
          ? undefined
          : normalizeImportPhone(mappedImportCell(sourceRow, mapping, "whatsappNumber")),
      occupation:
        typeof mappedImportCell(sourceRow, mapping, "occupation") === "undefined"
          ? undefined
          : normalizeImportText(mappedImportCell(sourceRow, mapping, "occupation"), 120),
      emergencyContactName:
        typeof mappedImportCell(sourceRow, mapping, "emergencyContactName") === "undefined"
          ? undefined
          : normalizeImportText(mappedImportCell(sourceRow, mapping, "emergencyContactName"), 140),
      emergencyContactPhone:
        typeof mappedImportCell(sourceRow, mapping, "emergencyContactPhone") === "undefined"
          ? undefined
          : normalizeImportPhone(mappedImportCell(sourceRow, mapping, "emergencyContactPhone")),
      emergencyContactRelation:
        typeof mappedImportCell(sourceRow, mapping, "emergencyContactRelation") === "undefined"
          ? undefined
          : normalizeImportText(mappedImportCell(sourceRow, mapping, "emergencyContactRelation"), 60),
      ministryTags: normalizeImportStringList(mappedImportCell(sourceRow, mapping, "ministryTags")),
      consentData: parseImportBooleanValue(mappedImportCell(sourceRow, mapping, "consentData")),
      consentContact: parseImportBooleanValue(mappedImportCell(sourceRow, mapping, "consentContact")),
      notes:
        typeof mappedImportCell(sourceRow, mapping, "notes") === "undefined"
          ? undefined
          : normalizeImportText(mappedImportCell(sourceRow, mapping, "notes"), 2000),
      joinDate: joinParsed.value,
      baptismStatus,
    };

    results.push({
      rowNumber: sourceRow.lineNumber,
      source: sourceRow,
      memberPatch: {
        fullName,
        memberId,
        email,
        phone,
        dateOfBirth: dobParsed.value,
      },
      profilePatch,
      dedupeBy,
      dedupeValue,
      dedupeKey,
      duplicateInFileOfLine,
      warnings,
      errors,
      action: "PENDING",
      reason: null,
      existingMember: null,
      duplicateMode: mode,
    });
  }

  return results;
}

function summarizePreparedMemberImportRows(preparedRows) {
  const summary = {
    totalRows: preparedRows.length,
    willCreate: 0,
    willUpdate: 0,
    willSkip: 0,
    errors: 0,
    warnings: 0,
  };
  for (const row of preparedRows) {
    summary.warnings += Array.isArray(row?.warnings) ? row.warnings.length : 0;
    if (row?.action === "CREATE") summary.willCreate += 1;
    else if (row?.action === "UPDATE") summary.willUpdate += 1;
    else if (String(row?.action || "").startsWith("SKIP")) summary.willSkip += 1;
    if (row?.action === "ERROR") summary.errors += 1;
  }
  return summary;
}

function resolvePreparedMemberImportActions(preparedRows, lookup, duplicateMode = "skip") {
  const mode = normalizeMemberImportDuplicateMode(duplicateMode);
  for (const row of preparedRows) {
    if (row.errors.length) {
      row.action = "ERROR";
      row.reason = row.errors.join(" ");
      continue;
    }

    let existing = null;
    if (row.dedupeBy === "email") {
      existing = lookup.byEmail.get(String(row.dedupeValue || "").toLowerCase()) || null;
    } else if (row.dedupeBy === "phone") {
      existing = lookup.byPhone.get(String(row.dedupeValue || "").replace(/\D+/g, "")) || null;
    } else if (row.dedupeBy === "memberId") {
      existing = lookup.byMemberId.get(String(row.dedupeValue || "").toLowerCase()) || null;
    }
    row.existingMember = existing || null;

    if (row.duplicateInFileOfLine) {
      row.action = "SKIP_DUPLICATE_FILE";
      row.reason = `Duplicate in uploaded file (first at row ${row.duplicateInFileOfLine}).`;
      continue;
    }

    if (existing) {
      if (mode === "update") {
        row.action = "UPDATE";
        row.reason = "Existing member will be updated.";
      } else {
        row.action = "SKIP_DUPLICATE_EXISTING";
        row.reason = "Duplicate member exists in this church.";
      }
      continue;
    }

    if (!row.memberPatch.fullName) {
      row.action = "ERROR";
      row.reason = "fullName is required when creating a new member.";
      row.errors.push("fullName is required when creating a new member.");
      continue;
    }

    row.action = "CREATE";
    row.reason = "New member will be created.";
  }
}

function projectPreparedImportRowForResponse(row) {
  return {
    rowNumber: row?.rowNumber ?? null,
    action: row?.action || "ERROR",
    reason: row?.reason || null,
    dedupeBy: row?.dedupeBy || null,
    dedupeValue: row?.dedupeValue || null,
    fullName: row?.memberPatch?.fullName || null,
    memberId: row?.memberPatch?.memberId || null,
    email: row?.memberPatch?.email || null,
    phone: row?.memberPatch?.phone || null,
    dateOfBirth: row?.memberPatch?.dateOfBirth || null,
    joinDate: row?.profilePatch?.joinDate || null,
    baptismStatus: row?.profilePatch?.baptismStatus || "UNKNOWN",
    warnings: Array.isArray(row?.warnings) ? row.warnings : [],
    errors: Array.isArray(row?.errors) ? row.errors : [],
    existingMember: row?.existingMember
      ? {
          id: row.existingMember.id || null,
          memberId: row.existingMember.memberId || null,
          fullName: row.existingMember.fullName || null,
          email: row.existingMember.email || null,
          phone: row.existingMember.phone || null,
          role: row.existingMember.role || null,
        }
      : null,
  };
}

function coalesceImportValue(nextValue, currentValue, fallback = null) {
  if (typeof nextValue === "undefined" || nextValue === null) {
    if (typeof currentValue === "undefined") return fallback;
    return currentValue;
  }
  return nextValue;
}

async function upsertChurchMemberProfileFromImport({
  tx,
  churchId,
  memberPk,
  memberId,
  actorId,
  profilePatch,
} = {}) {
  const existing = await tx.oneOrNone(
    `
    select
      household_name as "householdName",
      household_role as "householdRole",
      address_line1 as "addressLine1",
      address_line2 as "addressLine2",
      suburb,
      city,
      province,
      postal_code as "postalCode",
      country,
      alternate_phone as "alternatePhone",
      whatsapp_number as "whatsappNumber",
      occupation,
      emergency_contact_name as "emergencyContactName",
      emergency_contact_phone as "emergencyContactPhone",
      emergency_contact_relation as "emergencyContactRelation",
      ministry_tags as "ministryTags",
      join_date as "joinDate",
      consent_data as "consentData",
      consent_contact as "consentContact",
      consent_updated_at as "consentUpdatedAt",
      notes,
      baptism_status as "baptismStatus"
    from church_member_profiles
    where church_id=$1 and member_pk=$2
    limit 1
    `,
    [churchId, memberPk]
  );

  const consentData = coalesceImportValue(profilePatch?.consentData, existing?.consentData === true, false) === true;
  const consentContact = coalesceImportValue(profilePatch?.consentContact, existing?.consentContact === true, false) === true;
  const consentTouched = typeof profilePatch?.consentData !== "undefined" || typeof profilePatch?.consentContact !== "undefined";
  const consentUpdatedAt = consentTouched ? new Date().toISOString() : existing?.consentUpdatedAt || null;

  const merged = {
    householdName: coalesceImportValue(profilePatch?.householdName, existing?.householdName, null),
    householdRole: coalesceImportValue(profilePatch?.householdRole, existing?.householdRole, null),
    addressLine1: coalesceImportValue(profilePatch?.addressLine1, existing?.addressLine1, null),
    addressLine2: coalesceImportValue(profilePatch?.addressLine2, existing?.addressLine2, null),
    suburb: coalesceImportValue(profilePatch?.suburb, existing?.suburb, null),
    city: coalesceImportValue(profilePatch?.city, existing?.city, null),
    province: coalesceImportValue(profilePatch?.province, existing?.province, null),
    postalCode: coalesceImportValue(profilePatch?.postalCode, existing?.postalCode, null),
    country: coalesceImportValue(profilePatch?.country, existing?.country, null),
    alternatePhone: coalesceImportValue(profilePatch?.alternatePhone, existing?.alternatePhone, null),
    whatsappNumber: coalesceImportValue(profilePatch?.whatsappNumber, existing?.whatsappNumber, null),
    occupation: coalesceImportValue(profilePatch?.occupation, existing?.occupation, null),
    emergencyContactName: coalesceImportValue(profilePatch?.emergencyContactName, existing?.emergencyContactName, null),
    emergencyContactPhone: coalesceImportValue(profilePatch?.emergencyContactPhone, existing?.emergencyContactPhone, null),
    emergencyContactRelation: coalesceImportValue(
      profilePatch?.emergencyContactRelation,
      existing?.emergencyContactRelation,
      null
    ),
    ministryTags:
      typeof profilePatch?.ministryTags === "undefined"
        ? normalizeStringArray(existing?.ministryTags || [])
        : normalizeStringArray(profilePatch?.ministryTags || []),
    joinDate: coalesceImportValue(profilePatch?.joinDate, formatDateIsoLike(existing?.joinDate), null),
    notes: coalesceImportValue(profilePatch?.notes, existing?.notes, null),
    baptismStatus: normalizeChurchMemberBaptismStatus(
      coalesceImportValue(profilePatch?.baptismStatus, existing?.baptismStatus, "UNKNOWN"),
      "UNKNOWN"
    ),
  };

  await tx.none(
    `
    insert into church_member_profiles (
      church_id,
      member_pk,
      member_id,
      household_name,
      household_role,
      address_line1,
      address_line2,
      suburb,
      city,
      province,
      postal_code,
      country,
      alternate_phone,
      whatsapp_number,
      occupation,
      emergency_contact_name,
      emergency_contact_phone,
      emergency_contact_relation,
      ministry_tags,
      join_date,
      consent_data,
      consent_contact,
      consent_updated_at,
      notes,
      baptism_status,
      created_by,
      updated_by
    )
    values (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19::jsonb,$20,$21,$22,$23,$24,$25,$26,$26
    )
    on conflict (church_id, member_pk)
    do update set
      member_id = excluded.member_id,
      household_name = excluded.household_name,
      household_role = excluded.household_role,
      address_line1 = excluded.address_line1,
      address_line2 = excluded.address_line2,
      suburb = excluded.suburb,
      city = excluded.city,
      province = excluded.province,
      postal_code = excluded.postal_code,
      country = excluded.country,
      alternate_phone = excluded.alternate_phone,
      whatsapp_number = excluded.whatsapp_number,
      occupation = excluded.occupation,
      emergency_contact_name = excluded.emergency_contact_name,
      emergency_contact_phone = excluded.emergency_contact_phone,
      emergency_contact_relation = excluded.emergency_contact_relation,
      ministry_tags = excluded.ministry_tags,
      join_date = excluded.join_date,
      consent_data = excluded.consent_data,
      consent_contact = excluded.consent_contact,
      consent_updated_at = excluded.consent_updated_at,
      notes = excluded.notes,
      baptism_status = excluded.baptism_status,
      updated_by = excluded.updated_by,
      updated_at = now()
    `,
    [
      churchId,
      memberPk,
      memberId || null,
      merged.householdName,
      merged.householdRole,
      merged.addressLine1,
      merged.addressLine2,
      merged.suburb,
      merged.city,
      merged.province,
      merged.postalCode,
      merged.country,
      merged.alternatePhone,
      merged.whatsappNumber,
      merged.occupation,
      merged.emergencyContactName,
      merged.emergencyContactPhone,
      merged.emergencyContactRelation,
      JSON.stringify(merged.ministryTags || []),
      merged.joinDate,
      consentData,
      consentContact,
      consentUpdatedAt,
      merged.notes,
      merged.baptismStatus,
      actorId || null,
    ]
  );
}

function normalizeMemberImportExecutionError(err) {
  if (err?.code === "23505") return "Duplicate value conflicts with an existing member record.";
  if (err?.code === "23514") return "Data failed a database validation rule.";
  if (err?.code === "22P02") return "Invalid value format for one of the mapped fields.";
  const message = err instanceof Error ? String(err.message || "") : String(err?.message || "");
  if (!message) return null;
  const safeClientPatterns = [
    "import file",
    "legacy .xls",
    "unsupported file type",
    "invalid xlsx",
    "target sheet",
    "sheet data",
    "no rows found",
    "no sheets found",
    "fullName is required",
    "row must include",
    "existing member could not be loaded",
    "duplicate match could not be resolved",
  ];
  const lowered = message.toLowerCase();
  if (safeClientPatterns.some((item) => lowered.includes(String(item).toLowerCase()))) {
    return message;
  }
  return null;
}

async function createChurchMemberFromImportRow({
  churchId,
  row,
  actorId = null,
  placeholderPasswordHash,
} = {}) {
  if (!placeholderPasswordHash) throw new Error("Missing placeholder password hash.");
  const memberPatch = row?.memberPatch && typeof row.memberPatch === "object" ? row.memberPatch : {};
  const profilePatch = row?.profilePatch && typeof row.profilePatch === "object" ? row.profilePatch : {};
  const fullName = String(memberPatch.fullName || "").trim();
  if (!fullName) throw new Error("fullName is required when creating a member.");

  return db.tx(async (t) => {
    const created = await t.one(
      `
      insert into members (
        full_name,
        phone,
        email,
        password_hash,
        role,
        church_id,
        date_of_birth,
        member_id,
        updated_at
      )
      values ($1,$2,$3,$4,'member',$5,$6,$7,now())
      returning
        id,
        member_id as "memberId",
        full_name as "fullName",
        phone,
        email,
        role,
        date_of_birth as "dateOfBirth"
      `,
      [
        fullName,
        memberPatch.phone || null,
        memberPatch.email || null,
        placeholderPasswordHash,
        churchId,
        memberPatch.dateOfBirth || null,
        memberPatch.memberId || null,
      ]
    );

    // Imported member records remain unclaimed until the person installs the app and claims profile ownership.
    try {
      await t.none(
        `
        update members
        set
          profile_claimed_at = null,
          profile_claim_source = 'IMPORTED',
          updated_at = now()
        where id = $1
        `,
        [created.id]
      );
    } catch (err) {
      if (err?.code !== "42703") throw err;
    }

    await upsertChurchMemberProfileFromImport({
      tx: t,
      churchId,
      memberPk: created.id,
      memberId: created.memberId || memberPatch.memberId || null,
      actorId,
      profilePatch,
    });

    return created;
  });
}

async function updateChurchMemberFromImportRow({ churchId, row, actorId = null } = {}) {
  const existingMemberId = String(row?.existingMember?.id || "").trim();
  if (!UUID_REGEX.test(existingMemberId)) {
    throw new Error("Duplicate match could not be resolved to a valid member.");
  }

  const memberPatch = row?.memberPatch && typeof row.memberPatch === "object" ? row.memberPatch : {};
  const profilePatch = row?.profilePatch && typeof row.profilePatch === "object" ? row.profilePatch : {};

  return db.tx(async (t) => {
    const current = await t.oneOrNone(
      `
      select
        id,
        member_id as "memberId",
        full_name as "fullName",
        phone,
        email,
        date_of_birth as "dateOfBirth"
      from members
      where church_id=$1 and id=$2
      limit 1
      `,
      [churchId, existingMemberId]
    );
    if (!current) {
      throw new Error("Existing member could not be loaded. Refresh preview and retry.");
    }

    const mergedFullName = coalesceImportValue(memberPatch.fullName, current.fullName, current.fullName) || current.fullName;
    const mergedMemberId = coalesceImportValue(memberPatch.memberId, current.memberId, current.memberId) || null;
    const mergedPhone = coalesceImportValue(memberPatch.phone, current.phone, current.phone) || null;
    const mergedEmail = coalesceImportValue(memberPatch.email, current.email, current.email) || null;
    const mergedDateOfBirth = coalesceImportValue(memberPatch.dateOfBirth, current.dateOfBirth, current.dateOfBirth) || null;

    const updated = await t.one(
      `
      update members
      set
        full_name = $3,
        member_id = $4,
        phone = $5,
        email = $6,
        date_of_birth = $7,
        updated_at = now()
      where church_id=$1 and id=$2
      returning
        id,
        member_id as "memberId",
        full_name as "fullName",
        phone,
        email,
        role,
        date_of_birth as "dateOfBirth"
      `,
      [churchId, existingMemberId, mergedFullName, mergedMemberId, mergedPhone, mergedEmail, mergedDateOfBirth]
    );

    await upsertChurchMemberProfileFromImport({
      tx: t,
      churchId,
      memberPk: updated.id,
      memberId: updated.memberId || null,
      actorId,
      profilePatch,
    });

    return updated;
  });
}

async function findChurchMemberByReference(churchId, memberRef) {
  const ref = String(memberRef || "").trim();
  if (!ref) return null;
  return db.oneOrNone(
    `
    select
      id,
      member_id,
      member_id as "memberId",
      full_name,
      full_name as "fullName",
      phone,
      email,
      role,
      date_of_birth as "dateOfBirth"
    from members
    where church_id=$1
      and (
        id::text = $2
        or member_id = $2
        or phone = $2
        or lower(coalesce(email, '')) = lower($2)
      )
    limit 1
    `,
    [churchId, ref]
  );
}

async function resolveChurchHouseholdChildMemberLink({
  churchId,
  parentMemberPk,
  childMemberPkInput,
  childMemberRefInput,
} = {}) {
  const parentPk = String(parentMemberPk || "").trim();
  const memberPkInput = String(childMemberPkInput || "").trim();
  const memberRefInput = String(childMemberRefInput || "").trim();

  let linkedByPk = null;
  if (memberPkInput) {
    if (!UUID_REGEX.test(memberPkInput)) {
      return { error: "childMemberPk must be a valid UUID.", status: 400 };
    }
    if (memberPkInput === parentPk) {
      return { error: "A member cannot be added as their own child.", status: 400 };
    }
    linkedByPk = await findMemberInChurchByUuid(churchId, memberPkInput);
    if (!linkedByPk) return { error: "childMemberPk is not in this church.", status: 400 };
  }

  let linkedByRef = null;
  if (memberRefInput) {
    linkedByRef = await findChurchMemberByReference(churchId, memberRefInput);
    if (!linkedByRef) return { error: "childMemberRef is not in this church.", status: 400 };
    if (linkedByRef.id === parentPk) {
      return { error: "A member cannot be added as their own child.", status: 400 };
    }
  }

  if (linkedByPk && linkedByRef && String(linkedByPk.id) !== String(linkedByRef.id)) {
    return { error: "childMemberPk and childMemberRef must reference the same member.", status: 400 };
  }

  const linkedChild = linkedByPk || linkedByRef || null;
  return {
    childMemberPk: linkedChild?.id || null,
    linkedChild,
  };
}

async function findChurchServiceById(churchId, serviceId, campusId = null) {
  return db.oneOrNone(
    `
    select
      s.id,
      s.church_id as "churchId",
      s.campus_id as "campusId",
      cc.name as "campusName",
      cc.code as "campusCode",
      s.service_name as "serviceName",
      s.service_date as "serviceDate",
      s.starts_at as "startsAt",
      s.ends_at as "endsAt",
      s.location,
      s.notes,
      s.published,
      s.created_at as "createdAt",
      s.updated_at as "updatedAt"
    from church_services s
    left join church_campuses cc on cc.id = s.campus_id
    where s.id=$1 and s.church_id=$2
      and ($3::uuid is null or s.campus_id = $3::uuid)
    limit 1
    `,
    [serviceId, churchId, campusId]
  );
}

async function findLatestChurchService(churchId, campusId = null) {
  return db.oneOrNone(
    `
    select
      s.id,
      s.church_id as "churchId",
      s.campus_id as "campusId",
      cc.name as "campusName",
      cc.code as "campusCode",
      s.service_name as "serviceName",
      s.service_date as "serviceDate",
      s.starts_at as "startsAt",
      s.ends_at as "endsAt",
      s.location,
      s.notes,
      s.published,
      s.created_at as "createdAt",
      s.updated_at as "updatedAt"
    from church_services s
    left join church_campuses cc on cc.id = s.campus_id
    where s.church_id=$1
      and ($2::uuid is null or s.campus_id = $2::uuid)
    order by s.service_date desc, coalesce(s.starts_at, s.created_at) desc
    limit 1
    `,
    [churchId, campusId]
  );
}

async function findChurchEventById(churchId, eventId, { tx = db } = {}) {
  if (!churchId || !UUID_REGEX.test(String(eventId || "").trim())) return null;
  return tx.oneOrNone(
    `
    select
      e.id,
      e.church_id as "churchId",
      e.campus_id as "campusId",
      cc.name as "campusName",
      cc.code as "campusCode",
      e.title,
      e.description,
      e.starts_at as "startsAt",
      e.ends_at as "endsAt",
      e.venue,
      e.poster_url as "posterUrl",
      e.poster_data_url as "posterDataUrl",
      e.capacity,
      e.is_ticketed as "isTicketed",
      e.price,
      e.status,
      e.notify_on_publish as "notifyOnPublish",
      e.published_at as "publishedAt",
      e.created_at as "createdAt",
      e.updated_at as "updatedAt"
    from church_events e
    left join church_campuses cc on cc.id = e.campus_id
    where e.church_id = $1 and e.id = $2
    limit 1
    `,
    [churchId, String(eventId || "").trim()]
  );
}

async function countChurchEventActiveRegistrations(churchId, eventId, { tx = db } = {}) {
  if (!churchId || !UUID_REGEX.test(String(eventId || "").trim())) return 0;
  const row = await tx.one(
    `
    select count(*)::int as count
    from church_event_registrations
    where church_id = $1
      and event_id = $2
      and status = any($3::text[])
    `,
    [churchId, String(eventId || "").trim(), CHURCH_EVENT_REGISTRATION_ACTIVE_STATUSES]
  );
  return Number(row?.count || 0);
}

async function loadPaidChurchEventTicketIntent(churchId, paymentIntentId, { tx = db } = {}) {
  const id = String(paymentIntentId || "").trim();
  if (!churchId || !UUID_REGEX.test(id)) return null;
  return tx.oneOrNone(
    `
    select
      id,
      church_id as "churchId",
      status,
      source,
      notes,
      amount,
      amount_gross as "amountGross",
      m_payment_id as "mPaymentId",
      updated_at as "updatedAt"
    from payment_intents
    where id = $1 and church_id = $2
    limit 1
    `,
    [id, churchId]
  );
}

function normalizeChurchLifePrayerCategory(value) {
  const key = normalizeUpperToken(value);
  if (CHURCH_LIFE_PRAYER_CATEGORIES.has(key)) return key;
  return "GENERAL";
}

function normalizeChurchLifePrayerVisibility(value, category) {
  const normalizedCategory = normalizeChurchLifePrayerCategory(category);
  const key = normalizeUpperToken(value);
  if (CHURCH_LIFE_PRAYER_VISIBILITIES.has(key)) {
    if (CHURCH_LIFE_PRAYER_SENSITIVE_CATEGORIES.has(normalizedCategory) && key === "CHURCH") return "RESTRICTED";
    return key;
  }
  return CHURCH_LIFE_PRAYER_SENSITIVE_CATEGORIES.has(normalizedCategory) ? "RESTRICTED" : "TEAM_ONLY";
}

function normalizeChurchLifePrayerTeam(value) {
  const key = normalizeUpperToken(value);
  if (key === "PASTOR") return "PASTORAL";
  if (key === "PRAYER_TEAM_LEAD") return "PRAYER_TEAM";
  if (CHURCH_LIFE_PRAYER_TEAMS.has(key)) return key;
  return null;
}

function normalizeChurchLifePrayerAssignmentRole(value) {
  const key = normalizeUpperToken(value);
  if (CHURCH_LIFE_PRAYER_ASSIGNMENT_ROLES.has(key)) return key;
  if (key === "PASTOR") return "PASTORAL";
  return null;
}

function normalizeChurchLifeEventStatus(value) {
  const key = normalizeUpperToken(value);
  if (CHURCH_LIFE_EVENT_STATUSES.has(key)) return key;
  return "DRAFT";
}

function normalizeChurchEventRegistrationStatus(value, fallback = "REGISTERED") {
  const key = normalizeUpperToken(value || fallback);
  if (CHURCH_EVENT_REGISTRATION_STATUSES.has(key)) return key;
  return fallback;
}

function buildChurchEventTicketNote({ eventId, memberPk, quantity = 1, unitPrice = null } = {}) {
  return JSON.stringify({
    type: CHURCH_EVENT_TICKET_SOURCE,
    eventId: String(eventId || "").trim(),
    memberPk: String(memberPk || "").trim(),
    quantity: Math.max(1, Math.min(CHURCH_EVENT_TICKET_MAX_QUANTITY, parsePositiveInt(quantity, 1) || 1)),
    unitPrice: unitPrice == null ? null : toCurrencyNumber(unitPrice),
  });
}

function parseChurchEventTicketNote(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    const source = String(parsed?.type || "").trim().toUpperCase();
    const eventId = String(parsed?.eventId || "").trim();
    const memberPk = String(parsed?.memberPk || "").trim();
    const quantity = Math.max(
      1,
      Math.min(CHURCH_EVENT_TICKET_MAX_QUANTITY, parsePositiveInt(parsed?.quantity, 1) || 1)
    );
    const unitPrice = parsed?.unitPrice == null ? null : Number(parsed.unitPrice);
    if (source !== CHURCH_EVENT_TICKET_SOURCE) return null;
    if (!UUID_REGEX.test(eventId) || !UUID_REGEX.test(memberPk)) return null;
    return {
      source,
      eventId,
      memberPk,
      quantity,
      unitPrice: Number.isFinite(unitPrice) ? toCurrencyNumber(unitPrice) : null,
    };
  } catch (_err) {
    return null;
  }
}

function normalizeChurchGroupType(value) {
  const key = normalizeUpperToken(value);
  if (CHURCH_GROUP_TYPES.has(key)) return key;
  return "MINISTRY";
}

function normalizeChurchGroupMemberRole(value) {
  const key = normalizeUpperToken(value);
  if (CHURCH_GROUP_MEMBER_ROLES.has(key)) return key;
  return "MEMBER";
}

function normalizeChurchGroupMeetingStatus(value, fallback = "SCHEDULED") {
  const safeFallback = CHURCH_GROUP_MEETING_STATUSES.has(normalizeUpperToken(fallback))
    ? normalizeUpperToken(fallback)
    : "SCHEDULED";
  const key = normalizeUpperToken(value || safeFallback);
  if (CHURCH_GROUP_MEETING_STATUSES.has(key)) return key;
  return safeFallback;
}

function normalizeChurchGroupAttendanceStatus(value, fallback = "PRESENT") {
  const safeFallback = CHURCH_GROUP_ATTENDANCE_STATUSES.has(normalizeUpperToken(fallback))
    ? normalizeUpperToken(fallback)
    : "PRESENT";
  const key = normalizeUpperToken(value || safeFallback);
  if (CHURCH_GROUP_ATTENDANCE_STATUSES.has(key)) return key;
  return safeFallback;
}

function normalizeChurchFollowupType(value) {
  const key = normalizeUpperToken(value);
  if (CHURCH_FOLLOWUP_TYPES.has(key)) return key;
  return "GENERAL";
}

function normalizeChurchFollowupStatus(value) {
  const key = normalizeUpperToken(value);
  if (CHURCH_FOLLOWUP_STATUSES.has(key)) return key;
  return "OPEN";
}

function normalizeChurchFollowupPriority(value) {
  const key = normalizeUpperToken(value);
  if (CHURCH_FOLLOWUP_PRIORITIES.has(key)) return key;
  return "MEDIUM";
}

function normalizeChurchFollowupTaskStatus(value) {
  const key = normalizeUpperToken(value);
  if (CHURCH_FOLLOWUP_TASK_STATUSES.has(key)) return key;
  return "TODO";
}

function parseChurchLifeOffset(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.trunc(n));
}

function normalizeStringArray(value, { maxItems = 25, maxLen = 60 } = {}) {
  const source = Array.isArray(value)
    ? value
    : String(value || "")
        .split(",")
        .map((item) => item.trim());
  const out = [];
  const seen = new Set();
  for (const raw of source) {
    const item = String(raw || "").trim().slice(0, maxLen);
    if (!item) continue;
    const key = item.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
    if (out.length >= maxItems) break;
  }
  return out;
}

function recommendedPrayerTeamForCategory(category) {
  const key = normalizeChurchLifePrayerCategory(category);
  if (key === "MENTAL_HEALTH" || key === "ADDICTION" || key === "GRIEF") return "CARE_TEAM";
  if (key === "HEALTH") return "PASTORAL";
  return "PRAYER_TEAM";
}

function buildChurchMemberIdPrefix(churchId, joinCode) {
  const joinPart = String(joinCode || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "")
    .slice(0, 4) || "CHUR";
  const churchPart = String(churchId || "")
    .replace(/[^a-fA-F0-9]/g, "")
    .toUpperCase()
    .slice(0, 6) || "000000";
  return `${joinPart}${churchPart}`;
}

function buildChurchMemberId(prefix, sequence) {
  const seq = Number.isFinite(Number(sequence)) ? Math.max(1, Math.trunc(Number(sequence))) : 1;
  return `${prefix}-${String(seq).padStart(5, "0")}`;
}

async function ensureChurchMemberIdentifiers(churchId) {
  const id = String(churchId || "").trim();
  if (!id) return { assigned: 0, totalMissing: 0 };

  return db.tx(async (t) => {
    await t.none(
      `
      insert into church_member_id_counters (church_id, last_value, updated_at)
      values ($1, 0, now())
      on conflict (church_id) do nothing
      `,
      [id]
    );

    const church = await t.oneOrNone("select join_code from churches where id=$1", [id]);
    const prefix = buildChurchMemberIdPrefix(id, church?.join_code);
    const missing = await t.manyOrNone(
      `
      select id
      from members
      where church_id=$1 and coalesce(member_id, '') = ''
      order by created_at asc, id asc
      `,
      [id]
    );
    if (!missing.length) return { assigned: 0, totalMissing: 0 };

    const counter = await t.one(
      "select last_value from church_member_id_counters where church_id=$1 for update",
      [id]
    );
    let next = Number(counter?.last_value || 0);
    let assigned = 0;

    for (const row of missing) {
      next += 1;
      const generated = buildChurchMemberId(prefix, next);
      await t.none(
        `
        update members
        set member_id=$2, updated_at=now()
        where id=$1 and coalesce(member_id, '') = ''
        `,
        [row.id, generated]
      );
      assigned += 1;
    }

    await t.none(
      "update church_member_id_counters set last_value=$2, updated_at=now() where church_id=$1",
      [id, next]
    );

    return { assigned, totalMissing: missing.length };
  });
}

function hasStrictChurchOperationsActive(subscription) {
  return hasChurchOperationsAccess(subscription);
}

async function requireChurchGrowthActive(req, res, next) {
  try {
    const role = String(req.user?.role || "").toLowerCase();
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    if (role === "super") return next();

    const subscription = await loadChurchOperationsSubscription(churchId);
    if (!hasStrictChurchOperationsActive(subscription)) {
      const computed = computeSubscriptionAccess(subscription);
      return res.status(403).json({
        error: "ChurPay Growth Church Life access is locked.",
        code: CHURCH_LIFE_ACCESS_REQUIRED_CODE,
        reason: computed.reason || "subscription_inactive",
        accessLevel: computed.accessLevel,
        banner: computed.banner,
        subscription: { ...subscription, hasAccess: false, reason: computed.reason, accessLevel: computed.accessLevel, banner: computed.banner },
      });
    }

    try {
      await ensureChurchMemberIdentifiers(churchId);
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({
          error: "Church Life features are not available yet. Run migrations and retry.",
        });
      }
      throw err;
    }

    req.churchOperationsSubscription = { ...subscription, hasAccess: true };
    return next();
  } catch (err) {
    console.error("[church-life] active guard error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
}

function normalizeChurchServiceRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    campusId: row?.campusId || null,
    campusName: row?.campusName || null,
    campusCode: row?.campusCode || null,
    serviceName: String(row?.serviceName || ""),
    serviceDate: formatDateIsoLike(row?.serviceDate),
    startsAt: row?.startsAt || null,
    endsAt: row?.endsAt || null,
    location: row?.location || null,
    notes: row?.notes || null,
    published: row?.published !== false,
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function normalizeChurchCheckinRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    campusId: row?.campusId || null,
    campusName: row?.campusName || null,
    campusCode: row?.campusCode || null,
    serviceId: row?.serviceId || null,
    memberPk: row?.memberPk || null,
    memberId: row?.memberId || null,
    memberName: row?.memberName || null,
    memberRole: row?.memberRole ? normalizeChurchStaffRole(row.memberRole) : null,
    method: normalizeUpperToken(row?.method || "TAP"),
    checkedInAt: row?.checkedInAt || null,
    notes: row?.notes || null,
  };
}

function normalizeChurchApologyRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    serviceId: row?.serviceId || null,
    memberPk: row?.memberPk || null,
    memberId: row?.memberId || null,
    memberName: row?.memberName || null,
    reason: row?.reason || null,
    message: row?.message || null,
    status: normalizeUpperToken(row?.status || "SUBMITTED"),
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
    resolvedAt: row?.resolvedAt || null,
  };
}

function normalizePrayerRequestRow(row) {
  const assignedTeam = normalizeChurchLifePrayerTeam(row?.assignedTeam || row?.assignedRole);
  const assignedToUserId = row?.assignedToUserId || row?.assignedMemberId || null;
  const assignedToUserName = row?.assignedToUserName || row?.assignedMemberName || null;
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    campusId: row?.campusId || null,
    campusName: row?.campusName || null,
    campusCode: row?.campusCode || null,
    memberPk: row?.memberPk || null,
    memberId: row?.memberId || null,
    memberName: row?.memberName || null,
    category: normalizeChurchLifePrayerCategory(row?.category),
    visibility: normalizeChurchLifePrayerVisibility(row?.visibility, row?.category),
    subject: row?.subject || null,
    message: row?.message || "",
    status: normalizeUpperToken(row?.status || "NEW"),
    assignedTeam,
    assignedToUserId,
    assignedToUserName,
    // Legacy aliases kept for backward compatibility in older clients.
    assignedRole: assignedTeam,
    assignedMemberId: assignedToUserId,
    assignedMemberName: assignedToUserName,
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
    closedAt: row?.closedAt || null,
  };
}

function redactPrayerContent(row, access) {
  const out = { ...row };
  if (hasChurchLifePermission(access, "prayer.sensitive.read")) return out;
  const category = normalizeChurchLifePrayerCategory(out.category);
  const visibility = normalizeChurchLifePrayerVisibility(out.visibility, category);
  const isSensitiveCategory = CHURCH_LIFE_PRAYER_SENSITIVE_CATEGORIES.has(category);
  const isRestrictedVisibility = visibility === "RESTRICTED" || visibility === "TEAM_ONLY";
  if (isSensitiveCategory || isRestrictedVisibility) {
    out.subject = out.subject ? "Restricted" : null;
    out.message = "Restricted";
  }
  return out;
}

function normalizeChurchEventRow(row) {
  const capacityRaw = row?.capacity;
  const capacity = capacityRaw == null ? null : Number(capacityRaw);
  const registrationCount = Number(row?.registrationCount || row?.registrationsCount || 0);
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    campusId: row?.campusId || null,
    campusName: row?.campusName || null,
    campusCode: row?.campusCode || null,
    title: row?.title || "",
    description: row?.description || null,
    startsAt: row?.startsAt || null,
    endsAt: row?.endsAt || null,
    venue: row?.venue || null,
    posterUrl: row?.posterUrl || null,
    posterDataUrl: row?.posterDataUrl || null,
    status: normalizeChurchLifeEventStatus(row?.status),
    capacity: Number.isFinite(capacity) && capacity > 0 ? Math.trunc(capacity) : null,
    registrationsCount: registrationCount,
    isFull: Number.isFinite(capacity) && capacity > 0 ? registrationCount >= Math.trunc(capacity) : false,
    availableSpots:
      Number.isFinite(capacity) && capacity > 0 ? Math.max(Math.trunc(capacity) - registrationCount, 0) : null,
    isTicketed: row?.isTicketed === true,
    price: row?.price == null ? null : Number(row?.price || 0),
    myRegistrationId: row?.myRegistrationId || null,
    myRegistrationStatus: row?.myRegistrationStatus
      ? normalizeChurchEventRegistrationStatus(row.myRegistrationStatus, "REGISTERED")
      : null,
    myRegisteredAt: row?.myRegisteredAt || null,
    notifyOnPublish: row?.notifyOnPublish !== false,
    publishedAt: row?.publishedAt || null,
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function normalizeChurchEventRegistrationRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    eventId: row?.eventId || null,
    memberPk: row?.memberPk || null,
    memberId: row?.memberId || null,
    memberName: row?.memberName || null,
    memberPhone: row?.memberPhone || null,
    memberEmail: row?.memberEmail || null,
    status: normalizeChurchEventRegistrationStatus(row?.status),
    checkedInAt: row?.checkedInAt || null,
    canceledAt: row?.canceledAt || null,
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function normalizeChurchMemberProfileRow(row) {
  const tagsRaw = row?.ministryTags;
  const tags = Array.isArray(tagsRaw) ? tagsRaw : normalizeStringArray(tagsRaw || []);
  const dateOfBirth = formatDateIsoLike(row?.dateOfBirth);
  const age = calculateAgeFromDateOfBirth(dateOfBirth);
  const ageGroup = resolveChurchMemberAgeGroup(dateOfBirth);
  const baptismStatus = normalizeChurchMemberBaptismStatus(row?.baptismStatus, "UNKNOWN");
  return {
    churchId: row?.churchId || null,
    memberPk: row?.memberPk || null,
    memberId: row?.memberId || null,
    fullName: row?.fullName || null,
    phone: row?.phone || null,
    email: row?.email || null,
    role: String(row?.role || "member").toLowerCase(),
    dateOfBirth,
    age,
    ageGroup: CHURCH_MEMBER_AGE_GROUPS.has(ageGroup) ? ageGroup : "UNKNOWN",
    householdName: row?.householdName || null,
    householdRole: row?.householdRole || null,
    addressLine1: row?.addressLine1 || null,
    addressLine2: row?.addressLine2 || null,
    suburb: row?.suburb || null,
    city: row?.city || null,
    province: row?.province || null,
    postalCode: row?.postalCode || null,
    country: row?.country || null,
    alternatePhone: row?.alternatePhone || null,
    whatsappNumber: row?.whatsappNumber || null,
    occupation: row?.occupation || null,
    emergencyContactName: row?.emergencyContactName || null,
    emergencyContactPhone: row?.emergencyContactPhone || null,
    emergencyContactRelation: row?.emergencyContactRelation || null,
    ministryTags: tags,
    joinDate: formatDateIsoLike(row?.joinDate),
    consentData: row?.consentData === true,
    consentContact: row?.consentContact === true,
    consentUpdatedAt: row?.consentUpdatedAt || null,
    notes: row?.notes || null,
    baptismStatus,
    childrenCount: Number(row?.childrenCount || 0),
    updatedAt: row?.updatedAt || null,
    memberCreatedAt: row?.memberCreatedAt || null,
  };
}

function normalizeChurchHouseholdChildRow(row) {
  const dateOfBirth = formatDateIsoLike(row?.dateOfBirth);
  const age = calculateAgeFromDateOfBirth(dateOfBirth);
  const ageGroup = resolveChurchMemberAgeGroup(dateOfBirth);
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    parentMemberPk: row?.parentMemberPk || null,
    parentMemberId: row?.parentMemberId || null,
    parentMemberName: row?.parentMemberName || null,
    childMemberPk: row?.childMemberPk || null,
    childMemberId: row?.childMemberId || null,
    childMemberName: row?.childMemberName || null,
    childName: row?.childName || row?.childMemberName || null,
    dateOfBirth,
    age,
    ageGroup: CHURCH_MEMBER_AGE_GROUPS.has(ageGroup) ? ageGroup : "UNKNOWN",
    gender: normalizeChurchHouseholdChildGender(row?.gender),
    relationship: normalizeChurchHouseholdRelationship(row?.relationship),
    schoolGrade: row?.schoolGrade || null,
    notes: row?.notes || null,
    active: row?.active !== false,
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function normalizeChurchHouseholdMemberCandidateRow(row) {
  const dateOfBirth = formatDateIsoLike(row?.dateOfBirth);
  const age = calculateAgeFromDateOfBirth(dateOfBirth);
  const ageGroup = resolveChurchMemberAgeGroup(dateOfBirth);
  return {
    memberPk: row?.memberPk || null,
    memberId: row?.memberId || null,
    fullName: row?.fullName || null,
    phone: row?.phone || null,
    email: row?.email || null,
    role: String(row?.role || "member")
      .trim()
      .toLowerCase(),
    dateOfBirth,
    age,
    ageGroup: CHURCH_MEMBER_AGE_GROUPS.has(ageGroup) ? ageGroup : "UNKNOWN",
    householdName: row?.householdName || null,
    householdRole: row?.householdRole || null,
    addressLine1: row?.addressLine1 || null,
    addressLine2: row?.addressLine2 || null,
    suburb: row?.suburb || null,
    city: row?.city || null,
    province: row?.province || null,
    postalCode: row?.postalCode || null,
    country: row?.country || null,
    alreadyLinked: row?.alreadyLinked === true,
  };
}

function normalizeChurchFamilyRelationshipRow(row) {
  const relatedDateOfBirth = formatDateIsoLike(row?.relatedDateOfBirth);
  const relatedAge = calculateAgeFromDateOfBirth(relatedDateOfBirth);
  const relatedAgeGroup = resolveChurchMemberAgeGroup(relatedDateOfBirth);
  return {
    id: row?.id || null,
    pairKey: row?.pairKey || null,
    churchId: row?.churchId || null,
    memberPk: row?.memberPk || null,
    relatedMemberPk: row?.relatedMemberPk || null,
    relationshipType: normalizeChurchFamilyRelationshipType(row?.relationshipType, "OTHER"),
    notes: row?.notes || null,
    source: String(row?.source || "MANUAL")
      .trim()
      .toUpperCase(),
    active: row?.active !== false,
    relatedMember: {
      memberPk: row?.relatedMemberPk || null,
      memberId: row?.relatedMemberId || null,
      fullName: row?.relatedMemberName || null,
      phone: row?.relatedMemberPhone || null,
      email: row?.relatedMemberEmail || null,
      role: String(row?.relatedMemberRole || "member")
        .trim()
        .toLowerCase(),
      dateOfBirth: relatedDateOfBirth,
      age: relatedAge,
      ageGroup: CHURCH_MEMBER_AGE_GROUPS.has(relatedAgeGroup) ? relatedAgeGroup : "UNKNOWN",
      householdName: row?.relatedHouseholdName || null,
      householdRole: row?.relatedHouseholdRole || null,
      addressLine1: row?.relatedAddressLine1 || null,
      addressLine2: row?.relatedAddressLine2 || null,
      suburb: row?.relatedSuburb || null,
      city: row?.relatedCity || null,
      province: row?.relatedProvince || null,
      postalCode: row?.relatedPostalCode || null,
      country: row?.relatedCountry || null,
    },
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function normalizeChurchFamilyCandidateRow(row) {
  const dateOfBirth = formatDateIsoLike(row?.dateOfBirth);
  const age = calculateAgeFromDateOfBirth(dateOfBirth);
  const ageGroup = resolveChurchMemberAgeGroup(dateOfBirth);
  const types = Array.isArray(row?.existingRelationshipTypes)
    ? row.existingRelationshipTypes
    : normalizeStringArray(row?.existingRelationshipTypes || [], { maxItems: 20, maxLen: 40 });
  const normalizedTypes = types.map((value) => normalizeChurchFamilyRelationshipType(value, "OTHER"));
  return {
    memberPk: row?.memberPk || null,
    memberId: row?.memberId || null,
    fullName: row?.fullName || null,
    phone: row?.phone || null,
    email: row?.email || null,
    role: String(row?.role || "member")
      .trim()
      .toLowerCase(),
    dateOfBirth,
    age,
    ageGroup: CHURCH_MEMBER_AGE_GROUPS.has(ageGroup) ? ageGroup : "UNKNOWN",
    householdName: row?.householdName || null,
    householdRole: row?.householdRole || null,
    addressLine1: row?.addressLine1 || null,
    addressLine2: row?.addressLine2 || null,
    suburb: row?.suburb || null,
    city: row?.city || null,
    province: row?.province || null,
    postalCode: row?.postalCode || null,
    country: row?.country || null,
    alreadyRelated: normalizedTypes.length > 0,
    existingRelationshipTypes: normalizedTypes,
  };
}

function normalizeChurchChildCheckinMethod(value, fallback = "TEACHER") {
  const safeFallback = CHURCH_LIFE_CHILD_CHECKIN_METHODS.has(String(fallback || "").toUpperCase())
    ? String(fallback || "").toUpperCase()
    : "TEACHER";
  const key = normalizeUpperToken(value || safeFallback);
  return CHURCH_LIFE_CHILD_CHECKIN_METHODS.has(key) ? key : safeFallback;
}

function normalizeChurchChildCheckoutMethod(value, fallback = "PARENT") {
  const safeFallback = CHURCH_LIFE_CHILD_CHECKOUT_METHODS.has(String(fallback || "").toUpperCase())
    ? String(fallback || "").toUpperCase()
    : "PARENT";
  const key = normalizeUpperToken(value || safeFallback);
  return CHURCH_LIFE_CHILD_CHECKOUT_METHODS.has(key) ? key : safeFallback;
}

function normalizeChurchChildCheckInRow(row, { includeParentContact = false } = {}) {
  const childDateOfBirth = formatDateIsoLike(row?.childDateOfBirth || row?.dateOfBirth);
  const childAge = calculateAgeFromDateOfBirth(childDateOfBirth);
  const childAgeGroup = resolveChurchMemberAgeGroup(childDateOfBirth);
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    serviceId: row?.serviceId || null,
    serviceName: row?.serviceName || null,
    serviceDate: formatDateIsoLike(row?.serviceDate),
    serviceStartsAt: row?.serviceStartsAt || null,
    campusId: row?.campusId || null,
    campusName: row?.campusName || null,
    campusCode: row?.campusCode || null,
    householdChildId: row?.householdChildId || null,
    childName: row?.childName || null,
    childMemberPk: row?.childMemberPk || null,
    childMemberId: row?.childMemberId || null,
    childDateOfBirth,
    childAge,
    childAgeGroup: CHURCH_MEMBER_AGE_GROUPS.has(childAgeGroup) ? childAgeGroup : "UNKNOWN",
    childGender: normalizeChurchHouseholdChildGender(row?.childGender || row?.gender),
    childRelationship: normalizeChurchHouseholdRelationship(row?.childRelationship || row?.relationship),
    childSchoolGrade: row?.childSchoolGrade || row?.schoolGrade || null,
    parentMemberPk: row?.parentMemberPk || null,
    parentMemberId: row?.parentMemberId || null,
    parentName: row?.parentName || row?.parentMemberName || null,
    parentPhone: includeParentContact ? row?.parentPhone || null : null,
    parentEmail: includeParentContact ? row?.parentEmail || null : null,
    checkInMethod: normalizeChurchChildCheckinMethod(row?.checkInMethod || row?.checkinMethod || "TEACHER"),
    checkedInAt: row?.checkedInAt || null,
    checkedInByMemberPk: row?.checkedInByMemberPk || null,
    checkedInByRole: normalizeChurchStaffRole(row?.checkedInByRole || null) || null,
    checkInNotes: row?.checkInNotes || null,
    checkedOutAt: row?.checkedOutAt || null,
    checkoutMethod: row?.checkedOutAt
      ? normalizeChurchChildCheckoutMethod(row?.checkoutMethod || row?.checkOutMethod || "PARENT")
      : null,
    checkedOutByMemberPk: row?.checkedOutByMemberPk || null,
    checkedOutByRole: row?.checkedOutByRole ? normalizeChurchStaffRole(row?.checkedOutByRole) : null,
    checkoutNotes: row?.checkoutNotes || null,
    status: row?.checkedOutAt ? "CHECKED_OUT" : "CHECKED_IN",
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function redactChurchMemberProfile(profile, access) {
  const out = { ...profile };
  if (!hasChurchLifePermission(access, "profiles.consent.read")) {
    out.consentData = false;
    out.consentContact = false;
    out.consentUpdatedAt = null;
  }
  if (!hasChurchLifePermission(access, "profiles.notes.read")) {
    out.notes = null;
  }
  return out;
}

function normalizeChurchGroupRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    name: row?.name || "",
    code: row?.code || null,
    groupType: normalizeChurchGroupType(row?.groupType),
    description: row?.description || null,
    leaderMemberPk: row?.leaderMemberPk || null,
    leaderMemberId: row?.leaderMemberId || null,
    leaderName: row?.leaderName || null,
    active: row?.active !== false,
    memberCount: Number(row?.memberCount || 0),
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function normalizeChurchGroupMemberRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    groupId: row?.groupId || null,
    memberPk: row?.memberPk || null,
    memberId: row?.memberId || null,
    fullName: row?.fullName || null,
    phone: row?.phone || null,
    email: row?.email || null,
    role: normalizeChurchGroupMemberRole(row?.role),
    joinedOn: formatDateIsoLike(row?.joinedOn),
    active: row?.active !== false,
    notes: row?.notes || null,
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function normalizeChurchGroupMeetingRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    groupId: row?.groupId || null,
    meetingDate: formatDateIsoLike(row?.meetingDate),
    startsAt: row?.startsAt || null,
    endsAt: row?.endsAt || null,
    location: row?.location || null,
    notes: row?.notes || null,
    status: normalizeChurchGroupMeetingStatus(row?.status),
    attendanceCount: Number(row?.attendanceCount || 0),
    presentCount: Number(row?.presentCount || 0),
    lateCount: Number(row?.lateCount || 0),
    absentCount: Number(row?.absentCount || 0),
    excusedCount: Number(row?.excusedCount || 0),
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function normalizeChurchGroupAttendanceRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    groupId: row?.groupId || null,
    meetingId: row?.meetingId || null,
    memberPk: row?.memberPk || null,
    memberId: row?.memberId || null,
    fullName: row?.fullName || null,
    phone: row?.phone || null,
    email: row?.email || null,
    role: normalizeChurchGroupMemberRole(row?.role),
    status: row?.status ? normalizeChurchGroupAttendanceStatus(row.status) : null,
    notes: row?.notes || null,
    markedBy: row?.markedBy || null,
    markedByName: row?.markedByName || null,
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function normalizeChurchFollowupRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    campusId: row?.campusId || null,
    campusName: row?.campusName || null,
    campusCode: row?.campusCode || null,
    memberPk: row?.memberPk || null,
    memberId: row?.memberId || null,
    personName: row?.personName || row?.visitorName || null,
    memberPhone: row?.memberPhone || null,
    memberEmail: row?.memberEmail || null,
    visitorName: row?.visitorName || null,
    visitorContact: row?.visitorContact || null,
    serviceId: row?.serviceId || null,
    serviceName: row?.serviceName || null,
    serviceDate: formatDateIsoLike(row?.serviceDate),
    followupType: normalizeChurchFollowupType(row?.followupType),
    status: normalizeChurchFollowupStatus(row?.status),
    priority: normalizeChurchFollowupPriority(row?.priority),
    title: row?.title || "",
    details: row?.details || null,
    assignedMemberId: row?.assignedMemberId || null,
    assignedMemberName: row?.assignedMemberName || null,
    dueAt: row?.dueAt || null,
    completedAt: row?.completedAt || null,
    notes: row?.notes || null,
    taskCount: Number(row?.taskCount || 0),
    tasksDone: Number(row?.tasksDone || 0),
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function redactFollowupContent(followup, access) {
  const out = { ...followup };
  if (!hasChurchLifePermission(access, "followups.sensitive.read")) {
    out.memberPhone = null;
    out.memberEmail = null;
    out.visitorContact = null;
    out.details = null;
    out.notes = null;
  }
  return out;
}

function normalizeChurchFollowupTaskRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    followupId: row?.followupId || null,
    title: row?.title || "",
    description: row?.description || null,
    status: normalizeChurchFollowupTaskStatus(row?.status),
    assignedMemberId: row?.assignedMemberId || null,
    assignedMemberName: row?.assignedMemberName || null,
    dueAt: row?.dueAt || null,
    completedAt: row?.completedAt || null,
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function normalizeChurchLifeAuditAction(value) {
  const key = normalizeUpperToken(value);
  if (CHURCH_LIFE_AUDIT_ACTIONS.has(key)) return key;
  return "PROFILE_UPDATED";
}

function normalizeChurchLifeAuditEntityType(value) {
  const key = normalizeUpperToken(value);
  if (CHURCH_LIFE_AUDIT_ENTITY_TYPES.has(key)) return key;
  return "MEMBER_PROFILE";
}

function normalizeChurchLifeAuditRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    actorMemberId: row?.actorMemberId || null,
    actorMemberRef: row?.actorMemberRef || null,
    actorName: row?.actorName || null,
    actorRole: row?.actorRole || null,
    action: normalizeChurchLifeAuditAction(row?.action),
    entityType: normalizeChurchLifeAuditEntityType(row?.entityType),
    entityId: row?.entityId || null,
    entityRef: row?.entityRef || null,
    beforeJson: row?.beforeJson && typeof row.beforeJson === "object" && !Array.isArray(row.beforeJson) ? row.beforeJson : {},
    afterJson: row?.afterJson && typeof row.afterJson === "object" && !Array.isArray(row.afterJson) ? row.afterJson : {},
    metaJson: row?.metaJson && typeof row.metaJson === "object" && !Array.isArray(row.metaJson) ? row.metaJson : {},
    createdAt: row?.createdAt || null,
  };
}

function normalizeAuditJson(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value;
}

function buildChurchMemberProfileAuditSnapshot(row) {
  if (!row || typeof row !== "object") return {};
  return {
    householdName: row?.householdName || null,
    householdRole: row?.householdRole || null,
    addressLine1: row?.addressLine1 || null,
    addressLine2: row?.addressLine2 || null,
    suburb: row?.suburb || null,
    city: row?.city || null,
    province: row?.province || null,
    postalCode: row?.postalCode || null,
    country: row?.country || null,
    alternatePhone: row?.alternatePhone || null,
    whatsappNumber: row?.whatsappNumber || null,
    occupation: row?.occupation || null,
    emergencyContactName: row?.emergencyContactName || null,
    emergencyContactPhone: row?.emergencyContactPhone || null,
    emergencyContactRelation: row?.emergencyContactRelation || null,
    ministryTags: Array.isArray(row?.ministryTags) ? row.ministryTags : normalizeStringArray(row?.ministryTags || []),
    joinDate: formatDateIsoLike(row?.joinDate),
    consentData: row?.consentData === true,
    consentContact: row?.consentContact === true,
    notes: row?.notes || null,
    consentUpdatedAt: row?.consentUpdatedAt || null,
    baptismStatus: normalizeChurchMemberBaptismStatus(row?.baptismStatus, "UNKNOWN"),
  };
}

async function writeChurchLifeAuditLog({
  churchId,
  actorMemberId = null,
  actorRole = null,
  action,
  entityType,
  entityId = null,
  entityRef = null,
  before = {},
  after = {},
  meta = {},
} = {}) {
  if (!churchId || !action || !entityType) return false;
  try {
    await db.none(
      `
      insert into church_life_audit_logs (
        church_id,
        actor_member_id,
        actor_role,
        action,
        entity_type,
        entity_id,
        entity_ref,
        before_json,
        after_json,
        meta_json,
        created_at
      )
      values (
        $1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::jsonb,now()
      )
      `,
      [
        churchId,
        actorMemberId || null,
        String(actorRole || "").trim().toLowerCase() || null,
        normalizeChurchLifeAuditAction(action),
        normalizeChurchLifeAuditEntityType(entityType),
        entityId || null,
        String(entityRef || "").trim() || null,
        JSON.stringify(normalizeAuditJson(before)),
        JSON.stringify(normalizeAuditJson(after)),
        JSON.stringify(normalizeAuditJson(meta)),
      ]
    );
    return true;
  } catch (err) {
    // Do not fail Church Life actions when audit table is missing/unavailable.
    if (err?.code === "42P01" || err?.code === "42703") return false;
    console.error("[admin/church-life/audit] write error", err?.message || err);
    return false;
  }
}

function normalizeChurchBroadcastStatus(value) {
  const status = normalizeUpperToken(value || "DRAFT");
  return CHURCH_BROADCAST_STATUSES.has(status) ? status : "DRAFT";
}

function normalizeChurchBroadcastRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    segmentKey: normalizeBroadcastSegmentKey(row?.segmentKey || "ALL_MEMBERS"),
    segmentTag: row?.segmentTag || null,
    title: row?.title || "",
    body: row?.body || "",
    dataJson: row?.dataJson && typeof row.dataJson === "object" ? row.dataJson : {},
    status: normalizeChurchBroadcastStatus(row?.status),
    audienceCount: Number(row?.audienceCount || 0),
    sentCount: Number(row?.sentCount || 0),
    failedCount: Number(row?.failedCount || 0),
    createdBy: row?.createdBy || null,
    createdByName: row?.createdByName || null,
    createdAt: row?.createdAt || null,
    sentAt: row?.sentAt || null,
  };
}

function normalizeChurchBroadcastRecipientRow(row) {
  const status = normalizeUpperToken(row?.status || "SENT");
  return {
    id: row?.id || null,
    broadcastId: row?.broadcastId || null,
    churchId: row?.churchId || null,
    memberPk: row?.memberPk || null,
    memberId: row?.memberId || null,
    memberName: row?.memberName || null,
    notificationId: row?.notificationId || null,
    status: status === "FAILED" ? "FAILED" : "SENT",
    error: row?.error || null,
    createdAt: row?.createdAt || null,
    readAt: row?.readAt || null,
  };
}

function normalizeChurchBroadcastAudiencePresetRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    name: row?.name || "",
    description: row?.description || null,
    segmentKey: normalizeBroadcastSegmentKey(row?.segmentKey || "ALL_MEMBERS"),
    segmentTag: row?.segmentTag || null,
    active: row?.active !== false,
    createdBy: row?.createdBy || null,
    createdByName: row?.createdByName || null,
    updatedBy: row?.updatedBy || null,
    updatedByName: row?.updatedByName || null,
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function normalizeChurchBroadcastTemplateRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    name: row?.name || "",
    title: row?.title || "",
    body: row?.body || "",
    defaultSegmentKey: normalizeBroadcastSegmentKey(row?.defaultSegmentKey || "ALL_MEMBERS"),
    defaultSegmentTag: row?.defaultSegmentTag || null,
    dataJson: row?.dataJson && typeof row.dataJson === "object" && !Array.isArray(row.dataJson) ? row.dataJson : {},
    active: row?.active !== false,
    createdBy: row?.createdBy || null,
    createdByName: row?.createdByName || null,
    updatedBy: row?.updatedBy || null,
    updatedByName: row?.updatedByName || null,
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

async function findMemberInChurchByUuid(churchId, memberPk) {
  if (!UUID_REGEX.test(String(memberPk || ""))) return null;
  return db.oneOrNone(
    `
    select
      id,
      church_id as "churchId",
      member_id as "memberId",
      full_name as "fullName",
      phone,
      email,
      role,
      date_of_birth as "dateOfBirth",
      created_at as "createdAt"
    from members
    where church_id=$1 and id=$2
    limit 1
    `,
    [churchId, memberPk]
  );
}

async function findChurchGroupById(churchId, groupId, { tx = db } = {}) {
  if (!UUID_REGEX.test(String(groupId || ""))) return null;
  return tx.oneOrNone(
    `
    select
      g.id,
      g.church_id as "churchId",
      g.name,
      g.code,
      g.group_type as "groupType",
      g.description,
      g.leader_member_pk as "leaderMemberPk",
      leader.member_id as "leaderMemberId",
      leader.full_name as "leaderName",
      g.active,
      g.created_at as "createdAt",
      g.updated_at as "updatedAt"
    from church_groups g
    left join members leader on leader.id = g.leader_member_pk
    where g.church_id = $1 and g.id = $2
    limit 1
    `,
    [churchId, groupId]
  );
}

async function findGroupMembership(churchId, groupId, memberPk, { tx = db, requireActive = true } = {}) {
  if (!UUID_REGEX.test(String(groupId || "")) || !UUID_REGEX.test(String(memberPk || ""))) return null;
  return tx.oneOrNone(
    `
    select
      gm.id,
      gm.group_id as "groupId",
      gm.member_pk as "memberPk",
      gm.member_role as role,
      gm.joined_on as "joinedOn",
      gm.active
    from church_group_members gm
    where gm.church_id = $1
      and gm.group_id = $2
      and gm.member_pk = $3
      and ($4::boolean = false or gm.active = true)
    limit 1
    `,
    [churchId, groupId, memberPk, requireActive]
  );
}

function normalizePrayerAssignmentRoleToTeam(value) {
  const role = normalizeChurchLifePrayerAssignmentRole(value);
  if (!role) return null;
  if (role === "PRAYER_TEAM_LEAD") return "PRAYER_TEAM";
  return normalizeChurchLifePrayerTeam(role);
}

function isPrayerFinanceRole(role) {
  return normalizeChurchStaffRole(role) === "finance";
}

async function listPrayerTeamsForMember(churchId, memberPk, memberRole = null) {
  const teams = new Set();
  const role = normalizeChurchStaffRole(memberRole);
  if (role === "pastor") teams.add("PASTORAL");
  if (role === "prayer_team_lead") teams.add("PRAYER_TEAM");

  try {
    const rows = await db.manyOrNone(
      `
      select team_role as "teamRole"
      from church_prayer_team_assignments
      where church_id=$1
        and member_pk=$2
        and active=true
      `,
      [churchId, memberPk]
    );
    rows.forEach((row) => {
      const mapped = normalizePrayerAssignmentRoleToTeam(row?.teamRole);
      if (mapped) teams.add(mapped);
    });
  } catch (err) {
    if (err?.code !== "42P01" && err?.code !== "42703") throw err;
  }

  return Array.from(teams);
}

async function isPrayerTeamLead(churchId, memberPk, memberRole = null) {
  if (normalizeChurchStaffRole(memberRole) === "prayer_team_lead") return true;
  try {
    const row = await db.oneOrNone(
      `
      select 1
      from church_prayer_team_assignments
      where church_id=$1
        and member_pk=$2
        and active=true
        and team_role='PRAYER_TEAM_LEAD'
      limit 1
      `,
      [churchId, memberPk]
    );
    return !!row;
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") return false;
    throw err;
  }
}

async function resolvePrayerAccessPolicy(req, churchId) {
  const role = normalizeChurchStaffRole(req.user?.role);
  const blocked = isPrayerFinanceRole(role);
  const lead = blocked ? false : await isPrayerTeamLead(churchId, req.user?.id || null, role);
  const canAssign = !blocked && (role === "super" || role === "admin" || role === "pastor" || lead);
  const teams = blocked ? [] : await listPrayerTeamsForMember(churchId, req.user?.id || null, role);
  const canReadAll = canAssign;
  return { role, blocked, lead, canAssign, canReadAll, teams };
}

async function listPrayerAssignees(churchId, assignedTeam) {
  const team = normalizeChurchLifePrayerTeam(assignedTeam);
  if (!team) return [];

  const assigned = await db.manyOrNone(
    `
    select m.id, m.full_name as "fullName"
    from church_prayer_team_assignments a
    join members m on m.id = a.member_pk
    where a.church_id=$1
      and a.active=true
      and (
        ($2::text = 'PASTORAL' and a.team_role in ('PASTORAL', 'PASTOR'))
        or ($2::text = 'PRAYER_TEAM' and a.team_role in ('PRAYER_TEAM', 'PRAYER_TEAM_LEAD'))
        or ($2::text = 'CARE_TEAM' and a.team_role = 'CARE_TEAM')
      )
      and m.church_id=$1
    order by m.created_at asc
    `,
    [churchId, team]
  );
  if (assigned.length) return assigned;

  return db.manyOrNone(
    `
    select m.id, m.full_name as "fullName"
    from members m
    where m.church_id=$1 and lower(m.role) in ('admin', 'pastor')
    order by m.created_at asc
    `,
    [churchId]
  );
}

async function listPrayerAssignableUsers(churchId) {
  let rows = [];
  try {
    rows = await db.manyOrNone(
      `
      select
        m.id,
        m.member_id as "memberId",
        m.full_name as "fullName",
        lower(coalesce(m.role, 'member')) as "role",
        coalesce(
          array_remove(
            array_agg(
              distinct case
                when a.team_role in ('PASTORAL', 'PASTOR') then 'PASTORAL'
                when a.team_role = 'PRAYER_TEAM_LEAD' then 'PRAYER_TEAM'
                when a.team_role in ('PRAYER_TEAM', 'CARE_TEAM') then a.team_role
                else null
              end
            ),
            null
          ),
          '{}'::text[]
        ) as teams,
        m.created_at as "createdAt"
      from members m
      left join church_prayer_team_assignments a
        on a.church_id = m.church_id
       and a.member_pk = m.id
       and a.active = true
      where m.church_id=$1
      group by m.id, m.member_id, m.full_name, m.role, m.created_at
      having lower(coalesce(m.role, 'member')) in ('admin', 'pastor', 'prayer_team_lead')
         or count(a.id) > 0
      order by m.created_at asc
      `,
      [churchId]
    );
  } catch (err) {
    if (err?.code !== "42P01" && err?.code !== "42703") throw err;
    rows = await db.manyOrNone(
      `
      select
        m.id,
        m.member_id as "memberId",
        m.full_name as "fullName",
        lower(coalesce(m.role, 'member')) as "role",
        '{}'::text[] as teams,
        m.created_at as "createdAt"
      from members m
      where m.church_id=$1
        and lower(coalesce(m.role, 'member')) in ('admin', 'pastor', 'prayer_team_lead')
      order by m.created_at asc
      `,
      [churchId]
    );
  }

  return rows.map((row) => {
    const teams = Array.isArray(row?.teams) ? row.teams.map((item) => normalizeChurchLifePrayerTeam(item)).filter(Boolean) : [];
    if (row?.role === "pastor" && !teams.includes("PASTORAL")) teams.push("PASTORAL");
    if (row?.role === "prayer_team_lead" && !teams.includes("PRAYER_TEAM")) teams.push("PRAYER_TEAM");
    return {
      id: row?.id || null,
      memberId: row?.memberId || null,
      fullName: row?.fullName || null,
      role: normalizeChurchStaffRole(row?.role || "member"),
      teams,
    };
  });
}

async function loadPrayerRequestWithNames(churchId, requestId) {
  return db.oneOrNone(
    `
    select
      pr.id,
      pr.church_id as "churchId",
      pr.campus_id as "campusId",
      cc.name as "campusName",
      cc.code as "campusCode",
      pr.member_pk as "memberPk",
      pr.member_id as "memberId",
      m.full_name as "memberName",
      pr.category,
      pr.visibility,
      pr.subject,
      pr.message,
      pr.status,
      coalesce(pr.assigned_team, case when pr.assigned_role = 'PASTOR' then 'PASTORAL' else pr.assigned_role end) as "assignedTeam",
      coalesce(pr.assigned_to_user_id, pr.assigned_member_id) as "assignedToUserId",
      am.full_name as "assignedToUserName",
      pr.assigned_at as "assignedAt",
      pr.created_at as "createdAt",
      pr.updated_at as "updatedAt",
      pr.closed_at as "closedAt"
    from church_prayer_requests pr
    join members m on m.id = pr.member_pk
    left join members am on am.id = coalesce(pr.assigned_to_user_id, pr.assigned_member_id)
    left join church_campuses cc on cc.id = pr.campus_id
    where pr.id=$1 and pr.church_id=$2
    limit 1
    `,
    [requestId, churchId]
  );
}

async function notifyPrayerRecipients({ churchId, prayerRequest }) {
  const team = normalizeChurchLifePrayerTeam(prayerRequest?.assignedTeam) || recommendedPrayerTeamForCategory(prayerRequest?.category);
  const recipients = await listPrayerAssignees(churchId, team);
  if (!recipients.length) return 0;

  const categoryLabel = normalizeChurchLifePrayerCategory(prayerRequest?.category).replaceAll("_", " ");
  const visibilityLabel = normalizeChurchLifePrayerVisibility(prayerRequest?.visibility, prayerRequest?.category);
  const fromMember = prayerRequest?.memberName || "A member";
  let sent = 0;

  for (const recipient of recipients) {
    await createNotification({
      churchId,
      memberId: recipient.id,
      type: "PRAYER_REQUEST_NEW",
      title: "New prayer request",
      body: `${fromMember} submitted a ${categoryLabel.toLowerCase()} prayer request.`,
      data: {
        prayerRequestId: prayerRequest.id,
        churchId,
        assignedTeam: team,
        visibility: visibilityLabel,
      },
      sendPush: true,
    });
    sent += 1;
  }

  return sent;
}

async function notifyEventPublished({ churchId, eventRow }) {
  const eventId = String(eventRow?.id || "").trim();
  if (!churchId || !eventId) return 0;

  const members = await db.manyOrNone(
    `
    select id
    from members
    where church_id=$1 and lower(coalesce(role, 'member')) in ('member', 'admin', 'accountant', 'finance', 'pastor', 'volunteer', 'usher', 'teacher')
    order by created_at asc
    `,
    [churchId]
  );
  if (!members.length) return 0;

  const startsAt = eventRow?.startsAt ? new Date(eventRow.startsAt) : null;
  const startsLabel = startsAt && !Number.isNaN(startsAt.getTime()) ? startsAt.toLocaleString() : "soon";
  const title = String(eventRow?.title || "Church event").trim() || "Church event";
  let sent = 0;

  for (const member of members) {
    await createNotification({
      churchId,
      memberId: member.id,
      type: "CHURCH_EVENT_PUBLISHED",
      title: "New church event",
      body: `${title} is now published for ${startsLabel}.`,
      data: {
        churchId,
        eventId,
        posterUrl: eventRow?.posterUrl || null,
        startsAt: eventRow?.startsAt || null,
        venue: eventRow?.venue || null,
      },
      sendPush: true,
    });
    sent += 1;
  }

  return sent;
}

function buildFeeBreakdown(amountRaw) {
  const amount = toCurrencyNumber(amountRaw);
  const cfg = readFeeConfig();
  const platformFeeAmount = toCurrencyNumber(cfg.fixed + amount * cfg.pct);
  const amountGross = toCurrencyNumber(amount + platformFeeAmount);
  const superadminCutAmount = toCurrencyNumber(platformFeeAmount * cfg.superPct);
  return {
    amount,
    platformFeeAmount,
    platformFeePct: cfg.pct,
    platformFeeFixed: cfg.fixed,
    amountGross,
    superadminCutAmount,
    superadminCutPct: cfg.superPct,
  };
}

function buildCashFeeBreakdown(amountRaw) {
  const amount = toCurrencyNumber(amountRaw);
  const cashCfg = readCashFeeConfig();
  const superCfg = readFeeConfig();

  const platformFeePct = cashCfg.enabled ? cashCfg.rate : 0;
  const platformFeeFixed = 0;
  const platformFeeAmount = toCurrencyNumber(amount * platformFeePct);
  const amountGross = toCurrencyNumber(amount + platformFeeAmount);
  const superadminCutAmount = toCurrencyNumber(platformFeeAmount * superCfg.superPct);

  return {
    amount,
    platformFeeAmount,
    platformFeePct,
    platformFeeFixed,
    amountGross,
    superadminCutAmount,
    superadminCutPct: superCfg.superPct,
    cashFeeEnabled: cashCfg.enabled,
  };
}

function normalizeMinistryScheduleStatus(value, fallback = "DRAFT") {
  const status = normalizeUpperToken(value || fallback);
  return MINISTRY_SCHEDULE_STATUSES.has(status) ? status : fallback;
}

function normalizeMinistryScheduleAssignmentStatus(value, fallback = "ASSIGNED") {
  const status = normalizeUpperToken(value || fallback);
  return MINISTRY_SCHEDULE_ASSIGNMENT_STATUSES.has(status) ? status : fallback;
}

function normalizeVolunteerTermStatus(value, fallback = "ACTIVE") {
  const status = normalizeUpperToken(value || fallback);
  return VOLUNTEER_TERM_STATUSES.has(status) ? status : fallback;
}

function normalizeVolunteerTermEndReason(value) {
  const reason = normalizeUpperToken(value);
  return VOLUNTEER_TERM_END_REASONS.has(reason) ? reason : null;
}

function normalizeVolunteerSystemRating(value) {
  const rating = normalizeUpperToken(value);
  return VOLUNTEER_SYSTEM_RATINGS.has(rating) ? rating : null;
}

function computeVolunteerLoadLevel(activeRoles) {
  const count = Math.max(0, Number(activeRoles || 0));
  if (count >= 4) return "OVERLOAD";
  if (count === 3) return "HIGH";
  return "NORMAL";
}

function normalizeResponsibilitiesTemplate(value) {
  if (Array.isArray(value)) {
    return value
      .map((item) => String(item || "").trim())
      .filter(Boolean)
      .slice(0, 40);
  }
  if (typeof value === "string") {
    return value
      .split(/\r?\n|,/g)
      .map((item) => String(item || "").trim())
      .filter(Boolean)
      .slice(0, 40);
  }
  return [];
}

function normalizeMinistryRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    campusId: row?.campusId || null,
    campusName: row?.campusName || null,
    campusCode: row?.campusCode || null,
    name: row?.name || null,
    code: row?.code || null,
    description: row?.description || null,
    active: row?.active !== false,
    roleCount: Number(row?.roleCount || 0),
    activeVolunteerCount: Number(row?.activeVolunteerCount || 0),
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function normalizeMinistryRoleRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    ministryId: row?.ministryId || null,
    ministryName: row?.ministryName || null,
    roleName: row?.roleName || null,
    defaultTermMonths: Number(row?.defaultTermMonths || 12),
    responsibilitiesTemplate: Array.isArray(row?.responsibilitiesTemplate)
      ? row.responsibilitiesTemplate
      : normalizeResponsibilitiesTemplate(row?.responsibilitiesTemplate),
    active: row?.active !== false,
    activeTermsCount: Number(row?.activeTermsCount || 0),
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function normalizeVolunteerTermRow(row) {
  const openStatuses = new Set(["ACTIVE", "REVIEW_DUE"]);
  const activeRolesCount = Number(row?.activeRolesCount || 0);
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    memberPk: row?.memberPk || null,
    memberId: row?.memberId || null,
    memberName: row?.memberName || null,
    memberRole: normalizeChurchStaffRole(row?.memberRole || "member"),
    ministryId: row?.ministryId || null,
    ministryName: row?.ministryName || null,
    ministryRoleId: row?.ministryRoleId || null,
    ministryRoleName: row?.ministryRoleName || null,
    startDate: formatDateIsoLike(row?.startDate),
    endDate: formatDateIsoLike(row?.endDate),
    termLengthMonths: Number(row?.termLengthMonths || 0),
    status: normalizeVolunteerTermStatus(row?.status),
    endReason: normalizeVolunteerTermEndReason(row?.endReason),
    attendanceRate: row?.attendanceRate == null ? null : Number(row.attendanceRate),
    reliabilityRate: row?.reliabilityRate == null ? null : Number(row.reliabilityRate),
    trainingCompleted: row?.trainingCompleted === true,
    systemRating:
      normalizeVolunteerSystemRating(row?.systemRating) || (openStatuses.has(normalizeVolunteerTermStatus(row?.status)) ? computeVolunteerLoadLevel(activeRolesCount) : null),
    manualReviewScore: row?.manualReviewScore == null ? null : Number(row.manualReviewScore),
    performanceNotes: row?.performanceNotes || null,
    reviewedBy: row?.reviewedBy || null,
    reviewedByName: row?.reviewedByName || null,
    reviewedAt: row?.reviewedAt || null,
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
    activeRolesCount,
    loadLevel: computeVolunteerLoadLevel(activeRolesCount),
  };
}

function normalizeMinistryScheduleRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    campusId: row?.campusId || null,
    campusName: row?.campusName || null,
    campusCode: row?.campusCode || null,
    ministryId: row?.ministryId || null,
    ministryName: row?.ministryName || null,
    serviceId: row?.serviceId || null,
    serviceName: row?.serviceName || null,
    scheduleDate: formatDateIsoLike(row?.scheduleDate),
    title: row?.title || null,
    notes: row?.notes || null,
    status: normalizeMinistryScheduleStatus(row?.status),
    assignmentCount: Number(row?.assignmentCount || 0),
    confirmedCount: Number(row?.confirmedCount || 0),
    servedCount: Number(row?.servedCount || 0),
    noShowCount: Number(row?.noShowCount || 0),
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

function normalizeMinistryScheduleAssignmentRow(row) {
  return {
    id: row?.id || null,
    churchId: row?.churchId || null,
    scheduleId: row?.scheduleId || null,
    memberPk: row?.memberPk || null,
    memberId: row?.memberId || null,
    memberName: row?.memberName || null,
    memberRole: normalizeChurchStaffRole(row?.memberRole || "member"),
    ministryRoleId: row?.ministryRoleId || null,
    ministryRoleName: row?.ministryRoleName || null,
    status: normalizeMinistryScheduleAssignmentStatus(row?.status),
    assignedBy: row?.assignedBy || null,
    respondedAt: row?.respondedAt || null,
    servedAt: row?.servedAt || null,
    notes: row?.notes || null,
    createdAt: row?.createdAt || null,
    updatedAt: row?.updatedAt || null,
  };
}

const UUID_REGEX = /^[0-9a-fA-F-]{36}$/;

function csvEscape(value) {
  if (value === null || typeof value === "undefined") return "";
  const str = String(value);
  if (/[",\n\r]/.test(str)) return `"${str.replace(/"/g, '""')}"`;
  return str;
}

const TX_STATUS_EXPR =
  "upper(coalesce(pi.status, case when t.provider in ('payfast','simulated','manual') then 'PAID' when t.provider is null then 'PAID' else t.provider end))";
// "Finalized" records: PayFast/manual/simulated are PAID, while cash needs explicit staff confirmation.
const STATEMENT_DEFAULT_STATUSES = ["PAID", "CONFIRMED"];

function toBoolean(val) {
  if (typeof val === "undefined" || val === null) return undefined;
  if (typeof val === "boolean") return val;
  if (typeof val === "number") return !!val;
  const str = String(val)
    .trim()
    .replace(/^['"]|['"]$/g, "")
    .toLowerCase();
  if (["1", "true", "yes", "on"].includes(str)) return true;
  if (["0", "false", "no", "off"].includes(str)) return false;
  return false;
}

function parseEnvBoolean(val) {
  if (typeof val === "undefined" || val === null) return undefined;
  const str = String(val)
    .trim()
    .replace(/^['"]|['"]$/g, "")
    .toLowerCase();
  if (!str) return undefined;
  if (["1", "true", "yes", "on"].includes(str)) return true;
  if (["0", "false", "no", "off"].includes(str)) return false;
  // Treat unknown env values as undefined so feature defaults apply.
  return undefined;
}

async function hasDbColumn(tableName, columnName, schema = "public") {
  if (!tableName || !columnName) return false;
  try {
    const row = await db.one(
      `
      select exists(
        select 1
        from information_schema.columns
        where table_schema = $1
          and table_name = $2
          and column_name = $3
      ) as "exists"
      `,
      [schema, tableName, columnName]
    );
    return row?.exists === true;
  } catch (_) {
    return false;
  }
}

function createInMemoryRateLimiter({ windowMs, max, keyPrefix = "" }) {
  const buckets = new Map();
  let gcCounter = 0;

  return (req, res, next) => {
    const now = Date.now();
    const keyPart = req.user?.id || req.ip || req.socket?.remoteAddress || "unknown";
    const key = `${keyPrefix}${keyPart}`;
    let bucket = buckets.get(key);
    if (!bucket || bucket.resetAt <= now) {
      bucket = { count: 0, resetAt: now + windowMs };
    }

    bucket.count += 1;
    buckets.set(key, bucket);

    gcCounter += 1;
    if (gcCounter % 200 === 0) {
      for (const [bucketKey, value] of buckets.entries()) {
        if (value.resetAt <= now) buckets.delete(bucketKey);
      }
    }

    if (bucket.count > max) {
      const retryAfter = Math.max(1, Math.ceil((bucket.resetAt - now) / 1000));
      res.setHeader("Retry-After", String(retryAfter));
      return res.status(429).json({ error: "Too many requests. Please retry shortly." });
    }

    return next();
  };
}

const payfastConnectRateLimiter = createInMemoryRateLimiter({
  windowMs: 10 * 60 * 1000,
  max: 8,
  keyPrefix: "payfast-connect:",
});

function normalizeFundCode(code, fallbackName) {
  const src = typeof code === "string" && code.trim() ? code.trim() : fallbackName;
  if (!src || typeof src !== "string") return null;
  const slug = src
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "")
    .slice(0, 50);
  return slug || null;
}

function resolveChurchId(req, res, requestedChurchId) {
  const ownChurchId = requireChurch(req, res);
  if (!ownChurchId) return null;

  if (!requestedChurchId || requestedChurchId === "me" || !UUID_REGEX.test(requestedChurchId)) {
    return ownChurchId;
  }

  if (requestedChurchId !== ownChurchId && !isAdminRole(req.user?.role)) {
    res.status(403).json({ error: "Forbidden" });
    return null;
  }

  return requestedChurchId;
}

async function findChurchMinistryById(churchId, ministryId) {
  if (!churchId || !UUID_REGEX.test(String(ministryId || "").trim())) return null;
  return db.oneOrNone(
    `
    select
      m.id,
      m.church_id as "churchId",
      m.campus_id as "campusId",
      cc.name as "campusName",
      cc.code as "campusCode",
      m.name,
      m.code,
      m.description,
      m.active,
      m.created_at as "createdAt",
      m.updated_at as "updatedAt"
    from church_ministries m
    left join church_campuses cc on cc.id = m.campus_id
    where m.church_id=$1 and m.id=$2
    limit 1
    `,
    [churchId, String(ministryId).trim()]
  );
}

async function loadChurchMinistryWithStats(churchId, ministryId) {
  if (!churchId || !UUID_REGEX.test(String(ministryId || "").trim())) return null;
  return db.oneOrNone(
    `
    select
      m.id,
      m.church_id as "churchId",
      m.campus_id as "campusId",
      cc.name as "campusName",
      cc.code as "campusCode",
      m.name,
      m.code,
      m.description,
      m.active,
      m.created_at as "createdAt",
      m.updated_at as "updatedAt",
      coalesce(role_agg.count, 0)::int as "roleCount",
      coalesce(term_agg.count, 0)::int as "activeVolunteerCount"
    from church_ministries m
    left join church_campuses cc on cc.id = m.campus_id
    left join lateral (
      select count(*)::int as count
      from church_ministry_roles r
      where r.church_id = m.church_id and r.ministry_id = m.id and r.active = true
    ) role_agg on true
    left join lateral (
      select count(*)::int as count
      from volunteer_role_terms t
      where t.church_id = m.church_id and t.ministry_id = m.id and t.status in ('ACTIVE', 'REVIEW_DUE')
    ) term_agg on true
    where m.church_id = $1 and m.id = $2
    limit 1
    `,
    [churchId, String(ministryId).trim()]
  );
}

async function findChurchMinistryRoleById(churchId, ministryRoleId) {
  if (!churchId || !UUID_REGEX.test(String(ministryRoleId || "").trim())) return null;
  return db.oneOrNone(
    `
    select
      r.id,
      r.church_id as "churchId",
      r.ministry_id as "ministryId",
      m.name as "ministryName",
      r.role_name as "roleName",
      r.default_term_months as "defaultTermMonths",
      r.responsibilities_template as "responsibilitiesTemplate",
      r.active,
      r.created_at as "createdAt",
      r.updated_at as "updatedAt"
    from church_ministry_roles r
    join church_ministries m on m.id = r.ministry_id and m.church_id = r.church_id
    where r.church_id=$1 and r.id=$2
    limit 1
    `,
    [churchId, String(ministryRoleId).trim()]
  );
}

async function loadChurchMinistryRoleWithStats(churchId, ministryRoleId) {
  if (!churchId || !UUID_REGEX.test(String(ministryRoleId || "").trim())) return null;
  return db.oneOrNone(
    `
    select
      r.id,
      r.church_id as "churchId",
      r.ministry_id as "ministryId",
      m.name as "ministryName",
      r.role_name as "roleName",
      r.default_term_months as "defaultTermMonths",
      r.responsibilities_template as "responsibilitiesTemplate",
      r.active,
      r.created_at as "createdAt",
      r.updated_at as "updatedAt",
      coalesce(term_agg.count, 0)::int as "activeTermsCount"
    from church_ministry_roles r
    join church_ministries m on m.id = r.ministry_id and m.church_id = r.church_id
    left join lateral (
      select count(*)::int as count
      from volunteer_role_terms t
      where t.church_id = r.church_id
        and t.ministry_role_id = r.id
        and t.status in ('ACTIVE', 'REVIEW_DUE')
    ) term_agg on true
    where r.church_id = $1 and r.id = $2
    limit 1
    `,
    [churchId, String(ministryRoleId).trim()]
  );
}

async function findMinistryScheduleById(churchId, scheduleId) {
  if (!churchId || !UUID_REGEX.test(String(scheduleId || "").trim())) return null;
  return db.oneOrNone(
    `
    select
      s.id,
      s.church_id as "churchId",
      s.campus_id as "campusId",
      cc.name as "campusName",
      cc.code as "campusCode",
      s.ministry_id as "ministryId",
      m.name as "ministryName",
      s.service_id as "serviceId",
      srv.service_name as "serviceName",
      s.schedule_date as "scheduleDate",
      s.title,
      s.notes,
      s.status,
      s.created_at as "createdAt",
      s.updated_at as "updatedAt"
    from ministry_schedules s
    join church_ministries m on m.id = s.ministry_id and m.church_id = s.church_id
    left join church_services srv on srv.id = s.service_id and srv.church_id = s.church_id
    left join church_campuses cc on cc.id = s.campus_id
    where s.church_id=$1 and s.id=$2
    limit 1
    `,
    [churchId, String(scheduleId).trim()]
  );
}

async function loadVolunteerRoleTermById(churchId, termId, conn = db) {
  if (!churchId || !UUID_REGEX.test(String(termId || "").trim())) return null;
  return conn.oneOrNone(
    `
    select
      t.id,
      t.church_id as "churchId",
      t.member_pk as "memberPk",
      t.member_id as "memberId",
      m.full_name as "memberName",
      lower(coalesce(m.role, 'member')) as "memberRole",
      t.ministry_id as "ministryId",
      ministry.name as "ministryName",
      t.ministry_role_id as "ministryRoleId",
      role.role_name as "ministryRoleName",
      t.start_date as "startDate",
      t.end_date as "endDate",
      t.term_length_months as "termLengthMonths",
      t.status,
      t.end_reason as "endReason",
      t.attendance_rate as "attendanceRate",
      t.reliability_rate as "reliabilityRate",
      t.training_completed as "trainingCompleted",
      t.system_rating as "systemRating",
      t.manual_review_score as "manualReviewScore",
      t.performance_notes as "performanceNotes",
      t.reviewed_by as "reviewedBy",
      reviewer.full_name as "reviewedByName",
      t.reviewed_at as "reviewedAt",
      t.created_at as "createdAt",
      t.updated_at as "updatedAt",
      coalesce(active_agg.count, 0)::int as "activeRolesCount"
    from volunteer_role_terms t
    join members m on m.id = t.member_pk and m.church_id = t.church_id
    join church_ministries ministry on ministry.id = t.ministry_id and ministry.church_id = t.church_id
    join church_ministry_roles role on role.id = t.ministry_role_id and role.church_id = t.church_id
    left join members reviewer on reviewer.id = t.reviewed_by and reviewer.church_id = t.church_id
    left join lateral (
      select count(*)::int as count
      from volunteer_role_terms active_terms
      where active_terms.church_id = t.church_id
        and active_terms.member_pk = t.member_pk
        and active_terms.status in ('ACTIVE', 'REVIEW_DUE')
    ) active_agg on true
    where t.church_id=$1 and t.id=$2
    limit 1
    `,
    [churchId, String(termId).trim()]
  );
}

async function loadMinistryScheduleWithStats(churchId, scheduleId) {
  if (!churchId || !UUID_REGEX.test(String(scheduleId || "").trim())) return null;
  return db.oneOrNone(
    `
    select
      s.id,
      s.church_id as "churchId",
      s.campus_id as "campusId",
      cc.name as "campusName",
      cc.code as "campusCode",
      s.ministry_id as "ministryId",
      m.name as "ministryName",
      s.service_id as "serviceId",
      srv.service_name as "serviceName",
      s.schedule_date as "scheduleDate",
      s.title,
      s.notes,
      s.status,
      s.created_at as "createdAt",
      s.updated_at as "updatedAt",
      coalesce(assignment_agg.total, 0)::int as "assignmentCount",
      coalesce(assignment_agg.confirmed, 0)::int as "confirmedCount",
      coalesce(assignment_agg.served, 0)::int as "servedCount",
      coalesce(assignment_agg.no_show, 0)::int as "noShowCount"
    from ministry_schedules s
    join church_ministries m on m.id = s.ministry_id and m.church_id = s.church_id
    left join church_services srv on srv.id = s.service_id and srv.church_id = s.church_id
    left join church_campuses cc on cc.id = s.campus_id
    left join lateral (
      select
        count(*)::int as total,
        count(*) filter (where a.status = 'CONFIRMED')::int as confirmed,
        count(*) filter (where a.status = 'SERVED')::int as served,
        count(*) filter (where a.status = 'NO_SHOW')::int as no_show
      from ministry_schedule_assignments a
      where a.schedule_id = s.id
    ) assignment_agg on true
    where s.church_id=$1 and s.id=$2
    limit 1
    `,
    [churchId, String(scheduleId).trim()]
  );
}

function buildVolunteerTermDefaultEndDate(startDate, termLengthMonths) {
  const start = parseChurchLifeDate(startDate);
  const months = Number(termLengthMonths);
  if (!start || !Number.isFinite(months) || months < 1) return null;
  const dt = new Date(`${start}T00:00:00.000Z`);
  dt.setUTCMonth(dt.getUTCMonth() + Math.trunc(months));
  dt.setUTCDate(dt.getUTCDate() - 1);
  return dt.toISOString().slice(0, 10);
}

function parseBoundedDecimal(value, { min = 0, max = 100, decimals = 2 } = {}) {
  if (value === null || typeof value === "undefined" || value === "") return null;
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  if (numeric < min || numeric > max) return null;
  const factor = Math.pow(10, decimals);
  return Math.round(numeric * factor) / factor;
}

function normalizePhoneIdentity(value) {
  return String(value || "").replace(/[^0-9]/g, "");
}

async function ensureDefaultFund(churchId) {
  const church = await db.oneOrNone("select id from churches where id=$1", [churchId]);
  if (!church) return;

  const existing = await db.oneOrNone(
    "select id, active from funds where church_id=$1 and code='general' limit 1",
    [churchId]
  );

  if (existing) {
    if (!existing.active) {
      await db.none("update funds set active=true where id=$1", [existing.id]);
    }
    return;
  }

  try {
    await db.none(
      "insert into funds (church_id, code, name, active) values ($1, 'general', 'General Offering', true)",
      [churchId]
    );
  } catch (err) {
    if (err?.code === "23505") {
      await db.none("update funds set active=true where church_id=$1 and code='general'", [churchId]);
      return;
    }
    throw err;
  }
}

async function listFundsForChurch(churchId, includeInactive = false) {
  const where = includeInactive ? "church_id=$1" : "church_id=$1 and coalesce(active, true)=true";
  let funds = await db.manyOrNone(
    `select id, code, name, active, created_at as "createdAt" from funds where ${where} order by name asc`,
    [churchId]
  );

  if (!includeInactive && funds.length === 0) {
    await ensureDefaultFund(churchId);
    funds = await db.manyOrNone(
      `select id, code, name, active, created_at as "createdAt" from funds where ${where} order by name asc`,
      [churchId]
    );
  }

  return funds;
}

function buildTransactionFilter({ churchId, fundId, channel, status, search, from, to }) {
  const where = ["t.church_id = $1"];
  const params = [churchId];
  let paramIndex = 2;

  if (fundId) {
    params.push(fundId);
    where.push(`t.fund_id = $${paramIndex}`);
    paramIndex++;
  }

  if (channel) {
    params.push(channel);
    where.push(`t.channel = $${paramIndex}`);
    paramIndex++;
  }

  if (status) {
    params.push(String(status).toUpperCase());
    where.push(
      `${TX_STATUS_EXPR} = $${paramIndex}`
    );
    paramIndex++;
  }

  if (typeof search === "string" && search.trim()) {
    const term = `%${search.trim()}%`;
    params.push(term);
    where.push(
      `(t.reference ilike $${paramIndex} or coalesce(pi.payer_name, pi.member_name, '') ilike $${paramIndex} or coalesce(pi.payer_phone, pi.member_phone, '') ilike $${paramIndex})`
    );
    paramIndex++;
  }

  if (from && !Number.isNaN(from.getTime())) {
    params.push(from);
    where.push(`t.created_at >= $${paramIndex}`);
    paramIndex++;
  }

  if (to && !Number.isNaN(to.getTime())) {
    params.push(to);
    where.push(`t.created_at <= $${paramIndex}`);
    paramIndex++;
  }

  return { where, params, nextParamIndex: paramIndex };
}

function buildStatementFilter({ churchId, fundId, channel, status, search, from, to, allStatuses }) {
  const base = buildTransactionFilter({ churchId, fundId, channel, status, search, from, to });
  const where = [...base.where];
  const params = [...base.params];
  let paramIndex = base.nextParamIndex;

  const includeAll = !!allStatuses;
  if (!includeAll && !status) {
    // Default: statement shows finalized records only.
    where.push(`${TX_STATUS_EXPR} = any($${paramIndex})`);
    params.push(STATEMENT_DEFAULT_STATUSES);
    paramIndex++;
  }

  return { where, params, nextParamIndex: paramIndex };
}

function startOfUtcMonthIsoDate() {
  const now = new Date();
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
  return start.toISOString().slice(0, 10);
}

function todayUtcIsoDate() {
  const now = new Date();
  const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  return d.toISOString().slice(0, 10);
}

function escapeHtml(value) {
  return String(value == null ? "" : value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatMoneyZar(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return "R 0.00";
  return `R ${n.toFixed(2)}`;
}

function formatDateIsoLike(value) {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toISOString().slice(0, 10);
}

function calcPercentChange(current, previous) {
  const c = Number(current || 0);
  const p = Number(previous || 0);
  if (!Number.isFinite(c) || !Number.isFinite(p)) return null;
  if (p <= 0) return c > 0 ? 100 : 0;
  return ((c - p) / p) * 100;
}

async function loadAdminStatementData({
  churchId,
  fundId,
  channel,
  status,
  search,
  allStatuses,
  fromIso,
  toIso,
  maxRows,
}) {
  const from = fromIso ? new Date(fromIso + "T00:00:00.000Z") : null;
  const to = toIso ? new Date(toIso + "T23:59:59.999Z") : null;

  const { where, params, nextParamIndex } = buildStatementFilter({
    churchId,
    fundId,
    channel,
    status,
    search,
    from,
    to,
    allStatuses,
  });

  const summary = await db.one(
    `
      select
        coalesce(sum(t.amount),0)::numeric(12,2) as "donationTotal",
        coalesce(sum(coalesce(t.platform_fee_amount,0)),0)::numeric(12,2) as "feeTotal",
        coalesce(sum(coalesce(t.payfast_fee_amount,0)),0)::numeric(12,2) as "payfastFeeTotal",
        coalesce(sum(coalesce(t.church_net_amount, t.amount)),0)::numeric(12,2) as "netReceivedTotal",
        coalesce(sum(coalesce(t.amount_gross, t.amount)),0)::numeric(12,2) as "totalCharged",
        coalesce(sum(coalesce(t.superadmin_cut_amount,0)),0)::numeric(12,2) as "superadminCutTotal",
        count(*)::int as "transactionCount"
      from transactions t
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
      where ${where.join(" and ")}
    `,
    params
  );

  const byFund = await db.manyOrNone(
    `
      select
        f.id as "fundId",
        f.code as "fundCode",
        f.name as "fundName",
        coalesce(sum(t.amount),0)::numeric(12,2) as "donationTotal",
        coalesce(sum(coalesce(t.platform_fee_amount,0)),0)::numeric(12,2) as "feeTotal",
        coalesce(sum(coalesce(t.payfast_fee_amount,0)),0)::numeric(12,2) as "payfastFeeTotal",
        coalesce(sum(coalesce(t.church_net_amount, t.amount)),0)::numeric(12,2) as "netReceivedTotal",
        coalesce(sum(coalesce(t.amount_gross, t.amount)),0)::numeric(12,2) as "totalCharged",
        count(*)::int as "transactionCount"
      from transactions t
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
      where ${where.join(" and ")}
      group by f.id, f.code, f.name
      order by f.name asc
    `,
    params
  );

  const byMethod = await db.manyOrNone(
    `
      select
        coalesce(nullif(lower(t.provider),''), 'unknown') as provider,
        coalesce(sum(t.amount),0)::numeric(12,2) as "donationTotal",
        coalesce(sum(coalesce(t.platform_fee_amount,0)),0)::numeric(12,2) as "feeTotal",
        coalesce(sum(coalesce(t.payfast_fee_amount,0)),0)::numeric(12,2) as "payfastFeeTotal",
        coalesce(sum(coalesce(t.church_net_amount, t.amount)),0)::numeric(12,2) as "netReceivedTotal",
        coalesce(sum(coalesce(t.amount_gross, t.amount)),0)::numeric(12,2) as "totalCharged",
        count(*)::int as "transactionCount"
      from transactions t
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
      where ${where.join(" and ")}
      group by coalesce(nullif(lower(t.provider),''), 'unknown')
      order by "totalCharged" desc
    `,
    params
  );

  let rows = null;
  if (maxRows) {
    const limited = Math.min(Math.max(Number(maxRows || 1), 1), 50000);
    const rowParams = [...params, limited];
    const limitIdx = nextParamIndex;

    rows = await db.manyOrNone(
      `
        select
          t.reference,
          ${TX_STATUS_EXPR} as status,
          t.provider,
          t.channel,
          t.amount,
          coalesce(t.platform_fee_amount,0)::numeric(12,2) as "platformFeeAmount",
          coalesce(t.payfast_fee_amount,0)::numeric(12,2) as "payfastFeeAmount",
          coalesce(t.church_net_amount, t.amount)::numeric(12,2) as "churchNetAmount",
          coalesce(t.amount_gross, t.amount)::numeric(12,2) as "amountGross",
          pi.service_date as "serviceDate",
          t.created_at as "createdAt",
          f.code as "fundCode",
          f.name as "fundName",
          coalesce(pi.payer_name, pi.member_name) as "memberName",
          coalesce(pi.payer_phone, pi.member_phone) as "memberPhone",
          coalesce(pi.payer_email, null) as "memberEmail",
          coalesce(pi.payer_type, 'member') as "payerType",
          coalesce(pi.source, 'DIRECT_APP') as "paymentSource",
          pi.on_behalf_of_member_id as "onBehalfOfMemberId",
          ob.full_name as "onBehalfOfMemberName",
          ob.phone as "onBehalfOfMemberPhone",
          ob.email as "onBehalfOfMemberEmail",
          t.provider_payment_id as "providerPaymentId"
        from transactions t
        join funds f on f.id = t.fund_id
        left join payment_intents pi on pi.id = t.payment_intent_id
        left join members ob on ob.id = pi.on_behalf_of_member_id
        where ${where.join(" and ")}
        order by t.created_at desc
        limit $${limitIdx}
      `,
      rowParams
    );
  }

  return {
    summary,
    breakdown: { byFund, byMethod },
    rows,
    meta: {
      from: fromIso,
      to: toIso,
      defaultStatuses: allStatuses || status ? null : STATEMENT_DEFAULT_STATUSES,
      allStatuses: !!allStatuses,
    },
  };
}

async function loadMember(userId) {
  try {
    return await db.one(
      `select
         m.id,
         m.member_id,
         m.full_name,
         m.phone,
         m.email,
         m.role,
         m.church_id,
         m.payfast_adhoc_token,
         m.payfast_adhoc_token_revoked_at,
         c.name as church_name
       from members m
       left join churches c on c.id = m.church_id
       where m.id=$1`,
      [userId]
    );
  } catch (err) {
    // Backward compatible fallback if saved-card columns aren't migrated yet.
    if (err?.code === "42703") {
      return db.one(
        `select m.id, m.member_id, m.full_name, m.phone, m.email, m.role, m.church_id, c.name as church_name
         from members m
         left join churches c on c.id = m.church_id
         where m.id=$1`,
        [userId]
      );
    }
    throw err;
  }
}

function nextSundayIsoDate() {
  // Use UTC so results are stable across devices/servers.
  const now = new Date();
  const day = now.getUTCDay(); // 0=Sun..6=Sat
  const daysUntilSunday = (7 - day) % 7 || 7; // always in the future
  const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  next.setUTCDate(next.getUTCDate() + daysUntilSunday);
  return next.toISOString().slice(0, 10);
}

function requireChurch(req, res) {
  if (!req.user?.church_id) {
    res.status(400).json({ error: "Join a church first" });
    return null;
  }
  return req.user.church_id;
}

function requireChurchAdminRole(req, res) {
  const role = normalizeChurchStaffRole(req.user?.role);
  if (role !== "admin" && role !== "super") {
    res.status(403).json({ error: "Church admin only" });
    return false;
  }
  return true;
}

async function requireChurchOperationsAccess(req, res, next) {
  try {
    const role = String(req.user?.role || "").toLowerCase();
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    if (role === "super") {
      req.churchOperationsSubscription = {
        ...normalizeChurchOperationsSubscription(null),
        hasAccess: true,
      };
      return next();
    }

    const subscription = await loadChurchOperationsSubscription(churchId);
    if (!hasChurchOperationsAccess(subscription)) {
      const computed = computeSubscriptionAccess(subscription);
      return res.status(402).json({
        error: "ChurPay Growth is locked. Renew subscription to continue.",
        code: "CHURCH_OPERATIONS_SUBSCRIPTION_REQUIRED",
        reason: computed.reason || "subscription_inactive",
        banner: computed.banner,
        subscription: { ...subscription, hasAccess: false, reason: computed.reason, accessLevel: computed.accessLevel, banner: computed.banner },
      });
    }

    req.churchOperationsSubscription = { ...subscription, hasAccess: true };
    return next();
  } catch (err) {
    console.error("[admin/church-operations] guard error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
}

function normalizeBaseUrl() {
  const base = process.env.PUBLIC_BASE_URL || process.env.BASE_URL;
  if (!base) return null;
  return base.endsWith("/") ? base.slice(0, -1) : base;
}

async function createGrowthSubscriptionCheckout({ churchId, member, subscription }) {
  const planConfig = churchSubscriptionPlanConfig(subscription?.planCode);
  const amount = toCurrencyNumber(Number(subscription?.priceCents || planConfig.priceCents) / 100);
  if (!Number.isFinite(amount) || amount <= 0) {
    const err = new Error("Invalid ChurPay Growth subscription amount.");
    err.code = "INVALID_SUBSCRIPTION_AMOUNT";
    throw err;
  }

  const mPaymentId = makeMpaymentId();
  const churchName = String(member?.church_name || "Church").trim() || "Church";
  const itemNameRaw = `ChurPay Growth - ${churchName} (${planConfig.code === "GROWTH_ANNUAL" ? "Annual" : "Monthly"} Subscription)`;
  const itemName = itemNameRaw
    .normalize("NFKD")
    .replace(/[^\x20-\x7E]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 100);

  const intent = await db.one(
    `
    insert into payment_intents (
      church_id, amount, currency, status, provider, channel,
      m_payment_id, item_name,
      payer_name, payer_phone, payer_email, payer_type,
      platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct,
      source, notes, service_date
    ) values (
      $1, $2, $3, 'PENDING', 'payfast', 'admin_portal',
      $4, $5,
      $6, $7, $8, 'admin',
      0, 0, 0, $2, 0, 0,
      $9, $10, now()::date
    )
    returning id, m_payment_id, amount, currency, item_name
    `,
    [
      churchId,
      amount,
      String(subscription?.currency || planConfig.currency || "ZAR").trim().toUpperCase() || "ZAR",
      mPaymentId,
      itemName || "ChurPay Growth Subscription",
      member?.full_name || null,
      member?.phone || null,
      member?.email || null,
      CHURPAY_GROWTH_SUBSCRIPTION_SOURCE,
      `ChurPay Growth subscription checkout (${planConfig.code})`,
    ]
  );

  const baseUrl = normalizeBaseUrl() || "https://api.churpay.com";
  let returnUrl = `${baseUrl}/admin/`;
  returnUrl = appendQueryParam(returnUrl, "tab", "settings");
  returnUrl = appendQueryParam(returnUrl, "growth", "success");
  returnUrl = appendQueryParam(returnUrl, "pi", intent.id);
  returnUrl = appendQueryParam(returnUrl, "mp", intent.m_payment_id);

  let cancelUrl = `${baseUrl}/admin/`;
  cancelUrl = appendQueryParam(cancelUrl, "tab", "settings");
  cancelUrl = appendQueryParam(cancelUrl, "growth", "cancelled");
  cancelUrl = appendQueryParam(cancelUrl, "pi", intent.id);
  cancelUrl = appendQueryParam(cancelUrl, "mp", intent.m_payment_id);

  const notifyUrl = `${baseUrl}/webhooks/payfast/itn`;
  const billingDate = new Date().toISOString().slice(0, 10);
  let checkoutResult = null;
  try {
    const gateway = await PaymentGatewayFactory.get(churchId);
    checkoutResult = await gateway.createIntent({
      churchId,
      amount: intent.amount,
      currency: intent.currency || "ZAR",
      source: CHURPAY_GROWTH_SUBSCRIPTION_SOURCE,
      metadata: { planCode: planConfig.code, paymentIntentId: intent.id },
      providerIntentRef: intent.m_payment_id,
      itemName: intent.item_name,
      returnUrl,
      cancelUrl,
      notifyUrl,
      reference1: churchId,
      reference2: planConfig.code,
      reference3: intent.id,
      reference4: CHURPAY_GROWTH_SUBSCRIPTION_SOURCE,
      payerName: member?.full_name || "Church admin",
      payerEmail: member?.email || undefined,
      subscriptionType: 1,
      billingDate,
      recurringAmount: intent.amount,
      frequency: Number(planConfig.payfastFrequency || 3),
      cycles: RECURRING_DEFAULT_CYCLES,
      preferGlobalCredentials: true,
      allowGlobalFallback: true,
    });
  } catch (err) {
    if (err?.code === "PAYFAST_NOT_CONNECTED") {
      const mapped = new Error(
        "PayFast checkout is not configured. Set global PAYFAST_MERCHANT_ID/PAYFAST_MERCHANT_KEY and retry."
      );
      mapped.code = "PAYFAST_SUBSCRIPTION_NOT_CONFIGURED";
      throw mapped;
    }
    throw err;
  }

  const checkoutUrl = checkoutResult.checkoutUrl;

  return {
    checkoutUrl,
    paymentIntentId: intent.id,
    mPaymentId: intent.m_payment_id,
    credentialSource: checkoutResult.credentialSource || "unknown",
  };
}

function normalizeWebBaseUrl() {
  const base = process.env.PUBLIC_WEB_BASE_URL || process.env.WEBSITE_BASE_URL || "https://churpay.com";
  return String(base || "https://churpay.com").trim().replace(/\/+$/, "");
}

function appendQueryParam(url, key, value) {
  if (!url || typeof value === "undefined" || value === null || value === "") return url;
  try {
    const parsed = new URL(url);
    parsed.searchParams.set(key, String(value));
    return parsed.toString();
  } catch (_err) {
    const sep = url.includes("?") ? "&" : "?";
    return `${url}${sep}${encodeURIComponent(String(key))}=${encodeURIComponent(String(value))}`;
  }
}

function getPayfastCallbackUrls(paymentIntentId, mPaymentId = null) {
  const baseUrl = normalizeBaseUrl() || "https://api.churpay.com";
  let returnUrl = `${baseUrl}/api/payfast/return`;
  let cancelUrl = `${baseUrl}/api/payfast/cancel`;
  const notifyUrl = `${baseUrl}/webhooks/payfast/itn`;

  returnUrl = appendQueryParam(returnUrl, "pi", paymentIntentId);
  cancelUrl = appendQueryParam(cancelUrl, "pi", paymentIntentId);
  returnUrl = appendQueryParam(returnUrl, "mp", mPaymentId);
  cancelUrl = appendQueryParam(cancelUrl, "mp", mPaymentId);

  return {
    returnUrl,
    cancelUrl,
    notifyUrl,
  };
}

async function createCheckoutWithGateway(churchId, payload) {
  const gateway = await PaymentGatewayFactory.get(churchId);
  return gateway.createIntent({ churchId, ...payload });
}

function renderPayfastBridgePage({ title, message, deepLink, fallbackUrl }) {
  const safeTitle = String(title || "Redirecting");
  const safeMessage = String(message || "Opening the app...");
  const link = String(deepLink || "");
  const fallback = String(fallbackUrl || "https://www.churpay.com");

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>${safeTitle}</title>
  <style>
    body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif;padding:24px;color:#222}
    .card{max-width:560px;margin:32px auto;padding:24px;border:1px solid #e5e7eb;border-radius:12px}
    a{color:#0b57d0}
  </style>
</head>
<body>
  <div class="card">
    <h2>${safeTitle}</h2>
    <p>${safeMessage}</p>
    <p><a href="${link}">Tap here if the app does not open</a></p>
    <p><a href="${fallback}">Continue in browser</a></p>
  </div>
  <script>
    (function () {
      var appUrl = ${JSON.stringify(link)};
      var fallbackUrl = ${JSON.stringify(fallback)};
      if (appUrl) window.location.replace(appUrl);
      setTimeout(function () { if (fallbackUrl) window.location.href = fallbackUrl; }, 1800);
    })();
  </script>
</body>
</html>`;
}

router.get("/funds", requireAuth, async (req, res) => {
  try {
    const requestedChurchId =
      typeof req.query?.churchId === "string" && req.query.churchId.trim() ? req.query.churchId.trim() : "me";
    const churchId = resolveChurchId(req, res, requestedChurchId);
    if (!churchId) return;

    const includeInactive = isAdminRole(req.user?.role) && ["1", "true", "yes", "all"].includes(String(req.query.includeInactive || req.query.all || "").toLowerCase());
    const funds = await listFundsForChurch(churchId, includeInactive);
    res.json({ funds });
  } catch (err) {
    console.error("[funds] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/churches/:churchId/funds", requireAuth, async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, req.params.churchId);
    if (!churchId) return;

    const includeInactive = isAdminRole(req.user?.role) && ["1", "true", "yes", "all"].includes(String(req.query.includeInactive || req.query.all || "").toLowerCase());
    const funds = await listFundsForChurch(churchId, includeInactive);
    res.json({ funds });
  } catch (err) {
    console.error("[funds] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Admin-only QR payload generator for in-app donation QR codes.
router.get("/churches/me/qr", requireStaff, requireAdminPortalTabsAny("qr"), async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, "me");
    if (!churchId) return;

    const fundId = typeof req.query?.fundId === "string" ? req.query.fundId.trim() : "";
    if (!fundId || !UUID_REGEX.test(fundId)) {
      return res.status(400).json({ error: "Valid fundId is required" });
    }

    const fund = await db.oneOrNone(
      "select id, code, name, active from funds where id=$1 and church_id=$2",
      [fundId, churchId]
    );
    if (!fund) {
      return res.status(404).json({ error: "Fund not found" });
    }

    const amountRaw = req.query?.amount;
    const amount = typeof amountRaw === "undefined" || amountRaw === null || amountRaw === ""
      ? null
      : Number(amountRaw);
    if (amount !== null && (!Number.isFinite(amount) || amount <= 0)) {
      return res.status(400).json({ error: "Invalid amount" });
    }

    const church = await db.oneOrNone(
      "select id, name, join_code from churches where id=$1",
      [churchId]
    );
    if (!church || !church.join_code) {
      return res.status(400).json({ error: "Church join code is missing" });
    }

    const qrPayload = {
      type: "churpay_donation",
      churchId,
      joinCode: church.join_code,
      fundId: fund.id,
      fundCode: fund.code,
    };
    if (amount !== null) qrPayload.amount = Number(amount.toFixed(2));

    const deepLinkBase = String(process.env.APP_DEEP_LINK_BASE || "churpaydemo://give")
      .trim()
      .replace(/\/+$/, "");
    let deepLink = deepLinkBase;
    deepLink = appendQueryParam(deepLink, "joinCode", church.join_code);
    deepLink = appendQueryParam(deepLink, "fund", fund.code);
    deepLink = appendQueryParam(deepLink, "churchId", churchId);
    deepLink = appendQueryParam(deepLink, "fundId", fund.id);
    deepLink = appendQueryParam(deepLink, "fundCode", fund.code);
    if (amount !== null) {
      deepLink = appendQueryParam(deepLink, "amount", Number(amount.toFixed(2)));
    }

    const webBase = normalizeWebBaseUrl();
    let webLink = `${webBase}/g/${encodeURIComponent(church.join_code)}`;
    webLink = appendQueryParam(webLink, "fund", fund.code);
    if (amount !== null) {
      webLink = appendQueryParam(webLink, "amount", Number(amount.toFixed(2)));
    }

    return res.json({
      qr: {
        value: webLink,
        payload: qrPayload,
      },
      qrPayload,
      deepLink,
      webLink,
      fund: {
        id: fund.id,
        code: fund.code,
        name: fund.name,
        active: fund.active,
      },
    });
  } catch (err) {
    console.error("[qr] GET /churches/me/qr error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

// Get transactions for a church with filters
router.get(["/churches/:churchId/transactions", "/churches/me/transactions"], requireAuth, async (req, res) => {
  try {
    const requestedChurchId = req.params.churchId || "me";
    const churchId = resolveChurchId(req, res, requestedChurchId);
    if (!churchId) return;

    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const fundId = req.query.fundId || null;
    const channel = req.query.channel || null;
    const status = req.query.status || null;
    const search = req.query.search || null;
    const from = req.query.from ? new Date(req.query.from + "T00:00:00.000Z") : null;
    const to = req.query.to ? new Date(req.query.to + "T23:59:59.999Z") : null;

    const { where, params: filterParams, nextParamIndex } = buildTransactionFilter({
      churchId,
      fundId,
      channel,
      status,
      search,
      from,
      to,
    });
    const params = [...filterParams];
    let paramIndex = nextParamIndex;

    params.push(limit);
    const limitIdx = paramIndex;
    paramIndex++;

    params.push(offset);
    const offsetIdx = paramIndex;

    const sql = `
      select
        t.id,
        t.reference,
        t.amount,
        coalesce(t.platform_fee_amount,0)::numeric(12,2) as "platformFeeAmount",
        coalesce(t.payfast_fee_amount,0)::numeric(12,2) as "payfastFeeAmount",
        coalesce(t.church_net_amount, t.amount)::numeric(12,2) as "churchNetAmount",
        coalesce(t.amount_gross, t.amount)::numeric(12,2) as "amountGross",
        coalesce(t.superadmin_cut_amount,0)::numeric(12,2) as "superadminCutAmount",
        t.channel,
        t.provider,
        t.provider_payment_id as "providerPaymentId",
        t.payment_intent_id as "paymentIntentId",
        coalesce(pi.status, case when t.provider in ('payfast','simulated','manual') then 'PAID' when t.provider is null then 'PAID' else upper(t.provider) end) as status,
        coalesce(pi.cash_verified_by_admin, false) as "cashVerifiedByAdmin",
        pi.cash_verification_note as "cashVerificationNote",
        pi.service_date as "serviceDate",
        t.created_at as "createdAt",
        coalesce(pi.payer_name, pi.member_name) as "memberName",
        coalesce(pi.payer_phone, pi.member_phone) as "memberPhone",
        coalesce(pi.payer_email, null) as "memberEmail",
        coalesce(pi.payer_type, 'member') as "payerType",
        coalesce(pi.source, 'DIRECT_APP') as "paymentSource",
        pi.on_behalf_of_member_id as "onBehalfOfMemberId",
        ob.full_name as "onBehalfOfMemberName",
        ob.phone as "onBehalfOfMemberPhone",
        ob.email as "onBehalfOfMemberEmail",
        f.id as "fundId",
        f.code as "fundCode",
        f.name as "fundName"
      from transactions t
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
      left join members ob on ob.id = pi.on_behalf_of_member_id
      where ${where.join(" and ")}
      order by t.created_at desc
      limit $${limitIdx} offset $${offsetIdx}
    `;

    const rows = await db.manyOrNone(sql, params);
    const countRow = await db.one(
      `
      select count(*)::int as count
      from transactions t
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
      where ${where.join(" and ")}
      `,
      filterParams
    );

    res.json({
      transactions: rows,
      meta: { limit, offset, count: Number(countRow.count || 0), returned: rows.length },
    });
  } catch (err) {
    console.error("[transactions] GET error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Failed to load transactions" });
  }
});

// Update fund (rename / toggle active)
router.patch("/funds/:id", requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    let { name, active } = req.body || {};
    name = typeof name === "string" ? name.trim() : name;
    active = toBoolean(active);

    const existing = await db.oneOrNone("select id, code from funds where id=$1 and church_id=$2", [id, churchId]);
    if (!existing) return res.status(404).json({ error: "Fund not found" });

    const sets = [];
    const params = [];
    let idx = 1;

    if (typeof name === "string") {
      sets.push(`name = $${idx++}`);
      params.push(name);
    }

    if (typeof active !== "undefined") {
      sets.push(`active = $${idx++}`);
      params.push(!!active);
    }

    if (!sets.length) {
      return res.status(400).json({ error: "No updates supplied" });
    }

    params.push(id);
    params.push(churchId);

    const updated = await db.one(
      `update funds set ${sets.join(", ")} where id=$${idx++} and church_id=$${idx} returning id, code, name, active`,
      params
    );

    res.json({ fund: updated });
  } catch (err) {
    console.error("[funds] PATCH /funds/:id error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Create a new fund
router.post("/funds", requireAdmin, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    let { code, name, active = true } = req.body || {};

    name = typeof name === "string" ? name.trim() : name;
    active = toBoolean(active);

    if (!name) {
      return res.status(400).json({ error: "Name is required" });
    }

    const normalizedCode = normalizeFundCode(code, name);
    if (!normalizedCode) {
      return res.status(400).json({ error: "Invalid fund code" });
    }

    // ensure church exists
    try {
      await db.one("select id from churches where id=$1", [churchId]);
    } catch (err) {
      if (err.message && err.message.includes("Expected 1 row, got 0")) {
        return res.status(404).json({ error: "Church not found" });
      }
      throw err;
    }

    // ensure uniqueness for this church (code already lowercased)
    const existing = await db.oneOrNone("select id from funds where church_id=$1 and code=$2", [churchId, normalizedCode]);
    if (existing) {
      return res.status(409).json({ error: "Fund code already exists" });
    }

    const row = await db.one(
      `insert into funds (church_id, code, name, active) values ($1,$2,$3,$4) returning id, code, name, active`,
      [churchId, normalizedCode, name, typeof active === "undefined" ? true : !!active]
    );

    res.json({ fund: row });
  } catch (err) {
    console.error("[funds] POST /funds error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Soft delete / deactivate a fund
router.delete("/funds/:id", requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    const existing = await db.oneOrNone("select id from funds where id=$1 and church_id=$2", [id, churchId]);
    if (!existing) return res.status(404).json({ error: "Fund not found" });

    const updated = await db.one(
      "update funds set active=false where id=$1 and church_id=$2 returning id, code, name, active",
      [id, churchId]
    );

    res.json({ fund: updated });
  } catch (err) {
    console.error("[funds] DELETE /funds/:id error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});


router.post("/payment-intents", requireAuth, async (req, res) => {
  try {
    let { fundId, amount, channel = "app", saveCard, useSavedCard } = req.body || {};
    const wantsSaveCard = ["1", "true", "yes"].includes(String(saveCard || "").toLowerCase()) || saveCard === true;
    const wantsUseSavedCard = ["1", "true", "yes"].includes(String(useSavedCard || "").toLowerCase()) || useSavedCard === true;
    if (wantsSaveCard && wantsUseSavedCard) {
      return res.status(400).json({ error: "Choose either saveCard or useSavedCard, not both" });
    }

    const amt = Number(amount);
    if (!Number.isFinite(amt) || amt <= 0) {
      return res.status(400).json({ error: "Invalid amount" });
    }
    const pricing = buildFeeBreakdown(amt);

    fundId = typeof fundId === "string" ? fundId.trim() : fundId;
    channel = typeof channel === "string" ? channel.trim() : channel;

    const member = await loadMember(req.user.id);
    if (!member.church_id) return res.status(400).json({ error: "Join a church first" });
    const churchId = member.church_id;

    if (wantsSaveCard) {
      if (!member.email) {
        return res.status(400).json({ error: "Add an email to your profile to save a card for next time." });
      }
    }

    if (wantsUseSavedCard) {
      const enabled = ["1", "true", "yes"].includes(String(process.env.PAYFAST_SAVED_CARD_ENABLED || "").toLowerCase());
      if (!enabled) {
        return res.status(503).json({
          error: "Saved card payments are coming soon. Please use PayFast for now.",
          code: "SAVED_CARD_COMING_SOON",
        });
      }
      const token = String(member.payfast_adhoc_token || "").trim();
      if (!token || member.payfast_adhoc_token_revoked_at) {
        return res.status(400).json({ error: "No saved card found for this account." });
      }
      if (!member.email) {
        return res.status(400).json({ error: "Add an email to your profile to use saved card payments." });
      }
    }

    if (!fundId) {
      return res.status(400).json({ error: "Missing fundId" });
    }

    let fund, church;
    try {
      fund = await db.one("select id, name, active from funds where id=$1 and church_id=$2", [fundId, churchId]);
      church = await db.one("select id, name from churches where id=$1", [churchId]);
      if (!fund.active) return res.status(400).json({ error: "Fund is inactive" });
    } catch (err) {
      if (err.message.includes("Expected 1 row, got 0")) {
        return res.status(404).json({ error: "Church or fund not found" });
      }
      console.error("[payments] DB error fetching church/fund", err);
      return res.status(500).json({ error: "Internal server error" });
    }

    const paymentsDisabled = ["1", "true", "yes"].includes(String(process.env.PAYMENTS_DISABLED || "").toLowerCase());
    const mPaymentId = makeMpaymentId();

    // PayFast can be picky about special characters in item_name.
    // Keep it ASCII, short, and predictable.
    const itemNameRaw = `${church.name} - ${fund.name}`;
    const itemName = itemNameRaw
      .normalize("NFKD")
      .replace(/[^\x20-\x7E]/g, "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 100);

    if (paymentsDisabled) {
      try {
        const intentId = crypto.randomUUID();
        const reference = mPaymentId;

        const intent = await db.one(
          `insert into payment_intents (
             id, church_id, fund_id, amount, currency, status, member_name, member_phone, payer_name, payer_phone, payer_type, channel, provider, provider_payment_id, m_payment_id, item_name, platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct, created_at, updated_at
           ) values (
             $1,$2,$3,$4,'ZAR','PENDING',$5,$6,$7,$8,'member',$9,'manual',null,$10,$11,$12,$13,$14,$15,$16,$17,now(),now()
           ) returning id`,
          [
            intentId,
            churchId,
            fundId,
            pricing.amount,
            member.full_name || "",
            member.phone || "",
            member.full_name || "",
            member.phone || "",
            channel || "manual",
            reference,
            itemName,
            pricing.platformFeeAmount,
            pricing.platformFeePct,
            pricing.platformFeeFixed,
            pricing.amountGross,
            pricing.superadminCutAmount,
            pricing.superadminCutPct,
          ]
        );

        const txRow = await db.one(
          `insert into transactions (
            church_id, fund_id, payment_intent_id, amount, platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct, payer_name, payer_phone, payer_type, reference, channel, provider, provider_payment_id, created_at
          ) values (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'member',$13,$14,'manual',null,now()
          ) returning id, reference, created_at`,
          [
            churchId,
            fundId,
            intent.id,
            pricing.amount,
            pricing.platformFeeAmount,
            pricing.platformFeePct,
            pricing.platformFeeFixed,
            pricing.amountGross,
            pricing.superadminCutAmount,
            pricing.superadminCutPct,
            member.full_name || "",
            member.phone || "",
            reference,
            channel || "manual",
          ]
        );

        return res.json({
          status: "MANUAL",
          paymentIntentId: intent.id,
          transactionId: txRow.id,
          reference: txRow.reference,
          instructions: "Please pay via EFT/Cash and use this reference.",
        });
      } catch (err) {
        console.error("[payments] manual fallback error", err);
        return res.status(500).json({ error: "Unable to record manual payment intent" });
      }
    }

    const intent = await db.one(
      `
      insert into payment_intents
        (church_id, fund_id, amount, status, provider,
         member_name, member_phone,
         payer_name, payer_phone, payer_email, payer_type,
         item_name, m_payment_id,
         platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct,
         source, save_card_requested)
      values ($1,$2,$3,'PENDING','payfast',
              $4,$5,
              $6,$7,$8,'member',
              $9,$10,
              $11,$12,$13,$14,$15,$16,
              $17,$18)
      returning *
    `,
      [
        churchId,
        fundId,
        pricing.amount,
        member.full_name || "",
        member.phone || "",
        member.full_name || "",
        member.phone || "",
        member.email || null,
        itemName,
        mPaymentId,
        pricing.platformFeeAmount,
        pricing.platformFeePct,
        pricing.platformFeeFixed,
        pricing.amountGross,
        pricing.superadminCutAmount,
        pricing.superadminCutPct,
        wantsUseSavedCard ? "SAVED_CARD" : "DIRECT_APP",
        wantsSaveCard,
      ]
    );

    const { returnUrl, cancelUrl, notifyUrl } = getPayfastCallbackUrls(intent.id, mPaymentId);

    let checkoutResult = null;
    try {
      checkoutResult = await createCheckoutWithGateway(churchId, {
        amount: intent.amount_gross || pricing.amountGross,
        currency: intent.currency || "ZAR",
        source: wantsUseSavedCard ? "SAVED_CARD" : "DIRECT_APP",
        metadata: { paymentIntentId: intent.id, fundId, wantsSaveCard, wantsUseSavedCard },
        providerIntentRef: mPaymentId,
        itemName,
        returnUrl,
        cancelUrl,
        notifyUrl,
        reference1: churchId,
        reference2: fundId,
        payerName: member.full_name || undefined,
        payerEmail: member.email || undefined,
        subscriptionType: wantsSaveCard || wantsUseSavedCard ? 2 : undefined,
        token: wantsUseSavedCard ? String(member.payfast_adhoc_token || "").trim() || undefined : undefined,
      });
    } catch (err) {
      if (err?.code === "PAYFAST_NOT_CONNECTED") {
        console.error("[payments] payfast credentials error", err?.message || err, err?.stack);
        return res.status(503).json({
          error: "Payments are not activated for this church. Ask your church admin to connect PayFast.",
          code: "PAYFAST_NOT_CONNECTED",
        });
      }
      throw err;
    }
    const checkoutUrl = checkoutResult.checkoutUrl;

    res.json({
      paymentIntentId: intent.id,
      mPaymentId: intent.m_payment_id || mPaymentId,
      checkoutUrl,
      pricing: {
        donationAmount: pricing.amount,
        churpayFee: pricing.platformFeeAmount,
        totalCharged: pricing.amountGross,
        superadminCutAmount: pricing.superadminCutAmount,
      },
    });
  } catch (err) {
    console.error("[payments] POST /payment-intents error", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/external-giving/payment-intents", requireAuth, async (req, res) => {
  try {
    const member = await loadMember(req.user.id);
    const joinCode = String(req.body?.joinCode || "").trim().toUpperCase();
    const fundIdInput = typeof req.body?.fundId === "string" ? req.body.fundId.trim() : "";
    const fundCodeInput = typeof req.body?.fundCode === "string" ? req.body.fundCode.trim().toLowerCase() : "";
    const channel = typeof req.body?.channel === "string" && req.body.channel.trim() ? req.body.channel.trim() : "app";
    const amountRaw = Number(req.body?.amount);
    if (!joinCode) return res.status(400).json({ error: "joinCode is required" });
    if (!Number.isFinite(amountRaw) || amountRaw <= 0) return res.status(400).json({ error: "Invalid amount" });
    if (!member?.phone && !member?.email) {
      return res.status(400).json({ error: "Add a phone or email to your profile to give to another church." });
    }

    const recipientChurch = await db.oneOrNone(
      `
      select id, name, join_code as "joinCode"
      from churches
      where upper(join_code) = upper($1)
      limit 1
      `,
      [joinCode]
    );
    if (!recipientChurch) return res.status(404).json({ error: "Church not found" });
    if (member?.church_id && recipientChurch.id === member.church_id) {
      return res.status(400).json({ error: "Use the standard giving flow for your church." });
    }

    let fund = null;
    if (fundIdInput) {
      fund = await db.oneOrNone(
        `
        select id, code, name, coalesce(active, true) as active
        from funds
        where id=$1 and church_id=$2
        `,
        [fundIdInput, recipientChurch.id]
      );
    } else if (fundCodeInput) {
      fund = await db.oneOrNone(
        `
        select id, code, name, coalesce(active, true) as active
        from funds
        where church_id=$1 and lower(code)=lower($2)
        `,
        [recipientChurch.id, fundCodeInput]
      );
    } else {
      fund = await db.oneOrNone(
        `
        select id, code, name, coalesce(active, true) as active
        from funds
        where church_id=$1 and lower(code)='general'
        limit 1
        `,
        [recipientChurch.id]
      );
      if (!fund) {
        fund = await db.oneOrNone(
          `
          select id, code, name, coalesce(active, true) as active
          from funds
          where church_id=$1 and coalesce(active, true)=true
          order by name asc
          limit 1
          `,
          [recipientChurch.id]
        );
      }
    }
    if (!fund) return res.status(404).json({ error: "Fund not found" });
    if (!fund.active) return res.status(400).json({ error: "Fund is inactive" });

    const pricing = buildFeeBreakdown(amountRaw);
    const mPaymentId = makeMpaymentId();
    const itemNameRaw = `${recipientChurch.name} - ${fund.name}`;
    const itemName = itemNameRaw
      .normalize("NFKD")
      .replace(/[^\x20-\x7E]/g, "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 100);

    const intent = await db.one(
      `
      insert into payment_intents (
        church_id, fund_id, amount, currency, status,
        member_name, member_phone,
        payer_name, payer_phone, payer_email, payer_type,
        channel, provider, provider_payment_id, m_payment_id, item_name,
        platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct,
        source,
        created_at, updated_at
      ) values (
        $1,$2,$3,'ZAR','PENDING',
        $4,$5,
        $6,$7,$8,'donor',
        $9,'payfast',null,$10,$11,
        $12,$13,$14,$15,$16,$17,
        $18,
        now(),now()
      ) returning id, m_payment_id as "mPaymentId", amount, amount_gross as "amountGross"
      `,
      [
        recipientChurch.id,
        fund.id,
        pricing.amount,
        member.full_name || "",
        member.phone || "",
        member.full_name || "",
        member.phone || "",
        member.email || null,
        channel,
        mPaymentId,
        itemName,
        pricing.platformFeeAmount,
        pricing.platformFeePct,
        pricing.platformFeeFixed,
        pricing.amountGross,
        pricing.superadminCutAmount,
        pricing.superadminCutPct,
        EXTERNAL_GIVING_SOURCE,
      ]
    );

    const callbacks = getPayfastCallbackUrls(intent.id, intent.mPaymentId || mPaymentId);
    let checkoutResult = null;
    try {
      checkoutResult = await createCheckoutWithGateway(recipientChurch.id, {
        amount: intent.amountGross || pricing.amountGross,
        currency: "ZAR",
        source: EXTERNAL_GIVING_SOURCE,
        metadata: { paymentIntentId: intent.id, fundId: fund.id, homeChurchId: member?.church_id || null },
        providerIntentRef: intent.mPaymentId || mPaymentId,
        itemName,
        returnUrl: callbacks.returnUrl,
        cancelUrl: callbacks.cancelUrl,
        notifyUrl: callbacks.notifyUrl,
        reference1: recipientChurch.id,
        reference2: fund.id,
        payerName: member.full_name || undefined,
        payerEmail: member.email || undefined,
      });
    } catch (err) {
      if (err?.code === "PAYFAST_NOT_CONNECTED") {
        return res.status(503).json({
          error: "Payments are not activated for this church. Ask their admin to connect PayFast.",
          code: "PAYFAST_NOT_CONNECTED",
        });
      }
      throw err;
    }
    const checkoutUrl = checkoutResult.checkoutUrl;

    return res.status(201).json({
      data: {
        paymentIntentId: intent.id,
        mPaymentId: intent.mPaymentId || mPaymentId,
        checkoutUrl,
        amount: pricing.amount,
        processingFee: pricing.platformFeeAmount,
        totalCharged: pricing.amountGross,
        currency: "ZAR",
        church: { id: recipientChurch.id, name: recipientChurch.name, joinCode: recipientChurch.joinCode },
        fund: { id: fund.id, code: fund.code, name: fund.name },
      },
      meta: {
        source: EXTERNAL_GIVING_SOURCE,
        payerType: "donor",
        provider: checkoutResult.provider || "payfast",
        homeChurchId: member?.church_id || null,
      },
    });
  } catch (err) {
    console.error("[external-giving] POST /external-giving/payment-intents error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to start external giving payment" });
  }
});

router.post("/external-giving/cash-givings", requireAuth, async (req, res) => {
  try {
    const member = await loadMember(req.user.id);
    const joinCode = String(req.body?.joinCode || "").trim().toUpperCase();
    const fundIdInput = typeof req.body?.fundId === "string" ? req.body.fundId.trim() : "";
    const fundCodeInput = typeof req.body?.fundCode === "string" ? req.body.fundCode.trim().toLowerCase() : "";
    const flow = typeof req.body?.flow === "string" ? req.body.flow.trim().toLowerCase() : "";
    const notes = typeof req.body?.notes === "string" ? req.body.notes.trim().slice(0, 500) : null;
    const channel = typeof req.body?.channel === "string" && req.body.channel.trim() ? req.body.channel.trim() : "member_app";
    const amountRaw = Number(req.body?.amount);
    const serviceDate =
      typeof req.body?.serviceDate === "string" && /^\d{4}-\d{2}-\d{2}$/.test(req.body.serviceDate.trim())
        ? req.body.serviceDate.trim()
        : nextSundayIsoDate();

    if (!joinCode) return res.status(400).json({ error: "joinCode is required" });
    if (!Number.isFinite(amountRaw) || amountRaw <= 0) return res.status(400).json({ error: "Invalid amount" });
    if (!member?.phone && !member?.email) {
      return res.status(400).json({ error: "Add a phone or email to your profile to give to another church." });
    }

    const recipientChurch = await db.oneOrNone(
      `
      select id, name, join_code as "joinCode"
      from churches
      where upper(join_code) = upper($1)
      limit 1
      `,
      [joinCode]
    );
    if (!recipientChurch) return res.status(404).json({ error: "Church not found" });
    if (member?.church_id && recipientChurch.id === member.church_id) {
      return res.status(400).json({ error: "Use the standard giving flow for your church." });
    }

    let fund = null;
    if (fundIdInput) {
      fund = await db.oneOrNone(
        `
        select id, code, name, coalesce(active, true) as active
        from funds
        where id=$1 and church_id=$2
        `,
        [fundIdInput, recipientChurch.id]
      );
    } else if (fundCodeInput) {
      fund = await db.oneOrNone(
        `
        select id, code, name, coalesce(active, true) as active
        from funds
        where church_id=$1 and lower(code)=lower($2)
        `,
        [recipientChurch.id, fundCodeInput]
      );
    } else {
      fund = await db.oneOrNone(
        `
        select id, code, name, coalesce(active, true) as active
        from funds
        where church_id=$1 and lower(code)='general'
        limit 1
        `,
        [recipientChurch.id]
      );
      if (!fund) {
        fund = await db.oneOrNone(
          `
          select id, code, name, coalesce(active, true) as active
          from funds
          where church_id=$1 and coalesce(active, true)=true
          order by name asc
          limit 1
          `,
          [recipientChurch.id]
        );
      }
    }
    if (!fund) return res.status(404).json({ error: "Fund not found" });
    if (!fund.active) return res.status(400).json({ error: "Fund is inactive" });

    const desiredStatus = flow === "prepared" ? "PREPARED" : "RECORDED";
    const pricing = buildCashFeeBreakdown(amountRaw);
    const reference = makeCashReference();
    const itemNameRaw = `${recipientChurch.name} - ${fund.name} (Cash)`;
    const itemName = itemNameRaw
      .normalize("NFKD")
      .replace(/[^\x20-\x7E]/g, "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 100);

    const intent = await db.one(
      `
      insert into payment_intents (
        church_id, fund_id, amount, currency, status,
        member_name, member_phone,
        payer_name, payer_phone, payer_email, payer_type,
        channel, provider, provider_payment_id, m_payment_id, item_name,
        platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct,
        source,
        service_date, notes, cash_verified_by_admin,
        created_at, updated_at
      ) values (
        $1,$2,$3,'ZAR',$4,
        $5,$6,
        $7,$8,$9,'donor',
        $10,'cash',null,$11,$12,
        $13,$14,$15,$16,$17,$18,
        $19,
        $20,$21,false,
        now(),now()
      ) returning *
      `,
      [
        recipientChurch.id,
        fund.id,
        pricing.amount,
        desiredStatus,
        member.full_name || "",
        member.phone || "",
        member.full_name || "",
        member.phone || "",
        member.email || null,
        channel,
        reference,
        itemName,
        pricing.platformFeeAmount,
        pricing.platformFeePct,
        pricing.platformFeeFixed,
        pricing.amountGross,
        pricing.superadminCutAmount,
        pricing.superadminCutPct,
        EXTERNAL_GIVING_SOURCE,
        serviceDate,
        notes,
      ]
    );

    const txRow = await db.one(
      `
      insert into transactions (
        church_id, fund_id, payment_intent_id,
        amount, platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct,
        payer_name, payer_phone, payer_email, payer_type,
        reference, channel, provider, provider_payment_id, created_at
      ) values (
        $1,$2,$3,
        $4,$5,$6,$7,$8,$9,$10,
        $11,$12,$13,'donor',
        $14,$15,'cash',null,now()
      ) returning id, reference, created_at
      `,
      [
        recipientChurch.id,
        fund.id,
        intent.id,
        pricing.amount,
        pricing.platformFeeAmount,
        pricing.platformFeePct,
        pricing.platformFeeFixed,
        pricing.amountGross,
        pricing.superadminCutAmount,
        pricing.superadminCutPct,
        member.full_name || "",
        member.phone || "",
        member.email || null,
        reference,
        channel,
      ]
    );

    try {
      await upsertChurchDonor({
        churchId: recipientChurch.id,
        payerName: member.full_name || "",
        payerEmail: member.email || null,
        payerPhone: member.phone || "",
        amount: pricing.amount,
        paymentIntentId: intent.id,
        transactionId: txRow.id,
        source: EXTERNAL_GIVING_SOURCE,
      });
    } catch (err) {
      console.error("[external-giving] donor upsert failed", err?.message || err);
    }

    try {
      const staff = await db.manyOrNone(
        `
        select id
        from members
        where church_id=$1 and lower(role) in ('admin','accountant','finance','pastor','volunteer','usher','teacher')
        `,
        [recipientChurch.id]
      );
      const amount = toCurrencyNumber(pricing.amount || 0);
      const statusLabel = String(intent.status || "").toUpperCase() || "RECORDED";
      for (const staffMember of staff) {
        await createNotification({
          memberId: staffMember.id,
          type: "CASH_RECORDED",
          title: "External donor cash record",
          body: `${member.full_name || "A donor"} recorded R ${amount.toFixed(2)} cash to ${fund.name} (${statusLabel}).`,
          data: {
            paymentIntentId: intent.id,
            transactionId: txRow.id,
            reference,
            churchId: recipientChurch.id,
            fundId: fund.id,
            amount,
            status: statusLabel,
            provider: "cash",
            payerType: "donor",
            source: EXTERNAL_GIVING_SOURCE,
            serviceDate,
            requiresAdminConfirmation: true,
          },
        });
      }
    } catch (err) {
      console.error("[external-giving] notify staff failed", err?.message || err);
    }

    return res.status(201).json({
      paymentIntentId: intent.id,
      transactionId: txRow.id,
      reference: txRow.reference,
      method: "CASH",
      status: intent.status,
      amount: pricing.amount,
      pricing: {
        donationAmount: pricing.amount,
        churpayFee: pricing.platformFeeAmount,
        totalCharged: pricing.amountGross,
        feeEnabled: pricing.cashFeeEnabled,
        feeRate: pricing.platformFeePct,
      },
      serviceDate,
      notes: notes || null,
      source: EXTERNAL_GIVING_SOURCE,
      fund: { id: fund.id, code: fund.code, name: fund.name },
      church: { id: recipientChurch.id, name: recipientChurch.name, joinCode: recipientChurch.joinCode },
      createdAt: txRow.created_at,
    });
  } catch (err) {
    console.error("[external-giving] POST /external-giving/cash-givings error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to record external cash giving" });
  }
});

router.post("/recurring-givings", requireAuth, async (req, res) => {
  try {
    const recurringCfg = readRecurringConfig();
    if (!recurringCfg.enabled) {
      return res.status(503).json({
        error: RECURRING_COMING_SOON_MESSAGE,
        code: "RECURRING_COMING_SOON",
        meta: { comingSoon: true },
      });
    }

    const churchId = requireChurch(req, res);
    if (!churchId) return;

    const member = await loadMember(req.user.id);
    const fundId = typeof req.body?.fundId === "string" ? req.body.fundId.trim() : "";
    const amount = Number(req.body?.amount);
    const frequency = parseRecurringFrequency(req.body?.frequency ?? RECURRING_DEFAULT_FREQUENCY);
    const cycles = parsePositiveInt(req.body?.cycles, RECURRING_DEFAULT_CYCLES);
    const notes = typeof req.body?.notes === "string" ? req.body.notes.trim().slice(0, 500) : null;
    const channel = typeof req.body?.channel === "string" && req.body.channel.trim() ? req.body.channel.trim() : "app";
    const billingDateInput = parseIsoDateOnly(req.body?.billingDate);
    const billingDate = billingDateInput || nextSundayIsoDate();

    if (!fundId || !UUID_REGEX.test(fundId)) {
      return res.status(400).json({ error: "Valid fundId is required" });
    }
    if (!Number.isFinite(amount) || amount <= 0) {
      return res.status(400).json({ error: "Invalid amount" });
    }
    if (!frequency) {
      return res.status(400).json({ error: "Invalid frequency. Use weekly/biweekly/monthly/quarterly/biannually/annually or PayFast code 1-6." });
    }
    if (cycles === null || cycles < 0) {
      return res.status(400).json({ error: "cycles must be an integer >= 0" });
    }

    const fund = await db.oneOrNone(
      "select id, code, name, coalesce(active,true) as active from funds where id=$1 and church_id=$2",
      [fundId, churchId]
    );
    if (!fund || !fund.active) {
      return res.status(404).json({ error: "Fund not found" });
    }

    const church = await db.oneOrNone("select id, name from churches where id=$1", [churchId]);
    if (!church) {
      return res.status(404).json({ error: "Church not found" });
    }

    const pricing = buildFeeBreakdown(amount);
    const mPaymentId = makeMpaymentId();
    const itemName = `${church.name} - ${fund.name} (Recurring)`;

    const created = await db.tx(async (t) => {
      const recurring = await t.one(
        `
        insert into recurring_givings (
          member_id, church_id, fund_id, status,
          frequency, cycles, cycles_completed, billing_date,
          donation_amount, platform_fee_amount, gross_amount,
          currency, setup_m_payment_id, notes, created_at, updated_at
        ) values (
          $1,$2,$3,'PENDING_SETUP',
          $4,$5,0,$6,
          $7,$8,$9,
          'ZAR',$10,$11,now(),now()
        ) returning
          id, member_id as "memberId", church_id as "churchId", fund_id as "fundId",
          status, frequency, cycles, cycles_completed as "cyclesCompleted",
          billing_date as "billingDate", donation_amount as "donationAmount",
          platform_fee_amount as "platformFeeAmount", gross_amount as "grossAmount",
          currency, payfast_token as "payfastToken", setup_payment_intent_id as "setupPaymentIntentId",
          setup_m_payment_id as "setupMPaymentId", notes, next_billing_date as "nextBillingDate",
          created_at as "createdAt", updated_at as "updatedAt"
        `,
        [
          member.id,
          churchId,
          fundId,
          frequency,
          cycles,
          billingDate,
          pricing.amount,
          pricing.platformFeeAmount,
          pricing.amountGross,
          mPaymentId,
          notes,
        ]
      );

      const intent = await t.one(
        `
        insert into payment_intents (
          church_id, fund_id, amount, currency, status,
          member_name, member_phone, payer_name, payer_phone, payer_type,
          channel, provider, provider_payment_id, m_payment_id, item_name,
          platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct,
          source, recurring_giving_id, recurring_cycle_no, service_date, notes,
          created_at, updated_at
        ) values (
          $1,$2,$3,'ZAR','PENDING',
          $4,$5,$6,$7,'member',
          $8,'payfast',null,$9,$10,
          $11,$12,$13,$14,$15,$16,
          'RECURRING',$17,1,$18,$19,
          now(),now()
        ) returning id, m_payment_id as "mPaymentId", amount, amount_gross as "amountGross"
        `,
        [
          churchId,
          fundId,
          pricing.amount,
          member.full_name || "",
          member.phone || "",
          member.full_name || "",
          member.phone || "",
          channel,
          mPaymentId,
          itemName,
          pricing.platformFeeAmount,
          pricing.platformFeePct,
          pricing.platformFeeFixed,
          pricing.amountGross,
          pricing.superadminCutAmount,
          pricing.superadminCutPct,
          recurring.id,
          billingDate,
          notes,
        ]
      );

      await t.none(
        "update recurring_givings set setup_payment_intent_id=$2, updated_at=now() where id=$1",
        [recurring.id, intent.id]
      );

      return { recurring, intent };
    });

    const callbacks = getPayfastCallbackUrls(created.intent.id, created.intent.mPaymentId || mPaymentId);
    let checkoutResult = null;
    try {
      checkoutResult = await createCheckoutWithGateway(churchId, {
        amount: pricing.amountGross,
        currency: "ZAR",
        source: "RECURRING",
        metadata: { paymentIntentId: created.intent.id, recurringGivingId: created.recurring.id, fundId },
        providerIntentRef: created.intent.mPaymentId || mPaymentId,
        itemName,
        returnUrl: callbacks.returnUrl,
        cancelUrl: callbacks.cancelUrl,
        notifyUrl: callbacks.notifyUrl,
        reference1: churchId,
        reference2: fundId,
        reference3: created.recurring.id,
        payerName: member.full_name || undefined,
        payerEmail: member.email || undefined,
        subscriptionType: 1,
        billingDate,
        recurringAmount: pricing.amountGross,
        frequency,
        cycles,
      });
    } catch (err) {
      if (err?.code === "PAYFAST_NOT_CONNECTED") {
        console.error("[recurring-givings] payfast credentials error", err?.message || err, err?.stack);
        return res.status(503).json({
          error: "Payments are not activated for this church. Ask your church admin to connect PayFast.",
          code: "PAYFAST_NOT_CONNECTED",
        });
      }
      throw err;
    }
    const checkoutUrl = checkoutResult.checkoutUrl;

    return res.status(201).json({
      data: {
        recurringGiving: created.recurring,
        setupPaymentIntentId: created.intent.id,
        mPaymentId: created.intent.mPaymentId || mPaymentId,
        checkoutUrl,
        pricing: {
          donationAmount: pricing.amount,
          churpayFee: pricing.platformFeeAmount,
          totalCharged: pricing.amountGross,
          churchNetAmountEstimated: pricing.amount,
        },
      },
      meta: {
        provider: checkoutResult.provider || "payfast",
        mode: checkoutResult.mode || null,
      },
    });
  } catch (err) {
    console.error("[recurring-givings] create error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to create recurring giving" });
  }
});

router.get("/recurring-givings", requireAuth, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const rows = await db.manyOrNone(
      `
      select
        rg.id,
        rg.status,
        rg.frequency,
        rg.cycles,
        rg.cycles_completed as "cyclesCompleted",
        rg.billing_date as "billingDate",
        rg.donation_amount as "donationAmount",
        rg.platform_fee_amount as "platformFeeAmount",
        rg.gross_amount as "grossAmount",
        rg.currency,
        rg.payfast_token as "payfastToken",
        rg.setup_payment_intent_id as "setupPaymentIntentId",
        rg.setup_m_payment_id as "setupMPaymentId",
        rg.notes,
        rg.last_charged_at as "lastChargedAt",
        rg.next_billing_date as "nextBillingDate",
        rg.created_at as "createdAt",
        rg.updated_at as "updatedAt",
        f.id as "fundId",
        f.code as "fundCode",
        f.name as "fundName"
      from recurring_givings rg
      join funds f on f.id = rg.fund_id
      where rg.member_id=$1 and rg.church_id=$2
      order by rg.created_at desc
      limit $3 offset $4
      `,
      [req.user.id, churchId, limit, offset]
    );

    const count = await db.one(
      "select count(*)::int as count from recurring_givings where member_id=$1 and church_id=$2",
      [req.user.id, churchId]
    );

    return res.json({
      recurringGivings: rows,
      meta: {
        limit,
        offset,
        count: Number(count.count || 0),
        returned: rows.length,
      },
    });
  } catch (err) {
    console.error("[recurring-givings] list error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to load recurring givings" });
  }
});

router.post("/recurring-givings/:id/cancel", requireAuth, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const recurringId = String(req.params?.id || "").trim();
    if (!UUID_REGEX.test(recurringId)) return res.status(400).json({ error: "Invalid recurring giving id" });

    const row = await db.oneOrNone(
      `
      select id, member_id as "memberId", church_id as "churchId", status
      from recurring_givings
      where id=$1
      limit 1
      `,
      [recurringId]
    );
    if (!row) return res.status(404).json({ error: "Recurring giving not found" });
    if (row.churchId !== churchId) return res.status(403).json({ error: "Forbidden" });
    if (!isAdminRole(req.user?.role) && row.memberId !== req.user.id) {
      return res.status(403).json({ error: "Forbidden" });
    }
    if (["CANCELLED", "COMPLETED"].includes(String(row.status || "").toUpperCase())) {
      return res.json({ ok: true, alreadyCancelled: true });
    }

    const updated = await db.one(
      `
      update recurring_givings
      set status='CANCELLED', cancelled_at=now(), updated_at=now()
      where id=$1
      returning
        id, status, payfast_token as "payfastToken", updated_at as "updatedAt", cancelled_at as "cancelledAt"
      `,
      [recurringId]
    );

    return res.json({ ok: true, recurringGiving: updated });
  } catch (err) {
    console.error("[recurring-givings] cancel error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to cancel recurring giving" });
  }
});

// ==========================
// PayFast: initiate payment
// ==========================
router.post("/payfast/initiate", requireAuth, async (req, res) => {
  try {
    let { fundId, amount, channel = "app" } = req.body || {};

    const baseUrl = normalizeBaseUrl();
    if (!baseUrl) return res.status(500).json({ error: "Server misconfigured: BASE_URL missing" });

    const amt = Number(amount);
    if (!Number.isFinite(amt) || amt <= 0) return res.status(400).json({ error: "Invalid amount" });
    const pricing = buildFeeBreakdown(amt);

    fundId = typeof fundId === "string" ? fundId.trim() : fundId;
    channel = typeof channel === "string" ? channel.trim() : channel;

    const member = await loadMember(req.user.id);
    if (!member.church_id) return res.status(400).json({ error: "Join a church first" });
    const churchId = member.church_id;

    if (!fundId || !UUID_REGEX.test(fundId)) return res.status(400).json({ error: "Invalid fundId" });

    const fund = await db.oneOrNone("select id, name, active from funds where id=$1 and church_id=$2", [fundId, churchId]);
    if (!fund || !fund.active) return res.status(404).json({ error: "Fund not found" });

    const itemNameRaw = `${fund.name}`;
    const itemName = itemNameRaw
      .normalize("NFKD")
      .replace(/[^\x20-\x7E]/g, "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 100);

    const intent = await db.one(
      `insert into payment_intents (
         church_id, fund_id, amount, status, member_name, member_phone, payer_name, payer_phone, payer_type, channel, provider, m_payment_id, item_name, platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct
       ) values (
         $1, $2, $3, 'PENDING', $4, $5, $6, $7, 'member', $8, 'payfast', gen_random_uuid(), $9, $10, $11, $12, $13, $14, $15
       ) returning id, amount, church_id, fund_id, m_payment_id, item_name, amount_gross, platform_fee_amount, superadmin_cut_amount`,
      [
        churchId,
        fundId,
        pricing.amount,
        member.full_name || null,
        member.phone || null,
        member.full_name || null,
        member.phone || null,
        channel || null,
        itemName,
        pricing.platformFeeAmount,
        pricing.platformFeePct,
        pricing.platformFeeFixed,
        pricing.amountGross,
        pricing.superadminCutAmount,
        pricing.superadminCutPct,
      ]
    );

    const callbackUrls = getPayfastCallbackUrls(intent.id, intent.m_payment_id || intent.id);
    const returnUrl = callbackUrls.returnUrl || `${baseUrl}/give?success=true`;
    const cancelUrl = callbackUrls.cancelUrl || `${baseUrl}/give?cancelled=true`;
    const notifyUrl = callbackUrls.notifyUrl || `${baseUrl}/webhooks/payfast/itn`;
    let checkoutResult = null;
    try {
      checkoutResult = await createCheckoutWithGateway(churchId, {
        amount: intent.amount_gross || pricing.amountGross,
        currency: "ZAR",
        source: "DIRECT_APP",
        metadata: { paymentIntentId: intent.id, fundId },
        providerIntentRef: intent.m_payment_id || intent.id,
        itemName: intent.item_name || fund.name,
        returnUrl,
        cancelUrl,
        notifyUrl,
        reference1: churchId,
        reference2: fundId,
        payerName: member.full_name,
      });
    } catch (err) {
      if (err?.code === "PAYFAST_NOT_CONNECTED") {
        return res.status(503).json({
          error: "Payments are not activated for this church. Ask your church admin to connect PayFast.",
          code: "PAYFAST_NOT_CONNECTED",
        });
      }
      throw err;
    }
    const paymentUrl = checkoutResult.checkoutUrl;

    return res.json({
      paymentUrl,
      id: intent.id,
      pricing: {
        donationAmount: pricing.amount,
        churpayFee: pricing.platformFeeAmount,
        totalCharged: pricing.amountGross,
        superadminCutAmount: pricing.superadminCutAmount,
      },
    });
  } catch (err) {
    console.error("[payfast/initiate] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/payfast/return", (req, res) => {
  const pi = typeof req.query?.pi === "string" ? req.query.pi.trim() : "";
  const mp = typeof req.query?.mp === "string" ? req.query.mp.trim() : "";
  const deepBase = String(process.env.APP_DEEP_LINK_BASE || "churpaydemo://payfast").trim().replace(/\/+$/, "");
  const fallbackUrl = String(process.env.PAYFAST_APP_FALLBACK_URL || process.env.PUBLIC_BASE_URL || "https://www.churpay.com")
    .trim()
    .replace(/\/+$/, "");

  let deepLink = `${deepBase}/return`;
  deepLink = appendQueryParam(deepLink, "pi", pi);
  deepLink = appendQueryParam(deepLink, "mp", mp);

  const html = renderPayfastBridgePage({
    title: "Payment complete",
    message: "Returning to Churpay app...",
    deepLink,
    fallbackUrl,
  });

  res.setHeader("Content-Type", "text/html; charset=utf-8");
  return res.status(200).send(html);
});

router.get("/payfast/cancel", (req, res) => {
  const pi = typeof req.query?.pi === "string" ? req.query.pi.trim() : "";
  const mp = typeof req.query?.mp === "string" ? req.query.mp.trim() : "";
  const deepBase = String(process.env.APP_DEEP_LINK_BASE || "churpaydemo://payfast").trim().replace(/\/+$/, "");
  const fallbackUrl = String(process.env.PAYFAST_APP_FALLBACK_URL || process.env.PUBLIC_BASE_URL || "https://www.churpay.com")
    .trim()
    .replace(/\/+$/, "");

  let deepLink = `${deepBase}/cancel`;
  deepLink = appendQueryParam(deepLink, "pi", pi);
  deepLink = appendQueryParam(deepLink, "mp", mp);

  const html = renderPayfastBridgePage({
    title: "Payment cancelled",
    message: "Returning to Churpay app...",
    deepLink,
    fallbackUrl,
  });

  res.setHeader("Content-Type", "text/html; charset=utf-8");
  return res.status(200).send(html);
});

// Legacy alias for old notify URLs. Canonical endpoint is /webhooks/payfast/itn.
router.post("/payfast/itn", payfastItnRawParser, (req, res) => {
  console.warn("[payments/payfast/itn] deprecated path hit; use /webhooks/payfast/itn");
  return handlePayfastItn(req, res);
});

router.get("/payment-intents/:id", requireAuth, async (req, res) => {
  try {
    const pi = await db.one("select * from payment_intents where id=$1", [req.params.id]);
    const ownChurchId = req.user?.church_id || null;
    const isAdmin = isAdminRole(req.user?.role);
    const userEmail = String(req.user?.email || "").trim().toLowerCase();
    const userPhone = normalizePhoneIdentity(req.user?.phone || "");
    const intentEmail = String(pi?.payer_email || "").trim().toLowerCase();
    const intentPhone = normalizePhoneIdentity(pi?.payer_phone || pi?.member_phone || "");
    const isExternalIntent = String(pi?.source || "").trim().toUpperCase() === EXTERNAL_GIVING_SOURCE;
    const canAccessExternalDonorIntent = isExternalIntent && (
      (userEmail && intentEmail && userEmail === intentEmail) ||
      (userPhone && intentPhone && userPhone === intentPhone)
    );
    if (!isAdmin && (!ownChurchId || ownChurchId !== pi.church_id) && !canAccessExternalDonorIntent) {
      return res.status(403).json({ error: "Forbidden" });
    }
    res.json(pi);
  } catch (err) {
    if (err.message.includes("Expected 1 row, got 0")) {
      return res.status(404).json({ error: "Payment intent not found" });
    }
    console.error("[payments] GET /payment-intents/:id error", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// ------------------------------------------------------------
// SIMULATED PAYMENT (MVP / DEMO MODE)
// Creates a PAID payment_intent + inserts a transaction ledger row
// ------------------------------------------------------------
if (!isProduction) {
  router.post("/simulate-payment", requireAuth, async (req, res) => {
    try {
      let { fundId, amount, channel = "app" } = req.body || {};

      const amt = Number(amount);
      if (!Number.isFinite(amt) || amt <= 0) {
        return res.status(400).json({ error: "Invalid amount" });
      }
      const pricing = buildFeeBreakdown(amt);

      fundId = typeof fundId === "string" ? fundId.trim() : fundId;

      const member = await loadMember(req.user.id);
      if (!member.church_id) return res.status(400).json({ error: "Join a church first" });
      const churchId = member.church_id;

      if (!fundId) {
        return res.status(400).json({ error: "Missing fundId" });
      }

      // Validate church + fund exist
      let fund, church;
      try {
        fund = await db.one("select id, name, active from funds where id=$1 and church_id=$2", [fundId, churchId]);
        if (!fund.active) return res.status(400).json({ error: "Fund is inactive" });
        church = await db.one("select id, name from churches where id=$1", [churchId]);
      } catch (err) {
        if (err.message.includes("Expected 1 row, got 0")) {
          return res.status(404).json({ error: "Church or fund not found" });
        }
        console.error("[simulate] DB error fetching church/fund", err);
        return res.status(500).json({ error: "Internal server error" });
      }

      const mPaymentId = makeMpaymentId();

      // Same PayFast-safe item name rules (ASCII + short)
      const itemNameRaw = `${church.name} - ${fund.name}`;
      const itemName = itemNameRaw
        .normalize("NFKD")
        .replace(/[^\x20-\x7E]/g, "")
        .replace(/\s+/g, " ")
        .trim()
        .slice(0, 100);

      const providerPaymentId = `SIM-${crypto.randomBytes(6).toString("hex").toUpperCase()}`;

      const result = await db.tx(async (t) => {
        // Create intent already PAID
        const intent = await t.one(
          `
          insert into payment_intents
            (church_id, fund_id, amount, currency, member_name, member_phone, payer_name, payer_phone, payer_type, status, provider, provider_payment_id, m_payment_id, item_name, platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct, created_at, updated_at)
          values
            ($1,$2,$3,'ZAR',$4,$5,$6,$7,'member','PAID','simulated',$8,$9,$10,$11,$12,$13,$14,$15,$16,now(),now())
          returning *
          `,
          [
            churchId,
            fundId,
            pricing.amount,
            member.full_name || "",
            member.phone || "",
            member.full_name || "",
            member.phone || "",
            providerPaymentId,
            mPaymentId,
            itemName,
            pricing.platformFeeAmount,
            pricing.platformFeePct,
            pricing.platformFeeFixed,
            pricing.amountGross,
            pricing.superadminCutAmount,
            pricing.superadminCutPct,
          ]
        );

        // Insert ledger transaction row
        const txRow = await t.one(
          `
          insert into transactions
            (church_id, fund_id, payment_intent_id, amount, platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct, payer_name, payer_phone, payer_type, reference, channel, provider, provider_payment_id, created_at)
          values
            ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'member',$13,$14,'simulated',$15,now())
          returning *
          `,
          [
            churchId,
            fundId,
            intent.id,
            intent.amount,
            intent.platform_fee_amount || 0,
            intent.platform_fee_pct || readFeeConfig().pct,
            intent.platform_fee_fixed || readFeeConfig().fixed,
            intent.amount_gross || intent.amount,
            intent.superadmin_cut_amount || 0,
            intent.superadmin_cut_pct || readFeeConfig().superPct,
            intent.payer_name || intent.member_name || "",
            intent.payer_phone || intent.member_phone || "",
            intent.m_payment_id,
            channel || "app",
            providerPaymentId,
          ]
        );

        return { intent, txRow };
      });

      return res.json({
        ok: true,
        paymentIntentId: result.intent.id,
        status: result.intent.status,
        transactionId: result.txRow.id,
        receipt: {
          reference: result.txRow.reference,
          amount: result.txRow.amount,
          fee: result.txRow.platform_fee_amount,
          totalCharged: result.txRow.amount_gross,
          fund: fund.name,
          church: church.name,
          channel: result.txRow.channel,
          createdAt: result.txRow.created_at,
        },
      });
    } catch (err) {
      console.error("[simulate] POST /simulate-payment error", err);
      return res.status(500).json({ error: "Internal server error" });
    }
  });
} else {
  router.post("/simulate-payment", (_req, res) => {
    res.status(404).json({ error: "Not found" });
  });
}
router.get("/churches/:churchId/totals", requireAdmin, async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, req.params.churchId);
    if (!churchId) return;

    const rows = await db.manyOrNone(
      `
      select
        f.code,
        f.name,
        coalesce(sum(case when ${TX_STATUS_EXPR} = any($2) then t.amount else 0 end),0)::numeric(12,2) as total
      from funds f
      left join transactions t on t.fund_id=f.id and t.church_id=f.church_id
      left join payment_intents pi on pi.id = t.payment_intent_id
      where f.church_id=$1
      group by f.code, f.name
      order by f.name asc
      `,
      [churchId, STATEMENT_DEFAULT_STATUSES]
    );

    const grand = rows.reduce((acc, r) => acc + Number(r.total), 0);

    res.json({ totals: rows, grandTotal: grand.toFixed(2) });
  } catch (err) {
    console.error("[totals] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/churches/payfast/status", requireAdmin, async (req, res) => {
  try {
    if (!requireChurchAdminRole(req, res)) return;
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    const status = await getChurchPayfastStatus(churchId);
    return res.json({
      connected: !!status.connected,
      connectedAt: status.connectedAt || null,
      mode: status.mode || normalizePayfastMode(process.env.PAYFAST_MODE),
      merchantIdMasked: status.merchantIdMasked || "",
      merchantKeyMasked: status.merchantKeyMasked || "",
      storageReady: !!status.storageReady,
      encryptionKeyConfigured: !!status.encryptionKeyConfigured,
      fallbackEnabled: !!status.fallbackEnabled,
      lastAttemptAt: status.lastAttemptAt || null,
      lastAttemptStatus: status.lastAttemptStatus || null,
      lastAttemptError: status.lastAttemptError || null,
    });
  } catch (err) {
    console.error("[churches/payfast/status] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/churches/payfast/connect", requireAdmin, payfastConnectRateLimiter, async (req, res) => {
  try {
    if (!requireChurchAdminRole(req, res)) return;
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    const merchantId = String(req.body?.merchantId || "").trim();
    const merchantKey = String(req.body?.merchantKey || "").trim();
    const passphrase = String(req.body?.passphrase || "").trim();

    if (!merchantId || !merchantKey) {
      return res.status(400).json({ error: "merchantId and merchantKey are required" });
    }

    const validation = await validatePayfastCredentialConnection({
      merchantId,
      merchantKey,
      passphrase,
      mode: process.env.PAYFAST_MODE,
    });

    if (!validation?.ok) {
      const failedMessage = validation?.error || "Invalid Merchant Credentials";
      await recordChurchPayfastConnectAttempt({
        churchId,
        status: "failed",
        error: failedMessage,
      });

      if (validation?.code === "PAYFAST_VALIDATION_UNAVAILABLE") {
        return res.status(503).json({ error: failedMessage, code: validation.code });
      }
      return res.status(400).json({ error: "Invalid Merchant Credentials" });
    }

    await connectChurchPayfastCredentials({
      churchId,
      merchantId,
      merchantKey,
      passphrase,
    });

    const merchantIdMasked =
      merchantId.length > 5 ? `${merchantId.slice(0, 3)}${"*".repeat(Math.max(1, merchantId.length - 5))}${merchantId.slice(-2)}` : "***";
    console.info("[churches/payfast/connect] connected", {
      churchId,
      adminId: req.user?.id || null,
      merchantIdMasked,
    });

    return res.json({ status: "connected" });
  } catch (err) {
    const code = String(err?.code || "");
    if (code === "PAYFAST_STORAGE_NOT_READY") {
      return res.status(503).json({ error: "PayFast credential storage is not ready. Run migrations and retry." });
    }
    if (code === "PAYFAST_CREDENTIAL_ENCRYPTION_KEY_MISSING") {
      return res.status(500).json({ error: "Server misconfigured: PAYFAST_CREDENTIAL_ENCRYPTION_KEY missing" });
    }
    console.error("[churches/payfast/connect] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/churches/payfast/disconnect", requireAdmin, async (req, res) => {
  try {
    if (!requireChurchAdminRole(req, res)) return;
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    await disconnectChurchPayfastCredentials(churchId);
    console.info("[churches/payfast/disconnect] disconnected", {
      churchId,
      adminId: req.user?.id || null,
    });

    return res.json({ status: "disconnected" });
  } catch (err) {
    const code = String(err?.code || "");
    if (code === "PAYFAST_STORAGE_NOT_READY") {
      return res.status(503).json({ error: "PayFast credential storage is not ready. Run migrations and retry." });
    }
    console.error("[churches/payfast/disconnect] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/admin/portal-settings", requireStaff, async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, "me");
    if (!churchId) return;

    const role = String(req.user?.role || "").toLowerCase();
    const access = await getAdminPortalAccess({ role, churchId });
    const accountantTabs = normalizeAdminPortalTabs(access.settings?.accountantTabs || []);

    return res.json({
      ok: true,
      role: access.role,
      allowedTabs: access.allowedTabs,
      settings: { accountantTabs },
      churchOperations: access.churchOperations || { ...normalizeChurchOperationsSubscription(null), hasAccess: false },
    });
  } catch (err) {
    console.error("[admin/portal-settings] GET error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.patch("/admin/portal-settings", requireAdmin, async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, "me");
    if (!churchId) return;

    const accountantTabs = normalizeAdminPortalTabs(req.body?.accountantTabs || []);
    if (!accountantTabs.length) {
      return res.status(400).json({ error: "Select at least one accountant tab." });
    }

    try {
      await db.none(
        `
        update churches
        set admin_portal_settings = jsonb_set(
          coalesce(admin_portal_settings, '{}'::jsonb),
          '{accountantTabs}',
          $2::jsonb,
          true
        )
        where id = $1
        `,
        [churchId, JSON.stringify(accountantTabs)]
      );
    } catch (err) {
      if (err?.code === "42703") {
        return res.status(503).json({ error: "Portal settings not available yet. Run migrations and retry." });
      }
      throw err;
    }

    return res.json({ ok: true, settings: { accountantTabs } });
  } catch (err) {
    console.error("[admin/portal-settings] PATCH error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/admin/church-operations/subscription", requireStaff, async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, "me");
    if (!churchId) return;
    const role = String(req.user?.role || "").toLowerCase();
    const access = await getAdminPortalAccess({ role, churchId });
    const subscription = access?.churchOperations || normalizeChurchOperationsSubscription(null);
    const hasAccess = !!subscription?.hasAccess;
    const status = String(subscription?.status || "SUSPENDED").trim().toUpperCase();
    let nextAction = "ACTIVATE";
    if (status === "ACTIVE") nextAction = "NONE";
    else if (status === "PAST_DUE" || status === "GRACE") nextAction = "UPDATE_PAYMENT";
    else if (status === "SUSPENDED" || status === "CANCELED") nextAction = "RENEW";
    const trialDaysRemaining = daysUntilDateTime(subscription?.trialEndsAt || null);
    const graceDaysRemaining = daysUntilDateTime(subscription?.graceEndsAt || null);
    return res.json({
      ok: true,
      churchId,
      hasAccess,
      nextAction,
      trialDaysRemaining,
      graceDaysRemaining,
      subscription,
    });
  } catch (err) {
    console.error("[admin/church-operations/subscription] GET error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/admin/church-operations/subscription/request", requireAdmin, async (req, res) => {
  try {
    if (!requireChurchAdminRole(req, res)) return;
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const requestedPlan = normalizeChurchSubscriptionPlanCode(req.body?.planCode || DEFAULT_CHURCH_SUBSCRIPTION_PLAN_CODE);
    let subscription = await loadChurchOperationsSubscription(churchId);
    const hadCanonicalRow = !!subscription?.id;

    if (!hadCanonicalRow) {
      subscription = await ensureChurchTrialSubscription({
        churchId,
        planCode: requestedPlan,
        actorType: "ADMIN",
        actorId: req.user?.id || null,
        source: "admin_portal_request",
      });
    }

    const computed = computeSubscriptionAccess(subscription);
    subscription = normalizeChurchOperationsSubscription({
      ...subscription,
      planCode: subscription?.planCode || requestedPlan,
      hasAccess: computed.hasAccess,
      status: computed.status,
      reason: computed.reason,
      accessLevel: computed.accessLevel,
      banner: computed.banner,
    });

    const trialStarted = !hadCanonicalRow && subscription.status === "TRIALING";
    const alreadyActive = subscription.status === "ACTIVE" && subscription.hasAccess === true;
    const paymentRequired = !alreadyActive;

    let checkout = null;
    let checkoutWarning = null;
    if (paymentRequired) {
      try {
        const member = await loadMember(req.user.id);
        checkout = await createGrowthSubscriptionCheckout({
          churchId,
          member,
          subscription: { ...subscription, planCode: requestedPlan },
        });
      } catch (err) {
        if (err?.code === "PAYFAST_SUBSCRIPTION_NOT_CONFIGURED") {
          checkoutWarning = err.message;
        } else if (err?.code === "INVALID_SUBSCRIPTION_AMOUNT") {
          return res.status(400).json({ error: err.message });
        } else {
          throw err;
        }
      }
    }

    const status = String(subscription?.status || "SUSPENDED").trim().toUpperCase();
    const nextAction = checkout?.checkoutUrl
      ? "PAYFAST_CHECKOUT"
      : checkoutWarning
      ? "CONFIGURE_PAYFAST"
      : status === "PAST_DUE" || status === "GRACE"
      ? "UPDATE_PAYMENT"
      : status === "SUSPENDED" || status === "CANCELED"
      ? "RENEW"
      : status === "ACTIVE"
      ? "NONE"
      : "ACTIVATE";

    return res.json({
      ok: true,
      requested: true,
      trialStarted,
      paymentRequired,
      trialDays: DEFAULT_CHURCH_SUBSCRIPTION_TRIAL_DAYS,
      trialEndsAt: subscription.trialEndsAt || null,
      alreadyActive,
      checkoutUrl: checkout?.checkoutUrl || null,
      paymentIntentId: checkout?.paymentIntentId || null,
      mPaymentId: checkout?.mPaymentId || null,
      credentialSource: checkout?.credentialSource || null,
      checkoutWarning,
      nextAction,
      subscription,
    });
  } catch (err) {
    if (err?.code === "SUBSCRIPTION_NOT_READY") {
      return res.status(503).json({ error: err.message });
    }
    console.error("[admin/church-operations/subscription] REQUEST error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get(
  "/admin/operations/overview",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchOperationsAccess,
  requireChurchLifePermission("ops.overview.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      const periodDays = 30;
      const churchLifeCount = async (sql, params = [churchId]) => {
        try {
          return await db.one(sql, params);
        } catch (err) {
          if (err?.code === "42P01" || err?.code === "42703") return { count: 0 };
          throw err;
        }
      };
      const [membersRow, fundsRow, txRow, giversRow, givingRow, checkInsRow, apologiesRow, servicesRow, attendance] = await Promise.all([
        db.one(`select count(*)::int as count from members where church_id=$1`, [churchId]),
        db.one(`select count(*)::int as count from funds where church_id=$1 and coalesce(active, true)=true`, [churchId]),
        db.one(
          `select count(*)::int as count from transactions where church_id=$1 and created_at >= now() - interval '${periodDays} days'`,
          [churchId]
        ),
        db.one(
          `
          select count(distinct coalesce(nullif(payer_phone,''), nullif(member_phone,''), nullif(payer_email,''), nullif(member_name,'')))::int as count
          from payment_intents
          where church_id=$1 and created_at >= now() - interval '${periodDays} days'
          `,
          [churchId]
        ),
        db.one(
          `
          select coalesce(sum(amount), 0)::numeric(12,2) as total
          from transactions
          where church_id=$1 and created_at >= now() - interval '${periodDays} days'
          `,
          [churchId]
        ),
        churchLifeCount(
          `
          select count(*)::int as count
          from church_checkins c
          join church_services s on s.id = c.service_id
          where c.church_id=$1
            and c.checked_in_at >= now() - interval '${periodDays} days'
            and ($2::uuid is null or s.campus_id = $2::uuid)
          `
          ,
          [churchId, campusId]
        ),
        churchLifeCount(
          `
          select count(*)::int as count
          from church_apologies a
          join church_services s on s.id = a.service_id
          where a.church_id=$1
            and a.created_at >= now() - interval '${periodDays} days'
            and ($2::uuid is null or s.campus_id = $2::uuid)
          `
          ,
          [churchId, campusId]
        ),
        churchLifeCount(
          `
          select count(*)::int as count
          from church_services
          where church_id=$1 and service_date >= (current_date - (${periodDays} * interval '1 day'))::date
            and ($2::uuid is null or campus_id = $2::uuid)
          `
          ,
          [churchId, campusId]
        ),
        readChurchAttendanceSummary(churchId, campusId),
      ]);

      return res.json({
        ok: true,
        summary: {
          members: Number(membersRow?.count || 0),
          activeFunds: Number(fundsRow?.count || 0),
          transactionsLast30Days: Number(txRow?.count || 0),
          uniqueGiversLast30Days: Number(giversRow?.count || 0),
          givingTotalLast30Days: Number(givingRow?.total || 0),
          checkInsLast30Days: Number(checkInsRow?.count || 0),
          apologiesLast30Days: Number(apologiesRow?.count || 0),
          servicesLast30Days: Number(servicesRow?.count || 0),
        },
        attendance,
        subscription: req.churchOperationsSubscription || null,
        meta: { periodDays, campusId: campusId || null },
      });
    } catch (err) {
      console.error("[admin/operations/overview] error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/campuses",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("services.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const includeInactive = toBoolean(req.query?.includeInactive) === true;

      const rows = await db.manyOrNone(
        `
        select
          id,
          church_id as "churchId",
          name,
          code,
          status,
          created_at as "createdAt",
          updated_at as "updatedAt"
        from church_campuses
        where church_id = $1
          and ($2::boolean = true or status = 'ACTIVE')
        order by
          case when upper(code) = 'MAIN' then 0 else 1 end asc,
          lower(name) asc,
          created_at asc
        `,
        [churchId, includeInactive]
      );

      return res.json({
        ok: true,
        campuses: rows.map(normalizeChurchCampusRow),
        meta: { includeInactive, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Campus management is not available yet. Run migrations and retry." });
      }
      console.error("[admin/campuses] list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/campuses",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("services.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const name = String(req.body?.name || "").trim().slice(0, 120);
      if (!name) return res.status(400).json({ error: "name is required." });
      const fallbackCodeFromName = name.replace(/[^A-Za-z0-9]+/g, "_");
      const code = normalizeChurchCampusCode(req.body?.code || fallbackCodeFromName || "CAMPUS");
      const status = normalizeChurchCampusStatus(req.body?.status || "ACTIVE");

      const row = await db.one(
        `
        insert into church_campuses (church_id, name, code, status, created_at, updated_at)
        values ($1,$2,$3,$4,now(),now())
        returning
          id,
          church_id as "churchId",
          name,
          code,
          status,
          created_at as "createdAt",
          updated_at as "updatedAt"
        `,
        [churchId, name, code, status]
      );

      return res.status(201).json({ ok: true, campus: normalizeChurchCampusRow(row) });
    } catch (err) {
      if (err?.code === "23505") {
        return res.status(409).json({ error: "A campus with this name/code already exists in this church." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Campus management is not available yet. Run migrations and retry." });
      }
      console.error("[admin/campuses] create error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/campuses/:id",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("services.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const campusId = String(req.params?.id || "").trim();
      if (!UUID_REGEX.test(campusId)) return res.status(400).json({ error: "Invalid campus id." });

      const current = await findChurchCampusById(churchId, campusId);
      if (!current) return res.status(404).json({ error: "Campus not found." });

      const name = String(typeof req.body?.name === "undefined" ? current.name || "" : req.body?.name || "")
        .trim()
        .slice(0, 120);
      if (!name) return res.status(400).json({ error: "name is required." });
      const code = normalizeChurchCampusCode(
        typeof req.body?.code === "undefined" ? current.code || "MAIN" : req.body?.code
      );
      const status = normalizeChurchCampusStatus(
        typeof req.body?.status === "undefined" ? current.status || "ACTIVE" : req.body?.status
      );

      const row = await db.one(
        `
        update church_campuses
        set
          name = $3,
          code = $4,
          status = $5,
          updated_at = now()
        where id = $1 and church_id = $2
        returning
          id,
          church_id as "churchId",
          name,
          code,
          status,
          created_at as "createdAt",
          updated_at as "updatedAt"
        `,
        [campusId, churchId, name, code, status]
      );

      return res.json({ ok: true, campus: normalizeChurchCampusRow(row) });
    } catch (err) {
      if (err?.code === "23505") {
        return res.status(409).json({ error: "A campus with this name/code already exists in this church." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Campus management is not available yet. Run migrations and retry." });
      }
      console.error("[admin/campuses] patch error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/operations/attendance",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchOperationsAccess,
  requireChurchLifePermission("ops.attendance.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      const limitRequested = Number(req.query?.limit || 20);
      const limit = Math.min(Math.max(Number.isFinite(limitRequested) ? Math.trunc(limitRequested) : 20, 1), 104);

      const rows = await db.manyOrNone(
        `
        select
          a.id,
          a.service_id as "serviceId",
          s.service_name as "serviceName",
          a.campus_id as "campusId",
          cc.name as "campusName",
          cc.code as "campusCode",
          a.service_date as "serviceDate",
          a.total_attendance as "totalAttendance",
          a.adults_count as "adultsCount",
          a.youth_count as "youthCount",
          a.children_count as "childrenCount",
          a.first_time_guests as "firstTimeGuests",
          a.notes,
          a.created_at as "createdAt",
          a.updated_at as "updatedAt"
        from church_attendance_records a
        left join church_services s on s.id = a.service_id and s.church_id = a.church_id
        left join church_campuses cc on cc.id = a.campus_id
        where a.church_id = $1
          and ($2::uuid is null or a.campus_id = $2::uuid)
        order by a.service_date desc, a.updated_at desc
        limit $3
        `,
        [churchId, campusId, limit]
      );
      const summary = await readChurchAttendanceSummary(churchId, campusId);

      return res.json({
        ok: true,
        rows: rows.map(normalizeAttendanceRow),
        summary,
        meta: { limit, returned: rows.length, campusId: campusId || null },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({
          error: "Attendance tracking is not available yet. Run migrations and retry.",
        });
      }
      console.error("[admin/operations/attendance] GET error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/operations/attendance",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchOperationsAccess,
  requireChurchLifePermission("ops.attendance.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      let requestedCampusId = null;
      try {
        requestedCampusId = await resolveChurchCampusId(churchId, req.body?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      const serviceId = String(req.body?.serviceId || "").trim();
      let selectedService = null;
      if (serviceId) {
        if (!UUID_REGEX.test(serviceId)) {
          return res.status(400).json({ error: "serviceId must be a UUID." });
        }
        selectedService = await findChurchServiceById(churchId, serviceId);
        if (!selectedService) {
          return res.status(400).json({ error: "serviceId is not in this church." });
        }
        const serviceCampusId = parseUuidOrNull(selectedService?.campusId);
        if (requestedCampusId && serviceCampusId !== requestedCampusId) {
          return res.status(400).json({ error: "serviceId does not belong to the selected campus." });
        }
      }

      const providedServiceDate = parseDateOnlyOrNull(req.body?.serviceDate);
      const selectedServiceDate = selectedService ? formatDateIsoLike(selectedService.serviceDate) : "";
      if (selectedServiceDate && providedServiceDate && selectedServiceDate !== providedServiceDate) {
        return res.status(400).json({ error: "serviceDate must match the selected service date." });
      }
      const serviceDate = selectedServiceDate || providedServiceDate;
      if (!serviceDate) {
        return res.status(400).json({ error: "serviceDate must be YYYY-MM-DD (or provide serviceId)." });
      }
      const finalCampusId = requestedCampusId || parseUuidOrNull(selectedService?.campusId) || null;

      const adultsRaw = req.body?.adultsCount;
      const youthRaw = req.body?.youthCount;
      const childrenRaw = req.body?.childrenCount;
      const totalRaw = String(req.body?.totalAttendance ?? "").trim();
      const totalParsed = parseNonNegativeWholeNumber(totalRaw, "totalAttendance");
      if (totalParsed.error) return res.status(400).json({ error: totalParsed.error });
      const adultsParsed = parseNonNegativeWholeNumber(adultsRaw, "adultsCount");
      if (adultsParsed.error) return res.status(400).json({ error: adultsParsed.error });
      const youthParsed = parseNonNegativeWholeNumber(youthRaw, "youthCount");
      if (youthParsed.error) return res.status(400).json({ error: youthParsed.error });
      const childrenParsed = parseNonNegativeWholeNumber(childrenRaw, "childrenCount");
      if (childrenParsed.error) return res.status(400).json({ error: childrenParsed.error });
      const guestsParsed = parseNonNegativeWholeNumber(req.body?.firstTimeGuests, "firstTimeGuests");
      if (guestsParsed.error) return res.status(400).json({ error: guestsParsed.error });

      const adultsCount = adultsParsed.value;
      const youthCount = youthParsed.value;
      const childrenCount = childrenParsed.value;
      const firstTimeGuests = guestsParsed.value;
      const breakdownTotal = adultsCount + youthCount + childrenCount;
      const hasBreakdownInput = [adultsRaw, youthRaw, childrenRaw].some((value) => String(value ?? "").trim() !== "");
      const totalAttendance = hasBreakdownInput ? breakdownTotal : totalParsed.value;

      const notesRaw = typeof req.body?.notes === "string" ? req.body.notes : "";
      const notes = notesRaw.trim().slice(0, 1000) || null;
      const actorId = parseUuidOrNull(req.user?.id);

      let row = null;
      if (selectedService?.id) {
        row = await db.one(
          `
          insert into church_attendance_records (
            church_id,
            service_id,
            campus_id,
            service_date,
            total_attendance,
            adults_count,
            youth_count,
            children_count,
            first_time_guests,
            notes,
            created_by,
            updated_by
          )
          values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11)
          on conflict (church_id, service_id)
          do update set
            campus_id = excluded.campus_id,
            service_date = excluded.service_date,
            total_attendance = excluded.total_attendance,
            adults_count = excluded.adults_count,
            youth_count = excluded.youth_count,
            children_count = excluded.children_count,
            first_time_guests = excluded.first_time_guests,
            notes = excluded.notes,
            updated_by = excluded.updated_by,
            updated_at = now()
          returning
            id,
            service_id as "serviceId",
            campus_id as "campusId",
            service_date as "serviceDate",
            total_attendance as "totalAttendance",
            adults_count as "adultsCount",
            youth_count as "youthCount",
            children_count as "childrenCount",
            first_time_guests as "firstTimeGuests",
            notes,
            created_at as "createdAt",
            updated_at as "updatedAt"
          `,
          [
            churchId,
            selectedService.id,
            finalCampusId,
            serviceDate,
            totalAttendance,
            adultsCount,
            youthCount,
            childrenCount,
            firstTimeGuests,
            notes,
            actorId,
          ]
        );
      } else {
        row = await db.oneOrNone(
          `
          with target as (
            select id
            from church_attendance_records
            where church_id = $1
              and service_id is null
              and service_date = $2
              and (
                ($3::uuid is null and campus_id is null)
                or campus_id = $3::uuid
              )
            order by updated_at desc
            limit 1
          )
          update church_attendance_records a
          set
            campus_id = $3,
            service_date = $2,
            total_attendance = $4,
            adults_count = $5,
            youth_count = $6,
            children_count = $7,
            first_time_guests = $8,
            notes = $9,
            service_id = null,
            updated_by = $10,
            updated_at = now()
          from target t
          where a.id = t.id
          returning
            a.id,
            a.service_id as "serviceId",
            a.campus_id as "campusId",
            a.service_date as "serviceDate",
            a.total_attendance as "totalAttendance",
            a.adults_count as "adultsCount",
            a.youth_count as "youthCount",
            a.children_count as "childrenCount",
            a.first_time_guests as "firstTimeGuests",
            a.notes,
            a.created_at as "createdAt",
            a.updated_at as "updatedAt"
          `,
          [
            churchId,
            serviceDate,
            finalCampusId,
            totalAttendance,
            adultsCount,
            youthCount,
            childrenCount,
            firstTimeGuests,
            notes,
            actorId,
          ]
        );

        if (!row) {
          row = await db.one(
            `
            insert into church_attendance_records (
              church_id,
              service_id,
              campus_id,
              service_date,
              total_attendance,
              adults_count,
              youth_count,
              children_count,
              first_time_guests,
              notes,
              created_by,
              updated_by
            )
            values ($1,null,$2,$3,$4,$5,$6,$7,$8,$9,$10,$10)
            returning
              id,
              service_id as "serviceId",
              campus_id as "campusId",
              service_date as "serviceDate",
              total_attendance as "totalAttendance",
              adults_count as "adultsCount",
              youth_count as "youthCount",
              children_count as "childrenCount",
              first_time_guests as "firstTimeGuests",
              notes,
              created_at as "createdAt",
              updated_at as "updatedAt"
            `,
            [
              churchId,
              finalCampusId,
              serviceDate,
              totalAttendance,
              adultsCount,
              youthCount,
              childrenCount,
              firstTimeGuests,
              notes,
              actorId,
            ]
          );
        }
      }

      let campus = null;
      if (row?.campusId) {
        campus = await findChurchCampusById(churchId, row.campusId);
      }
      const summary = await readChurchAttendanceSummary(churchId, requestedCampusId);
      return res.status(201).json({
        ok: true,
        row: normalizeAttendanceRow({
          ...row,
          serviceId: selectedService?.id || null,
          serviceName: selectedService?.serviceName || null,
          campusName: campus?.name || null,
          campusCode: campus?.code || null,
        }),
        summary,
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({
          error: "Attendance tracking is not available yet. Run migrations and retry.",
        });
      }
      if (err?.code === "23514") {
        return res.status(400).json({
          error: "Invalid attendance values. Check totals and breakdown counts.",
        });
      }
      console.error("[admin/operations/attendance] POST error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/operations/insights",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchOperationsAccess,
  requireChurchLifePermission("ops.insights.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const weeksRequested = Number(req.query?.weeks || 12);
      const weeks = Math.min(Math.max(Number.isFinite(weeksRequested) ? Math.trunc(weeksRequested) : 12, 4), 52);
      const serviceLimitRequested = Number(req.query?.serviceLimit || 20);
      const serviceLimit = Math.min(
        Math.max(Number.isFinite(serviceLimitRequested) ? Math.trunc(serviceLimitRequested) : 20, 5),
        120
      );
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      const now = new Date();
      const utcToday = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
      const daysFromMonday = (utcToday.getUTCDay() + 6) % 7;
      const currentWeekStart = new Date(utcToday);
      currentWeekStart.setUTCDate(currentWeekStart.getUTCDate() - daysFromMonday);
      const periodStartDate = new Date(currentWeekStart);
      periodStartDate.setUTCDate(periodStartDate.getUTCDate() - (weeks - 1) * 7);
      const periodStartIso = periodStartDate.toISOString().slice(0, 10);

      const [
        hasTxServiceDate,
        hasTxOnBehalfMember,
        hasPiOnBehalfMember,
        hasPiPayerEmail,
        hasPiPayerPhone,
        hasPiPayerName,
        hasPiMemberPhone,
      ] = await Promise.all([
        hasDbColumn("transactions", "service_date"),
        hasDbColumn("transactions", "on_behalf_of_member_id"),
        hasDbColumn("payment_intents", "on_behalf_of_member_id"),
        hasDbColumn("payment_intents", "payer_email"),
        hasDbColumn("payment_intents", "payer_phone"),
        hasDbColumn("payment_intents", "payer_name"),
        hasDbColumn("payment_intents", "member_phone"),
      ]);

      const txServiceDateExpr = hasTxServiceDate
        ? "coalesce(t.service_date, (t.created_at at time zone 'UTC')::date)"
        : "(t.created_at at time zone 'UTC')::date";

      const donorKeyParts = [];
      if (hasPiPayerEmail) donorKeyParts.push("nullif(lower(pi.payer_email), '')");
      if (hasPiPayerPhone) donorKeyParts.push("nullif(pi.payer_phone, '')");
      if (hasPiPayerName) donorKeyParts.push("nullif(lower(pi.payer_name), '')");
      if (hasPiMemberPhone) donorKeyParts.push("nullif(pi.member_phone, '')");
      donorKeyParts.push("t.reference");
      const donorKeyExpr = `coalesce(${donorKeyParts.join(", ")})`;

      const atRiskMemberExpr = hasPiOnBehalfMember
        ? "pi.on_behalf_of_member_id"
        : hasTxOnBehalfMember
          ? "t.on_behalf_of_member_id"
          : null;

      const atRiskRowsPromise = atRiskMemberExpr
        ? db.manyOrNone(
            `
          with windows as (
            select
              (current_date - interval '28 days')::date as recent_start,
              (current_date - interval '56 days')::date as previous_start
          ),
          attendance_rollup as (
            select
              c.member_pk,
              count(*) filter (where s.service_date >= w.recent_start)::int as recent_attendance,
              count(*) filter (where s.service_date >= w.previous_start and s.service_date < w.recent_start)::int as previous_attendance
            from church_checkins c
            join church_services s on s.id = c.service_id
            cross join windows w
            where c.church_id = $1
              and s.service_date >= w.previous_start
              and ($3::uuid is null or s.campus_id = $3::uuid)
            group by c.member_pk
          ),
          giving_rollup as (
            select
              ${atRiskMemberExpr} as member_pk,
              coalesce(sum(case when t.created_at >= w.recent_start::timestamp then t.amount end), 0)::numeric(12,2) as recent_giving,
              coalesce(sum(case when t.created_at >= w.previous_start::timestamp and t.created_at < w.recent_start::timestamp then t.amount end), 0)::numeric(12,2) as previous_giving
            from transactions t
            join payment_intents pi on pi.id = t.payment_intent_id
            cross join windows w
            where t.church_id = $1
              and ${TX_STATUS_EXPR} = any($2)
              and ${atRiskMemberExpr} is not null
              and t.created_at >= w.previous_start::timestamp
            group by ${atRiskMemberExpr}
          ),
          risky as (
            select
              m.id as "memberPk",
              m.member_id as "memberId",
              m.full_name as "fullName",
              m.phone,
              m.email,
              coalesce(a.previous_attendance,0)::int as "previousAttendance",
              coalesce(a.recent_attendance,0)::int as "recentAttendance",
              coalesce(g.previous_giving,0)::numeric(12,2) as "previousGiving",
              coalesce(g.recent_giving,0)::numeric(12,2) as "recentGiving"
            from members m
            left join attendance_rollup a on a.member_pk = m.id
            left join giving_rollup g on g.member_pk = m.id
            where m.church_id = $1
          )
          select
            "memberPk",
            "memberId",
            "fullName",
            phone,
            email,
            "previousAttendance",
            "recentAttendance",
            "previousGiving",
            "recentGiving"
          from risky
          where "previousAttendance" > 0
            and "recentAttendance" < "previousAttendance"
            and "previousGiving" > 0
            and "recentGiving" < "previousGiving"
          order by ("previousAttendance" - "recentAttendance") desc, ("previousGiving" - "recentGiving") desc
          limit 20
          `,
            [churchId, STATEMENT_DEFAULT_STATUSES, campusId]
          )
        : Promise.resolve([]);

      const [weeklyRows, overallRow, retentionRow, byFundRows, byServiceRows, byCampusRows, atRiskRows] = await Promise.all([
        db.manyOrNone(
          `
          with series as (
            select generate_series(
              (date_trunc('week', now()) - (($2::int - 1) * interval '1 week'))::date,
              date_trunc('week', now())::date,
              interval '1 week'
            )::date as week_start
          ),
          first_seen as (
            select
              c.member_pk,
              min(s.service_date) as first_service_date
            from church_checkins c
            join church_services s on s.id = c.service_id
            where c.church_id = $1
              and ($4::uuid is null or s.campus_id = $4::uuid)
            group by c.member_pk
          ),
          weekly_attendance as (
            select
              date_trunc('week', s.service_date::timestamp)::date as week_start,
              count(*)::int as checkins_count,
              count(distinct c.member_pk)::int as unique_attendees,
              count(distinct case
                when date_trunc('week', fs.first_service_date::timestamp)::date = date_trunc('week', s.service_date::timestamp)::date
                then c.member_pk
                else null
              end)::int as first_time_visitors,
              count(distinct case
                when date_trunc('week', fs.first_service_date::timestamp)::date < date_trunc('week', s.service_date::timestamp)::date
                then c.member_pk
                else null
              end)::int as returning_visitors
            from church_checkins c
            join church_services s on s.id = c.service_id
            join first_seen fs on fs.member_pk = c.member_pk
            where c.church_id = $1
              and s.service_date >= (select min(week_start) from series)
              and ($4::uuid is null or s.campus_id = $4::uuid)
            group by 1
          ),
          weekly_giving as (
            select
              date_trunc('week', t.created_at)::date as week_start,
              coalesce(sum(t.amount),0)::numeric(12,2) as giving_amount,
              count(distinct ${donorKeyExpr})::int as donor_count
            from transactions t
            left join payment_intents pi on pi.id = t.payment_intent_id
            where t.church_id = $1
              and ${TX_STATUS_EXPR} = any($3)
              and t.created_at >= (select min(week_start)::timestamp from series)
            group by 1
          )
          select
            s.week_start as "weekStart",
            coalesce(a.checkins_count, 0)::int as "checkInsCount",
            coalesce(a.unique_attendees, 0)::int as "uniqueAttendees",
            coalesce(a.first_time_visitors, 0)::int as "firstTimeVisitors",
            coalesce(a.returning_visitors, 0)::int as "returningVisitors",
            coalesce(g.giving_amount, 0)::numeric(12,2) as "givingAmount",
            coalesce(g.donor_count, 0)::int as "donorCount"
          from series s
          left join weekly_attendance a on a.week_start = s.week_start
          left join weekly_giving g on g.week_start = s.week_start
          order by s.week_start asc
          `,
          [churchId, weeks, STATEMENT_DEFAULT_STATUSES, campusId]
        ),
        db.one(
          `
          with first_seen as (
            select
              c.member_pk,
              min(s.service_date) as first_service_date
            from church_checkins c
            join church_services s on s.id = c.service_id
            where c.church_id = $1
              and ($4::uuid is null or s.campus_id = $4::uuid)
            group by c.member_pk
          ),
          period_checkins as (
            select
              c.member_pk,
              s.service_date
            from church_checkins c
            join church_services s on s.id = c.service_id
            where c.church_id = $1
              and s.service_date >= $2::date
              and ($4::uuid is null or s.campus_id = $4::uuid)
          ),
          period_attendees as (
            select distinct member_pk
            from period_checkins
          ),
          period_givers as (
            select distinct ${donorKeyExpr} as donor_key
            from transactions t
            left join payment_intents pi on pi.id = t.payment_intent_id
            where t.church_id = $1
              and ${TX_STATUS_EXPR} = any($3)
              and t.created_at >= $2::timestamp
          ),
          period_giving as (
            select coalesce(sum(t.amount),0)::numeric(12,2) as total_amount
            from transactions t
            left join payment_intents pi on pi.id = t.payment_intent_id
            where t.church_id = $1
              and ${TX_STATUS_EXPR} = any($3)
              and t.created_at >= $2::timestamp
          )
          select
            (select count(*) from period_checkins)::int as "checkInsCount",
            (select count(*) from period_attendees)::int as "uniqueAttendees",
            (select count(*) from period_givers)::int as "uniqueGivers",
            coalesce((select total_amount from period_giving), 0)::numeric(12,2) as "givingTotal",
            coalesce((
              select count(*)
              from period_attendees pa
              join first_seen fs on fs.member_pk = pa.member_pk
              where fs.first_service_date >= $2::date
            ), 0)::int as "firstTimeVisitors",
            coalesce((
              select count(*)
              from period_attendees pa
              join first_seen fs on fs.member_pk = pa.member_pk
              where fs.first_service_date < $2::date
            ), 0)::int as "returningVisitors"
          `,
          [churchId, periodStartIso, STATEMENT_DEFAULT_STATUSES, campusId]
        ),
        db.one(
          `
          with first_seen as (
            select
              c.member_pk,
              min(s.service_date) as first_service_date
            from church_checkins c
            join church_services s on s.id = c.service_id
            where c.church_id = $1
              and ($3::uuid is null or s.campus_id = $3::uuid)
            group by c.member_pk
          ),
          eligible as (
            select
              fs.member_pk,
              fs.first_service_date
            from first_seen fs
            where fs.first_service_date >= $2::date
              and fs.first_service_date <= (current_date - interval '28 days')::date
          ),
          retained as (
            select e.member_pk
            from eligible e
            where exists (
              select 1
              from church_checkins c
              join church_services s on s.id = c.service_id
              where c.church_id = $1
                and c.member_pk = e.member_pk
                and s.service_date > e.first_service_date
                and s.service_date <= (e.first_service_date + 28)
                and ($3::uuid is null or s.campus_id = $3::uuid)
            )
          )
          select
            (select count(*) from eligible)::int as "eligibleCount",
            (select count(*) from retained)::int as "retainedCount"
          `,
          [churchId, periodStartIso, campusId]
        ),
        db.manyOrNone(
          `
          with attendees as (
            select count(distinct c.member_pk)::int as unique_attendees
            from church_checkins c
            join church_services s on s.id = c.service_id
            where c.church_id = $1
              and s.service_date >= $2::date
              and ($4::uuid is null or s.campus_id = $4::uuid)
          ),
          fund_totals as (
            select
              t.fund_id as "fundId",
              coalesce(f.code, '-') as "fundCode",
              coalesce(f.name, 'Unassigned') as "fundName",
              coalesce(sum(t.amount),0)::numeric(12,2) as "givingAmount"
            from transactions t
            left join payment_intents pi on pi.id = t.payment_intent_id
            left join funds f on f.id = t.fund_id and f.church_id = t.church_id
            where t.church_id = $1
              and ${TX_STATUS_EXPR} = any($3)
              and t.created_at >= $2::timestamp
            group by t.fund_id, f.code, f.name
          ),
          totals as (
            select coalesce(sum("givingAmount"),0)::numeric(12,2) as giving_total
            from fund_totals
          )
          select
            ft."fundId",
            ft."fundCode",
            ft."fundName",
            ft."givingAmount",
            case
              when coalesce(t.giving_total,0) > 0
              then round((ft."givingAmount" / t.giving_total) * 100, 2)
              else 0::numeric(10,2)
            end as "sharePct",
            case
              when a.unique_attendees > 0
              then round(ft."givingAmount" / a.unique_attendees, 2)
              else 0::numeric(12,2)
            end as "givingPerAttendee"
          from fund_totals ft
          cross join attendees a
          cross join totals t
          order by ft."givingAmount" desc, ft."fundName" asc
          limit 24
          `,
          [churchId, periodStartIso, STATEMENT_DEFAULT_STATUSES, campusId]
        ),
        db.manyOrNone(
          `
          with service_rows as (
            select
              s.id as "serviceId",
              s.service_name as "serviceName",
              s.service_date as "serviceDate",
              s.campus_id as "campusId",
              coalesce(cc.name, 'Unspecified') as campus,
              (
                select count(*)::int
                from church_checkins c
                where c.church_id = $1 and c.service_id = s.id
              ) as "checkInsCount",
              (
                select count(distinct c.member_pk)::int
                from church_checkins c
                where c.church_id = $1 and c.service_id = s.id
              ) as "attendanceCount",
              (
                select count(*)::int
                from church_apologies a
                where a.church_id = $1 and a.service_id = s.id
              ) as "apologiesCount"
            from church_services s
            left join church_campuses cc on cc.id = s.campus_id
            where s.church_id = $1
              and s.service_date >= $2::date
              and ($5::uuid is null or s.campus_id = $5::uuid)
          ),
          date_attendance as (
            select
              "serviceDate",
              sum("attendanceCount")::numeric(12,2) as total_attendance
            from service_rows
            group by "serviceDate"
          ),
          giving_by_date as (
            select
              ${txServiceDateExpr} as service_date,
              coalesce(sum(t.amount),0)::numeric(12,2) as giving_amount
            from transactions t
            left join payment_intents pi on pi.id = t.payment_intent_id
            where t.church_id = $1
              and ${TX_STATUS_EXPR} = any($3)
              and ${txServiceDateExpr} >= $2::date
            group by 1
          ),
          allocated as (
            select
              sr.*,
              coalesce(da.total_attendance, 0)::numeric(12,2) as date_attendance_total,
              coalesce(gbd.giving_amount, 0)::numeric(12,2) as date_giving_total,
              case
                when coalesce(da.total_attendance, 0) > 0
                then round(coalesce(gbd.giving_amount, 0) * (sr."attendanceCount"::numeric / da.total_attendance), 2)
                else 0::numeric(12,2)
              end as "allocatedGivingAmount"
            from service_rows sr
            left join date_attendance da on da."serviceDate" = sr."serviceDate"
            left join giving_by_date gbd on gbd.service_date = sr."serviceDate"
          )
          select
            "serviceId",
            "serviceName",
            "serviceDate",
            "campusId",
            campus,
            "checkInsCount",
            "attendanceCount",
            "apologiesCount",
            "allocatedGivingAmount",
            case
              when "attendanceCount" > 0
              then round("allocatedGivingAmount" / "attendanceCount", 2)
              else 0::numeric(12,2)
            end as "givingPerAttendee"
          from allocated
          order by "serviceDate" desc, "serviceName" asc
          limit $4
          `,
          [churchId, periodStartIso, STATEMENT_DEFAULT_STATUSES, serviceLimit, campusId]
        ),
        db.manyOrNone(
          `
          with service_rows as (
            select
              s.id as "serviceId",
              s.service_date as "serviceDate",
              s.campus_id as "campusId",
              coalesce(cc.name, 'Unspecified') as campus,
              (
                select count(distinct c.member_pk)::int
                from church_checkins c
                where c.church_id = $1 and c.service_id = s.id
              ) as "attendanceCount"
            from church_services s
            left join church_campuses cc on cc.id = s.campus_id
            where s.church_id = $1
              and s.service_date >= $2::date
              and ($4::uuid is null or s.campus_id = $4::uuid)
          ),
          date_attendance as (
            select
              "serviceDate",
              sum("attendanceCount")::numeric(12,2) as total_attendance
            from service_rows
            group by "serviceDate"
          ),
          giving_by_date as (
            select
              ${txServiceDateExpr} as service_date,
              coalesce(sum(t.amount),0)::numeric(12,2) as giving_amount
            from transactions t
            left join payment_intents pi on pi.id = t.payment_intent_id
            where t.church_id = $1
              and ${TX_STATUS_EXPR} = any($3)
              and ${txServiceDateExpr} >= $2::date
            group by 1
          ),
          allocated as (
            select
              sr.campus,
              sr."attendanceCount",
              case
                when coalesce(da.total_attendance, 0) > 0
                then coalesce(gbd.giving_amount, 0) * (sr."attendanceCount"::numeric / da.total_attendance)
                else 0::numeric
              end as allocated_giving
            from service_rows sr
            left join date_attendance da on da."serviceDate" = sr."serviceDate"
            left join giving_by_date gbd on gbd.service_date = sr."serviceDate"
          )
          select
            campus,
            count(*)::int as "servicesCount",
            coalesce(sum("attendanceCount"),0)::int as "attendanceCount",
            coalesce(round(sum(allocated_giving),2),0)::numeric(12,2) as "allocatedGivingAmount",
            case
              when coalesce(sum("attendanceCount"),0) > 0
              then round(sum(allocated_giving) / sum("attendanceCount"), 2)
              else 0::numeric(12,2)
            end as "givingPerAttendee"
          from allocated
          group by campus
          order by "attendanceCount" desc, "allocatedGivingAmount" desc, campus asc
          `,
          [churchId, periodStartIso, STATEMENT_DEFAULT_STATUSES, campusId]
        ),
        atRiskRowsPromise,
      ]);

      const uniqueAttendees = Number(overallRow?.uniqueAttendees || 0);
      const uniqueGivers = Number(overallRow?.uniqueGivers || 0);
      const checkInsCount = Number(overallRow?.checkInsCount || 0);
      const givingTotal = Number(overallRow?.givingTotal || 0);
      const donorParticipationRatePct = uniqueAttendees > 0 ? (uniqueGivers / uniqueAttendees) * 100 : 0;
      const givingPerAttendee = uniqueAttendees > 0 ? givingTotal / uniqueAttendees : 0;
      const givingPerCheckIn = checkInsCount > 0 ? givingTotal / checkInsCount : 0;
      const retentionEligibleCount = Number(retentionRow?.eligibleCount || 0);
      const retentionRetainedCount = Number(retentionRow?.retainedCount || 0);
      const retentionRatePct =
        retentionEligibleCount > 0 ? (retentionRetainedCount / retentionEligibleCount) * 100 : 0;

      return res.json({
        ok: true,
        overview: {
          weeks,
          periodStart: periodStartIso,
          checkInsCount,
          uniqueAttendees,
          uniqueGivers,
          firstTimeVisitors: Number(overallRow?.firstTimeVisitors || 0),
          returningVisitors: Number(overallRow?.returningVisitors || 0),
          givingTotal,
          donorParticipationRatePct,
          givingPerAttendee,
          givingPerCheckIn,
          retentionEligibleCount,
          retentionRetainedCount,
          retentionRatePct,
          atRiskMembersCount: atRiskRows.length,
        },
        weeklyTrend: weeklyRows.map((row) => {
          const attendees = Number(row?.uniqueAttendees || 0);
          const donors = Number(row?.donorCount || 0);
          return {
            weekStart: formatDateIsoLike(row?.weekStart),
            checkInsCount: Number(row?.checkInsCount || 0),
            uniqueAttendees: attendees,
            firstTimeVisitors: Number(row?.firstTimeVisitors || 0),
            returningVisitors: Number(row?.returningVisitors || 0),
            givingAmount: Number(row?.givingAmount || 0),
            donorCount: donors,
            donorParticipationRatePct: attendees > 0 ? (donors / attendees) * 100 : 0,
          };
        }),
        byFund: byFundRows.map((row) => ({
          fundId: row?.fundId || null,
          fundCode: row?.fundCode || null,
          fundName: row?.fundName || "Unassigned",
          givingAmount: Number(row?.givingAmount || 0),
          sharePct: Number(row?.sharePct || 0),
          givingPerAttendee: Number(row?.givingPerAttendee || 0),
        })),
        byCampus: byCampusRows.map((row) => ({
          campus: row?.campus || "Unspecified",
          servicesCount: Number(row?.servicesCount || 0),
          attendanceCount: Number(row?.attendanceCount || 0),
          allocatedGivingAmount: Number(row?.allocatedGivingAmount || 0),
          givingPerAttendee: Number(row?.givingPerAttendee || 0),
        })),
        byService: byServiceRows.map((row) => ({
          serviceId: row?.serviceId || null,
          serviceName: row?.serviceName || "Service",
          serviceDate: formatDateIsoLike(row?.serviceDate),
          campusId: row?.campusId || null,
          campus: row?.campus || "Unspecified",
          checkInsCount: Number(row?.checkInsCount || 0),
          attendanceCount: Number(row?.attendanceCount || 0),
          apologiesCount: Number(row?.apologiesCount || 0),
          allocatedGivingAmount: Number(row?.allocatedGivingAmount || 0),
          givingPerAttendee: Number(row?.givingPerAttendee || 0),
        })),
        atRiskMembers: atRiskRows.map((row) => ({
          memberPk: row?.memberPk || null,
          memberId: row?.memberId || null,
          fullName: row?.fullName || null,
          phone: row?.phone || null,
          email: row?.email || null,
          previousAttendance: Number(row?.previousAttendance || 0),
          recentAttendance: Number(row?.recentAttendance || 0),
          previousGiving: Number(row?.previousGiving || 0),
          recentGiving: Number(row?.recentGiving || 0),
        })),
        meta: {
          weeks,
          serviceLimit,
          campusId: campusId || null,
          statuses: STATEMENT_DEFAULT_STATUSES.slice(),
          givingAllocation: "Service and campus giving is allocated by attendance share on each service_date.",
          generatedAt: new Date().toISOString(),
        },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({
          error: "Operations insights are not available yet. Run migrations and retry.",
        });
      }
      console.error("[admin/operations/insights] error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get("/church-life/status", requireAuth, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    const subscription = await loadChurchOperationsSubscription(churchId);
    const computed = computeSubscriptionAccess(subscription);
    const active = computed.hasAccess;
    let member = await loadMember(req.user.id);

    if (active) {
      try {
        await ensureChurchMemberIdentifiers(churchId);
        member = await loadMember(req.user.id);
      } catch (err) {
        if (err?.code === "42P01" || err?.code === "42703") {
          return res.status(503).json({
            error: "Church Life features are not available yet. Run migrations and retry.",
          });
        }
        throw err;
      }
    }

    return res.json({
      ok: true,
      churchId,
      active,
      reason: active ? null : computed.reason || "subscription_inactive",
      accessLevel: computed.accessLevel,
      banner: computed.banner,
      memberId: String(member?.member_id || "").trim() || null,
      subscription: { ...subscription, hasAccess: active, accessLevel: computed.accessLevel, banner: computed.banner, reason: computed.reason },
    });
  } catch (err) {
    console.error("[church-life/status] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/church-life/services", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    let campusId = null;
    try {
      campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
    } catch (campusErr) {
      return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
    }
    const memberPk = String(req.user?.id || "").trim();
    const memberPkForJoin = UUID_REGEX.test(memberPk) ? memberPk : null;
    const limit = parseChurchLifeLimit(req.query?.limit, 20, 120);
    const fromDate = parseChurchLifeDate(req.query?.from) || formatDateIsoLike(new Date(Date.now() - 60 * 24 * 60 * 60 * 1000));

    const rows = await db.manyOrNone(
      `
      select
        s.id,
        s.church_id as "churchId",
        s.campus_id as "campusId",
        cc.name as "campusName",
        cc.code as "campusCode",
        s.service_name as "serviceName",
        s.service_date as "serviceDate",
        s.starts_at as "startsAt",
        s.ends_at as "endsAt",
        s.location,
        s.notes,
        s.published,
        s.created_at as "createdAt",
        s.updated_at as "updatedAt",
        ci.checked_in_at as "checkedInAt",
        ci.method as "checkInMethod",
        ap.id as "apologyId",
        ap.status as "apologyStatus",
        ap.created_at as "apologyCreatedAt"
      from church_services s
      left join church_campuses cc on cc.id = s.campus_id
      left join church_checkins ci on ci.service_id = s.id and ci.member_pk = $2
      left join church_apologies ap on ap.service_id = s.id and ap.member_pk = $2
      where s.church_id = $1
        and s.published = true
        and s.service_date >= $3::date
        and ($5::uuid is null or s.campus_id = $5::uuid)
      order by s.service_date desc, coalesce(s.starts_at, s.created_at) desc
      limit $4
      `,
      [churchId, memberPk, fromDate, limit, campusId]
    );

    return res.json({
      ok: true,
      services: rows.map((row) => ({
        ...normalizeChurchServiceRow(row),
        memberStatus: {
          checkedInAt: row?.checkedInAt || null,
          checkInMethod: row?.checkInMethod ? normalizeUpperToken(row.checkInMethod) : null,
          apologyId: row?.apologyId || null,
          apologyStatus: row?.apologyStatus ? normalizeUpperToken(row.apologyStatus) : null,
          apologyCreatedAt: row?.apologyCreatedAt || null,
        },
      })),
      meta: { limit, fromDate, campusId: campusId || null },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({
        error: "Church Life services are not available yet. Run migrations and retry.",
      });
    }
    console.error("[church-life/services] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/church-life/check-ins", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    let campusId = null;
    try {
      campusId = await resolveChurchCampusId(churchId, req.body?.campusId, { fieldName: "campusId" });
    } catch (campusErr) {
      return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
    }

    const serviceId = String(req.body?.serviceId || "").trim();
    if (!UUID_REGEX.test(serviceId)) {
      return res.status(400).json({ error: "serviceId is required." });
    }

    const method = normalizeUpperToken(req.body?.method || "TAP");
    if (!CHURCH_LIFE_CHECKIN_METHODS.has(method) || method === "USHER") {
      return res.status(400).json({ error: "method must be TAP or QR for member check-in." });
    }

    const service = await findChurchServiceById(churchId, serviceId);
    if (!service) return res.status(404).json({ error: "Service not found." });
    if (!service.published) return res.status(400).json({ error: "This service is not available for check-in." });
    if (campusId && service.campusId && service.campusId !== campusId) {
      return res.status(400).json({ error: "serviceId does not belong to the selected campus." });
    }
    const finalCampusId = campusId || service.campusId || null;

    const member = await loadMember(req.user.id);
    const notes = String(req.body?.notes || "").trim().slice(0, 250) || null;

    const row = await db.oneOrNone(
      `
      with inserted as (
        insert into church_checkins (
          church_id, campus_id, service_id, member_pk, member_id, method, checked_in_at, created_by, notes
        )
        values ($1,$2,$3,$4,$5,$6,now(),$4,$7)
        on conflict (church_id, service_id, member_pk) do nothing
        returning
          id,
          church_id as "churchId",
          campus_id as "campusId",
          service_id as "serviceId",
          member_pk as "memberPk",
          member_id as "memberId",
          method,
          checked_in_at as "checkedInAt",
          notes,
          false as "idempotent"
      )
      select * from inserted
      union all
      select
        c.id,
        c.church_id as "churchId",
        c.campus_id as "campusId",
        c.service_id as "serviceId",
        c.member_pk as "memberPk",
        c.member_id as "memberId",
        c.method,
        c.checked_in_at as "checkedInAt",
        c.notes,
        true as "idempotent"
      from church_checkins c
      where c.church_id=$1 and c.service_id=$3 and c.member_pk=$4
        and not exists (select 1 from inserted)
      limit 1
      `,
      [churchId, finalCampusId, serviceId, req.user.id, member?.member_id || null, method, notes]
    );
    if (!row) {
      return res.status(500).json({ error: "Could not resolve check-in state." });
    }

    return res.status(row.idempotent ? 200 : 201).json({
      ok: true,
      idempotent: row.idempotent === true,
      checkIn: {
        ...normalizeChurchCheckinRow({
          ...row,
          campusName: service?.campusName || null,
          campusCode: service?.campusCode || null,
        }),
        memberName: member?.full_name || null,
        memberRole: member?.role || req.user?.role || null,
      },
      service: {
        id: service.id,
        serviceName: service.serviceName,
        serviceDate: formatDateIsoLike(service.serviceDate),
      },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life check-ins are not available yet. Run migrations and retry." });
    }
    console.error("[church-life/check-ins] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/church-life/apologies", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    const serviceId = String(req.body?.serviceId || "").trim();
    if (!UUID_REGEX.test(serviceId)) {
      return res.status(400).json({ error: "serviceId is required." });
    }

    const reason = String(req.body?.reason || "").trim().slice(0, 140) || null;
    const message = String(req.body?.message || "").trim().slice(0, 1000) || null;
    if (!reason && !message) {
      return res.status(400).json({ error: "Provide a reason or message." });
    }

    const service = await db.oneOrNone(
      `
      select id, service_name as "serviceName", service_date as "serviceDate"
      from church_services
      where id=$1 and church_id=$2
      limit 1
      `,
      [serviceId, churchId]
    );
    if (!service) return res.status(404).json({ error: "Service not found." });

    const member = await loadMember(req.user.id);
    const row = await db.one(
      `
      insert into church_apologies (
        church_id, service_id, member_pk, member_id, reason, message, status
      )
      values ($1,$2,$3,$4,$5,$6,'SUBMITTED')
      on conflict (service_id, member_pk)
      do update set
        reason = excluded.reason,
        message = excluded.message,
        status = 'SUBMITTED',
        updated_at = now()
      returning
        id,
        church_id as "churchId",
        service_id as "serviceId",
        member_pk as "memberPk",
        member_id as "memberId",
        reason,
        message,
        status,
        created_at as "createdAt",
        updated_at as "updatedAt",
        resolved_at as "resolvedAt"
      `,
      [churchId, serviceId, req.user.id, member?.member_id || null, reason, message]
    );

    try {
      await createNotification({
        churchId,
        memberId: req.user.id,
        type: "CHURCH_APOLOGY_SUBMITTED",
        title: "Apology submitted",
        body: `Your apology for ${service.serviceName || "this service"} was submitted.`,
        data: {
          churchId,
          serviceId,
          apologyId: row.id,
          serviceDate: formatDateIsoLike(service.serviceDate),
          reason: reason || null,
        },
      });
    } catch (notifyErr) {
      console.error("[church-life/apologies] notify member failed", notifyErr?.message || notifyErr);
    }

    return res.status(201).json({
      ok: true,
      apology: {
        ...normalizeChurchApologyRow(row),
        memberName: member?.full_name || null,
      },
      service: {
        id: service.id,
        serviceName: service.serviceName,
        serviceDate: formatDateIsoLike(service.serviceDate),
      },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life apologies are not available yet. Run migrations and retry." });
    }
    console.error("[church-life/apologies] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/church-life/events", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const memberPk = String(req.user?.id || "").trim();
    const memberPkForJoin = UUID_REGEX.test(memberPk) ? memberPk : null;
    let campusId = null;
    try {
      campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
    } catch (campusErr) {
      return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
    }
    const limit = parseChurchLifeLimit(req.query?.limit, 20, 100);
    const includePastDays = Number.isFinite(Number(req.query?.includePastDays))
      ? Math.max(0, Math.min(365, Math.trunc(Number(req.query.includePastDays))))
      : 30;

    let rows = [];
    try {
      rows = await db.manyOrNone(
        `
        select
          e.id,
          e.church_id as "churchId",
          e.campus_id as "campusId",
          cc.name as "campusName",
          cc.code as "campusCode",
          e.title,
          e.description,
          e.starts_at as "startsAt",
          e.ends_at as "endsAt",
          e.venue,
          e.poster_url as "posterUrl",
          e.poster_data_url as "posterDataUrl",
          e.capacity,
          e.is_ticketed as "isTicketed",
          e.price,
          coalesce(reg.count, 0)::int as "registrationCount",
          my.id as "myRegistrationId",
          my.status as "myRegistrationStatus",
          my.created_at as "myRegisteredAt",
          e.status,
          e.notify_on_publish as "notifyOnPublish",
          e.published_at as "publishedAt",
          e.created_at as "createdAt",
          e.updated_at as "updatedAt"
        from church_events e
        left join church_campuses cc on cc.id = e.campus_id
        left join lateral (
          select count(*)::int as count
          from church_event_registrations r
          where r.event_id = e.id
            and r.status = any($5::text[])
        ) reg on true
        left join church_event_registrations my
          on my.event_id = e.id
          and my.member_pk = $6::uuid
        where e.church_id = $1
          and e.status = 'PUBLISHED'
          and e.starts_at >= (current_date - ($2::int * interval '1 day'))
          and ($4::uuid is null or e.campus_id = $4::uuid)
        order by e.starts_at asc
        limit $3
        `,
        [churchId, includePastDays, limit, campusId, CHURCH_EVENT_REGISTRATION_ACTIVE_STATUSES, memberPkForJoin]
      );
    } catch (memberJoinErr) {
      console.error("[church-life/events] member-join fallback", memberJoinErr?.message || memberJoinErr);
      rows = await db.manyOrNone(
        `
        select
          e.id,
          e.church_id as "churchId",
          e.campus_id as "campusId",
          cc.name as "campusName",
          cc.code as "campusCode",
          e.title,
          e.description,
          e.starts_at as "startsAt",
          e.ends_at as "endsAt",
          e.venue,
          e.poster_url as "posterUrl",
          e.poster_data_url as "posterDataUrl",
          e.capacity,
          e.is_ticketed as "isTicketed",
          e.price,
          coalesce(reg.count, 0)::int as "registrationCount",
          null::uuid as "myRegistrationId",
          null::text as "myRegistrationStatus",
          null::timestamptz as "myRegisteredAt",
          e.status,
          e.notify_on_publish as "notifyOnPublish",
          e.published_at as "publishedAt",
          e.created_at as "createdAt",
          e.updated_at as "updatedAt"
        from church_events e
        left join church_campuses cc on cc.id = e.campus_id
        left join lateral (
          select count(*)::int as count
          from church_event_registrations r
          where r.event_id = e.id
            and r.status = any($5::text[])
        ) reg on true
        where e.church_id = $1
          and e.status = 'PUBLISHED'
          and e.starts_at >= (current_date - ($2::int * interval '1 day'))
          and ($4::uuid is null or e.campus_id = $4::uuid)
        order by e.starts_at asc
        limit $3
        `,
        [churchId, includePastDays, limit, campusId, CHURCH_EVENT_REGISTRATION_ACTIVE_STATUSES]
      );
    }

    return res.json({
      ok: true,
      events: rows.map(normalizeChurchEventRow),
      meta: { limit, includePastDays, campusId: campusId || null },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life events are not available yet. Run migrations and retry." });
    }
    console.error("[church-life/events] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/church-life/events/:eventId/ticket-checkout", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const eventId = String(req.params?.eventId || "").trim();
    if (!UUID_REGEX.test(eventId)) return res.status(400).json({ error: "Invalid eventId." });
    const memberPk = String(req.user?.id || "").trim();
    if (!UUID_REGEX.test(memberPk)) return res.status(403).json({ error: "Your profile is not linked for ticket purchase." });
    const member = await loadMember(memberPk);

    const event = await findChurchEventById(churchId, eventId);
    if (!event) return res.status(404).json({ error: "Event not found." });
    if (normalizeChurchLifeEventStatus(event.status) !== "PUBLISHED") {
      return res.status(400).json({ error: "This event is not open for tickets." });
    }
    const endsAtMs = Date.parse(String(event?.endsAt || ""));
    if (Number.isFinite(endsAtMs) && endsAtMs < Date.now()) {
      return res.status(400).json({ error: "This event has ended." });
    }
    const isTicketed = event?.isTicketed === true && Number(event?.price || 0) > 0;
    if (!isTicketed) {
      return res.status(400).json({ error: "This event does not require ticket payment. Use Register instead." });
    }

    const existing = await db.oneOrNone(
      `
      select
        id,
        church_id as "churchId",
        event_id as "eventId",
        member_pk as "memberPk",
        member_id as "memberId",
        status,
        checked_in_at as "checkedInAt",
        canceled_at as "canceledAt",
        created_at as "createdAt",
        updated_at as "updatedAt"
      from church_event_registrations
      where church_id = $1 and event_id = $2 and member_pk = $3
      limit 1
      `,
      [churchId, eventId, memberPk]
    );
    const existingStatus = normalizeChurchEventRegistrationStatus(existing?.status || "", "");
    const alreadyActive = CHURCH_EVENT_REGISTRATION_ACTIVE_STATUSES.includes(existingStatus);
    const unitPrice = toCurrencyNumber(Number(event?.price || 0));
    const quantity = Math.max(
      1,
      Math.min(CHURCH_EVENT_TICKET_MAX_QUANTITY, parsePositiveInt(req.body?.quantity, 1) || 1)
    );

    const registrationCount = await countChurchEventActiveRegistrations(churchId, eventId);
    if (!alreadyActive) {
      const capacity = event?.capacity == null ? null : Number(event.capacity);
      if (Number.isFinite(capacity) && capacity > 0 && registrationCount >= capacity) {
        return res.status(409).json({ error: "This event is full." });
      }
    }

    if (alreadyActive) {
      return res.json({
        ok: true,
        alreadyRegistered: true,
        idempotent: true,
        event: normalizeChurchEventRow({
          ...event,
          registrationCount,
          myRegistrationId: existing?.id || null,
          myRegistrationStatus: existing?.status || "REGISTERED",
          myRegisteredAt: existing?.createdAt || null,
        }),
        registration: normalizeChurchEventRegistrationRow({
          ...existing,
          memberName: member?.full_name || null,
          memberPhone: member?.phone || null,
          memberEmail: member?.email || null,
        }),
      });
    }

    const amount = toCurrencyNumber(unitPrice * quantity);
    if (!(Number.isFinite(amount) && amount > 0)) {
      return res.status(400).json({ error: "Ticket amount is invalid." });
    }
    const pricing = buildFeeBreakdown(amount);
    const mPaymentId = makeMpaymentId();
    const itemName = `${String(event?.title || "Event ticket").trim() || "Event ticket"}${quantity > 1 ? ` x${quantity}` : ""}`
      .normalize("NFKD")
      .replace(/[^\x20-\x7E]/g, "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 100);

    const intent = await db.one(
      `
      insert into payment_intents (
        church_id,
        amount,
        currency,
        status,
        member_name,
        member_phone,
        payer_name,
        payer_phone,
        payer_email,
        payer_type,
        channel,
        provider,
        m_payment_id,
        item_name,
        platform_fee_amount,
        platform_fee_pct,
        platform_fee_fixed,
        amount_gross,
        superadmin_cut_amount,
        superadmin_cut_pct,
        source,
        notes,
        service_date
      ) values (
        $1,$2,'ZAR','PENDING',$3,$4,$5,$6,$7,'member','member_app','payfast',$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18
      )
      returning
        id,
        m_payment_id as "mPaymentId",
        amount,
        amount_gross as "amountGross",
        item_name as "itemName",
        source
      `,
      [
        churchId,
        pricing.amount,
        member?.full_name || null,
        member?.phone || null,
        member?.full_name || null,
        member?.phone || null,
        member?.email || null,
        mPaymentId,
        itemName || "Event ticket",
        pricing.platformFeeAmount,
        pricing.platformFeePct,
        pricing.platformFeeFixed,
        pricing.amountGross,
        pricing.superadminCutAmount,
        pricing.superadminCutPct,
        CHURCH_EVENT_TICKET_SOURCE,
        buildChurchEventTicketNote({ eventId, memberPk, quantity, unitPrice }),
        formatDateIsoLike(event?.startsAt) || formatDateIsoLike(new Date()),
      ]
    );

    const callbackUrls = getPayfastCallbackUrls(intent.id, intent.mPaymentId || intent.id);
    let checkoutResult = null;
    try {
      checkoutResult = await createCheckoutWithGateway(churchId, {
        amount: intent.amountGross || pricing.amountGross,
        currency: "ZAR",
        source: CHURCH_EVENT_TICKET_SOURCE,
        metadata: { paymentIntentId: intent.id, eventId, memberPk, quantity, unitPrice },
        providerIntentRef: intent.mPaymentId || intent.id,
        itemName: intent.itemName || itemName || "Event ticket",
        returnUrl: callbackUrls.returnUrl,
        cancelUrl: callbackUrls.cancelUrl,
        notifyUrl: callbackUrls.notifyUrl,
        reference1: churchId,
        reference2: eventId,
        reference3: intent.id,
        reference4: CHURCH_EVENT_TICKET_SOURCE,
        payerName: member?.full_name || undefined,
        payerEmail: member?.email || undefined,
      });
    } catch (err) {
      if (err?.code === "PAYFAST_NOT_CONNECTED") {
        console.error("[church-life/events] ticket-checkout credentials error", err?.message || err, err?.stack);
        return res.status(503).json({
          error: "Ticket payments are not activated for this church. Ask your church admin to connect PayFast.",
          code: "PAYFAST_NOT_CONNECTED",
        });
      }
      throw err;
    }

    const checkoutUrl = checkoutResult.checkoutUrl;

    return res.status(201).json({
      ok: true,
      checkoutUrl,
      paymentUrl: checkoutUrl,
      paymentIntentId: intent.id,
      mPaymentId: intent.mPaymentId || null,
      source: CHURCH_EVENT_TICKET_SOURCE,
      event: normalizeChurchEventRow({
        ...event,
        registrationCount,
      }),
      pricing: {
        unitPrice,
        quantity,
        ticketAmount: pricing.amount,
        churpayFee: pricing.platformFeeAmount,
        totalCharged: pricing.amountGross,
      },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life event ticket checkout is not available yet. Run migrations and retry." });
    }
    if (err?.code === "23514" && String(err?.constraint || "") === "payment_intents_source_check") {
      return res.status(503).json({
        error: "Church Life event ticket checkout is not available yet. Run migrations and retry.",
        code: "TICKET_CHECKOUT_REQUIRES_MIGRATION",
      });
    }
    console.error("[church-life/events] ticket-checkout error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/church-life/events/:eventId/register", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const eventId = String(req.params?.eventId || "").trim();
    if (!UUID_REGEX.test(eventId)) return res.status(400).json({ error: "Invalid eventId." });
    const memberPk = String(req.user?.id || "").trim();
    if (!UUID_REGEX.test(memberPk)) return res.status(403).json({ error: "Your profile is not linked for event registration." });
    const ticketPaymentIntentId = String(req.body?.ticketPaymentIntentId || "").trim();
    const member = await loadMember(memberPk);

    const outcome = await db.tx(async (t) => {
      const event = await findChurchEventById(churchId, eventId, { tx: t });
      if (!event) throw Object.assign(new Error("Event not found."), { statusCode: 404 });
      if (normalizeChurchLifeEventStatus(event.status) !== "PUBLISHED") {
        throw Object.assign(new Error("This event is not open for registration."), { statusCode: 400 });
      }
      const endsAtMs = Date.parse(String(event?.endsAt || ""));
      if (Number.isFinite(endsAtMs) && endsAtMs < Date.now()) {
        throw Object.assign(new Error("This event has ended."), { statusCode: 400 });
      }

      const existing = await t.oneOrNone(
        `
        select id, status
        from church_event_registrations
        where church_id = $1 and event_id = $2 and member_pk = $3
        limit 1
        for update
        `,
        [churchId, eventId, memberPk]
      );
      const existingStatus = normalizeChurchEventRegistrationStatus(existing?.status || "", "");
      const alreadyActive = CHURCH_EVENT_REGISTRATION_ACTIVE_STATUSES.includes(existingStatus);
      const isTicketedEvent = event?.isTicketed === true && Number(event?.price || 0) > 0;

      if (!alreadyActive && isTicketedEvent) {
        if (!UUID_REGEX.test(ticketPaymentIntentId)) {
          throw Object.assign(
            new Error("This event requires a paid ticket. Tap Buy ticket first, then retry registration."),
            { statusCode: 402 }
          );
        }
        const ticketIntent = await loadPaidChurchEventTicketIntent(churchId, ticketPaymentIntentId, { tx: t });
        if (!ticketIntent) {
          throw Object.assign(new Error("Ticket payment intent was not found for this church."), { statusCode: 404 });
        }
        if (String(ticketIntent?.status || "").trim().toUpperCase() !== "PAID") {
          throw Object.assign(new Error("Ticket payment is not confirmed yet. Complete payment and retry."), { statusCode: 402 });
        }
        if (String(ticketIntent?.source || "").trim().toUpperCase() !== CHURCH_EVENT_TICKET_SOURCE) {
          throw Object.assign(new Error("Ticket payment intent does not belong to event checkout."), { statusCode: 400 });
        }
        const ticketMeta = parseChurchEventTicketNote(ticketIntent?.notes);
        if (!ticketMeta || ticketMeta.eventId !== eventId || ticketMeta.memberPk !== memberPk) {
          throw Object.assign(new Error("Ticket payment does not match this member or event."), { statusCode: 403 });
        }
      }

      if (!alreadyActive) {
        const activeCount = await countChurchEventActiveRegistrations(churchId, eventId, { tx: t });
        const capacity = event?.capacity == null ? null : Number(event.capacity);
        if (Number.isFinite(capacity) && capacity > 0 && activeCount >= capacity) {
          throw Object.assign(new Error("This event is full."), { statusCode: 409 });
        }
      }

      const saved = await t.one(
        `
        insert into church_event_registrations (
          church_id, event_id, member_pk, member_id, status, checked_in_at, canceled_at, created_by, updated_by
        )
        values ($1,$2,$3,$4,'REGISTERED',null,null,$5,$5)
        on conflict (event_id, member_pk)
        do update set
          member_id = excluded.member_id,
          status = 'REGISTERED',
          canceled_at = null,
          updated_by = excluded.updated_by,
          updated_at = now()
        returning
          id,
          church_id as "churchId",
          event_id as "eventId",
          member_pk as "memberPk",
          member_id as "memberId",
          status,
          checked_in_at as "checkedInAt",
          canceled_at as "canceledAt",
          created_at as "createdAt",
          updated_at as "updatedAt"
        `,
        [churchId, eventId, memberPk, member?.member_id || null, req.user?.id || null]
      );
      const registrationCount = await countChurchEventActiveRegistrations(churchId, eventId, { tx: t });
      return { event, saved, registrationCount, idempotent: alreadyActive };
    });

    return res.status(outcome.idempotent ? 200 : 201).json({
      ok: true,
      idempotent: outcome.idempotent,
      event: normalizeChurchEventRow({
        ...outcome.event,
        registrationCount: outcome.registrationCount,
        myRegistrationId: outcome.saved.id,
        myRegistrationStatus: outcome.saved.status,
        myRegisteredAt: outcome.saved.createdAt,
      }),
      registration: normalizeChurchEventRegistrationRow({
        ...outcome.saved,
        memberName: member?.full_name || null,
        memberPhone: member?.phone || null,
        memberEmail: member?.email || null,
      }),
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life event registration is not available yet. Run migrations and retry." });
    }
    const statusCode = Number(err?.statusCode || err?.status || 0);
    if (statusCode >= 400 && statusCode < 500) {
      return res.status(statusCode).json({ error: err?.message || "Could not register for event." });
    }
    console.error("[church-life/events] register error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/church-life/events/:eventId/cancel", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const eventId = String(req.params?.eventId || "").trim();
    if (!UUID_REGEX.test(eventId)) return res.status(400).json({ error: "Invalid eventId." });
    const memberPk = String(req.user?.id || "").trim();
    if (!UUID_REGEX.test(memberPk)) return res.status(403).json({ error: "Your profile is not linked for event registration." });
    const member = await loadMember(memberPk);

    const outcome = await db.tx(async (t) => {
      const event = await findChurchEventById(churchId, eventId, { tx: t });
      if (!event) throw Object.assign(new Error("Event not found."), { statusCode: 404 });

      const existing = await t.oneOrNone(
        `
        select
          id,
          church_id as "churchId",
          event_id as "eventId",
          member_pk as "memberPk",
          member_id as "memberId",
          status,
          checked_in_at as "checkedInAt",
          canceled_at as "canceledAt",
          created_at as "createdAt",
          updated_at as "updatedAt"
        from church_event_registrations
        where church_id = $1 and event_id = $2 and member_pk = $3
        limit 1
        for update
        `,
        [churchId, eventId, memberPk]
      );
      if (!existing) throw Object.assign(new Error("You are not registered for this event."), { statusCode: 404 });

      const currentStatus = normalizeChurchEventRegistrationStatus(existing?.status || "", "");
      const alreadyCanceled = currentStatus === "CANCELLED";
      let saved = existing;
      if (!alreadyCanceled) {
        saved = await t.one(
          `
          update church_event_registrations
          set
            status = 'CANCELLED',
            canceled_at = coalesce(canceled_at, now()),
            updated_by = $4,
            updated_at = now()
          where church_id = $1 and event_id = $2 and member_pk = $3
          returning
            id,
            church_id as "churchId",
            event_id as "eventId",
            member_pk as "memberPk",
            member_id as "memberId",
            status,
            checked_in_at as "checkedInAt",
            canceled_at as "canceledAt",
            created_at as "createdAt",
            updated_at as "updatedAt"
          `,
          [churchId, eventId, memberPk, req.user?.id || null]
        );
      }
      const registrationCount = await countChurchEventActiveRegistrations(churchId, eventId, { tx: t });
      return { event, saved, registrationCount, idempotent: alreadyCanceled };
    });

    return res.json({
      ok: true,
      idempotent: outcome.idempotent,
      event: normalizeChurchEventRow({
        ...outcome.event,
        registrationCount: outcome.registrationCount,
        myRegistrationId: outcome.saved.id,
        myRegistrationStatus: "CANCELLED",
        myRegisteredAt: outcome.saved.createdAt,
      }),
      registration: normalizeChurchEventRegistrationRow({
        ...outcome.saved,
        memberName: member?.full_name || null,
        memberPhone: member?.phone || null,
        memberEmail: member?.email || null,
      }),
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life event registration is not available yet. Run migrations and retry." });
    }
    const statusCode = Number(err?.statusCode || err?.status || 0);
    if (statusCode >= 400 && statusCode < 500) {
      return res.status(statusCode).json({ error: err?.message || "Could not cancel event registration." });
    }
    console.error("[church-life/events] cancel registration error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/church-life/groups", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const memberPk = String(req.user?.id || "").trim();
    if (!UUID_REGEX.test(memberPk)) {
      return res.status(403).json({ error: "Your profile is not linked for group access." });
    }
    const includeInactive = toBoolean(req.query?.includeInactive) === true;
    const limit = parseChurchLifeLimit(req.query?.limit, 40, 120);

    const rows = await db.manyOrNone(
      `
      select
        g.id,
        g.church_id as "churchId",
        g.name,
        g.code,
        g.group_type as "groupType",
        g.description,
        g.leader_member_pk as "leaderMemberPk",
        leader.member_id as "leaderMemberId",
        leader.full_name as "leaderName",
        g.active,
        g.created_at as "createdAt",
        g.updated_at as "updatedAt",
        gm.member_role as "membershipRole",
        gm.joined_on as "membershipJoinedOn",
        gm.active as "membershipActive",
        coalesce(group_members.count, 0)::int as "memberCount",
        next_meeting.id as "nextMeetingId",
        next_meeting.meeting_date as "nextMeetingDate",
        next_meeting.starts_at as "nextMeetingStartsAt",
        next_meeting.status as "nextMeetingStatus",
        last_meeting.id as "lastMeetingId",
        last_meeting.meeting_date as "lastMeetingDate",
        last_meeting.status as "lastMeetingStatus",
        coalesce(my_att.marked_count, 0)::int as "myAttendanceMarkedCount",
        coalesce(my_att.present_like_count, 0)::int as "myAttendancePresentCount",
        my_att.last_meeting_date as "myAttendanceLastMeetingDate",
        my_last.last_status as "myAttendanceLastStatus"
      from church_group_members gm
      join church_groups g
        on g.id = gm.group_id
       and g.church_id = gm.church_id
      left join members leader on leader.id = g.leader_member_pk
      left join lateral (
        select count(*)::int as count
        from church_group_members members_count
        where members_count.group_id = g.id and members_count.active = true
      ) group_members on true
      left join lateral (
        select id, meeting_date, starts_at, status
        from church_group_meetings cgms
        where cgms.group_id = g.id
          and cgms.status = 'SCHEDULED'
          and cgms.meeting_date >= current_date
        order by cgms.meeting_date asc, coalesce(cgms.starts_at, cgms.created_at) asc
        limit 1
      ) next_meeting on true
      left join lateral (
        select id, meeting_date, status
        from church_group_meetings cgml
        where cgml.group_id = g.id
        order by cgml.meeting_date desc, coalesce(cgml.starts_at, cgml.created_at) desc
        limit 1
      ) last_meeting on true
      left join lateral (
        select
          count(*)::int as marked_count,
          count(*) filter (where cga.status in ('PRESENT', 'LATE'))::int as present_like_count,
          max(cgm.meeting_date) as last_meeting_date
        from church_group_attendance cga
        join church_group_meetings cgm on cgm.id = cga.meeting_id
        where cga.group_id = g.id and cga.member_pk = $2
      ) my_att on true
      left join lateral (
        select cga_last.status as last_status
        from church_group_attendance cga_last
        join church_group_meetings cgm_last on cgm_last.id = cga_last.meeting_id
        where cga_last.group_id = g.id and cga_last.member_pk = $2
        order by cgm_last.meeting_date desc, cga_last.updated_at desc
        limit 1
      ) my_last on true
      where gm.church_id = $1
        and gm.member_pk = $2
        and ($3::boolean = true or (gm.active = true and g.active = true))
      order by g.active desc, g.name asc
      limit $4
      `,
      [churchId, memberPk, includeInactive, limit]
    );

    return res.json({
      ok: true,
      groups: rows.map((row) => {
        const marked = Number(row?.myAttendanceMarkedCount || 0);
        const present = Number(row?.myAttendancePresentCount || 0);
        const attendanceRate = marked > 0 ? Number(((present / marked) * 100).toFixed(2)) : null;
        return {
          ...normalizeChurchGroupRow(row),
          membership: {
            role: normalizeChurchGroupMemberRole(row?.membershipRole),
            joinedOn: formatDateIsoLike(row?.membershipJoinedOn),
            active: row?.membershipActive !== false,
          },
          nextMeeting: row?.nextMeetingId
            ? {
                id: row.nextMeetingId,
                meetingDate: formatDateIsoLike(row?.nextMeetingDate),
                startsAt: row?.nextMeetingStartsAt || null,
                status: normalizeChurchGroupMeetingStatus(row?.nextMeetingStatus),
              }
            : null,
          lastMeeting: row?.lastMeetingId
            ? {
                id: row.lastMeetingId,
                meetingDate: formatDateIsoLike(row?.lastMeetingDate),
                status: normalizeChurchGroupMeetingStatus(row?.lastMeetingStatus, "COMPLETED"),
              }
            : null,
          myAttendance: {
            markedCount: marked,
            presentCount: present,
            attendanceRate,
            lastMeetingDate: formatDateIsoLike(row?.myAttendanceLastMeetingDate),
            lastStatus: row?.myAttendanceLastStatus ? normalizeChurchGroupAttendanceStatus(row.myAttendanceLastStatus) : null,
          },
        };
      }),
      meta: { limit, includeInactive, returned: rows.length },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life groups are not available yet. Run migrations and retry." });
    }
    console.error("[church-life/groups] list error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/church-life/groups/:groupId/meetings", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const groupId = String(req.params?.groupId || "").trim();
    if (!UUID_REGEX.test(groupId)) return res.status(400).json({ error: "Invalid groupId." });
    const memberPk = String(req.user?.id || "").trim();
    if (!UUID_REGEX.test(memberPk)) {
      return res.status(403).json({ error: "Your profile is not linked for group access." });
    }

    const group = await findChurchGroupById(churchId, groupId);
    if (!group) return res.status(404).json({ error: "Group not found." });
    const membership = await findGroupMembership(churchId, groupId, memberPk, { requireActive: true });
    if (!membership) return res.status(403).json({ error: "You are not assigned to this group." });

    const statusRaw = String(req.query?.status || "").trim();
    if (statusRaw && !CHURCH_GROUP_MEETING_STATUSES.has(normalizeUpperToken(statusRaw))) {
      return res.status(400).json({ error: "status must be SCHEDULED, COMPLETED, or CANCELLED when provided." });
    }
    const status = statusRaw ? normalizeChurchGroupMeetingStatus(statusRaw) : null;
    const includePastDays = Number.isFinite(Number(req.query?.includePastDays))
      ? Math.max(0, Math.min(365, Math.trunc(Number(req.query.includePastDays))))
      : 120;
    const limit = parseChurchLifeLimit(req.query?.limit, 40, 120);

    const rows = await db.manyOrNone(
      `
      select
        gm.id,
        gm.church_id as "churchId",
        gm.group_id as "groupId",
        gm.meeting_date as "meetingDate",
        gm.starts_at as "startsAt",
        gm.ends_at as "endsAt",
        gm.location,
        gm.notes,
        gm.status,
        gm.created_at as "createdAt",
        gm.updated_at as "updatedAt",
        coalesce(agg.total, 0)::int as "attendanceCount",
        coalesce(agg.present_count, 0)::int as "presentCount",
        coalesce(agg.late_count, 0)::int as "lateCount",
        coalesce(agg.absent_count, 0)::int as "absentCount",
        coalesce(agg.excused_count, 0)::int as "excusedCount",
        mine.status as "myAttendanceStatus",
        mine.updated_at as "myAttendanceMarkedAt"
      from church_group_meetings gm
      left join lateral (
        select
          count(*)::int as total,
          count(*) filter (where status='PRESENT')::int as present_count,
          count(*) filter (where status='LATE')::int as late_count,
          count(*) filter (where status='ABSENT')::int as absent_count,
          count(*) filter (where status='EXCUSED')::int as excused_count
        from church_group_attendance cga
        where cga.meeting_id = gm.id
      ) agg on true
      left join church_group_attendance mine
        on mine.meeting_id = gm.id
       and mine.member_pk = $3
      where gm.church_id = $1
        and gm.group_id = $2
        and gm.meeting_date >= current_date - ($4::int * interval '1 day')
        and ($5::text is null or gm.status = $5::text)
      order by gm.meeting_date desc, coalesce(gm.starts_at, gm.created_at) desc
      limit $6
      `,
      [churchId, groupId, memberPk, includePastDays, status, limit]
    );

    return res.json({
      ok: true,
      group: {
        ...normalizeChurchGroupRow(group),
        membership: {
          role: normalizeChurchGroupMemberRole(membership?.role),
          joinedOn: formatDateIsoLike(membership?.joinedOn),
          active: membership?.active !== false,
        },
      },
      meetings: rows.map((row) => ({
        ...normalizeChurchGroupMeetingRow(row),
        myAttendanceStatus: row?.myAttendanceStatus ? normalizeChurchGroupAttendanceStatus(row.myAttendanceStatus) : null,
        myAttendanceMarkedAt: row?.myAttendanceMarkedAt || null,
      })),
      meta: { limit, includePastDays, status, returned: rows.length },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life group meetings are not available yet. Run migrations and retry." });
    }
    console.error("[church-life/groups] meetings list error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/church-life/volunteer-terms", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const memberPk = String(req.user?.id || "").trim();
    if (!UUID_REGEX.test(memberPk)) {
      return res.status(403).json({ error: "Your profile is not linked for volunteer terms." });
    }

    const limit = parseChurchLifeLimit(req.query?.limit, 80, 220);
    const includeHistory = toBoolean(req.query?.includeHistory) === true;
    const status = normalizeVolunteerTermStatus(req.query?.status, "");
    const dueWithinDaysRaw = req.query?.dueWithinDays;
    const dueWithinDays =
      typeof dueWithinDaysRaw === "undefined" || dueWithinDaysRaw === null || dueWithinDaysRaw === ""
        ? null
        : parsePositiveInt(dueWithinDaysRaw, null);
    if (dueWithinDaysRaw !== undefined && dueWithinDays === null) {
      return res.status(400).json({ error: "dueWithinDays must be a positive integer." });
    }
    let campusId = null;
    try {
      campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
    } catch (campusErr) {
      return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
    }

    const where = ["t.church_id = $1", "t.member_pk = $2::uuid"];
    const params = [churchId, memberPk];
    let idx = 3;

    if (!includeHistory) {
      where.push(`t.status in ('ACTIVE','REVIEW_DUE','RENEWED')`);
    }
    if (status && VOLUNTEER_TERM_STATUSES.has(status)) {
      where.push(`t.status = $${idx}`);
      params.push(status);
      idx += 1;
    }
    if (campusId) {
      where.push(`ministry.campus_id = $${idx}::uuid`);
      params.push(campusId);
      idx += 1;
    }
    if (dueWithinDays !== null) {
      where.push(`t.end_date is not null and t.end_date <= current_date + $${idx}::int`);
      params.push(dueWithinDays);
      idx += 1;
    }

    params.push(limit);
    const rows = await db.manyOrNone(
      `
      select
        t.id,
        t.church_id as "churchId",
        t.member_pk as "memberPk",
        t.member_id as "memberId",
        m.full_name as "memberName",
        lower(coalesce(m.role, 'member')) as "memberRole",
        t.ministry_id as "ministryId",
        ministry.name as "ministryName",
        t.ministry_role_id as "ministryRoleId",
        role.role_name as "ministryRoleName",
        t.start_date as "startDate",
        t.end_date as "endDate",
        t.term_length_months as "termLengthMonths",
        t.status,
        t.end_reason as "endReason",
        t.attendance_rate as "attendanceRate",
        t.reliability_rate as "reliabilityRate",
        t.training_completed as "trainingCompleted",
        t.system_rating as "systemRating",
        t.manual_review_score as "manualReviewScore",
        t.performance_notes as "performanceNotes",
        t.reviewed_by as "reviewedBy",
        reviewer.full_name as "reviewedByName",
        t.reviewed_at as "reviewedAt",
        t.created_at as "createdAt",
        t.updated_at as "updatedAt",
        coalesce(active_agg.count, 0)::int as "activeRolesCount"
      from volunteer_role_terms t
      join members m on m.id = t.member_pk and m.church_id = t.church_id
      join church_ministries ministry on ministry.id = t.ministry_id and ministry.church_id = t.church_id
      join church_ministry_roles role on role.id = t.ministry_role_id and role.church_id = t.church_id
      left join members reviewer on reviewer.id = t.reviewed_by and reviewer.church_id = t.church_id
      left join lateral (
        select count(*)::int as count
        from volunteer_role_terms active_terms
        where active_terms.church_id = t.church_id
          and active_terms.member_pk = t.member_pk
          and active_terms.status in ('ACTIVE', 'REVIEW_DUE')
      ) active_agg on true
      where ${where.join(" and ")}
      order by
        case t.status
          when 'REVIEW_DUE' then 0
          when 'ACTIVE' then 1
          when 'RENEWED' then 2
          else 3
        end asc,
        coalesce(t.end_date, (current_date + 3650)) asc,
        t.created_at desc
      limit $${idx}
      `,
      params
    );

    const member = await loadMember(memberPk);
    const roleLabels = Array.from(
      new Set(
        rows
          .map((row) => String(row?.ministryRoleName || "").trim())
          .filter(Boolean)
      )
    );

    return res.json({
      ok: true,
      member: {
        memberPk,
        memberId: member?.member_id || null,
        memberName: member?.full_name || req.user?.fullName || null,
        roleLabels,
      },
      volunteerTerms: rows.map(normalizeVolunteerTermRow),
      meta: {
        limit,
        status: status || null,
        includeHistory,
        dueWithinDays,
        campusId: campusId || null,
        returned: rows.length,
      },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
    }
    console.error("[church-life/volunteer-terms] list error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/church-life/volunteer-schedules", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const memberPk = String(req.user?.id || "").trim();
    if (!UUID_REGEX.test(memberPk)) {
      return res.status(403).json({ error: "Your profile is not linked for volunteer schedules." });
    }

    const limit = parseChurchLifeLimit(req.query?.limit, 80, 240);
    const scheduleStatus = normalizeMinistryScheduleStatus(req.query?.status, "");
    const assignmentStatus = normalizeMinistryScheduleAssignmentStatus(req.query?.assignmentStatus, "");
    const fromDate = req.query?.fromDate ? parseChurchLifeDate(req.query?.fromDate) : null;
    const toDate = req.query?.toDate ? parseChurchLifeDate(req.query?.toDate) : null;
    if (req.query?.fromDate && !fromDate) return res.status(400).json({ error: "fromDate must be YYYY-MM-DD." });
    if (req.query?.toDate && !toDate) return res.status(400).json({ error: "toDate must be YYYY-MM-DD." });
    const includePastDays = Number.isFinite(Number(req.query?.includePastDays))
      ? Math.max(0, Math.min(365, Math.trunc(Number(req.query.includePastDays))))
      : 14;
    let campusId = null;
    try {
      campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
    } catch (campusErr) {
      return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
    }

    const where = ["a.church_id = $1", "a.member_pk = $2::uuid"];
    const params = [churchId, memberPk];
    let idx = 3;

    if (scheduleStatus && MINISTRY_SCHEDULE_STATUSES.has(scheduleStatus)) {
      where.push(`s.status = $${idx}`);
      params.push(scheduleStatus);
      idx += 1;
    }
    if (assignmentStatus && MINISTRY_SCHEDULE_ASSIGNMENT_STATUSES.has(assignmentStatus)) {
      where.push(`a.status = $${idx}`);
      params.push(assignmentStatus);
      idx += 1;
    }
    if (campusId) {
      where.push(`s.campus_id = $${idx}::uuid`);
      params.push(campusId);
      idx += 1;
    }
    if (fromDate) {
      where.push(`s.schedule_date >= $${idx}::date`);
      params.push(fromDate);
      idx += 1;
    } else {
      where.push(`s.schedule_date >= current_date - $${idx}::int`);
      params.push(includePastDays);
      idx += 1;
    }
    if (toDate) {
      where.push(`s.schedule_date <= $${idx}::date`);
      params.push(toDate);
      idx += 1;
    }

    params.push(limit);
    const rows = await db.manyOrNone(
      `
      select
        s.id,
        s.church_id as "churchId",
        s.campus_id as "campusId",
        cc.name as "campusName",
        cc.code as "campusCode",
        s.ministry_id as "ministryId",
        ministry.name as "ministryName",
        s.service_id as "serviceId",
        srv.service_name as "serviceName",
        s.schedule_date as "scheduleDate",
        s.title,
        s.notes,
        s.status,
        s.created_at as "createdAt",
        s.updated_at as "updatedAt",
        coalesce(assignment_agg.total, 0)::int as "assignmentCount",
        coalesce(assignment_agg.confirmed, 0)::int as "confirmedCount",
        coalesce(assignment_agg.served, 0)::int as "servedCount",
        coalesce(assignment_agg.no_show, 0)::int as "noShowCount",
        a.id as "assignmentId",
        a.member_pk as "memberPk",
        a.member_id as "memberId",
        m.full_name as "memberName",
        lower(coalesce(m.role, 'member')) as "memberRole",
        a.ministry_role_id as "ministryRoleId",
        role.role_name as "ministryRoleName",
        a.status as "assignmentStatus",
        a.assigned_by as "assignedBy",
        a.responded_at as "respondedAt",
        a.served_at as "servedAt",
        a.notes as "assignmentNotes",
        a.created_at as "assignmentCreatedAt",
        a.updated_at as "assignmentUpdatedAt"
      from ministry_schedule_assignments a
      join ministry_schedules s on s.id = a.schedule_id and s.church_id = a.church_id
      join church_ministries ministry on ministry.id = s.ministry_id and ministry.church_id = s.church_id
      join members m on m.id = a.member_pk and m.church_id = a.church_id
      left join church_ministry_roles role on role.id = a.ministry_role_id and role.church_id = a.church_id
      left join church_services srv on srv.id = s.service_id and srv.church_id = s.church_id
      left join church_campuses cc on cc.id = s.campus_id
      left join lateral (
        select
          count(*)::int as total,
          count(*) filter (where status = 'CONFIRMED')::int as confirmed,
          count(*) filter (where status = 'SERVED')::int as served,
          count(*) filter (where status = 'NO_SHOW')::int as no_show
        from ministry_schedule_assignments agg
        where agg.schedule_id = s.id
      ) assignment_agg on true
      where ${where.join(" and ")}
      order by s.schedule_date asc, s.created_at asc
      limit $${idx}
      `,
      params
    );

    const todayIso = new Date().toISOString().slice(0, 10);
    const schedules = rows.map((row) => ({
      ...normalizeMinistryScheduleRow(row),
      assignment: normalizeMinistryScheduleAssignmentRow({
        id: row?.assignmentId || null,
        churchId: row?.churchId || null,
        scheduleId: row?.id || null,
        memberPk: row?.memberPk || null,
        memberId: row?.memberId || null,
        memberName: row?.memberName || null,
        memberRole: row?.memberRole || null,
        ministryRoleId: row?.ministryRoleId || null,
        ministryRoleName: row?.ministryRoleName || null,
        status: row?.assignmentStatus || null,
        assignedBy: row?.assignedBy || null,
        respondedAt: row?.respondedAt || null,
        servedAt: row?.servedAt || null,
        notes: row?.assignmentNotes || null,
        createdAt: row?.assignmentCreatedAt || null,
        updatedAt: row?.assignmentUpdatedAt || null,
      }),
    }));

    const todaySchedules = schedules.filter((row) => String(row?.scheduleDate || "") === todayIso);
    const upcomingSchedules = schedules.filter((row) => String(row?.scheduleDate || "") >= todayIso);

    return res.json({
      ok: true,
      schedules,
      summary: {
        todayDate: todayIso,
        totalAssigned: schedules.length,
        todayCount: todaySchedules.length,
        upcomingCount: upcomingSchedules.length,
      },
      meta: {
        limit,
        status: scheduleStatus || null,
        assignmentStatus: assignmentStatus || null,
        fromDate: fromDate || null,
        toDate: toDate || null,
        includePastDays,
        campusId: campusId || null,
        returned: schedules.length,
      },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
    }
    console.error("[church-life/volunteer-schedules] list error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/church-life/prayer-requests", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    let campusId = null;
    try {
      campusId = await resolveChurchCampusId(churchId, req.body?.campusId, { fieldName: "campusId" });
    } catch (campusErr) {
      return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
    }

    const category = normalizeChurchLifePrayerCategory(req.body?.category);
    const visibility = normalizeChurchLifePrayerVisibility(req.body?.visibility, category);
    const subject = String(req.body?.subject || "").trim().slice(0, 140) || null;
    const message = String(req.body?.message || "").trim().slice(0, 3000);
    if (!message) return res.status(400).json({ error: "message is required." });

    const member = await loadMember(req.user.id);
    const recommendedTeam = recommendedPrayerTeamForCategory(category);

    const row = await db.one(
      `
      insert into church_prayer_requests (
        church_id, campus_id, member_pk, member_id, category, visibility, subject, message, status
      )
      values ($1,$2,$3,$4,$5,$6,$7,$8,'NEW')
      returning
        id,
        church_id as "churchId",
        campus_id as "campusId",
        member_pk as "memberPk",
        member_id as "memberId",
        category,
        visibility,
        subject,
        message,
        status,
        assigned_team as "assignedTeam",
        assigned_to_user_id as "assignedToUserId",
        assigned_at as "assignedAt",
        created_at as "createdAt",
        updated_at as "updatedAt",
        closed_at as "closedAt"
      `,
      [churchId, campusId, req.user.id, member?.member_id || null, category, visibility, subject, message]
    );
    let campus = null;
    if (row?.campusId) {
      campus = await findChurchCampusById(churchId, row.campusId);
    }

    const prayerRequest = {
      ...normalizePrayerRequestRow({
        ...row,
        campusName: campus?.name || null,
        campusCode: campus?.code || null,
      }),
      memberName: member?.full_name || null,
    };
    const notifiedCount = 0;

    try {
      await createNotification({
        churchId,
        memberId: req.user.id,
        type: "PRAYER_REQUEST_RECEIVED",
        title: "Prayer request received",
        body: "Your prayer request was received and queued for pastoral care.",
        data: {
          churchId,
          prayerRequestId: prayerRequest.id,
          category: prayerRequest.category,
          visibility: prayerRequest.visibility,
          recommendedTeam,
        },
      });
    } catch (notifyErr) {
      console.error("[church-life/prayer] notify member failed", notifyErr?.message || notifyErr);
    }

    return res.status(201).json({
      ok: true,
      prayerRequest,
      routing: {
        recommendedTeam,
        notifiedCount,
      },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life prayer inbox is not available yet. Run migrations and retry." });
    }
    console.error("[church-life/prayer] create error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/church-life/prayer-requests", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    let campusId = null;
    try {
      campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
    } catch (campusErr) {
      return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
    }

    const limit = parseChurchLifeLimit(req.query?.limit, 20, 100);
    const rows = await db.manyOrNone(
      `
      select
        pr.id,
        pr.church_id as "churchId",
        pr.campus_id as "campusId",
        cc.name as "campusName",
        cc.code as "campusCode",
        pr.member_pk as "memberPk",
        pr.member_id as "memberId",
        m.full_name as "memberName",
        pr.category,
        pr.visibility,
        pr.subject,
        pr.message,
        pr.status,
        coalesce(pr.assigned_team, case when pr.assigned_role = 'PASTOR' then 'PASTORAL' else pr.assigned_role end) as "assignedTeam",
        coalesce(pr.assigned_to_user_id, pr.assigned_member_id) as "assignedToUserId",
        am.full_name as "assignedToUserName",
        pr.created_at as "createdAt",
        pr.updated_at as "updatedAt",
        pr.closed_at as "closedAt"
      from church_prayer_requests pr
      join members m on m.id = pr.member_pk
      left join members am on am.id = coalesce(pr.assigned_to_user_id, pr.assigned_member_id)
      left join church_campuses cc on cc.id = pr.campus_id
      where pr.church_id=$1 and pr.member_pk=$2
        and ($3::uuid is null or pr.campus_id = $3)
      order by pr.created_at desc
      limit $4
      `,
      [churchId, req.user.id, campusId, limit]
    );

    return res.json({
      ok: true,
      prayerRequests: rows.map(normalizePrayerRequestRow),
      meta: { limit, campusId: campusId || null, returned: rows.length },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life prayer inbox is not available yet. Run migrations and retry." });
    }
    console.error("[church-life/prayer] list error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/church-life/prayer/inbox", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    const policy = await resolvePrayerAccessPolicy(req, churchId);
    if (policy.blocked) {
      return res.status(403).json({ error: "Prayer inbox is not available for finance roles." });
    }

    const status = normalizeUpperToken(req.query?.status);
    const category = normalizeUpperToken(req.query?.category);
    const team = normalizeChurchLifePrayerTeam(req.query?.team || req.query?.assignedTeam);
    const assignedTo = String(req.query?.assignedTo || req.query?.assignedToUserId || "").trim();
    const limit = parseChurchLifeLimit(req.query?.limit, 80, 300);
    let campusId = null;
    try {
      campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
    } catch (campusErr) {
      return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
    }

    const where = ["pr.church_id = $1"];
    const params = [churchId];
    let idx = 2;

    if (status && ["NEW", "ASSIGNED", "IN_PROGRESS", "CLOSED"].includes(status)) {
      where.push(`pr.status = $${idx}`);
      params.push(status);
      idx += 1;
    }
    if (CHURCH_LIFE_PRAYER_CATEGORIES.has(category)) {
      where.push(`pr.category = $${idx}`);
      params.push(category);
      idx += 1;
    }
    if (team) {
      where.push(
        `coalesce(pr.assigned_team, case when pr.assigned_role = 'PASTOR' then 'PASTORAL' else pr.assigned_role end) = $${idx}`
      );
      params.push(team);
      idx += 1;
    }
    if (assignedTo) {
      where.push(`coalesce(pr.assigned_to_user_id, pr.assigned_member_id)::text = $${idx}`);
      params.push(assignedTo);
      idx += 1;
    }
    if (campusId) {
      where.push(`pr.campus_id = $${idx}`);
      params.push(campusId);
      idx += 1;
    }

    if (!policy.canReadAll) {
      const scopeClauses = [];
      scopeClauses.push(`coalesce(pr.assigned_to_user_id, pr.assigned_member_id) = $${idx}::uuid`);
      params.push(req.user?.id || null);
      idx += 1;
      if (policy.teams.length) {
        scopeClauses.push(
          `coalesce(pr.assigned_team, case when pr.assigned_role = 'PASTOR' then 'PASTORAL' else pr.assigned_role end) = any($${idx}::text[])`
        );
        params.push(policy.teams);
        idx += 1;
      }
      where.push(`(${scopeClauses.join(" or ")})`);
    }

    params.push(limit);
    const rows = await db.manyOrNone(
      `
      select
        pr.id,
        pr.church_id as "churchId",
        pr.campus_id as "campusId",
        cc.name as "campusName",
        cc.code as "campusCode",
        pr.member_pk as "memberPk",
        pr.member_id as "memberId",
        m.full_name as "memberName",
        pr.category,
        pr.visibility,
        pr.subject,
        pr.message,
        pr.status,
        coalesce(pr.assigned_team, case when pr.assigned_role = 'PASTOR' then 'PASTORAL' else pr.assigned_role end) as "assignedTeam",
        coalesce(pr.assigned_to_user_id, pr.assigned_member_id) as "assignedToUserId",
        am.full_name as "assignedToUserName",
        pr.assigned_at as "assignedAt",
        pr.created_at as "createdAt",
        pr.updated_at as "updatedAt",
        pr.closed_at as "closedAt"
      from church_prayer_requests pr
      join members m on m.id = pr.member_pk
      left join members am on am.id = coalesce(pr.assigned_to_user_id, pr.assigned_member_id)
      left join church_campuses cc on cc.id = pr.campus_id
      where ${where.join(" and ")}
      order by
        case
          when pr.status='NEW' then 0
          when pr.status='ASSIGNED' then 1
          when pr.status='IN_PROGRESS' then 2
          else 3
        end asc,
        pr.created_at desc
      limit $${idx}
      `,
      params
    );
    const access = getChurchLifeAccess(req);
    const assignees = policy.canAssign ? await listPrayerAssignableUsers(churchId) : [];

    return res.json({
      ok: true,
      prayerRequests: rows.map((row) => redactPrayerContent(normalizePrayerRequestRow(row), access)),
      assignees,
      meta: {
        limit,
        status: status || null,
        team: team || null,
        assignedTo: assignedTo || null,
        campusId: campusId || null,
        returned: rows.length,
        canAssign: policy.canAssign,
        canReadAll: policy.canReadAll,
      },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life prayer inbox is not available yet. Run migrations and retry." });
    }
    console.error("[church-life/prayer/inbox] list error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/church-life/prayer/:requestId/assign", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const requestId = String(req.params?.requestId || "").trim();
    if (!UUID_REGEX.test(requestId)) return res.status(400).json({ error: "Invalid requestId." });

    const policy = await resolvePrayerAccessPolicy(req, churchId);
    if (policy.blocked) {
      return res.status(403).json({ error: "Prayer inbox is not available for finance roles." });
    }
    if (!policy.canAssign) {
      return res.status(403).json({ error: "Only admins, pastors, and prayer team leads can assign requests." });
    }

    const assignedTeam = normalizeChurchLifePrayerTeam(req.body?.assignedTeam);
    if (!assignedTeam) {
      return res.status(400).json({ error: "assignedTeam must be one of PRAYER_TEAM, CARE_TEAM, PASTORAL." });
    }
    const assignedToUserIdInput = String(req.body?.assignedToUserId || "").trim();
    let assignedToUserId = null;
    if (assignedToUserIdInput) {
      if (!UUID_REGEX.test(assignedToUserIdInput)) return res.status(400).json({ error: "assignedToUserId is invalid." });
      const assignedUser = await db.oneOrNone(
        `
        select id, lower(coalesce(role, 'member')) as role
        from members
        where church_id=$1 and id=$2
        limit 1
        `,
        [churchId, assignedToUserIdInput]
      );
      if (!assignedUser) return res.status(400).json({ error: "assignedToUserId is not in this church." });
      if (normalizeChurchStaffRole(assignedUser.role) === "finance") {
        return res.status(400).json({ error: "Finance users cannot be assigned prayer requests." });
      }
      const candidateTeams = await listPrayerTeamsForMember(churchId, assignedUser.id, assignedUser.role);
      const exempt = ["admin", "super", "pastor"].includes(normalizeChurchStaffRole(assignedUser.role));
      if (!exempt && !candidateTeams.includes(assignedTeam)) {
        return res.status(400).json({ error: "assignedToUserId does not belong to the selected team." });
      }
      assignedToUserId = assignedUser.id;
    }

    const beforeRow = await loadPrayerRequestWithNames(churchId, requestId);
    if (!beforeRow) return res.status(404).json({ error: "Prayer request not found." });

    await db.none(
      `
      update church_prayer_requests
      set
        assigned_team = $3,
        assigned_to_user_id = $4::uuid,
        assigned_role = case when $3 = 'PASTORAL' then 'PASTOR' else $3 end,
        assigned_member_id = $4::uuid,
        assigned_at = now(),
        status = case when status in ('NEW', 'CLOSED') then 'ASSIGNED' else status end,
        closed_at = case when status='CLOSED' then null else closed_at end,
        closed_by = case when status='CLOSED' then null else closed_by end,
        updated_at = now()
      where church_id=$1 and id=$2
      `,
      [churchId, requestId, assignedTeam, assignedToUserId]
    );

    const afterRow = await loadPrayerRequestWithNames(churchId, requestId);
    const access = getChurchLifeAccess(req);
    await writeChurchLifeAuditLog({
      churchId,
      actorMemberId: req.user?.id || null,
      actorRole: req.user?.role || null,
      action: "PRAYER_ASSIGNED",
      entityType: "PRAYER_REQUEST",
      entityId: requestId,
      entityRef: beforeRow?.memberId || null,
      before: {
        status: beforeRow?.status || null,
        assignedTeam: normalizeChurchLifePrayerTeam(beforeRow?.assignedTeam),
        assignedToUserId: beforeRow?.assignedToUserId || null,
      },
      after: {
        status: afterRow?.status || null,
        assignedTeam: normalizeChurchLifePrayerTeam(afterRow?.assignedTeam),
        assignedToUserId: afterRow?.assignedToUserId || null,
      },
      meta: {
        route: "church-life/prayer/assign",
      },
    });

    return res.json({
      ok: true,
      prayerRequest: redactPrayerContent(normalizePrayerRequestRow(afterRow), access),
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life prayer inbox is not available yet. Run migrations and retry." });
    }
    console.error("[church-life/prayer/assign] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/church-life/prayer/:requestId/close", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const requestId = String(req.params?.requestId || "").trim();
    if (!UUID_REGEX.test(requestId)) return res.status(400).json({ error: "Invalid requestId." });

    const policy = await resolvePrayerAccessPolicy(req, churchId);
    if (policy.blocked) {
      return res.status(403).json({ error: "Prayer inbox is not available for finance roles." });
    }

    const beforeRow = await loadPrayerRequestWithNames(churchId, requestId);
    if (!beforeRow) return res.status(404).json({ error: "Prayer request not found." });

    const assignedToUserId = String(beforeRow?.assignedToUserId || "");
    const assignedTeam = normalizeChurchLifePrayerTeam(beforeRow?.assignedTeam);
    const canClose =
      policy.canAssign ||
      assignedToUserId === String(req.user?.id || "") ||
      (assignedTeam && policy.teams.includes(assignedTeam));
    if (!canClose) {
      return res.status(403).json({ error: "You are not allowed to close this prayer request." });
    }

    await db.none(
      `
      update church_prayer_requests
      set
        status = 'CLOSED',
        closed_at = coalesce(closed_at, now()),
        closed_by = $3,
        updated_at = now()
      where church_id=$1 and id=$2
      `,
      [churchId, requestId, req.user?.id || null]
    );

    const afterRow = await loadPrayerRequestWithNames(churchId, requestId);
    const access = getChurchLifeAccess(req);
    await writeChurchLifeAuditLog({
      churchId,
      actorMemberId: req.user?.id || null,
      actorRole: req.user?.role || null,
      action: "PRAYER_STATUS_CHANGED",
      entityType: "PRAYER_REQUEST",
      entityId: requestId,
      entityRef: beforeRow?.memberId || null,
      before: { status: beforeRow?.status || null },
      after: { status: afterRow?.status || null },
      meta: { route: "church-life/prayer/close" },
    });

    return res.json({
      ok: true,
      prayerRequest: redactPrayerContent(normalizePrayerRequestRow(afterRow), access),
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life prayer inbox is not available yet. Run migrations and retry." });
    }
    console.error("[church-life/prayer/close] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/church-life/prayer/:requestId/start", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const requestId = String(req.params?.requestId || "").trim();
    if (!UUID_REGEX.test(requestId)) return res.status(400).json({ error: "Invalid requestId." });

    const policy = await resolvePrayerAccessPolicy(req, churchId);
    if (policy.blocked) {
      return res.status(403).json({ error: "Prayer inbox is not available for finance roles." });
    }

    const beforeRow = await loadPrayerRequestWithNames(churchId, requestId);
    if (!beforeRow) return res.status(404).json({ error: "Prayer request not found." });

    const assignedToUserId = String(beforeRow?.assignedToUserId || "");
    const assignedTeam = normalizeChurchLifePrayerTeam(beforeRow?.assignedTeam);
    const canStart =
      policy.canAssign ||
      assignedToUserId === String(req.user?.id || "") ||
      (assignedTeam && policy.teams.includes(assignedTeam));
    if (!canStart) {
      return res.status(403).json({ error: "You are not allowed to start this prayer request." });
    }

    await db.none(
      `
      update church_prayer_requests
      set
        status = 'IN_PROGRESS',
        closed_at = null,
        closed_by = null,
        updated_at = now()
      where church_id=$1 and id=$2
      `,
      [churchId, requestId]
    );

    const afterRow = await loadPrayerRequestWithNames(churchId, requestId);
    const access = getChurchLifeAccess(req);
    await writeChurchLifeAuditLog({
      churchId,
      actorMemberId: req.user?.id || null,
      actorRole: req.user?.role || null,
      action: "PRAYER_STATUS_CHANGED",
      entityType: "PRAYER_REQUEST",
      entityId: requestId,
      entityRef: beforeRow?.memberId || null,
      before: { status: beforeRow?.status || null },
      after: { status: afterRow?.status || null },
      meta: { route: "church-life/prayer/start" },
    });

    return res.json({
      ok: true,
      prayerRequest: redactPrayerContent(normalizePrayerRequestRow(afterRow), access),
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life prayer inbox is not available yet. Run migrations and retry." });
    }
    console.error("[church-life/prayer/start] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/church-life/prayer/:requestId/reopen", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const requestId = String(req.params?.requestId || "").trim();
    if (!UUID_REGEX.test(requestId)) return res.status(400).json({ error: "Invalid requestId." });

    const policy = await resolvePrayerAccessPolicy(req, churchId);
    if (policy.blocked) {
      return res.status(403).json({ error: "Prayer inbox is not available for finance roles." });
    }

    const beforeRow = await loadPrayerRequestWithNames(churchId, requestId);
    if (!beforeRow) return res.status(404).json({ error: "Prayer request not found." });

    const assignedToUserId = String(beforeRow?.assignedToUserId || "");
    const assignedTeam = normalizeChurchLifePrayerTeam(beforeRow?.assignedTeam);
    const canReopen =
      policy.canAssign ||
      assignedToUserId === String(req.user?.id || "") ||
      (assignedTeam && policy.teams.includes(assignedTeam));
    if (!canReopen) {
      return res.status(403).json({ error: "You are not allowed to reopen this prayer request." });
    }

    const reopenStatus = beforeRow?.assignedTeam || beforeRow?.assignedToUserId ? "ASSIGNED" : "NEW";
    await db.none(
      `
      update church_prayer_requests
      set
        status = $3,
        closed_at = null,
        closed_by = null,
        updated_at = now()
      where church_id=$1 and id=$2
      `,
      [churchId, requestId, reopenStatus]
    );

    const afterRow = await loadPrayerRequestWithNames(churchId, requestId);
    const access = getChurchLifeAccess(req);
    await writeChurchLifeAuditLog({
      churchId,
      actorMemberId: req.user?.id || null,
      actorRole: req.user?.role || null,
      action: "PRAYER_STATUS_CHANGED",
      entityType: "PRAYER_REQUEST",
      entityId: requestId,
      entityRef: beforeRow?.memberId || null,
      before: { status: beforeRow?.status || null },
      after: { status: afterRow?.status || null },
      meta: { route: "church-life/prayer/reopen" },
    });

    return res.json({
      ok: true,
      prayerRequest: redactPrayerContent(normalizePrayerRequestRow(afterRow), access),
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life prayer inbox is not available yet. Run migrations and retry." });
    }
    console.error("[church-life/prayer/reopen] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get(
  "/admin/church-life/services",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("services.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const limit = parseChurchLifeLimit(req.query?.limit, 60, 200);
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      const rows = await db.manyOrNone(
        `
        select
          s.id,
          s.church_id as "churchId",
          s.campus_id as "campusId",
          cc.name as "campusName",
          cc.code as "campusCode",
          s.service_name as "serviceName",
          s.service_date as "serviceDate",
          s.starts_at as "startsAt",
          s.ends_at as "endsAt",
          s.location,
          s.notes,
          s.published,
          s.created_at as "createdAt",
          s.updated_at as "updatedAt",
          coalesce(ci.count, 0)::int as "checkInsCount",
          coalesce(ap.count, 0)::int as "apologiesCount"
        from church_services s
        left join lateral (
          select count(*)::int as count
          from church_checkins c
          where c.service_id = s.id
        ) ci on true
        left join lateral (
          select count(*)::int as count
          from church_apologies a
          where a.service_id = s.id
        ) ap on true
        left join church_campuses cc on cc.id = s.campus_id
        where s.church_id=$1
          and ($2::uuid is null or s.campus_id = $2)
        order by s.service_date desc, coalesce(s.starts_at, s.created_at) desc
        limit $3
        `,
        [churchId, campusId, limit]
      );

      return res.json({
        ok: true,
        services: rows.map((row) => ({
          ...normalizeChurchServiceRow(row),
          checkInsCount: Number(row?.checkInsCount || 0),
          apologiesCount: Number(row?.apologiesCount || 0),
        })),
        meta: { limit, campusId: campusId || null, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life services are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/services] list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/services",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("services.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const serviceName = String(req.body?.serviceName || "").trim().slice(0, 120);
      const serviceDate = parseChurchLifeDate(req.body?.serviceDate);
      if (!serviceName) return res.status(400).json({ error: "serviceName is required." });
      if (!serviceDate) return res.status(400).json({ error: "serviceDate must be YYYY-MM-DD." });

      const startsAt = parseChurchLifeDateTimeOrNull(req.body?.startsAt);
      const endsAt = parseChurchLifeDateTimeOrNull(req.body?.endsAt);
      if (req.body?.startsAt && !startsAt) return res.status(400).json({ error: "startsAt is invalid." });
      if (req.body?.endsAt && !endsAt) return res.status(400).json({ error: "endsAt is invalid." });
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.body?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      const location = String(req.body?.location || "").trim().slice(0, 180) || null;
      const notes = String(req.body?.notes || "").trim().slice(0, 1000) || null;
      const published = toBoolean(req.body?.published) !== false;

      const row = await db.one(
        `
        with inserted as (
          insert into church_services (
            church_id, campus_id, service_name, service_date, starts_at, ends_at, location, notes, published, created_by, updated_by
          )
          values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$10)
          returning
            id,
            church_id,
            campus_id,
            service_name,
            service_date,
            starts_at,
            ends_at,
            location,
            notes,
            published,
            created_at,
            updated_at
        )
        select
          i.id,
          i.church_id as "churchId",
          i.campus_id as "campusId",
          cc.name as "campusName",
          cc.code as "campusCode",
          i.service_name as "serviceName",
          i.service_date as "serviceDate",
          i.starts_at as "startsAt",
          i.ends_at as "endsAt",
          i.location,
          i.notes,
          i.published,
          i.created_at as "createdAt",
          i.updated_at as "updatedAt"
        from inserted i
        left join church_campuses cc on cc.id = i.campus_id
        `,
        [churchId, campusId, serviceName, serviceDate, startsAt, endsAt, location, notes, published, req.user.id]
      );

      return res.status(201).json({ ok: true, service: normalizeChurchServiceRow(row) });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life services are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/services] create error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/church-life/services/:serviceId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("services.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const serviceId = String(req.params?.serviceId || "").trim();
      if (!UUID_REGEX.test(serviceId)) return res.status(400).json({ error: "Invalid serviceId" });

      const current = await db.oneOrNone("select * from church_services where id=$1 and church_id=$2", [serviceId, churchId]);
      if (!current) return res.status(404).json({ error: "Service not found" });

      const serviceName = String(req.body?.serviceName ?? current.service_name ?? "")
        .trim()
        .slice(0, 120);
      const serviceDate = parseChurchLifeDate(req.body?.serviceDate ?? current.service_date);
      const startsAt = parseChurchLifeDateTimeOrNull(
        typeof req.body?.startsAt === "undefined" ? current.starts_at : req.body?.startsAt
      );
      const endsAt = parseChurchLifeDateTimeOrNull(
        typeof req.body?.endsAt === "undefined" ? current.ends_at : req.body?.endsAt
      );
      let campusId = current?.campus_id || null;
      if (Object.prototype.hasOwnProperty.call(req.body || {}, "campusId")) {
        try {
          campusId = await resolveChurchCampusId(churchId, req.body?.campusId, { fieldName: "campusId" });
        } catch (campusErr) {
          return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
        }
      }
      const location =
        String(typeof req.body?.location === "undefined" ? current.location || "" : req.body?.location || "")
          .trim()
          .slice(0, 180) || null;
      const notes =
        String(typeof req.body?.notes === "undefined" ? current.notes || "" : req.body?.notes || "")
          .trim()
          .slice(0, 1000) || null;
      const published = typeof req.body?.published === "undefined" ? current.published !== false : toBoolean(req.body?.published) === true;

      if (!serviceName) return res.status(400).json({ error: "serviceName is required." });
      if (!serviceDate) return res.status(400).json({ error: "serviceDate must be YYYY-MM-DD." });

      const row = await db.one(
        `
        with updated as (
          update church_services
          set
            campus_id = $3,
            service_name = $4,
            service_date = $5,
            starts_at = $6,
            ends_at = $7,
            location = $8,
            notes = $9,
            published = $10,
            updated_by = $11,
            updated_at = now()
          where id = $1 and church_id = $2
          returning
            id,
            church_id,
            campus_id,
            service_name,
            service_date,
            starts_at,
            ends_at,
            location,
            notes,
            published,
            created_at,
            updated_at
        )
        select
          u.id,
          u.church_id as "churchId",
          u.campus_id as "campusId",
          cc.name as "campusName",
          cc.code as "campusCode",
          u.service_name as "serviceName",
          u.service_date as "serviceDate",
          u.starts_at as "startsAt",
          u.ends_at as "endsAt",
          u.location,
          u.notes,
          u.published,
          u.created_at as "createdAt",
          u.updated_at as "updatedAt"
        from updated u
        left join church_campuses cc on cc.id = u.campus_id
        `,
        [serviceId, churchId, campusId, serviceName, serviceDate, startsAt, endsAt, location, notes, published, req.user.id]
      );

      return res.json({ ok: true, service: normalizeChurchServiceRow(row) });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life services are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/services] update error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/check-ins/usher",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("checkins.usher.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const serviceId = String(req.body?.serviceId || "").trim();
      const memberRef = String(req.body?.memberId || req.body?.memberRef || "").trim();
      if (!UUID_REGEX.test(serviceId)) return res.status(400).json({ error: "serviceId is required." });
      if (!memberRef) return res.status(400).json({ error: "memberId is required (member UUID or member_id)." });
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.body?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      const service = await findChurchServiceById(churchId, serviceId);
      if (!service) return res.status(404).json({ error: "Service not found." });
      if (campusId && service.campusId && service.campusId !== campusId) {
        return res.status(400).json({ error: "serviceId does not belong to the selected campus." });
      }
      const finalCampusId = campusId || service.campusId || null;

      const member = await findChurchMemberByReference(churchId, memberRef);
      if (!member) return res.status(404).json({ error: "Member not found for this church." });
      const notes = String(req.body?.notes || "").trim().slice(0, 250) || null;
      const row = await db.oneOrNone(
        `
        with inserted as (
          insert into church_checkins (
            church_id, campus_id, service_id, member_pk, member_id, method, checked_in_at, created_by, notes
          )
          values ($1,$2,$3,$4,$5,'USHER',now(),$6,$7)
          on conflict (church_id, service_id, member_pk) do nothing
          returning
            id,
            church_id as "churchId",
            campus_id as "campusId",
            service_id as "serviceId",
            member_pk as "memberPk",
            member_id as "memberId",
            method,
            checked_in_at as "checkedInAt",
            notes,
            false as "idempotent"
        )
        select * from inserted
        union all
        select
          c.id,
          c.church_id as "churchId",
          c.campus_id as "campusId",
          c.service_id as "serviceId",
          c.member_pk as "memberPk",
          c.member_id as "memberId",
          c.method,
          c.checked_in_at as "checkedInAt",
          c.notes,
          true as "idempotent"
        from church_checkins c
        where c.church_id=$1 and c.service_id=$3 and c.member_pk=$4
          and not exists (select 1 from inserted)
        limit 1
        `,
        [churchId, finalCampusId, serviceId, member.id, member.member_id || null, req.user.id, notes]
      );
      if (!row) {
        return res.status(500).json({ error: "Could not resolve check-in state." });
      }

      return res.status(row.idempotent ? 200 : 201).json({
        ok: true,
        idempotent: row.idempotent === true,
        checkIn: {
          ...normalizeChurchCheckinRow({
            ...row,
            campusName: service?.campusName || null,
            campusCode: service?.campusCode || null,
          }),
          memberName: member.full_name || null,
          memberRole: member.role || null,
        },
        service: {
          id: service.id,
          serviceName: service.serviceName,
          serviceDate: formatDateIsoLike(service.serviceDate),
        },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life check-ins are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/check-ins/usher] error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/check-ins/live",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("checkins.live.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const limit = parseChurchLifeLimit(req.query?.limit, 120, 400);
      const requestedServiceId = String(req.query?.serviceId || "").trim();
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }
      let service = null;

      if (requestedServiceId) {
        if (!UUID_REGEX.test(requestedServiceId)) {
          return res.status(400).json({ error: "serviceId must be a UUID." });
        }
        service = await findChurchServiceById(churchId, requestedServiceId, campusId);
        if (!service) return res.status(404).json({ error: "Service not found." });
      } else {
        service = await findLatestChurchService(churchId, campusId);
      }

      if (!service) {
        return res.json({
          ok: true,
          service: null,
          rows: [],
          summary: { total: 0, tapCount: 0, qrCount: 0, usherCount: 0, lastCheckInAt: null },
          meta: { limit, returned: 0, campusId: campusId || null, generatedAt: new Date().toISOString() },
        });
      }

      const [rows, summary] = await Promise.all([
        db.manyOrNone(
          `
          select
            c.id,
            c.church_id as "churchId",
            c.campus_id as "campusId",
            c.service_id as "serviceId",
            c.member_pk as "memberPk",
            c.member_id as "memberId",
            c.method,
            c.checked_in_at as "checkedInAt",
            c.notes,
            m.full_name as "memberName",
            lower(coalesce(m.role, 'member')) as "memberRole",
            m.phone as "memberPhone",
            m.email as "memberEmail",
            cc.name as "campusName",
            cc.code as "campusCode"
          from church_checkins c
          join members m on m.id = c.member_pk
          left join church_campuses cc on cc.id = c.campus_id
          where c.church_id=$1 and c.service_id=$2
          order by c.checked_in_at desc
          limit $3
          `,
          [churchId, service.id, limit]
        ),
        db.one(
          `
          select
            count(*)::int as total,
            count(*) filter (where method='TAP')::int as "tapCount",
            count(*) filter (where method='QR')::int as "qrCount",
            count(*) filter (where method='USHER')::int as "usherCount",
            max(checked_in_at) as "lastCheckInAt"
          from church_checkins
          where church_id=$1 and service_id=$2
          `,
          [churchId, service.id]
        ),
      ]);
      const access = getChurchLifeAccess(req);
      const canReadContact = hasChurchLifePermission(access, "checkins.contact.read");

      return res.json({
        ok: true,
        service: normalizeChurchServiceRow(service),
        rows: rows.map((row) => ({
          ...normalizeChurchCheckinRow(row),
          memberPhone: canReadContact ? row?.memberPhone || null : null,
          memberEmail: canReadContact ? row?.memberEmail || null : null,
        })),
        summary: {
          total: Number(summary?.total || 0),
          tapCount: Number(summary?.tapCount || 0),
          qrCount: Number(summary?.qrCount || 0),
          usherCount: Number(summary?.usherCount || 0),
          lastCheckInAt: summary?.lastCheckInAt || null,
        },
        meta: { limit, returned: rows.length, campusId: campusId || null, generatedAt: new Date().toISOString() },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life check-ins are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/check-ins/live] error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/check-ins/auto-mark",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("checkins.attendance.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const serviceId = String(req.body?.serviceId || "").trim();
      if (!UUID_REGEX.test(serviceId)) return res.status(400).json({ error: "serviceId is required." });

      const service = await findChurchServiceById(churchId, serviceId);
      if (!service) return res.status(404).json({ error: "Service not found." });
      const serviceDate = formatDateIsoLike(service.serviceDate);
      const serviceCampusId = service.campusId || null;
      const previousAttendance = await db.oneOrNone(
        `
        select
          id,
          service_id as "serviceId",
          campus_id as "campusId",
          service_date as "serviceDate",
          total_attendance as "totalAttendance",
          adults_count as "adultsCount",
          youth_count as "youthCount",
          children_count as "childrenCount",
          first_time_guests as "firstTimeGuests",
          notes,
          created_at as "createdAt",
          updated_at as "updatedAt"
        from church_attendance_records
        where church_id=$1 and service_id=$2
        limit 1
        `,
        [churchId, service.id]
      );

      const checkinSummary = await db.one(
        `
        select
          count(*)::int as total,
          count(*) filter (where method='TAP')::int as "tapCount",
          count(*) filter (where method='QR')::int as "qrCount",
          count(*) filter (where method='USHER')::int as "usherCount"
        from church_checkins
        where church_id=$1 and service_id=$2
        `,
        [churchId, serviceId]
      );

      const totalAttendance = Number(checkinSummary?.total || 0);
      const actorId = parseUuidOrNull(req.user?.id);
      const adultsParsed = parseNonNegativeWholeNumber(req.body?.adultsCount, "adultsCount");
      if (adultsParsed.error) return res.status(400).json({ error: adultsParsed.error });
      const youthParsed = parseNonNegativeWholeNumber(req.body?.youthCount, "youthCount");
      if (youthParsed.error) return res.status(400).json({ error: youthParsed.error });
      const childrenParsed = parseNonNegativeWholeNumber(req.body?.childrenCount, "childrenCount");
      if (childrenParsed.error) return res.status(400).json({ error: childrenParsed.error });
      const guestsParsed = parseNonNegativeWholeNumber(req.body?.firstTimeGuests, "firstTimeGuests");
      if (guestsParsed.error) return res.status(400).json({ error: guestsParsed.error });

      const adultsCount = adultsParsed.value;
      const youthCount = youthParsed.value;
      const childrenCount = childrenParsed.value;
      const firstTimeGuests = guestsParsed.value;
      if (adultsCount + youthCount + childrenCount > totalAttendance) {
        return res.status(400).json({
          error: "adultsCount + youthCount + childrenCount cannot exceed total check-ins.",
        });
      }

      const notesSuffix = String(req.body?.notes || "").trim().slice(0, 450);
      const autoNote = `Auto-marked from ${totalAttendance} check-ins for ${service.serviceName}.`;
      const notes = notesSuffix ? `${autoNote} ${notesSuffix}`.slice(0, 1000) : autoNote;

      const row = await db.one(
        `
        insert into church_attendance_records (
          church_id,
          service_id,
          campus_id,
          service_date,
          total_attendance,
          adults_count,
          youth_count,
          children_count,
          first_time_guests,
          notes,
          created_by,
          updated_by
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11)
        on conflict (church_id, service_id)
        do update set
          campus_id = excluded.campus_id,
          service_date = excluded.service_date,
          total_attendance = excluded.total_attendance,
          adults_count = excluded.adults_count,
          youth_count = excluded.youth_count,
          children_count = excluded.children_count,
          first_time_guests = excluded.first_time_guests,
          notes = excluded.notes,
          updated_by = excluded.updated_by,
          updated_at = now()
        returning
          id,
          service_id as "serviceId",
          campus_id as "campusId",
          service_date as "serviceDate",
          total_attendance as "totalAttendance",
          adults_count as "adultsCount",
          youth_count as "youthCount",
          children_count as "childrenCount",
          first_time_guests as "firstTimeGuests",
          notes,
          created_at as "createdAt",
          updated_at as "updatedAt"
        `,
        [
          churchId,
          service.id,
          serviceCampusId,
          serviceDate,
          totalAttendance,
          adultsCount,
          youthCount,
          childrenCount,
          firstTimeGuests,
          notes,
          actorId,
        ]
      );

      const summary = await readChurchAttendanceSummary(churchId, serviceCampusId);
      await writeChurchLifeAuditLog({
        churchId,
        actorMemberId: actorId,
        actorRole: req.user?.role || null,
        action: previousAttendance ? "ATTENDANCE_OVERRIDE" : "ATTENDANCE_AUTO_MARK",
        entityType: "ATTENDANCE_RECORD",
        entityId: row?.id || null,
        entityRef: serviceDate,
        before: previousAttendance ? normalizeAttendanceRow(previousAttendance) : {},
        after: normalizeAttendanceRow(row),
        meta: {
          serviceId,
          serviceName: service.serviceName || null,
          serviceDate,
          totalCheckins: totalAttendance,
          tapCount: Number(checkinSummary?.tapCount || 0),
          qrCount: Number(checkinSummary?.qrCount || 0),
          usherCount: Number(checkinSummary?.usherCount || 0),
        },
      });
      return res.status(201).json({
        ok: true,
        row: normalizeAttendanceRow({
          ...row,
          serviceId: service.id,
          serviceName: service.serviceName || null,
          campusName: service.campusName || null,
          campusCode: service.campusCode || null,
        }),
        service: normalizeChurchServiceRow(service),
        checkInSummary: {
          total: totalAttendance,
          tapCount: Number(checkinSummary?.tapCount || 0),
          qrCount: Number(checkinSummary?.qrCount || 0),
          usherCount: Number(checkinSummary?.usherCount || 0),
        },
        summary,
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Attendance tracking is not available yet. Run migrations and retry." });
      }
      if (err?.code === "23514") {
        return res.status(400).json({
          error: "Invalid attendance values. Check totals and breakdown counts.",
        });
      }
      console.error("[admin/church-life/check-ins/auto-mark] error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/check-ins/import-csv",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("checkins.import.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const serviceId = String(req.body?.serviceId || "").trim();
      if (!UUID_REGEX.test(serviceId)) return res.status(400).json({ error: "serviceId is required." });
      const service = await findChurchServiceById(churchId, serviceId);
      if (!service) return res.status(404).json({ error: "Service not found." });

      const csvText = String(req.body?.csvText || "").trim();
      if (!csvText) return res.status(400).json({ error: "csvText is required." });

      const { rows, hasHeader } = parseChurchLifeCheckinCsv(csvText);
      if (!rows.length) {
        return res.status(400).json({
          error: "No import rows detected. Add member references in CSV.",
        });
      }
      if (rows.length > 1500) {
        return res.status(400).json({
          error: "CSV import is limited to 1500 rows per request.",
        });
      }

      const defaultMethodCandidate = normalizeUpperToken(req.body?.defaultMethod || "USHER");
      const defaultMethod = CHURCH_LIFE_CHECKIN_METHODS.has(defaultMethodCandidate) ? defaultMethodCandidate : "USHER";
      const actorId = req.user?.id || null;
      const imported = [];
      const errors = [];
      let skippedCount = 0;

      for (const row of rows) {
        const memberRef = String(row.memberRef || "").trim();
        if (!memberRef) {
          skippedCount += 1;
          errors.push({ lineNumber: row.lineNumber, reason: "Missing member reference." });
          continue;
        }

        const member = await findChurchMemberByReference(churchId, memberRef);
        if (!member) {
          skippedCount += 1;
          errors.push({ lineNumber: row.lineNumber, memberRef, reason: "Member not found in this church." });
          continue;
        }

        const methodRaw = normalizeUpperToken(row.method || defaultMethod);
        const method = CHURCH_LIFE_CHECKIN_METHODS.has(methodRaw) ? methodRaw : defaultMethod;
        const notes = String(row.notes || "").trim().slice(0, 250) || null;
        const checkedInAtCandidate = String(row.checkedInAt || "").trim();
        const checkedInDate = checkedInAtCandidate ? new Date(checkedInAtCandidate) : null;
        const checkedInAt =
          checkedInDate && !Number.isNaN(checkedInDate.getTime()) ? checkedInDate.toISOString() : null;

        const saved = await db.one(
          `
          insert into church_checkins (
            church_id, campus_id, service_id, member_pk, member_id, method, checked_in_at, created_by, notes
          )
          values ($1,$2,$3,$4,$5,$6,coalesce($7::timestamptz, now()),$8,$9)
          on conflict (church_id, service_id, member_pk)
          do update set
            campus_id = excluded.campus_id,
            method = excluded.method,
            checked_in_at = excluded.checked_in_at,
            created_by = excluded.created_by,
            notes = excluded.notes
          returning
            id,
            church_id as "churchId",
            campus_id as "campusId",
            service_id as "serviceId",
            member_pk as "memberPk",
            member_id as "memberId",
            method,
            checked_in_at as "checkedInAt",
            notes
          `,
          [churchId, service.campusId || null, serviceId, member.id, member.member_id || null, method, checkedInAt, actorId, notes]
        );

        imported.push({
          ...normalizeChurchCheckinRow(saved),
          memberName: member.full_name || null,
          memberRole: member.role || null,
          sourceMemberRef: memberRef,
          lineNumber: row.lineNumber,
        });
      }
      await writeChurchLifeAuditLog({
        churchId,
        actorMemberId: req.user?.id || null,
        actorRole: req.user?.role || null,
        action: "ATTENDANCE_IMPORT_CSV",
        entityType: "IMPORT_BATCH",
        entityId: null,
        entityRef: serviceId,
        before: {},
        after: {},
        meta: {
          serviceId,
          serviceName: service.serviceName || null,
          serviceDate: formatDateIsoLike(service.serviceDate),
          importedCount: imported.length,
          skippedCount,
          errorCount: errors.length,
          hasHeader,
          defaultMethod,
        },
      });

      return res.status(201).json({
        ok: true,
        service: normalizeChurchServiceRow(service),
        importedCount: imported.length,
        skippedCount,
        errorCount: errors.length,
        hasHeader,
        preview: imported.slice(0, 50),
        errors: errors.slice(0, 200),
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life check-ins are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/check-ins/import-csv] error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/apologies",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("apologies.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const serviceId = String(req.query?.serviceId || "").trim();
      const limit = parseChurchLifeLimit(req.query?.limit, 80, 300);

      const where = ["a.church_id=$1"];
      const params = [churchId];
      let idx = 2;
      if (serviceId) {
        where.push(`a.service_id=$${idx}`);
        params.push(serviceId);
        idx += 1;
      }
      params.push(limit);

      const rows = await db.manyOrNone(
        `
        select
          a.id,
          a.church_id as "churchId",
          a.service_id as "serviceId",
          a.member_pk as "memberPk",
          a.member_id as "memberId",
          m.full_name as "memberName",
          a.reason,
          a.message,
          a.status,
          a.created_at as "createdAt",
          a.updated_at as "updatedAt",
          a.resolved_at as "resolvedAt",
          s.service_name as "serviceName",
          s.service_date as "serviceDate"
        from church_apologies a
        join members m on m.id = a.member_pk
        left join church_services s on s.id = a.service_id
        where ${where.join(" and ")}
        order by coalesce(s.service_date, a.created_at::date) desc, a.created_at desc
        limit $${idx}
        `,
        params
      );

      return res.json({
        ok: true,
        apologies: rows.map((row) => ({
          ...normalizeChurchApologyRow(row),
          serviceName: row?.serviceName || null,
          serviceDate: formatDateIsoLike(row?.serviceDate),
        })),
        meta: { limit, returned: rows.length, serviceId: serviceId || null },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life apologies are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/apologies] error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/prayer-assignments",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("prayer.assignments.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const [members, assignments] = await Promise.all([
        db.manyOrNone(
          `
          select id, member_id as "memberId", full_name as "fullName", role
          from members
          where church_id=$1
          order by created_at asc
          `,
          [churchId]
        ),
        db.manyOrNone(
          `
          select
            id,
            church_id as "churchId",
            member_pk as "memberPk",
            team_role as "teamRole",
            active,
            created_at as "createdAt",
            updated_at as "updatedAt"
          from church_prayer_team_assignments
          where church_id=$1
          order by active desc, team_role asc, created_at asc
          `,
          [churchId]
        ),
      ]);

      return res.json({ ok: true, members, assignments });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life prayer assignments are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/prayer-assignments] error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.put(
  "/admin/church-life/prayer-assignments",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("prayer.assignments.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const rows = Array.isArray(req.body?.assignments) ? req.body.assignments : [];
      if (rows.length > 500) return res.status(400).json({ error: "Too many assignment rows." });

      const normalized = rows
        .map((row) => ({
          memberPk: String(row?.memberPk || row?.memberId || "").trim(),
          teamRole: normalizeChurchLifePrayerAssignmentRole(row?.teamRole),
          active: toBoolean(row?.active) !== false,
        }))
        .filter((row) => UUID_REGEX.test(row.memberPk) && row.teamRole);

      await db.tx(async (t) => {
        await t.none("delete from church_prayer_team_assignments where church_id=$1", [churchId]);
        for (const row of normalized) {
          await t.none(
            `
            insert into church_prayer_team_assignments (church_id, member_pk, team_role, active, created_at, updated_at)
            values ($1,$2,$3,$4,now(),now())
            `,
            [churchId, row.memberPk, row.teamRole, row.active]
          );
        }
      });

      return res.json({ ok: true, saved: normalized.length });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life prayer assignments are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/prayer-assignments] save error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/prayer-requests",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("prayer.requests.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const status = normalizeUpperToken(req.query?.status);
      const category = normalizeUpperToken(req.query?.category);
      const visibility = normalizeUpperToken(req.query?.visibility);
      const assignedTeam = normalizeChurchLifePrayerTeam(req.query?.assignedTeam || req.query?.assignedRole);
      const assignedMemberId = String(req.query?.assignedToUserId || req.query?.assignedMemberId || "").trim();
      const limit = parseChurchLifeLimit(req.query?.limit, 80, 300);
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      const where = ["pr.church_id = $1"];
      const params = [churchId];
      let idx = 2;

      if (status && ["NEW", "ASSIGNED", "IN_PROGRESS", "CLOSED"].includes(status)) {
        where.push(`pr.status = $${idx}`);
        params.push(status);
        idx += 1;
      }
      if (CHURCH_LIFE_PRAYER_CATEGORIES.has(category)) {
        where.push(`pr.category = $${idx}`);
        params.push(category);
        idx += 1;
      }
      if (CHURCH_LIFE_PRAYER_VISIBILITIES.has(visibility)) {
        where.push(`pr.visibility = $${idx}`);
        params.push(visibility);
        idx += 1;
      }
      if (assignedTeam) {
        where.push(
          `coalesce(pr.assigned_team, case when pr.assigned_role = 'PASTOR' then 'PASTORAL' else pr.assigned_role end) = $${idx}`
        );
        params.push(assignedTeam);
        idx += 1;
      }
      if (assignedMemberId) {
        where.push(`coalesce(pr.assigned_to_user_id, pr.assigned_member_id)::text = $${idx}`);
        params.push(assignedMemberId);
        idx += 1;
      }
      if (campusId) {
        where.push(`pr.campus_id = $${idx}`);
        params.push(campusId);
        idx += 1;
      }
      params.push(limit);

      const rows = await db.manyOrNone(
        `
        select
          pr.id,
          pr.church_id as "churchId",
          pr.campus_id as "campusId",
          cc.name as "campusName",
          cc.code as "campusCode",
          pr.member_pk as "memberPk",
          pr.member_id as "memberId",
          m.full_name as "memberName",
          pr.category,
          pr.visibility,
          pr.subject,
          pr.message,
          pr.status,
          coalesce(pr.assigned_team, case when pr.assigned_role = 'PASTOR' then 'PASTORAL' else pr.assigned_role end) as "assignedTeam",
          coalesce(pr.assigned_to_user_id, pr.assigned_member_id) as "assignedToUserId",
          am.full_name as "assignedToUserName",
          pr.created_at as "createdAt",
          pr.updated_at as "updatedAt",
          pr.closed_at as "closedAt"
        from church_prayer_requests pr
        join members m on m.id = pr.member_pk
        left join members am on am.id = coalesce(pr.assigned_to_user_id, pr.assigned_member_id)
        left join church_campuses cc on cc.id = pr.campus_id
        where ${where.join(" and ")}
        order by pr.created_at desc
        limit $${idx}
        `,
        params
      );
      const access = getChurchLifeAccess(req);

      return res.json({
        ok: true,
        prayerRequests: rows.map((row) => redactPrayerContent(normalizePrayerRequestRow(row), access)),
        meta: { limit, campusId: campusId || null, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life prayer inbox is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/prayer] list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/church-life/prayer-requests/:requestId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("prayer.requests.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const requestId = String(req.params?.requestId || "").trim();
      if (!UUID_REGEX.test(requestId)) return res.status(400).json({ error: "Invalid requestId" });

      const existing = await db.oneOrNone(
        `
        select id, church_id, campus_id, status
        from church_prayer_requests
        where id=$1 and church_id=$2
        `,
        [requestId, churchId]
      );
      if (!existing) return res.status(404).json({ error: "Prayer request not found." });

      const nextStatusRaw = normalizeUpperToken(req.body?.status);
      const nextStatus = ["NEW", "ASSIGNED", "IN_PROGRESS", "CLOSED"].includes(nextStatusRaw)
        ? nextStatusRaw
        : normalizeUpperToken(existing.status || "NEW");
      let campusId = null;
      if (Object.prototype.hasOwnProperty.call(req.body || {}, "campusId")) {
        try {
          campusId = await resolveChurchCampusId(churchId, req.body?.campusId, { fieldName: "campusId" });
        } catch (campusErr) {
          return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
        }
      }
      const finalCampusId = Object.prototype.hasOwnProperty.call(req.body || {}, "campusId")
        ? campusId
        : existing.campus_id || null;
      const assignedTeam = normalizeChurchLifePrayerTeam(req.body?.assignedTeam || req.body?.assignedRole);
      const assignedMemberId = String(req.body?.assignedToUserId || req.body?.assignedMemberId || "").trim();
      const visibility = normalizeChurchLifePrayerVisibility(req.body?.visibility, req.body?.category || "GENERAL");

      let finalAssignedMemberId = null;
      if (assignedMemberId) {
        const member = await db.oneOrNone(
          "select id from members where church_id=$1 and id::text=$2 limit 1",
          [churchId, assignedMemberId]
        );
        if (!member) return res.status(400).json({ error: "assignedMemberId is not in this church." });
        finalAssignedMemberId = member.id;
      }

      const row = await db.one(
        `
        update church_prayer_requests
        set
          campus_id = $3::uuid,
          status = $4,
          assigned_team = coalesce($5, assigned_team),
          assigned_to_user_id = coalesce($6::uuid, assigned_to_user_id),
          assigned_role = coalesce(case when $5::text = 'PASTORAL' then 'PASTOR' else $5::text end, assigned_role),
          assigned_member_id = coalesce($6::uuid, assigned_member_id),
          assigned_at = case
            when $5::text is not null or $6::uuid is not null then now()
            else assigned_at
          end,
          visibility = $7,
          closed_at = case when $4 = 'CLOSED' then coalesce(closed_at, now()) else null end,
          closed_by = case when $4 = 'CLOSED' then $8 else null end,
          updated_at = now()
        where id=$1 and church_id=$2
        returning
          id,
          church_id as "churchId",
          member_pk as "memberPk",
          member_id as "memberId",
          category,
          visibility,
          subject,
          message,
          status,
          coalesce(assigned_team, case when assigned_role = 'PASTOR' then 'PASTORAL' else assigned_role end) as "assignedTeam",
          coalesce(assigned_to_user_id, assigned_member_id) as "assignedToUserId",
          created_at as "createdAt",
          updated_at as "updatedAt",
          closed_at as "closedAt"
        `,
        [requestId, churchId, finalCampusId, nextStatus, assignedTeam, finalAssignedMemberId, visibility, req.user.id]
      );

      const withNames = await db.one(
        `
        select
          pr.id,
          pr.church_id as "churchId",
          pr.campus_id as "campusId",
          cc.name as "campusName",
          cc.code as "campusCode",
          pr.member_pk as "memberPk",
          pr.member_id as "memberId",
          m.full_name as "memberName",
          pr.category,
          pr.visibility,
          pr.subject,
          pr.message,
          pr.status,
          coalesce(pr.assigned_team, case when pr.assigned_role = 'PASTOR' then 'PASTORAL' else pr.assigned_role end) as "assignedTeam",
          coalesce(pr.assigned_to_user_id, pr.assigned_member_id) as "assignedToUserId",
          am.full_name as "assignedToUserName",
          pr.created_at as "createdAt",
          pr.updated_at as "updatedAt",
          pr.closed_at as "closedAt"
        from church_prayer_requests pr
        join members m on m.id = pr.member_pk
        left join members am on am.id = coalesce(pr.assigned_to_user_id, pr.assigned_member_id)
        left join church_campuses cc on cc.id = pr.campus_id
        where pr.id=$1
        `,
        [row.id]
      );
      const access = getChurchLifeAccess(req);

      return res.json({
        ok: true,
        prayerRequest: redactPrayerContent(normalizePrayerRequestRow(withNames), access),
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life prayer inbox is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/prayer] patch error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/events",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("events.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const status = normalizeChurchLifeEventStatus(req.query?.status || "PUBLISHED");
      const limit = parseChurchLifeLimit(req.query?.limit, 40, 150);
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      const rows = await db.manyOrNone(
        `
        select
          e.id,
          e.church_id as "churchId",
          e.campus_id as "campusId",
          cc.name as "campusName",
          cc.code as "campusCode",
          e.title,
          e.description,
          e.starts_at as "startsAt",
          e.ends_at as "endsAt",
          e.venue,
          e.poster_url as "posterUrl",
          e.poster_data_url as "posterDataUrl",
          e.capacity,
          e.is_ticketed as "isTicketed",
          e.price,
          coalesce(reg.count, 0)::int as "registrationCount",
          e.status,
          e.notify_on_publish as "notifyOnPublish",
          e.published_at as "publishedAt",
          e.created_at as "createdAt",
          e.updated_at as "updatedAt"
        from church_events e
        left join church_campuses cc on cc.id = e.campus_id
        left join lateral (
          select count(*)::int as count
          from church_event_registrations r
          where r.event_id = e.id
            and r.status = any($5::text[])
        ) reg on true
        where e.church_id=$1 and e.status=$2
          and ($3::uuid is null or e.campus_id = $3)
        order by e.starts_at asc
        limit $4
        `,
        [churchId, status, campusId, limit, CHURCH_EVENT_REGISTRATION_ACTIVE_STATUSES]
      );

      return res.json({
        ok: true,
        events: rows.map(normalizeChurchEventRow),
        meta: { status, limit, campusId: campusId || null, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life events are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/events] list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/events",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("events.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const title = String(req.body?.title || "").trim().slice(0, 160);
      const description = String(req.body?.description || "").trim().slice(0, 4000) || null;
      const startsAt = parseChurchLifeDateTimeOrNull(req.body?.startsAt);
      const endsAt = parseChurchLifeDateTimeOrNull(req.body?.endsAt);
      const venue = String(req.body?.venue || "").trim().slice(0, 220) || null;
      const posterUrl = String(req.body?.posterUrl || "").trim().slice(0, 800) || null;
      const posterDataUrl = String(req.body?.posterDataUrl || "").trim();
      const posterData = posterDataUrl ? posterDataUrl.slice(0, 2_500_000) : null;
      const status = normalizeChurchLifeEventStatus(req.body?.status || "DRAFT");
      const notifyOnPublish = toBoolean(req.body?.notifyOnPublish) !== false;
      const rawCapacity = req.body?.capacity;
      const parsedCapacity =
        rawCapacity === null || typeof rawCapacity === "undefined" || rawCapacity === "" ? null : parsePositiveInt(rawCapacity, null);
      if (
        !(rawCapacity === null || typeof rawCapacity === "undefined" || rawCapacity === "") &&
        (parsedCapacity === null || parsedCapacity < 1 || parsedCapacity > 200000)
      ) {
        return res.status(400).json({ error: "capacity must be a whole number between 1 and 200000." });
      }
      const capacity = parsedCapacity && parsedCapacity > 0 ? parsedCapacity : null;
      const isTicketed = toBoolean(req.body?.isTicketed) === true;
      let price = null;
      if (Object.prototype.hasOwnProperty.call(req.body || {}, "price")) {
        if (req.body?.price === null || req.body?.price === "") {
          price = null;
        } else {
          const parsedPrice = Number(req.body?.price);
          if (!Number.isFinite(parsedPrice) || parsedPrice < 0 || parsedPrice > 100000000) {
            return res.status(400).json({ error: "price must be a valid amount between 0 and 100000000." });
          }
          price = toCurrencyNumber(parsedPrice);
        }
      }
      if (isTicketed && !(Number.isFinite(Number(price)) && Number(price) > 0)) {
        return res.status(400).json({ error: "price is required when isTicketed=true." });
      }
      if (!isTicketed) {
        price = null;
      }
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.body?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      if (!title) return res.status(400).json({ error: "title is required." });
      if (!startsAt) return res.status(400).json({ error: "startsAt is required." });
      if (req.body?.endsAt && !endsAt) return res.status(400).json({ error: "endsAt is invalid." });

      const row = await db.one(
        `
        with inserted as (
          insert into church_events (
            church_id, campus_id, title, description, starts_at, ends_at, venue,
            poster_url, poster_data_url, capacity, is_ticketed, price,
            status, notify_on_publish, published_at, created_by, updated_by
          )
          values (
            $1,$2,$3,$4,$5,$6,$7,
            $8,$9,$10,$11,$12,$13,$14,case when $13 = 'PUBLISHED' then now() else null end,$15,$15
          )
          returning
            id,
            church_id,
            campus_id,
            title,
            description,
            starts_at,
            ends_at,
            venue,
            poster_url,
            poster_data_url,
            capacity,
            is_ticketed,
            price,
            status,
            notify_on_publish,
            published_at,
            created_at,
            updated_at
        )
        select
          i.id,
          i.church_id as "churchId",
          i.campus_id as "campusId",
          cc.name as "campusName",
          cc.code as "campusCode",
          i.title,
          i.description,
          i.starts_at as "startsAt",
          i.ends_at as "endsAt",
          i.venue,
          i.poster_url as "posterUrl",
          i.poster_data_url as "posterDataUrl",
          i.capacity,
          i.is_ticketed as "isTicketed",
          i.price,
          0::int as "registrationCount",
          i.status,
          i.notify_on_publish as "notifyOnPublish",
          i.published_at as "publishedAt",
          i.created_at as "createdAt",
          i.updated_at as "updatedAt"
        from inserted i
        left join church_campuses cc on cc.id = i.campus_id
        `,
        [
          churchId,
          campusId,
          title,
          description,
          startsAt,
          endsAt,
          venue,
          posterUrl,
          posterData,
          capacity,
          isTicketed,
          price,
          status,
          notifyOnPublish,
          req.user.id,
        ]
      );

      const eventPayload = normalizeChurchEventRow(row);
      let notifications = 0;
      if (eventPayload.status === "PUBLISHED" && eventPayload.notifyOnPublish) {
        notifications = await notifyEventPublished({ churchId, eventRow: eventPayload });
      }

      return res.status(201).json({ ok: true, event: eventPayload, notificationsSent: notifications });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life events are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/events] create error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/church-life/events/:eventId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("events.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const eventId = String(req.params?.eventId || "").trim();
      if (!UUID_REGEX.test(eventId)) return res.status(400).json({ error: "Invalid eventId" });

      const current = await db.oneOrNone("select * from church_events where id=$1 and church_id=$2", [eventId, churchId]);
      if (!current) return res.status(404).json({ error: "Event not found" });

      const title = String(req.body?.title ?? current.title ?? "")
        .trim()
        .slice(0, 160);
      const description =
        String(typeof req.body?.description === "undefined" ? current.description || "" : req.body?.description || "")
          .trim()
          .slice(0, 4000) || null;
      const startsAt = parseChurchLifeDateTimeOrNull(
        typeof req.body?.startsAt === "undefined" ? current.starts_at : req.body?.startsAt
      );
      const endsAt = parseChurchLifeDateTimeOrNull(
        typeof req.body?.endsAt === "undefined" ? current.ends_at : req.body?.endsAt
      );
      const venue =
        String(typeof req.body?.venue === "undefined" ? current.venue || "" : req.body?.venue || "")
          .trim()
          .slice(0, 220) || null;
      const posterUrl =
        String(typeof req.body?.posterUrl === "undefined" ? current.poster_url || "" : req.body?.posterUrl || "")
          .trim()
          .slice(0, 800) || null;
      const posterDataRaw =
        String(typeof req.body?.posterDataUrl === "undefined" ? current.poster_data_url || "" : req.body?.posterDataUrl || "").trim();
      const posterData = posterDataRaw ? posterDataRaw.slice(0, 2_500_000) : null;
      const status = normalizeChurchLifeEventStatus(req.body?.status || current.status || "DRAFT");
      const notifyOnPublish =
        typeof req.body?.notifyOnPublish === "undefined"
          ? current.notify_on_publish !== false
          : toBoolean(req.body?.notifyOnPublish) !== false;
      const rawCapacity = Object.prototype.hasOwnProperty.call(req.body || {}, "capacity") ? req.body?.capacity : current.capacity;
      const parsedCapacity =
        rawCapacity === null || typeof rawCapacity === "undefined" || rawCapacity === "" ? null : parsePositiveInt(rawCapacity, null);
      if (
        !(rawCapacity === null || typeof rawCapacity === "undefined" || rawCapacity === "") &&
        (parsedCapacity === null || parsedCapacity < 1 || parsedCapacity > 200000)
      ) {
        return res.status(400).json({ error: "capacity must be a whole number between 1 and 200000." });
      }
      const capacity = parsedCapacity && parsedCapacity > 0 ? parsedCapacity : null;
      const isTicketed =
        typeof req.body?.isTicketed === "undefined" ? current.is_ticketed === true : toBoolean(req.body?.isTicketed) === true;
      let price =
        typeof req.body?.price === "undefined"
          ? current.price == null
            ? null
            : toCurrencyNumber(Number(current.price))
          : req.body?.price;
      if (price === null || price === "") {
        price = null;
      } else {
        const parsedPrice = Number(price);
        if (!Number.isFinite(parsedPrice) || parsedPrice < 0 || parsedPrice > 100000000) {
          return res.status(400).json({ error: "price must be a valid amount between 0 and 100000000." });
        }
        price = toCurrencyNumber(parsedPrice);
      }
      if (isTicketed && !(Number.isFinite(Number(price)) && Number(price) > 0)) {
        return res.status(400).json({ error: "price is required when isTicketed=true." });
      }
      if (!isTicketed) {
        price = null;
      }
      let campusId = current?.campus_id || null;
      if (Object.prototype.hasOwnProperty.call(req.body || {}, "campusId")) {
        try {
          campusId = await resolveChurchCampusId(churchId, req.body?.campusId, { fieldName: "campusId" });
        } catch (campusErr) {
          return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
        }
      }

      if (!title) return res.status(400).json({ error: "title is required." });
      if (!startsAt) return res.status(400).json({ error: "startsAt is required." });
      if (req.body?.endsAt && !endsAt) return res.status(400).json({ error: "endsAt is invalid." });

      const row = await db.one(
        `
        with updated as (
          update church_events
          set
            campus_id = $3,
            title = $4,
            description = $5,
            starts_at = $6,
            ends_at = $7,
            venue = $8,
            poster_url = $9,
            poster_data_url = $10,
            capacity = $11,
            is_ticketed = $12,
            price = $13,
            status = $14,
            notify_on_publish = $15,
            published_at = case
              when status <> 'PUBLISHED' and $14 = 'PUBLISHED' then now()
              when $14 <> 'PUBLISHED' then null
              else published_at
            end,
            updated_by = $16,
            updated_at = now()
          where id=$1 and church_id=$2
          returning
            id,
            church_id,
            campus_id,
            title,
            description,
            starts_at,
            ends_at,
            venue,
            poster_url,
            poster_data_url,
            capacity,
            is_ticketed,
            price,
            status,
            notify_on_publish,
            published_at,
            created_at,
            updated_at
        )
        select
          u.id,
          u.church_id as "churchId",
          u.campus_id as "campusId",
          cc.name as "campusName",
          cc.code as "campusCode",
          u.title,
          u.description,
          u.starts_at as "startsAt",
          u.ends_at as "endsAt",
          u.venue,
          u.poster_url as "posterUrl",
          u.poster_data_url as "posterDataUrl",
          u.capacity,
          u.is_ticketed as "isTicketed",
          u.price,
          coalesce(reg.count, 0)::int as "registrationCount",
          u.status,
          u.notify_on_publish as "notifyOnPublish",
          u.published_at as "publishedAt",
          u.created_at as "createdAt",
          u.updated_at as "updatedAt"
        from updated u
        left join church_campuses cc on cc.id = u.campus_id
        left join lateral (
          select count(*)::int as count
          from church_event_registrations r
          where r.event_id = u.id
            and r.status = any($17::text[])
        ) reg on true
        `,
        [
          eventId,
          churchId,
          campusId,
          title,
          description,
          startsAt,
          endsAt,
          venue,
          posterUrl,
          posterData,
          capacity,
          isTicketed,
          price,
          status,
          notifyOnPublish,
          req.user.id,
          CHURCH_EVENT_REGISTRATION_ACTIVE_STATUSES,
        ]
      );

      const eventPayload = normalizeChurchEventRow(row);
      const transitionedToPublished = String(current.status || "").toUpperCase() !== "PUBLISHED" && eventPayload.status === "PUBLISHED";
      let notifications = 0;
      if (transitionedToPublished && eventPayload.notifyOnPublish) {
        notifications = await notifyEventPublished({ churchId, eventRow: eventPayload });
      }

      return res.json({ ok: true, event: eventPayload, notificationsSent: notifications });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life events are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/events] patch error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/events/:eventId/registrations",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("events.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const eventId = String(req.params?.eventId || "").trim();
      if (!UUID_REGEX.test(eventId)) return res.status(400).json({ error: "Invalid eventId." });
      const statusFilterRaw = String(req.query?.status || "").trim();
      const statusFilter = statusFilterRaw ? normalizeChurchEventRegistrationStatus(statusFilterRaw, "") : "";
      if (statusFilterRaw && !CHURCH_EVENT_REGISTRATION_STATUSES.has(statusFilter)) {
        return res.status(400).json({ error: "status must be REGISTERED, CHECKED_IN, or CANCELLED." });
      }
      const limit = parseChurchLifeLimit(req.query?.limit, 250, 1000);

      const event = await findChurchEventById(churchId, eventId);
      if (!event) return res.status(404).json({ error: "Event not found." });

      const params = [churchId, eventId];
      let where = "r.church_id = $1 and r.event_id = $2";
      if (statusFilter) {
        params.push(statusFilter);
        where += ` and r.status = $${params.length}`;
      }
      params.push(limit);
      const limitParam = params.length;

      const rows = await db.manyOrNone(
        `
        select
          r.id,
          r.church_id as "churchId",
          r.event_id as "eventId",
          r.member_pk as "memberPk",
          r.member_id as "memberId",
          m.full_name as "memberName",
          m.phone as "memberPhone",
          m.email as "memberEmail",
          r.status,
          r.checked_in_at as "checkedInAt",
          r.canceled_at as "canceledAt",
          r.created_at as "createdAt",
          r.updated_at as "updatedAt"
        from church_event_registrations r
        join members m on m.id = r.member_pk
        where ${where}
        order by r.created_at desc
        limit $${limitParam}
        `,
        params
      );
      const summary = await db.one(
        `
        select
          count(*)::int as total,
          count(*) filter (where status = 'REGISTERED')::int as "registeredCount",
          count(*) filter (where status = 'CHECKED_IN')::int as "checkedInCount",
          count(*) filter (where status = 'CANCELLED')::int as "canceledCount"
        from church_event_registrations
        where church_id = $1 and event_id = $2
        `,
        [churchId, eventId]
      );
      const activeCount = Number(summary?.registeredCount || 0) + Number(summary?.checkedInCount || 0);

      return res.json({
        ok: true,
        event: normalizeChurchEventRow({ ...event, registrationCount: activeCount }),
        registrations: rows.map(normalizeChurchEventRegistrationRow),
        summary: {
          total: Number(summary?.total || 0),
          registeredCount: Number(summary?.registeredCount || 0),
          checkedInCount: Number(summary?.checkedInCount || 0),
          canceledCount: Number(summary?.canceledCount || 0),
        },
        meta: { status: statusFilter || null, limit, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Event registrations are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/events] registrations list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/events/:eventId/registrations/export.csv",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("events.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const eventId = String(req.params?.eventId || "").trim();
      if (!UUID_REGEX.test(eventId)) return res.status(400).json({ error: "Invalid eventId." });
      const event = await findChurchEventById(churchId, eventId);
      if (!event) return res.status(404).json({ error: "Event not found." });

      const rows = await db.manyOrNone(
        `
        select
          r.id,
          r.member_id as "memberId",
          m.full_name as "memberName",
          m.phone as "memberPhone",
          m.email as "memberEmail",
          r.status,
          r.checked_in_at as "checkedInAt",
          r.canceled_at as "canceledAt",
          r.created_at as "createdAt"
        from church_event_registrations r
        join members m on m.id = r.member_pk
        where r.church_id = $1 and r.event_id = $2
        order by r.created_at desc
        `,
        [churchId, eventId]
      );

      const header = ["registrationId", "memberId", "memberName", "memberPhone", "memberEmail", "status", "checkedInAt", "canceledAt", "createdAt"];
      const lines = [header.join(",")];
      rows.forEach((row) => {
        lines.push(
          [
            csvEscape(row?.id),
            csvEscape(row?.memberId),
            csvEscape(row?.memberName),
            csvEscape(row?.memberPhone),
            csvEscape(row?.memberEmail),
            csvEscape(normalizeChurchEventRegistrationStatus(row?.status)),
            csvEscape(row?.checkedInAt),
            csvEscape(row?.canceledAt),
            csvEscape(row?.createdAt),
          ].join(",")
        );
      });
      const fileName = `event-registrations-${String(event?.title || "event").replace(/[^a-z0-9_-]+/gi, "-").toLowerCase()}-${eventId}.csv`;
      res.setHeader("Content-Type", "text/csv; charset=utf-8");
      res.setHeader("Content-Disposition", `attachment; filename=\"${fileName}\"`);
      return res.status(200).send(lines.join("\n"));
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Event registrations are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/events] registrations export error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/church-life/events/:eventId/registrations/:registrationId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("events.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const eventId = String(req.params?.eventId || "").trim();
      const registrationId = String(req.params?.registrationId || "").trim();
      if (!UUID_REGEX.test(eventId) || !UUID_REGEX.test(registrationId)) {
        return res.status(400).json({ error: "Invalid eventId or registrationId." });
      }
      const nextStatus = normalizeChurchEventRegistrationStatus(req.body?.status, "");
      if (!CHURCH_EVENT_REGISTRATION_STATUSES.has(nextStatus)) {
        return res.status(400).json({ error: "status must be REGISTERED, CHECKED_IN, or CANCELLED." });
      }

      const updated = await db.tx(async (t) => {
        const event = await findChurchEventById(churchId, eventId, { tx: t });
        if (!event) throw Object.assign(new Error("Event not found."), { statusCode: 404 });

        const current = await t.oneOrNone(
          `
          select
            r.id,
            r.church_id as "churchId",
            r.event_id as "eventId",
            r.member_pk as "memberPk",
            r.member_id as "memberId",
            r.status,
            r.checked_in_at as "checkedInAt",
            r.canceled_at as "canceledAt",
            r.created_at as "createdAt",
            r.updated_at as "updatedAt"
          from church_event_registrations r
          where r.church_id = $1 and r.event_id = $2 and r.id = $3
          limit 1
          for update
          `,
          [churchId, eventId, registrationId]
        );
        if (!current) throw Object.assign(new Error("Registration not found."), { statusCode: 404 });

        const currentStatus = normalizeChurchEventRegistrationStatus(current?.status || "", "");
        if (
          CHURCH_EVENT_REGISTRATION_ACTIVE_STATUSES.includes(nextStatus) &&
          !CHURCH_EVENT_REGISTRATION_ACTIVE_STATUSES.includes(currentStatus)
        ) {
          const activeCount = await countChurchEventActiveRegistrations(churchId, eventId, { tx: t });
          const capacity = event?.capacity == null ? null : Number(event.capacity);
          if (Number.isFinite(capacity) && capacity > 0 && activeCount >= capacity) {
            throw Object.assign(new Error("This event is full."), { statusCode: 409 });
          }
        }

        const row = await t.one(
          `
          update church_event_registrations
          set
            status = $4,
            checked_in_at = case
              when $4 = 'CHECKED_IN' then coalesce(checked_in_at, now())
              when $4 = 'REGISTERED' then null
              else checked_in_at
            end,
            canceled_at = case
              when $4 = 'CANCELLED' then coalesce(canceled_at, now())
              else null
            end,
            updated_by = $5,
            updated_at = now()
          where church_id = $1 and event_id = $2 and id = $3
          returning
            id,
            church_id as "churchId",
            event_id as "eventId",
            member_pk as "memberPk",
            member_id as "memberId",
            status,
            checked_in_at as "checkedInAt",
            canceled_at as "canceledAt",
            created_at as "createdAt",
            updated_at as "updatedAt"
          `,
          [churchId, eventId, registrationId, nextStatus, req.user?.id || null]
        );
        return row;
      });

      const withMember = await db.oneOrNone(
        `
        select
          r.id,
          r.church_id as "churchId",
          r.event_id as "eventId",
          r.member_pk as "memberPk",
          r.member_id as "memberId",
          m.full_name as "memberName",
          m.phone as "memberPhone",
          m.email as "memberEmail",
          r.status,
          r.checked_in_at as "checkedInAt",
          r.canceled_at as "canceledAt",
          r.created_at as "createdAt",
          r.updated_at as "updatedAt"
        from church_event_registrations r
        join members m on m.id = r.member_pk
        where r.church_id = $1 and r.event_id = $2 and r.id = $3
        limit 1
        `,
        [churchId, eventId, registrationId]
      );

      return res.json({
        ok: true,
        registration: normalizeChurchEventRegistrationRow(withMember || updated),
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Event registrations are not available yet. Run migrations and retry." });
      }
      const statusCode = Number(err?.statusCode || err?.status || 0);
      if (statusCode >= 400 && statusCode < 500) {
        return res.status(statusCode).json({ error: err?.message || "Could not update registration." });
      }
      console.error("[admin/church-life/events] registration patch error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  [
    "/admin/church-life/member-profiles/import/preview",
    "/admin/church-life/import/members/upload",
    "/admin/church-life/import/members/preview",
  ],
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations", "members"),
  requireChurchGrowthActive,
  requireChurchLifePermission("profiles.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const fileName = String(req.body?.fileName || "").trim();
      const fileBase64 = String(req.body?.fileBase64 || "").trim();
      const mimeType = String(req.body?.mimeType || "").trim();
      const sheetName = String(req.body?.sheetName || "").trim() || null;
      const duplicateMode = normalizeMemberImportDuplicateMode(req.body?.duplicateMode);

      if (!fileBase64) return res.status(400).json({ error: "Import file payload is required." });

      const parsed = parseMemberImportFileTable({
        fileName,
        fileBase64,
        mimeType,
        sheetName,
      });
      if (parsed.rows.length > CHURCH_MEMBER_IMPORT_MAX_ROWS) {
        return res.status(400).json({
          error: `Import is limited to ${CHURCH_MEMBER_IMPORT_MAX_ROWS} rows per file.`,
        });
      }

      const { mapping, recommendedMapping } = normalizeMemberImportMapping(req.body?.mapping, parsed.headers);
      const preparedRows = prepareMemberImportRows(parsed.rows, mapping, duplicateMode);
      const lookup = await loadMemberImportExistingLookup(churchId, preparedRows);
      resolvePreparedMemberImportActions(preparedRows, lookup, duplicateMode);

      const summary = summarizePreparedMemberImportRows(preparedRows);
      summary.willImport = summary.willCreate + summary.willUpdate;

      return res.json({
        ok: true,
        file: {
          name: fileName || null,
          mimeType: mimeType || null,
          fileType: parsed.fileType,
          rowCount: parsed.rows.length,
          headerCount: parsed.headers.length,
        },
        duplicateMode,
        headers: parsed.headers,
        fields: CHURCH_MEMBER_IMPORT_TARGET_FIELDS,
        mapping,
        recommendedMapping,
        summary,
        rows: preparedRows.slice(0, 400).map(projectPreparedImportRowForResponse),
        rowsTruncated: preparedRows.length > 400,
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church CRM member imports are not available yet. Run migrations and retry." });
      }
      const message = normalizeMemberImportExecutionError(err);
      if (message && message !== "Import failed.") {
        return res.status(400).json({ error: message });
      }
      console.error("[admin/church-life/member-profiles/import] preview error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  [
    "/admin/church-life/member-profiles/import/execute",
    "/admin/church-life/import/members/execute",
  ],
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations", "members"),
  requireChurchGrowthActive,
  requireChurchLifePermission("profiles.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const fileName = String(req.body?.fileName || "").trim();
      const fileBase64 = String(req.body?.fileBase64 || "").trim();
      const mimeType = String(req.body?.mimeType || "").trim();
      const sheetName = String(req.body?.sheetName || "").trim() || null;
      const duplicateMode = normalizeMemberImportDuplicateMode(req.body?.duplicateMode);
      if (!fileBase64) return res.status(400).json({ error: "Import file payload is required." });

      const parsed = parseMemberImportFileTable({
        fileName,
        fileBase64,
        mimeType,
        sheetName,
      });
      if (parsed.rows.length > CHURCH_MEMBER_IMPORT_MAX_ROWS) {
        return res.status(400).json({
          error: `Import is limited to ${CHURCH_MEMBER_IMPORT_MAX_ROWS} rows per file.`,
        });
      }

      const { mapping, recommendedMapping } = normalizeMemberImportMapping(req.body?.mapping, parsed.headers);
      const preparedRows = prepareMemberImportRows(parsed.rows, mapping, duplicateMode);
      const lookup = await loadMemberImportExistingLookup(churchId, preparedRows);
      resolvePreparedMemberImportActions(preparedRows, lookup, duplicateMode);

      const actorId = req.user?.id || null;
      const placeholderPasswordHash = await bcrypt.hash(crypto.randomUUID(), 10);
      let createdCount = 0;
      let updatedCount = 0;
      let skippedCount = 0;
      let failedCount = 0;
      let warningsCount = 0;

      for (const row of preparedRows) {
        warningsCount += Array.isArray(row?.warnings) ? row.warnings.length : 0;
        try {
          if (row.action === "CREATE") {
            const created = await createChurchMemberFromImportRow({
              churchId,
              row,
              actorId,
              placeholderPasswordHash,
            });
            row.reason = "Member created.";
            row.resultMember = created;
            createdCount += 1;
            continue;
          }
          if (row.action === "UPDATE") {
            const updated = await updateChurchMemberFromImportRow({ churchId, row, actorId });
            row.reason = "Member updated.";
            row.resultMember = updated;
            updatedCount += 1;
            continue;
          }
          if (String(row.action || "").startsWith("SKIP")) {
            skippedCount += 1;
            continue;
          }
          if (row.action === "ERROR") {
            failedCount += 1;
            continue;
          }
        } catch (err) {
          const reason = normalizeMemberImportExecutionError(err);
          row.action = "ERROR";
          row.reason = reason;
          if (!Array.isArray(row.errors)) row.errors = [];
          row.errors.push(reason);
          failedCount += 1;
        }
      }

      if (createdCount > 0) {
        try {
          await ensureChurchMemberIdentifiers(churchId);
        } catch (err) {
          if (err?.code !== "42P01" && err?.code !== "42703") throw err;
        }
      }

      const nonSuccessRows = preparedRows.filter((row) => row.action !== "CREATE" && row.action !== "UPDATE");
      const errorReportCsv = nonSuccessRows.length ? buildMemberImportErrorCsv(nonSuccessRows) : "";
      const summary = {
        totalRows: preparedRows.length,
        created: createdCount,
        updated: updatedCount,
        skipped: skippedCount,
        failed: failedCount,
        warnings: warningsCount,
      };
      summary.imported = summary.created + summary.updated;

      return res.json({
        ok: true,
        file: {
          name: fileName || null,
          mimeType: mimeType || null,
          fileType: parsed.fileType,
          rowCount: parsed.rows.length,
          headerCount: parsed.headers.length,
        },
        duplicateMode,
        headers: parsed.headers,
        fields: CHURCH_MEMBER_IMPORT_TARGET_FIELDS,
        mapping,
        recommendedMapping,
        summary,
        rows: preparedRows.slice(0, 400).map((row) => ({
          ...projectPreparedImportRowForResponse(row),
          resultMember: row?.resultMember
            ? {
                id: row.resultMember.id || null,
                memberId: row.resultMember.memberId || null,
                fullName: row.resultMember.fullName || null,
                phone: row.resultMember.phone || null,
                email: row.resultMember.email || null,
              }
            : null,
        })),
        rowsTruncated: preparedRows.length > 400,
        errorReport:
          nonSuccessRows.length > 0
            ? {
                fileName: `member-import-errors-${Date.now()}.csv`,
                contentType: "text/csv",
                rowCount: nonSuccessRows.length,
                base64: Buffer.from(errorReportCsv, "utf8").toString("base64"),
              }
            : null,
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church CRM member imports are not available yet. Run migrations and retry." });
      }
      const message = normalizeMemberImportExecutionError(err);
      if (message && message !== "Import failed.") {
        return res.status(400).json({ error: message });
      }
      console.error("[admin/church-life/member-profiles/import] execute error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/member-profiles",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations", "members"),
  requireChurchGrowthActive,
  requireChurchLifePermission("profiles.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const search = String(req.query?.search || "").trim();
      const searchLike = `%${search}%`;
      const tag = String(req.query?.tag || "").trim().toLowerCase();
      const limit = parseChurchLifeLimit(req.query?.limit, 80, 300);
      const offset = parseChurchLifeOffset(req.query?.offset);

      const rows = await db.manyOrNone(
        `
        select
          m.id as "memberPk",
          m.church_id as "churchId",
          m.member_id as "memberId",
          m.full_name as "fullName",
          m.phone,
          m.email,
          m.role,
          m.date_of_birth as "dateOfBirth",
          m.created_at as "memberCreatedAt",
          p.household_name as "householdName",
          p.household_role as "householdRole",
          p.address_line1 as "addressLine1",
          p.address_line2 as "addressLine2",
          p.suburb,
          p.city,
          p.province,
          p.postal_code as "postalCode",
          p.country,
          p.alternate_phone as "alternatePhone",
          p.whatsapp_number as "whatsappNumber",
          p.occupation,
          p.emergency_contact_name as "emergencyContactName",
          p.emergency_contact_phone as "emergencyContactPhone",
          p.emergency_contact_relation as "emergencyContactRelation",
          p.ministry_tags as "ministryTags",
          p.join_date as "joinDate",
          p.consent_data as "consentData",
          p.consent_contact as "consentContact",
          p.consent_updated_at as "consentUpdatedAt",
          p.baptism_status as "baptismStatus",
          p.notes,
          p.updated_at as "updatedAt"
        from members m
        left join church_member_profiles p
          on p.church_id = m.church_id and p.member_pk = m.id
        where m.church_id = $1
          and (
            $2 = ''
            or m.member_id ilike $3
            or m.full_name ilike $3
            or m.phone ilike $3
            or coalesce(m.email, '') ilike $3
          )
          and (
            $4 = ''
            or exists (
              select 1
              from jsonb_array_elements_text(coalesce(p.ministry_tags, '[]'::jsonb)) as tag_item(value)
              where lower(tag_item.value) = $4
            )
          )
        order by m.created_at asc, m.id asc
        limit $5 offset $6
        `,
        [churchId, search, searchLike, tag, limit, offset]
      );
      const access = getChurchLifeAccess(req);
      const memberPks = rows
        .map((row) => String(row?.memberPk || "").trim())
        .filter((memberPk) => UUID_REGEX.test(memberPk));
      const childrenCountsByMemberPk = new Map();

      if (memberPks.length) {
        try {
          const childRows = await db.manyOrNone(
            `
            select
              parent_member_pk as "memberPk",
              count(*)::int as "childrenCount"
            from church_household_children
            where church_id = $1
              and active = true
              and parent_member_pk = any($2::uuid[])
            group by parent_member_pk
            `,
            [churchId, memberPks]
          );
          childRows.forEach((row) => {
            const key = String(row?.memberPk || "").trim();
            if (!key) return;
            childrenCountsByMemberPk.set(key, Number(row?.childrenCount || 0));
          });
        } catch (err) {
          if (err?.code !== "42P01" && err?.code !== "42703") throw err;
        }
      }

      return res.json({
        ok: true,
        profiles: rows.map((row) => {
          const memberPk = String(row?.memberPk || "").trim();
          return redactChurchMemberProfile(
            normalizeChurchMemberProfileRow({
              ...row,
              childrenCount: childrenCountsByMemberPk.get(memberPk) || 0,
            }),
            access
          );
        }),
        meta: { search, tag: tag || null, limit, offset, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church CRM member profiles are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/member-profiles] list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.put(
  "/admin/church-life/member-profiles/:memberPk",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations", "members"),
  requireChurchGrowthActive,
  requireChurchLifePermission("profiles.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const access = getChurchLifeAccess(req);

      const memberPk = String(req.params?.memberPk || "").trim();
      if (!UUID_REGEX.test(memberPk)) return res.status(400).json({ error: "Invalid memberPk" });

      const member = await findMemberInChurchByUuid(churchId, memberPk);
      if (!member) return res.status(404).json({ error: "Member not found in this church." });

      const existing = await db.oneOrNone(
        `
        select
          household_name as "householdName",
          household_role as "householdRole",
          address_line1 as "addressLine1",
          address_line2 as "addressLine2",
          suburb,
          city,
          province,
          postal_code as "postalCode",
          country,
          alternate_phone as "alternatePhone",
          whatsapp_number as "whatsappNumber",
          occupation,
          emergency_contact_name as "emergencyContactName",
          emergency_contact_phone as "emergencyContactPhone",
          emergency_contact_relation as "emergencyContactRelation",
          ministry_tags as "ministryTags",
          join_date as "joinDate",
          consent_data as "consentData",
          consent_contact as "consentContact",
          consent_updated_at as "consentUpdatedAt",
          baptism_status as "baptismStatus",
          notes
        from church_member_profiles
        where church_id=$1 and member_pk=$2
        limit 1
        `,
        [churchId, memberPk]
      );
      const beforeSnapshot = buildChurchMemberProfileAuditSnapshot(existing);

      const householdNameInput = req.body?.householdName;
      const householdRoleInput = req.body?.householdRole;
      const addressLine1Input = req.body?.addressLine1;
      const addressLine2Input = req.body?.addressLine2;
      const suburbInput = req.body?.suburb;
      const cityInput = req.body?.city;
      const provinceInput = req.body?.province;
      const postalCodeInput = req.body?.postalCode;
      const countryInput = req.body?.country;
      const alternatePhoneInput = req.body?.alternatePhone;
      const whatsappNumberInput = req.body?.whatsappNumber;
      const occupationInput = req.body?.occupation;
      const emergencyContactNameInput = req.body?.emergencyContactName;
      const emergencyContactPhoneInput = req.body?.emergencyContactPhone;
      const emergencyContactRelationInput = req.body?.emergencyContactRelation;
      const ministryTagsInput = req.body?.ministryTags;
      const joinDateInput = req.body?.joinDate;
      const consentDataInput = req.body?.consentData;
      const consentContactInput = req.body?.consentContact;
      const notesInput = req.body?.notes;
      const baptismStatusInput = req.body?.baptismStatus;
      const normalizeProfileText = (value, maxLen) => String(value || "").trim().slice(0, maxLen) || null;

      const householdName =
        typeof householdNameInput === "undefined"
          ? existing?.householdName || null
          : normalizeProfileText(householdNameInput, 120);
      const householdRole =
        typeof householdRoleInput === "undefined"
          ? existing?.householdRole || null
          : normalizeProfileText(householdRoleInput, 80);
      const addressLine1 =
        typeof addressLine1Input === "undefined"
          ? existing?.addressLine1 || null
          : normalizeProfileText(addressLine1Input, 180);
      const addressLine2 =
        typeof addressLine2Input === "undefined"
          ? existing?.addressLine2 || null
          : normalizeProfileText(addressLine2Input, 180);
      const suburb =
        typeof suburbInput === "undefined" ? existing?.suburb || null : normalizeProfileText(suburbInput, 120);
      const city = typeof cityInput === "undefined" ? existing?.city || null : normalizeProfileText(cityInput, 120);
      const province =
        typeof provinceInput === "undefined" ? existing?.province || null : normalizeProfileText(provinceInput, 120);
      const postalCode =
        typeof postalCodeInput === "undefined"
          ? existing?.postalCode || null
          : normalizeProfileText(postalCodeInput, 40);
      const country =
        typeof countryInput === "undefined" ? existing?.country || null : normalizeProfileText(countryInput, 120);
      const alternatePhone =
        typeof alternatePhoneInput === "undefined"
          ? existing?.alternatePhone || null
          : normalizeProfileText(alternatePhoneInput, 40);
      const whatsappNumber =
        typeof whatsappNumberInput === "undefined"
          ? existing?.whatsappNumber || null
          : normalizeProfileText(whatsappNumberInput, 40);
      const occupation =
        typeof occupationInput === "undefined"
          ? existing?.occupation || null
          : normalizeProfileText(occupationInput, 120);
      const emergencyContactName =
        typeof emergencyContactNameInput === "undefined"
          ? existing?.emergencyContactName || null
          : normalizeProfileText(emergencyContactNameInput, 140);
      const emergencyContactPhone =
        typeof emergencyContactPhoneInput === "undefined"
          ? existing?.emergencyContactPhone || null
          : normalizeProfileText(emergencyContactPhoneInput, 40);
      const emergencyContactRelation =
        typeof emergencyContactRelationInput === "undefined"
          ? existing?.emergencyContactRelation || null
          : normalizeProfileText(emergencyContactRelationInput, 60);
      const ministryTags =
        typeof ministryTagsInput === "undefined"
          ? normalizeStringArray(existing?.ministryTags || [])
          : normalizeStringArray(ministryTagsInput, { maxItems: 30, maxLen: 60 });
      const joinDate = (() => {
        if (typeof joinDateInput === "undefined") {
          const existingJoinDate = formatDateIsoLike(existing?.joinDate);
          return existingJoinDate || null;
        }
        if (joinDateInput === null || joinDateInput === "") return null;
        return parseChurchLifeDate(joinDateInput);
      })();
      if (typeof joinDateInput !== "undefined" && joinDateInput !== null && joinDateInput !== "" && !joinDate) {
        return res.status(400).json({ error: "joinDate must be YYYY-MM-DD." });
      }

      const consentData =
        typeof consentDataInput === "undefined" ? existing?.consentData === true : toBoolean(consentDataInput) === true;
      const consentContact =
        typeof consentContactInput === "undefined"
          ? existing?.consentContact === true
          : toBoolean(consentContactInput) === true;
      const notes =
        typeof notesInput === "undefined" ? existing?.notes || null : normalizeProfileText(notesInput, 2000);
      const baptismStatus =
        typeof baptismStatusInput === "undefined"
          ? normalizeChurchMemberBaptismStatus(existing?.baptismStatus, "UNKNOWN")
          : normalizeChurchMemberBaptismStatus(baptismStatusInput, "UNKNOWN");
      const hasConsentFieldUpdate = typeof consentDataInput !== "undefined" || typeof consentContactInput !== "undefined";
      if (hasConsentFieldUpdate && !hasChurchLifePermission(access, "profiles.consent.write")) {
        return res.status(403).json({
          error: "You do not have permission to update consent fields.",
          code: CHURCH_LIFE_PERMISSION_DENIED_CODE,
          role: access.role,
          permission: "profiles.consent.write",
        });
      }
      const consentUpdatedAt = hasConsentFieldUpdate ? new Date().toISOString() : existing?.consentUpdatedAt || null;

      const row = await db.one(
        `
        insert into church_member_profiles (
          church_id,
          member_pk,
          member_id,
          household_name,
          household_role,
          address_line1,
          address_line2,
          suburb,
          city,
          province,
          postal_code,
          country,
          alternate_phone,
          whatsapp_number,
          occupation,
          emergency_contact_name,
          emergency_contact_phone,
          emergency_contact_relation,
          ministry_tags,
          join_date,
          consent_data,
          consent_contact,
          consent_updated_at,
          baptism_status,
          notes,
          created_by,
          updated_by
        )
        values (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19::jsonb,$20,$21,$22,$23,$24,$25,$26,$26
        )
        on conflict (church_id, member_pk)
        do update set
          member_id = excluded.member_id,
          household_name = excluded.household_name,
          household_role = excluded.household_role,
          address_line1 = excluded.address_line1,
          address_line2 = excluded.address_line2,
          suburb = excluded.suburb,
          city = excluded.city,
          province = excluded.province,
          postal_code = excluded.postal_code,
          country = excluded.country,
          alternate_phone = excluded.alternate_phone,
          whatsapp_number = excluded.whatsapp_number,
          occupation = excluded.occupation,
          emergency_contact_name = excluded.emergency_contact_name,
          emergency_contact_phone = excluded.emergency_contact_phone,
          emergency_contact_relation = excluded.emergency_contact_relation,
          ministry_tags = excluded.ministry_tags,
          join_date = excluded.join_date,
          consent_data = excluded.consent_data,
          consent_contact = excluded.consent_contact,
          consent_updated_at = excluded.consent_updated_at,
          baptism_status = excluded.baptism_status,
          notes = excluded.notes,
          updated_by = excluded.updated_by,
          updated_at = now()
        returning
          church_id as "churchId",
          member_pk as "memberPk",
          member_id as "memberId",
          household_name as "householdName",
          household_role as "householdRole",
          address_line1 as "addressLine1",
          address_line2 as "addressLine2",
          suburb,
          city,
          province,
          postal_code as "postalCode",
          country,
          alternate_phone as "alternatePhone",
          whatsapp_number as "whatsappNumber",
          occupation,
          emergency_contact_name as "emergencyContactName",
          emergency_contact_phone as "emergencyContactPhone",
          emergency_contact_relation as "emergencyContactRelation",
          ministry_tags as "ministryTags",
          join_date as "joinDate",
          consent_data as "consentData",
          consent_contact as "consentContact",
          consent_updated_at as "consentUpdatedAt",
          baptism_status as "baptismStatus",
          notes,
          updated_at as "updatedAt"
        `,
        [
          churchId,
          memberPk,
          member.memberId || null,
          householdName,
          householdRole,
          addressLine1,
          addressLine2,
          suburb,
          city,
          province,
          postalCode,
          country,
          alternatePhone,
          whatsappNumber,
          occupation,
          emergencyContactName,
          emergencyContactPhone,
          emergencyContactRelation,
          JSON.stringify(ministryTags),
          joinDate,
          consentData,
          consentContact,
          consentUpdatedAt,
          baptismStatus,
          notes,
          req.user?.id || null,
        ]
      );
      const afterSnapshot = buildChurchMemberProfileAuditSnapshot(row);
      await writeChurchLifeAuditLog({
        churchId,
        actorMemberId: req.user?.id || null,
        actorRole: req.user?.role || null,
        action: existing ? "PROFILE_UPDATED" : "PROFILE_CREATED",
        entityType: "MEMBER_PROFILE",
        entityId: memberPk,
        entityRef: member.memberId || null,
        before: beforeSnapshot,
        after: afterSnapshot,
        meta: {
          memberPk,
          memberId: member.memberId || null,
          memberName: member.fullName || null,
        },
      });

      const reloaded = await db.oneOrNone(
        `
        select
          m.id as "memberPk",
          m.church_id as "churchId",
          m.member_id as "memberId",
          m.full_name as "fullName",
          m.phone,
          m.email,
          m.role,
          m.date_of_birth as "dateOfBirth",
          m.created_at as "memberCreatedAt",
          p.household_name as "householdName",
          p.household_role as "householdRole",
          p.address_line1 as "addressLine1",
          p.address_line2 as "addressLine2",
          p.suburb,
          p.city,
          p.province,
          p.postal_code as "postalCode",
          p.country,
          p.alternate_phone as "alternatePhone",
          p.whatsapp_number as "whatsappNumber",
          p.occupation,
          p.emergency_contact_name as "emergencyContactName",
          p.emergency_contact_phone as "emergencyContactPhone",
          p.emergency_contact_relation as "emergencyContactRelation",
          p.ministry_tags as "ministryTags",
          p.join_date as "joinDate",
          p.consent_data as "consentData",
          p.consent_contact as "consentContact",
          p.consent_updated_at as "consentUpdatedAt",
          p.baptism_status as "baptismStatus",
          p.notes,
          p.updated_at as "updatedAt"
        from members m
        left join church_member_profiles p
          on p.church_id = m.church_id and p.member_pk = m.id
        where m.church_id = $1 and m.id = $2
        limit 1
        `,
        [churchId, memberPk]
      );

      let childrenCount = 0;
      try {
        const childCountRow = await db.oneOrNone(
          `
          select count(*)::int as "childrenCount"
          from church_household_children
          where church_id = $1
            and parent_member_pk = $2
            and active = true
          `,
          [churchId, memberPk]
        );
        childrenCount = Number(childCountRow?.childrenCount || 0);
      } catch (childErr) {
        if (childErr?.code !== "42P01" && childErr?.code !== "42703") throw childErr;
      }

      const profileRow = reloaded
        ? normalizeChurchMemberProfileRow({
            ...reloaded,
            childrenCount,
          })
        : normalizeChurchMemberProfileRow({
            ...row,
            fullName: member.fullName,
            phone: member.phone,
            email: member.email,
            role: member.role,
            dateOfBirth: member.dateOfBirth,
            memberCreatedAt: member.createdAt,
            childrenCount: 0,
          });

      return res.json({
        ok: true,
        profile: redactChurchMemberProfile(profileRow, access),
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church CRM member profiles are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/member-profiles] save error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

const CHURCH_PROFILE_ADDRESS_FIELD_META = [
  { key: "addressLine1", column: "address_line1", maxLen: 180 },
  { key: "addressLine2", column: "address_line2", maxLen: 180 },
  { key: "suburb", column: "suburb", maxLen: 120 },
  { key: "city", column: "city", maxLen: 120 },
  { key: "province", column: "province", maxLen: 120 },
  { key: "postalCode", column: "postal_code", maxLen: 40 },
  { key: "country", column: "country", maxLen: 120 },
];

function normalizeChurchProfileAddressValue(value, maxLen) {
  return String(value || "")
    .trim()
    .slice(0, maxLen) || null;
}

function normalizeChurchProfileAddressCompareValue(value, maxLen) {
  const normalized = normalizeChurchProfileAddressValue(value, maxLen);
  return normalized ? normalized.toLowerCase() : null;
}

function toChurchProfileAddressObject(row) {
  const out = {};
  for (const field of CHURCH_PROFILE_ADDRESS_FIELD_META) {
    out[field.key] = normalizeChurchProfileAddressValue(row?.[field.key], field.maxLen);
  }
  return out;
}

function hasChurchProfileAddressData(address) {
  return CHURCH_PROFILE_ADDRESS_FIELD_META.some((field) => Boolean(normalizeChurchProfileAddressValue(address?.[field.key], field.maxLen)));
}

function areChurchProfileAddressesEqual(leftAddress, rightAddress) {
  return CHURCH_PROFILE_ADDRESS_FIELD_META.every((field) => {
    const left = normalizeChurchProfileAddressCompareValue(leftAddress?.[field.key], field.maxLen);
    const right = normalizeChurchProfileAddressCompareValue(rightAddress?.[field.key], field.maxLen);
    return left === right;
  });
}

function isChurchHouseholdAddressCompatible(parentAddress, candidateAddress) {
  const parentHasAddress = hasChurchProfileAddressData(parentAddress);
  const candidateHasAddress = hasChurchProfileAddressData(candidateAddress);
  if (!parentHasAddress || !candidateHasAddress) return true;
  return areChurchProfileAddressesEqual(parentAddress, candidateAddress);
}

function mergeChurchProfileAddressObjects(targetAddress, sourceAddress, { overwrite = false } = {}) {
  const next = {};
  const changedFields = [];
  for (const field of CHURCH_PROFILE_ADDRESS_FIELD_META) {
    const currentValue = normalizeChurchProfileAddressValue(targetAddress?.[field.key], field.maxLen);
    const sourceValue = normalizeChurchProfileAddressValue(sourceAddress?.[field.key], field.maxLen);
    const mergedValue = overwrite ? sourceValue : currentValue || sourceValue;
    next[field.key] = mergedValue || null;
    if (currentValue !== (mergedValue || null)) {
      changedFields.push(field.key);
    }
  }
  return { nextAddress: next, changedFields };
}

async function getChurchMemberProfileAddress(churchId, memberPk) {
  return db.oneOrNone(
    `
    select
      address_line1 as "addressLine1",
      address_line2 as "addressLine2",
      suburb,
      city,
      province,
      postal_code as "postalCode",
      country
    from church_member_profiles
    where church_id = $1 and member_pk = $2
    limit 1
    `,
    [churchId, memberPk]
  );
}

async function upsertChurchMemberProfileAddress({
  churchId,
  memberPk,
  memberId = null,
  address = {},
  actorId = null,
} = {}) {
  const normalizedAddress = toChurchProfileAddressObject(address);
  return db.one(
    `
    insert into church_member_profiles (
      church_id,
      member_pk,
      member_id,
      address_line1,
      address_line2,
      suburb,
      city,
      province,
      postal_code,
      country,
      created_by,
      updated_by
    )
    values (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11
    )
    on conflict (church_id, member_pk) do update
    set
      member_id = excluded.member_id,
      address_line1 = excluded.address_line1,
      address_line2 = excluded.address_line2,
      suburb = excluded.suburb,
      city = excluded.city,
      province = excluded.province,
      postal_code = excluded.postal_code,
      country = excluded.country,
      updated_by = excluded.updated_by,
      updated_at = now()
    returning
      address_line1 as "addressLine1",
      address_line2 as "addressLine2",
      suburb,
      city,
      province,
      postal_code as "postalCode",
      country
    `,
    [
      churchId,
      memberPk,
      memberId,
      normalizedAddress.addressLine1,
      normalizedAddress.addressLine2,
      normalizedAddress.suburb,
      normalizedAddress.city,
      normalizedAddress.province,
      normalizedAddress.postalCode,
      normalizedAddress.country,
      actorId,
    ]
  );
}

async function syncChurchHouseholdAddressFromLinkedMember({
  churchId,
  targetMemberPk,
  sourceMemberPk,
  actorId = null,
  overwrite = false,
} = {}) {
  if (!UUID_REGEX.test(String(targetMemberPk || "")) || !UUID_REGEX.test(String(sourceMemberPk || ""))) {
    return { updated: false, copiedFields: [], reason: "invalid_member_reference" };
  }
  if (String(targetMemberPk) === String(sourceMemberPk)) {
    return { updated: false, copiedFields: [], reason: "same_member" };
  }

  const targetMember = await findMemberInChurchByUuid(churchId, targetMemberPk);
  if (!targetMember) return { updated: false, copiedFields: [], reason: "target_member_not_found" };

  const [sourceProfile, targetProfile] = await Promise.all([
    getChurchMemberProfileAddress(churchId, sourceMemberPk),
    getChurchMemberProfileAddress(churchId, targetMemberPk),
  ]);

  const sourceAddress = toChurchProfileAddressObject(sourceProfile || {});
  if (!hasChurchProfileAddressData(sourceAddress)) {
    return { updated: false, copiedFields: [], reason: "source_address_missing" };
  }

  const currentAddress = toChurchProfileAddressObject(targetProfile || {});
  const { nextAddress, changedFields } = mergeChurchProfileAddressObjects(currentAddress, sourceAddress, { overwrite });
  if (!changedFields.length) {
    return { updated: false, copiedFields: [], reason: "no_changes" };
  }

  const saved = await upsertChurchMemberProfileAddress({
    churchId,
    memberPk: targetMemberPk,
    memberId: targetMember.memberId || null,
    address: nextAddress,
    actorId: actorId || null,
  });

  return {
    updated: true,
    copiedFields: changedFields,
    address: toChurchProfileAddressObject(saved || {}),
  };
}

async function resolveChurchHouseholdAddressCompatibility({
  churchId,
  parentMemberPk,
  childMemberPk,
} = {}) {
  if (!UUID_REGEX.test(String(parentMemberPk || "")) || !UUID_REGEX.test(String(childMemberPk || ""))) {
    return {
      parentHasAddress: false,
      childHasAddress: false,
      mismatch: false,
      syncDirection: null,
    };
  }

  const [parentProfile, childProfile] = await Promise.all([
    getChurchMemberProfileAddress(churchId, parentMemberPk),
    getChurchMemberProfileAddress(churchId, childMemberPk),
  ]);

  const parentAddress = toChurchProfileAddressObject(parentProfile || {});
  const childAddress = toChurchProfileAddressObject(childProfile || {});
  const parentHasAddress = hasChurchProfileAddressData(parentAddress);
  const childHasAddress = hasChurchProfileAddressData(childAddress);
  const mismatch = parentHasAddress && childHasAddress && !areChurchProfileAddressesEqual(parentAddress, childAddress);
  const syncDirection = parentHasAddress && !childHasAddress
    ? "parent_to_child"
    : !parentHasAddress && childHasAddress
      ? "child_to_parent"
      : null;

  return {
    parentHasAddress,
    childHasAddress,
    mismatch,
    syncDirection,
  };
}

async function syncChurchHouseholdAddressByDirection({
  churchId,
  parentMemberPk,
  childMemberPk,
  direction,
  actorId = null,
  overwrite = false,
} = {}) {
  if (direction === "parent_to_child") {
    return syncChurchHouseholdAddressFromLinkedMember({
      churchId,
      targetMemberPk: childMemberPk,
      sourceMemberPk: parentMemberPk,
      actorId,
      overwrite,
    });
  }
  if (direction === "child_to_parent") {
    return syncChurchHouseholdAddressFromLinkedMember({
      churchId,
      targetMemberPk: parentMemberPk,
      sourceMemberPk: childMemberPk,
      actorId,
      overwrite,
    });
  }
  return { updated: false, copiedFields: [] };
}

async function listChurchHouseholdMemberCandidates({
  churchId,
  parentMemberPk,
  search = "",
  limit = 20,
} = {}) {
  const safeLimit = parseChurchLifeLimit(limit, 20, 200);
  const normalizedSearch = String(search || "").trim();
  const likeSearch = normalizedSearch ? `%${normalizedSearch.replace(/[%_\\]/g, "\\$&")}%` : null;
  return db.manyOrNone(
    `
    select
      m.id as "memberPk",
      m.member_id as "memberId",
      m.full_name as "fullName",
      m.phone,
      m.email,
      m.role,
      m.date_of_birth as "dateOfBirth",
      p.household_name as "householdName",
      p.household_role as "householdRole",
      p.address_line1 as "addressLine1",
      p.address_line2 as "addressLine2",
      p.suburb,
      p.city,
      p.province,
      p.postal_code as "postalCode",
      p.country,
      exists (
        select 1
        from church_household_children c
        where c.church_id = m.church_id
          and c.parent_member_pk = $2
          and c.child_member_pk = m.id
          and c.active = true
      ) as "alreadyLinked"
    from members m
    left join church_member_profiles p
      on p.church_id = m.church_id
     and p.member_pk = m.id
    where m.church_id = $1
      and m.id <> $2
      and (
        $3::text is null
        or m.member_id ilike $4 escape '\\'
        or m.full_name ilike $4 escape '\\'
        or coalesce(m.phone, '') ilike $4 escape '\\'
        or coalesce(m.email, '') ilike $4 escape '\\'
      )
    order by
      case
        when $3::text is null then 1
        when lower(coalesce(m.member_id, '')) = lower($3::text) then 0
        when lower(coalesce(m.phone, '')) = lower($3::text) then 0
        when lower(coalesce(m.email, '')) = lower($3::text) then 0
        when lower(coalesce(m.full_name, '')) = lower($3::text) then 0
        else 1
      end asc,
      m.full_name asc nulls last,
      m.member_id asc nulls last,
      m.created_at desc
    limit $5
    `,
    [churchId, parentMemberPk, normalizedSearch || null, likeSearch, safeLimit]
  );
}

async function resolveChurchFamilyRelatedMember({
  churchId,
  memberPk,
  relatedMemberPkInput,
  relatedMemberRefInput,
} = {}) {
  const ownerMemberPk = String(memberPk || "").trim();
  const relatedPkRaw = String(relatedMemberPkInput || "").trim();
  const relatedRefRaw = String(relatedMemberRefInput || "").trim();

  let byPk = null;
  if (relatedPkRaw) {
    if (!UUID_REGEX.test(relatedPkRaw)) {
      return { error: "relatedMemberPk must be a valid UUID.", status: 400 };
    }
    if (relatedPkRaw === ownerMemberPk) {
      return { error: "A member cannot be linked to themselves.", status: 400 };
    }
    byPk = await findMemberInChurchByUuid(churchId, relatedPkRaw);
    if (!byPk) return { error: "relatedMemberPk is not in this church.", status: 400 };
  }

  let byRef = null;
  if (relatedRefRaw) {
    byRef = await findChurchMemberByReference(churchId, relatedRefRaw);
    if (!byRef) return { error: "relatedMemberRef is not in this church.", status: 400 };
    if (String(byRef.id) === ownerMemberPk) {
      return { error: "A member cannot be linked to themselves.", status: 400 };
    }
  }

  if (byPk && byRef && String(byPk.id) !== String(byRef.id)) {
    return { error: "relatedMemberPk and relatedMemberRef must refer to the same member.", status: 400 };
  }

  const relatedMember = byPk || byRef || null;
  if (!relatedMember) {
    return { error: "relatedMemberPk or relatedMemberRef is required.", status: 400 };
  }
  return { relatedMember };
}

async function upsertChurchFamilyRelationshipEdge({
  tx,
  churchId,
  pairKey,
  memberPk,
  relatedMemberPk,
  relationshipType,
  notes = null,
  source = "MANUAL",
  actorId = null,
} = {}) {
  const relationship = normalizeChurchFamilyRelationshipType(relationshipType, "OTHER");
  const sourceKey = String(source || "MANUAL")
    .trim()
    .toUpperCase();
  return tx.one(
    `
    insert into church_family_relationships (
      church_id,
      pair_key,
      member_pk,
      related_member_pk,
      relationship_type,
      notes,
      source,
      active,
      created_by,
      updated_by
    )
    values (
      $1,$2,$3,$4,$5,$6,$7,true,$8,$8
    )
    on conflict (church_id, member_pk, related_member_pk, relationship_type)
    do update set
      pair_key = excluded.pair_key,
      notes = excluded.notes,
      source = excluded.source,
      active = true,
      updated_by = excluded.updated_by,
      updated_at = now()
    returning
      id,
      pair_key as "pairKey",
      church_id as "churchId",
      member_pk as "memberPk",
      related_member_pk as "relatedMemberPk",
      relationship_type as "relationshipType",
      notes,
      source,
      active,
      created_at as "createdAt",
      updated_at as "updatedAt"
    `,
    [churchId, pairKey, memberPk, relatedMemberPk, relationship, notes, sourceKey, actorId]
  );
}

async function createChurchFamilyRelationshipPair({
  churchId,
  memberPk,
  relatedMemberPk,
  relationshipType,
  notes = null,
  actorId = null,
} = {}) {
  const pairKey = crypto.randomUUID();
  const forwardType = normalizeChurchFamilyRelationshipType(relationshipType, "OTHER");
  const reverseType = reciprocalChurchFamilyRelationshipType(forwardType);

  return db.tx(async (t) => {
    const forward = await upsertChurchFamilyRelationshipEdge({
      tx: t,
      churchId,
      pairKey,
      memberPk,
      relatedMemberPk,
      relationshipType: forwardType,
      notes,
      source: "MANUAL",
      actorId,
    });

    const reciprocalNotes =
      notes &&
      String(notes)
        .trim()
        .slice(0, 1200)
      ? `Reciprocal of ${forwardType.toLowerCase()}: ${String(notes).trim().slice(0, 900)}`
      : null;

    const reverse = await upsertChurchFamilyRelationshipEdge({
      tx: t,
      churchId,
      pairKey,
      memberPk: relatedMemberPk,
      relatedMemberPk: memberPk,
      relationshipType: reverseType,
      notes: reciprocalNotes,
      source: "RECIPROCAL",
      actorId,
    });

    return { forward, reverse, pairKey };
  });
}

async function listChurchFamilyRelationships({
  churchId,
  memberPk,
  includeInactive = false,
  limit = 120,
} = {}) {
  const safeLimit = parseChurchLifeLimit(limit, 120, 500);
  return db.manyOrNone(
    `
    select
      r.id,
      r.pair_key as "pairKey",
      r.church_id as "churchId",
      r.member_pk as "memberPk",
      r.related_member_pk as "relatedMemberPk",
      r.relationship_type as "relationshipType",
      r.notes,
      r.source,
      r.active,
      r.created_at as "createdAt",
      r.updated_at as "updatedAt",
      rel.member_id as "relatedMemberId",
      rel.full_name as "relatedMemberName",
      rel.phone as "relatedMemberPhone",
      rel.email as "relatedMemberEmail",
      rel.role as "relatedMemberRole",
      rel.date_of_birth as "relatedDateOfBirth",
      p.household_name as "relatedHouseholdName",
      p.household_role as "relatedHouseholdRole",
      p.address_line1 as "relatedAddressLine1",
      p.address_line2 as "relatedAddressLine2",
      p.suburb as "relatedSuburb",
      p.city as "relatedCity",
      p.province as "relatedProvince",
      p.postal_code as "relatedPostalCode",
      p.country as "relatedCountry"
    from church_family_relationships r
    join members rel on rel.id = r.related_member_pk and rel.church_id = r.church_id
    left join church_member_profiles p
      on p.church_id = r.church_id
     and p.member_pk = r.related_member_pk
    where r.church_id = $1
      and r.member_pk = $2
      and ($3::boolean = true or r.active = true)
    order by
      r.active desc,
      r.relationship_type asc,
      rel.full_name asc nulls last,
      r.created_at desc
    limit $4
    `,
    [churchId, memberPk, includeInactive, safeLimit]
  );
}

async function listChurchFamilyCandidates({
  churchId,
  memberPk,
  search = "",
  limit = 30,
} = {}) {
  const safeLimit = parseChurchLifeLimit(limit, 30, 200);
  const normalizedSearch = String(search || "").trim();
  const likeSearch = normalizedSearch ? `%${normalizedSearch.replace(/[%_\\]/g, "\\$&")}%` : null;
  return db.manyOrNone(
    `
    select
      m.id as "memberPk",
      m.member_id as "memberId",
      m.full_name as "fullName",
      m.phone,
      m.email,
      m.role,
      m.date_of_birth as "dateOfBirth",
      p.household_name as "householdName",
      p.household_role as "householdRole",
      p.address_line1 as "addressLine1",
      p.address_line2 as "addressLine2",
      p.suburb,
      p.city,
      p.province,
      p.postal_code as "postalCode",
      p.country,
      (
        select coalesce(array_agg(distinct fr.relationship_type order by fr.relationship_type), '{}')
        from church_family_relationships fr
        where fr.church_id = m.church_id
          and fr.member_pk = $2
          and fr.related_member_pk = m.id
          and fr.active = true
      ) as "existingRelationshipTypes"
    from members m
    left join church_member_profiles p
      on p.church_id = m.church_id
     and p.member_pk = m.id
    where m.church_id = $1
      and m.id <> $2
      and (
        $3::text is null
        or m.member_id ilike $4 escape '\\'
        or m.full_name ilike $4 escape '\\'
        or coalesce(m.phone, '') ilike $4 escape '\\'
        or coalesce(m.email, '') ilike $4 escape '\\'
      )
    order by
      case
        when $3::text is null then 1
        when lower(coalesce(m.member_id, '')) = lower($3::text) then 0
        when lower(coalesce(m.phone, '')) = lower($3::text) then 0
        when lower(coalesce(m.email, '')) = lower($3::text) then 0
        when lower(coalesce(m.full_name, '')) = lower($3::text) then 0
        else 1
      end asc,
      m.full_name asc nulls last,
      m.member_id asc nulls last,
      m.created_at desc
    limit $5
    `,
    [churchId, memberPk, normalizedSearch || null, likeSearch, safeLimit]
  );
}

async function findChurchFamilyRelationshipById({
  churchId,
  memberPk,
  relationshipId,
} = {}) {
  return db.oneOrNone(
    `
    select
      r.id,
      r.pair_key as "pairKey",
      r.church_id as "churchId",
      r.member_pk as "memberPk",
      r.related_member_pk as "relatedMemberPk",
      r.relationship_type as "relationshipType",
      r.notes,
      r.source,
      r.active,
      r.created_at as "createdAt",
      r.updated_at as "updatedAt",
      rel.member_id as "relatedMemberId",
      rel.full_name as "relatedMemberName",
      rel.phone as "relatedMemberPhone",
      rel.email as "relatedMemberEmail",
      rel.role as "relatedMemberRole",
      rel.date_of_birth as "relatedDateOfBirth",
      p.household_name as "relatedHouseholdName",
      p.household_role as "relatedHouseholdRole",
      p.address_line1 as "relatedAddressLine1",
      p.address_line2 as "relatedAddressLine2",
      p.suburb as "relatedSuburb",
      p.city as "relatedCity",
      p.province as "relatedProvince",
      p.postal_code as "relatedPostalCode",
      p.country as "relatedCountry"
    from church_family_relationships r
    join members rel on rel.id = r.related_member_pk and rel.church_id = r.church_id
    left join church_member_profiles p
      on p.church_id = r.church_id
     and p.member_pk = r.related_member_pk
    where r.church_id = $1
      and r.member_pk = $2
      and r.id = $3
    limit 1
    `,
    [churchId, memberPk, relationshipId]
  );
}

async function listChurchHouseholdChildren({
  churchId,
  parentMemberPk,
  includeInactive = false,
  limit = 120,
} = {}) {
  const safeLimit = parseChurchLifeLimit(limit, 120, 500);
  return db.manyOrNone(
    `
    select
      c.id,
      c.church_id as "churchId",
      c.parent_member_pk as "parentMemberPk",
      p.member_id as "parentMemberId",
      p.full_name as "parentMemberName",
      c.child_member_pk as "childMemberPk",
      cm.member_id as "childMemberId",
      cm.full_name as "childMemberName",
      coalesce(nullif(trim(c.child_name), ''), cm.full_name) as "childName",
      coalesce(c.date_of_birth, cm.date_of_birth) as "dateOfBirth",
      c.gender,
      c.relationship,
      c.school_grade as "schoolGrade",
      c.notes,
      c.active,
      c.created_at as "createdAt",
      c.updated_at as "updatedAt"
    from church_household_children c
    join members p on p.id = c.parent_member_pk and p.church_id = c.church_id
    left join members cm on cm.id = c.child_member_pk and cm.church_id = c.church_id
    where c.church_id = $1
      and c.parent_member_pk = $2
      and ($3::boolean = true or c.active = true)
    order by c.active desc, coalesce(c.date_of_birth, cm.date_of_birth) desc nulls last, c.created_at asc
    limit $4
    `,
    [churchId, parentMemberPk, includeInactive, safeLimit]
  );
}

async function findChurchHouseholdChildById(churchId, childId) {
  return db.oneOrNone(
    `
    select
      c.id,
      c.church_id as "churchId",
      c.parent_member_pk as "parentMemberPk",
      p.member_id as "parentMemberId",
      p.full_name as "parentName",
      p.phone as "parentPhone",
      p.email as "parentEmail",
      c.child_member_pk as "childMemberPk",
      cm.member_id as "childMemberId",
      coalesce(nullif(trim(c.child_name), ''), cm.full_name) as "childName",
      coalesce(c.date_of_birth, cm.date_of_birth) as "childDateOfBirth",
      c.gender as "childGender",
      c.relationship as "childRelationship",
      c.school_grade as "childSchoolGrade",
      c.active
    from church_household_children c
    join members p on p.id = c.parent_member_pk and p.church_id = c.church_id
    left join members cm on cm.id = c.child_member_pk and cm.church_id = c.church_id
    where c.church_id=$1 and c.id=$2
    limit 1
    `,
    [churchId, childId]
  );
}

async function listChurchChildCheckIns({
  churchId,
  serviceId = null,
  campusId = null,
  status = "open",
  parentMemberPk = null,
  limit = 120,
} = {}) {
  const safeLimit = parseChurchLifeLimit(limit, 120, 500);
  const normalizedStatus = (() => {
    const key = String(status || "")
      .trim()
      .toLowerCase();
    if (key === "all") return "all";
    if (key === "checked_out") return "checked_out";
    return "open";
  })();
  const safeServiceId = serviceId && UUID_REGEX.test(String(serviceId || "").trim()) ? String(serviceId || "").trim() : null;
  const safeCampusId = campusId && UUID_REGEX.test(String(campusId || "").trim()) ? String(campusId || "").trim() : null;
  const safeParentMemberPk =
    parentMemberPk && UUID_REGEX.test(String(parentMemberPk || "").trim()) ? String(parentMemberPk || "").trim() : null;

  return db.manyOrNone(
    `
    select
      cc.id,
      cc.church_id as "churchId",
      cc.service_id as "serviceId",
      s.service_name as "serviceName",
      s.service_date as "serviceDate",
      s.starts_at as "serviceStartsAt",
      coalesce(cc.campus_id, s.campus_id) as "campusId",
      camp.name as "campusName",
      camp.code as "campusCode",
      cc.household_child_id as "householdChildId",
      cc.parent_member_pk as "parentMemberPk",
      cc.parent_member_id as "parentMemberId",
      cc.parent_name as "parentName",
      cc.parent_phone as "parentPhone",
      cc.parent_email as "parentEmail",
      cc.child_name as "childName",
      c.child_member_pk as "childMemberPk",
      cm.member_id as "childMemberId",
      coalesce(c.date_of_birth, cm.date_of_birth) as "childDateOfBirth",
      c.gender as "childGender",
      c.relationship as "childRelationship",
      c.school_grade as "childSchoolGrade",
      cc.checkin_method as "checkInMethod",
      cc.checked_in_at as "checkedInAt",
      cc.checked_in_by_member_pk as "checkedInByMemberPk",
      cc.checked_in_by_role as "checkedInByRole",
      cc.checkin_notes as "checkInNotes",
      cc.checked_out_at as "checkedOutAt",
      cc.checkout_method as "checkoutMethod",
      cc.checked_out_by_member_pk as "checkedOutByMemberPk",
      cc.checked_out_by_role as "checkedOutByRole",
      cc.checkout_notes as "checkoutNotes",
      cc.created_at as "createdAt",
      cc.updated_at as "updatedAt"
    from church_children_checkins cc
    join church_services s on s.id = cc.service_id and s.church_id = cc.church_id
    left join church_campuses camp on camp.id = coalesce(cc.campus_id, s.campus_id)
    left join church_household_children c on c.id = cc.household_child_id and c.church_id = cc.church_id
    left join members cm on cm.id = c.child_member_pk and cm.church_id = cc.church_id
    where cc.church_id = $1
      and ($2::uuid is null or cc.service_id = $2)
      and (
        $3::text = 'all'
        or ($3::text = 'open' and cc.checked_out_at is null)
        or ($3::text = 'checked_out' and cc.checked_out_at is not null)
      )
      and ($4::uuid is null or coalesce(cc.campus_id, s.campus_id) = $4::uuid)
      and ($5::uuid is null or cc.parent_member_pk = $5)
    order by cc.checked_in_at desc
    limit $6
    `,
    [churchId, safeServiceId, normalizedStatus, safeCampusId, safeParentMemberPk, safeLimit]
  );
}

async function findChurchChildCheckInRecord(churchId, checkInId, { parentMemberPk = null } = {}) {
  const safeParentMemberPk =
    parentMemberPk && UUID_REGEX.test(String(parentMemberPk || "").trim()) ? String(parentMemberPk || "").trim() : null;
  return db.oneOrNone(
    `
    select
      cc.id,
      cc.church_id as "churchId",
      cc.service_id as "serviceId",
      s.service_name as "serviceName",
      s.service_date as "serviceDate",
      s.starts_at as "serviceStartsAt",
      coalesce(cc.campus_id, s.campus_id) as "campusId",
      camp.name as "campusName",
      camp.code as "campusCode",
      cc.household_child_id as "householdChildId",
      cc.parent_member_pk as "parentMemberPk",
      cc.parent_member_id as "parentMemberId",
      cc.parent_name as "parentName",
      cc.parent_phone as "parentPhone",
      cc.parent_email as "parentEmail",
      cc.child_name as "childName",
      c.child_member_pk as "childMemberPk",
      cm.member_id as "childMemberId",
      coalesce(c.date_of_birth, cm.date_of_birth) as "childDateOfBirth",
      c.gender as "childGender",
      c.relationship as "childRelationship",
      c.school_grade as "childSchoolGrade",
      cc.checkin_method as "checkInMethod",
      cc.checked_in_at as "checkedInAt",
      cc.checked_in_by_member_pk as "checkedInByMemberPk",
      cc.checked_in_by_role as "checkedInByRole",
      cc.checkin_notes as "checkInNotes",
      cc.checked_out_at as "checkedOutAt",
      cc.checkout_method as "checkoutMethod",
      cc.checked_out_by_member_pk as "checkedOutByMemberPk",
      cc.checked_out_by_role as "checkedOutByRole",
      cc.checkout_notes as "checkoutNotes",
      cc.created_at as "createdAt",
      cc.updated_at as "updatedAt"
    from church_children_checkins cc
    join church_services s on s.id = cc.service_id and s.church_id = cc.church_id
    left join church_campuses camp on camp.id = coalesce(cc.campus_id, s.campus_id)
    left join church_household_children c on c.id = cc.household_child_id and c.church_id = cc.church_id
    left join members cm on cm.id = c.child_member_pk and cm.church_id = cc.church_id
    where cc.church_id = $1
      and cc.id = $2
      and ($3::uuid is null or cc.parent_member_pk = $3)
    limit 1
    `,
    [churchId, checkInId, safeParentMemberPk]
  );
}

router.get(
  "/admin/church-life/member-profiles/:memberPk/children",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations", "members"),
  requireChurchGrowthActive,
  requireChurchLifePermission("profiles.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const memberPk = String(req.params?.memberPk || "").trim();
      if (!UUID_REGEX.test(memberPk)) return res.status(400).json({ error: "Invalid memberPk" });

      const parent = await findMemberInChurchByUuid(churchId, memberPk);
      if (!parent) return res.status(404).json({ error: "Member not found in this church." });

      const includeInactive = toBoolean(req.query?.includeInactive) === true;
      const limit = parseChurchLifeLimit(req.query?.limit, 120, 500);
      const rows = await listChurchHouseholdChildren({
        churchId,
        parentMemberPk: memberPk,
        includeInactive,
        limit,
      });

      return res.json({
        ok: true,
        parent: {
          memberPk: parent.id,
          memberId: parent.memberId || null,
          fullName: parent.fullName || null,
        },
        children: rows.map(normalizeChurchHouseholdChildRow),
        meta: { includeInactive, limit, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Household child profiles are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/member-profiles] children list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/member-profiles/:memberPk/children",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations", "members"),
  requireChurchGrowthActive,
  requireChurchLifePermission("profiles.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const memberPk = String(req.params?.memberPk || "").trim();
      if (!UUID_REGEX.test(memberPk)) return res.status(400).json({ error: "Invalid memberPk" });

      const parent = await findMemberInChurchByUuid(churchId, memberPk);
      if (!parent) return res.status(404).json({ error: "Member not found in this church." });

      const childLinkResolution = await resolveChurchHouseholdChildMemberLink({
        churchId,
        parentMemberPk: memberPk,
        childMemberPkInput: req.body?.childMemberPk,
        childMemberRefInput: req.body?.childMemberRef,
      });
      if (childLinkResolution?.error) {
        return res.status(Number(childLinkResolution.status || 400)).json({ error: childLinkResolution.error });
      }
      const childMemberPk = childLinkResolution.childMemberPk || null;
      const linkedChild = childLinkResolution.linkedChild || null;

      const childName = String(req.body?.childName || "")
        .trim()
        .slice(0, 160);
      const dateOfBirthInput = req.body?.dateOfBirth;
      let dateOfBirth = null;
      if (typeof dateOfBirthInput !== "undefined" && dateOfBirthInput !== null && dateOfBirthInput !== "") {
        dateOfBirth = parseChurchLifeDate(dateOfBirthInput);
        if (!dateOfBirth) return res.status(400).json({ error: "dateOfBirth must be YYYY-MM-DD." });
      } else if (linkedChild?.dateOfBirth) {
        dateOfBirth = formatDateIsoLike(linkedChild.dateOfBirth) || null;
      }
      const gender = normalizeChurchHouseholdChildGender(req.body?.gender);
      const relationship = normalizeChurchHouseholdRelationship(req.body?.relationship || "CHILD");
      const schoolGrade = String(req.body?.schoolGrade || "")
        .trim()
        .slice(0, 80) || null;
      const notes = String(req.body?.notes || "")
        .trim()
        .slice(0, 1200) || null;
      const active = toBoolean(req.body?.active) !== false;
      const shareAddressInput = req.body?.shareAddress;
      const shareAddress = typeof shareAddressInput === "undefined" ? true : toBoolean(shareAddressInput) !== false;
      const overwriteAddress = toBoolean(req.body?.overwriteAddress) === true;
      const resolvedName = childName || linkedChild?.fullName || "";
      if (!resolvedName) {
        return res.status(400).json({ error: "childName is required unless childMemberPk is provided." });
      }

      let addressCompatibility = null;
      if (linkedChild?.id) {
        addressCompatibility = await resolveChurchHouseholdAddressCompatibility({
          churchId,
          parentMemberPk: memberPk,
          childMemberPk: linkedChild.id,
        });
        if (addressCompatibility?.mismatch) {
          return res.status(409).json({
            error: "Household is for members at the same address. Use Family Tree for relatives with different addresses.",
            code: "HOUSEHOLD_ADDRESS_MISMATCH",
          });
        }
      }

      const row = await db.one(
        `
        insert into church_household_children (
          church_id,
          parent_member_pk,
          child_member_pk,
          child_name,
          date_of_birth,
          gender,
          relationship,
          school_grade,
          notes,
          active,
          created_by,
          updated_by
        )
        values (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11
        )
        returning
          id,
          church_id as "churchId",
          parent_member_pk as "parentMemberPk",
          child_member_pk as "childMemberPk",
          child_name as "childName",
          date_of_birth as "dateOfBirth",
          gender,
          relationship,
          school_grade as "schoolGrade",
          notes,
          active,
          created_at as "createdAt",
          updated_at as "updatedAt"
        `,
        [
          churchId,
          memberPk,
          childMemberPk,
          resolvedName,
          dateOfBirth,
          gender,
          relationship,
          schoolGrade,
          notes,
          active,
          req.user?.id || null,
        ]
      );

      const withNames = await db.one(
        `
        select
          c.id,
          c.church_id as "churchId",
          c.parent_member_pk as "parentMemberPk",
          p.member_id as "parentMemberId",
          p.full_name as "parentMemberName",
          c.child_member_pk as "childMemberPk",
          cm.member_id as "childMemberId",
          cm.full_name as "childMemberName",
          coalesce(nullif(trim(c.child_name), ''), cm.full_name) as "childName",
          coalesce(c.date_of_birth, cm.date_of_birth) as "dateOfBirth",
          c.gender,
          c.relationship,
          c.school_grade as "schoolGrade",
          c.notes,
          c.active,
          c.created_at as "createdAt",
          c.updated_at as "updatedAt"
        from church_household_children c
        join members p on p.id = c.parent_member_pk and p.church_id = c.church_id
        left join members cm on cm.id = c.child_member_pk and cm.church_id = c.church_id
        where c.id = $1 and c.church_id = $2
        limit 1
        `,
        [row.id, churchId]
      );

      let addressSync = { updated: false, copiedFields: [] };
      if (linkedChild?.id && shareAddress) {
        addressSync = await syncChurchHouseholdAddressByDirection({
          churchId,
          parentMemberPk: memberPk,
          childMemberPk: linkedChild.id,
          direction: addressCompatibility?.syncDirection || null,
          actorId: req.user?.id || null,
          overwrite: overwriteAddress,
        });
      }

      return res.status(201).json({
        ok: true,
        child: normalizeChurchHouseholdChildRow(withNames),
        addressSync: {
          updated: addressSync?.updated === true,
          copiedFields: Array.isArray(addressSync?.copiedFields) ? addressSync.copiedFields : [],
        },
      });
    } catch (err) {
      if (err?.code === "23505") {
        return res.status(409).json({ error: "This child is already linked to the selected parent." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Household child profiles are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/member-profiles] child create error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/church-life/member-profiles/:memberPk/children/:childId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations", "members"),
  requireChurchGrowthActive,
  requireChurchLifePermission("profiles.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const memberPk = String(req.params?.memberPk || "").trim();
      const childId = String(req.params?.childId || "").trim();
      if (!UUID_REGEX.test(memberPk) || !UUID_REGEX.test(childId)) {
        return res.status(400).json({ error: "Invalid memberPk or childId" });
      }

      const parent = await findMemberInChurchByUuid(churchId, memberPk);
      if (!parent) return res.status(404).json({ error: "Member not found in this church." });

      const current = await db.oneOrNone(
        `
        select
          id,
          church_id as "churchId",
          parent_member_pk as "parentMemberPk",
          child_member_pk as "childMemberPk",
          child_name as "childName",
          date_of_birth as "dateOfBirth",
          gender,
          relationship,
          school_grade as "schoolGrade",
          notes,
          active
        from church_household_children
        where id=$1 and church_id=$2 and parent_member_pk=$3
        limit 1
        `,
        [childId, churchId, memberPk]
      );
      if (!current) return res.status(404).json({ error: "Child profile not found." });

      let childMemberPk = current.childMemberPk || null;
      let linkedChild = null;
      const hasChildMemberPkInput = Object.prototype.hasOwnProperty.call(req.body || {}, "childMemberPk");
      const hasChildMemberRefInput = Object.prototype.hasOwnProperty.call(req.body || {}, "childMemberRef");
      if (hasChildMemberPkInput || hasChildMemberRefInput) {
        const nextChildMemberPk = hasChildMemberPkInput ? String(req.body?.childMemberPk || "").trim() : "";
        const nextChildMemberRef = hasChildMemberRefInput ? String(req.body?.childMemberRef || "").trim() : "";
        if (hasChildMemberPkInput && !nextChildMemberPk && (!hasChildMemberRefInput || !nextChildMemberRef)) {
          childMemberPk = null;
        } else if (nextChildMemberPk || nextChildMemberRef) {
          const childLinkResolution = await resolveChurchHouseholdChildMemberLink({
            churchId,
            parentMemberPk: memberPk,
            childMemberPkInput: nextChildMemberPk || undefined,
            childMemberRefInput: nextChildMemberRef || undefined,
          });
          if (childLinkResolution?.error) {
            return res.status(Number(childLinkResolution.status || 400)).json({ error: childLinkResolution.error });
          }
          childMemberPk = childLinkResolution.childMemberPk || null;
          linkedChild = childLinkResolution.linkedChild || null;
        }
      }

      const childName = String(
        typeof req.body?.childName === "undefined" ? current.childName || "" : req.body?.childName || ""
      )
        .trim()
        .slice(0, 160);
      const dateOfBirthInput =
        typeof req.body?.dateOfBirth === "undefined" ? formatDateIsoLike(current.dateOfBirth) : req.body?.dateOfBirth;
      let dateOfBirth = null;
      if (dateOfBirthInput !== null && dateOfBirthInput !== "") {
        dateOfBirth = parseChurchLifeDate(dateOfBirthInput);
        if (!dateOfBirth) return res.status(400).json({ error: "dateOfBirth must be YYYY-MM-DD." });
      }
      const gender = normalizeChurchHouseholdChildGender(
        typeof req.body?.gender === "undefined" ? current.gender : req.body?.gender
      );
      const relationship = normalizeChurchHouseholdRelationship(
        typeof req.body?.relationship === "undefined" ? current.relationship : req.body?.relationship
      );
      const schoolGrade =
        String(typeof req.body?.schoolGrade === "undefined" ? current.schoolGrade || "" : req.body?.schoolGrade || "")
          .trim()
          .slice(0, 80) || null;
      const notes =
        String(typeof req.body?.notes === "undefined" ? current.notes || "" : req.body?.notes || "")
          .trim()
          .slice(0, 1200) || null;
      const active = typeof req.body?.active === "undefined" ? current.active !== false : toBoolean(req.body?.active) !== false;
      const shareAddressInput = req.body?.shareAddress;
      const shareAddress = typeof shareAddressInput === "undefined" ? false : toBoolean(shareAddressInput) !== false;
      const overwriteAddress = toBoolean(req.body?.overwriteAddress) === true;

      if (!childName && !childMemberPk) {
        return res.status(400).json({ error: "childName is required unless childMemberPk is set." });
      }

      let addressCompatibility = null;
      if (childMemberPk) {
        addressCompatibility = await resolveChurchHouseholdAddressCompatibility({
          churchId,
          parentMemberPk: memberPk,
          childMemberPk,
        });
        if (addressCompatibility?.mismatch) {
          return res.status(409).json({
            error: "Household is for members at the same address. Use Family Tree for relatives with different addresses.",
            code: "HOUSEHOLD_ADDRESS_MISMATCH",
          });
        }
      }

      const row = await db.one(
        `
        update church_household_children
        set
          child_member_pk = $4,
          child_name = $5,
          date_of_birth = $6,
          gender = $7,
          relationship = $8,
          school_grade = $9,
          notes = $10,
          active = $11,
          updated_by = $12,
          updated_at = now()
        where id=$1 and church_id=$2 and parent_member_pk=$3
        returning id
        `,
        [childId, churchId, memberPk, childMemberPk, childName || null, dateOfBirth, gender, relationship, schoolGrade, notes, active, req.user?.id || null]
      );

      const withNames = await db.one(
        `
        select
          c.id,
          c.church_id as "churchId",
          c.parent_member_pk as "parentMemberPk",
          p.member_id as "parentMemberId",
          p.full_name as "parentMemberName",
          c.child_member_pk as "childMemberPk",
          cm.member_id as "childMemberId",
          cm.full_name as "childMemberName",
          coalesce(nullif(trim(c.child_name), ''), cm.full_name) as "childName",
          coalesce(c.date_of_birth, cm.date_of_birth) as "dateOfBirth",
          c.gender,
          c.relationship,
          c.school_grade as "schoolGrade",
          c.notes,
          c.active,
          c.created_at as "createdAt",
          c.updated_at as "updatedAt"
        from church_household_children c
        join members p on p.id = c.parent_member_pk and p.church_id = c.church_id
        left join members cm on cm.id = c.child_member_pk and cm.church_id = c.church_id
        where c.id = $1 and c.church_id = $2
        limit 1
        `,
        [row.id, churchId]
      );

      let addressSync = { updated: false, copiedFields: [] };
      if (shareAddress && childMemberPk) {
        addressSync = await syncChurchHouseholdAddressByDirection({
          churchId,
          parentMemberPk: memberPk,
          childMemberPk,
          direction: addressCompatibility?.syncDirection || null,
          actorId: req.user?.id || null,
          overwrite: overwriteAddress,
        });
      }

      return res.json({
        ok: true,
        child: normalizeChurchHouseholdChildRow(withNames),
        addressSync: {
          updated: addressSync?.updated === true,
          copiedFields: Array.isArray(addressSync?.copiedFields) ? addressSync.copiedFields : [],
        },
      });
    } catch (err) {
      if (err?.code === "23505") {
        return res.status(409).json({ error: "This child is already linked to the selected parent." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Household child profiles are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/member-profiles] child patch error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.delete(
  "/admin/church-life/member-profiles/:memberPk/children/:childId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations", "members"),
  requireChurchGrowthActive,
  requireChurchLifePermission("profiles.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const memberPk = String(req.params?.memberPk || "").trim();
      const childId = String(req.params?.childId || "").trim();
      if (!UUID_REGEX.test(memberPk) || !UUID_REGEX.test(childId)) {
        return res.status(400).json({ error: "Invalid memberPk or childId" });
      }

      const row = await db.oneOrNone(
        `
        update church_household_children
        set
          active = false,
          updated_by = $4,
          updated_at = now()
        where id=$1 and church_id=$2 and parent_member_pk=$3
        returning id
        `,
        [childId, churchId, memberPk, req.user?.id || null]
      );
      if (!row) return res.status(404).json({ error: "Child profile not found." });

      return res.json({ ok: true, childId: row.id, active: false });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Household child profiles are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/member-profiles] child delete error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get("/church-life/household/children", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const includeInactive = toBoolean(req.query?.includeInactive) === true;
    const limit = parseChurchLifeLimit(req.query?.limit, 120, 500);
    const rows = await listChurchHouseholdChildren({
      churchId,
      parentMemberPk: req.user.id,
      includeInactive,
      limit,
    });
    return res.json({
      ok: true,
      children: rows.map(normalizeChurchHouseholdChildRow),
      meta: { includeInactive, limit, returned: rows.length },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Household child profiles are not available yet. Run migrations and retry." });
    }
    console.error("[church-life/household/children] list error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/church-life/household/member-candidates", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    const parentMemberPk = String(req.user?.id || "").trim();
    if (!UUID_REGEX.test(parentMemberPk)) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const search = String(req.query?.search || req.query?.q || "").trim();
    const allowEmpty = toBoolean(req.query?.allowEmpty) === true;
    if (!search && !allowEmpty) {
      return res.status(400).json({ error: "search is required." });
    }

    const limit = parseChurchLifeLimit(req.query?.limit, 20, 200);
    const rows = await listChurchHouseholdMemberCandidates({
      churchId,
      parentMemberPk,
      search,
      limit,
    });
    const parentAddress = toChurchProfileAddressObject(
      (await getChurchMemberProfileAddress(churchId, parentMemberPk)) || {}
    );
    const compatibleRows = rows.filter((row) =>
      isChurchHouseholdAddressCompatible(parentAddress, toChurchProfileAddressObject(row || {}))
    );

    return res.json({
      ok: true,
      members: compatibleRows.map(normalizeChurchHouseholdMemberCandidateRow),
      meta: {
        search: search || null,
        limit,
        returned: compatibleRows.length,
        sameAddressOnly: true,
      },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Household member candidates are not available yet. Run migrations and retry." });
    }
    console.error("[church-life/household/member-candidates] list error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/church-life/family/relationships", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    const memberPk = String(req.user?.id || "").trim();
    if (!UUID_REGEX.test(memberPk)) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const includeInactive = toBoolean(req.query?.includeInactive) === true;
    const limit = parseChurchLifeLimit(req.query?.limit, 120, 500);
    const rows = await listChurchFamilyRelationships({
      churchId,
      memberPk,
      includeInactive,
      limit,
    });
    const relationships = rows.map(normalizeChurchFamilyRelationshipRow);
    const summary = relationships.reduce(
      (acc, item) => {
        acc.total += 1;
        if (item?.active === false) return acc;
        acc.active += 1;
        const type = normalizeChurchFamilyRelationshipType(item?.relationshipType, "OTHER");
        acc.byType[type] = (acc.byType[type] || 0) + 1;
        return acc;
      },
      { total: 0, active: 0, byType: {} }
    );

    return res.json({
      ok: true,
      relationships,
      relationshipTypes: Array.from(CHURCH_FAMILY_RELATIONSHIP_TYPES.values()),
      summary,
      meta: { includeInactive, limit, returned: relationships.length },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life family tree is not available yet. Run migrations and retry." });
    }
    console.error("[church-life/family/relationships] list error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/church-life/family/candidates", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    const memberPk = String(req.user?.id || "").trim();
    if (!UUID_REGEX.test(memberPk)) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const search = String(req.query?.search || req.query?.q || "").trim();
    const allowEmpty = toBoolean(req.query?.allowEmpty) === true;
    if (!search && !allowEmpty) {
      return res.status(400).json({ error: "search is required." });
    }
    const limit = parseChurchLifeLimit(req.query?.limit, 30, 200);
    const rows = await listChurchFamilyCandidates({
      churchId,
      memberPk,
      search,
      limit,
    });

    return res.json({
      ok: true,
      members: rows.map(normalizeChurchFamilyCandidateRow),
      relationshipTypes: Array.from(CHURCH_FAMILY_RELATIONSHIP_TYPES.values()),
      meta: { search: search || null, limit, returned: rows.length },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life family tree is not available yet. Run migrations and retry." });
    }
    console.error("[church-life/family/candidates] list error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/church-life/family/relationships", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    const memberPk = String(req.user?.id || "").trim();
    if (!UUID_REGEX.test(memberPk)) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const relationshipType = normalizeChurchFamilyRelationshipType(
      req.body?.relationshipType || req.body?.type || "OTHER",
      "OTHER"
    );
    const notes =
      String(req.body?.notes || "")
        .trim()
        .slice(0, 1200) || null;

    const relatedResolution = await resolveChurchFamilyRelatedMember({
      churchId,
      memberPk,
      relatedMemberPkInput: req.body?.relatedMemberPk,
      relatedMemberRefInput: req.body?.relatedMemberRef || req.body?.relatedMemberId || req.body?.relatedMemberIdentifier,
    });
    if (relatedResolution?.error) {
      return res.status(Number(relatedResolution.status || 400)).json({ error: relatedResolution.error });
    }
    const relatedMemberPk = String(relatedResolution?.relatedMember?.id || "").trim();
    if (!UUID_REGEX.test(relatedMemberPk)) {
      return res.status(400).json({ error: "Invalid relatedMemberPk." });
    }

    await db.none(
      `
      update church_family_relationships
      set
        active = false,
        updated_by = $4,
        updated_at = now()
      where church_id = $1
        and active = true
        and (
          (member_pk = $2 and related_member_pk = $3)
          or (member_pk = $3 and related_member_pk = $2)
        )
      `,
      [churchId, memberPk, relatedMemberPk, req.user?.id || null]
    );

    const created = await createChurchFamilyRelationshipPair({
      churchId,
      memberPk,
      relatedMemberPk,
      relationshipType,
      notes,
      actorId: req.user?.id || null,
    });

    const row = await findChurchFamilyRelationshipById({
      churchId,
      memberPk,
      relationshipId: created?.forward?.id,
    });

    return res.status(201).json({
      ok: true,
      relationship: normalizeChurchFamilyRelationshipRow(row || created?.forward),
      reciprocalRelationshipId: created?.reverse?.id || null,
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life family tree is not available yet. Run migrations and retry." });
    }
    if (err?.code === "23505") {
      return res.status(409).json({ error: "This family relationship already exists." });
    }
    console.error("[church-life/family/relationships] create error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.delete("/church-life/family/relationships/:relationshipId", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    const memberPk = String(req.user?.id || "").trim();
    if (!UUID_REGEX.test(memberPk)) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const relationshipId = String(req.params?.relationshipId || "").trim();
    if (!UUID_REGEX.test(relationshipId)) {
      return res.status(400).json({ error: "Invalid relationshipId." });
    }

    const row = await db.oneOrNone(
      `
      select
        id,
        pair_key as "pairKey",
        member_pk as "memberPk",
        related_member_pk as "relatedMemberPk",
        active
      from church_family_relationships
      where church_id = $1
        and member_pk = $2
        and id = $3
      limit 1
      `,
      [churchId, memberPk, relationshipId]
    );
    if (!row) return res.status(404).json({ error: "Relationship not found." });

    const deactivated = await db.result(
      `
      update church_family_relationships
      set
        active = false,
        updated_by = $4,
        updated_at = now()
      where church_id = $1
        and pair_key = $2
        and (
          (member_pk = $3 and related_member_pk = $5)
          or (member_pk = $5 and related_member_pk = $3)
        )
        and active = true
      `,
      [churchId, row.pairKey, memberPk, req.user?.id || null, row.relatedMemberPk]
    );

    return res.json({
      ok: true,
      relationshipId: row.id,
      pairKey: row.pairKey,
      deactivated: Number(deactivated?.rowCount || 0),
      active: false,
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Church Life family tree is not available yet. Run migrations and retry." });
    }
    console.error("[church-life/family/relationships] delete error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/church-life/household/children", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    const parentMemberPk = String(req.user.id || "").trim();
    const childLinkResolution = await resolveChurchHouseholdChildMemberLink({
      churchId,
      parentMemberPk,
      childMemberPkInput: req.body?.childMemberPk,
      childMemberRefInput: req.body?.childMemberRef,
    });
    if (childLinkResolution?.error) {
      return res.status(Number(childLinkResolution.status || 400)).json({ error: childLinkResolution.error });
    }
    const childMemberPk = childLinkResolution.childMemberPk || null;
    const linkedChild = childLinkResolution.linkedChild || null;

    const childName = String(req.body?.childName || "")
      .trim()
      .slice(0, 160);
    const dateOfBirthInput = req.body?.dateOfBirth;
    let dateOfBirth = null;
    if (typeof dateOfBirthInput !== "undefined" && dateOfBirthInput !== null && dateOfBirthInput !== "") {
      dateOfBirth = parseChurchLifeDate(dateOfBirthInput);
      if (!dateOfBirth) return res.status(400).json({ error: "dateOfBirth must be YYYY-MM-DD." });
    } else if (linkedChild?.dateOfBirth) {
      dateOfBirth = formatDateIsoLike(linkedChild.dateOfBirth) || null;
    }
    const gender = normalizeChurchHouseholdChildGender(req.body?.gender);
    const relationship = normalizeChurchHouseholdRelationship(req.body?.relationship || "CHILD");
    const schoolGrade = String(req.body?.schoolGrade || "")
      .trim()
      .slice(0, 80) || null;
    const notes = String(req.body?.notes || "")
      .trim()
      .slice(0, 1200) || null;
    const active = toBoolean(req.body?.active) !== false;
    const shareAddressInput = req.body?.shareAddress;
    const shareAddress = typeof shareAddressInput === "undefined" ? true : toBoolean(shareAddressInput) !== false;
    const overwriteAddress = toBoolean(req.body?.overwriteAddress) === true;
    const resolvedName = childName || linkedChild?.fullName || "";
    if (!resolvedName) {
      return res.status(400).json({ error: "childName is required unless childMemberPk is provided." });
    }

    let addressCompatibility = null;
    if (linkedChild?.id) {
      addressCompatibility = await resolveChurchHouseholdAddressCompatibility({
        churchId,
        parentMemberPk,
        childMemberPk: linkedChild.id,
      });
      if (addressCompatibility?.mismatch) {
        return res.status(409).json({
          error: "Household is for members at the same address. Use Family Tree for relatives with different addresses.",
          code: "HOUSEHOLD_ADDRESS_MISMATCH",
        });
      }
    }

    const row = await db.one(
      `
      insert into church_household_children (
        church_id,
        parent_member_pk,
        child_member_pk,
        child_name,
        date_of_birth,
        gender,
        relationship,
        school_grade,
        notes,
        active,
        created_by,
        updated_by
      )
      values (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11
      )
      returning
        id,
        church_id as "churchId",
        parent_member_pk as "parentMemberPk",
        child_member_pk as "childMemberPk",
        child_name as "childName",
        date_of_birth as "dateOfBirth",
        gender,
        relationship,
        school_grade as "schoolGrade",
        notes,
        active,
        created_at as "createdAt",
        updated_at as "updatedAt"
      `,
      [churchId, parentMemberPk, childMemberPk, resolvedName, dateOfBirth, gender, relationship, schoolGrade, notes, active, req.user.id]
    );

    const withNames = await db.one(
      `
      select
        c.id,
        c.church_id as "churchId",
        c.parent_member_pk as "parentMemberPk",
        p.member_id as "parentMemberId",
        p.full_name as "parentMemberName",
        c.child_member_pk as "childMemberPk",
        cm.member_id as "childMemberId",
        cm.full_name as "childMemberName",
        coalesce(nullif(trim(c.child_name), ''), cm.full_name) as "childName",
        coalesce(c.date_of_birth, cm.date_of_birth) as "dateOfBirth",
        c.gender,
        c.relationship,
        c.school_grade as "schoolGrade",
        c.notes,
        c.active,
        c.created_at as "createdAt",
        c.updated_at as "updatedAt"
      from church_household_children c
      join members p on p.id = c.parent_member_pk and p.church_id = c.church_id
      left join members cm on cm.id = c.child_member_pk and cm.church_id = c.church_id
      where c.id = $1 and c.church_id = $2
      limit 1
      `,
      [row.id, churchId]
    );

    let addressSync = { updated: false, copiedFields: [] };
    if (linkedChild?.id && shareAddress) {
      addressSync = await syncChurchHouseholdAddressByDirection({
        churchId,
        parentMemberPk,
        childMemberPk: linkedChild.id,
        direction: addressCompatibility?.syncDirection || null,
        actorId: req.user?.id || null,
        overwrite: overwriteAddress,
      });
    }

    return res.status(201).json({
      ok: true,
      child: normalizeChurchHouseholdChildRow(withNames),
      addressSync: {
        updated: addressSync?.updated === true,
        copiedFields: Array.isArray(addressSync?.copiedFields) ? addressSync.copiedFields : [],
      },
    });
  } catch (err) {
    if (err?.code === "23505") {
      return res.status(409).json({ error: "This child is already linked to your profile." });
    }
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Household child profiles are not available yet. Run migrations and retry." });
    }
    console.error("[church-life/household/children] create error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.patch("/church-life/household/children/:childId", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const parentMemberPk = String(req.user.id || "").trim();
    const childId = String(req.params?.childId || "").trim();
    if (!UUID_REGEX.test(childId)) return res.status(400).json({ error: "Invalid childId" });

    const current = await db.oneOrNone(
      `
      select
        id,
        child_member_pk as "childMemberPk",
        child_name as "childName",
        date_of_birth as "dateOfBirth",
        gender,
        relationship,
        school_grade as "schoolGrade",
        notes,
        active
      from church_household_children
      where id=$1 and church_id=$2 and parent_member_pk=$3
      limit 1
      `,
      [childId, churchId, parentMemberPk]
    );
    if (!current) return res.status(404).json({ error: "Child profile not found." });

    let childMemberPk = current.childMemberPk || null;
    let linkedChild = null;
    const hasChildMemberPkInput = Object.prototype.hasOwnProperty.call(req.body || {}, "childMemberPk");
    const hasChildMemberRefInput = Object.prototype.hasOwnProperty.call(req.body || {}, "childMemberRef");
    if (hasChildMemberPkInput || hasChildMemberRefInput) {
      const nextChildMemberPk = hasChildMemberPkInput ? String(req.body?.childMemberPk || "").trim() : "";
      const nextChildMemberRef = hasChildMemberRefInput ? String(req.body?.childMemberRef || "").trim() : "";
      if (hasChildMemberPkInput && !nextChildMemberPk && (!hasChildMemberRefInput || !nextChildMemberRef)) {
        childMemberPk = null;
      } else if (nextChildMemberPk || nextChildMemberRef) {
        const childLinkResolution = await resolveChurchHouseholdChildMemberLink({
          churchId,
          parentMemberPk,
          childMemberPkInput: nextChildMemberPk || undefined,
          childMemberRefInput: nextChildMemberRef || undefined,
        });
        if (childLinkResolution?.error) {
          return res.status(Number(childLinkResolution.status || 400)).json({ error: childLinkResolution.error });
        }
        childMemberPk = childLinkResolution.childMemberPk || null;
        linkedChild = childLinkResolution.linkedChild || null;
      }
    }

    const childName = String(
      typeof req.body?.childName === "undefined" ? current.childName || "" : req.body?.childName || ""
    )
      .trim()
      .slice(0, 160);
    const dateOfBirthInput =
      typeof req.body?.dateOfBirth === "undefined" ? formatDateIsoLike(current.dateOfBirth) : req.body?.dateOfBirth;
    let dateOfBirth = null;
    if (dateOfBirthInput !== null && dateOfBirthInput !== "") {
      dateOfBirth = parseChurchLifeDate(dateOfBirthInput);
      if (!dateOfBirth) return res.status(400).json({ error: "dateOfBirth must be YYYY-MM-DD." });
    }
    const gender = normalizeChurchHouseholdChildGender(
      typeof req.body?.gender === "undefined" ? current.gender : req.body?.gender
    );
    const relationship = normalizeChurchHouseholdRelationship(
      typeof req.body?.relationship === "undefined" ? current.relationship : req.body?.relationship
    );
    const schoolGrade =
      String(typeof req.body?.schoolGrade === "undefined" ? current.schoolGrade || "" : req.body?.schoolGrade || "")
        .trim()
        .slice(0, 80) || null;
    const notes =
      String(typeof req.body?.notes === "undefined" ? current.notes || "" : req.body?.notes || "")
        .trim()
        .slice(0, 1200) || null;
    const active = typeof req.body?.active === "undefined" ? current.active !== false : toBoolean(req.body?.active) !== false;
    const shareAddressInput = req.body?.shareAddress;
    const shareAddress = typeof shareAddressInput === "undefined" ? false : toBoolean(shareAddressInput) !== false;
    const overwriteAddress = toBoolean(req.body?.overwriteAddress) === true;
    if (!childName && !childMemberPk) {
      return res.status(400).json({ error: "childName is required unless childMemberPk is set." });
    }

    let addressCompatibility = null;
    if (childMemberPk) {
      addressCompatibility = await resolveChurchHouseholdAddressCompatibility({
        churchId,
        parentMemberPk,
        childMemberPk,
      });
      if (addressCompatibility?.mismatch) {
        return res.status(409).json({
          error: "Household is for members at the same address. Use Family Tree for relatives with different addresses.",
          code: "HOUSEHOLD_ADDRESS_MISMATCH",
        });
      }
    }

    const row = await db.one(
      `
      update church_household_children
      set
        child_member_pk = $4,
        child_name = $5,
        date_of_birth = $6,
        gender = $7,
        relationship = $8,
        school_grade = $9,
        notes = $10,
        active = $11,
        updated_by = $12,
        updated_at = now()
      where id=$1 and church_id=$2 and parent_member_pk=$3
      returning id
      `,
      [childId, churchId, parentMemberPk, childMemberPk, childName || null, dateOfBirth, gender, relationship, schoolGrade, notes, active, req.user.id]
    );

    const withNames = await db.one(
      `
      select
        c.id,
        c.church_id as "churchId",
        c.parent_member_pk as "parentMemberPk",
        p.member_id as "parentMemberId",
        p.full_name as "parentMemberName",
        c.child_member_pk as "childMemberPk",
        cm.member_id as "childMemberId",
        cm.full_name as "childMemberName",
        coalesce(nullif(trim(c.child_name), ''), cm.full_name) as "childName",
        coalesce(c.date_of_birth, cm.date_of_birth) as "dateOfBirth",
        c.gender,
        c.relationship,
        c.school_grade as "schoolGrade",
        c.notes,
        c.active,
        c.created_at as "createdAt",
        c.updated_at as "updatedAt"
      from church_household_children c
      join members p on p.id = c.parent_member_pk and p.church_id = c.church_id
      left join members cm on cm.id = c.child_member_pk and cm.church_id = c.church_id
      where c.id = $1 and c.church_id = $2
      limit 1
      `,
      [row.id, churchId]
    );

    let addressSync = { updated: false, copiedFields: [] };
    if (shareAddress && childMemberPk) {
      addressSync = await syncChurchHouseholdAddressByDirection({
        churchId,
        parentMemberPk,
        childMemberPk,
        direction: addressCompatibility?.syncDirection || null,
        actorId: req.user?.id || null,
        overwrite: overwriteAddress,
      });
    }

    return res.json({
      ok: true,
      child: normalizeChurchHouseholdChildRow(withNames),
      addressSync: {
        updated: addressSync?.updated === true,
        copiedFields: Array.isArray(addressSync?.copiedFields) ? addressSync.copiedFields : [],
      },
    });
  } catch (err) {
    if (err?.code === "23505") {
      return res.status(409).json({ error: "This child is already linked to your profile." });
    }
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Household child profiles are not available yet. Run migrations and retry." });
    }
    console.error("[church-life/household/children] patch error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.delete("/church-life/household/children/:childId", requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;
    const parentMemberPk = String(req.user.id || "").trim();
    const childId = String(req.params?.childId || "").trim();
    if (!UUID_REGEX.test(childId)) return res.status(400).json({ error: "Invalid childId" });

    const row = await db.oneOrNone(
      `
      update church_household_children
      set
        active = false,
        updated_by = $4,
        updated_at = now()
      where id=$1 and church_id=$2 and parent_member_pk=$3
      returning id
      `,
      [childId, churchId, parentMemberPk, req.user.id]
    );
    if (!row) return res.status(404).json({ error: "Child profile not found." });
    return res.json({ ok: true, childId: row.id, active: false });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Household child profiles are not available yet. Run migrations and retry." });
    }
    console.error("[church-life/household/children] delete error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get(
  "/admin/church-life/children-household",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("children.household.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const parentRef = String(req.query?.parentRef || req.query?.memberRef || req.query?.memberId || "").trim();
      if (!parentRef) return res.status(400).json({ error: "parentRef is required." });

      const parent = await findChurchMemberByReference(churchId, parentRef);
      if (!parent) return res.status(404).json({ error: "Parent member not found in this church." });

      const limit = parseChurchLifeLimit(req.query?.limit, 80, 200);
      const rows = await listChurchHouseholdChildren({
        churchId,
        parentMemberPk: parent.id,
        includeInactive: false,
        limit,
      });

      return res.json({
        ok: true,
        parent: {
          memberPk: parent.id,
          memberId: parent.member_id || null,
          fullName: parent.full_name || null,
          phone: parent.phone || null,
          email: parent.email || null,
        },
        children: rows.map(normalizeChurchHouseholdChildRow),
        meta: { limit, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Children's Church household profiles are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/children-household] error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  ["/admin/church-life/children-check-ins", "/admin/church-life/children/check-ins"],
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("children.checkins.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const serviceId = String(req.body?.serviceId || "").trim();
      const requestedHouseholdChildId = String(req.body?.householdChildId || "").trim();
      const walkInChildName = String(req.body?.childName || "")
        .trim()
        .slice(0, 160);
      const walkInParentName = String(req.body?.parentName || "")
        .trim()
        .slice(0, 160);
      const walkInParentPhone = String(req.body?.parentPhone || "")
        .trim()
        .slice(0, 40);
      const walkInParentEmail = String(req.body?.parentEmail || "")
        .trim()
        .slice(0, 160);
      const checkInMethod = normalizeChurchChildCheckinMethod(req.body?.checkInMethod || req.body?.method || "TEACHER");
      const checkInNotes = String(req.body?.checkInNotes || req.body?.notes || "")
        .trim()
        .slice(0, 250) || null;
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.body?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      if (!UUID_REGEX.test(serviceId)) return res.status(400).json({ error: "serviceId is required." });

      const hasHouseholdChildId = UUID_REGEX.test(requestedHouseholdChildId);
      if (!hasHouseholdChildId && !walkInChildName) {
        return res.status(400).json({ error: "householdChildId or childName is required." });
      }

      const service = await findChurchServiceById(churchId, serviceId, campusId);
      if (!service) return res.status(404).json({ error: "Service not found." });
      if (!service.published) return res.status(400).json({ error: "This service is not available for children check-in." });
      const finalCampusId = campusId || service.campusId || null;

      let child = null;
      let householdChildId = null;
      let parentMemberPk = null;
      let parentMemberId = null;
      let parentName = null;
      let parentPhone = null;
      let parentEmail = null;
      let childName = walkInChildName || null;

      if (hasHouseholdChildId) {
        householdChildId = requestedHouseholdChildId;
        child = await findChurchHouseholdChildById(churchId, householdChildId);
        if (!child || child.active === false) return res.status(404).json({ error: "Child profile not found or inactive." });

        parentMemberPk = child.parentMemberPk;
        parentMemberId = child.parentMemberId || null;
        parentName = child.parentName || null;
        parentPhone = child.parentPhone || null;
        parentEmail = child.parentEmail || null;
        childName = child.childName || child.childMemberId || walkInChildName || "Child";
      } else {
        parentName = walkInParentName || null;
        parentPhone = walkInParentPhone || null;
        parentEmail = walkInParentEmail || null;
        childName = walkInChildName;
      }

      if (householdChildId) {
        const existingOpen = await db.oneOrNone(
          `
          select id
          from church_children_checkins
          where church_id=$1
            and service_id=$2
            and household_child_id=$3
            and checked_out_at is null
          limit 1
          `,
          [churchId, serviceId, householdChildId]
        );
        if (existingOpen?.id) {
          const current = await findChurchChildCheckInRecord(churchId, existingOpen.id);
          return res.status(409).json({
            error: "This child is already checked in for the selected service.",
            code: "CHILD_ALREADY_CHECKED_IN",
            checkIn: current ? normalizeChurchChildCheckInRow(current, { includeParentContact: true }) : null,
          });
        }
      }

      const insertRow = await db.one(
        `
        insert into church_children_checkins (
          church_id,
          campus_id,
          service_id,
          household_child_id,
          parent_member_pk,
          parent_member_id,
          parent_name,
          parent_phone,
          parent_email,
          child_name,
          checkin_method,
          checked_in_at,
          checked_in_by_member_pk,
          checked_in_by_role,
          checkin_notes
        )
        values (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now(),$12,$13,$14
        )
        returning id
        `,
        [
          churchId,
          finalCampusId,
          serviceId,
          householdChildId,
          parentMemberPk,
          parentMemberId,
          parentName,
          parentPhone,
          parentEmail,
          childName,
          checkInMethod,
          req.user?.id || null,
          normalizeChurchStaffRole(req.user?.role || "teacher"),
          checkInNotes,
        ]
      );

      const saved = await findChurchChildCheckInRecord(churchId, insertRow.id);

      return res.status(201).json({
        ok: true,
        checkIn: normalizeChurchChildCheckInRow(saved, { includeParentContact: true }),
        service: normalizeChurchServiceRow(service),
      });
    } catch (err) {
      if (err?.code === "23505") {
        return res.status(409).json({ error: "This child is already checked in for the selected service." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Children's Church check-ins are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/children-check-ins] create error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  ["/admin/church-life/children-check-ins", "/admin/church-life/children/check-ins"],
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("children.checkins.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const requestedServiceId = String(req.query?.serviceId || "").trim();
      const statusRaw = String(req.query?.status || "open")
        .trim()
        .toLowerCase();
      const status = statusRaw === "all" || statusRaw === "checked_out" ? statusRaw : "open";
      const limit = parseChurchLifeLimit(req.query?.limit, 120, 400);
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      let service = null;
      if (requestedServiceId) {
        if (!UUID_REGEX.test(requestedServiceId)) return res.status(400).json({ error: "serviceId must be a UUID." });
        service = await findChurchServiceById(churchId, requestedServiceId, campusId);
        if (!service) return res.status(404).json({ error: "Service not found." });
      } else {
        service = await findLatestChurchService(churchId, campusId);
      }

      if (!service) {
        return res.json({
          ok: true,
          service: null,
          checkIns: [],
          summary: { total: 0, checkedInCount: 0, checkedOutCount: 0, lastCheckInAt: null, lastPickupAt: null },
          meta: { status, limit, campusId: campusId || null, returned: 0, generatedAt: new Date().toISOString() },
        });
      }

      const rows = await listChurchChildCheckIns({
        churchId,
        serviceId: service.id,
        campusId,
        status,
        limit,
      });
      const access = getChurchLifeAccess(req);
      const canReadContact =
        hasChurchLifePermission(access, "children.contact.read") || hasChurchLifePermission(access, "checkins.contact.read");
      const normalizedRows = rows.map((row) =>
        normalizeChurchChildCheckInRow(row, { includeParentContact: canReadContact })
      );
      const checkedOutCount = normalizedRows.filter((row) => row.status === "CHECKED_OUT").length;
      const checkedInCount = normalizedRows.length - checkedOutCount;

      return res.json({
        ok: true,
        service: normalizeChurchServiceRow(service),
        checkIns: normalizedRows,
        summary: {
          total: normalizedRows.length,
          checkedInCount,
          checkedOutCount,
          lastCheckInAt: normalizedRows.length ? normalizedRows[0].checkedInAt || null : null,
          lastPickupAt:
            normalizedRows
              .filter((row) => row.checkedOutAt)
              .sort((a, b) => String(b.checkedOutAt || "").localeCompare(String(a.checkedOutAt || "")))[0]?.checkedOutAt || null,
        },
        meta: { status, limit, campusId: campusId || null, returned: normalizedRows.length, generatedAt: new Date().toISOString() },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Children's Church check-ins are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/children-check-ins] list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  [
    "/admin/church-life/children-check-ins/:checkInId/pickup",
    "/admin/church-life/children/check-ins/:checkInId/pickup",
  ],
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("children.pickups.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const checkInId = String(req.params?.checkInId || "").trim();
      if (!UUID_REGEX.test(checkInId)) return res.status(400).json({ error: "Invalid checkInId" });

      const defaultMethod = normalizeChurchStaffRole(req.user?.role) === "teacher" ? "TEACHER" : "USHER";
      const checkoutMethod = normalizeChurchChildCheckoutMethod(req.body?.checkoutMethod || req.body?.method || defaultMethod);
      const checkoutNotes = String(req.body?.checkoutNotes || req.body?.notes || "")
        .trim()
        .slice(0, 250) || null;

      const existing = await findChurchChildCheckInRecord(churchId, checkInId);
      if (!existing) return res.status(404).json({ error: "Children check-in record not found." });
      if (existing.checkedOutAt) {
        return res.status(409).json({
          error: "Child is already checked out.",
          code: "CHILD_ALREADY_CHECKED_OUT",
          checkIn: normalizeChurchChildCheckInRow(existing, { includeParentContact: true }),
        });
      }

      const updated = await db.oneOrNone(
        `
        update church_children_checkins
        set
          checked_out_at = now(),
          checkout_method = $3,
          checked_out_by_member_pk = $4,
          checked_out_by_role = $5,
          checkout_notes = $6,
          updated_at = now()
        where id=$1 and church_id=$2 and checked_out_at is null
        returning id
        `,
        [
          checkInId,
          churchId,
          checkoutMethod,
          req.user?.id || null,
          normalizeChurchStaffRole(req.user?.role || "teacher"),
          checkoutNotes,
        ]
      );
      if (!updated) return res.status(409).json({ error: "Child is already checked out." });

      const saved = await findChurchChildCheckInRecord(churchId, checkInId);
      return res.json({
        ok: true,
        checkIn: normalizeChurchChildCheckInRow(saved, { includeParentContact: true }),
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Children's Church check-ins are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/children-check-ins] pickup error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(["/church-life/children-check-ins", "/church-life/children/check-ins"], requireAuth, requireChurchGrowthActive, async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    const requestedServiceId = String(req.query?.serviceId || "").trim();
    const statusRaw = String(req.query?.status || "open")
      .trim()
      .toLowerCase();
    const status = statusRaw === "all" || statusRaw === "checked_out" ? statusRaw : "open";
    const limit = parseChurchLifeLimit(req.query?.limit, 80, 300);
    let campusId = null;
    try {
      campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
    } catch (campusErr) {
      return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
    }

    let service = null;
    if (requestedServiceId) {
      if (!UUID_REGEX.test(requestedServiceId)) return res.status(400).json({ error: "serviceId must be a UUID." });
      service = await findChurchServiceById(churchId, requestedServiceId, campusId);
      if (!service) return res.status(404).json({ error: "Service not found." });
    }

    const rows = await listChurchChildCheckIns({
      churchId,
      serviceId: service?.id || null,
      campusId,
      status,
      parentMemberPk: req.user.id,
      limit,
    });
    const normalizedRows = rows.map((row) => normalizeChurchChildCheckInRow(row, { includeParentContact: true }));
    const checkedOutCount = normalizedRows.filter((row) => row.status === "CHECKED_OUT").length;
    const checkedInCount = normalizedRows.length - checkedOutCount;

    return res.json({
      ok: true,
      service: service ? normalizeChurchServiceRow(service) : null,
      checkIns: normalizedRows,
      summary: {
        total: normalizedRows.length,
        checkedInCount,
        checkedOutCount,
      },
      meta: { status, limit, campusId: campusId || null, returned: normalizedRows.length },
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Children's Church check-ins are not available yet. Run migrations and retry." });
    }
    console.error("[church-life/children-check-ins] list error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post(
  ["/church-life/children-check-ins/:checkInId/pickup", "/church-life/children/check-ins/:checkInId/pickup"],
  requireAuth,
  requireChurchGrowthActive,
  async (req, res) => {
  try {
    const churchId = requireChurch(req, res);
    if (!churchId) return;

    const checkInId = String(req.params?.checkInId || "").trim();
    if (!UUID_REGEX.test(checkInId)) return res.status(400).json({ error: "Invalid checkInId" });

    const checkoutMethod = normalizeChurchChildCheckoutMethod(req.body?.checkoutMethod || req.body?.method || "PARENT", "PARENT");
    const checkoutNotes = String(req.body?.checkoutNotes || req.body?.notes || "")
      .trim()
      .slice(0, 250) || null;

    const existing = await findChurchChildCheckInRecord(churchId, checkInId, { parentMemberPk: req.user.id });
    if (!existing) return res.status(404).json({ error: "Children check-in record not found." });
    if (existing.checkedOutAt) {
      return res.status(409).json({
        error: "Child is already checked out.",
        code: "CHILD_ALREADY_CHECKED_OUT",
        checkIn: normalizeChurchChildCheckInRow(existing, { includeParentContact: true }),
      });
    }

    const updated = await db.oneOrNone(
      `
      update church_children_checkins
      set
        checked_out_at = now(),
        checkout_method = $3,
        checked_out_by_member_pk = $4,
        checked_out_by_role = $5,
        checkout_notes = $6,
        updated_at = now()
      where id=$1
        and church_id=$2
        and parent_member_pk=$4
        and checked_out_at is null
      returning id
      `,
      [checkInId, churchId, checkoutMethod, req.user.id, String(req.user?.role || "member").toLowerCase(), checkoutNotes]
    );
    if (!updated) return res.status(409).json({ error: "Child is already checked out." });

    const saved = await findChurchChildCheckInRecord(churchId, checkInId, { parentMemberPk: req.user.id });
    return res.json({
      ok: true,
      checkIn: normalizeChurchChildCheckInRow(saved, { includeParentContact: true }),
    });
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") {
      return res.status(503).json({ error: "Children's Church check-ins are not available yet. Run migrations and retry." });
    }
    console.error("[church-life/children-check-ins] pickup error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get(
  "/admin/church-life/groups",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("groups.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const includeInactive = toBoolean(req.query?.includeInactive) === true;
      const limit = parseChurchLifeLimit(req.query?.limit, 120, 500);

      const rows = await db.manyOrNone(
        `
        select
          g.id,
          g.church_id as "churchId",
          g.name,
          g.code,
          g.group_type as "groupType",
          g.description,
          g.leader_member_pk as "leaderMemberPk",
          leader.member_id as "leaderMemberId",
          leader.full_name as "leaderName",
          g.active,
          g.created_at as "createdAt",
          g.updated_at as "updatedAt",
          coalesce(members.count, 0)::int as "memberCount"
        from church_groups g
        left join members leader on leader.id = g.leader_member_pk
        left join lateral (
          select count(*)::int as count
          from church_group_members gm
          where gm.group_id = g.id and gm.active = true
        ) members on true
        where g.church_id=$1
          and ($2::boolean = true or g.active = true)
        order by g.active desc, g.name asc
        limit $3
        `,
        [churchId, includeInactive, limit]
      );

      return res.json({
        ok: true,
        groups: rows.map(normalizeChurchGroupRow),
        meta: { limit, includeInactive, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church CRM groups are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/groups] list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/groups",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("groups.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const name = String(req.body?.name || "").trim().slice(0, 140);
      if (!name) return res.status(400).json({ error: "name is required." });
      const code = String(req.body?.code || "").trim().slice(0, 40) || null;
      const groupType = normalizeChurchGroupType(req.body?.groupType);
      const description = String(req.body?.description || "").trim().slice(0, 2000) || null;
      const active = toBoolean(req.body?.active) !== false;
      const leaderMemberPk = String(req.body?.leaderMemberPk || "").trim();
      let leader = null;
      if (leaderMemberPk) {
        leader = await findMemberInChurchByUuid(churchId, leaderMemberPk);
        if (!leader) return res.status(400).json({ error: "leaderMemberPk is not in this church." });
      }

      const row = await db.one(
        `
        insert into church_groups (
          church_id, name, code, group_type, description, leader_member_pk, active, created_by, updated_by
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$8)
        returning
          id,
          church_id as "churchId",
          name,
          code,
          group_type as "groupType",
          description,
          leader_member_pk as "leaderMemberPk",
          active,
          created_at as "createdAt",
          updated_at as "updatedAt"
        `,
        [churchId, name, code, groupType, description, leader?.id || null, active, req.user?.id || null]
      );

      return res.status(201).json({
        ok: true,
        group: normalizeChurchGroupRow({
          ...row,
          leaderMemberId: leader?.memberId || null,
          leaderName: leader?.fullName || null,
        }),
      });
    } catch (err) {
      if (err?.code === "23505") {
        return res.status(409).json({ error: "A group with this name already exists." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church CRM groups are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/groups] create error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/church-life/groups/:groupId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("groups.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const groupId = String(req.params?.groupId || "").trim();
      if (!UUID_REGEX.test(groupId)) return res.status(400).json({ error: "Invalid groupId" });

      const current = await db.oneOrNone(
        `
        select
          id,
          name,
          code,
          group_type as "groupType",
          description,
          leader_member_pk as "leaderMemberPk",
          active
        from church_groups
        where id=$1 and church_id=$2
        limit 1
        `,
        [groupId, churchId]
      );
      if (!current) return res.status(404).json({ error: "Group not found." });

      const name = String(req.body?.name ?? current.name ?? "")
        .trim()
        .slice(0, 140);
      if (!name) return res.status(400).json({ error: "name is required." });
      const code = String(typeof req.body?.code === "undefined" ? current.code || "" : req.body?.code || "")
        .trim()
        .slice(0, 40) || null;
      const groupType = normalizeChurchGroupType(req.body?.groupType || current.groupType);
      const description =
        String(typeof req.body?.description === "undefined" ? current.description || "" : req.body?.description || "")
          .trim()
          .slice(0, 2000) || null;
      const active = typeof req.body?.active === "undefined" ? current.active !== false : toBoolean(req.body?.active) !== false;

      let leaderMemberPk = current.leaderMemberPk || null;
      if (typeof req.body?.leaderMemberPk !== "undefined") {
        const requestedLeader = String(req.body?.leaderMemberPk || "").trim();
        if (!requestedLeader) {
          leaderMemberPk = null;
        } else {
          const leader = await findMemberInChurchByUuid(churchId, requestedLeader);
          if (!leader) return res.status(400).json({ error: "leaderMemberPk is not in this church." });
          leaderMemberPk = leader.id;
        }
      }

      const row = await db.one(
        `
        update church_groups
        set
          name = $3,
          code = $4,
          group_type = $5,
          description = $6,
          leader_member_pk = $7,
          active = $8,
          updated_by = $9,
          updated_at = now()
        where id = $1 and church_id = $2
        returning
          id,
          church_id as "churchId",
          name,
          code,
          group_type as "groupType",
          description,
          leader_member_pk as "leaderMemberPk",
          active,
          created_at as "createdAt",
          updated_at as "updatedAt"
        `,
        [groupId, churchId, name, code, groupType, description, leaderMemberPk, active, req.user?.id || null]
      );

      const leaderNameRow = leaderMemberPk
        ? await db.oneOrNone("select member_id as \"leaderMemberId\", full_name as \"leaderName\" from members where id=$1", [leaderMemberPk])
        : null;

      return res.json({
        ok: true,
        group: normalizeChurchGroupRow({
          ...row,
          leaderMemberId: leaderNameRow?.leaderMemberId || null,
          leaderName: leaderNameRow?.leaderName || null,
        }),
      });
    } catch (err) {
      if (err?.code === "23505") {
        return res.status(409).json({ error: "A group with this name already exists." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church CRM groups are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/groups] patch error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/groups/:groupId/members",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("groups.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const groupId = String(req.params?.groupId || "").trim();
      if (!UUID_REGEX.test(groupId)) return res.status(400).json({ error: "Invalid groupId" });

      const group = await db.oneOrNone(
        "select id, name from church_groups where id=$1 and church_id=$2 limit 1",
        [groupId, churchId]
      );
      if (!group) return res.status(404).json({ error: "Group not found." });

      const rows = await db.manyOrNone(
        `
        select
          gm.id,
          gm.church_id as "churchId",
          gm.group_id as "groupId",
          gm.member_pk as "memberPk",
          gm.member_id as "memberId",
          m.full_name as "fullName",
          m.phone,
          m.email,
          gm.member_role as role,
          gm.joined_on as "joinedOn",
          gm.active,
          gm.notes,
          gm.created_at as "createdAt",
          gm.updated_at as "updatedAt"
        from church_group_members gm
        join members m on m.id = gm.member_pk
        where gm.church_id=$1 and gm.group_id=$2
        order by gm.active desc, gm.member_role asc, m.full_name asc
        `,
        [churchId, groupId]
      );

      return res.json({
        ok: true,
        group: { id: group.id, name: group.name },
        members: rows.map(normalizeChurchGroupMemberRow),
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church CRM groups are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/groups] members list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.put(
  "/admin/church-life/groups/:groupId/members",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("groups.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const groupId = String(req.params?.groupId || "").trim();
      if (!UUID_REGEX.test(groupId)) return res.status(400).json({ error: "Invalid groupId" });

      const group = await db.oneOrNone(
        "select id, name from church_groups where id=$1 and church_id=$2 limit 1",
        [groupId, churchId]
      );
      if (!group) return res.status(404).json({ error: "Group not found." });

      const rows = Array.isArray(req.body?.members) ? req.body.members : [];
      if (rows.length > 2000) return res.status(400).json({ error: "Too many member rows (max 2000)." });

      const normalized = rows
        .map((row) => {
          const memberPk = String(row?.memberPk || "").trim();
          const joinedOn = parseChurchLifeDate(row?.joinedOn);
          const hasJoinInput = !(typeof row?.joinedOn === "undefined" || row?.joinedOn === null || row?.joinedOn === "");
          return {
            memberPk,
            memberRole: normalizeChurchGroupMemberRole(row?.memberRole || row?.role),
            joinedOn: hasJoinInput ? joinedOn : null,
            active: toBoolean(row?.active) !== false,
            notes: String(row?.notes || "").trim().slice(0, 500) || null,
            joinDateProvided: hasJoinInput,
          };
        })
        .filter((row) => UUID_REGEX.test(row.memberPk));

      const hasInvalidJoinDate = normalized.some((row) => row.joinDateProvided && !row.joinedOn);
      if (hasInvalidJoinDate) {
        return res.status(400).json({ error: "joinedOn must be YYYY-MM-DD when provided." });
      }

      const memberPkSet = Array.from(new Set(normalized.map((row) => row.memberPk)));
      if (memberPkSet.length) {
        const validRows = await db.manyOrNone("select id::text as id from members where church_id=$1 and id = any($2::uuid[])", [
          churchId,
          memberPkSet,
        ]);
        const validSet = new Set(validRows.map((row) => String(row.id)));
        const invalid = memberPkSet.filter((id) => !validSet.has(id));
        if (invalid.length) {
          return res.status(400).json({
            error: "Some members are not in this church.",
            invalidMemberPks: invalid.slice(0, 20),
          });
        }
      }

      await db.tx(async (t) => {
        await t.none("delete from church_group_members where church_id=$1 and group_id=$2", [churchId, groupId]);
        for (const row of normalized) {
          const member = await t.one(
            "select member_id from members where church_id=$1 and id=$2",
            [churchId, row.memberPk]
          );
          await t.none(
            `
            insert into church_group_members (
              church_id, group_id, member_pk, member_id, member_role, joined_on, active, notes, created_by, updated_by
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9)
            `,
            [
              churchId,
              groupId,
              row.memberPk,
              member?.member_id || null,
              row.memberRole,
              row.joinDateProvided ? row.joinedOn : null,
              row.active,
              row.notes,
              req.user?.id || null,
            ]
          );
        }
      });

      return res.json({ ok: true, group: { id: group.id, name: group.name }, saved: normalized.length });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church CRM groups are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/groups] members save error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/groups/:groupId/meetings",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("groups.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const groupId = String(req.params?.groupId || "").trim();
      if (!UUID_REGEX.test(groupId)) return res.status(400).json({ error: "Invalid groupId" });
      const group = await findChurchGroupById(churchId, groupId);
      if (!group) return res.status(404).json({ error: "Group not found." });

      const statusRaw = String(req.query?.status || "").trim();
      if (statusRaw && !CHURCH_GROUP_MEETING_STATUSES.has(normalizeUpperToken(statusRaw))) {
        return res.status(400).json({ error: "status must be SCHEDULED, COMPLETED, or CANCELLED when provided." });
      }
      const status = statusRaw ? normalizeChurchGroupMeetingStatus(statusRaw) : null;
      const limit = parseChurchLifeLimit(req.query?.limit, 80, 300);

      const rows = await db.manyOrNone(
        `
        select
          gm.id,
          gm.church_id as "churchId",
          gm.group_id as "groupId",
          gm.meeting_date as "meetingDate",
          gm.starts_at as "startsAt",
          gm.ends_at as "endsAt",
          gm.location,
          gm.notes,
          gm.status,
          gm.created_at as "createdAt",
          gm.updated_at as "updatedAt",
          coalesce(agg.total, 0)::int as "attendanceCount",
          coalesce(agg.present_count, 0)::int as "presentCount",
          coalesce(agg.late_count, 0)::int as "lateCount",
          coalesce(agg.absent_count, 0)::int as "absentCount",
          coalesce(agg.excused_count, 0)::int as "excusedCount"
        from church_group_meetings gm
        left join lateral (
          select
            count(*)::int as total,
            count(*) filter (where status='PRESENT')::int as present_count,
            count(*) filter (where status='LATE')::int as late_count,
            count(*) filter (where status='ABSENT')::int as absent_count,
            count(*) filter (where status='EXCUSED')::int as excused_count
          from church_group_attendance cga
          where cga.meeting_id = gm.id
        ) agg on true
        where gm.church_id = $1
          and gm.group_id = $2
          and ($3::text is null or gm.status = $3::text)
        order by gm.meeting_date desc, coalesce(gm.starts_at, gm.created_at) desc
        limit $4
        `,
        [churchId, groupId, status, limit]
      );

      return res.json({
        ok: true,
        group: normalizeChurchGroupRow(group),
        meetings: rows.map(normalizeChurchGroupMeetingRow),
        meta: { limit, status, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life group meetings are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/groups] meetings list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/groups/:groupId/meetings",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("groups.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const groupId = String(req.params?.groupId || "").trim();
      if (!UUID_REGEX.test(groupId)) return res.status(400).json({ error: "Invalid groupId" });
      const group = await findChurchGroupById(churchId, groupId);
      if (!group) return res.status(404).json({ error: "Group not found." });

      const meetingDate = parseChurchLifeDate(req.body?.meetingDate);
      if (!meetingDate) return res.status(400).json({ error: "meetingDate is required (YYYY-MM-DD)." });
      const startsAtInput = req.body?.startsAt;
      const startsAt = parseChurchLifeDateTimeOrNull(startsAtInput);
      if ((startsAtInput || startsAtInput === "") && startsAtInput !== null && startsAtInput !== "" && !startsAt) {
        return res.status(400).json({ error: "startsAt must be a valid ISO datetime when provided." });
      }
      const endsAtInput = req.body?.endsAt;
      const endsAt = parseChurchLifeDateTimeOrNull(endsAtInput);
      if ((endsAtInput || endsAtInput === "") && endsAtInput !== null && endsAtInput !== "" && !endsAt) {
        return res.status(400).json({ error: "endsAt must be a valid ISO datetime when provided." });
      }
      if (startsAt && endsAt && Date.parse(endsAt) < Date.parse(startsAt)) {
        return res.status(400).json({ error: "endsAt cannot be before startsAt." });
      }
      const status = normalizeChurchGroupMeetingStatus(req.body?.status || "SCHEDULED");
      const location = String(req.body?.location || "").trim().slice(0, 250) || null;
      const notes = String(req.body?.notes || "").trim().slice(0, 2000) || null;

      const row = await db.one(
        `
        insert into church_group_meetings (
          church_id, group_id, meeting_date, starts_at, ends_at, location, notes, status, created_by, updated_by
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9)
        returning
          id,
          church_id as "churchId",
          group_id as "groupId",
          meeting_date as "meetingDate",
          starts_at as "startsAt",
          ends_at as "endsAt",
          location,
          notes,
          status,
          created_at as "createdAt",
          updated_at as "updatedAt"
        `,
        [churchId, groupId, meetingDate, startsAt, endsAt, location, notes, status, req.user?.id || null]
      );

      return res.status(201).json({
        ok: true,
        group: normalizeChurchGroupRow(group),
        meeting: normalizeChurchGroupMeetingRow(row),
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life group meetings are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/groups] meetings create error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/church-life/groups/:groupId/meetings/:meetingId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("groups.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const groupId = String(req.params?.groupId || "").trim();
      const meetingId = String(req.params?.meetingId || "").trim();
      if (!UUID_REGEX.test(groupId)) return res.status(400).json({ error: "Invalid groupId" });
      if (!UUID_REGEX.test(meetingId)) return res.status(400).json({ error: "Invalid meetingId" });
      const group = await findChurchGroupById(churchId, groupId);
      if (!group) return res.status(404).json({ error: "Group not found." });

      const current = await db.oneOrNone(
        `
        select
          id,
          meeting_date as "meetingDate",
          starts_at as "startsAt",
          ends_at as "endsAt",
          location,
          notes,
          status
        from church_group_meetings
        where church_id=$1 and group_id=$2 and id=$3
        limit 1
        `,
        [churchId, groupId, meetingId]
      );
      if (!current) return res.status(404).json({ error: "Meeting not found." });

      const meetingDateInput = req.body?.meetingDate;
      const meetingDate = typeof meetingDateInput === "undefined" ? formatDateIsoLike(current.meetingDate) : parseChurchLifeDate(meetingDateInput);
      if (!meetingDate) return res.status(400).json({ error: "meetingDate must be YYYY-MM-DD when provided." });

      const startsAtInput = typeof req.body?.startsAt === "undefined" ? current.startsAt : req.body?.startsAt;
      const startsAt = parseChurchLifeDateTimeOrNull(startsAtInput);
      if (startsAtInput !== null && startsAtInput !== "" && typeof startsAtInput !== "undefined" && !startsAt) {
        return res.status(400).json({ error: "startsAt must be a valid ISO datetime when provided." });
      }
      const endsAtInput = typeof req.body?.endsAt === "undefined" ? current.endsAt : req.body?.endsAt;
      const endsAt = parseChurchLifeDateTimeOrNull(endsAtInput);
      if (endsAtInput !== null && endsAtInput !== "" && typeof endsAtInput !== "undefined" && !endsAt) {
        return res.status(400).json({ error: "endsAt must be a valid ISO datetime when provided." });
      }
      if (startsAt && endsAt && Date.parse(endsAt) < Date.parse(startsAt)) {
        return res.status(400).json({ error: "endsAt cannot be before startsAt." });
      }

      const status =
        typeof req.body?.status === "undefined"
          ? normalizeChurchGroupMeetingStatus(current.status)
          : normalizeChurchGroupMeetingStatus(req.body?.status);
      const location =
        String(typeof req.body?.location === "undefined" ? current.location || "" : req.body?.location || "")
          .trim()
          .slice(0, 250) || null;
      const notes =
        String(typeof req.body?.notes === "undefined" ? current.notes || "" : req.body?.notes || "")
          .trim()
          .slice(0, 2000) || null;

      const row = await db.one(
        `
        update church_group_meetings
        set
          meeting_date = $4,
          starts_at = $5,
          ends_at = $6,
          location = $7,
          notes = $8,
          status = $9,
          updated_by = $10,
          updated_at = now()
        where church_id=$1 and group_id=$2 and id=$3
        returning
          id,
          church_id as "churchId",
          group_id as "groupId",
          meeting_date as "meetingDate",
          starts_at as "startsAt",
          ends_at as "endsAt",
          location,
          notes,
          status,
          created_at as "createdAt",
          updated_at as "updatedAt"
        `,
        [churchId, groupId, meetingId, meetingDate, startsAt, endsAt, location, notes, status, req.user?.id || null]
      );

      return res.json({
        ok: true,
        group: normalizeChurchGroupRow(group),
        meeting: normalizeChurchGroupMeetingRow(row),
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life group meetings are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/groups] meetings patch error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/groups/:groupId/meetings/:meetingId/attendance",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("groups.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const groupId = String(req.params?.groupId || "").trim();
      const meetingId = String(req.params?.meetingId || "").trim();
      if (!UUID_REGEX.test(groupId)) return res.status(400).json({ error: "Invalid groupId" });
      if (!UUID_REGEX.test(meetingId)) return res.status(400).json({ error: "Invalid meetingId" });
      const group = await findChurchGroupById(churchId, groupId);
      if (!group) return res.status(404).json({ error: "Group not found." });

      const meeting = await db.oneOrNone(
        `
        select
          id,
          church_id as "churchId",
          group_id as "groupId",
          meeting_date as "meetingDate",
          starts_at as "startsAt",
          ends_at as "endsAt",
          location,
          notes,
          status,
          created_at as "createdAt",
          updated_at as "updatedAt"
        from church_group_meetings
        where church_id=$1 and group_id=$2 and id=$3
        limit 1
        `,
        [churchId, groupId, meetingId]
      );
      if (!meeting) return res.status(404).json({ error: "Meeting not found." });

      const rows = await db.manyOrNone(
        `
        select
          gmem.member_pk as "memberPk",
          gmem.member_id as "memberId",
          gmem.member_role as role,
          gmem.active,
          m.full_name as "fullName",
          m.phone,
          m.email,
          cga.id,
          cga.church_id as "churchId",
          cga.group_id as "groupId",
          cga.meeting_id as "meetingId",
          cga.status,
          cga.notes,
          cga.marked_by as "markedBy",
          marker.full_name as "markedByName",
          cga.created_at as "createdAt",
          cga.updated_at as "updatedAt"
        from church_group_members gmem
        join members m on m.id = gmem.member_pk
        left join church_group_attendance cga
          on cga.meeting_id = $3
         and cga.member_pk = gmem.member_pk
        left join members marker on marker.id = cga.marked_by
        where gmem.church_id = $1 and gmem.group_id = $2
        order by gmem.active desc, gmem.member_role asc, m.full_name asc
        `,
        [churchId, groupId, meetingId]
      );

      const summary = rows.reduce(
        (acc, row) => {
          const status = normalizeChurchGroupAttendanceStatus(row?.status || "", "");
          if (!status) {
            acc.unmarked += 1;
            return acc;
          }
          acc.marked += 1;
          if (status === "PRESENT") acc.present += 1;
          else if (status === "LATE") acc.late += 1;
          else if (status === "ABSENT") acc.absent += 1;
          else if (status === "EXCUSED") acc.excused += 1;
          return acc;
        },
        { total: rows.length, marked: 0, unmarked: 0, present: 0, late: 0, absent: 0, excused: 0 }
      );

      return res.json({
        ok: true,
        group: normalizeChurchGroupRow(group),
        meeting: normalizeChurchGroupMeetingRow({
          ...meeting,
          attendanceCount: summary.marked,
          presentCount: summary.present,
          lateCount: summary.late,
          absentCount: summary.absent,
          excusedCount: summary.excused,
        }),
        attendance: rows.map(normalizeChurchGroupAttendanceRow),
        summary,
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life group attendance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/groups] attendance list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.put(
  "/admin/church-life/groups/:groupId/meetings/:meetingId/attendance",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("groups.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const groupId = String(req.params?.groupId || "").trim();
      const meetingId = String(req.params?.meetingId || "").trim();
      if (!UUID_REGEX.test(groupId)) return res.status(400).json({ error: "Invalid groupId" });
      if (!UUID_REGEX.test(meetingId)) return res.status(400).json({ error: "Invalid meetingId" });
      const group = await findChurchGroupById(churchId, groupId);
      if (!group) return res.status(404).json({ error: "Group not found." });

      const meeting = await db.oneOrNone(
        `
        select id
        from church_group_meetings
        where church_id=$1 and group_id=$2 and id=$3
        limit 1
        `,
        [churchId, groupId, meetingId]
      );
      if (!meeting) return res.status(404).json({ error: "Meeting not found." });

      const inputRows = Array.isArray(req.body?.attendance) ? req.body.attendance : [];
      if (inputRows.length > 2000) return res.status(400).json({ error: "Too many attendance rows (max 2000)." });
      const markerMemberPk = parseUuidOrNull(req.user?.id);

      const invalidStatuses = [];
      const normalized = [];
      for (const row of inputRows) {
        const memberPk = String(row?.memberPk || "").trim();
        if (!UUID_REGEX.test(memberPk)) continue;
        const rawStatus = String(row?.status || "").trim();
        if (rawStatus && !CHURCH_GROUP_ATTENDANCE_STATUSES.has(normalizeUpperToken(rawStatus))) {
          invalidStatuses.push(rawStatus);
          continue;
        }
        normalized.push({
          memberPk,
          status: normalizeChurchGroupAttendanceStatus(rawStatus || "PRESENT"),
          notes: String(row?.notes || "").trim().slice(0, 500) || null,
        });
      }
      if (invalidStatuses.length) {
        return res.status(400).json({
          error: "attendance.status contains invalid values.",
          invalidStatuses: Array.from(new Set(invalidStatuses)).slice(0, 20),
        });
      }
      const deduped = Array.from(new Map(normalized.map((row) => [row.memberPk, row])).values());
      const memberPkSet = deduped.map((row) => row.memberPk);

      if (memberPkSet.length) {
        const membershipRows = await db.manyOrNone(
          `
          select member_pk::text as "memberPk"
          from church_group_members
          where church_id = $1
            and group_id = $2
            and member_pk = any($3::uuid[])
          `,
          [churchId, groupId, memberPkSet]
        );
        const allowed = new Set(membershipRows.map((row) => String(row.memberPk)));
        const invalidMembers = memberPkSet.filter((memberPk) => !allowed.has(memberPk));
        if (invalidMembers.length) {
          return res.status(400).json({
            error: "Some members are not in this group.",
            invalidMemberPks: invalidMembers.slice(0, 20),
          });
        }
      }

      await db.tx(async (t) => {
        await t.none("delete from church_group_attendance where church_id=$1 and group_id=$2 and meeting_id=$3", [
          churchId,
          groupId,
          meetingId,
        ]);
        for (const row of deduped) {
          const member = await t.oneOrNone(
            "select member_id from members where church_id=$1 and id=$2",
            [churchId, row.memberPk]
          );
          await t.none(
            `
            insert into church_group_attendance (
              church_id, group_id, meeting_id, member_pk, member_id, status, notes, marked_by
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8)
            `,
            [churchId, groupId, meetingId, row.memberPk, member?.member_id || null, row.status, row.notes, markerMemberPk]
          );
        }
      });

      const summaryRow = await db.one(
        `
        select
          count(*)::int as total,
          count(*) filter (where status='PRESENT')::int as present,
          count(*) filter (where status='LATE')::int as late,
          count(*) filter (where status='ABSENT')::int as absent,
          count(*) filter (where status='EXCUSED')::int as excused
        from church_group_attendance
        where church_id = $1 and group_id = $2 and meeting_id = $3
        `,
        [churchId, groupId, meetingId]
      );

      return res.json({
        ok: true,
        group: normalizeChurchGroupRow(group),
        meeting: { id: meetingId },
        saved: deduped.length,
        summary: {
          total: Number(summaryRow?.total || 0),
          present: Number(summaryRow?.present || 0),
          late: Number(summaryRow?.late || 0),
          absent: Number(summaryRow?.absent || 0),
          excused: Number(summaryRow?.excused || 0),
        },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life group attendance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/groups] attendance save error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/followups",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("followups.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const status = normalizeChurchFollowupStatus(req.query?.status);
      const type = normalizeChurchFollowupType(req.query?.type);
      const assignedMemberId = String(req.query?.assignedMemberId || "").trim();
      const limit = parseChurchLifeLimit(req.query?.limit, 120, 400);
      const search = String(req.query?.search || "").trim();
      const searchLike = `%${search}%`;
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      const where = ["f.church_id=$1"];
      const params = [churchId];
      let idx = 2;

      if (String(req.query?.status || "").trim()) {
        where.push(`f.status=$${idx}`);
        params.push(status);
        idx += 1;
      }
      if (String(req.query?.type || "").trim()) {
        where.push(`f.followup_type=$${idx}`);
        params.push(type);
        idx += 1;
      }
      if (assignedMemberId) {
        where.push(`f.assigned_member_id::text=$${idx}`);
        params.push(assignedMemberId);
        idx += 1;
      }
      if (search) {
        where.push(
          `(f.title ilike $${idx} or coalesce(f.visitor_name, '') ilike $${idx} or coalesce(m.full_name, '') ilike $${idx} or coalesce(f.member_id,'') ilike $${idx})`
        );
        params.push(searchLike);
        idx += 1;
      }
      if (campusId) {
        where.push(`f.campus_id = $${idx}`);
        params.push(campusId);
        idx += 1;
      }
      params.push(limit);

      const rows = await db.manyOrNone(
        `
        select
          f.id,
          f.church_id as "churchId",
          f.campus_id as "campusId",
          cc.name as "campusName",
          cc.code as "campusCode",
          f.member_pk as "memberPk",
          f.member_id as "memberId",
          m.full_name as "personName",
          m.phone as "memberPhone",
          m.email as "memberEmail",
          f.visitor_name as "visitorName",
          f.visitor_contact as "visitorContact",
          f.service_id as "serviceId",
          s.service_name as "serviceName",
          s.service_date as "serviceDate",
          f.followup_type as "followupType",
          f.status,
          f.priority,
          f.title,
          f.details,
          f.assigned_member_id as "assignedMemberId",
          am.full_name as "assignedMemberName",
          f.due_at as "dueAt",
          f.completed_at as "completedAt",
          f.notes,
          f.created_at as "createdAt",
          f.updated_at as "updatedAt",
          coalesce(task_agg.total, 0)::int as "taskCount",
          coalesce(task_agg.done, 0)::int as "tasksDone"
        from church_followups f
        left join members m on m.id = f.member_pk
        left join members am on am.id = f.assigned_member_id
        left join church_services s on s.id = f.service_id
        left join church_campuses cc on cc.id = f.campus_id
        left join lateral (
          select
            count(*)::int as total,
            count(*) filter (where status='DONE')::int as done
          from church_followup_tasks ft
          where ft.followup_id = f.id
        ) task_agg on true
        where ${where.join(" and ")}
        order by coalesce(f.due_at, f.created_at) asc, f.created_at desc
        limit $${idx}
        `,
        params
      );
      const access = getChurchLifeAccess(req);

      return res.json({
        ok: true,
        followups: rows.map((row) => redactFollowupContent(normalizeChurchFollowupRow(row), access)),
        meta: { limit, campusId: campusId || null, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church CRM follow-up board is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/followups] list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/followups",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("followups.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const access = getChurchLifeAccess(req);

      const memberRef = String(req.body?.memberRef || "").trim();
      const visitorName = String(req.body?.visitorName || "").trim().slice(0, 160) || null;
      const visitorContact = String(req.body?.visitorContact || "").trim().slice(0, 180) || null;
      const followupType = normalizeChurchFollowupType(req.body?.followupType || req.body?.type);
      const status = normalizeChurchFollowupStatus(req.body?.status || "OPEN");
      const priority = normalizeChurchFollowupPriority(req.body?.priority || "MEDIUM");
      const title = String(req.body?.title || "").trim().slice(0, 200);
      const details = String(req.body?.details || "").trim().slice(0, 5000) || null;
      const notes = String(req.body?.notes || "").trim().slice(0, 1000) || null;
      const dueAt = parseChurchLifeDateTimeOrNull(req.body?.dueAt);
      if (req.body?.dueAt && !dueAt) return res.status(400).json({ error: "dueAt is invalid." });
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.body?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      if (!title) return res.status(400).json({ error: "title is required." });

      let member = null;
      if (memberRef) {
        member = await findChurchMemberByReference(churchId, memberRef);
        if (!member) return res.status(400).json({ error: "memberRef is not in this church." });
      }

      if (!member && !visitorName) {
        return res.status(400).json({
          error: "Provide memberRef or visitorName for follow-up.",
        });
      }

      const serviceId = String(req.body?.serviceId || "").trim();
      let finalServiceId = null;
      if (serviceId) {
        if (!UUID_REGEX.test(serviceId)) return res.status(400).json({ error: "serviceId must be a UUID." });
        const service = await findChurchServiceById(churchId, serviceId);
        if (!service) return res.status(400).json({ error: "serviceId is not in this church." });
        if (campusId && service.campusId && service.campusId !== campusId) {
          return res.status(400).json({ error: "serviceId does not belong to the selected campus." });
        }
        finalServiceId = service.id;
        if (!campusId && service.campusId) {
          campusId = service.campusId;
        }
      }

      const assignedMemberIdInput = String(req.body?.assignedMemberId || "").trim();
      let assignedMemberId = null;
      if (assignedMemberIdInput) {
        const assigned = await findMemberInChurchByUuid(churchId, assignedMemberIdInput);
        if (!assigned) return res.status(400).json({ error: "assignedMemberId is not in this church." });
        assignedMemberId = assigned.id;
      }

      const row = await db.one(
        `
        insert into church_followups (
          church_id,
          campus_id,
          member_pk,
          member_id,
          visitor_name,
          visitor_contact,
          service_id,
          followup_type,
          status,
          priority,
          title,
          details,
          assigned_member_id,
          due_at,
          notes,
          created_by,
          updated_by
        )
        values (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$16
        )
        returning
          id,
          church_id as "churchId",
          campus_id as "campusId",
          member_pk as "memberPk",
          member_id as "memberId",
          visitor_name as "visitorName",
          visitor_contact as "visitorContact",
          service_id as "serviceId",
          followup_type as "followupType",
          status,
          priority,
          title,
          details,
          assigned_member_id as "assignedMemberId",
          due_at as "dueAt",
          completed_at as "completedAt",
          notes,
          created_at as "createdAt",
          updated_at as "updatedAt"
        `,
        [
          churchId,
          campusId,
          member?.id || null,
          member?.member_id || null,
          member ? null : visitorName,
          member ? null : visitorContact,
          finalServiceId,
          followupType,
          status,
          priority,
          title,
          details,
          assignedMemberId,
          dueAt,
          notes,
          req.user?.id || null,
        ]
      );

      const fullRow = await db.one(
        `
        select
          f.id,
          f.church_id as "churchId",
          f.campus_id as "campusId",
          cc.name as "campusName",
          cc.code as "campusCode",
          f.member_pk as "memberPk",
          f.member_id as "memberId",
          m.full_name as "personName",
          m.phone as "memberPhone",
          m.email as "memberEmail",
          f.visitor_name as "visitorName",
          f.visitor_contact as "visitorContact",
          f.service_id as "serviceId",
          s.service_name as "serviceName",
          s.service_date as "serviceDate",
          f.followup_type as "followupType",
          f.status,
          f.priority,
          f.title,
          f.details,
          f.assigned_member_id as "assignedMemberId",
          am.full_name as "assignedMemberName",
          f.due_at as "dueAt",
          f.completed_at as "completedAt",
          f.notes,
          f.created_at as "createdAt",
          f.updated_at as "updatedAt",
          0::int as "taskCount",
          0::int as "tasksDone"
        from church_followups f
        left join members m on m.id = f.member_pk
        left join members am on am.id = f.assigned_member_id
        left join church_services s on s.id = f.service_id
        left join church_campuses cc on cc.id = f.campus_id
        where f.id=$1
        `,
        [row.id]
      );

      const followup = redactFollowupContent(normalizeChurchFollowupRow(fullRow), access);
      try {
        if (member?.id) {
          await createNotification({
            churchId,
            memberId: member.id,
            type: "FOLLOWUP_CREATED",
            title: "Church follow-up created",
            body: `${followup.title || "A follow-up"} was opened for you.`,
            data: {
              churchId,
              followupId: followup.id,
              followupType: followup.followupType,
              priority: followup.priority,
              status: followup.status,
            },
          });
        }
        if (assignedMemberId && assignedMemberId !== member?.id) {
          await createNotification({
            churchId,
            memberId: assignedMemberId,
            type: "FOLLOWUP_ASSIGNED",
            title: "New follow-up assignment",
            body: `You were assigned: ${followup.title || "Church follow-up"}.`,
            data: {
              churchId,
              followupId: followup.id,
              followupType: followup.followupType,
              priority: followup.priority,
              status: followup.status,
            },
          });
        }
      } catch (notifyErr) {
        console.error("[admin/church-life/followups] notify failed", notifyErr?.message || notifyErr);
      }

      return res.status(201).json({ ok: true, followup });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church CRM follow-up board is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/followups] create error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/church-life/followups/:followupId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("followups.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const access = getChurchLifeAccess(req);
      const followupId = String(req.params?.followupId || "").trim();
      if (!UUID_REGEX.test(followupId)) return res.status(400).json({ error: "Invalid followupId" });

      const current = await db.oneOrNone(
        `
        select
          id,
          church_id as "churchId",
          campus_id as "campusId",
          member_pk as "memberPk",
          member_id as "memberId",
          visitor_name as "visitorName",
          visitor_contact as "visitorContact",
          service_id as "serviceId",
          followup_type as "followupType",
          status,
          priority,
          title,
          details,
          assigned_member_id as "assignedMemberId",
          due_at as "dueAt",
          notes
        from church_followups
        where id=$1 and church_id=$2
        limit 1
        `,
        [followupId, churchId]
      );
      if (!current) return res.status(404).json({ error: "Follow-up not found." });

      const followupType = normalizeChurchFollowupType(req.body?.followupType || current.followupType);
      const status = normalizeChurchFollowupStatus(req.body?.status || current.status);
      const priority = normalizeChurchFollowupPriority(req.body?.priority || current.priority);
      const title = String(typeof req.body?.title === "undefined" ? current.title || "" : req.body?.title || "")
        .trim()
        .slice(0, 200);
      if (!title) return res.status(400).json({ error: "title is required." });
      const details =
        String(typeof req.body?.details === "undefined" ? current.details || "" : req.body?.details || "")
          .trim()
          .slice(0, 5000) || null;
      const notes =
        String(typeof req.body?.notes === "undefined" ? current.notes || "" : req.body?.notes || "")
          .trim()
          .slice(0, 1000) || null;
      const visitorName =
        String(typeof req.body?.visitorName === "undefined" ? current.visitorName || "" : req.body?.visitorName || "")
          .trim()
          .slice(0, 160) || null;
      const visitorContact =
        String(typeof req.body?.visitorContact === "undefined" ? current.visitorContact || "" : req.body?.visitorContact || "")
          .trim()
          .slice(0, 180) || null;

      const dueAtInput = typeof req.body?.dueAt === "undefined" ? current.dueAt : req.body?.dueAt;
      const dueAt = dueAtInput === null || dueAtInput === "" ? null : parseChurchLifeDateTimeOrNull(dueAtInput);
      if (dueAtInput && !dueAt) return res.status(400).json({ error: "dueAt is invalid." });
      let campusId = current.campusId || null;
      if (Object.prototype.hasOwnProperty.call(req.body || {}, "campusId")) {
        try {
          campusId = await resolveChurchCampusId(churchId, req.body?.campusId, { fieldName: "campusId" });
        } catch (campusErr) {
          return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
        }
      }

      let assignedMemberId = current.assignedMemberId || null;
      if (typeof req.body?.assignedMemberId !== "undefined") {
        const assignedInput = String(req.body?.assignedMemberId || "").trim();
        if (!assignedInput) {
          assignedMemberId = null;
        } else {
          const assigned = await findMemberInChurchByUuid(churchId, assignedInput);
          if (!assigned) return res.status(400).json({ error: "assignedMemberId is not in this church." });
          assignedMemberId = assigned.id;
        }
      }

      let serviceId = current.serviceId || null;
      if (typeof req.body?.serviceId !== "undefined") {
        const serviceInput = String(req.body?.serviceId || "").trim();
        if (!serviceInput) {
          serviceId = null;
        } else {
          if (!UUID_REGEX.test(serviceInput)) return res.status(400).json({ error: "serviceId must be a UUID." });
          const service = await findChurchServiceById(churchId, serviceInput);
          if (!service) return res.status(400).json({ error: "serviceId is not in this church." });
          if (campusId && service.campusId && service.campusId !== campusId) {
            return res.status(400).json({ error: "serviceId does not belong to the selected campus." });
          }
          serviceId = service.id;
          if (!campusId && service.campusId) {
            campusId = service.campusId;
          }
        }
      }

      if (serviceId && campusId) {
        const service = await findChurchServiceById(churchId, serviceId);
        if (service?.campusId && service.campusId !== campusId) {
          return res.status(400).json({ error: "serviceId does not belong to the selected campus." });
        }
      }

      const row = await db.one(
        `
        update church_followups
        set
          campus_id = $3,
          visitor_name = $4,
          visitor_contact = $5,
          service_id = $6,
          followup_type = $7,
          status = $8,
          priority = $9,
          title = $10,
          details = $11,
          assigned_member_id = $12,
          due_at = $13,
          completed_at = case
            when $8 in ('CLOSED', 'CANCELLED') then coalesce(completed_at, now())
            else null
          end,
          closed_by = case
            when $8 in ('CLOSED', 'CANCELLED') then $15
            else null
          end,
          notes = $14,
          updated_by = $15,
          updated_at = now()
        where id=$1 and church_id=$2
        returning id
        `,
        [
          followupId,
          churchId,
          campusId,
          visitorName,
          visitorContact,
          serviceId,
          followupType,
          status,
          priority,
          title,
          details,
          assignedMemberId,
          dueAt,
          notes,
          req.user?.id || null,
        ]
      );

      const fullRow = await db.one(
        `
        select
          f.id,
          f.church_id as "churchId",
          f.campus_id as "campusId",
          cc.name as "campusName",
          cc.code as "campusCode",
          f.member_pk as "memberPk",
          f.member_id as "memberId",
          m.full_name as "personName",
          m.phone as "memberPhone",
          m.email as "memberEmail",
          f.visitor_name as "visitorName",
          f.visitor_contact as "visitorContact",
          f.service_id as "serviceId",
          s.service_name as "serviceName",
          s.service_date as "serviceDate",
          f.followup_type as "followupType",
          f.status,
          f.priority,
          f.title,
          f.details,
          f.assigned_member_id as "assignedMemberId",
          am.full_name as "assignedMemberName",
          f.due_at as "dueAt",
          f.completed_at as "completedAt",
          f.notes,
          f.created_at as "createdAt",
          f.updated_at as "updatedAt",
          coalesce(task_agg.total, 0)::int as "taskCount",
          coalesce(task_agg.done, 0)::int as "tasksDone"
        from church_followups f
        left join members m on m.id = f.member_pk
        left join members am on am.id = f.assigned_member_id
        left join church_services s on s.id = f.service_id
        left join church_campuses cc on cc.id = f.campus_id
        left join lateral (
          select
            count(*)::int as total,
            count(*) filter (where status='DONE')::int as done
          from church_followup_tasks ft
          where ft.followup_id = f.id
        ) task_agg on true
        where f.id=$1
        `,
        [row.id]
      );

      return res.json({ ok: true, followup: redactFollowupContent(normalizeChurchFollowupRow(fullRow), access) });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church CRM follow-up board is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/followups] patch error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/followups/:followupId/tasks",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("followups.tasks.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const followupId = String(req.params?.followupId || "").trim();
      if (!UUID_REGEX.test(followupId)) return res.status(400).json({ error: "Invalid followupId" });

      const followup = await db.oneOrNone(
        "select id, title from church_followups where id=$1 and church_id=$2 limit 1",
        [followupId, churchId]
      );
      if (!followup) return res.status(404).json({ error: "Follow-up not found." });

      const rows = await db.manyOrNone(
        `
        select
          t.id,
          t.church_id as "churchId",
          t.followup_id as "followupId",
          t.title,
          t.description,
          t.status,
          t.assigned_member_id as "assignedMemberId",
          am.full_name as "assignedMemberName",
          t.due_at as "dueAt",
          t.completed_at as "completedAt",
          t.created_at as "createdAt",
          t.updated_at as "updatedAt"
        from church_followup_tasks t
        left join members am on am.id = t.assigned_member_id
        where t.church_id=$1 and t.followup_id=$2
        order by t.created_at asc
        `,
        [churchId, followupId]
      );

      return res.json({
        ok: true,
        followup: { id: followup.id, title: followup.title },
        tasks: rows.map(normalizeChurchFollowupTaskRow),
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church CRM follow-up tasks are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/followups] task list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/followups/:followupId/tasks",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("followups.tasks.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const followupId = String(req.params?.followupId || "").trim();
      if (!UUID_REGEX.test(followupId)) return res.status(400).json({ error: "Invalid followupId" });

      const followup = await db.oneOrNone("select id from church_followups where id=$1 and church_id=$2 limit 1", [
        followupId,
        churchId,
      ]);
      if (!followup) return res.status(404).json({ error: "Follow-up not found." });

      const title = String(req.body?.title || "").trim().slice(0, 220);
      if (!title) return res.status(400).json({ error: "title is required." });
      const description = String(req.body?.description || "").trim().slice(0, 4000) || null;
      const status = normalizeChurchFollowupTaskStatus(req.body?.status || "TODO");
      const dueAt = parseChurchLifeDateTimeOrNull(req.body?.dueAt);
      if (req.body?.dueAt && !dueAt) return res.status(400).json({ error: "dueAt is invalid." });

      const assignedMemberIdInput = String(req.body?.assignedMemberId || "").trim();
      let assignedMemberId = null;
      if (assignedMemberIdInput) {
        const assigned = await findMemberInChurchByUuid(churchId, assignedMemberIdInput);
        if (!assigned) return res.status(400).json({ error: "assignedMemberId is not in this church." });
        assignedMemberId = assigned.id;
      }

      const row = await db.one(
        `
        insert into church_followup_tasks (
          church_id, followup_id, title, description, status, assigned_member_id, due_at, completed_at, created_by, updated_by
        )
        values (
          $1,$2,$3,$4,$5,$6,$7,case when $5='DONE' then now() else null end,$8,$8
        )
        returning
          id,
          church_id as "churchId",
          followup_id as "followupId",
          title,
          description,
          status,
          assigned_member_id as "assignedMemberId",
          due_at as "dueAt",
          completed_at as "completedAt",
          created_at as "createdAt",
          updated_at as "updatedAt"
        `,
        [churchId, followupId, title, description, status, assignedMemberId, dueAt, req.user?.id || null]
      );

      const withNames = await db.one(
        `
        select
          t.id,
          t.church_id as "churchId",
          t.followup_id as "followupId",
          t.title,
          t.description,
          t.status,
          t.assigned_member_id as "assignedMemberId",
          am.full_name as "assignedMemberName",
          t.due_at as "dueAt",
          t.completed_at as "completedAt",
          t.created_at as "createdAt",
          t.updated_at as "updatedAt"
        from church_followup_tasks t
        left join members am on am.id = t.assigned_member_id
        where t.id=$1
        `,
        [row.id]
      );

      return res.status(201).json({ ok: true, task: normalizeChurchFollowupTaskRow(withNames) });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church CRM follow-up tasks are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/followups] task create error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/church-life/followups/:followupId/tasks/:taskId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("followups.tasks.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const followupId = String(req.params?.followupId || "").trim();
      const taskId = String(req.params?.taskId || "").trim();
      if (!UUID_REGEX.test(followupId) || !UUID_REGEX.test(taskId)) {
        return res.status(400).json({ error: "Invalid followupId or taskId" });
      }

      const current = await db.oneOrNone(
        `
        select
          id,
          title,
          description,
          status,
          assigned_member_id as "assignedMemberId",
          due_at as "dueAt"
        from church_followup_tasks
        where id=$1 and church_id=$2 and followup_id=$3
        limit 1
        `,
        [taskId, churchId, followupId]
      );
      if (!current) return res.status(404).json({ error: "Task not found." });

      const title = String(typeof req.body?.title === "undefined" ? current.title || "" : req.body?.title || "")
        .trim()
        .slice(0, 220);
      if (!title) return res.status(400).json({ error: "title is required." });
      const description =
        String(typeof req.body?.description === "undefined" ? current.description || "" : req.body?.description || "")
          .trim()
          .slice(0, 4000) || null;
      const status = normalizeChurchFollowupTaskStatus(req.body?.status || current.status);
      const dueAtInput = typeof req.body?.dueAt === "undefined" ? current.dueAt : req.body?.dueAt;
      const dueAt = dueAtInput === null || dueAtInput === "" ? null : parseChurchLifeDateTimeOrNull(dueAtInput);
      if (dueAtInput && !dueAt) return res.status(400).json({ error: "dueAt is invalid." });

      let assignedMemberId = current.assignedMemberId || null;
      if (typeof req.body?.assignedMemberId !== "undefined") {
        const assignedInput = String(req.body?.assignedMemberId || "").trim();
        if (!assignedInput) {
          assignedMemberId = null;
        } else {
          const assigned = await findMemberInChurchByUuid(churchId, assignedInput);
          if (!assigned) return res.status(400).json({ error: "assignedMemberId is not in this church." });
          assignedMemberId = assigned.id;
        }
      }

      const row = await db.one(
        `
        update church_followup_tasks
        set
          title = $4,
          description = $5,
          status = $6,
          assigned_member_id = $7,
          due_at = $8,
          completed_at = case
            when $6 = 'DONE' then coalesce(completed_at, now())
            else null
          end,
          completed_by = case
            when $6 = 'DONE' then $9
            else null
          end,
          updated_by = $9,
          updated_at = now()
        where id=$1 and church_id=$2 and followup_id=$3
        returning
          id,
          church_id as "churchId",
          followup_id as "followupId",
          title,
          description,
          status,
          assigned_member_id as "assignedMemberId",
          due_at as "dueAt",
          completed_at as "completedAt",
          created_at as "createdAt",
          updated_at as "updatedAt"
        `,
        [taskId, churchId, followupId, title, description, status, assignedMemberId, dueAt, req.user?.id || null]
      );

      const withNames = await db.one(
        `
        select
          t.id,
          t.church_id as "churchId",
          t.followup_id as "followupId",
          t.title,
          t.description,
          t.status,
          t.assigned_member_id as "assignedMemberId",
          am.full_name as "assignedMemberName",
          t.due_at as "dueAt",
          t.completed_at as "completedAt",
          t.created_at as "createdAt",
          t.updated_at as "updatedAt"
        from church_followup_tasks t
        left join members am on am.id = t.assigned_member_id
        where t.id=$1
        `,
        [row.id]
      );

      return res.json({ ok: true, task: normalizeChurchFollowupTaskRow(withNames) });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church CRM follow-up tasks are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/followups] task patch error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/audit-logs",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("audit.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const actionRaw = String(req.query?.action || "").trim();
      const entityTypeRaw = String(req.query?.entityType || "").trim();
      const actionKey = normalizeUpperToken(actionRaw);
      const entityTypeKey = normalizeUpperToken(entityTypeRaw);
      if (actionRaw && !CHURCH_LIFE_AUDIT_ACTIONS.has(actionKey)) {
        return res.status(400).json({ error: "Invalid action filter." });
      }
      if (entityTypeRaw && !CHURCH_LIFE_AUDIT_ENTITY_TYPES.has(entityTypeKey)) {
        return res.status(400).json({ error: "Invalid entityType filter." });
      }
      const action = actionKey || "";
      const entityType = entityTypeKey || "";

      const limit = parseChurchLifeLimit(req.query?.limit, 120, 500);
      const offset = parseChurchLifeOffset(req.query?.offset);

      const where = ["l.church_id=$1"];
      const params = [churchId];
      let idx = 2;
      if (action) {
        where.push(`l.action=$${idx}`);
        params.push(action);
        idx += 1;
      }
      if (entityType) {
        where.push(`l.entity_type=$${idx}`);
        params.push(entityType);
        idx += 1;
      }

      const countRow = await db.one(
        `select count(*)::int as count from church_life_audit_logs l where ${where.join(" and ")}`,
        params
      );

      params.push(limit, offset);
      const rows = await db.manyOrNone(
        `
        select
          l.id,
          l.church_id as "churchId",
          l.actor_member_id as "actorMemberId",
          actor.member_id as "actorMemberRef",
          actor.full_name as "actorName",
          l.actor_role as "actorRole",
          l.action,
          l.entity_type as "entityType",
          l.entity_id as "entityId",
          l.entity_ref as "entityRef",
          l.before_json as "beforeJson",
          l.after_json as "afterJson",
          l.meta_json as "metaJson",
          l.created_at as "createdAt"
        from church_life_audit_logs l
        left join members actor on actor.id = l.actor_member_id
        where ${where.join(" and ")}
        order by l.created_at desc
        limit $${idx} offset $${idx + 1}
        `,
        params
      );

      return res.json({
        ok: true,
        logs: rows.map(normalizeChurchLifeAuditRow),
        meta: {
          action: action || null,
          entityType: entityType || null,
          limit,
          offset,
          count: Number(countRow?.count || 0),
          returned: rows.length,
        },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Church Life audit logs are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/audit-logs] list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/broadcast-audiences",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("broadcasts.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const includeInactive = toBoolean(req.query?.includeInactive) === true;
      const limit = parseChurchLifeLimit(req.query?.limit, 120, 500);

      const rows = await db.manyOrNone(
        `
        select
          a.id,
          a.church_id as "churchId",
          a.name,
          a.description,
          a.segment_key as "segmentKey",
          a.segment_tag as "segmentTag",
          a.active,
          a.created_by as "createdBy",
          c.full_name as "createdByName",
          a.updated_by as "updatedBy",
          u.full_name as "updatedByName",
          a.created_at as "createdAt",
          a.updated_at as "updatedAt"
        from church_broadcast_saved_audiences a
        left join members c on c.id = a.created_by
        left join members u on u.id = a.updated_by
        where a.church_id=$1
          and ($2::boolean = true or a.active = true)
        order by a.active desc, a.name asc, a.created_at desc
        limit $3
        `,
        [churchId, includeInactive, limit]
      );

      return res.json({
        ok: true,
        audiences: rows.map(normalizeChurchBroadcastAudiencePresetRow),
        meta: { includeInactive, limit, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Church Growth broadcast audiences are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/broadcast-audiences] list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/broadcast-audiences",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("broadcasts.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const name = String(req.body?.name || "")
        .trim()
        .slice(0, 140);
      if (!name) return res.status(400).json({ error: "name is required." });
      const segmentKey = normalizeBroadcastSegmentKey(req.body?.segmentKey);
      const segmentTag = String(req.body?.segmentTag || "")
        .trim()
        .slice(0, 120);
      if (segmentKey === "TAG" && !segmentTag) {
        return res.status(400).json({ error: "segmentTag is required when segmentKey=TAG." });
      }
      const description = String(req.body?.description || "")
        .trim()
        .slice(0, 600) || null;
      const active = toBoolean(req.body?.active) !== false;

      const row = await db.one(
        `
        insert into church_broadcast_saved_audiences (
          church_id, name, description, segment_key, segment_tag, active, created_by, updated_by
        )
        values ($1,$2,$3,$4,$5,$6,$7,$7)
        returning
          id,
          church_id as "churchId",
          name,
          description,
          segment_key as "segmentKey",
          segment_tag as "segmentTag",
          active,
          created_by as "createdBy",
          updated_by as "updatedBy",
          created_at as "createdAt",
          updated_at as "updatedAt"
        `,
        [churchId, name, description, segmentKey, segmentKey === "TAG" ? segmentTag : null, active, req.user?.id || null]
      );

      const actorName = req.user?.fullName || null;
      return res.status(201).json({
        ok: true,
        audience: normalizeChurchBroadcastAudiencePresetRow({
          ...row,
          createdByName: actorName,
          updatedByName: actorName,
        }),
      });
    } catch (err) {
      if (err?.code === "23505") {
        return res.status(409).json({ error: "A saved audience with this name already exists." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Church Growth broadcast audiences are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/broadcast-audiences] create error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/church-life/broadcast-audiences/:audienceId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("broadcasts.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const audienceId = String(req.params?.audienceId || "").trim();
      if (!UUID_REGEX.test(audienceId)) return res.status(400).json({ error: "Invalid audienceId" });

      const current = await db.oneOrNone(
        `
        select
          id,
          name,
          description,
          segment_key as "segmentKey",
          segment_tag as "segmentTag",
          active
        from church_broadcast_saved_audiences
        where id=$1 and church_id=$2
        limit 1
        `,
        [audienceId, churchId]
      );
      if (!current) return res.status(404).json({ error: "Saved audience not found." });

      const name = String(typeof req.body?.name === "undefined" ? current.name || "" : req.body?.name || "")
        .trim()
        .slice(0, 140);
      if (!name) return res.status(400).json({ error: "name is required." });
      const segmentKey = normalizeBroadcastSegmentKey(
        typeof req.body?.segmentKey === "undefined" ? current.segmentKey : req.body?.segmentKey
      );
      const segmentTag = String(
        typeof req.body?.segmentTag === "undefined" ? current.segmentTag || "" : req.body?.segmentTag || ""
      )
        .trim()
        .slice(0, 120);
      if (segmentKey === "TAG" && !segmentTag) {
        return res.status(400).json({ error: "segmentTag is required when segmentKey=TAG." });
      }
      const description = String(
        typeof req.body?.description === "undefined" ? current.description || "" : req.body?.description || ""
      )
        .trim()
        .slice(0, 600) || null;
      const active = typeof req.body?.active === "undefined" ? current.active !== false : toBoolean(req.body?.active) !== false;

      const row = await db.one(
        `
        update church_broadcast_saved_audiences
        set
          name = $3,
          description = $4,
          segment_key = $5,
          segment_tag = $6,
          active = $7,
          updated_by = $8,
          updated_at = now()
        where id=$1 and church_id=$2
        returning
          id,
          church_id as "churchId",
          name,
          description,
          segment_key as "segmentKey",
          segment_tag as "segmentTag",
          active,
          created_by as "createdBy",
          updated_by as "updatedBy",
          created_at as "createdAt",
          updated_at as "updatedAt"
        `,
        [audienceId, churchId, name, description, segmentKey, segmentKey === "TAG" ? segmentTag : null, active, req.user?.id || null]
      );

      const names = await db.oneOrNone(
        `
        select
          c.full_name as "createdByName",
          u.full_name as "updatedByName"
        from church_broadcast_saved_audiences a
        left join members c on c.id = a.created_by
        left join members u on u.id = a.updated_by
        where a.id=$1
        limit 1
        `,
        [audienceId]
      );

      return res.json({
        ok: true,
        audience: normalizeChurchBroadcastAudiencePresetRow({
          ...row,
          createdByName: names?.createdByName || null,
          updatedByName: names?.updatedByName || null,
        }),
      });
    } catch (err) {
      if (err?.code === "23505") {
        return res.status(409).json({ error: "A saved audience with this name already exists." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Church Growth broadcast audiences are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/broadcast-audiences] patch error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/broadcast-templates",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("broadcasts.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const includeInactive = toBoolean(req.query?.includeInactive) === true;
      const limit = parseChurchLifeLimit(req.query?.limit, 120, 500);

      const rows = await db.manyOrNone(
        `
        select
          t.id,
          t.church_id as "churchId",
          t.name,
          t.title,
          t.body,
          t.default_segment_key as "defaultSegmentKey",
          t.default_segment_tag as "defaultSegmentTag",
          t.data_json as "dataJson",
          t.active,
          t.created_by as "createdBy",
          c.full_name as "createdByName",
          t.updated_by as "updatedBy",
          u.full_name as "updatedByName",
          t.created_at as "createdAt",
          t.updated_at as "updatedAt"
        from church_broadcast_templates t
        left join members c on c.id = t.created_by
        left join members u on u.id = t.updated_by
        where t.church_id=$1
          and ($2::boolean = true or t.active = true)
        order by t.active desc, t.name asc, t.created_at desc
        limit $3
        `,
        [churchId, includeInactive, limit]
      );

      return res.json({
        ok: true,
        templates: rows.map(normalizeChurchBroadcastTemplateRow),
        meta: { includeInactive, limit, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Church Growth broadcast templates are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/broadcast-templates] list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/broadcast-templates",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("broadcasts.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const name = String(req.body?.name || "")
        .trim()
        .slice(0, 140);
      if (!name) return res.status(400).json({ error: "name is required." });
      const title = String(req.body?.title || "")
        .trim()
        .slice(0, 140);
      if (!title) return res.status(400).json({ error: "title is required." });
      const body = String(req.body?.body || "")
        .trim()
        .slice(0, 2000);
      if (!body) return res.status(400).json({ error: "body is required." });

      const defaultSegmentKey = normalizeBroadcastSegmentKey(req.body?.defaultSegmentKey);
      const defaultSegmentTag = String(req.body?.defaultSegmentTag || "")
        .trim()
        .slice(0, 120);
      if (defaultSegmentKey === "TAG" && !defaultSegmentTag) {
        return res.status(400).json({ error: "defaultSegmentTag is required when defaultSegmentKey=TAG." });
      }
      const dataJson =
        req.body?.dataJson && typeof req.body.dataJson === "object" && !Array.isArray(req.body.dataJson)
          ? req.body.dataJson
          : {};
      const active = toBoolean(req.body?.active) !== false;

      const row = await db.one(
        `
        insert into church_broadcast_templates (
          church_id, name, title, body, default_segment_key, default_segment_tag, data_json, active, created_by, updated_by
        )
        values ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$9)
        returning
          id,
          church_id as "churchId",
          name,
          title,
          body,
          default_segment_key as "defaultSegmentKey",
          default_segment_tag as "defaultSegmentTag",
          data_json as "dataJson",
          active,
          created_by as "createdBy",
          updated_by as "updatedBy",
          created_at as "createdAt",
          updated_at as "updatedAt"
        `,
        [
          churchId,
          name,
          title,
          body,
          defaultSegmentKey,
          defaultSegmentKey === "TAG" ? defaultSegmentTag : null,
          JSON.stringify(dataJson),
          active,
          req.user?.id || null,
        ]
      );

      const actorName = req.user?.fullName || null;
      return res.status(201).json({
        ok: true,
        template: normalizeChurchBroadcastTemplateRow({
          ...row,
          createdByName: actorName,
          updatedByName: actorName,
        }),
      });
    } catch (err) {
      if (err?.code === "23505") {
        return res.status(409).json({ error: "A broadcast template with this name already exists." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Church Growth broadcast templates are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/broadcast-templates] create error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/church-life/broadcast-templates/:templateId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("broadcasts.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const templateId = String(req.params?.templateId || "").trim();
      if (!UUID_REGEX.test(templateId)) return res.status(400).json({ error: "Invalid templateId" });

      const current = await db.oneOrNone(
        `
        select
          id,
          name,
          title,
          body,
          default_segment_key as "defaultSegmentKey",
          default_segment_tag as "defaultSegmentTag",
          data_json as "dataJson",
          active
        from church_broadcast_templates
        where id=$1 and church_id=$2
        limit 1
        `,
        [templateId, churchId]
      );
      if (!current) return res.status(404).json({ error: "Broadcast template not found." });

      const name = String(typeof req.body?.name === "undefined" ? current.name || "" : req.body?.name || "")
        .trim()
        .slice(0, 140);
      if (!name) return res.status(400).json({ error: "name is required." });
      const title = String(typeof req.body?.title === "undefined" ? current.title || "" : req.body?.title || "")
        .trim()
        .slice(0, 140);
      if (!title) return res.status(400).json({ error: "title is required." });
      const body = String(typeof req.body?.body === "undefined" ? current.body || "" : req.body?.body || "")
        .trim()
        .slice(0, 2000);
      if (!body) return res.status(400).json({ error: "body is required." });

      const defaultSegmentKey = normalizeBroadcastSegmentKey(
        typeof req.body?.defaultSegmentKey === "undefined" ? current.defaultSegmentKey : req.body?.defaultSegmentKey
      );
      const defaultSegmentTag = String(
        typeof req.body?.defaultSegmentTag === "undefined" ? current.defaultSegmentTag || "" : req.body?.defaultSegmentTag || ""
      )
        .trim()
        .slice(0, 120);
      if (defaultSegmentKey === "TAG" && !defaultSegmentTag) {
        return res.status(400).json({ error: "defaultSegmentTag is required when defaultSegmentKey=TAG." });
      }
      const dataJson =
        typeof req.body?.dataJson === "undefined"
          ? current?.dataJson && typeof current.dataJson === "object" && !Array.isArray(current.dataJson)
            ? current.dataJson
            : {}
          : req.body?.dataJson && typeof req.body.dataJson === "object" && !Array.isArray(req.body.dataJson)
            ? req.body.dataJson
            : {};
      const active = typeof req.body?.active === "undefined" ? current.active !== false : toBoolean(req.body?.active) !== false;

      const row = await db.one(
        `
        update church_broadcast_templates
        set
          name = $3,
          title = $4,
          body = $5,
          default_segment_key = $6,
          default_segment_tag = $7,
          data_json = $8::jsonb,
          active = $9,
          updated_by = $10,
          updated_at = now()
        where id=$1 and church_id=$2
        returning
          id,
          church_id as "churchId",
          name,
          title,
          body,
          default_segment_key as "defaultSegmentKey",
          default_segment_tag as "defaultSegmentTag",
          data_json as "dataJson",
          active,
          created_by as "createdBy",
          updated_by as "updatedBy",
          created_at as "createdAt",
          updated_at as "updatedAt"
        `,
        [
          templateId,
          churchId,
          name,
          title,
          body,
          defaultSegmentKey,
          defaultSegmentKey === "TAG" ? defaultSegmentTag : null,
          JSON.stringify(dataJson),
          active,
          req.user?.id || null,
        ]
      );

      const names = await db.oneOrNone(
        `
        select
          c.full_name as "createdByName",
          u.full_name as "updatedByName"
        from church_broadcast_templates t
        left join members c on c.id = t.created_by
        left join members u on u.id = t.updated_by
        where t.id=$1
        limit 1
        `,
        [templateId]
      );

      return res.json({
        ok: true,
        template: normalizeChurchBroadcastTemplateRow({
          ...row,
          createdByName: names?.createdByName || null,
          updatedByName: names?.updatedByName || null,
        }),
      });
    } catch (err) {
      if (err?.code === "23505") {
        return res.status(409).json({ error: "A broadcast template with this name already exists." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Church Growth broadcast templates are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/broadcast-templates] patch error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/broadcast-segments",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("broadcasts.read"),
  async (_req, res) => {
    return res.json({ ok: true, segments: churchGrowthSegmentsCatalog() });
  }
);

router.get(
  "/admin/church-life/broadcast-audience",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("broadcasts.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const segmentKey = normalizeBroadcastSegmentKey(req.query?.segmentKey);
      const segmentTag = String(req.query?.segmentTag || "")
        .trim()
        .slice(0, 120);
      if (segmentKey === "TAG" && !segmentTag) {
        return res.status(400).json({ error: "segmentTag is required when segmentKey=TAG." });
      }

      const limit = parseChurchLifeLimit(req.query?.limit, 120, 1000);
      const result = await listChurchBroadcastAudience({
        churchId,
        segmentKey,
        segmentTag,
        limit,
      });

      return res.json({
        ok: true,
        segmentKey: result.segmentKey,
        segmentTag: result.segmentTag,
        count: Number(result.count || 0),
        audience: Array.isArray(result.audience) ? result.audience : [],
        meta: { limit },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Church Growth broadcasts are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/broadcast-audience] error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/broadcasts",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("broadcasts.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const templateId = String(req.body?.templateId || "").trim();
      const audienceId = String(req.body?.audienceId || "").trim();

      let template = null;
      if (templateId) {
        if (!UUID_REGEX.test(templateId)) return res.status(400).json({ error: "Invalid templateId" });
        template = await db.oneOrNone(
          `
          select
            id,
            church_id as "churchId",
            name,
            title,
            body,
            default_segment_key as "defaultSegmentKey",
            default_segment_tag as "defaultSegmentTag",
            data_json as "dataJson",
            active
          from church_broadcast_templates
          where id=$1 and church_id=$2
          limit 1
          `,
          [templateId, churchId]
        );
        if (!template) return res.status(404).json({ error: "Broadcast template not found." });
      }

      let audiencePreset = null;
      if (audienceId) {
        if (!UUID_REGEX.test(audienceId)) return res.status(400).json({ error: "Invalid audienceId" });
        audiencePreset = await db.oneOrNone(
          `
          select
            id,
            church_id as "churchId",
            name,
            segment_key as "segmentKey",
            segment_tag as "segmentTag",
            active
          from church_broadcast_saved_audiences
          where id=$1 and church_id=$2
          limit 1
          `,
          [audienceId, churchId]
        );
        if (!audiencePreset) return res.status(404).json({ error: "Saved audience not found." });
      }

      const titleInput = String(req.body?.title || "")
        .trim()
        .slice(0, 140);
      const bodyInput = String(req.body?.body || "")
        .trim()
        .slice(0, 2000);
      const title = titleInput || String(template?.title || "").trim().slice(0, 140);
      const body = bodyInput || String(template?.body || "").trim().slice(0, 2000);
      if (!title) return res.status(400).json({ error: "title is required." });
      if (!body) return res.status(400).json({ error: "body is required." });

      const hasSegmentKeyInput = typeof req.body?.segmentKey !== "undefined" && String(req.body?.segmentKey || "").trim() !== "";
      const hasSegmentTagInput = typeof req.body?.segmentTag !== "undefined";
      const requestedSegmentKey = normalizeBroadcastSegmentKey(req.body?.segmentKey);
      const requestedSegmentTag = String(req.body?.segmentTag || "")
        .trim()
        .slice(0, 120);

      let segmentKey = hasSegmentKeyInput
        ? requestedSegmentKey
        : normalizeBroadcastSegmentKey(
            audiencePreset?.segmentKey || template?.defaultSegmentKey || "ALL_MEMBERS"
          );
      let segmentTag = hasSegmentTagInput
        ? requestedSegmentTag
        : String(audiencePreset?.segmentTag || template?.defaultSegmentTag || "")
            .trim()
            .slice(0, 120);
      if (segmentKey !== "TAG") segmentTag = "";
      if (segmentKey === "TAG" && !segmentTag) {
        return res.status(400).json({ error: "segmentTag is required when segmentKey=TAG." });
      }

      const templateData =
        template?.dataJson && typeof template.dataJson === "object" && !Array.isArray(template.dataJson) ? template.dataJson : {};
      const rawData = req.body?.data;
      const requestData = rawData && typeof rawData === "object" && !Array.isArray(rawData) ? rawData : {};
      const dataJson = {
        ...templateData,
        ...requestData,
        templateId: template?.id || null,
        templateName: template?.name || null,
        audienceId: audiencePreset?.id || null,
        audienceName: audiencePreset?.name || null,
      };

      const result = await sendChurchInAppBroadcast({
        churchId,
        createdBy: req.user?.id || null,
        title,
        body,
        segmentKey,
        segmentTag,
        data: dataJson,
      });

      const recipientsRaw = Array.isArray(result?.recipients) ? result.recipients : [];
      const recipientLimit = parseChurchLifeLimit(req.query?.recipientLimit, 120, 300);

      return res.status(201).json({
        ok: true,
        broadcast: normalizeChurchBroadcastRow(result?.broadcast || {}),
        summary: {
          audienceCount: Number(result?.summary?.audienceCount || 0),
          sentCount: Number(result?.summary?.sentCount || 0),
          failedCount: Number(result?.summary?.failedCount || 0),
        },
        recipients: recipientsRaw.slice(0, recipientLimit).map(normalizeChurchBroadcastRecipientRow),
        meta: {
          recipientReturned: Math.min(recipientsRaw.length, recipientLimit),
          recipientTotal: recipientsRaw.length,
          recipientLimit,
        },
        resolved: {
          templateId: template?.id || null,
          audienceId: audiencePreset?.id || null,
          segmentKey,
          segmentTag: segmentKey === "TAG" ? segmentTag : null,
        },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Church Growth broadcasts are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/broadcasts] create error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/broadcasts",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("broadcasts.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const statusInput = String(req.query?.status || "").trim().toUpperCase();
      const status = CHURCH_BROADCAST_STATUSES.has(statusInput) ? statusInput : null;
      const limit = parseChurchLifeLimit(req.query?.limit, 80, 250);
      const offsetRaw = Number(req.query?.offset || 0);
      const offset = Number.isFinite(offsetRaw) && offsetRaw > 0 ? Math.trunc(offsetRaw) : 0;

      const where = ["b.church_id=$1"];
      const params = [churchId];
      let idx = 2;
      if (status) {
        where.push(`b.status=$${idx}`);
        params.push(status);
        idx += 1;
      }

      const countRow = await db.one(
        `select count(*)::int as count from church_broadcasts b where ${where.join(" and ")}`,
        params
      );

      params.push(limit);
      params.push(offset);

      const rows = await db.manyOrNone(
        `
        select
          b.id,
          b.church_id as "churchId",
          b.segment_key as "segmentKey",
          b.segment_tag as "segmentTag",
          b.title,
          b.body,
          b.data_json as "dataJson",
          b.status,
          b.audience_count as "audienceCount",
          b.sent_count as "sentCount",
          b.failed_count as "failedCount",
          b.created_by as "createdBy",
          creator.full_name as "createdByName",
          b.created_at as "createdAt",
          b.sent_at as "sentAt"
        from church_broadcasts b
        left join members creator on creator.id = b.created_by
        where ${where.join(" and ")}
        order by b.created_at desc
        limit $${idx} offset $${idx + 1}
        `,
        params
      );

      return res.json({
        ok: true,
        broadcasts: rows.map(normalizeChurchBroadcastRow),
        meta: {
          status,
          limit,
          offset,
          count: Number(countRow.count || 0),
          returned: rows.length,
        },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Church Growth broadcasts are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/broadcasts] list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/broadcasts/:broadcastId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("broadcasts.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const broadcastId = String(req.params?.broadcastId || "").trim();
      if (!UUID_REGEX.test(broadcastId)) return res.status(400).json({ error: "Invalid broadcastId" });

      const row = await db.oneOrNone(
        `
        select
          b.id,
          b.church_id as "churchId",
          b.segment_key as "segmentKey",
          b.segment_tag as "segmentTag",
          b.title,
          b.body,
          b.data_json as "dataJson",
          b.status,
          b.audience_count as "audienceCount",
          b.sent_count as "sentCount",
          b.failed_count as "failedCount",
          b.created_by as "createdBy",
          creator.full_name as "createdByName",
          b.created_at as "createdAt",
          b.sent_at as "sentAt"
        from church_broadcasts b
        left join members creator on creator.id = b.created_by
        where b.id=$1 and b.church_id=$2
        limit 1
        `,
        [broadcastId, churchId]
      );
      if (!row) return res.status(404).json({ error: "Broadcast not found." });

      const recipientLimit = parseChurchLifeLimit(req.query?.recipientLimit, 150, 600);
      const recipients = await db.manyOrNone(
        `
        select
          r.id,
          r.broadcast_id as "broadcastId",
          r.church_id as "churchId",
          r.member_pk as "memberPk",
          r.member_id as "memberId",
          m.full_name as "memberName",
          r.notification_id as "notificationId",
          r.status,
          r.error,
          r.created_at as "createdAt",
          coalesce(r.read_at, n.read_at) as "readAt"
        from church_broadcast_recipients r
        left join members m on m.id = r.member_pk
        left join notifications n on n.id = r.notification_id
        where r.broadcast_id=$1 and r.church_id=$2
        order by r.created_at asc
        limit $3
        `,
        [broadcastId, churchId, recipientLimit]
      );

      return res.json({
        ok: true,
        broadcast: normalizeChurchBroadcastRow(row),
        recipients: recipients.map(normalizeChurchBroadcastRecipientRow),
        meta: {
          recipientLimit,
          recipientReturned: recipients.length,
        },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Church Growth broadcasts are not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/broadcasts] detail error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/auto-followups/preview",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("autofollowups.preview"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const sampleLimit = parseChurchLifeLimit(req.query?.sampleLimit, 25, 200);
      const preview = await previewAutoFollowups({ churchId, sampleLimit });
      return res.json({ ok: true, ...preview });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Church CRM follow-up automation is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/followups] auto preview error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/auto-followups/run",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("autofollowups.run"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const limitPerRule = parseChurchLifeLimit(req.body?.limitPerRule, 120, 1000);
      const result = await generateChurchAutoFollowups({
        churchId,
        actorId: req.user?.id || null,
        limitPerRule,
      });
      return res.json({ ok: true, ...result, meta: { limitPerRule } });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res
          .status(503)
          .json({ error: "Church CRM follow-up automation is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/followups] auto run error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get("/admin/dashboard/totals", requireStaff, requireAdminPortalTabsAny("dashboard"), async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, "me");
    if (!churchId) return;

    const rows = await db.manyOrNone(
      `
      select f.code, f.name, coalesce(sum(t.amount),0)::numeric(12,2) as total
      from funds f
      left join transactions t on t.fund_id=f.id and t.church_id=f.church_id
      where f.church_id=$1
      group by f.code, f.name
      order by f.name asc
      `,
      [churchId]
    );

    const grand = rows.reduce((acc, r) => acc + Number(r.total), 0);
    res.json({ totals: rows, grandTotal: grand.toFixed(2) });
  } catch (err) {
    console.error("[admin/dashboard/totals] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get(
  "/admin/donors",
  requireStaff,
  requireAdminPortalTabsAny("dashboard", "transactions", "members"),
  async (req, res) => {
    try {
      const churchId = resolveChurchId(req, res, "me");
      if (!churchId) return;

      const role = normalizeChurchStaffRole(req.user?.role);
      if (!DONOR_ADMIN_ROLES.has(role)) {
        return res.status(403).json({ error: "Only admin and finance roles can access donors." });
      }

      const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);
      const offset = Math.max(Number(req.query.offset || 0), 0);
      const source = typeof req.query?.source === "string" ? req.query.source.trim().toUpperCase() : "";
      const search = typeof req.query?.search === "string" ? req.query.search.trim() : "";

      const where = ["d.church_id = $1"];
      const params = [churchId];
      let idx = 2;

      if (source) {
        params.push(source);
        where.push(`upper(coalesce(d.last_source, '')) = $${idx}`);
        idx += 1;
      }

      if (search) {
        params.push(`%${search}%`);
        where.push(
          `(coalesce(d.full_name, '') ilike $${idx} or coalesce(d.email, '') ilike $${idx} or coalesce(d.phone, '') ilike $${idx})`
        );
        idx += 1;
      }

      const countRow = await db.one(
        `
        select count(*)::int as count
        from church_donors d
        where ${where.join(" and ")}
        `,
        params
      );

      const rows = await db.manyOrNone(
        `
        select
          d.id,
          d.church_id as "churchId",
          d.full_name as "fullName",
          d.email,
          d.phone,
          d.first_given_at as "firstGivenAt",
          d.last_given_at as "lastGivenAt",
          d.giving_count as "givingCount",
          d.total_given_amount as "totalGivenAmount",
          d.last_source as "lastSource",
          d.last_payment_intent_id as "lastPaymentIntentId",
          d.last_transaction_id as "lastTransactionId",
          d.updated_at as "updatedAt"
        from church_donors d
        where ${where.join(" and ")}
        order by d.last_given_at desc, d.updated_at desc
        limit $${idx} offset $${idx + 1}
        `,
        [...params, limit, offset]
      );

      return res.json({
        donors: rows.map((row) => ({
          ...row,
          totalGivenAmount: Number(row.totalGivenAmount || 0),
          givingCount: Number(row.givingCount || 0),
        })),
        meta: {
          limit,
          offset,
          count: Number(countRow?.count || 0),
          returned: rows.length,
        },
      });
    } catch (err) {
      if (err?.code === "42P01") {
        return res
          .status(503)
          .json({ error: "Donor records are not available yet. Run migrations and retry.", code: "CHURCH_DONORS_NOT_READY" });
      }
      console.error("[admin/donors] error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get("/admin/reports/digital-growth", requireStaff, requireAdminPortalTabsAny("growth"), async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, "me");
    if (!churchId) return;

    const weeksRequested = Number(req.query?.weeks || 12);
    const weeks = Math.min(Math.max(Number.isFinite(weeksRequested) ? Math.trunc(weeksRequested) : 12, 4), 52);

    const weeklyTrendRows = await db.manyOrNone(
      `
      with series as (
        select generate_series(
          (date_trunc('week', now()) - (($2::int - 1) * interval '1 week'))::date,
          date_trunc('week', now())::date,
          interval '1 week'
        )::date as week_start
      ),
      agg as (
        select
          date_trunc('week', t.created_at)::date as week_start,
          coalesce(sum(t.amount),0)::numeric(12,2) as amount_total,
          count(*)::int as tx_count
        from transactions t
        left join payment_intents pi on pi.id = t.payment_intent_id
        where t.church_id = $1
          and ${TX_STATUS_EXPR} = any($3)
          and t.created_at >= (select min(week_start)::timestamp from series)
        group by 1
      )
      select
        s.week_start as "weekStart",
        coalesce(a.amount_total,0)::numeric(12,2) as amount,
        coalesce(a.tx_count,0)::int as "transactionCount"
      from series s
      left join agg a on a.week_start = s.week_start
      order by s.week_start asc
      `,
      [churchId, weeks, STATEMENT_DEFAULT_STATUSES]
    );

    const monthly = await db.one(
      `
      with bounds as (
        select
          date_trunc('month', now()) as this_start,
          (date_trunc('month', now()) + interval '1 month') as next_start,
          (date_trunc('month', now()) - interval '1 month') as prev_start
      )
      select
        coalesce(sum(case when t.created_at >= b.this_start and t.created_at < b.next_start then t.amount end),0)::numeric(12,2) as "thisMonthAmount",
        coalesce(sum(case when t.created_at >= b.prev_start and t.created_at < b.this_start then t.amount end),0)::numeric(12,2) as "previousMonthAmount",
        count(case when t.created_at >= b.this_start and t.created_at < b.next_start then 1 end)::int as "thisMonthTransactions",
        count(case when t.created_at >= b.prev_start and t.created_at < b.this_start then 1 end)::int as "previousMonthTransactions",
        count(distinct case
          when t.created_at >= b.this_start and t.created_at < b.next_start
          then coalesce(nullif(lower(pi.payer_email),''), nullif(pi.payer_phone,''), nullif(lower(pi.payer_name),''), t.reference)
          else null
        end)::int as "thisMonthDonors",
        count(distinct case
          when t.created_at >= b.prev_start and t.created_at < b.this_start
          then coalesce(nullif(lower(pi.payer_email),''), nullif(pi.payer_phone,''), nullif(lower(pi.payer_name),''), t.reference)
          else null
        end)::int as "previousMonthDonors"
      from transactions t
      left join payment_intents pi on pi.id = t.payment_intent_id
      cross join bounds b
      where t.church_id = $1
        and ${TX_STATUS_EXPR} = any($2)
      `,
      [churchId, STATEMENT_DEFAULT_STATUSES]
    );

    let recurring = {
      configuredRecurringCount: 0,
      activeRecurringCount: 0,
      monthlyRecurringGross: 0,
      monthlyRecurringDonation: 0,
    };
    try {
      const recurringRaw = await db.one(
        `
        select
          count(*) filter (where status in ('PENDING_SETUP','ACTIVE','PAUSED'))::int as "configuredRecurringCount",
          count(*) filter (where status = 'ACTIVE')::int as "activeRecurringCount",
          coalesce(sum(gross_amount) filter (where status = 'ACTIVE' and frequency = 3),0)::numeric(12,2) as "monthlyRecurringGross",
          coalesce(sum(donation_amount) filter (where status = 'ACTIVE' and frequency = 3),0)::numeric(12,2) as "monthlyRecurringDonation"
        from recurring_givings
        where church_id = $1
        `,
        [churchId]
      );
      recurring = {
        configuredRecurringCount: Number(recurringRaw.configuredRecurringCount || 0),
        activeRecurringCount: Number(recurringRaw.activeRecurringCount || 0),
        monthlyRecurringGross: Number(recurringRaw.monthlyRecurringGross || 0),
        monthlyRecurringDonation: Number(recurringRaw.monthlyRecurringDonation || 0),
      };
    } catch (err) {
      // Older DBs may not have recurring table yet; keep zeros.
      if (!(err?.code === "42P01" || err?.code === "42703")) throw err;
    }

    const thisMonthAmount = Number(monthly.thisMonthAmount || 0);
    const previousMonthAmount = Number(monthly.previousMonthAmount || 0);
    const thisMonthDonors = Number(monthly.thisMonthDonors || 0);
    const previousMonthDonors = Number(monthly.previousMonthDonors || 0);
    const thisMonthTransactions = Number(monthly.thisMonthTransactions || 0);
    const previousMonthTransactions = Number(monthly.previousMonthTransactions || 0);

    const now = new Date();
    const thisMonthLabel = new Intl.DateTimeFormat("en-ZA", {
      month: "long",
      year: "numeric",
      timeZone: "UTC",
    }).format(now);
    const previousMonthLabel = new Intl.DateTimeFormat("en-ZA", {
      month: "long",
      year: "numeric",
      timeZone: "UTC",
    }).format(new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - 1, 1)));

    return res.json({
      overview: {
        thisMonthAmount,
        previousMonthAmount,
        amountChangePct: calcPercentChange(thisMonthAmount, previousMonthAmount),
        thisMonthDonors,
        previousMonthDonors,
        donorChangePct: calcPercentChange(thisMonthDonors, previousMonthDonors),
        thisMonthTransactions,
        previousMonthTransactions,
        transactionChangePct: calcPercentChange(thisMonthTransactions, previousMonthTransactions),
        recurringEnabled: readRecurringConfig().enabled,
        configuredRecurringCount: recurring.configuredRecurringCount,
        activeRecurringCount: recurring.activeRecurringCount,
        monthlyRecurringGross: recurring.monthlyRecurringGross,
        monthlyRecurringDonation: recurring.monthlyRecurringDonation,
      },
      labels: {
        thisMonth: thisMonthLabel,
        previousMonth: previousMonthLabel,
      },
      weeklyTrend: weeklyTrendRows.map((row) => ({
        weekStart: formatDateIsoLike(row.weekStart),
        amount: Number(row.amount || 0),
        transactionCount: Number(row.transactionCount || 0),
      })),
      meta: {
        weeks,
        statuses: STATEMENT_DEFAULT_STATUSES.slice(),
        generatedAt: new Date().toISOString(),
      },
    });
  } catch (err) {
    console.error("[admin/reports/digital-growth] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get(
  "/admin/dashboard/transactions/recent",
  requireStaff,
  requireAdminPortalTabsAny("dashboard", "transactions"),
  async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, "me");
    if (!churchId) return;

    const limit = Math.min(Math.max(Number(req.query.limit || 20), 1), 200);
    const offset = Math.max(Number(req.query.offset || 0), 0);
    const fundId = req.query.fundId || null;
    const channel = req.query.channel || null;
    const status = req.query.status || null;
    const search = req.query.search || null;
    const from = req.query.from ? new Date(req.query.from + "T00:00:00.000Z") : null;
    const to = req.query.to ? new Date(req.query.to + "T23:59:59.999Z") : null;

    const { where, params: filterParams, nextParamIndex } = buildTransactionFilter({
      churchId,
      fundId,
      channel,
      status,
      search,
      from,
      to,
    });
    const params = [...filterParams];
    let paramIndex = nextParamIndex;

    params.push(limit);
    const limitIdx = paramIndex;
    paramIndex++;
    params.push(offset);
    const offsetIdx = paramIndex;

    const rows = await db.manyOrNone(
      `
      select
        t.id,
        t.payment_intent_id as "paymentIntentId",
        t.reference,
        t.amount,
        coalesce(t.platform_fee_amount,0)::numeric(12,2) as "platformFeeAmount",
        coalesce(t.payfast_fee_amount,0)::numeric(12,2) as "payfastFeeAmount",
        coalesce(t.church_net_amount, t.amount)::numeric(12,2) as "churchNetAmount",
        coalesce(t.amount_gross, t.amount)::numeric(12,2) as "amountGross",
        coalesce(t.superadmin_cut_amount,0)::numeric(12,2) as "superadminCutAmount",
        t.channel,
        t.provider,
        t.provider_payment_id as "providerPaymentId",
        coalesce(pi.status, case when t.provider in ('payfast','simulated','manual') then 'PAID' when t.provider is null then 'PAID' else upper(t.provider) end) as status,
        pi.service_date as "serviceDate",
        coalesce(pi.cash_verified_by_admin,false) as "cashVerifiedByAdmin",
        pi.cash_verified_at as "cashVerifiedAt",
        pi.cash_verified_by as "cashVerifiedBy",
        pi.cash_verification_note as "cashVerificationNote",
        t.created_at as "createdAt",
        coalesce(pi.payer_name, pi.member_name) as "memberName",
        coalesce(pi.payer_phone, pi.member_phone) as "memberPhone",
        coalesce(pi.payer_email, null) as "memberEmail",
        coalesce(pi.payer_type, 'member') as "payerType",
        coalesce(pi.source, 'DIRECT_APP') as "paymentSource",
        pi.on_behalf_of_member_id as "onBehalfOfMemberId",
        ob.full_name as "onBehalfOfMemberName",
        ob.phone as "onBehalfOfMemberPhone",
        ob.email as "onBehalfOfMemberEmail",
        f.id as "fundId",
        f.code as "fundCode",
        f.name as "fundName"
      from transactions t
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
      left join members ob on ob.id = pi.on_behalf_of_member_id
      where ${where.join(" and ")}
      order by t.created_at desc
      limit $${limitIdx} offset $${offsetIdx}
      `,
      params
    );

    const countRow = await db.one(
      `
      select count(*)::int as count
      from transactions t
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
      where ${where.join(" and ")}
      `,
      filterParams
    );

    res.json({
      transactions: rows,
      meta: { limit, offset, count: Number(countRow.count || 0), returned: rows.length },
    });
  } catch (err) {
    console.error("[admin/dashboard/recent] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get(
  "/admin/dashboard/transactions/export",
  requireStaff,
  requireAdminPortalTabsAny("dashboard", "transactions"),
  async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, "me");
    if (!churchId) return;

    const maxRows = Math.min(Math.max(Number(req.query.limit || 5000), 1), 10000);
    const fundId = req.query.fundId || null;
    const channel = req.query.channel || null;
    const status = req.query.status || null;
    const search = req.query.search || null;
    const from = req.query.from ? new Date(req.query.from + "T00:00:00.000Z") : null;
    const to = req.query.to ? new Date(req.query.to + "T23:59:59.999Z") : null;

    const { where, params, nextParamIndex } = buildTransactionFilter({
      churchId,
      fundId,
      channel,
      status,
      search,
      from,
      to,
    });
    params.push(maxRows);
    const limitIdx = nextParamIndex;

    const rows = await db.manyOrNone(
      `
      select
        t.id,
        t.reference,
        t.amount,
        coalesce(t.platform_fee_amount,0)::numeric(12,2) as "platformFeeAmount",
        coalesce(t.payfast_fee_amount,0)::numeric(12,2) as "payfastFeeAmount",
        coalesce(t.church_net_amount, t.amount)::numeric(12,2) as "churchNetAmount",
        coalesce(t.amount_gross, t.amount)::numeric(12,2) as "amountGross",
        coalesce(t.superadmin_cut_amount,0)::numeric(12,2) as "superadminCutAmount",
        t.channel,
        t.provider,
        coalesce(pi.status, case when t.provider in ('payfast','simulated','manual') then 'PAID' when t.provider is null then 'PAID' else upper(t.provider) end) as status,
        pi.service_date as "serviceDate",
        t.created_at as "createdAt",
        coalesce(pi.payer_name, pi.member_name) as "memberName",
        coalesce(pi.payer_phone, pi.member_phone) as "memberPhone",
        coalesce(pi.payer_email, null) as "memberEmail",
        coalesce(pi.payer_type, 'member') as "payerType",
        coalesce(pi.source, 'DIRECT_APP') as "paymentSource",
        pi.on_behalf_of_member_id as "onBehalfOfMemberId",
        ob.full_name as "onBehalfOfMemberName",
        ob.phone as "onBehalfOfMemberPhone",
        ob.email as "onBehalfOfMemberEmail",
        f.code as "fundCode",
        f.name as "fundName"
      from transactions t
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
      left join members ob on ob.id = pi.on_behalf_of_member_id
      where ${where.join(" and ")}
      order by t.created_at desc
      limit $${limitIdx}
      `,
      params
    );

    const header = [
      "id",
      "reference",
      "donationAmount",
      "feeAmount",
      "totalCharged",
      "superadminCutAmount",
      "channel",
      "provider",
      "status",
      "createdAt",
      "memberName",
      "memberPhone",
      "fundCode",
      "fundName",
    ];
    const lines = [header.join(",")];
    for (const row of rows) {
      lines.push([
        csvEscape(row.id),
        csvEscape(row.reference),
        csvEscape(row.amount),
        csvEscape(row.platformFeeAmount),
        csvEscape(row.amountGross),
        csvEscape(row.superadminCutAmount),
        csvEscape(row.channel),
        csvEscape(row.provider),
        csvEscape(row.status),
        csvEscape(row.createdAt),
        csvEscape(row.memberName),
        csvEscape(row.memberPhone),
        csvEscape(row.fundCode),
        csvEscape(row.fundName),
      ].join(","));
    }

    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename=\"transactions-${stamp}.csv\"`);
    res.status(200).send(lines.join("\n"));
  } catch (err) {
    console.error("[admin/dashboard/export] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/admin/statements/summary", requireStaff, requireAdminPortalTabsAny("statements"), async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, "me");
    if (!churchId) return;

    const fundId = req.query.fundId || null;
    const channel = req.query.channel || null;
    const status = req.query.status || null;
    const search = req.query.search || null;
    const allStatuses = ["1", "true", "yes", "all"].includes(String(req.query.allStatuses || "").toLowerCase());

    const fromIso = (typeof req.query.from === "string" && req.query.from.trim()) ? req.query.from.trim() : startOfUtcMonthIsoDate();
    const toIso = (typeof req.query.to === "string" && req.query.to.trim()) ? req.query.to.trim() : todayUtcIsoDate();
    const data = await loadAdminStatementData({
      churchId,
      fundId,
      channel,
      status,
      search,
      allStatuses,
      fromIso,
      toIso,
      maxRows: 0,
    });

    res.json({ summary: data.summary, breakdown: data.breakdown, meta: data.meta });
  } catch (err) {
    console.error("[admin/statements/summary] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/admin/statements/export", requireStaff, requireAdminPortalTabsAny("statements"), async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, "me");
    if (!churchId) return;

    const maxRows = Math.min(Math.max(Number(req.query.limit || 20000), 1), 50000);
    const fundId = req.query.fundId || null;
    const channel = req.query.channel || null;
    const status = req.query.status || null;
    const search = req.query.search || null;
    const allStatuses = ["1", "true", "yes", "all"].includes(String(req.query.allStatuses || "").toLowerCase());

    const fromIso = (typeof req.query.from === "string" && req.query.from.trim()) ? req.query.from.trim() : startOfUtcMonthIsoDate();
    const toIso = (typeof req.query.to === "string" && req.query.to.trim()) ? req.query.to.trim() : todayUtcIsoDate();
    const data = await loadAdminStatementData({
      churchId,
      fundId,
      channel,
      status,
      search,
      allStatuses,
      fromIso,
      toIso,
      maxRows,
    });
    const rows = data.rows || [];

    let donationTotal = 0;
    let feeTotal = 0;
    let payfastFeeTotal = 0;
    let netReceivedTotal = 0;
    let grossTotal = 0;

    const header = [
      "reference",
      "status",
      "provider",
      "channel",
      "donationAmount",
      "churpayFeeAmount",
      "payfastFeeAmount",
      "netReceivedAmount",
      "totalCharged",
      "serviceDate",
      "createdAt",
      "fundCode",
      "fundName",
      "memberName",
      "memberPhone",
      "memberEmail",
      "payerType",
      "providerPaymentId",
    ];
    const lines = [header.join(",")];

    for (const row of rows) {
      const a = Number(row.amount || 0);
      const f = Number(row.platformFeeAmount || 0);
      const pf = Number(row.payfastFeeAmount || 0);
      const net = Number(row.churchNetAmount || 0);
      const g = Number(row.amountGross || 0);
      if (Number.isFinite(a)) donationTotal += a;
      if (Number.isFinite(f)) feeTotal += f;
      if (Number.isFinite(pf)) payfastFeeTotal += pf;
      if (Number.isFinite(net)) netReceivedTotal += net;
      if (Number.isFinite(g)) grossTotal += g;

      lines.push([
        csvEscape(row.reference),
        csvEscape(row.status),
        csvEscape(row.provider),
        csvEscape(row.channel),
        csvEscape(row.amount),
        csvEscape(row.platformFeeAmount),
        csvEscape(row.payfastFeeAmount),
        csvEscape(row.churchNetAmount),
        csvEscape(row.amountGross),
        csvEscape(row.serviceDate),
        csvEscape(row.createdAt),
        csvEscape(row.fundCode),
        csvEscape(row.fundName),
        csvEscape(row.memberName),
        csvEscape(row.memberPhone),
        csvEscape(row.memberEmail),
        csvEscape(row.payerType),
        csvEscape(row.providerPaymentId),
      ].join(","));
    }

    lines.push([
      "TOTAL",
      "",
      "",
      "",
      donationTotal.toFixed(2),
      feeTotal.toFixed(2),
      payfastFeeTotal.toFixed(2),
      netReceivedTotal.toFixed(2),
      grossTotal.toFixed(2),
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
    ].join(","));

    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename=\"statement-${fromIso}-to-${toIso}-${stamp}.csv\"`);
    res.status(200).send(lines.join("\n"));
  } catch (err) {
    console.error("[admin/statements/export] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/admin/statements/print", requireStaff, requireAdminPortalTabsAny("statements"), async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, "me");
    if (!churchId) return;

    const maxRows = Math.min(Math.max(Number(req.query.limit || 20000), 1), 50000);
    const fundId = req.query.fundId || null;
    const channel = req.query.channel || null;
    const status = req.query.status || null;
    const search = req.query.search || null;
    const allStatuses = ["1", "true", "yes", "all"].includes(String(req.query.allStatuses || "").toLowerCase());

    const fromIso = (typeof req.query.from === "string" && req.query.from.trim()) ? req.query.from.trim() : startOfUtcMonthIsoDate();
    const toIso = (typeof req.query.to === "string" && req.query.to.trim()) ? req.query.to.trim() : todayUtcIsoDate();

    const church = await db.oneOrNone(`select id, name, join_code from churches where id=$1`, [churchId]);
    const data = await loadAdminStatementData({
      churchId,
      fundId,
      channel,
      status,
      search,
      allStatuses,
      fromIso,
      toIso,
      maxRows,
    });

    const rows = data.rows || [];
    const summary = data.summary || {};
    const byFund = data.breakdown?.byFund || [];
    const byMethod = data.breakdown?.byMethod || [];

    const assetBase = normalizeBaseUrl() || "https://api.churpay.com";
    const logoUrl = `${assetBase}/assets/brand/churpay-logo.svg`;
    const autoprint = ["1", "true", "yes"].includes(String(req.query.autoprint || "").toLowerCase());

    const totalsRow = {
      donationTotal: Number(summary.donationTotal || 0),
      churpayFeeTotal: Number(summary.feeTotal || 0),
      payfastFeeTotal: Number(summary.payfastFeeTotal || 0),
      netReceivedTotal: Number(summary.netReceivedTotal || 0),
      totalCharged: Number(summary.totalCharged || 0),
      transactionCount: Number(summary.transactionCount || 0),
    };

    const title = `Churpay Statement - ${(church && church.name) ? church.name : "Church"} - ${fromIso} to ${toIso}`;

    const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>${escapeHtml(title)}</title>
  <style>
    :root { --bg: #ffffff; --text: #0f172a; --muted: #475569; --line: #e2e8f0; --brand: #0ea5b7; }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; background: var(--bg); color: var(--text); }
    .wrap { max-width: 980px; margin: 0 auto; padding: 28px 20px 40px; }
    .header { display: flex; align-items: center; justify-content: space-between; gap: 16px; border-bottom: 2px solid var(--line); padding-bottom: 14px; }
    .brand { display:flex; align-items:center; gap: 14px; min-width: 260px; }
    .brand img { height: 42px; width: auto; display:block; }
    .hgroup h1 { margin: 0; font-size: 18px; letter-spacing: .2px; }
    .hgroup p { margin: 4px 0 0; font-size: 12px; color: var(--muted); }
    .meta { text-align: right; }
    .meta .label { font-size: 12px; color: var(--muted); }
    .meta .value { font-size: 14px; font-weight: 700; margin-top: 4px; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-top: 18px; }
    .card { border: 1px solid var(--line); border-radius: 12px; padding: 14px; }
    .card h2 { margin: 0 0 10px; font-size: 14px; }
    .stats { display:grid; grid-template-columns: 1fr 1fr; gap: 10px; }
    .stat { border: 1px solid var(--line); border-radius: 10px; padding: 10px; }
    .stat .k { font-size: 12px; color: var(--muted); }
    .stat .v { margin-top: 6px; font-size: 16px; font-weight: 800; }
    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; padding: 9px 10px; border-bottom: 1px solid var(--line); vertical-align: top; font-size: 12px; overflow-wrap: anywhere; word-break: break-word; }
    th { font-size: 12px; color: var(--muted); font-weight: 700; }
    .table-shell { border: 1px solid var(--line); border-radius: 10px; overflow: hidden; }
    .table-scroll { width: 100%; overflow-x: auto; overflow-y: hidden; }
    .table-scroll table { min-width: 680px; }
    .table-scroll.tx table { min-width: 1220px; }
    .pill { display:inline-block; padding: 2px 8px; border-radius: 999px; border: 1px solid var(--line); font-size: 11px; }
    .pill.ok { border-color: rgba(14,165,183,.35); background: rgba(14,165,183,.10); color: #0b4b57; }
    .pill.warn { border-color: rgba(245,158,11,.35); background: rgba(245,158,11,.12); color: #7c2d12; }
    .section { margin-top: 16px; }
    .section h3 { margin: 0 0 10px; font-size: 14px; }
    .muted { color: var(--muted); }
    .footer { margin-top: 18px; padding-top: 12px; border-top: 1px solid var(--line); font-size: 11px; color: var(--muted); display:flex; justify-content: space-between; gap: 10px; }
    @media print {
      .wrap { max-width: none; padding: 0; }
      .card { break-inside: avoid; }
      .table-scroll { overflow: visible !important; }
      .table-scroll table { min-width: 0 !important; }
      a { color: inherit; text-decoration: none; }
    }
  </style>
</head>
<body>
  <main class="wrap">
    <header class="header">
      <div class="brand">
        <img src="${escapeHtml(logoUrl)}" alt="Churpay" />
        <div class="hgroup">
          <h1>${escapeHtml((church && church.name) ? church.name : "Church statement")}</h1>
          <p>Giving statement for reconciliation and reporting.</p>
        </div>
      </div>
      <div class="meta">
        <div class="label">Period</div>
        <div class="value">${escapeHtml(fromIso)} to ${escapeHtml(toIso)}</div>
        <div class="label" style="margin-top:6px;">Generated</div>
        <div class="value" style="font-size:12px;font-weight:700;">${escapeHtml(new Date().toISOString().replace("T", " ").slice(0, 19) + " UTC")}</div>
      </div>
    </header>

    <section class="grid">
      <div class="card">
        <h2>Summary</h2>
        <div class="stats">
          <div class="stat"><div class="k">Donation total</div><div class="v">${escapeHtml(formatMoneyZar(totalsRow.donationTotal))}</div></div>
          <div class="stat"><div class="k">Processing fee (Churpay)</div><div class="v">${escapeHtml(formatMoneyZar(totalsRow.churpayFeeTotal))}</div></div>
          <div class="stat"><div class="k">PayFast fees (church cost)</div><div class="v">${escapeHtml(formatMoneyZar(totalsRow.payfastFeeTotal))}</div></div>
          <div class="stat"><div class="k">Net received</div><div class="v">${escapeHtml(formatMoneyZar(totalsRow.netReceivedTotal))}</div></div>
          <div class="stat"><div class="k">Total charged</div><div class="v">${escapeHtml(formatMoneyZar(totalsRow.totalCharged))}</div></div>
          <div class="stat"><div class="k">Transactions</div><div class="v">${escapeHtml(String(totalsRow.transactionCount || 0))}</div></div>
        </div>
        <p class="muted" style="margin:10px 0 0;font-size:11px;">
          ${escapeHtml(data.meta?.defaultStatuses ? ("Finalized statuses: " + data.meta.defaultStatuses.join(", ")) : (allStatuses ? "All statuses included." : "Status filter applied."))}
        </p>
      </div>

      <div class="card">
        <h2>Breakdown</h2>
        <div class="table-shell">
          <div class="table-scroll">
            <table>
              <thead><tr><th>Fund</th><th>Donation</th><th>Processing fee</th><th>PayFast fee</th><th>Net received</th><th>Total charged</th><th>Count</th></tr></thead>
              <tbody>
                ${byFund.length ? byFund.map((r) => `
                  <tr>
                    <td>${escapeHtml(r.fundName || r.fundCode || "-")}</td>
                    <td>${escapeHtml(formatMoneyZar(r.donationTotal))}</td>
                    <td>${escapeHtml(formatMoneyZar(r.feeTotal))}</td>
                    <td>${escapeHtml(formatMoneyZar(r.payfastFeeTotal))}</td>
                    <td>${escapeHtml(formatMoneyZar(r.netReceivedTotal))}</td>
                    <td>${escapeHtml(formatMoneyZar(r.totalCharged))}</td>
                    <td>${escapeHtml(String(r.transactionCount || 0))}</td>
                  </tr>
                `).join("") : `<tr><td colspan="7" class="muted">No records.</td></tr>`}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </section>

    <section class="section">
      <h3>By method</h3>
      <div class="card">
        <div class="table-shell">
          <div class="table-scroll">
            <table>
              <thead><tr><th>Method</th><th>Donation</th><th>Processing fee</th><th>PayFast fee</th><th>Net received</th><th>Total charged</th><th>Count</th></tr></thead>
              <tbody>
                ${byMethod.length ? byMethod.map((r) => `
                  <tr>
                    <td>${escapeHtml(String(r.provider || "unknown").toUpperCase())}</td>
                    <td>${escapeHtml(formatMoneyZar(r.donationTotal))}</td>
                    <td>${escapeHtml(formatMoneyZar(r.feeTotal))}</td>
                    <td>${escapeHtml(formatMoneyZar(r.payfastFeeTotal))}</td>
                    <td>${escapeHtml(formatMoneyZar(r.netReceivedTotal))}</td>
                    <td>${escapeHtml(formatMoneyZar(r.totalCharged))}</td>
                    <td>${escapeHtml(String(r.transactionCount || 0))}</td>
                  </tr>
                `).join("") : `<tr><td colspan="7" class="muted">No records.</td></tr>`}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </section>

    <section class="section">
      <h3>Transactions (most recent first)</h3>
      <div class="card">
        <div class="table-shell">
          <div class="table-scroll tx">
            <table>
              <thead>
                <tr>
                  <th>Reference</th>
                  <th>Status</th>
                  <th>Method</th>
                  <th>Donation</th>
                  <th>Processing fee</th>
                  <th>PayFast fee</th>
                  <th>Net received</th>
                  <th>Total charged</th>
                  <th>Fund</th>
                  <th>Member</th>
                  <th>Service date</th>
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>
                ${rows.length ? rows.map((r) => `
                  <tr>
                    <td>${escapeHtml(r.reference || "-")}</td>
                    <td><span class="pill ${STATEMENT_DEFAULT_STATUSES.includes(String(r.status || "").toUpperCase()) ? "ok" : "warn"}">${escapeHtml(String(r.status || "-"))}</span></td>
                    <td>${escapeHtml(String(r.provider || "-").toUpperCase())}</td>
                    <td>${escapeHtml(formatMoneyZar(r.amount))}</td>
                    <td>${escapeHtml(formatMoneyZar(r.platformFeeAmount))}</td>
                    <td>${escapeHtml(formatMoneyZar(r.payfastFeeAmount))}</td>
                    <td>${escapeHtml(formatMoneyZar(r.churchNetAmount))}</td>
                    <td>${escapeHtml(formatMoneyZar(r.amountGross))}</td>
                    <td>${escapeHtml(r.fundName || r.fundCode || "-")}</td>
                    <td>${escapeHtml(r.memberName || r.memberPhone || "-")}</td>
                    <td>${escapeHtml(formatDateIsoLike(r.serviceDate))}</td>
                    <td>${escapeHtml(new Date(r.createdAt).toISOString().replace("T", " ").slice(0, 19) + " UTC")}</td>
                  </tr>
                `).join("") : `<tr><td colspan="12" class="muted">No records for this period.</td></tr>`}
              </tbody>
            </table>
          </div>
        </div>
        <p class="muted" style="margin:10px 0 0;font-size:11px;">Rows shown: ${escapeHtml(String(rows.length))} (limit=${escapeHtml(String(maxRows))}).</p>
      </div>
    </section>

    <footer class="footer">
      <div>Powered by Churpay</div>
      <div class="muted">If you need help, contact Churpay support.</div>
    </footer>
  </main>

  <script>
    (function () {
      var autoprint = ${JSON.stringify(autoprint)};
      if (!autoprint) return;
      window.setTimeout(function () {
        try { window.print(); } catch (_) {}
      }, 250);
    })();
  </script>
</body>
</html>`;

    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    res.status(200).send(html);
  } catch (err) {
    console.error("[admin/statements/print] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.patch("/admin/members/:memberId/role", requireAdmin, async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, "me");
    if (!churchId) return;

    const memberId = String(req.params.memberId || "").trim();
    if (!memberId || !UUID_REGEX.test(memberId)) {
      return res.status(400).json({ error: "Valid memberId is required" });
    }

    if (String(req.user?.id || "") === memberId) {
      return res.status(400).json({ error: "You cannot change your own role." });
    }

    const role = String(req.body?.role || "").trim().toLowerCase();
    if (!["member", "admin", "accountant", "finance", "pastor", "volunteer", "usher", "teacher"].includes(role)) {
      return res.status(400).json({ error: "Invalid role" });
    }

    const existing = await db.oneOrNone(
      `select id, role from members where id=$1 and church_id=$2`,
      [memberId, churchId]
    );
    if (!existing) return res.status(404).json({ error: "Member not found" });

    const currentRole = String(existing.role || "").toLowerCase();
    if (currentRole === "admin" && role !== "admin") {
      const row = await db.one(
        `
        select count(*)::int as count
        from members
        where church_id=$1 and lower(role)='admin' and id <> $2
        `,
        [churchId, memberId]
      );
      if (Number(row.count || 0) <= 0) {
        return res.status(409).json({ error: "You cannot remove the last admin from this church." });
      }
    }

    const updated = await db.one(
      `
      update members
      set role=$3, updated_at=now()
      where id=$1 and church_id=$2
      returning
        id,
        full_name as "fullName",
        phone,
        email,
        date_of_birth as "dateOfBirth",
        role,
        created_at as "createdAt",
        updated_at as "updatedAt"
      `,
      [memberId, churchId, role]
    );

    return res.json({ ok: true, member: updated });
  } catch (err) {
    if (err?.code === "23514" && String(err?.constraint || "") === "members_role_valid") {
      return res.status(409).json({
        error: "Role update blocked by database role policy. Run staff role migrations and retry.",
        code: "MEMBER_ROLE_CONSTRAINT_OUTDATED",
      });
    }
    console.error("[admin/members] role update error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/admin/members", requireStaff, requireAdminPortalTabsAny("members"), async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, "me");
    if (!churchId) return;

    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);
    const offset = Math.max(Number(req.query.offset || 0), 0);
    const role = typeof req.query.role === "string" ? req.query.role.trim().toLowerCase() : "";
    const search = typeof req.query.search === "string" ? req.query.search.trim() : "";

    const where = ["m.church_id = $1"];
    const params = [churchId];
    let idx = 2;

    if (role) {
      params.push(role);
      where.push(`lower(m.role) = $${idx}`);
      idx++;
    }

    if (search) {
      const term = `%${search}%`;
      params.push(term);
      where.push(
        `(coalesce(m.full_name, '') ilike $${idx} or coalesce(m.email, '') ilike $${idx} or coalesce(m.phone, '') ilike $${idx})`
      );
      idx++;
    }

    const countRow = await db.one(
      `
      select count(*)::int as count
      from members m
      where ${where.join(" and ")}
      `,
      params
    );

    const rows = await db.manyOrNone(
      `
      select
        m.id,
        m.full_name as "fullName",
        m.phone,
        m.email,
        m.date_of_birth as "dateOfBirth",
        m.role,
        m.created_at as "createdAt",
        m.updated_at as "updatedAt"
      from members m
      where ${where.join(" and ")}
      order by case when lower(m.role) in ('admin','super','accountant','finance','pastor','volunteer','usher','teacher') then 0 else 1 end, m.created_at desc
      limit $${idx} offset $${idx + 1}
      `,
      [...params, limit, offset]
    );

    res.json({
      members: rows,
      meta: { limit, offset, count: Number(countRow.count || 0), returned: rows.length },
    });
  } catch (err) {
    if (err?.code === "42P01") {
      return res.status(404).json({ error: "Members endpoint unavailable" });
    }
    console.error("[admin/members] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Cash giving (no processor): member records cash giving for receipts/analytics.
router.post("/cash-givings", requireAuth, async (req, res) => {
  try {
    let { fundId, amount, flow, serviceDate, notes, channel = "app" } = req.body || {};

    const amt = Number(amount);
    if (!Number.isFinite(amt) || amt <= 0) {
      return res.status(400).json({ error: "Invalid amount" });
    }

    fundId = typeof fundId === "string" ? fundId.trim() : fundId;
    flow = typeof flow === "string" ? flow.trim().toLowerCase() : "";
    channel = typeof channel === "string" ? channel.trim() : channel;
    notes = typeof notes === "string" ? notes.trim() : null;

    if (!fundId || !UUID_REGEX.test(fundId)) {
      return res.status(400).json({ error: "Valid fundId is required" });
    }

    const desiredStatus = flow === "prepared" ? "PREPARED" : "RECORDED";
    const isoServiceDate =
      typeof serviceDate === "string" && /^\d{4}-\d{2}-\d{2}$/.test(serviceDate.trim())
        ? serviceDate.trim()
        : nextSundayIsoDate();

    const member = await loadMember(req.user.id);
    if (!member.church_id) return res.status(400).json({ error: "Join a church first" });
    const churchId = member.church_id;

    let fund, church;
    try {
      fund = await db.one("select id, code, name, active from funds where id=$1 and church_id=$2", [fundId, churchId]);
      church = await db.one("select id, name from churches where id=$1", [churchId]);
      if (!fund.active) return res.status(400).json({ error: "Fund is inactive" });
    } catch (err) {
      if (err.message.includes("Expected 1 row, got 0")) {
        return res.status(404).json({ error: "Church or fund not found" });
      }
      console.error("[cash-givings] DB error fetching church/fund", err);
      return res.status(500).json({ error: "Internal server error" });
    }

    const pricing = buildCashFeeBreakdown(amt);
    const reference = makeCashReference();
    const itemNameRaw = `${church.name} - ${fund.name} (Cash)`;
    const itemName = itemNameRaw
      .normalize("NFKD")
      .replace(/[^\x20-\x7E]/g, "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 100);

    const intent = await db.one(
      `
      insert into payment_intents
        (church_id, fund_id, amount, currency, status, provider, member_name, member_phone, payer_name, payer_phone, payer_type, channel, item_name, m_payment_id,
         platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct,
         service_date, notes, cash_verified_by_admin)
      values
        ($1,$2,$3,'ZAR',$4,'cash',$5,$6,$7,$8,'member',$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,false)
      returning *
      `,
      [
        churchId,
        fundId,
        pricing.amount,
        desiredStatus,
        member.full_name || "",
        member.phone || "",
        member.full_name || "",
        member.phone || "",
        channel || "app",
        itemName,
        reference,
        pricing.platformFeeAmount,
        pricing.platformFeePct,
        pricing.platformFeeFixed,
        pricing.amountGross,
        pricing.superadminCutAmount,
        pricing.superadminCutPct,
        isoServiceDate,
        notes,
      ]
    );

    const txRow = await db.one(
      `
      insert into transactions (
        church_id, fund_id, payment_intent_id,
        amount, platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct,
        payer_name, payer_phone, payer_email, payer_type,
        reference, channel, provider, provider_payment_id, created_at
      ) values (
        $1,$2,$3,
        $4,$5,$6,$7,$8,$9,$10,
        $11,$12,$13,'member',
        $14,$15,'cash',null,now()
      ) returning id, reference, created_at
      `,
      [
        churchId,
        fundId,
        intent.id,
        pricing.amount,
        pricing.platformFeeAmount,
        pricing.platformFeePct,
        pricing.platformFeeFixed,
        pricing.amountGross,
        pricing.superadminCutAmount,
        pricing.superadminCutPct,
        member.full_name || "",
        member.phone || "",
        member.email || null,
        reference,
        channel || "app",
      ]
    );

    // Best-effort: notify church staff to confirm the cash record.
    try {
      const staff = await db.manyOrNone(
        `
        select id
        from members
        where church_id=$1 and lower(role) in ('admin','accountant','finance','pastor','volunteer','usher','teacher')
        `,
        [churchId]
      );
      const amount = toCurrencyNumber(pricing.amount || 0);
      const statusLabel = String(intent.status || "").toUpperCase() || "RECORDED";
      for (const staffMember of staff) {
        await createNotification({
          memberId: staffMember.id,
          type: "CASH_RECORDED",
          title: "Cash giving recorded",
          body: `${member.full_name || "A member"} recorded R ${amount.toFixed(2)} cash to ${fund.name} (${statusLabel}).`,
          data: {
            paymentIntentId: intent.id,
            transactionId: txRow.id,
            reference,
            churchId,
            fundId,
            amount,
            status: statusLabel,
            provider: "cash",
            payerType: "member",
            serviceDate: isoServiceDate,
            requiresAdminConfirmation: true,
          },
        });
      }
    } catch (err) {
      console.error("[cash-givings] notify staff failed", err?.message || err);
    }

    res.status(201).json({
      paymentIntentId: intent.id,
      transactionId: txRow.id,
      reference: txRow.reference,
      method: "CASH",
      status: intent.status,
      amount: pricing.amount,
      pricing: {
        donationAmount: pricing.amount,
        churpayFee: pricing.platformFeeAmount,
        totalCharged: pricing.amountGross,
        feeEnabled: pricing.cashFeeEnabled,
        feeRate: pricing.platformFeePct,
      },
      serviceDate: isoServiceDate,
      notes: notes || null,
      fund: { id: fund.id, code: fund.code, name: fund.name },
      church: { id: church.id, name: church.name },
      createdAt: txRow.created_at,
    });
  } catch (err) {
    console.error("[cash-givings] POST error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Member creates a shareable giving link for an external payer (no login) to donate on their behalf.
router.post("/giving-links", requireAuth, async (req, res) => {
  try {
    const member = await loadMember(req.user.id);
    if (!member?.church_id) return res.status(400).json({ error: "Join a church first" });
    const churchId = member.church_id;

    const fundId = typeof req.body?.fundId === "string" ? req.body.fundId.trim() : "";
    const amountTypeRaw = typeof req.body?.amountType === "string" ? req.body.amountType.trim().toUpperCase() : "FIXED";
    const amountType = amountTypeRaw === "OPEN" ? "OPEN" : "FIXED";
    const amountFixed = amountType === "FIXED" ? toCurrencyNumber(req.body?.amountFixed) : null;
    const message = typeof req.body?.message === "string" ? req.body.message.trim().slice(0, 500) : null;
    const expiresInHoursRaw = Number(req.body?.expiresInHours || 48);
    const maxUsesRaw = Number(req.body?.maxUses || 1);

    if (!fundId || !UUID_REGEX.test(fundId)) return res.status(400).json({ error: "Valid fundId is required" });
    if (amountType === "FIXED" && (!Number.isFinite(amountFixed) || amountFixed <= 0)) {
      return res.status(400).json({ error: "amountFixed must be > 0 for FIXED links" });
    }

    const fund = await db.oneOrNone(
      `select id, code, name, coalesce(active,true) as active
       from funds
       where id=$1 and church_id=$2`,
      [fundId, churchId]
    );
    if (!fund) return res.status(404).json({ error: "Fund not found" });
    if (!fund.active) return res.status(400).json({ error: "Fund is inactive" });

    const expiresInHours = Number.isFinite(expiresInHoursRaw) ? expiresInHoursRaw : 48;
    const boundedHours = Math.max(1, Math.min(expiresInHours, 168)); // 1 hour .. 7 days
    const expiresAt = new Date(Date.now() + boundedHours * 60 * 60 * 1000);

    const maxUses = Number.isFinite(maxUsesRaw) ? Math.max(1, Math.min(maxUsesRaw, 5)) : 1;

    let link = null;
    for (let attempt = 0; attempt < 5; attempt += 1) {
      const token = makeGivingLinkToken();
      try {
        link = await db.one(
          `
          insert into giving_links (
            token, requester_member_id, church_id, fund_id,
            amount_type, amount_fixed, currency, message,
            status, expires_at, max_uses, use_count, created_at
          ) values (
            $1,$2,$3,$4,
            $5,$6,'ZAR',$7,
            'ACTIVE',$8,$9,0,now()
          )
          returning
            id,
            token,
            amount_type as "amountType",
            amount_fixed as "amountFixed",
            status,
            expires_at as "expiresAt",
            max_uses as "maxUses",
            use_count as "useCount",
            created_at as "createdAt"
          `,
          [token, member.id, churchId, fundId, amountType, amountFixed, message, expiresAt, maxUses]
        );
        break;
      } catch (err) {
        if (String(err?.code || "") === "23505") continue; // token collision
        throw err;
      }
    }
    if (!link) return res.status(500).json({ error: "Failed to create giving link" });

    const shareUrl = `${normalizeWebBaseUrl()}/l/${encodeURIComponent(link.token)}`;

    return res.status(201).json({
      data: {
        givingLink: {
          id: link.id,
          token: link.token,
          amountType: link.amountType,
          amountFixed: link.amountFixed === null ? null : Number(link.amountFixed),
          status: link.status,
          expiresAt: link.expiresAt,
          maxUses: link.maxUses,
          useCount: link.useCount,
          createdAt: link.createdAt,
          message,
        },
        shareUrl,
        fund: { id: fund.id, code: fund.code, name: fund.name },
      },
    });
  } catch (err) {
    console.error("[giving-links] create error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get(
  "/admin/church-life/ministries",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("volunteers.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const includeInactive = toBoolean(req.query?.includeInactive) === true;
      const limit = parseChurchLifeLimit(req.query?.limit, 120, 500);
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      const rows = await db.manyOrNone(
        `
        select
          m.id,
          m.church_id as "churchId",
          m.campus_id as "campusId",
          cc.name as "campusName",
          cc.code as "campusCode",
          m.name,
          m.code,
          m.description,
          m.active,
          m.created_at as "createdAt",
          m.updated_at as "updatedAt",
          coalesce(role_agg.count, 0)::int as "roleCount",
          coalesce(term_agg.count, 0)::int as "activeVolunteerCount"
        from church_ministries m
        left join church_campuses cc on cc.id = m.campus_id
        left join lateral (
          select count(*)::int as count
          from church_ministry_roles r
          where r.church_id = m.church_id and r.ministry_id = m.id and r.active = true
        ) role_agg on true
        left join lateral (
          select count(*)::int as count
          from volunteer_role_terms t
          where t.church_id = m.church_id and t.ministry_id = m.id and t.status in ('ACTIVE', 'REVIEW_DUE')
        ) term_agg on true
        where m.church_id=$1
          and ($2::boolean = true or m.active = true)
          and ($3::uuid is null or m.campus_id = $3)
        order by lower(m.name) asc, m.created_at asc
        limit $4
        `,
        [churchId, includeInactive, campusId, limit]
      );

      return res.json({
        ok: true,
        ministries: rows.map(normalizeMinistryRow),
        meta: { includeInactive, limit, campusId: campusId || null, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/ministries] list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/ministries",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("volunteers.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;

      const name = String(req.body?.name || "").trim().slice(0, 140);
      if (!name) return res.status(400).json({ error: "name is required." });
      const codeInput = String(req.body?.code || "").trim().slice(0, 32);
      const code = codeInput ? codeInput.toUpperCase() : null;
      const description = String(req.body?.description || "").trim().slice(0, 1200) || null;
      const active = toBoolean(req.body?.active) !== false;
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.body?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      const inserted = await db.one(
        `
        insert into church_ministries (
          church_id, campus_id, name, code, description, active, created_by, updated_by
        )
        values ($1,$2,$3,$4,$5,$6,$7,$7)
        returning id
        `,
        [churchId, campusId, name, code, description, active, req.user?.id || null]
      );

      const ministry = await loadChurchMinistryWithStats(churchId, inserted.id);
      await writeChurchLifeAuditLog({
        churchId,
        actorMemberId: req.user?.id || null,
        actorRole: req.user?.role || null,
        action: "MINISTRY_CREATED",
        entityType: "MINISTRY",
        entityId: ministry?.id || null,
        entityRef: ministry?.code || ministry?.name || null,
        before: {},
        after: { ministry: normalizeMinistryRow(ministry) },
        meta: { route: "admin/church-life/ministries:create" },
      });

      return res.status(201).json({ ok: true, ministry: normalizeMinistryRow(ministry) });
    } catch (err) {
      if (err?.code === "23505") {
        return res.status(409).json({ error: "A ministry with the same name/code already exists." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/ministries] create error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/church-life/ministries/:ministryId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("volunteers.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const ministryId = String(req.params?.ministryId || "").trim();
      if (!UUID_REGEX.test(ministryId)) return res.status(400).json({ error: "Invalid ministryId" });

      const current = await loadChurchMinistryWithStats(churchId, ministryId);
      if (!current) return res.status(404).json({ error: "Ministry not found." });

      const name = String(typeof req.body?.name === "undefined" ? current.name || "" : req.body?.name || "")
        .trim()
        .slice(0, 140);
      if (!name) return res.status(400).json({ error: "name is required." });
      const codeInput =
        typeof req.body?.code === "undefined" ? String(current.code || "") : String(req.body?.code || "");
      const code = codeInput.trim().slice(0, 32);
      const normalizedCode = code ? code.toUpperCase() : null;
      const description = String(
        typeof req.body?.description === "undefined" ? current.description || "" : req.body?.description || ""
      )
        .trim()
        .slice(0, 1200) || null;
      const active = typeof req.body?.active === "undefined" ? current.active !== false : toBoolean(req.body?.active) !== false;
      let campusId = current.campusId || null;
      if (typeof req.body?.campusId !== "undefined") {
        try {
          campusId = await resolveChurchCampusId(churchId, req.body?.campusId, { fieldName: "campusId" });
        } catch (campusErr) {
          return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
        }
      }

      await db.none(
        `
        update church_ministries
        set
          campus_id = $3,
          name = $4,
          code = $5,
          description = $6,
          active = $7,
          updated_by = $8,
          updated_at = now()
        where church_id=$1 and id=$2
        `,
        [churchId, ministryId, campusId, name, normalizedCode, description, active, req.user?.id || null]
      );

      const ministry = await loadChurchMinistryWithStats(churchId, ministryId);
      await writeChurchLifeAuditLog({
        churchId,
        actorMemberId: req.user?.id || null,
        actorRole: req.user?.role || null,
        action: "MINISTRY_UPDATED",
        entityType: "MINISTRY",
        entityId: ministry?.id || null,
        entityRef: ministry?.code || ministry?.name || null,
        before: { ministry: normalizeMinistryRow(current) },
        after: { ministry: normalizeMinistryRow(ministry) },
        meta: { route: "admin/church-life/ministries:patch" },
      });

      return res.json({ ok: true, ministry: normalizeMinistryRow(ministry) });
    } catch (err) {
      if (err?.code === "23505") {
        return res.status(409).json({ error: "A ministry with the same name/code already exists." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/ministries] patch error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/ministry-roles",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("volunteers.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const includeInactive = toBoolean(req.query?.includeInactive) === true;
      const limit = parseChurchLifeLimit(req.query?.limit, 160, 500);
      const ministryId = String(req.query?.ministryId || "").trim();
      if (ministryId && !UUID_REGEX.test(ministryId)) {
        return res.status(400).json({ error: "ministryId must be a UUID." });
      }

      const rows = await db.manyOrNone(
        `
        select
          r.id,
          r.church_id as "churchId",
          r.ministry_id as "ministryId",
          m.name as "ministryName",
          r.role_name as "roleName",
          r.default_term_months as "defaultTermMonths",
          r.responsibilities_template as "responsibilitiesTemplate",
          r.active,
          r.created_at as "createdAt",
          r.updated_at as "updatedAt",
          coalesce(term_agg.count, 0)::int as "activeTermsCount"
        from church_ministry_roles r
        join church_ministries m on m.id = r.ministry_id and m.church_id = r.church_id
        left join lateral (
          select count(*)::int as count
          from volunteer_role_terms t
          where t.church_id = r.church_id
            and t.ministry_role_id = r.id
            and t.status in ('ACTIVE', 'REVIEW_DUE')
        ) term_agg on true
        where r.church_id=$1
          and ($2::uuid is null or r.ministry_id=$2)
          and ($3::boolean = true or r.active = true)
        order by lower(m.name) asc, lower(r.role_name) asc, r.created_at asc
        limit $4
        `,
        [churchId, ministryId || null, includeInactive, limit]
      );

      return res.json({
        ok: true,
        ministryRoles: rows.map(normalizeMinistryRoleRow),
        meta: { includeInactive, limit, ministryId: ministryId || null, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/ministry-roles] list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/ministry-roles",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("volunteers.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const ministryId = String(req.body?.ministryId || "").trim();
      if (!UUID_REGEX.test(ministryId)) return res.status(400).json({ error: "ministryId is required." });
      const ministry = await findChurchMinistryById(churchId, ministryId);
      if (!ministry) return res.status(400).json({ error: "ministryId is not in this church." });

      const roleName = String(req.body?.roleName || "").trim().slice(0, 120);
      if (!roleName) return res.status(400).json({ error: "roleName is required." });
      const defaultTermMonthsRaw = req.body?.defaultTermMonths;
      const defaultTermMonths =
        typeof defaultTermMonthsRaw === "undefined"
          ? 12
          : parsePositiveInt(defaultTermMonthsRaw, null);
      if (!defaultTermMonths || defaultTermMonths < 1 || defaultTermMonths > 120) {
        return res.status(400).json({ error: "defaultTermMonths must be between 1 and 120." });
      }
      const responsibilitiesTemplate = normalizeResponsibilitiesTemplate(req.body?.responsibilitiesTemplate);
      const active = toBoolean(req.body?.active) !== false;

      const inserted = await db.one(
        `
        insert into church_ministry_roles (
          church_id, ministry_id, role_name, default_term_months, responsibilities_template, active, created_by, updated_by
        )
        values ($1,$2,$3,$4,$5::jsonb,$6,$7,$7)
        returning id
        `,
        [
          churchId,
          ministryId,
          roleName,
          defaultTermMonths,
          JSON.stringify(responsibilitiesTemplate),
          active,
          req.user?.id || null,
        ]
      );

      const ministryRole = await loadChurchMinistryRoleWithStats(churchId, inserted.id);
      await writeChurchLifeAuditLog({
        churchId,
        actorMemberId: req.user?.id || null,
        actorRole: req.user?.role || null,
        action: "MINISTRY_ROLE_CREATED",
        entityType: "MINISTRY_ROLE",
        entityId: ministryRole?.id || null,
        entityRef: ministryRole?.roleName || null,
        before: {},
        after: { ministryRole: normalizeMinistryRoleRow(ministryRole) },
        meta: { route: "admin/church-life/ministry-roles:create" },
      });

      return res.status(201).json({ ok: true, ministryRole: normalizeMinistryRoleRow(ministryRole) });
    } catch (err) {
      if (err?.code === "23505") {
        return res.status(409).json({ error: "This role already exists for the ministry." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/ministry-roles] create error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/church-life/ministry-roles/:ministryRoleId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("volunteers.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const ministryRoleId = String(req.params?.ministryRoleId || "").trim();
      if (!UUID_REGEX.test(ministryRoleId)) return res.status(400).json({ error: "Invalid ministryRoleId" });

      const current = await loadChurchMinistryRoleWithStats(churchId, ministryRoleId);
      if (!current) return res.status(404).json({ error: "Ministry role not found." });

      let ministryId = current.ministryId;
      if (typeof req.body?.ministryId !== "undefined") {
        const requestedMinistryId = String(req.body?.ministryId || "").trim();
        if (!UUID_REGEX.test(requestedMinistryId)) return res.status(400).json({ error: "ministryId must be a UUID." });
        const ministry = await findChurchMinistryById(churchId, requestedMinistryId);
        if (!ministry) return res.status(400).json({ error: "ministryId is not in this church." });
        ministryId = ministry.id;
      }

      const roleName = String(
        typeof req.body?.roleName === "undefined" ? current.roleName || "" : req.body?.roleName || ""
      )
        .trim()
        .slice(0, 120);
      if (!roleName) return res.status(400).json({ error: "roleName is required." });

      const termMonthsRaw =
        typeof req.body?.defaultTermMonths === "undefined" ? current.defaultTermMonths : req.body?.defaultTermMonths;
      const defaultTermMonths = parsePositiveInt(termMonthsRaw, null);
      if (!defaultTermMonths || defaultTermMonths < 1 || defaultTermMonths > 120) {
        return res.status(400).json({ error: "defaultTermMonths must be between 1 and 120." });
      }

      const responsibilitiesTemplate =
        typeof req.body?.responsibilitiesTemplate === "undefined"
          ? normalizeResponsibilitiesTemplate(current.responsibilitiesTemplate)
          : normalizeResponsibilitiesTemplate(req.body?.responsibilitiesTemplate);
      const active = typeof req.body?.active === "undefined" ? current.active !== false : toBoolean(req.body?.active) !== false;

      await db.none(
        `
        update church_ministry_roles
        set
          ministry_id = $3,
          role_name = $4,
          default_term_months = $5,
          responsibilities_template = $6::jsonb,
          active = $7,
          updated_by = $8,
          updated_at = now()
        where church_id = $1 and id = $2
        `,
        [
          churchId,
          ministryRoleId,
          ministryId,
          roleName,
          defaultTermMonths,
          JSON.stringify(responsibilitiesTemplate),
          active,
          req.user?.id || null,
        ]
      );

      const ministryRole = await loadChurchMinistryRoleWithStats(churchId, ministryRoleId);
      await writeChurchLifeAuditLog({
        churchId,
        actorMemberId: req.user?.id || null,
        actorRole: req.user?.role || null,
        action: "MINISTRY_ROLE_UPDATED",
        entityType: "MINISTRY_ROLE",
        entityId: ministryRole?.id || null,
        entityRef: ministryRole?.roleName || null,
        before: { ministryRole: normalizeMinistryRoleRow(current) },
        after: { ministryRole: normalizeMinistryRoleRow(ministryRole) },
        meta: { route: "admin/church-life/ministry-roles:patch" },
      });

      return res.json({ ok: true, ministryRole: normalizeMinistryRoleRow(ministryRole) });
    } catch (err) {
      if (err?.code === "23505") {
        return res.status(409).json({ error: "This role already exists for the ministry." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/ministry-roles] patch error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/volunteer-terms/attention",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("volunteers.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const access = getChurchLifeAccess(req);
      const canManage = hasChurchLifePermission(access, "volunteers.write");
      const memberPk = String(req.user?.id || "").trim();
      if (!canManage && !UUID_REGEX.test(memberPk)) {
        return res.status(403).json({ error: "Your profile is not linked for volunteer insights." });
      }

      const daysRaw = req.query?.days;
      const parsedDays = parsePositiveInt(daysRaw, 30);
      if (daysRaw !== undefined && daysRaw !== null && daysRaw !== "" && parsedDays === null) {
        return res.status(400).json({ error: "days must be a positive integer." });
      }
      const days = Math.min(Math.max(parsedDays || 30, 1), 180);
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      const summary = await db.one(
        `
        with scoped_terms as (
          select
            t.member_pk,
            t.end_date
          from volunteer_role_terms t
          join church_ministries ministry on ministry.id = t.ministry_id and ministry.church_id = t.church_id
          where t.church_id = $1
            and t.status in ('ACTIVE', 'REVIEW_DUE')
            and ($2::uuid is null or ministry.campus_id = $2::uuid)
            and ($3::boolean = true or t.member_pk = $4::uuid)
        ),
        load_by_member as (
          select member_pk, count(*)::int as active_roles
          from scoped_terms
          group by member_pk
        )
        select
          (select count(*)::int from scoped_terms) as "openTerms",
          (select count(*)::int from scoped_terms where end_date is not null and end_date <= (current_date + $5::int)) as "reviewsDue",
          (select count(*)::int from load_by_member where active_roles = 3) as "highLoadMembers",
          (select count(*)::int from load_by_member where active_roles >= 4) as "overloadMembers"
        `,
        [churchId, campusId, canManage, memberPk || null, days]
      );

      const overloadRows = await db.manyOrNone(
        `
        with scoped_terms as (
          select
            t.member_pk
          from volunteer_role_terms t
          join church_ministries ministry on ministry.id = t.ministry_id and ministry.church_id = t.church_id
          where t.church_id = $1
            and t.status in ('ACTIVE', 'REVIEW_DUE')
            and ($2::uuid is null or ministry.campus_id = $2::uuid)
            and ($3::boolean = true or t.member_pk = $4::uuid)
        ),
        load_by_member as (
          select member_pk, count(*)::int as active_roles
          from scoped_terms
          group by member_pk
        )
        select
          m.id as "memberPk",
          m.member_id as "memberId",
          m.full_name as "memberName",
          l.active_roles as "activeRoles"
        from load_by_member l
        join members m on m.id = l.member_pk and m.church_id = $1
        where l.active_roles >= 3
        order by l.active_roles desc, m.full_name asc
        limit 25
        `,
        [churchId, campusId, canManage, memberPk || null]
      );

      return res.json({
        ok: true,
        summary: {
          days,
          campusId: campusId || null,
          canManage,
          openTerms: Number(summary?.openTerms || 0),
          reviewsDue: Number(summary?.reviewsDue || 0),
          highLoadMembers: Number(summary?.highLoadMembers || 0),
          overloadMembers: Number(summary?.overloadMembers || 0),
        },
        overloadMembers: overloadRows.map((row) => ({
          memberPk: row?.memberPk || null,
          memberId: row?.memberId || null,
          memberName: row?.memberName || null,
          activeRoles: Number(row?.activeRoles || 0),
        })),
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/volunteer-terms] attention error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/volunteer-terms",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("volunteers.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const access = getChurchLifeAccess(req);
      const canManage = hasChurchLifePermission(access, "volunteers.write");
      const limit = parseChurchLifeLimit(req.query?.limit, 120, 500);
      const status = normalizeVolunteerTermStatus(req.query?.status, "");
      const memberPk = String(req.query?.memberPk || "").trim();
      const ministryId = String(req.query?.ministryId || "").trim();
      const ministryRoleId = String(req.query?.ministryRoleId || "").trim();
      const dueWithinDaysRaw = req.query?.dueWithinDays;
      const dueWithinDays =
        typeof dueWithinDaysRaw === "undefined" || dueWithinDaysRaw === null || dueWithinDaysRaw === ""
          ? null
          : parsePositiveInt(dueWithinDaysRaw, null);
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      if (memberPk && !UUID_REGEX.test(memberPk)) return res.status(400).json({ error: "memberPk must be a UUID." });
      if (ministryId && !UUID_REGEX.test(ministryId)) return res.status(400).json({ error: "ministryId must be a UUID." });
      if (ministryRoleId && !UUID_REGEX.test(ministryRoleId)) {
        return res.status(400).json({ error: "ministryRoleId must be a UUID." });
      }
      if (dueWithinDaysRaw !== undefined && dueWithinDays === null) {
        return res.status(400).json({ error: "dueWithinDays must be a positive integer." });
      }
      if (!canManage && memberPk && memberPk !== String(req.user?.id || "")) {
        return res.status(403).json({ error: "You can only view your own volunteer terms." });
      }

      const where = ["t.church_id = $1"];
      const params = [churchId];
      let idx = 2;

      if (status && VOLUNTEER_TERM_STATUSES.has(status)) {
        where.push(`t.status = $${idx}`);
        params.push(status);
        idx += 1;
      }
      if (memberPk) {
        where.push(`t.member_pk = $${idx}::uuid`);
        params.push(memberPk);
        idx += 1;
      }
      if (ministryId) {
        where.push(`t.ministry_id = $${idx}::uuid`);
        params.push(ministryId);
        idx += 1;
      }
      if (ministryRoleId) {
        where.push(`t.ministry_role_id = $${idx}::uuid`);
        params.push(ministryRoleId);
        idx += 1;
      }
      if (campusId) {
        where.push(`ministry.campus_id = $${idx}::uuid`);
        params.push(campusId);
        idx += 1;
      }
      if (dueWithinDays !== null) {
        where.push(`t.end_date is not null and t.end_date <= current_date + $${idx}::int`);
        params.push(dueWithinDays);
        idx += 1;
      }
      if (!canManage) {
        where.push(`t.member_pk = $${idx}::uuid`);
        params.push(req.user?.id || null);
        idx += 1;
      }

      params.push(limit);
      const rows = await db.manyOrNone(
        `
        select
          t.id,
          t.church_id as "churchId",
          t.member_pk as "memberPk",
          t.member_id as "memberId",
          m.full_name as "memberName",
          lower(coalesce(m.role, 'member')) as "memberRole",
          t.ministry_id as "ministryId",
          ministry.name as "ministryName",
          t.ministry_role_id as "ministryRoleId",
          role.role_name as "ministryRoleName",
          t.start_date as "startDate",
          t.end_date as "endDate",
          t.term_length_months as "termLengthMonths",
          t.status,
          t.end_reason as "endReason",
          t.attendance_rate as "attendanceRate",
          t.reliability_rate as "reliabilityRate",
          t.training_completed as "trainingCompleted",
          t.system_rating as "systemRating",
          t.manual_review_score as "manualReviewScore",
          t.performance_notes as "performanceNotes",
          t.reviewed_by as "reviewedBy",
          reviewer.full_name as "reviewedByName",
          t.reviewed_at as "reviewedAt",
          t.created_at as "createdAt",
          t.updated_at as "updatedAt",
          coalesce(active_agg.count, 0)::int as "activeRolesCount"
        from volunteer_role_terms t
        join members m on m.id = t.member_pk and m.church_id = t.church_id
        join church_ministries ministry on ministry.id = t.ministry_id and ministry.church_id = t.church_id
        join church_ministry_roles role on role.id = t.ministry_role_id and role.church_id = t.church_id
        left join members reviewer on reviewer.id = t.reviewed_by and reviewer.church_id = t.church_id
        left join lateral (
          select count(*)::int as count
          from volunteer_role_terms active_terms
          where active_terms.church_id = t.church_id
            and active_terms.member_pk = t.member_pk
            and active_terms.status in ('ACTIVE', 'REVIEW_DUE')
        ) active_agg on true
        where ${where.join(" and ")}
        order by
          case t.status
            when 'REVIEW_DUE' then 0
            when 'ACTIVE' then 1
            when 'RENEWED' then 2
            else 3
          end asc,
          coalesce(t.end_date, (current_date + 3650)) asc,
          t.created_at desc
        limit $${idx}
        `,
        params
      );

      return res.json({
        ok: true,
        volunteerTerms: rows.map(normalizeVolunteerTermRow),
        meta: {
          limit,
          status: status || null,
          memberPk: memberPk || (!canManage ? req.user?.id || null : null),
          ministryId: ministryId || null,
          ministryRoleId: ministryRoleId || null,
          dueWithinDays,
          campusId: campusId || null,
          canManage,
          returned: rows.length,
        },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/volunteer-terms] list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/volunteer-terms/reviews-due",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("volunteers.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const access = getChurchLifeAccess(req);
      const canManage = hasChurchLifePermission(access, "volunteers.write");
      const limit = parseChurchLifeLimit(req.query?.limit, 120, 400);
      const days = parsePositiveInt(req.query?.days, 30);

      const rows = await db.manyOrNone(
        `
        select
          t.id,
          t.church_id as "churchId",
          t.member_pk as "memberPk",
          t.member_id as "memberId",
          m.full_name as "memberName",
          lower(coalesce(m.role, 'member')) as "memberRole",
          t.ministry_id as "ministryId",
          ministry.name as "ministryName",
          t.ministry_role_id as "ministryRoleId",
          role.role_name as "ministryRoleName",
          t.start_date as "startDate",
          t.end_date as "endDate",
          t.term_length_months as "termLengthMonths",
          t.status,
          t.end_reason as "endReason",
          t.attendance_rate as "attendanceRate",
          t.reliability_rate as "reliabilityRate",
          t.training_completed as "trainingCompleted",
          t.system_rating as "systemRating",
          t.manual_review_score as "manualReviewScore",
          t.performance_notes as "performanceNotes",
          t.reviewed_by as "reviewedBy",
          reviewer.full_name as "reviewedByName",
          t.reviewed_at as "reviewedAt",
          t.created_at as "createdAt",
          t.updated_at as "updatedAt",
          coalesce(active_agg.count, 0)::int as "activeRolesCount"
        from volunteer_role_terms t
        join members m on m.id = t.member_pk and m.church_id = t.church_id
        join church_ministries ministry on ministry.id = t.ministry_id and ministry.church_id = t.church_id
        join church_ministry_roles role on role.id = t.ministry_role_id and role.church_id = t.church_id
        left join members reviewer on reviewer.id = t.reviewed_by and reviewer.church_id = t.church_id
        left join lateral (
          select count(*)::int as count
          from volunteer_role_terms active_terms
          where active_terms.church_id = t.church_id
            and active_terms.member_pk = t.member_pk
            and active_terms.status in ('ACTIVE', 'REVIEW_DUE')
        ) active_agg on true
        where t.church_id = $1
          and t.status in ('ACTIVE', 'REVIEW_DUE')
          and t.end_date is not null
          and t.end_date <= current_date + $2::int
          and ($3::boolean = true or t.member_pk = $4::uuid)
        order by t.end_date asc, t.created_at desc
        limit $5
        `,
        [churchId, days, canManage, req.user?.id || null, limit]
      );

      return res.json({
        ok: true,
        volunteerTerms: rows.map(normalizeVolunteerTermRow),
        meta: { days, limit, canManage, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/volunteer-terms] due list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/volunteer-terms",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("volunteers.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const memberPk = String(req.body?.memberPk || "").trim();
      const ministryRoleId = String(req.body?.ministryRoleId || "").trim();
      if (!UUID_REGEX.test(memberPk)) return res.status(400).json({ error: "memberPk is required." });
      if (!UUID_REGEX.test(ministryRoleId)) return res.status(400).json({ error: "ministryRoleId is required." });

      const member = await findMemberInChurchByUuid(churchId, memberPk);
      if (!member) return res.status(400).json({ error: "memberPk is not in this church." });

      const ministryRole = await findChurchMinistryRoleById(churchId, ministryRoleId);
      if (!ministryRole) return res.status(400).json({ error: "ministryRoleId is not in this church." });

      const startDate = parseChurchLifeDate(req.body?.startDate) || new Date().toISOString().slice(0, 10);
      const termLengthMonthsRaw =
        typeof req.body?.termLengthMonths === "undefined" ? ministryRole.defaultTermMonths : req.body?.termLengthMonths;
      const termLengthMonths = parsePositiveInt(termLengthMonthsRaw, null);
      if (!termLengthMonths || termLengthMonths < 1 || termLengthMonths > 120) {
        return res.status(400).json({ error: "termLengthMonths must be between 1 and 120." });
      }
      const providedEndDate = parseChurchLifeDate(req.body?.endDate);
      let endDate = providedEndDate || buildVolunteerTermDefaultEndDate(startDate, termLengthMonths);
      if (!endDate) return res.status(400).json({ error: "Unable to compute endDate from startDate/termLengthMonths." });

      const status = normalizeVolunteerTermStatus(req.body?.status, "ACTIVE");
      const endReason = normalizeVolunteerTermEndReason(req.body?.endReason);
      if (status === "ENDED" && !endReason) {
        return res.status(400).json({ error: "endReason is required when status is ENDED." });
      }
      if (status === "RENEWED" && !endReason) {
        return res.status(400).json({ error: "endReason is required when status is RENEWED." });
      }
      const attendanceRateRaw = typeof req.body?.attendanceRate === "undefined" ? null : req.body?.attendanceRate;
      const attendanceRate = parseBoundedDecimal(attendanceRateRaw, { min: 0, max: 100, decimals: 2 });
      if (attendanceRateRaw !== null && attendanceRateRaw !== undefined && attendanceRateRaw !== "" && attendanceRate === null) {
        return res.status(400).json({ error: "attendanceRate must be between 0 and 100." });
      }
      const reliabilityRateRaw = typeof req.body?.reliabilityRate === "undefined" ? null : req.body?.reliabilityRate;
      const reliabilityRate = parseBoundedDecimal(reliabilityRateRaw, { min: 0, max: 100, decimals: 2 });
      if (reliabilityRateRaw !== null && reliabilityRateRaw !== undefined && reliabilityRateRaw !== "" && reliabilityRate === null) {
        return res.status(400).json({ error: "reliabilityRate must be between 0 and 100." });
      }
      const manualReviewScoreRaw =
        typeof req.body?.manualReviewScore === "undefined" ? null : req.body?.manualReviewScore;
      const manualReviewScore = parseBoundedDecimal(manualReviewScoreRaw, { min: 0, max: 100, decimals: 2 });
      if (
        manualReviewScoreRaw !== null &&
        manualReviewScoreRaw !== undefined &&
        manualReviewScoreRaw !== "" &&
        manualReviewScore === null
      ) {
        return res.status(400).json({ error: "manualReviewScore must be between 0 and 100." });
      }
      const trainingCompleted = toBoolean(req.body?.trainingCompleted) === true;
      const systemRating = normalizeVolunteerSystemRating(req.body?.systemRating);
      const performanceNotes = String(req.body?.performanceNotes || "").trim().slice(0, 4000) || null;
      const reviewedBy = String(req.body?.reviewedBy || "").trim();
      if (reviewedBy && !UUID_REGEX.test(reviewedBy)) return res.status(400).json({ error: "reviewedBy must be a UUID." });
      let reviewedByMemberId = null;
      if (reviewedBy) {
        const reviewedByMember = await findMemberInChurchByUuid(churchId, reviewedBy);
        if (!reviewedByMember) return res.status(400).json({ error: "reviewedBy is not in this church." });
        reviewedByMemberId = reviewedByMember.id;
      }
      const reviewedAtInput = req.body?.reviewedAt;
      const reviewedAt =
        typeof reviewedAtInput === "undefined" || reviewedAtInput === null || reviewedAtInput === ""
          ? null
          : parseChurchLifeDateTimeOrNull(reviewedAtInput);
      if (reviewedAtInput && !reviewedAt) return res.status(400).json({ error: "reviewedAt is invalid." });

      const promoteFromTermId = String(req.body?.promoteFromTermId || "").trim();
      if (promoteFromTermId && !UUID_REGEX.test(promoteFromTermId)) {
        return res.status(400).json({ error: "promoteFromTermId must be a UUID." });
      }

      const result = await db.tx(async (t) => {
        let promotedFromBefore = null;
        if (promoteFromTermId) {
          promotedFromBefore = await loadVolunteerRoleTermById(churchId, promoteFromTermId, t);
          if (!promotedFromBefore) {
            throw Object.assign(new Error("promoteFromTermId not found in this church."), { statusCode: 400 });
          }
          if (promotedFromBefore.memberPk !== member.id || promotedFromBefore.ministryId !== ministryRole.ministryId) {
            throw Object.assign(new Error("promoteFromTermId must belong to the same member and ministry."), {
              statusCode: 400,
            });
          }

          await t.none(
            `
            update volunteer_role_terms
            set
              status = 'ENDED',
              end_reason = 'PROMOTED',
              end_date = coalesce(end_date, current_date),
              updated_by = $3,
              updated_at = now()
            where church_id = $1 and id = $2
            `,
            [churchId, promoteFromTermId, req.user?.id || null]
          );
        }

        const inserted = await t.one(
          `
          insert into volunteer_role_terms (
            church_id,
            member_pk,
            member_id,
            ministry_id,
            ministry_role_id,
            start_date,
            end_date,
            term_length_months,
            status,
            end_reason,
            attendance_rate,
            reliability_rate,
            training_completed,
            system_rating,
            manual_review_score,
            performance_notes,
            reviewed_by,
            reviewed_at,
            created_by,
            updated_by
          )
          values (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$19
          )
          returning id
          `,
          [
            churchId,
            member.id,
            member.memberId || null,
            ministryRole.ministryId,
            ministryRole.id,
            startDate,
            endDate,
            termLengthMonths,
            status,
            endReason,
            attendanceRate,
            reliabilityRate,
            trainingCompleted,
            systemRating,
            manualReviewScore,
            performanceNotes,
            reviewedByMemberId,
            reviewedAt,
            req.user?.id || null,
          ]
        );

        const term = await loadVolunteerRoleTermById(churchId, inserted.id, t);
        const promotedFromAfter = promoteFromTermId ? await loadVolunteerRoleTermById(churchId, promoteFromTermId, t) : null;
        return { term, promotedFromBefore, promotedFromAfter };
      });

      await writeChurchLifeAuditLog({
        churchId,
        actorMemberId: req.user?.id || null,
        actorRole: req.user?.role || null,
        action: "VOLUNTEER_TERM_CREATED",
        entityType: "VOLUNTEER_TERM",
        entityId: result?.term?.id || null,
        entityRef: result?.term?.memberId || result?.term?.memberPk || null,
        before: {},
        after: { term: normalizeVolunteerTermRow(result?.term) },
        meta: { route: "admin/church-life/volunteer-terms:create" },
      });

      if (result?.promotedFromBefore && result?.promotedFromAfter) {
        await writeChurchLifeAuditLog({
          churchId,
          actorMemberId: req.user?.id || null,
          actorRole: req.user?.role || null,
          action: "VOLUNTEER_TERM_ENDED",
          entityType: "VOLUNTEER_TERM",
          entityId: result.promotedFromAfter.id,
          entityRef: result.promotedFromAfter.memberId || result.promotedFromAfter.memberPk || null,
          before: { term: normalizeVolunteerTermRow(result.promotedFromBefore) },
          after: { term: normalizeVolunteerTermRow(result.promotedFromAfter) },
          meta: { route: "admin/church-life/volunteer-terms:promote" },
        });
      }

      return res.status(201).json({ ok: true, volunteerTerm: normalizeVolunteerTermRow(result?.term) });
    } catch (err) {
      if (err?.statusCode) {
        return res.status(Number(err.statusCode) || 400).json({ error: err?.message || "Invalid request" });
      }
      if (err?.code === "23505") {
        return res.status(409).json({ error: "Member already has an active role term for this ministry." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/volunteer-terms] create error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/church-life/volunteer-terms/:termId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("volunteers.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const termId = String(req.params?.termId || "").trim();
      if (!UUID_REGEX.test(termId)) return res.status(400).json({ error: "Invalid termId" });

      const current = await loadVolunteerRoleTermById(churchId, termId);
      if (!current) return res.status(404).json({ error: "Volunteer term not found." });

      const status = normalizeVolunteerTermStatus(
        typeof req.body?.status === "undefined" ? current.status : req.body?.status,
        current.status
      );
      const endReason =
        typeof req.body?.endReason === "undefined"
          ? normalizeVolunteerTermEndReason(current.endReason)
          : normalizeVolunteerTermEndReason(req.body?.endReason);

      const startDate = parseChurchLifeDate(
        typeof req.body?.startDate === "undefined" ? current.startDate : req.body?.startDate
      );
      if (!startDate) return res.status(400).json({ error: "startDate must be YYYY-MM-DD." });

      const termLengthMonthsRaw =
        typeof req.body?.termLengthMonths === "undefined" ? current.termLengthMonths : req.body?.termLengthMonths;
      const termLengthMonths = parsePositiveInt(termLengthMonthsRaw, null);
      if (!termLengthMonths || termLengthMonths < 1 || termLengthMonths > 120) {
        return res.status(400).json({ error: "termLengthMonths must be between 1 and 120." });
      }

      const requestedEndDateInput =
        typeof req.body?.endDate === "undefined" ? current.endDate : req.body?.endDate;
      let endDate = requestedEndDateInput ? parseChurchLifeDate(requestedEndDateInput) : null;
      if (requestedEndDateInput && !endDate) return res.status(400).json({ error: "endDate must be YYYY-MM-DD." });
      if (!endDate && (status === "ACTIVE" || status === "REVIEW_DUE")) {
        endDate = buildVolunteerTermDefaultEndDate(startDate, termLengthMonths);
      }
      if (!endDate && status === "ENDED") {
        endDate = new Date().toISOString().slice(0, 10);
      }
      if (!endDate) return res.status(400).json({ error: "endDate is required for this status." });

      if (status === "ENDED" && !endReason) {
        return res.status(400).json({ error: "endReason is required when status is ENDED." });
      }

      const attendanceRateRaw =
        typeof req.body?.attendanceRate === "undefined" ? current.attendanceRate : req.body?.attendanceRate;
      const attendanceRate = parseBoundedDecimal(attendanceRateRaw, { min: 0, max: 100, decimals: 2 });
      if (attendanceRateRaw !== null && attendanceRateRaw !== undefined && attendanceRateRaw !== "" && attendanceRate === null) {
        return res.status(400).json({ error: "attendanceRate must be between 0 and 100." });
      }
      const reliabilityRateRaw =
        typeof req.body?.reliabilityRate === "undefined" ? current.reliabilityRate : req.body?.reliabilityRate;
      const reliabilityRate = parseBoundedDecimal(reliabilityRateRaw, { min: 0, max: 100, decimals: 2 });
      if (
        reliabilityRateRaw !== null &&
        reliabilityRateRaw !== undefined &&
        reliabilityRateRaw !== "" &&
        reliabilityRate === null
      ) {
        return res.status(400).json({ error: "reliabilityRate must be between 0 and 100." });
      }
      const manualReviewScoreRaw =
        typeof req.body?.manualReviewScore === "undefined" ? current.manualReviewScore : req.body?.manualReviewScore;
      const manualReviewScore = parseBoundedDecimal(manualReviewScoreRaw, { min: 0, max: 100, decimals: 2 });
      if (
        manualReviewScoreRaw !== null &&
        manualReviewScoreRaw !== undefined &&
        manualReviewScoreRaw !== "" &&
        manualReviewScore === null
      ) {
        return res.status(400).json({ error: "manualReviewScore must be between 0 and 100." });
      }
      const trainingCompleted =
        typeof req.body?.trainingCompleted === "undefined"
          ? current.trainingCompleted === true
          : toBoolean(req.body?.trainingCompleted) === true;
      const systemRating =
        typeof req.body?.systemRating === "undefined"
          ? normalizeVolunteerSystemRating(current.systemRating)
          : normalizeVolunteerSystemRating(req.body?.systemRating);
      const performanceNotes = String(
        typeof req.body?.performanceNotes === "undefined" ? current.performanceNotes || "" : req.body?.performanceNotes || ""
      )
        .trim()
        .slice(0, 4000) || null;
      const reviewedByInput = String(
        typeof req.body?.reviewedBy === "undefined" ? current.reviewedBy || "" : req.body?.reviewedBy || ""
      ).trim();
      if (reviewedByInput && !UUID_REGEX.test(reviewedByInput)) {
        return res.status(400).json({ error: "reviewedBy must be a UUID." });
      }
      let reviewedBy = null;
      if (reviewedByInput) {
        const reviewedByMember = await findMemberInChurchByUuid(churchId, reviewedByInput);
        if (!reviewedByMember) return res.status(400).json({ error: "reviewedBy is not in this church." });
        reviewedBy = reviewedByMember.id;
      }
      const reviewedAtInput = typeof req.body?.reviewedAt === "undefined" ? current.reviewedAt : req.body?.reviewedAt;
      const reviewedAt =
        reviewedAtInput === null || reviewedAtInput === ""
          ? null
          : parseChurchLifeDateTimeOrNull(reviewedAtInput);
      if (reviewedAtInput && reviewedAtInput !== "" && !reviewedAt) {
        return res.status(400).json({ error: "reviewedAt is invalid." });
      }

      await db.none(
        `
        update volunteer_role_terms
        set
          start_date = $3,
          end_date = $4,
          term_length_months = $5,
          status = $6,
          end_reason = $7,
          attendance_rate = $8,
          reliability_rate = $9,
          training_completed = $10,
          system_rating = $11,
          manual_review_score = $12,
          performance_notes = $13,
          reviewed_by = $14,
          reviewed_at = $15,
          updated_by = $16,
          updated_at = now()
        where church_id = $1 and id = $2
        `,
        [
          churchId,
          termId,
          startDate,
          endDate,
          termLengthMonths,
          status,
          endReason,
          attendanceRate,
          reliabilityRate,
          trainingCompleted,
          systemRating,
          manualReviewScore,
          performanceNotes,
          reviewedBy,
          reviewedAt,
          req.user?.id || null,
        ]
      );

      const updated = await loadVolunteerRoleTermById(churchId, termId);
      const endedNow = current.status !== "ENDED" && updated?.status === "ENDED";
      await writeChurchLifeAuditLog({
        churchId,
        actorMemberId: req.user?.id || null,
        actorRole: req.user?.role || null,
        action: endedNow ? "VOLUNTEER_TERM_ENDED" : "VOLUNTEER_TERM_UPDATED",
        entityType: "VOLUNTEER_TERM",
        entityId: updated?.id || null,
        entityRef: updated?.memberId || updated?.memberPk || null,
        before: { term: normalizeVolunteerTermRow(current) },
        after: { term: normalizeVolunteerTermRow(updated) },
        meta: { route: "admin/church-life/volunteer-terms:patch" },
      });

      return res.json({ ok: true, volunteerTerm: normalizeVolunteerTermRow(updated) });
    } catch (err) {
      if (err?.code === "23505") {
        return res.status(409).json({ error: "Member already has an active role term for this ministry." });
      }
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/volunteer-terms] patch error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/schedules",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("schedules.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const access = getChurchLifeAccess(req);
      const canManage = hasChurchLifePermission(access, "schedules.write");
      const limit = parseChurchLifeLimit(req.query?.limit, 120, 500);
      const status = normalizeMinistryScheduleStatus(req.query?.status, "");
      const ministryId = String(req.query?.ministryId || "").trim();
      const fromDate = req.query?.fromDate ? parseChurchLifeDate(req.query?.fromDate) : null;
      const toDate = req.query?.toDate ? parseChurchLifeDate(req.query?.toDate) : null;
      if (req.query?.fromDate && !fromDate) return res.status(400).json({ error: "fromDate must be YYYY-MM-DD." });
      if (req.query?.toDate && !toDate) return res.status(400).json({ error: "toDate must be YYYY-MM-DD." });
      if (ministryId && !UUID_REGEX.test(ministryId)) return res.status(400).json({ error: "ministryId must be a UUID." });
      let campusId = null;
      try {
        campusId = await resolveChurchCampusId(churchId, req.query?.campusId, { fieldName: "campusId" });
      } catch (campusErr) {
        return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
      }

      const where = ["s.church_id = $1"];
      const params = [churchId];
      let idx = 2;

      if (status && MINISTRY_SCHEDULE_STATUSES.has(status)) {
        where.push(`s.status = $${idx}`);
        params.push(status);
        idx += 1;
      }
      if (ministryId) {
        where.push(`s.ministry_id = $${idx}::uuid`);
        params.push(ministryId);
        idx += 1;
      }
      if (campusId) {
        where.push(`s.campus_id = $${idx}::uuid`);
        params.push(campusId);
        idx += 1;
      }
      if (fromDate) {
        where.push(`s.schedule_date >= $${idx}::date`);
        params.push(fromDate);
        idx += 1;
      }
      if (toDate) {
        where.push(`s.schedule_date <= $${idx}::date`);
        params.push(toDate);
        idx += 1;
      }
      if (!canManage) {
        where.push(
          `exists (
            select 1
            from ministry_schedule_assignments mine
            where mine.schedule_id = s.id and mine.member_pk = $${idx}::uuid
          )`
        );
        params.push(req.user?.id || null);
        idx += 1;
      }

      params.push(limit);
      const rows = await db.manyOrNone(
        `
        select
          s.id,
          s.church_id as "churchId",
          s.campus_id as "campusId",
          cc.name as "campusName",
          cc.code as "campusCode",
          s.ministry_id as "ministryId",
          m.name as "ministryName",
          s.service_id as "serviceId",
          srv.service_name as "serviceName",
          s.schedule_date as "scheduleDate",
          s.title,
          s.notes,
          s.status,
          s.created_at as "createdAt",
          s.updated_at as "updatedAt",
          coalesce(assignment_agg.total, 0)::int as "assignmentCount",
          coalesce(assignment_agg.confirmed, 0)::int as "confirmedCount",
          coalesce(assignment_agg.served, 0)::int as "servedCount",
          coalesce(assignment_agg.no_show, 0)::int as "noShowCount"
        from ministry_schedules s
        join church_ministries m on m.id = s.ministry_id and m.church_id = s.church_id
        left join church_services srv on srv.id = s.service_id and srv.church_id = s.church_id
        left join church_campuses cc on cc.id = s.campus_id
        left join lateral (
          select
            count(*)::int as total,
            count(*) filter (where a.status = 'CONFIRMED')::int as confirmed,
            count(*) filter (where a.status = 'SERVED')::int as served,
            count(*) filter (where a.status = 'NO_SHOW')::int as no_show
          from ministry_schedule_assignments a
          where a.schedule_id = s.id
        ) assignment_agg on true
        where ${where.join(" and ")}
        order by s.schedule_date desc, s.created_at desc
        limit $${idx}
        `,
        params
      );

      return res.json({
        ok: true,
        schedules: rows.map(normalizeMinistryScheduleRow),
        meta: {
          limit,
          status: status || null,
          ministryId: ministryId || null,
          campusId: campusId || null,
          fromDate: fromDate || null,
          toDate: toDate || null,
          canManage,
          returned: rows.length,
        },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/schedules] list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/schedules",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("schedules.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const ministryId = String(req.body?.ministryId || "").trim();
      if (!UUID_REGEX.test(ministryId)) return res.status(400).json({ error: "ministryId is required." });
      const ministry = await findChurchMinistryById(churchId, ministryId);
      if (!ministry) return res.status(400).json({ error: "ministryId is not in this church." });

      const scheduleDate = parseChurchLifeDate(req.body?.scheduleDate);
      if (!scheduleDate) return res.status(400).json({ error: "scheduleDate must be YYYY-MM-DD." });

      const title = String(req.body?.title || `${ministry.name} Schedule`)
        .trim()
        .slice(0, 200);
      if (!title) return res.status(400).json({ error: "title is required." });
      const notes = String(req.body?.notes || "").trim().slice(0, 2000) || null;
      const status = normalizeMinistryScheduleStatus(req.body?.status, "DRAFT");
      const serviceIdInput = String(req.body?.serviceId || "").trim();
      let serviceId = null;
      if (serviceIdInput) {
        if (!UUID_REGEX.test(serviceIdInput)) return res.status(400).json({ error: "serviceId must be a UUID." });
        const service = await findChurchServiceById(churchId, serviceIdInput);
        if (!service) return res.status(400).json({ error: "serviceId is not in this church." });
        serviceId = service.id;
      }
      let campusId = null;
      if (typeof req.body?.campusId !== "undefined") {
        try {
          campusId = await resolveChurchCampusId(churchId, req.body?.campusId, { fieldName: "campusId" });
        } catch (campusErr) {
          return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
        }
      } else if (ministry?.campusId) {
        campusId = ministry.campusId;
      }

      const inserted = await db.one(
        `
        insert into ministry_schedules (
          church_id, campus_id, ministry_id, service_id, schedule_date, title, notes, status, created_by, updated_by
        )
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9)
        returning id
        `,
        [churchId, campusId, ministry.id, serviceId, scheduleDate, title, notes, status, req.user?.id || null]
      );

      const schedule = await loadMinistryScheduleWithStats(churchId, inserted.id);
      await writeChurchLifeAuditLog({
        churchId,
        actorMemberId: req.user?.id || null,
        actorRole: req.user?.role || null,
        action: "SCHEDULE_CREATED",
        entityType: "MINISTRY_SCHEDULE",
        entityId: schedule?.id || null,
        entityRef: schedule?.title || null,
        before: {},
        after: { schedule: normalizeMinistryScheduleRow(schedule) },
        meta: { route: "admin/church-life/schedules:create" },
      });

      return res.status(201).json({ ok: true, schedule: normalizeMinistryScheduleRow(schedule) });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/schedules] create error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/church-life/schedules/:scheduleId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("schedules.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const scheduleId = String(req.params?.scheduleId || "").trim();
      if (!UUID_REGEX.test(scheduleId)) return res.status(400).json({ error: "Invalid scheduleId" });

      const current = await loadMinistryScheduleWithStats(churchId, scheduleId);
      if (!current) return res.status(404).json({ error: "Schedule not found." });

      let ministryId = current.ministryId;
      if (typeof req.body?.ministryId !== "undefined") {
        const requestedMinistryId = String(req.body?.ministryId || "").trim();
        if (!UUID_REGEX.test(requestedMinistryId)) return res.status(400).json({ error: "ministryId must be a UUID." });
        const ministry = await findChurchMinistryById(churchId, requestedMinistryId);
        if (!ministry) return res.status(400).json({ error: "ministryId is not in this church." });
        ministryId = ministry.id;
      }

      const scheduleDate = parseChurchLifeDate(
        typeof req.body?.scheduleDate === "undefined" ? current.scheduleDate : req.body?.scheduleDate
      );
      if (!scheduleDate) return res.status(400).json({ error: "scheduleDate must be YYYY-MM-DD." });
      const title = String(typeof req.body?.title === "undefined" ? current.title || "" : req.body?.title || "")
        .trim()
        .slice(0, 200);
      if (!title) return res.status(400).json({ error: "title is required." });
      const notes = String(typeof req.body?.notes === "undefined" ? current.notes || "" : req.body?.notes || "")
        .trim()
        .slice(0, 2000) || null;
      const status = normalizeMinistryScheduleStatus(
        typeof req.body?.status === "undefined" ? current.status : req.body?.status,
        current.status
      );
      let serviceId = current.serviceId || null;
      if (typeof req.body?.serviceId !== "undefined") {
        const serviceIdInput = String(req.body?.serviceId || "").trim();
        if (!serviceIdInput) {
          serviceId = null;
        } else {
          if (!UUID_REGEX.test(serviceIdInput)) return res.status(400).json({ error: "serviceId must be a UUID." });
          const service = await findChurchServiceById(churchId, serviceIdInput);
          if (!service) return res.status(400).json({ error: "serviceId is not in this church." });
          serviceId = service.id;
        }
      }
      let campusId = current.campusId || null;
      if (typeof req.body?.campusId !== "undefined") {
        try {
          campusId = await resolveChurchCampusId(churchId, req.body?.campusId, { fieldName: "campusId" });
        } catch (campusErr) {
          return res.status(400).json({ error: campusErr?.message || "Invalid campusId" });
        }
      }

      await db.none(
        `
        update ministry_schedules
        set
          campus_id = $3,
          ministry_id = $4,
          service_id = $5,
          schedule_date = $6,
          title = $7,
          notes = $8,
          status = $9,
          updated_by = $10,
          updated_at = now()
        where church_id = $1 and id = $2
        `,
        [churchId, scheduleId, campusId, ministryId, serviceId, scheduleDate, title, notes, status, req.user?.id || null]
      );

      const schedule = await loadMinistryScheduleWithStats(churchId, scheduleId);
      await writeChurchLifeAuditLog({
        churchId,
        actorMemberId: req.user?.id || null,
        actorRole: req.user?.role || null,
        action: "SCHEDULE_UPDATED",
        entityType: "MINISTRY_SCHEDULE",
        entityId: schedule?.id || null,
        entityRef: schedule?.title || null,
        before: { schedule: normalizeMinistryScheduleRow(current) },
        after: { schedule: normalizeMinistryScheduleRow(schedule) },
        meta: { route: "admin/church-life/schedules:patch" },
      });

      return res.json({ ok: true, schedule: normalizeMinistryScheduleRow(schedule) });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/schedules] patch error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.get(
  "/admin/church-life/schedules/:scheduleId/assignments",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("schedules.read"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const scheduleId = String(req.params?.scheduleId || "").trim();
      if (!UUID_REGEX.test(scheduleId)) return res.status(400).json({ error: "Invalid scheduleId" });

      const access = getChurchLifeAccess(req);
      const canManage = hasChurchLifePermission(access, "schedules.write");
      const schedule = await loadMinistryScheduleWithStats(churchId, scheduleId);
      if (!schedule) return res.status(404).json({ error: "Schedule not found." });

      if (!canManage) {
        const mine = await db.oneOrNone(
          `
          select 1
          from ministry_schedule_assignments
          where church_id=$1 and schedule_id=$2 and member_pk=$3
          limit 1
          `,
          [churchId, scheduleId, req.user?.id || null]
        );
        if (!mine) return res.status(403).json({ error: "You are not assigned to this schedule." });
      }

      const status = normalizeMinistryScheduleAssignmentStatus(req.query?.status, "");
      const where = ["a.church_id = $1", "a.schedule_id = $2"];
      const params = [churchId, scheduleId];
      let idx = 3;
      if (status && MINISTRY_SCHEDULE_ASSIGNMENT_STATUSES.has(status)) {
        where.push(`a.status = $${idx}`);
        params.push(status);
      }

      const rows = await db.manyOrNone(
        `
        select
          a.id,
          a.church_id as "churchId",
          a.schedule_id as "scheduleId",
          a.member_pk as "memberPk",
          a.member_id as "memberId",
          m.full_name as "memberName",
          lower(coalesce(m.role, 'member')) as "memberRole",
          a.ministry_role_id as "ministryRoleId",
          r.role_name as "ministryRoleName",
          a.status,
          a.assigned_by as "assignedBy",
          a.responded_at as "respondedAt",
          a.served_at as "servedAt",
          a.notes,
          a.created_at as "createdAt",
          a.updated_at as "updatedAt"
        from ministry_schedule_assignments a
        join members m on m.id = a.member_pk and m.church_id = a.church_id
        left join church_ministry_roles r on r.id = a.ministry_role_id and r.church_id = a.church_id
        where ${where.join(" and ")}
        order by
          case a.status
            when 'SERVED' then 0
            when 'CONFIRMED' then 1
            when 'ASSIGNED' then 2
            when 'NO_SHOW' then 3
            else 4
          end asc,
          m.full_name asc
        `,
        params
      );

      return res.json({
        ok: true,
        schedule: normalizeMinistryScheduleRow(schedule),
        assignments: rows.map(normalizeMinistryScheduleAssignmentRow),
        meta: { status: status || null, canManage, returned: rows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/schedules] assignment list error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.post(
  "/admin/church-life/schedules/:scheduleId/assignments",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("schedules.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const scheduleId = String(req.params?.scheduleId || "").trim();
      if (!UUID_REGEX.test(scheduleId)) return res.status(400).json({ error: "Invalid scheduleId" });
      const schedule = await loadMinistryScheduleWithStats(churchId, scheduleId);
      if (!schedule) return res.status(404).json({ error: "Schedule not found." });

      const payloadRows = Array.isArray(req.body?.assignments)
        ? req.body.assignments
        : req.body && typeof req.body === "object"
          ? [req.body]
          : [];
      if (!payloadRows.length) return res.status(400).json({ error: "assignments payload is required." });

      const normalizedRows = payloadRows
        .map((row) => {
          const memberPk = String(row?.memberPk || "").trim();
          const ministryRoleId = String(row?.ministryRoleId || "").trim();
          const status = normalizeMinistryScheduleAssignmentStatus(row?.status, "ASSIGNED");
          const notes = String(row?.notes || "").trim().slice(0, 1200) || null;
          return {
            memberPk,
            ministryRoleId: ministryRoleId || null,
            status,
            notes,
          };
        })
        .filter((row) => UUID_REGEX.test(row.memberPk));

      if (!normalizedRows.length) {
        return res.status(400).json({ error: "At least one valid assignment row is required." });
      }

      const uniqueMemberPks = Array.from(new Set(normalizedRows.map((row) => row.memberPk)));
      const members = await db.manyOrNone(
        `
        select id, member_id as "memberId"
        from members
        where church_id = $1 and id = any($2::uuid[])
        `,
        [churchId, uniqueMemberPks]
      );
      const memberMap = new Map(members.map((row) => [row.id, row]));
      const invalidMembers = uniqueMemberPks.filter((id) => !memberMap.has(id));
      if (invalidMembers.length) {
        return res.status(400).json({
          error: "Some memberPk values are not in this church.",
          invalidMemberPks: invalidMembers.slice(0, 20),
        });
      }

      if (normalizedRows.some((row) => row.ministryRoleId && !UUID_REGEX.test(row.ministryRoleId))) {
        return res.status(400).json({ error: "ministryRoleId must be a UUID when provided." });
      }

      const uniqueRoleIds = Array.from(
        new Set(normalizedRows.map((row) => row.ministryRoleId).filter((roleId) => UUID_REGEX.test(roleId || "")))
      );
      if (uniqueRoleIds.length) {
        const validRoles = await db.manyOrNone(
          `
          select id
          from church_ministry_roles
          where church_id = $1
            and ministry_id = $2
            and id = any($3::uuid[])
          `,
          [churchId, schedule.ministryId, uniqueRoleIds]
        );
        const validRoleSet = new Set(validRoles.map((row) => row.id));
        const invalidRoleIds = uniqueRoleIds.filter((id) => !validRoleSet.has(id));
        if (invalidRoleIds.length) {
          return res.status(400).json({
            error: "Some ministryRoleId values are not part of this schedule's ministry.",
            invalidRoleIds: invalidRoleIds.slice(0, 20),
          });
        }
      }

      await db.tx(async (t) => {
        for (const row of normalizedRows) {
          const member = memberMap.get(row.memberPk);
          await t.none(
            `
            insert into ministry_schedule_assignments (
              church_id,
              schedule_id,
              member_pk,
              member_id,
              ministry_role_id,
              status,
              assigned_by,
              responded_at,
              served_at,
              notes,
              created_at,
              updated_at
            )
            values (
              $1,$2,$3,$4,$5,$6,$7,
              case when $6 in ('CONFIRMED','DECLINED','NO_SHOW') then now() else null end,
              case when $6 = 'SERVED' then now() else null end,
              $8, now(), now()
            )
            on conflict (schedule_id, member_pk)
            do update set
              member_id = excluded.member_id,
              ministry_role_id = excluded.ministry_role_id,
              status = excluded.status,
              assigned_by = excluded.assigned_by,
              responded_at = case
                when excluded.status in ('CONFIRMED','DECLINED','NO_SHOW')
                  then coalesce(ministry_schedule_assignments.responded_at, now())
                when excluded.status = 'SERVED'
                  then coalesce(ministry_schedule_assignments.responded_at, now())
                else ministry_schedule_assignments.responded_at
              end,
              served_at = case
                when excluded.status = 'SERVED'
                  then coalesce(ministry_schedule_assignments.served_at, now())
                when excluded.status in ('ASSIGNED','CONFIRMED','DECLINED','NO_SHOW')
                  then null
                else ministry_schedule_assignments.served_at
              end,
              notes = excluded.notes,
              updated_at = now()
            `,
            [
              churchId,
              scheduleId,
              row.memberPk,
              member?.memberId || null,
              row.ministryRoleId || null,
              row.status,
              req.user?.id || null,
              row.notes,
            ]
          );
        }
      });

      await writeChurchLifeAuditLog({
        churchId,
        actorMemberId: req.user?.id || null,
        actorRole: req.user?.role || null,
        action: "SCHEDULE_ASSIGNMENT_UPDATED",
        entityType: "MINISTRY_SCHEDULE",
        entityId: schedule.id,
        entityRef: schedule.title || null,
        before: {},
        after: { savedAssignments: normalizedRows.length },
        meta: { route: "admin/church-life/schedules/assignments:upsert" },
      });

      const assignmentRows = await db.manyOrNone(
        `
        select
          a.id,
          a.church_id as "churchId",
          a.schedule_id as "scheduleId",
          a.member_pk as "memberPk",
          a.member_id as "memberId",
          m.full_name as "memberName",
          lower(coalesce(m.role, 'member')) as "memberRole",
          a.ministry_role_id as "ministryRoleId",
          r.role_name as "ministryRoleName",
          a.status,
          a.assigned_by as "assignedBy",
          a.responded_at as "respondedAt",
          a.served_at as "servedAt",
          a.notes,
          a.created_at as "createdAt",
          a.updated_at as "updatedAt"
        from ministry_schedule_assignments a
        join members m on m.id = a.member_pk and m.church_id = a.church_id
        left join church_ministry_roles r on r.id = a.ministry_role_id and r.church_id = a.church_id
        where a.church_id = $1 and a.schedule_id = $2
        order by m.full_name asc
        `,
        [churchId, scheduleId]
      );

      return res.json({
        ok: true,
        schedule: normalizeMinistryScheduleRow(schedule),
        assignments: assignmentRows.map(normalizeMinistryScheduleAssignmentRow),
        meta: { saved: normalizedRows.length, returned: assignmentRows.length },
      });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/schedules] assignment upsert error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

router.patch(
  "/admin/church-life/schedules/:scheduleId/assignments/:assignmentId",
  requireChurchLifeStaff,
  requireAdminPortalTabsAny("operations"),
  requireChurchGrowthActive,
  requireChurchLifePermission("schedules.write"),
  async (req, res) => {
    try {
      const churchId = requireChurch(req, res);
      if (!churchId) return;
      const scheduleId = String(req.params?.scheduleId || "").trim();
      const assignmentId = String(req.params?.assignmentId || "").trim();
      if (!UUID_REGEX.test(scheduleId) || !UUID_REGEX.test(assignmentId)) {
        return res.status(400).json({ error: "Invalid scheduleId or assignmentId" });
      }

      const current = await db.oneOrNone(
        `
        select
          a.id,
          a.church_id as "churchId",
          a.schedule_id as "scheduleId",
          a.member_pk as "memberPk",
          a.member_id as "memberId",
          m.full_name as "memberName",
          lower(coalesce(m.role, 'member')) as "memberRole",
          a.ministry_role_id as "ministryRoleId",
          r.role_name as "ministryRoleName",
          a.status,
          a.assigned_by as "assignedBy",
          a.responded_at as "respondedAt",
          a.served_at as "servedAt",
          a.notes,
          a.created_at as "createdAt",
          a.updated_at as "updatedAt"
        from ministry_schedule_assignments a
        join members m on m.id = a.member_pk and m.church_id = a.church_id
        left join church_ministry_roles r on r.id = a.ministry_role_id and r.church_id = a.church_id
        where a.church_id = $1 and a.schedule_id = $2 and a.id = $3
        limit 1
        `,
        [churchId, scheduleId, assignmentId]
      );
      if (!current) return res.status(404).json({ error: "Schedule assignment not found." });

      let ministryRoleId = current.ministryRoleId || null;
      if (typeof req.body?.ministryRoleId !== "undefined") {
        const roleIdInput = String(req.body?.ministryRoleId || "").trim();
        if (!roleIdInput) {
          ministryRoleId = null;
        } else {
          if (!UUID_REGEX.test(roleIdInput)) return res.status(400).json({ error: "ministryRoleId must be a UUID." });
          const role = await db.oneOrNone(
            `
            select r.id
            from church_ministry_roles r
            join ministry_schedules s on s.ministry_id = r.ministry_id and s.church_id = r.church_id
            where r.church_id = $1 and s.id = $2 and r.id = $3
            limit 1
            `,
            [churchId, scheduleId, roleIdInput]
          );
          if (!role) return res.status(400).json({ error: "ministryRoleId is not valid for this schedule's ministry." });
          ministryRoleId = role.id;
        }
      }

      const status = normalizeMinistryScheduleAssignmentStatus(
        typeof req.body?.status === "undefined" ? current.status : req.body?.status,
        current.status
      );
      const notes = String(typeof req.body?.notes === "undefined" ? current.notes || "" : req.body?.notes || "")
        .trim()
        .slice(0, 1200) || null;

      await db.none(
        `
        update ministry_schedule_assignments
        set
          ministry_role_id = $4,
          status = $5,
          assigned_by = $6,
          responded_at = case
            when $5 in ('CONFIRMED','DECLINED','NO_SHOW')
              then coalesce(responded_at, now())
            when $5 = 'SERVED'
              then coalesce(responded_at, now())
            else responded_at
          end,
          served_at = case
            when $5 = 'SERVED'
              then coalesce(served_at, now())
            when $5 in ('ASSIGNED','CONFIRMED','DECLINED','NO_SHOW')
              then null
            else served_at
          end,
          notes = $7,
          updated_at = now()
        where church_id = $1 and schedule_id = $2 and id = $3
        `,
        [churchId, scheduleId, assignmentId, ministryRoleId, status, req.user?.id || null, notes]
      );

      const updated = await db.one(
        `
        select
          a.id,
          a.church_id as "churchId",
          a.schedule_id as "scheduleId",
          a.member_pk as "memberPk",
          a.member_id as "memberId",
          m.full_name as "memberName",
          lower(coalesce(m.role, 'member')) as "memberRole",
          a.ministry_role_id as "ministryRoleId",
          r.role_name as "ministryRoleName",
          a.status,
          a.assigned_by as "assignedBy",
          a.responded_at as "respondedAt",
          a.served_at as "servedAt",
          a.notes,
          a.created_at as "createdAt",
          a.updated_at as "updatedAt"
        from ministry_schedule_assignments a
        join members m on m.id = a.member_pk and m.church_id = a.church_id
        left join church_ministry_roles r on r.id = a.ministry_role_id and r.church_id = a.church_id
        where a.church_id = $1 and a.schedule_id = $2 and a.id = $3
        limit 1
        `,
        [churchId, scheduleId, assignmentId]
      );

      await writeChurchLifeAuditLog({
        churchId,
        actorMemberId: req.user?.id || null,
        actorRole: req.user?.role || null,
        action: "SCHEDULE_ASSIGNMENT_UPDATED",
        entityType: "MINISTRY_ASSIGNMENT",
        entityId: updated.id,
        entityRef: updated.memberId || updated.memberPk || null,
        before: { assignment: normalizeMinistryScheduleAssignmentRow(current) },
        after: { assignment: normalizeMinistryScheduleAssignmentRow(updated) },
        meta: { route: "admin/church-life/schedules/assignments:patch" },
      });

      return res.json({ ok: true, assignment: normalizeMinistryScheduleAssignmentRow(updated) });
    } catch (err) {
      if (err?.code === "42P01" || err?.code === "42703") {
        return res.status(503).json({ error: "Volunteer governance is not available yet. Run migrations and retry." });
      }
      console.error("[admin/church-life/schedules] assignment patch error", err?.message || err, err?.stack);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

// Admin verification for cash records (prepared/recorded).
router.get("/admin/cash-givings", requireStaff, requireAdminPortalTabsAny("transactions", "statements"), async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, "me");
    if (!churchId) return;

    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);
    const offset = Math.max(Number(req.query.offset || 0), 0);
    const fundId = typeof req.query.fundId === "string" ? req.query.fundId.trim() : "";
    const statusRaw = typeof req.query.status === "string" ? req.query.status.trim().toUpperCase() : "";
    const includeVerified = toBoolean(req.query.includeVerified) === true;

    const where = ["pi.church_id=$1", "pi.provider='cash'"];
    const params = [churchId];
    let idx = 2;

    if (!includeVerified) {
      where.push("coalesce(pi.cash_verified_by_admin,false)=false");
    }

    if (fundId && UUID_REGEX.test(fundId)) {
      params.push(fundId);
      where.push(`pi.fund_id=$${idx}`);
      idx++;
    }

    if (statusRaw) {
      params.push(statusRaw);
      where.push(`upper(coalesce(pi.status,''))=$${idx}`);
      idx++;
    } else {
      params.push(["PREPARED", "RECORDED"]);
      where.push(`upper(coalesce(pi.status,'')) = any($${idx})`);
      idx++;
    }

    const countRow = await db.one(
      `
      select count(*)::int as count
      from payment_intents pi
      where ${where.join(" and ")}
      `,
      params
    );

    params.push(limit);
    const limitIdx = idx;
    idx++;
    params.push(offset);
    const offsetIdx = idx;

    const rows = await db.manyOrNone(
      `
      select
        pi.id as "paymentIntentId",
        pi.m_payment_id as reference,
        pi.status,
        pi.amount,
        pi.amount_gross as "amountGross",
        pi.platform_fee_amount as "platformFeeAmount",
        pi.platform_fee_pct as "platformFeePct",
        pi.platform_fee_fixed as "platformFeeFixed",
        pi.superadmin_cut_amount as "superadminCutAmount",
        pi.superadmin_cut_pct as "superadminCutPct",
        pi.service_date as "serviceDate",
        pi.notes,
        coalesce(pi.cash_verified_by_admin,false) as "cashVerifiedByAdmin",
        pi.cash_verified_at as "cashVerifiedAt",
        pi.cash_verified_by as "cashVerifiedBy",
        pi.cash_verification_note as "cashVerificationNote",
        pi.payer_name as "payerName",
        pi.payer_phone as "payerPhone",
        pi.payer_email as "payerEmail",
        coalesce(pi.payer_type,'member') as "payerType",
        pi.channel,
        pi.created_at as "createdAt",
        t.id as "transactionId",
        f.id as "fundId",
        f.code as "fundCode",
        f.name as "fundName"
      from payment_intents pi
      join funds f on f.id = pi.fund_id
      left join transactions t on t.payment_intent_id = pi.id
      where ${where.join(" and ")}
      order by pi.created_at desc
      limit $${limitIdx} offset $${offsetIdx}
      `,
      params
    );

    return res.json({
      cashGivings: rows,
      meta: { limit, offset, count: Number(countRow.count || 0), returned: rows.length },
    });
  } catch (err) {
    console.error("[admin/cash-givings] list error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post(
  "/admin/cash-givings/:paymentIntentId/confirm",
  requireStaff,
  requireAdminPortalTabsAny("transactions", "statements"),
  async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, "me");
    if (!churchId) return;
    const { paymentIntentId } = req.params;
    if (!paymentIntentId || !UUID_REGEX.test(paymentIntentId)) {
      return res.status(400).json({ error: "Valid paymentIntentId is required" });
    }

    const updated = await db.oneOrNone(
      `
      update payment_intents
      set
        status='CONFIRMED',
        cash_verified_by_admin=true,
        cash_verified_by=$1,
        cash_verified_at=now(),
        updated_at=now()
      where id=$2 and church_id=$3 and provider='cash'
      returning
        id,
        status,
        church_id as "churchId",
        fund_id as "fundId",
        amount,
        member_phone as "memberPhone",
        m_payment_id as reference,
        service_date as "serviceDate",
        cash_verified_by_admin as "verifiedByAdmin"
      `,
      [req.user.id, paymentIntentId, churchId]
    );
    if (!updated) return res.status(404).json({ error: "Cash giving not found" });

    // Best-effort: notify the member who created the cash record.
    if (updated.memberPhone) {
      try {
        const member = await db.oneOrNone(
          `select id from members where phone=$1 and church_id=$2`,
          [String(updated.memberPhone || "").trim(), churchId]
        );
        if (member?.id) {
          const fund = await db.oneOrNone(`select name from funds where id=$1 and church_id=$2`, [updated.fundId, churchId]);
          const amount = toCurrencyNumber(updated.amount || 0);
          const fundName = String(fund?.name || "").trim() || "a fund";
          await createNotification({
            memberId: member.id,
            type: "CASH_CONFIRMED",
            title: "Cash giving confirmed",
            body: `Your cash record of R ${amount.toFixed(2)} to ${fundName} was confirmed.`,
            data: {
              paymentIntentId: updated.id,
              reference: updated.reference,
              churchId,
              fundId: updated.fundId,
              amount,
              status: "CONFIRMED",
            },
          });
        }
      } catch (err) {
        console.error("[admin/cash-givings] notify member (confirm) failed", err?.message || err);
      }
    }

    res.json({ ok: true, cashGiving: updated });
  } catch (err) {
    console.error("[admin/cash-givings] confirm error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
  }
);

router.post(
  "/admin/cash-givings/:paymentIntentId/reject",
  requireStaff,
  requireAdminPortalTabsAny("transactions", "statements"),
  async (req, res) => {
  try {
    const churchId = resolveChurchId(req, res, "me");
    if (!churchId) return;
    const { paymentIntentId } = req.params;
    if (!paymentIntentId || !UUID_REGEX.test(paymentIntentId)) {
      return res.status(400).json({ error: "Valid paymentIntentId is required" });
    }
    const note = typeof req.body?.note === "string" ? req.body.note.trim() : "";
    if (!note) return res.status(400).json({ error: "note is required when rejecting" });

    const updated = await db.oneOrNone(
      `
      update payment_intents
      set
        status='REJECTED',
        cash_verified_by_admin=true,
        cash_verified_by=$1,
        cash_verified_at=now(),
        cash_verification_note=$2,
        updated_at=now()
      where id=$3 and church_id=$4 and provider='cash'
      returning
        id,
        status,
        church_id as "churchId",
        fund_id as "fundId",
        amount,
        member_phone as "memberPhone",
        m_payment_id as reference,
        service_date as "serviceDate",
        cash_verification_note as note
      `,
      [req.user.id, note, paymentIntentId, churchId]
    );
    if (!updated) return res.status(404).json({ error: "Cash giving not found" });

    // Best-effort: notify the member who created the cash record.
    if (updated.memberPhone) {
      try {
        const member = await db.oneOrNone(
          `select id from members where phone=$1 and church_id=$2`,
          [String(updated.memberPhone || "").trim(), churchId]
        );
        if (member?.id) {
          const fund = await db.oneOrNone(`select name from funds where id=$1 and church_id=$2`, [updated.fundId, churchId]);
          const amount = toCurrencyNumber(updated.amount || 0);
          const fundName = String(fund?.name || "").trim() || "a fund";
          const rejectionNote = String(updated.note || "").trim();
          await createNotification({
            memberId: member.id,
            type: "CASH_REJECTED",
            title: "Cash giving rejected",
            body: rejectionNote
              ? `Your cash record of R ${amount.toFixed(2)} to ${fundName} was rejected: ${rejectionNote}`
              : `Your cash record of R ${amount.toFixed(2)} to ${fundName} was rejected.`,
            data: {
              paymentIntentId: updated.id,
              reference: updated.reference,
              churchId,
              fundId: updated.fundId,
              amount,
              status: "REJECTED",
              note: rejectionNote || null,
            },
          });
        }
      } catch (err) {
        console.error("[admin/cash-givings] notify member (reject) failed", err?.message || err);
      }
    }

    res.json({ ok: true, cashGiving: updated });
  } catch (err) {
    console.error("[admin/cash-givings] reject error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
  }
);

export default router;
