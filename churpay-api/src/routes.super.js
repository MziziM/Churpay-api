import express from "express";
import crypto from "node:crypto";
import bcrypt from "bcryptjs";
import { authenticateSuperAdmin, getSuperAdminConfig, requireSuperAdmin, signSuperToken } from "./auth.js";
import { db } from "./db.js";
import { ensureUniqueJoinCode } from "./join-code.js";
import { sendEmail } from "./email-delivery.js";
import { createVerificationChallenge } from "./email-verification.js";
import {
  issueLoginTwoFactorChallenge,
  superTwoFactorEnabled,
  verifyLoginTwoFactorChallenge,
} from "./login-two-factor.js";

const router = express.Router();

const MAX_ONBOARDING_DOC_BYTES = 10 * 1024 * 1024;
const allowedOnboardingMimeTypes = new Set([
  "application/pdf",
  "image/jpeg",
  "image/png",
  "image/webp",
]);

function parseDate(value, endOfDay = false) {
  if (!value) return null;
  const suffix = endOfDay ? "T23:59:59.999Z" : "T00:00:00.000Z";
  const parsed = new Date(`${value}${suffix}`);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function normalize(value) {
  return String(value || "").trim();
}

function normalizeEmail(value) {
  const email = normalize(value).toLowerCase();
  if (!email) return "";
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? email : "";
}

function normalizePhone(value) {
  const phone = normalize(value).replace(/\s+/g, "");
  if (!phone) return "";
  return /^[+\d][\d-]{6,20}$/.test(phone) ? phone : "";
}

function normalizeAccountNumber(value) {
  const raw = normalize(value).replace(/\s+/g, "");
  const digits = raw.replace(/[^0-9]/g, "");
  if (!digits) return "";
  if (digits.length < 6 || digits.length > 20) return "";
  return digits;
}

function normalizeBranchCode(value) {
  const raw = normalize(value).replace(/\s+/g, "");
  const digits = raw.replace(/[^0-9]/g, "");
  if (!digits) return "";
  if (digits.length < 4 || digits.length > 10) return "";
  return digits;
}

function sanitizeFilename(value) {
  return normalize(value).replace(/[^\w.\-()\s]/g, "").slice(0, 120);
}

function parseUploadedDocument(raw, fallbackName) {
  if (!raw) return null;

  let mime = "";
  let filename = fallbackName;
  let base64 = "";

  if (typeof raw === "string") {
    const dataUri = raw.trim();
    const match = dataUri.match(/^data:([^;]+);base64,(.+)$/i);
    if (match) {
      mime = normalize(match[1]).toLowerCase();
      base64 = normalize(match[2]);
    } else {
      base64 = normalize(dataUri);
    }
  } else if (typeof raw === "object") {
    mime = normalize(raw.mimeType || raw.mime || raw.contentType).toLowerCase();
    filename = sanitizeFilename(raw.filename || raw.name || fallbackName) || fallbackName;
    base64 = normalize(raw.base64 || raw.content || raw.data);
  }

  if (!base64) return null;

  try {
    const buffer = Buffer.from(base64, "base64");
    if (!buffer?.length) return null;
    if (buffer.length > MAX_ONBOARDING_DOC_BYTES) {
      return { error: `Document must be ${Math.round(MAX_ONBOARDING_DOC_BYTES / (1024 * 1024))}MB or smaller` };
    }
    if (!allowedOnboardingMimeTypes.has(mime)) {
      return { error: "Document must be PDF, JPEG, PNG, or WEBP" };
    }
    return {
      buffer,
      mime,
      filename: sanitizeFilename(filename || fallbackName) || fallbackName,
    };
  } catch (_err) {
    return { error: "Invalid document encoding" };
  }
}

function normalizeDateOfBirth(value) {
  if (value === null) return null;
  const raw = normalize(value);
  if (!raw) return null;

  let year = "";
  let month = "";
  let day = "";
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    [year, month, day] = raw.split("-");
  } else if (/^\d{2}[-/]\d{2}[-/]\d{4}$/.test(raw)) {
    const [d, m, y] = raw.split(/[-/]/);
    year = y;
    month = m;
    day = d;
  } else {
    return null;
  }

  const y = Number.parseInt(year, 10);
  const m = Number.parseInt(month, 10);
  const d = Number.parseInt(day, 10);
  if (!Number.isInteger(y) || !Number.isInteger(m) || !Number.isInteger(d)) return null;
  if (y < 1900 || m < 1 || m > 12 || d < 1 || d > 31) return null;

  const parsed = new Date(Date.UTC(y, m - 1, d));
  if (parsed.getUTCFullYear() !== y || parsed.getUTCMonth() !== m - 1 || parsed.getUTCDate() !== d) return null;

  const today = new Date();
  const todayUtc = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()));
  if (parsed > todayUtc) return null;

  return `${String(y).padStart(4, "0")}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}

function isTruthy(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return ["1", "true", "yes", "on"].includes(normalized);
}

function isUuid(value) {
  const text = normalize(value);
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(text);
}

function csvEscape(value) {
  if (value === null || typeof value === "undefined") return "";
  const text = String(value);
  if (/[",\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

function normalizeWebBaseUrl() {
  const base = String(process.env.PUBLIC_WEB_BASE_URL || process.env.WEBSITE_BASE_URL || "https://churpay.com").trim();
  return base.replace(/\/+$/, "");
}

function normalizeApiBaseUrl() {
  const base = String(process.env.PUBLIC_BASE_URL || "https://api.churpay.com").trim();
  return base.replace(/\/+$/, "");
}

function adminPortalUrl() {
  return `${normalizeApiBaseUrl()}/admin/`;
}

function formatTimestamp(value) {
  if (!value) return "";
  try {
    const d = new Date(value);
    return Number.isNaN(d.getTime()) ? "" : d.toISOString();
  } catch (_err) {
    return "";
  }
}

const JOB_STATUS = new Set(["DRAFT", "PUBLISHED", "CLOSED"]);
const JOB_EMPLOYMENT_TYPES = new Set(["FULL_TIME", "PART_TIME", "CONTRACT", "INTERNSHIP", "VOLUNTEER"]);

function normalizeJobStatus(value, fallback = "DRAFT") {
  const status = normalize(value).toUpperCase();
  if (JOB_STATUS.has(status)) return status;
  return fallback;
}

function normalizeEmploymentType(value, fallback = "FULL_TIME") {
  const type = normalize(value).toUpperCase();
  if (JOB_EMPLOYMENT_TYPES.has(type)) return type;
  return fallback;
}

function parseDateTime(value) {
  const text = normalize(value);
  if (!text) return null;
  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function slugifyJobTitle(value) {
  const base = normalize(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
  return base || "job";
}

function normalizeApplicationUrl(value) {
  const text = normalize(value);
  if (!text) return "";
  try {
    const parsed = new URL(text);
    if (!["http:", "https:"].includes(parsed.protocol)) return "";
    return parsed.toString();
  } catch (_err) {
    return "";
  }
}

async function ensureUniqueJobSlug(desiredSlug, excludeId = null) {
  let candidate = slugifyJobTitle(desiredSlug);
  let suffix = 2;

  // Keep bumping suffix until the slug is available (or belongs to the same row when editing).
  for (;;) {
    const existing = await db.oneOrNone(
      `select id from job_adverts where lower(slug) = lower($1) and ($2::uuid is null or id <> $2::uuid) limit 1`,
      [candidate, excludeId || null]
    );
    if (!existing) return candidate;
    candidate = `${slugifyJobTitle(desiredSlug)}-${suffix}`;
    suffix += 1;
  }
}

async function sendOnboardingStatusEmailApproved({
  to,
  adminFullName,
  churchName,
  joinCode,
  requestId,
  approvedAt,
  temporaryPassword,
}) {
  const email = normalizeEmail(to);
  if (!email) return { ok: false, skipped: true };

  const portalUrl = adminPortalUrl();
  const trackUrl = `${normalizeWebBaseUrl()}/onboarding?requestId=${encodeURIComponent(String(requestId || ""))}`;
  const subject = "Your Churpay church onboarding was approved";

  const text = [
    `Hi ${adminFullName || "there"},`,
    "",
    `Good news: your church onboarding for ${churchName || "your church"} was approved.`,
    joinCode ? `Join code: ${joinCode}` : null,
    approvedAt ? `Approved at: ${formatTimestamp(approvedAt)}` : null,
    "",
    "You can now sign in to the Churpay Admin Portal:",
    portalUrl,
    "",
    temporaryPassword
      ? `Temporary password: ${temporaryPassword}\n\nIf this is a production account, please change it immediately.`
      : "Use the password you chose during onboarding to log in.",
    "",
    "Track this onboarding request:",
    trackUrl,
  ]
    .filter(Boolean)
    .join("\n");

  const html = [
    `<p>Hi ${adminFullName || "there"},</p>`,
    `<p><strong>Good news:</strong> your church onboarding for <strong>${churchName || "your church"}</strong> was approved.</p>`,
    joinCode ? `<p><strong>Join code:</strong> ${joinCode}</p>` : "",
    approvedAt ? `<p><strong>Approved at:</strong> ${formatTimestamp(approvedAt)}</p>` : "",
    `<p>You can now sign in to the Churpay Admin Portal: <a href="${portalUrl}">${portalUrl}</a></p>`,
    temporaryPassword
      ? `<p><strong>Temporary password:</strong> ${temporaryPassword}</p><p>If this is a production account, please change it immediately.</p>`
      : "<p>Use the password you chose during onboarding to log in.</p>",
    `<p>Track this onboarding request: <a href="${trackUrl}">${trackUrl}</a></p>`,
  ].join("");

  const delivery = await sendEmail({ to: email, subject, text, html });
  return { ok: true, delivery, portalUrl, trackUrl };
}

async function sendOnboardingStatusEmailRejected({
  to,
  adminFullName,
  churchName,
  requestedJoinCode,
  requestId,
  rejectedAt,
  verificationNote,
}) {
  const email = normalizeEmail(to);
  if (!email) return { ok: false, skipped: true };

  const trackUrl = `${normalizeWebBaseUrl()}/onboarding?requestId=${encodeURIComponent(String(requestId || ""))}`;
  const subject = "Your Churpay church onboarding was rejected";

  const text = [
    `Hi ${adminFullName || "there"},`,
    "",
    `Your church onboarding for ${churchName || "your church"} was rejected.`,
    requestedJoinCode ? `Requested join code: ${requestedJoinCode}` : null,
    rejectedAt ? `Reviewed at: ${formatTimestamp(rejectedAt)}` : null,
    "",
    verificationNote ? `Reason/comment:\n${verificationNote}` : null,
    "",
    "You can submit a new onboarding request after fixing the issue.",
    "",
    "Track this onboarding request:",
    trackUrl,
  ]
    .filter(Boolean)
    .join("\n");

  const html = [
    `<p>Hi ${adminFullName || "there"},</p>`,
    `<p>Your church onboarding for <strong>${churchName || "your church"}</strong> was rejected.</p>`,
    requestedJoinCode ? `<p><strong>Requested join code:</strong> ${requestedJoinCode}</p>` : "",
    rejectedAt ? `<p><strong>Reviewed at:</strong> ${formatTimestamp(rejectedAt)}</p>` : "",
    verificationNote ? `<p><strong>Reason/comment:</strong></p><pre style="white-space:pre-wrap">${verificationNote}</pre>` : "",
    "<p>You can submit a new onboarding request after fixing the issue.</p>",
    `<p>Track this onboarding request: <a href="${trackUrl}">${trackUrl}</a></p>`,
  ].join("");

  const delivery = await sendEmail({ to: email, subject, text, html });
  return { ok: true, delivery, trackUrl };
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

async function hasColumn(tableName, columnName) {
  const row = await db.one(
    `
    select exists (
      select 1
      from information_schema.columns
      where table_schema = 'public' and table_name = $1 and column_name = $2
    ) as ok
    `,
    [tableName, columnName]
  );
  return !!row.ok;
}

async function hasTable(tableName) {
  const row = await db.one(
    `
    select exists (
      select 1
      from information_schema.tables
      where table_schema = 'public' and table_name = $1
    ) as ok
    `,
    [normalize(tableName)]
  );
  return !!row.ok;
}

async function safeCount(sql, params = []) {
  try {
    const row = await db.one(sql, params);
    return Number(row?.count || 0);
  } catch (err) {
    if (err?.code === "42P01" || err?.code === "42703") return 0;
    throw err;
  }
}

const ACCOUNTANT_CONFIGURABLE_TABS = ["dashboard", "transactions", "statements", "funds", "qr", "members"];
const ACCOUNTANT_DEFAULT_TABS = ["dashboard", "transactions", "statements"];

function normalizeAccountantTabs(value) {
  const list = Array.isArray(value) ? value : [];
  const seen = new Set();
  const out = [];
  for (const raw of list) {
    const key = normalize(raw).toLowerCase();
    if (!key) continue;
    if (!ACCOUNTANT_CONFIGURABLE_TABS.includes(key)) continue;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(key);
  }
  return out;
}

function makeTemporaryPassword() {
  return crypto.randomBytes(24).toString("base64url");
}

async function createOrPromoteAdminMember(
  t,
  { churchId, fullName, phone, email, passwordHash = null, termsAcceptedAt = null, termsVersion = null }
) {
  const normalizedEmail = normalizeEmail(email);
  const normalizedPhone = normalizePhone(phone);
  const normalizedName = normalize(fullName) || "Church Admin";
  if (!normalizedEmail && !normalizedPhone) {
    throw new Error("Admin email or phone is required");
  }

  const hasTermsAcceptedAt = await hasColumn("members", "terms_accepted_at").catch(() => false);
  const hasTermsVersion = await hasColumn("members", "terms_version").catch(() => false);
  const normalizedTermsAcceptedAt = termsAcceptedAt ? new Date(termsAcceptedAt) : null;
  const normalizedTermsVersion =
    typeof termsVersion === "number" && Number.isFinite(termsVersion)
      ? termsVersion
      : Number.isFinite(Number(termsVersion))
        ? Number(termsVersion)
        : null;

  const existing = await t.oneOrNone(
    `
    select id, full_name, phone, email, role, church_id, password_hash
    from members
    where (coalesce($1::text, '') <> '' and lower(coalesce(email, '')) = lower($1::text))
       or (coalesce($2::text, '') <> '' and coalesce(phone, '') = $2::text)
    order by created_at asc
    limit 1
    `,
    [normalizedEmail, normalizedPhone]
  );

  if (existing) {
    const updateSet = [
      `full_name = coalesce(nullif($2, ''), full_name)`,
      `phone = coalesce(nullif($3, ''), phone)`,
      `email = coalesce(nullif($4, ''), email)`,
      `role = 'admin'`,
      `church_id = $5`,
    ];
    const updateParams = [existing.id, normalizedName, normalizedPhone || null, normalizedEmail || null, churchId];
    if (passwordHash) {
      updateSet.push(`password_hash = $6`);
      updateParams.push(passwordHash);
    }
    if (hasTermsAcceptedAt && normalizedTermsAcceptedAt) {
      updateSet.push(`terms_accepted_at = coalesce(terms_accepted_at, $${updateParams.length + 1})`);
      updateParams.push(normalizedTermsAcceptedAt);
    }
    if (hasTermsVersion && normalizedTermsVersion !== null) {
      updateSet.push(`terms_version = coalesce(terms_version, $${updateParams.length + 1})`);
      updateParams.push(normalizedTermsVersion);
    }

    const updated = await t.one(
      `
      update members
      set
        ${updateSet.join(",\n        ")},
        updated_at = now()
      where id = $1
      returning id, full_name as "fullName", phone, email, role, church_id as "churchId"
      `,
      updateParams
    );

    return {
      member: updated,
      created: false,
      temporaryPassword: null,
    };
  }

  const temporaryPassword = passwordHash ? null : makeTemporaryPassword();
  const finalPasswordHash = passwordHash || (await bcrypt.hash(temporaryPassword, 10));
  const insertColumns = ["full_name", "phone", "email", "password_hash", "role", "church_id", "created_at", "updated_at"];
  const insertValues = [
    "$1",
    "nullif($2, '')",
    "nullif($3, '')",
    "$4",
    "'admin'",
    "$5",
    "now()",
    "now()",
  ];
  const insertParams = [normalizedName, normalizedPhone || null, normalizedEmail || null, finalPasswordHash, churchId];

  if (hasTermsAcceptedAt && normalizedTermsAcceptedAt) {
    insertColumns.push("terms_accepted_at");
    insertValues.push(`$${insertParams.length + 1}`);
    insertParams.push(normalizedTermsAcceptedAt);
  }
  if (hasTermsVersion && normalizedTermsVersion !== null) {
    insertColumns.push("terms_version");
    insertValues.push(`$${insertParams.length + 1}`);
    insertParams.push(normalizedTermsVersion);
  }

  const createdMember = await t.one(
    `
    insert into members (${insertColumns.join(", ")})
    values (${insertValues.join(", ")})
    returning id, full_name as "fullName", phone, email, role, church_id as "churchId"
    `,
    insertParams
  );

  return {
    member: createdMember,
    created: true,
    temporaryPassword,
  };
}

function buildTransactionFilter({ churchId, fundId, provider, status, search, from, to }) {
  const where = ["1=1"];
  const params = [];
  let idx = 1;

  if (churchId) {
    where.push(`t.church_id = $${idx}`);
    params.push(churchId);
    idx++;
  }

  if (fundId) {
    where.push(`t.fund_id = $${idx}`);
    params.push(fundId);
    idx++;
  }

  if (provider) {
    where.push(`lower(coalesce(t.provider, '')) = $${idx}`);
    params.push(String(provider).toLowerCase());
    idx++;
  }

  if (status) {
    const normalizedStatus = String(status).toUpperCase();
    where.push(
      `upper(coalesce(pi.status, case when t.provider in ('payfast','simulated','manual') then 'PAID' when t.provider is null then 'PAID' else upper(t.provider) end)) = $${idx}`
    );
    params.push(normalizedStatus);
    idx++;
  }

  if (search) {
    const term = `%${String(search).trim()}%`;
    where.push(
      `(coalesce(t.reference, '') ilike $${idx} or coalesce(pi.payer_name, pi.member_name, '') ilike $${idx} or coalesce(pi.payer_phone, pi.member_phone, '') ilike $${idx} or coalesce(c.name, '') ilike $${idx} or coalesce(f.name, '') ilike $${idx})`
    );
    params.push(term);
    idx++;
  }

  if (from) {
    where.push(`t.created_at >= $${idx}`);
    params.push(from);
    idx++;
  }

  if (to) {
    where.push(`t.created_at <= $${idx}`);
    params.push(to);
    idx++;
  }

  return { where, params, nextParamIndex: idx };
}

router.post("/login", async (req, res) => {
  try {
    const result = authenticateSuperAdmin(req.body?.identifier || req.body?.email, req.body?.password);
    if (!result.ok) return res.status(result.status).json({ error: result.error });

    if (!superTwoFactorEnabled()) {
      return res.json({
        ok: true,
        token: result.token,
        profile: result.profile,
      });
    }

    const destinationEmail = normalizeEmail(result.profile?.email);
    if (!destinationEmail) {
      return res.status(400).json({
        error: "Super admin account needs an email address before two-factor sign-in can be used.",
        code: "TWO_FACTOR_EMAIL_REQUIRED",
      });
    }

    const challenge = await issueLoginTwoFactorChallenge({
      role: "super",
      memberId: null,
      identifier: destinationEmail,
      email: destinationEmail,
      recipientName: result.profile?.fullName || "there",
    });

    return res.json({
      ok: true,
      requiresTwoFactor: true,
      code: "TWO_FACTOR_REQUIRED",
      twoFactor: {
        challengeId: challenge.challengeId,
        channel: challenge.channel,
        emailMasked: challenge.emailMasked,
        expiresAt: challenge.expiresAt,
        role: challenge.role,
      },
    });
  } catch (err) {
    if (String(err?.message || "").toLowerCase().includes("two-factor sign-in upgrade")) {
      return res.status(503).json({ error: err.message });
    }
    console.error("[super/login] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/login/verify-2fa", async (req, res) => {
  try {
    const verification = await verifyLoginTwoFactorChallenge({
      challengeId: req.body?.challengeId,
      role: "super",
      code: req.body?.code,
      token: req.body?.token,
    });
    if (!verification.ok) {
      return res.status(verification.status || 400).json({ error: verification.error, code: verification.code });
    }

    const config = getSuperAdminConfig();
    const challengeEmail = normalizeEmail(verification.challenge?.email);
    if (!config?.email || !challengeEmail || challengeEmail !== config.email) {
      return res.status(401).json({ error: "Invalid credentials" });
    }

    return res.json({
      ok: true,
      token: signSuperToken(config.email),
      profile: { role: "super", email: config.email, fullName: "Super Admin" },
    });
  } catch (err) {
    if (String(err?.message || "").toLowerCase().includes("two-factor sign-in upgrade")) {
      return res.status(503).json({ error: err.message });
    }
    console.error("[super/login/verify-2fa] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/me", requireSuperAdmin, (_req, res) => {
  const config = getSuperAdminConfig();
  return res.json({
    profile: {
      role: "super",
      email: config.email || "super@churpay.com",
      fullName: "Super Admin",
    },
  });
});

router.get("/ping", requireSuperAdmin, (req, res) => {
  return res.json({ ok: true, super: req.superAdmin?.email });
});

router.get(["/dashboard/summary", "/dashboard/overview"], requireSuperAdmin, async (req, res) => {
  try {
    const churchId = typeof req.query.churchId === "string" && req.query.churchId.trim() ? req.query.churchId.trim() : null;
    const from = parseDate(req.query.from, false);
    const to = parseDate(req.query.to, true);

    const churchFilterSql = churchId ? " and t.church_id = $1" : "";
    const churchFilterParams = churchId ? [churchId] : [];

    const totalRow = await db.one(
      `
      select
        coalesce(sum(case when t.created_at >= now() - interval '1 day' then t.amount else 0 end), 0)::numeric(12,2) as today_total,
        coalesce(sum(case when t.created_at >= now() - interval '7 day' then t.amount else 0 end), 0)::numeric(12,2) as week_total,
        coalesce(sum(case when t.created_at >= now() - interval '30 day' then t.amount else 0 end), 0)::numeric(12,2) as month_total,
        coalesce(sum(case when t.created_at >= now() - interval '1 day' then coalesce(t.platform_fee_amount,0) else 0 end), 0)::numeric(12,2) as today_fee_total,
        coalesce(sum(case when t.created_at >= now() - interval '7 day' then coalesce(t.platform_fee_amount,0) else 0 end), 0)::numeric(12,2) as week_fee_total,
        coalesce(sum(case when t.created_at >= now() - interval '30 day' then coalesce(t.platform_fee_amount,0) else 0 end), 0)::numeric(12,2) as month_fee_total,
        coalesce(sum(case when t.created_at >= now() - interval '1 day' then coalesce(t.superadmin_cut_amount,0) else 0 end), 0)::numeric(12,2) as today_superadmin_cut_total,
        coalesce(sum(case when t.created_at >= now() - interval '7 day' then coalesce(t.superadmin_cut_amount,0) else 0 end), 0)::numeric(12,2) as week_superadmin_cut_total,
        coalesce(sum(case when t.created_at >= now() - interval '30 day' then coalesce(t.superadmin_cut_amount,0) else 0 end), 0)::numeric(12,2) as month_superadmin_cut_total,
        coalesce(sum(coalesce(t.amount_gross, t.amount)), 0)::numeric(12,2) as processed_total,
        coalesce(sum(coalesce(t.platform_fee_amount,0)), 0)::numeric(12,2) as fee_total,
        coalesce(sum(coalesce(t.payfast_fee_amount,0)), 0)::numeric(12,2) as payfast_fee_total,
        coalesce(sum(coalesce(t.superadmin_cut_amount,0)), 0)::numeric(12,2) as superadmin_cut_total,
        count(*)::int as total_transactions,
        count(distinct nullif(coalesce(pi.payer_phone, pi.member_phone, pi.payer_name, pi.member_name), ''))::int as total_donors,
        count(*) filter (where upper(coalesce(pi.status, 'PAID')) = 'FAILED')::int as failed_payments
      from transactions t
      left join payment_intents pi on pi.id = t.payment_intent_id
      where 1=1 ${churchFilterSql}
      `,
      churchFilterParams
    );

    const churchesRow = await db.one(
      `
      select count(*)::int as total_churches
      from churches c
      ${churchId ? "where c.id = $1" : ""}
      `,
      churchId ? [churchId] : []
    );

    const fundsRow = await db.one(
      `
      select count(*)::int as active_funds
      from funds f
      where coalesce(f.active, true) = true
      ${churchId ? "and f.church_id = $1" : ""}
      `,
      churchId ? [churchId] : []
    );

    const whereData = [];
    const whereParams = [];
    let idx = 1;
    if (churchId) {
      whereData.push(`t.church_id = $${idx}`);
      whereParams.push(churchId);
      idx++;
    }
    if (from) {
      whereData.push(`t.created_at >= $${idx}`);
      whereParams.push(from);
      idx++;
    }
    if (to) {
      whereData.push(`t.created_at <= $${idx}`);
      whereParams.push(to);
      idx++;
    }

    const recent = await db.manyOrNone(
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
        t.created_at as "createdAt",
        c.id as "churchId",
        c.name as "churchName",
        f.id as "fundId",
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
        ob.email as "onBehalfOfMemberEmail"
      from transactions t
      join churches c on c.id = t.church_id
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
      left join members ob on ob.id = pi.on_behalf_of_member_id
      ${whereData.length ? `where ${whereData.join(" and ")}` : ""}
      order by t.created_at desc
      limit 10
      `,
      whereParams
    );

    const churches = await db.manyOrNone(
      `select id, name, join_code as "joinCode", created_at as "createdAt" from churches order by created_at desc limit 500`
    );

    res.json({
      summary: {
        todayTotal: Number(totalRow.today_total || 0).toFixed(2),
        weekTotal: Number(totalRow.week_total || 0).toFixed(2),
        monthTotal: Number(totalRow.month_total || 0).toFixed(2),
        totalChurches: Number(churchesRow.total_churches || 0),
        activeFunds: Number(fundsRow.active_funds || 0),
        failedPayments: Number(totalRow.failed_payments || 0),
        totalDonors: Number(totalRow.total_donors || 0),
        totalTransactions: Number(totalRow.total_transactions || 0),
        totalProcessed: Number(totalRow.processed_total || 0).toFixed(2),
        totalFeesCollected: Number(totalRow.fee_total || 0).toFixed(2),
        totalPayfastFees: Number(totalRow.payfast_fee_total || 0).toFixed(2),
        totalSuperadminCut: Number(totalRow.superadmin_cut_total || 0).toFixed(2),
        netPlatformRevenue: (Number(totalRow.fee_total || 0) - Number(totalRow.superadmin_cut_total || 0)).toFixed(2),
        todayFeeTotal: Number(totalRow.today_fee_total || 0).toFixed(2),
        weekFeeTotal: Number(totalRow.week_fee_total || 0).toFixed(2),
        monthFeeTotal: Number(totalRow.month_fee_total || 0).toFixed(2),
        todaySuperadminCutTotal: Number(totalRow.today_superadmin_cut_total || 0).toFixed(2),
        weekSuperadminCutTotal: Number(totalRow.week_superadmin_cut_total || 0).toFixed(2),
        monthSuperadminCutTotal: Number(totalRow.month_superadmin_cut_total || 0).toFixed(2),
      },
      recentTransactions: recent,
      churches,
    });
  } catch (err) {
    console.error("[super/dashboard] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/churches", requireSuperAdmin, async (req, res) => {
  try {
    const search = String(req.query.search || "").trim();
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);
    const offset = Math.max(Number(req.query.offset || 0), 0);
    const hasActive = await hasColumn("churches", "active");

    const where = [];
    const params = [];
    let idx = 1;

    if (search) {
      params.push(`%${search}%`);
      where.push(`(coalesce(c.name, '') ilike $${idx} or coalesce(c.join_code, '') ilike $${idx})`);
      idx++;
    }

    const countRow = await db.one(
      `select count(*)::int as count from churches c ${where.length ? `where ${where.join(" and ")}` : ""}`,
      params
    );

    const rows = await db.manyOrNone(
      `
      select
        c.id,
        c.name,
        c.join_code as "joinCode",
        ${hasActive ? "coalesce(c.active, true)" : "true"} as active,
        c.created_at as "createdAt"
      from churches c
      ${where.length ? `where ${where.join(" and ")}` : ""}
      order by c.created_at desc
      limit $${idx} offset $${idx + 1}
      `,
      [...params, limit, offset]
    );

    res.json({
      churches: rows,
      meta: { limit, offset, count: Number(countRow.count || 0), returned: rows.length },
    });
  } catch (err) {
    console.error("[super/churches] list error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/churches", requireSuperAdmin, async (req, res) => {
  try {
    const name = String(req.body?.name || "").trim();
    const requestedJoinCode = String(req.body?.joinCode || "").trim().toUpperCase();
    if (!name) return res.status(400).json({ error: "Church name is required" });

    const joinCode = await ensureUniqueJoinCode({
      db,
      churchName: name,
      desiredJoinCode: requestedJoinCode || null,
    });
    const hasActive = await hasColumn("churches", "active");
    const row = hasActive
      ? await db.one(
          `insert into churches (name, join_code, active) values ($1, $2, true) returning id, name, join_code as "joinCode", coalesce(active, true) as active, created_at as "createdAt"`,
          [name, joinCode]
        )
      : await db.one(
          `insert into churches (name, join_code) values ($1, $2) returning id, name, join_code as "joinCode", true as active, created_at as "createdAt"`,
          [name, joinCode]
        );

    res.status(201).json({ church: row });
  } catch (err) {
    if (err?.code === "23505") return res.status(409).json({ error: "Join code already exists" });
    console.error("[super/churches] create error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.patch("/churches/:churchId", requireSuperAdmin, async (req, res) => {
  try {
    const churchId = String(req.params.churchId || "").trim();
    if (!churchId) return res.status(400).json({ error: "Church ID is required" });
    const hasActive = await hasColumn("churches", "active");

    const updates = [];
    const params = [];
    let idx = 1;

    if (typeof req.body?.name !== "undefined") {
      const name = String(req.body.name || "").trim();
      if (!name) return res.status(400).json({ error: "Church name is required" });
      updates.push(`name = $${idx++}`);
      params.push(name);
    }

    if (typeof req.body?.joinCode !== "undefined") {
      const joinCode = String(req.body.joinCode || "").trim().toUpperCase();
      if (!joinCode) return res.status(400).json({ error: "Join code is required" });
      updates.push(`join_code = $${idx++}`);
      params.push(joinCode);
    }

    if (hasActive && typeof req.body?.active !== "undefined") {
      updates.push(`active = $${idx++}`);
      params.push(!!req.body.active);
    }

    if (!updates.length) return res.status(400).json({ error: "No updates supplied" });

    params.push(churchId);
    const row = await db.oneOrNone(
      `
      update churches
      set ${updates.join(", ")}
      where id = $${idx}
      returning id, name, join_code as "joinCode", ${hasActive ? "coalesce(active, true)" : "true"} as active, created_at as "createdAt"
      `,
      params
    );
    if (!row) return res.status(404).json({ error: "Church not found" });
    res.json({ church: row });
  } catch (err) {
    if (err?.code === "23505") return res.status(409).json({ error: "Join code already exists" });
    console.error("[super/churches] patch error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.delete("/churches/:churchId", requireSuperAdmin, async (req, res) => {
  try {
    const churchId = normalize(req.params.churchId);
    if (!churchId) return res.status(400).json({ error: "Church ID is required" });
    if (!isUuid(churchId)) return res.status(400).json({ error: "Church ID must be a valid UUID" });

    const hasActive = await hasColumn("churches", "active");
    const result = await db.tx(async (t) => {
      const church = await t.oneOrNone(
        `
        select
          id,
          name,
          join_code as "joinCode",
          ${hasActive ? "coalesce(active, true)" : "true"} as active,
          created_at as "createdAt"
        from churches
        where id = $1
        `,
        [churchId]
      );
      if (!church) return { ok: false, error: "not_found" };

      const counts = { transactions: 0, paymentIntents: 0 };

      if (hasActive && church.active) {
        return { ok: false, error: "must_disable_first", church };
      }

      try {
        const row = await t.one(`select count(*)::int as count from transactions where church_id = $1`, [churchId]);
        counts.transactions = Number(row.count || 0);
      } catch (err) {
        // Table may not exist in older schemas.
        if (err?.code !== "42P01") throw err;
      }

      try {
        const row = await t.one(`select count(*)::int as count from payment_intents where church_id = $1`, [churchId]);
        counts.paymentIntents = Number(row.count || 0);
      } catch (err) {
        // Table/column may not exist in older schemas.
        if (err?.code !== "42P01" && err?.code !== "42703") throw err;
      }

      if (counts.transactions > 0 || counts.paymentIntents > 0) {
        return { ok: false, error: "has_financial_records", church, counts };
      }

      await t.none(`delete from churches where id = $1`, [churchId]);
      return { ok: true, church, counts };
    });

    if (!result.ok && result.error === "not_found") return res.status(404).json({ error: "Church not found" });

    if (!result.ok && result.error === "must_disable_first") {
      return res.status(409).json({ error: "Disable this church before deleting it.", church: result.church });
    }

    if (!result.ok && result.error === "has_financial_records") {
      return res.status(409).json({
        error: "Cannot delete this church because it has payment/transaction history. Disable it instead.",
        meta: result.counts,
      });
    }

    if (!result.ok) {
      return res.status(500).json({ error: "Internal server error" });
    }

    return res.json({
      ok: true,
      church: result.church,
      meta: result.counts,
    });
  } catch (err) {
    console.error("[super/churches] delete error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.delete("/churches/:churchId/purge", requireSuperAdmin, async (req, res) => {
  try {
    if (!isTruthy(process.env.DATA_PURGE_ENABLED)) {
      return res.status(403).json({
        error: "Data purge is disabled. Set DATA_PURGE_ENABLED=true temporarily to allow permanent deletion of test data.",
      });
    }

    const churchId = normalize(req.params.churchId);
    if (!churchId) return res.status(400).json({ error: "Church ID is required" });
    if (!isUuid(churchId)) return res.status(400).json({ error: "Church ID must be a valid UUID" });

    const confirm = normalize(req.query.confirm);
    if (!confirm || confirm !== churchId) {
      return res.status(400).json({
        error: "Missing confirmation. Pass ?confirm=<churchId> to permanently purge this church and its data.",
      });
    }

    const hasActive = await hasColumn("churches", "active");

    const result = await db.tx(async (t) => {
      const church = await t.oneOrNone(
        `
        select
          id,
          name,
          join_code as "joinCode",
          ${hasActive ? "coalesce(active, true)" : "true"} as active,
          created_at as "createdAt"
        from churches
        where id = $1
        `,
        [churchId]
      );
      if (!church) return { ok: false, error: "not_found" };

      if (hasActive && church.active) {
        return { ok: false, error: "must_disable_first", church };
      }

      const counts = {
        transactions: 0,
        paymentIntents: 0,
        givingLinks: 0,
        funds: 0,
        members: 0,
        admins: 0,
        onboardingRequests: 0,
      };

      async function safeCount(sql, params) {
        try {
          const row = await t.one(sql, params);
          return Number(row.count || 0);
        } catch (err) {
          if (err?.code === "42P01" || err?.code === "42703") return 0;
          throw err;
        }
      }

      async function safeDelete(sql, params) {
        try {
          const res = await t.query(sql, params);
          return Number(res.rowCount || 0);
        } catch (err) {
          if (err?.code === "42P01" || err?.code === "42703") return 0;
          throw err;
        }
      }

      counts.transactions = await safeCount(`select count(*)::int as count from transactions where church_id = $1`, [
        churchId,
      ]);
      counts.paymentIntents = await safeCount(`select count(*)::int as count from payment_intents where church_id = $1`, [
        churchId,
      ]);
      counts.givingLinks = await safeCount(`select count(*)::int as count from giving_links where church_id = $1`, [churchId]);
      counts.funds = await safeCount(`select count(*)::int as count from funds where church_id = $1`, [churchId]);
      counts.members = await safeCount(`select count(*)::int as count from members where church_id = $1`, [churchId]);
      counts.admins = await safeCount(`select count(*)::int as count from admins where church_id = $1`, [churchId]);

      if (church.joinCode) {
        counts.onboardingRequests = await safeCount(
          `select count(*)::int as count from church_onboarding_requests where approved_church_id = $1 or requested_join_code = $2`,
          [churchId, church.joinCode]
        );
      } else {
        counts.onboardingRequests = await safeCount(
          `select count(*)::int as count from church_onboarding_requests where approved_church_id = $1`,
          [churchId]
        );
      }

      // Delete in dependency-safe order.
      await safeDelete(`delete from transactions where church_id = $1`, [churchId]);
      await safeDelete(`delete from payment_intents where church_id = $1`, [churchId]);
      await safeDelete(`delete from giving_links where church_id = $1`, [churchId]);
      await safeDelete(`delete from funds where church_id = $1`, [churchId]);
      await safeDelete(`delete from admins where church_id = $1`, [churchId]);
      await safeDelete(`delete from members where church_id = $1`, [churchId]);

      if (church.joinCode) {
        await safeDelete(
          `delete from church_onboarding_requests where approved_church_id = $1 or requested_join_code = $2`,
          [churchId, church.joinCode]
        );
      } else {
        await safeDelete(`delete from church_onboarding_requests where approved_church_id = $1`, [churchId]);
      }

      await safeDelete(`delete from churches where id = $1`, [churchId]);
      return { ok: true, church, counts };
    });

    if (!result.ok && result.error === "not_found") return res.status(404).json({ error: "Church not found" });
    if (!result.ok && result.error === "must_disable_first") {
      return res.status(409).json({ error: "Disable this church before purging it.", church: result.church });
    }
    if (!result.ok) return res.status(500).json({ error: "Internal server error" });

    return res.json({ ok: true, church: result.church, meta: result.counts });
  } catch (err) {
    console.error("[super/churches] purge error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/churches/:churchId", requireSuperAdmin, async (req, res) => {
  try {
    const churchId = String(req.params.churchId || "").trim();
    if (!churchId) return res.status(400).json({ error: "Church ID is required" });
    const hasActive = await hasColumn("churches", "active");
    const hasPortalSettings = await hasColumn("churches", "admin_portal_settings").catch(() => false);
    const hasChurchBankAccounts = await hasTable("church_bank_accounts");

    const church = await db.oneOrNone(
      `
      select
        c.id,
        c.name,
        c.join_code as "joinCode",
        ${hasActive ? "coalesce(c.active, true)" : "true"} as active,
        ${hasPortalSettings ? "coalesce(c.admin_portal_settings, '{}'::jsonb)" : "'{}'::jsonb"} as "adminPortalSettings",
        c.created_at as "createdAt"
      from churches c
      where c.id = $1
      `,
      [churchId]
    );
    if (!church) return res.status(404).json({ error: "Church not found" });

    const funds = await db.manyOrNone(
      `
      select
        f.id,
        f.code,
        f.name,
        coalesce(f.active, true) as active,
        f.created_at as "createdAt"
      from funds f
      where f.church_id = $1
      order by f.created_at desc
      limit 200
      `,
      [churchId]
    );

    const transactions = await db.manyOrNone(
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
        t.provider_payment_id as "providerPaymentId",
        coalesce(pi.status, case when t.provider in ('payfast','simulated','manual') then 'PAID' when t.provider is null then 'PAID' else upper(t.provider) end) as status,
        t.created_at as "createdAt",
        f.id as "fundId",
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
        ob.email as "onBehalfOfMemberEmail"
      from transactions t
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
      left join members ob on ob.id = pi.on_behalf_of_member_id
      where t.church_id = $1
      order by t.created_at desc
      limit 50
      `,
      [churchId]
    );

    const admins = await db.manyOrNone(
      `
      select
        m.id,
        m.full_name as "fullName",
        m.phone,
        m.email,
        m.role,
        m.created_at as "createdAt"
      from members m
      where m.church_id = $1 and lower(m.role) in ('admin', 'accountant')
      order by m.created_at desc
      `,
      [churchId]
    );

    const bankAccounts = hasChurchBankAccounts
      ? await db.manyOrNone(
          `
          select
            id,
            bank_name as "bankName",
            account_name as "accountName",
            account_number as "accountNumber",
            branch_code as "branchCode",
            account_type as "accountType",
            coalesce(is_primary, false) as "isPrimary",
            created_at as "createdAt",
            updated_at as "updatedAt"
          from church_bank_accounts
          where church_id = $1
          order by is_primary desc, created_at asc
          `,
          [churchId]
        )
      : [];

    res.json({ church, funds, transactions, admins, bankAccounts });
  } catch (err) {
    console.error("[super/church] detail error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.put("/churches/:churchId/bank-accounts", requireSuperAdmin, async (req, res) => {
  try {
    const churchId = normalize(req.params.churchId);
    if (!isUuid(churchId)) return res.status(400).json({ error: "Invalid churchId" });

    const hasChurchBankAccounts = await hasTable("church_bank_accounts");
    if (!hasChurchBankAccounts) {
      return res.status(503).json({ error: "Bank accounts upgrade in progress. Run migrations and retry." });
    }

    const inputAccounts = Array.isArray(req.body?.bankAccounts) ? req.body.bankAccounts : [];
    const normalized = inputAccounts
      .map((row) => ({
        bankName: normalize(row?.bankName),
        accountName: normalize(row?.accountName),
        accountNumber: normalizeAccountNumber(row?.accountNumber),
        branchCode: normalizeBranchCode(row?.branchCode),
        accountType: normalize(row?.accountType),
        isPrimary: !!row?.isPrimary,
      }))
      .filter((row) => row.bankName || row.accountName || row.accountNumber || row.branchCode || row.accountType);

    if (!normalized.length) return res.status(400).json({ error: "At least one bank account is required" });
    if (normalized.length > 5) return res.status(400).json({ error: "A maximum of 5 bank accounts is supported" });

    for (const acct of normalized) {
      if (!acct.bankName || !acct.accountName || !acct.accountNumber) {
        return res.status(400).json({ error: "Each bank account must include bankName, accountName, and accountNumber" });
      }
    }

    const primaryCount = normalized.reduce((acc, row) => acc + (row.isPrimary ? 1 : 0), 0);
    if (primaryCount > 1) return res.status(400).json({ error: "Only one primary bank account is allowed" });
    if (primaryCount === 0) normalized[0].isPrimary = true;

    const result = await db.tx(async (t) => {
      const exists = await t.oneOrNone("select id from churches where id=$1", [churchId]);
      if (!exists) return { notFound: true };

      await t.none("delete from church_bank_accounts where church_id=$1", [churchId]);
      for (const acct of normalized) {
        await t.none(
          `
          insert into church_bank_accounts (
            church_id,
            bank_name,
            account_name,
            account_number,
            branch_code,
            account_type,
            is_primary,
            created_at,
            updated_at
          ) values ($1,$2,$3,$4,nullif($5,''),nullif($6,''),$7,now(),now())
          `,
          [
            churchId,
            acct.bankName,
            acct.accountName,
            acct.accountNumber,
            acct.branchCode || null,
            acct.accountType || null,
            !!acct.isPrimary,
          ]
        );
      }

      const rows = await t.manyOrNone(
        `
        select
          id,
          bank_name as "bankName",
          account_name as "accountName",
          account_number as "accountNumber",
          branch_code as "branchCode",
          account_type as "accountType",
          coalesce(is_primary, false) as "isPrimary",
          created_at as "createdAt",
          updated_at as "updatedAt"
        from church_bank_accounts
        where church_id = $1
        order by is_primary desc, created_at asc
        `,
        [churchId]
      );

      return { bankAccounts: rows };
    });

    if (result?.notFound) return res.status(404).json({ error: "Church not found" });
    return res.json({ ok: true, churchId, bankAccounts: result.bankAccounts || [] });
  } catch (err) {
    console.error("[super/churches/bank-accounts] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.patch("/churches/:churchId/admin-portal-settings", requireSuperAdmin, async (req, res) => {
  try {
    const churchId = normalize(req.params.churchId);
    if (!isUuid(churchId)) return res.status(400).json({ error: "Invalid churchId" });

    const accountantTabs = normalizeAccountantTabs(req.body?.accountantTabs || []);
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

    return res.json({ ok: true, churchId, settings: { accountantTabs } });
  } catch (err) {
    console.error("[super/churches/admin-portal-settings] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/church-onboarding", requireSuperAdmin, async (req, res) => {
  try {
    const status = normalize(req.query.status).toLowerCase();
    const search = normalize(req.query.search);
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const where = ["1=1"];
    const params = [];
    let idx = 1;
    const hasOnboardingEmailVerification = await hasColumn("church_onboarding_requests", "admin_email_verified");
    const hasOnboardingBankAccounts = await hasTable("church_onboarding_bank_accounts");

    if (status && ["pending", "approved", "rejected"].includes(status)) {
      where.push(`r.verification_status = $${idx}`);
      params.push(status);
      idx++;
    }

    if (search) {
      where.push(
        `(coalesce(r.church_name, '') ilike $${idx} or coalesce(r.admin_full_name, '') ilike $${idx} or coalesce(r.admin_email, '') ilike $${idx} or coalesce(r.admin_phone, '') ilike $${idx} or coalesce(r.requested_join_code, '') ilike $${idx})`
      );
      params.push(`%${search}%`);
      idx++;
    }

    const countRow = await db.one(
      `select count(*)::int as count from church_onboarding_requests r where ${where.join(" and ")}`,
      params
    );

    const rows = await db.manyOrNone(
      `
      select
        r.id,
        r.church_name as "churchName",
        r.requested_join_code as "requestedJoinCode",
        r.admin_full_name as "adminFullName",
        r.admin_phone as "adminPhone",
        r.admin_email as "adminEmail",
        ${hasOnboardingEmailVerification ? 'coalesce(r.admin_email_verified, false) as "adminEmailVerified",' : 'true as "adminEmailVerified",'}
        ${hasOnboardingEmailVerification ? 'r.admin_email_verified_at as "adminEmailVerifiedAt",' : 'null::timestamptz as "adminEmailVerifiedAt",'}
        r.verification_status as "verificationStatus",
        r.verification_note as "verificationNote",
        r.verified_by as "verifiedBy",
        r.verified_at as "verifiedAt",
        r.approved_church_id as "approvedChurchId",
        r.approved_admin_member_id as "approvedAdminMemberId",
        r.cipc_filename as "cipcFilename",
        r.cipc_mime as "cipcMime",
        octet_length(r.cipc_document) as "cipcBytes",
        r.bank_confirmation_filename as "bankConfirmationFilename",
        r.bank_confirmation_mime as "bankConfirmationMime",
        octet_length(r.bank_confirmation_document) as "bankConfirmationBytes",
        ${
          hasOnboardingBankAccounts
            ? `(select count(*)::int from church_onboarding_bank_accounts ba where ba.request_id = r.id) as "bankAccountsCount",
        (select ba.bank_name from church_onboarding_bank_accounts ba where ba.request_id = r.id and ba.is_primary = true order by ba.created_at asc limit 1) as "primaryBankName",
        (select right(ba.account_number, 4) from church_onboarding_bank_accounts ba where ba.request_id = r.id and ba.is_primary = true order by ba.created_at asc limit 1) as "primaryAccountLast4",`
            : `0 as "bankAccountsCount",
        null::text as "primaryBankName",
        null::text as "primaryAccountLast4",`
        }
        r.created_at as "createdAt",
        r.updated_at as "updatedAt"
      from church_onboarding_requests r
      where ${where.join(" and ")}
      order by r.created_at desc
      limit $${idx} offset $${idx + 1}
      `,
      [...params, limit, offset]
    );

    return res.json({
      requests: rows,
      meta: {
        limit,
        offset,
        count: Number(countRow.count || 0),
        returned: rows.length,
      },
    });
  } catch (err) {
    console.error("[super/church-onboarding] list error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/church-onboarding/:requestId", requireSuperAdmin, async (req, res) => {
  try {
    const requestId = normalize(req.params.requestId);
    if (!requestId) return res.status(400).json({ error: "requestId is required" });
    const hasOnboardingEmailVerification = await hasColumn("church_onboarding_requests", "admin_email_verified");
    const hasOnboardingBankAccounts = await hasTable("church_onboarding_bank_accounts");

    const row = await db.oneOrNone(
      `
      select
        r.id,
        r.church_name as "churchName",
        r.requested_join_code as "requestedJoinCode",
        r.admin_full_name as "adminFullName",
        r.admin_phone as "adminPhone",
        r.admin_email as "adminEmail",
        ${hasOnboardingEmailVerification ? 'coalesce(r.admin_email_verified, false) as "adminEmailVerified",' : 'true as "adminEmailVerified",'}
        ${hasOnboardingEmailVerification ? 'r.admin_email_verified_at as "adminEmailVerifiedAt",' : 'null::timestamptz as "adminEmailVerifiedAt",'}
        r.verification_status as "verificationStatus",
        r.verification_note as "verificationNote",
        r.verified_by as "verifiedBy",
        r.verified_at as "verifiedAt",
        r.approved_church_id as "approvedChurchId",
        r.approved_admin_member_id as "approvedAdminMemberId",
        r.created_at as "createdAt",
        r.updated_at as "updatedAt",
        r.cipc_filename as "cipcFilename",
        r.cipc_mime as "cipcMime",
        octet_length(r.cipc_document) as "cipcBytes",
        r.bank_confirmation_filename as "bankConfirmationFilename",
        r.bank_confirmation_mime as "bankConfirmationMime",
        octet_length(r.bank_confirmation_document) as "bankConfirmationBytes"
      from church_onboarding_requests r
      where r.id = $1
      `,
      [requestId]
    );

    if (!row) return res.status(404).json({ error: "Onboarding request not found" });

    const bankAccounts = hasOnboardingBankAccounts
      ? await db.manyOrNone(
          `
          select
            id,
            bank_name as "bankName",
            account_name as "accountName",
            account_number as "accountNumber",
            branch_code as "branchCode",
            account_type as "accountType",
            coalesce(is_primary, false) as "isPrimary",
            created_at as "createdAt",
            updated_at as "updatedAt"
          from church_onboarding_bank_accounts
          where request_id = $1
          order by is_primary desc, created_at asc
          `,
          [requestId]
        )
      : [];

    return res.json({ request: { ...row, bankAccounts } });
  } catch (err) {
    console.error("[super/church-onboarding] detail error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/church-onboarding/:requestId/documents/:documentType", requireSuperAdmin, async (req, res) => {
  try {
    const requestId = normalize(req.params.requestId);
    const documentType = normalize(req.params.documentType).toLowerCase();
    if (!requestId) return res.status(400).json({ error: "requestId is required" });
    if (!["cipc", "bank"].includes(documentType)) {
      return res.status(400).json({ error: "documentType must be one of: cipc, bank" });
    }

    const selectedColumn = documentType === "cipc" ? "cipc_document" : "bank_confirmation_document";
    const selectedFileName = documentType === "cipc" ? "cipc_filename" : "bank_confirmation_filename";
    const selectedMime = documentType === "cipc" ? "cipc_mime" : "bank_confirmation_mime";
    const row = await db.oneOrNone(
      `
      select ${selectedColumn} as document, ${selectedFileName} as filename, ${selectedMime} as mime
      from church_onboarding_requests
      where id = $1
      `,
      [requestId]
    );

    if (!row || !row.document) return res.status(404).json({ error: "Document not found" });
    const inlineRequested = ["1", "true", "yes"].includes(String(req.query.inline || "").toLowerCase());
    const safeFilename = String(row.filename || `${documentType}-document`).replace(/"/g, "");
    res.setHeader("Content-Type", row.mime || "application/octet-stream");
    res.setHeader("Content-Disposition", `${inlineRequested ? "inline" : "attachment"}; filename="${safeFilename}"`);
    return res.status(200).send(row.document);
  } catch (err) {
    console.error("[super/church-onboarding] document error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.put("/church-onboarding/:requestId/documents/:documentType", requireSuperAdmin, async (req, res) => {
  try {
    const requestId = normalize(req.params.requestId);
    const documentType = normalize(req.params.documentType).toLowerCase();
    if (!requestId) return res.status(400).json({ error: "requestId is required" });
    if (!isUuid(requestId)) return res.status(400).json({ error: "requestId must be a valid UUID" });
    if (!["cipc", "bank"].includes(documentType)) {
      return res.status(400).json({ error: "documentType must be one of: cipc, bank" });
    }

    const fallbackName = documentType === "cipc" ? "cipc-document" : "bank-confirmation";
    const doc = parseUploadedDocument(req.body?.document || req.body, fallbackName);
    if (!doc) return res.status(400).json({ error: "Document payload is required" });
    if (doc?.error) return res.status(400).json({ error: doc.error });

    const result = await db.oneOrNone(
      `
      update church_onboarding_requests
      set
        ${
          documentType === "cipc"
            ? "cipc_document=$2, cipc_filename=$3, cipc_mime=$4"
            : "bank_confirmation_document=$2, bank_confirmation_filename=$3, bank_confirmation_mime=$4"
        },
        updated_at=now()
      where id=$1
      returning id
      `,
      [requestId, doc.buffer, doc.filename, doc.mime]
    );
    if (!result) return res.status(404).json({ error: "Onboarding request not found" });

    return res.json({
      ok: true,
      requestId,
      documentType,
      filename: doc.filename,
      mime: doc.mime,
      bytes: doc.buffer.length,
    });
  } catch (err) {
    console.error("[super/church-onboarding] document replace error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.patch("/church-onboarding/:requestId", requireSuperAdmin, async (req, res) => {
  try {
    const requestId = normalize(req.params.requestId);
    if (!requestId) return res.status(400).json({ error: "requestId is required" });
    if (!isUuid(requestId)) return res.status(400).json({ error: "requestId must be a valid UUID" });

    const hasOnboardingEmailVerification = await hasColumn("church_onboarding_requests", "admin_email_verified").catch(
      () => false
    );
    const hasOnboardingBankAccounts = await hasTable("church_onboarding_bank_accounts");

    const rawBankAccounts = req.body?.bankAccounts;
    const shouldUpdateBankAccounts = Array.isArray(rawBankAccounts);

    const result = await db.tx(async (t) => {
      const current = await t.oneOrNone(
        `
        select
          id,
          church_name,
          requested_join_code,
          admin_full_name,
          admin_phone,
          admin_email,
          ${hasOnboardingEmailVerification ? "admin_email_verified" : "null::boolean as admin_email_verified"}
        from church_onboarding_requests
        where id = $1
        for update
        `,
        [requestId]
      );
      if (!current) return { notFound: true };

      const updates = [];
      const params = [];
      let idx = 2;

      if (typeof req.body?.churchName !== "undefined") {
        const value = normalize(req.body.churchName);
        if (!value) throw new Error("churchName is required");
        updates.push(`church_name = $${idx++}`);
        params.push(value);
      }

      if (typeof req.body?.requestedJoinCode !== "undefined") {
        const value = normalize(req.body.requestedJoinCode).toUpperCase();
        if (!value) throw new Error("requestedJoinCode is required");
        updates.push(`requested_join_code = $${idx++}`);
        params.push(value);
      }

      if (typeof req.body?.adminFullName !== "undefined") {
        const value = normalize(req.body.adminFullName);
        if (!value) throw new Error("adminFullName is required");
        updates.push(`admin_full_name = $${idx++}`);
        params.push(value);
      }

      if (typeof req.body?.adminPhone !== "undefined") {
        const value = normalizePhone(req.body.adminPhone);
        if (!value) throw new Error("adminPhone must be a valid phone number");
        updates.push(`admin_phone = $${idx++}`);
        params.push(value);
      }

      let emailChanged = false;
      if (typeof req.body?.adminEmail !== "undefined") {
        const value = normalizeEmail(req.body.adminEmail);
        if (!value) throw new Error("adminEmail must be a valid email");
        emailChanged = value.toLowerCase() !== String(current.admin_email || "").toLowerCase();
        updates.push(`admin_email = $${idx++}`);
        params.push(value);
      }

      if (emailChanged && hasOnboardingEmailVerification) {
        updates.push(`admin_email_verified = false`);
        updates.push(`admin_email_verified_at = null`);
        updates.push(`admin_email_verification_token_hash = null`);
        updates.push(`admin_email_verification_code_hash = null`);
        updates.push(`admin_email_verification_expires_at = null`);
        updates.push(`admin_email_verification_sent_at = null`);
        updates.push(`admin_email_verification_attempts = 0`);
      }

      let updatedRow = null;
      if (updates.length) {
        updatedRow = await t.one(
          `
          update church_onboarding_requests
          set
            ${updates.join(",\n            ")},
            updated_at = now()
          where id = $1
          returning
            id,
            church_name as "churchName",
            requested_join_code as "requestedJoinCode",
            admin_full_name as "adminFullName",
            admin_phone as "adminPhone",
            admin_email as "adminEmail",
            ${hasOnboardingEmailVerification ? 'coalesce(admin_email_verified, false) as "adminEmailVerified",' : 'true as "adminEmailVerified",'}
            ${hasOnboardingEmailVerification ? 'admin_email_verified_at as "adminEmailVerifiedAt",' : 'null::timestamptz as "adminEmailVerifiedAt",'}
            verification_status as "verificationStatus",
            verification_note as "verificationNote",
            verified_by as "verifiedBy",
            verified_at as "verifiedAt",
            approved_church_id as "approvedChurchId",
            approved_admin_member_id as "approvedAdminMemberId",
            created_at as "createdAt",
            updated_at as "updatedAt",
            cipc_filename as "cipcFilename",
            cipc_mime as "cipcMime",
            octet_length(cipc_document) as "cipcBytes",
            bank_confirmation_filename as "bankConfirmationFilename",
            bank_confirmation_mime as "bankConfirmationMime",
            octet_length(bank_confirmation_document) as "bankConfirmationBytes"
          `,
          [requestId, ...params]
        );
      } else {
        updatedRow = await t.one(
          `
          select
            r.id,
            r.church_name as "churchName",
            r.requested_join_code as "requestedJoinCode",
            r.admin_full_name as "adminFullName",
            r.admin_phone as "adminPhone",
            r.admin_email as "adminEmail",
            ${hasOnboardingEmailVerification ? 'coalesce(r.admin_email_verified, false) as "adminEmailVerified",' : 'true as "adminEmailVerified",'}
            ${hasOnboardingEmailVerification ? 'r.admin_email_verified_at as "adminEmailVerifiedAt",' : 'null::timestamptz as "adminEmailVerifiedAt",'}
            r.verification_status as "verificationStatus",
            r.verification_note as "verificationNote",
            r.verified_by as "verifiedBy",
            r.verified_at as "verifiedAt",
            r.approved_church_id as "approvedChurchId",
            r.approved_admin_member_id as "approvedAdminMemberId",
            r.created_at as "createdAt",
            r.updated_at as "updatedAt",
            r.cipc_filename as "cipcFilename",
            r.cipc_mime as "cipcMime",
            octet_length(r.cipc_document) as "cipcBytes",
            r.bank_confirmation_filename as "bankConfirmationFilename",
            r.bank_confirmation_mime as "bankConfirmationMime",
            octet_length(r.bank_confirmation_document) as "bankConfirmationBytes"
          from church_onboarding_requests r
          where r.id = $1
          `,
          [requestId]
        );
      }

      if (shouldUpdateBankAccounts) {
        if (!hasOnboardingBankAccounts) {
          throw new Error("bankAccounts upgrade in progress. Run migrations and retry.");
        }

        const inputAccounts = Array.isArray(rawBankAccounts) ? rawBankAccounts : [];
        const normalized = inputAccounts
          .map((row) => ({
            bankName: normalize(row?.bankName),
            accountName: normalize(row?.accountName),
            accountNumber: normalizeAccountNumber(row?.accountNumber),
            branchCode: normalizeBranchCode(row?.branchCode),
            accountType: normalize(row?.accountType),
            isPrimary: !!row?.isPrimary,
          }))
          .filter((row) => row.bankName || row.accountName || row.accountNumber || row.branchCode || row.accountType);

        if (!normalized.length) throw new Error("At least one bank account is required");
        if (normalized.length > 5) throw new Error("A maximum of 5 bank accounts is supported");

        for (const acct of normalized) {
          if (!acct.bankName || !acct.accountName || !acct.accountNumber) {
            throw new Error("Each bank account must include bankName, accountName, and accountNumber");
          }
        }

        const primaryCount = normalized.reduce((acc, row) => acc + (row.isPrimary ? 1 : 0), 0);
        if (primaryCount > 1) throw new Error("Only one primary bank account is allowed");
        if (primaryCount === 0) normalized[0].isPrimary = true;

        await t.none(`delete from church_onboarding_bank_accounts where request_id=$1`, [requestId]);
        for (const acct of normalized) {
          await t.none(
            `
            insert into church_onboarding_bank_accounts (
              request_id,
              bank_name,
              account_name,
              account_number,
              branch_code,
              account_type,
              is_primary,
              created_at,
              updated_at
            ) values ($1,$2,$3,$4,nullif($5,''),nullif($6,''),$7,now(),now())
            `,
            [
              requestId,
              acct.bankName,
              acct.accountName,
              acct.accountNumber,
              acct.branchCode || null,
              acct.accountType || null,
              !!acct.isPrimary,
            ]
          );
        }
      }

      const bankAccounts = hasOnboardingBankAccounts
        ? await t.manyOrNone(
            `
            select
              id,
              bank_name as "bankName",
              account_name as "accountName",
              account_number as "accountNumber",
              branch_code as "branchCode",
              account_type as "accountType",
              coalesce(is_primary, false) as "isPrimary",
              created_at as "createdAt",
              updated_at as "updatedAt"
            from church_onboarding_bank_accounts
            where request_id = $1
            order by is_primary desc, created_at asc
            `,
            [requestId]
          )
        : [];

      return { request: { ...updatedRow, bankAccounts } };
    });

    if (result?.notFound) return res.status(404).json({ error: "Onboarding request not found" });
    return res.json(result);
  } catch (err) {
    const message = String(err?.message || "");
    if (message.toLowerCase().includes("required") || message.toLowerCase().includes("must")) {
      return res.status(400).json({ error: message });
    }
    console.error("[super/church-onboarding] patch error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/church-onboarding/:requestId/approve", requireSuperAdmin, async (req, res) => {
  try {
    const requestId = normalize(req.params.requestId);
    if (!requestId) return res.status(400).json({ error: "requestId is required" });

    const verificationNote = normalize(req.body?.verificationNote);
    const overrideChurchName = normalize(req.body?.churchName);
    const overrideJoinCode = normalize(req.body?.joinCode).toUpperCase();
    const overrideAdminFullName = normalize(req.body?.adminFullName);
    const overrideAdminPhone = normalizePhone(req.body?.adminPhone);
    const overrideAdminEmail = normalizeEmail(req.body?.adminEmail);
    const hasChurchActive = await hasColumn("churches", "active");
    const hasOnboardingPasswordHash = await hasColumn("church_onboarding_requests", "admin_password_hash");
    const hasOnboardingEmailVerification = await hasColumn("church_onboarding_requests", "admin_email_verified");
    const hasOnboardingTerms = await hasColumn("church_onboarding_requests", "terms_version").catch(() => false);
    const hasChurchTerms = await hasColumn("churches", "terms_version").catch(() => false);
    const hasOnboardingCookie = await hasColumn("church_onboarding_requests", "cookie_consent_version").catch(
      () => false
    );
    const hasChurchCookie = await hasColumn("churches", "cookie_consent_version").catch(() => false);
    const hasOnboardingBankAccounts = await hasTable("church_onboarding_bank_accounts");
    const hasChurchBankAccounts = await hasTable("church_bank_accounts");

    const result = await db.tx(async (t) => {
      const request = await t.oneOrNone(
        `
        select
          r.id,
          r.church_name,
          r.requested_join_code,
          r.admin_full_name,
          r.admin_phone,
          r.admin_email,
          ${hasOnboardingPasswordHash ? "r.admin_password_hash" : "null::text as admin_password_hash"},
          ${hasOnboardingEmailVerification ? "coalesce(r.admin_email_verified, false) as admin_email_verified" : "true as admin_email_verified"},
          ${hasOnboardingTerms ? "r.terms_accepted_at, r.terms_version," : "null::timestamptz as terms_accepted_at, null::int as terms_version,"}
          ${
            hasOnboardingCookie
              ? "r.cookie_consent_at, r.cookie_consent_version,"
              : "null::timestamptz as cookie_consent_at, null::int as cookie_consent_version,"
          }
          r.verification_status,
          r.approved_church_id,
          r.approved_admin_member_id
        from church_onboarding_requests r
        where r.id = $1
        for update
        `,
        [requestId]
      );
      if (!request) return { notFound: true };
      if (hasOnboardingEmailVerification && !request.admin_email_verified) {
        return { verificationPending: true };
      }
      const originalStatus = String(request.verification_status || "").toLowerCase();

      const churchName = overrideChurchName || normalize(request.church_name);
      if (!churchName) throw new Error("Church name is required to approve");

      let churchId = request.approved_church_id || null;
      let church = null;

      if (churchId) {
        church = await t.oneOrNone(
          `select id, name, join_code as "joinCode", ${hasChurchActive ? "coalesce(active, true)" : "true"} as active from churches where id = $1`,
          [churchId]
        );
      }

      if (!church) {
        const joinCode = await ensureUniqueJoinCode({
          db: t,
          churchName,
          desiredJoinCode: overrideJoinCode || request.requested_join_code || null,
        });
        church = hasChurchActive
          ? await t.one(
              `insert into churches (name, join_code, active) values ($1, $2, true) returning id, name, join_code as "joinCode", coalesce(active, true) as active`,
              [churchName, joinCode]
            )
          : await t.one(
              `insert into churches (name, join_code) values ($1, $2) returning id, name, join_code as "joinCode", true as active`,
              [churchName, joinCode]
            );
        churchId = church.id;
      }

      if (hasChurchTerms && hasOnboardingTerms && (request.terms_version || request.terms_accepted_at)) {
        await t.none(
          `
          update churches
          set
            terms_accepted_at = coalesce(terms_accepted_at, $2),
            terms_version = coalesce(terms_version, $3),
            updated_at = now()
          where id = $1
          `,
          [churchId, request.terms_accepted_at || null, request.terms_version || null]
        );
      }

      if (hasChurchCookie && hasOnboardingCookie && (request.cookie_consent_version || request.cookie_consent_at)) {
        await t.none(
          `
          update churches
          set
            cookie_consent_at = coalesce(cookie_consent_at, $2),
            cookie_consent_version = coalesce(cookie_consent_version, $3),
            updated_at = now()
          where id = $1
          `,
          [churchId, request.cookie_consent_at || null, request.cookie_consent_version || null]
        );
      }

      const adminFullName = overrideAdminFullName || normalize(request.admin_full_name);
      const adminPhone = overrideAdminPhone || normalizePhone(request.admin_phone);
      const adminEmail = overrideAdminEmail || normalizeEmail(request.admin_email);

      if (hasOnboardingBankAccounts && hasChurchBankAccounts) {
        const accounts = await t.manyOrNone(
          `
          select
            bank_name as bank_name,
            account_name as account_name,
            account_number as account_number,
            branch_code as branch_code,
            account_type as account_type,
            coalesce(is_primary, false) as is_primary
          from church_onboarding_bank_accounts
          where request_id = $1
          order by is_primary desc, created_at asc
          `,
          [requestId]
        );

        if (accounts.length) {
          // On approval, treat onboarding-provided bank accounts as the source of truth.
          await t.none(`delete from church_bank_accounts where church_id = $1`, [churchId]);
          for (const account of accounts) {
            await t.none(
              `
              insert into church_bank_accounts (
                church_id,
                bank_name,
                account_name,
                account_number,
                branch_code,
                account_type,
                is_primary,
                created_at,
                updated_at
              ) values (
                $1, $2, $3, $4, nullif($5, ''), nullif($6, ''), $7, now(), now()
              )
              `,
              [
                churchId,
                normalize(account.bank_name),
                normalize(account.account_name),
                normalizeAccountNumber(account.account_number),
                normalizeBranchCode(account.branch_code),
                normalize(account.account_type),
                !!account.is_primary,
              ]
            );
          }
        }
      }

      const adminResult = await createOrPromoteAdminMember(t, {
        churchId,
        fullName: adminFullName,
        phone: adminPhone,
        email: adminEmail,
        passwordHash: request.admin_password_hash || null,
        termsAcceptedAt: request.terms_accepted_at || null,
        termsVersion: request.terms_version || null,
      });

      const updatedRequest = await t.one(
        `
        update church_onboarding_requests
        set
          verification_status = 'approved',
          verification_note = nullif($2, ''),
          verified_by = $3,
          verified_at = now(),
          approved_church_id = $4,
          approved_admin_member_id = $5,
          updated_at = now()
        where id = $1
        returning
          id,
          verification_status as "verificationStatus",
          verification_note as "verificationNote",
          verified_by as "verifiedBy",
          verified_at as "verifiedAt",
          approved_church_id as "approvedChurchId",
          approved_admin_member_id as "approvedAdminMemberId",
          updated_at as "updatedAt"
        `,
        [requestId, verificationNote || null, req.superAdmin?.email || "super-admin", churchId, adminResult.member.id]
      );

      return {
        request: updatedRequest,
        church,
        admin: {
          ...adminResult.member,
          created: adminResult.created,
          temporaryPassword: adminResult.temporaryPassword,
        },
        meta: {
          originalStatus,
          adminEmail: adminEmail || null,
          adminFullName: adminFullName || null,
          requestedJoinCode: request.requested_join_code || null,
        },
      };
    });

    if (result?.notFound) return res.status(404).json({ error: "Onboarding request not found" });
    if (result?.verificationPending) {
      return res.status(409).json({ error: "Admin email must be verified before onboarding approval" });
    }

    let email = null;
    if (result?.meta?.originalStatus && result.meta.originalStatus !== "approved") {
      try {
        email = await sendOnboardingStatusEmailApproved({
          to: result.meta.adminEmail,
          adminFullName: result.meta.adminFullName,
          churchName: result.church?.name,
          joinCode: result.church?.joinCode,
          requestId,
          approvedAt: result.request?.verifiedAt,
          temporaryPassword: result.admin?.temporaryPassword || null,
        });
      } catch (err) {
        console.error("[super/church-onboarding] approved email failed", err?.message || err, err?.stack);
        email = { ok: false, error: "failed" };
      }
    }

    return res.json({ ...result, email });
  } catch (err) {
    if (String(err?.message || "").toLowerCase().includes("required")) {
      return res.status(400).json({ error: err.message });
    }
    console.error("[super/church-onboarding] approve error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/church-onboarding/:requestId/reject", requireSuperAdmin, async (req, res) => {
  try {
    const requestId = normalize(req.params.requestId);
    if (!requestId) return res.status(400).json({ error: "requestId is required" });
    const verificationNote = normalize(req.body?.verificationNote || req.body?.reason);
    if (!verificationNote) return res.status(400).json({ error: "verificationNote is required when rejecting" });

    const existing = await db.oneOrNone(
      `
      select
        id,
        church_name,
        requested_join_code,
        admin_full_name,
        admin_email,
        verification_status
      from church_onboarding_requests
      where id = $1
      `,
      [requestId]
    );
    if (!existing) return res.status(404).json({ error: "Onboarding request not found" });
    const originalStatus = String(existing.verification_status || "").toLowerCase();

    const row = await db.oneOrNone(
      `
      update church_onboarding_requests
      set
        verification_status = 'rejected',
        verification_note = $2,
        verified_by = $3,
        verified_at = now(),
        updated_at = now()
      where id = $1
      returning
        id,
        verification_status as "verificationStatus",
        verification_note as "verificationNote",
        verified_by as "verifiedBy",
        verified_at as "verifiedAt",
        updated_at as "updatedAt"
      `,
      [requestId, verificationNote, req.superAdmin?.email || "super-admin"]
    );

    if (!row) return res.status(404).json({ error: "Onboarding request not found" });

    let email = null;
    if (originalStatus !== "rejected") {
      try {
        email = await sendOnboardingStatusEmailRejected({
          to: existing.admin_email,
          adminFullName: existing.admin_full_name,
          churchName: existing.church_name,
          requestedJoinCode: String(existing.requested_join_code || "").toUpperCase(),
          requestId,
          rejectedAt: row.verifiedAt,
          verificationNote,
        });
      } catch (err) {
        console.error("[super/church-onboarding] rejected email failed", err?.message || err, err?.stack);
        email = { ok: false, error: "failed" };
      }
    }

    return res.json({ request: row, email });
  } catch (err) {
    console.error("[super/church-onboarding] reject error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.delete("/church-onboarding/:requestId", requireSuperAdmin, async (req, res) => {
  try {
    const requestId = normalize(req.params.requestId);
    if (!requestId) return res.status(400).json({ error: "requestId is required" });

    // Only allow deleting rejected requests (cleanup of spam/test). Approved requests are part of audit trail.
    const existing = await db.oneOrNone(
      `
      select id, verification_status as status
      from church_onboarding_requests
      where id = $1
      `,
      [requestId]
    );
    if (!existing) return res.status(404).json({ error: "Onboarding request not found" });

    const status = String(existing.status || "").toLowerCase();
    if (status !== "rejected") {
      return res.status(409).json({ error: "Only rejected onboarding requests can be deleted." });
    }

    // Defensive confirmation to prevent accidental deletes in the UI.
    const confirm = normalize(req.query.confirm);
    if (confirm !== requestId) {
      return res.status(400).json({ error: "Missing confirmation. Pass ?confirm=<requestId>." });
    }

    await db.none(`delete from church_onboarding_requests where id = $1 and verification_status = 'rejected'`, [
      requestId,
    ]);
    return res.json({ ok: true });
  } catch (err) {
    console.error("[super/church-onboarding] delete error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/churches/:churchId/admins/invite", requireSuperAdmin, async (req, res) => {
  try {
    const churchId = normalize(req.params.churchId);
    if (!churchId) return res.status(400).json({ error: "Church ID is required" });

    const church = await db.oneOrNone(
      `select id, name, join_code as "joinCode" from churches where id = $1`,
      [churchId]
    );
    if (!church) return res.status(404).json({ error: "Church not found" });

    const approvedOnboarding = await db.oneOrNone(
      `
      select id
      from church_onboarding_requests
      where approved_church_id = $1 and verification_status = 'approved'
      order by verified_at desc nulls last
      limit 1
      `,
      [churchId]
    );
    if (!approvedOnboarding) {
      return res.status(403).json({ error: "Church verification approval is required before inviting admins" });
    }

    const fullName = normalize(req.body?.fullName);
    const phone = normalizePhone(req.body?.phone);
    const email = normalizeEmail(req.body?.email);
    if (!fullName) return res.status(400).json({ error: "fullName is required" });
    if (!phone && !email) return res.status(400).json({ error: "phone or email is required" });

    const inviteResult = await db.tx(async (t) => {
      const adminResult = await createOrPromoteAdminMember(t, {
        churchId,
        fullName,
        phone,
        email,
      });
      return adminResult;
    });

    return res.status(201).json({
      admin: {
        ...inviteResult.member,
        created: inviteResult.created,
      },
      invite: {
        temporaryPassword: inviteResult.temporaryPassword,
      },
      church,
    });
  } catch (err) {
    if (String(err?.message || "").toLowerCase().includes("required")) {
      return res.status(400).json({ error: err.message });
    }
    console.error("[super/church-admin-invite] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/jobs", requireSuperAdmin, async (req, res) => {
  try {
    const statusRaw = normalize(req.query.status).toUpperCase();
    const status = JOB_STATUS.has(statusRaw) ? statusRaw : "";
    const search = normalize(req.query.search);
    const churchId = normalize(req.query.churchId);
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const where = ["1=1"];
    const params = [];
    let idx = 1;

    if (status) {
      where.push(`j.status = $${idx}`);
      params.push(status);
      idx += 1;
    }

    if (churchId) {
      if (!isUuid(churchId)) return res.status(400).json({ error: "churchId must be a valid UUID" });
      where.push(`j.church_id = $${idx}`);
      params.push(churchId);
      idx += 1;
    }

    if (search) {
      where.push(
        `(coalesce(j.title, '') ilike $${idx} or coalesce(j.location, '') ilike $${idx} or coalesce(j.department, '') ilike $${idx} or coalesce(c.name, '') ilike $${idx})`
      );
      params.push(`%${search}%`);
      idx += 1;
    }

    const countRow = await db.one(
      `
      select count(*)::int as count
      from job_adverts j
      left join churches c on c.id = j.church_id
      where ${where.join(" and ")}
      `,
      params
    );

    const rows = await db.manyOrNone(
      `
      select
        j.id,
        j.title,
        j.slug,
        j.church_id as "churchId",
        c.name as "churchName",
        c.join_code as "churchJoinCode",
        j.employment_type as "employmentType",
        j.location,
        j.department,
        j.summary,
        j.description,
        j.requirements,
        j.application_url as "applicationUrl",
        j.application_email as "applicationEmail",
        j.status,
        j.published_at as "publishedAt",
        j.expires_at as "expiresAt",
        j.created_by as "createdBy",
        j.created_at as "createdAt",
        j.updated_at as "updatedAt"
      from job_adverts j
      left join churches c on c.id = j.church_id
      where ${where.join(" and ")}
      order by coalesce(j.published_at, j.created_at) desc, j.created_at desc
      limit $${idx} offset $${idx + 1}
      `,
      [...params, limit, offset]
    );

    return res.json({
      jobs: rows,
      meta: {
        limit,
        offset,
        count: Number(countRow.count || 0),
        returned: rows.length,
      },
    });
  } catch (err) {
    console.error("[super/jobs] list error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/jobs", requireSuperAdmin, async (req, res) => {
  try {
    const title = normalize(req.body?.title);
    const description = normalize(req.body?.description);
    const churchIdRaw = normalize(req.body?.churchId);
    const churchId = churchIdRaw || null;
    const employmentType = normalizeEmploymentType(req.body?.employmentType, "FULL_TIME");
    const location = normalize(req.body?.location) || "South Africa";
    const department = normalize(req.body?.department) || null;
    const summary = normalize(req.body?.summary) || null;
    const requirements = normalize(req.body?.requirements) || null;
    const applicationUrlRaw = normalize(req.body?.applicationUrl);
    const applicationEmailRaw = normalize(req.body?.applicationEmail);
    const applicationUrl = applicationUrlRaw ? normalizeApplicationUrl(applicationUrlRaw) : "";
    const applicationEmail = applicationEmailRaw ? normalizeEmail(applicationEmailRaw) : "";
    const status = normalizeJobStatus(req.body?.status, "DRAFT");
    const expiresAtRaw = req.body?.expiresAt;
    const expiresAt = parseDateTime(expiresAtRaw);
    const publishedAtRaw = req.body?.publishedAt;
    const publishedAtInput = parseDateTime(publishedAtRaw);

    if (!title) return res.status(400).json({ error: "title is required" });
    if (!description) return res.status(400).json({ error: "description is required" });
    if (churchId && !isUuid(churchId)) return res.status(400).json({ error: "churchId must be a valid UUID" });
    if (applicationUrlRaw && !applicationUrl) return res.status(400).json({ error: "applicationUrl must be a valid http(s) URL" });
    if (applicationEmailRaw && !applicationEmail) return res.status(400).json({ error: "applicationEmail is invalid" });
    if (!applicationUrl && !applicationEmail) {
      return res.status(400).json({ error: "applicationUrl or applicationEmail is required" });
    }
    if (typeof expiresAtRaw !== "undefined" && expiresAtRaw !== null && String(expiresAtRaw).trim() !== "" && !expiresAt) {
      return res.status(400).json({ error: "expiresAt must be a valid date/time" });
    }
    if (typeof publishedAtRaw !== "undefined" && publishedAtRaw !== null && String(publishedAtRaw).trim() !== "" && !publishedAtInput) {
      return res.status(400).json({ error: "publishedAt must be a valid date/time" });
    }

    if (churchId) {
      const church = await db.oneOrNone(`select id from churches where id = $1`, [churchId]);
      if (!church) return res.status(404).json({ error: "Church not found" });
    }

    const slug = await ensureUniqueJobSlug(title);
    const publishedAt = status === "PUBLISHED" ? publishedAtInput || new Date() : null;

    const row = await db.one(
      `
      insert into job_adverts (
        title,
        slug,
        church_id,
        employment_type,
        location,
        department,
        summary,
        description,
        requirements,
        application_url,
        application_email,
        status,
        published_at,
        expires_at,
        created_by,
        created_at,
        updated_at
      ) values (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,now(),now()
      )
      returning
        id,
        title,
        slug,
        church_id as "churchId",
        employment_type as "employmentType",
        location,
        department,
        summary,
        description,
        requirements,
        application_url as "applicationUrl",
        application_email as "applicationEmail",
        status,
        published_at as "publishedAt",
        expires_at as "expiresAt",
        created_by as "createdBy",
        created_at as "createdAt",
        updated_at as "updatedAt"
      `,
      [
        title,
        slug,
        churchId,
        employmentType,
        location,
        department,
        summary,
        description,
        requirements,
        applicationUrl || null,
        applicationEmail || null,
        status,
        publishedAt,
        expiresAt,
        req.superAdmin?.email || "super-admin",
      ]
    );

    return res.status(201).json({ job: row });
  } catch (err) {
    console.error("[super/jobs] create error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.patch("/jobs/:jobId", requireSuperAdmin, async (req, res) => {
  try {
    const jobId = normalize(req.params.jobId);
    if (!isUuid(jobId)) return res.status(400).json({ error: "jobId must be a valid UUID" });

    const existing = await db.oneOrNone(
      `
      select
        id,
        title,
        slug,
        church_id as "churchId",
        employment_type as "employmentType",
        location,
        department,
        summary,
        description,
        requirements,
        application_url as "applicationUrl",
        application_email as "applicationEmail",
        status,
        published_at as "publishedAt",
        expires_at as "expiresAt"
      from job_adverts
      where id = $1
      `,
      [jobId]
    );
    if (!existing) return res.status(404).json({ error: "Job advert not found" });

    const sets = [];
    const params = [];
    let idx = 1;

    let nextTitle = existing.title;
    let nextStatus = existing.status;
    let nextApplicationUrl = existing.applicationUrl || "";
    let nextApplicationEmail = existing.applicationEmail || "";
    let nextPublishedAt = existing.publishedAt;

    if (typeof req.body?.title !== "undefined") {
      const title = normalize(req.body.title);
      if (!title) return res.status(400).json({ error: "title is required" });
      nextTitle = title;
      const slug = await ensureUniqueJobSlug(title, jobId);
      sets.push(`title = $${idx++}`);
      params.push(title);
      sets.push(`slug = $${idx++}`);
      params.push(slug);
    }

    if (typeof req.body?.churchId !== "undefined") {
      const churchIdRaw = normalize(req.body.churchId);
      const churchId = churchIdRaw || null;
      if (churchId && !isUuid(churchId)) return res.status(400).json({ error: "churchId must be a valid UUID" });
      if (churchId) {
        const church = await db.oneOrNone(`select id from churches where id = $1`, [churchId]);
        if (!church) return res.status(404).json({ error: "Church not found" });
      }
      sets.push(`church_id = $${idx++}`);
      params.push(churchId);
    }

    if (typeof req.body?.employmentType !== "undefined") {
      const employmentType = normalizeEmploymentType(req.body.employmentType, existing.employmentType);
      sets.push(`employment_type = $${idx++}`);
      params.push(employmentType);
    }

    if (typeof req.body?.location !== "undefined") {
      const location = normalize(req.body.location) || "South Africa";
      sets.push(`location = $${idx++}`);
      params.push(location);
    }

    if (typeof req.body?.department !== "undefined") {
      const department = normalize(req.body.department) || null;
      sets.push(`department = $${idx++}`);
      params.push(department);
    }

    if (typeof req.body?.summary !== "undefined") {
      const summary = normalize(req.body.summary) || null;
      sets.push(`summary = $${idx++}`);
      params.push(summary);
    }

    if (typeof req.body?.description !== "undefined") {
      const description = normalize(req.body.description);
      if (!description) return res.status(400).json({ error: "description is required" });
      sets.push(`description = $${idx++}`);
      params.push(description);
    }

    if (typeof req.body?.requirements !== "undefined") {
      const requirements = normalize(req.body.requirements) || null;
      sets.push(`requirements = $${idx++}`);
      params.push(requirements);
    }

    if (typeof req.body?.applicationUrl !== "undefined") {
      const raw = normalize(req.body.applicationUrl);
      const applicationUrl = raw ? normalizeApplicationUrl(raw) : "";
      if (raw && !applicationUrl) return res.status(400).json({ error: "applicationUrl must be a valid http(s) URL" });
      nextApplicationUrl = applicationUrl;
      sets.push(`application_url = $${idx++}`);
      params.push(applicationUrl || null);
    }

    if (typeof req.body?.applicationEmail !== "undefined") {
      const raw = normalize(req.body.applicationEmail);
      const applicationEmail = raw ? normalizeEmail(raw) : "";
      if (raw && !applicationEmail) return res.status(400).json({ error: "applicationEmail is invalid" });
      nextApplicationEmail = applicationEmail;
      sets.push(`application_email = $${idx++}`);
      params.push(applicationEmail || null);
    }

    if (typeof req.body?.status !== "undefined") {
      const status = normalizeJobStatus(req.body.status, existing.status);
      nextStatus = status;
      sets.push(`status = $${idx++}`);
      params.push(status);
    }

    if (typeof req.body?.publishedAt !== "undefined") {
      const raw = normalize(req.body.publishedAt);
      if (!raw) {
        nextPublishedAt = null;
      } else {
        const publishedAt = parseDateTime(raw);
        if (!publishedAt) return res.status(400).json({ error: "publishedAt must be a valid date/time" });
        nextPublishedAt = publishedAt;
      }
    }

    if (typeof req.body?.expiresAt !== "undefined") {
      const raw = normalize(req.body.expiresAt);
      if (!raw) {
        sets.push(`expires_at = null`);
      } else {
        const expiresAt = parseDateTime(raw);
        if (!expiresAt) return res.status(400).json({ error: "expiresAt must be a valid date/time" });
        sets.push(`expires_at = $${idx++}`);
        params.push(expiresAt);
      }
    }

    if (nextStatus === "PUBLISHED" && !nextPublishedAt) {
      nextPublishedAt = new Date();
    }
    if (nextStatus === "DRAFT") {
      nextPublishedAt = null;
    }

    if (typeof req.body?.status !== "undefined" || typeof req.body?.publishedAt !== "undefined") {
      if (nextPublishedAt) {
        sets.push(`published_at = $${idx++}`);
        params.push(nextPublishedAt);
      } else {
        sets.push(`published_at = null`);
      }
    }

    if (!nextApplicationUrl && !nextApplicationEmail) {
      return res.status(400).json({ error: "applicationUrl or applicationEmail is required" });
    }

    if (!sets.length) return res.status(400).json({ error: "No updates supplied" });

    sets.push(`updated_at = now()`);

    const row = await db.one(
      `
      update job_adverts
      set ${sets.join(", ")}
      where id = $${idx}
      returning
        id,
        title,
        slug,
        church_id as "churchId",
        employment_type as "employmentType",
        location,
        department,
        summary,
        description,
        requirements,
        application_url as "applicationUrl",
        application_email as "applicationEmail",
        status,
        published_at as "publishedAt",
        expires_at as "expiresAt",
        created_by as "createdBy",
        created_at as "createdAt",
        updated_at as "updatedAt"
      `,
      [...params, jobId]
    );

    return res.json({ job: row });
  } catch (err) {
    console.error("[super/jobs] update error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.delete("/jobs/:jobId", requireSuperAdmin, async (req, res) => {
  try {
    const jobId = normalize(req.params.jobId);
    if (!isUuid(jobId)) return res.status(400).json({ error: "jobId must be a valid UUID" });
    const deleted = await db.result(`delete from job_adverts where id = $1`, [jobId]);
    if (!deleted.rowCount) return res.status(404).json({ error: "Job advert not found" });
    return res.json({ ok: true });
  } catch (err) {
    console.error("[super/jobs] delete error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/transactions", requireSuperAdmin, async (req, res) => {
  try {
    const churchId = typeof req.query.churchId === "string" && req.query.churchId.trim() ? req.query.churchId.trim() : null;
    const fundId = typeof req.query.fundId === "string" && req.query.fundId.trim() ? req.query.fundId.trim() : null;
    const provider = typeof req.query.provider === "string" && req.query.provider.trim() ? req.query.provider.trim() : null;
    const status = typeof req.query.status === "string" && req.query.status.trim() ? req.query.status.trim() : null;
    const search = typeof req.query.search === "string" ? req.query.search.trim() : "";
    const from = parseDate(req.query.from, false);
    const to = parseDate(req.query.to, true);
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const { where, params: filterParams, nextParamIndex } = buildTransactionFilter({
      churchId,
      fundId,
      provider,
      status,
      search,
      from,
      to,
    });

    const params = [...filterParams, limit, offset];
    const rows = await db.manyOrNone(
      `
      select
        t.id,
        t.reference,
        t.amount,
        coalesce(t.platform_fee_amount,0)::numeric(12,2) as "platformFeeAmount",
        coalesce(t.amount_gross, t.amount)::numeric(12,2) as "amountGross",
        coalesce(t.superadmin_cut_amount,0)::numeric(12,2) as "superadminCutAmount",
        t.channel,
        t.provider,
        t.provider_payment_id as "providerPaymentId",
        coalesce(pi.status, case when t.provider in ('payfast','simulated','manual') then 'PAID' when t.provider is null then 'PAID' else upper(t.provider) end) as status,
        pi.service_date as "serviceDate",
        t.created_at as "createdAt",
        c.id as "churchId",
        c.name as "churchName",
        f.id as "fundId",
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
        ob.email as "onBehalfOfMemberEmail"
      from transactions t
      join churches c on c.id = t.church_id
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
      left join members ob on ob.id = pi.on_behalf_of_member_id
      where ${where.join(" and ")}
      order by t.created_at desc
      limit $${nextParamIndex} offset $${nextParamIndex + 1}
      `,
      params
    );

    const countRow = await db.one(
      `
      select count(*)::int as count
      from transactions t
      join churches c on c.id = t.church_id
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
    console.error("[super/transactions] list error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/transactions/export", requireSuperAdmin, async (req, res) => {
  try {
    const churchId = typeof req.query.churchId === "string" && req.query.churchId.trim() ? req.query.churchId.trim() : null;
    const fundId = typeof req.query.fundId === "string" && req.query.fundId.trim() ? req.query.fundId.trim() : null;
    const provider = typeof req.query.provider === "string" && req.query.provider.trim() ? req.query.provider.trim() : null;
    const status = typeof req.query.status === "string" && req.query.status.trim() ? req.query.status.trim() : null;
    const search = typeof req.query.search === "string" ? req.query.search.trim() : "";
    const from = parseDate(req.query.from, false);
    const to = parseDate(req.query.to, true);
    const limit = Math.min(Math.max(Number(req.query.limit || 5000), 1), 10000);

    const { where, params, nextParamIndex } = buildTransactionFilter({
      churchId,
      fundId,
      provider,
      status,
      search,
      from,
      to,
    });

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
        c.name as "churchName",
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
        ob.email as "onBehalfOfMemberEmail"
      from transactions t
      join churches c on c.id = t.church_id
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
      left join members ob on ob.id = pi.on_behalf_of_member_id
      where ${where.join(" and ")}
      order by t.created_at desc
      limit $${nextParamIndex}
      `,
      [...params, limit]
    );

    const header = [
      "id",
      "reference",
      "donationAmount",
      "feeAmount",
      "payfastFeeAmount",
      "churchNetAmount",
      "totalCharged",
      "superadminCutAmount",
      "channel",
      "provider",
      "status",
      "createdAt",
      "churchName",
      "fundCode",
      "fundName",
      "memberName",
      "memberPhone",
      "memberEmail",
      "payerType",
    ];
    const lines = [header.join(",")];
    for (const row of rows) {
      lines.push(
        [
          csvEscape(row.id),
          csvEscape(row.reference),
          csvEscape(row.amount),
          csvEscape(row.platformFeeAmount),
          csvEscape(row.payfastFeeAmount),
          csvEscape(row.churchNetAmount),
          csvEscape(row.amountGross),
          csvEscape(row.superadminCutAmount),
          csvEscape(row.channel),
          csvEscape(row.provider),
          csvEscape(row.status),
          csvEscape(row.createdAt),
          csvEscape(row.churchName),
          csvEscape(row.fundCode),
          csvEscape(row.fundName),
          csvEscape(row.memberName),
          csvEscape(row.memberPhone),
          csvEscape(row.memberEmail),
          csvEscape(row.payerType),
        ].join(",")
      );
    }

    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="super-transactions-${stamp}.csv"`);
    res.status(200).send(lines.join("\n"));
  } catch (err) {
    console.error("[super/transactions] export error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/funds", requireSuperAdmin, async (req, res) => {
  try {
    const churchId = typeof req.query.churchId === "string" && req.query.churchId.trim() ? req.query.churchId.trim() : null;
    const search = typeof req.query.search === "string" ? req.query.search.trim() : "";
    const limit = Math.min(Math.max(Number(req.query.limit || 100), 1), 300);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const where = ["1=1"];
    const params = [];
    let idx = 1;

    if (churchId) {
      where.push(`f.church_id = $${idx}`);
      params.push(churchId);
      idx++;
    }
    if (search) {
      where.push(`(coalesce(f.name, '') ilike $${idx} or coalesce(f.code, '') ilike $${idx} or coalesce(c.name, '') ilike $${idx})`);
      params.push(`%${search}%`);
      idx++;
    }

    const countRow = await db.one(
      `
      select count(*)::int as count
      from funds f
      join churches c on c.id = f.church_id
      where ${where.join(" and ")}
      `,
      params
    );

    const rows = await db.manyOrNone(
      `
      select
        f.id,
        f.church_id as "churchId",
        c.name as "churchName",
        f.code,
        f.name,
        coalesce(f.active, true) as active,
        f.created_at as "createdAt"
      from funds f
      join churches c on c.id = f.church_id
      where ${where.join(" and ")}
      order by f.created_at desc
      limit $${idx} offset $${idx + 1}
      `,
      [...params, limit, offset]
    );

    res.json({
      funds: rows,
      meta: { limit, offset, count: Number(countRow.count || 0), returned: rows.length },
    });
  } catch (err) {
    console.error("[super/funds] list error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.patch("/funds/:id", requireSuperAdmin, async (req, res) => {
  try {
    const id = String(req.params.id || "").trim();
    if (!id) return res.status(400).json({ error: "Fund ID is required" });

    const sets = [];
    const params = [];
    let idx = 1;

    if (typeof req.body?.name !== "undefined") {
      const name = String(req.body.name || "").trim();
      if (!name) return res.status(400).json({ error: "Fund name is required" });
      sets.push(`name = $${idx++}`);
      params.push(name);
    }

    if (typeof req.body?.code !== "undefined") {
      const code = String(req.body.code || "").trim().toLowerCase();
      if (!code) return res.status(400).json({ error: "Fund code is required" });
      sets.push(`code = $${idx++}`);
      params.push(code);
    }

    if (typeof req.body?.active !== "undefined") {
      sets.push(`active = $${idx++}`);
      params.push(!!req.body.active);
    }

    if (!sets.length) return res.status(400).json({ error: "No updates supplied" });
    params.push(id);

    const row = await db.oneOrNone(
      `
      update funds
      set ${sets.join(", ")}
      where id = $${idx}
      returning id, church_id as "churchId", code, name, coalesce(active, true) as active, created_at as "createdAt"
      `,
      params
    );
    if (!row) return res.status(404).json({ error: "Fund not found" });
    res.json({ fund: row });
  } catch (err) {
    if (err?.code === "23505") return res.status(409).json({ error: "Fund code already exists for this church" });
    console.error("[super/funds] patch error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/qr", requireSuperAdmin, async (req, res) => {
  try {
    const churchId = String(req.query.churchId || "").trim();
    const fundId = String(req.query.fundId || "").trim();
    const amountRaw = req.query.amount;
    if (!churchId || !fundId) return res.status(400).json({ error: "churchId and fundId are required" });

    const church = await db.oneOrNone(`select id, name, join_code as "joinCode" from churches where id = $1`, [churchId]);
    if (!church) return res.status(404).json({ error: "Church not found" });
    if (!church.joinCode) return res.status(400).json({ error: "Church join code is missing" });
    const fund = await db.oneOrNone(`select id, code, name, coalesce(active, true) as active from funds where id = $1 and church_id = $2`, [fundId, churchId]);
    if (!fund) return res.status(404).json({ error: "Fund not found" });

    let amount = null;
    if (amountRaw !== null && typeof amountRaw !== "undefined" && String(amountRaw).trim() !== "") {
      const n = Number(amountRaw);
      if (!Number.isFinite(n) || n <= 0) return res.status(400).json({ error: "Amount must be greater than 0" });
      amount = Number(n.toFixed(2));
    }

    const payload = {
      type: "churpay_donation",
      churchId: church.id,
      joinCode: church.joinCode,
      fundId: fund.id,
      fundCode: fund.code,
    };
    if (amount !== null) payload.amount = amount;
    const deepLinkBase = String(process.env.APP_DEEP_LINK_BASE || "churpaydemo://give").trim().replace(/\/+$/, "");
    let deepLink = deepLinkBase;
    deepLink = appendQueryParam(deepLink, "joinCode", church.joinCode);
    deepLink = appendQueryParam(deepLink, "fund", fund.code);
    deepLink = appendQueryParam(deepLink, "churchId", church.id);
    deepLink = appendQueryParam(deepLink, "fundId", fund.id);
    deepLink = appendQueryParam(deepLink, "fundCode", fund.code);
    if (amount !== null) deepLink = appendQueryParam(deepLink, "amount", amount);

    const webBase = normalizeWebBaseUrl();
    let webLink = `${webBase}/g/${encodeURIComponent(church.joinCode)}`;
    webLink = appendQueryParam(webLink, "fund", fund.code);
    if (amount !== null) webLink = appendQueryParam(webLink, "amount", amount);
    const qrValue = webLink;

    res.json({
      qr: { value: qrValue, payload },
      qrPayload: payload,
      deepLink,
      webLink,
      church: { id: church.id, name: church.name, joinCode: church.joinCode },
      fund: { id: fund.id, code: fund.code, name: fund.name, active: fund.active },
    });
  } catch (err) {
    console.error("[super/qr] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/members", requireSuperAdmin, async (req, res) => {
  try {
    const search = typeof req.query.search === "string" ? req.query.search.trim() : "";
    const role = typeof req.query.role === "string" ? req.query.role.trim().toLowerCase() : "";
    const churchId = typeof req.query.churchId === "string" && req.query.churchId.trim() ? req.query.churchId.trim() : "";
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 300);
    const offset = Math.max(Number(req.query.offset || 0), 0);
    const hasLastActiveAt = await hasColumn("members", "last_active_at").catch(() => false);
    const lastSeenExpr = hasLastActiveAt ? "coalesce(m.last_active_at, m.created_at)" : "m.created_at";

    const where = ["1=1"];
    const params = [];
    let idx = 1;

    if (churchId) {
      where.push(`m.church_id = $${idx}`);
      params.push(churchId);
      idx++;
    }
    if (role) {
      where.push(`lower(m.role) = $${idx}`);
      params.push(role);
      idx++;
    }
    if (search) {
      where.push(`(coalesce(m.full_name, '') ilike $${idx} or coalesce(m.email, '') ilike $${idx} or coalesce(m.phone, '') ilike $${idx} or coalesce(c.name, '') ilike $${idx})`);
      params.push(`%${search}%`);
      idx++;
    }

    const countRow = await db.one(
      `
      select count(*)::int as count
      from members m
      left join churches c on c.id = m.church_id
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
        m.church_id as "churchId",
        c.join_code as "churchJoinCode",
        c.name as "churchName",
        ${hasLastActiveAt ? "m.last_active_at" : "null::timestamptz"} as "lastActiveAt",
        ${lastSeenExpr} as "lastSeenAt",
        coalesce(extract(epoch from (now() - ${lastSeenExpr})), 0)::bigint as "inactiveSeconds",
        coalesce(extract(epoch from (now() - ${lastSeenExpr})) / 31557600.0, 0)::numeric(12,4) as "inactiveYears",
        m.created_at as "createdAt"
      from members m
      left join churches c on c.id = m.church_id
      where ${where.join(" and ")}
      order by m.created_at desc
      limit $${idx} offset $${idx + 1}
      `,
      [...params, limit, offset]
    );

    res.json({
      members: rows,
      meta: { limit, offset, count: Number(countRow.count || 0), returned: rows.length },
    });
  } catch (err) {
    console.error("[super/members] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.patch("/members/:memberId/date-of-birth", requireSuperAdmin, async (req, res) => {
  try {
    const memberId = req.params.memberId;
    if (!isUuid(memberId)) return res.status(400).json({ error: "Invalid member id" });

    const hasDateOfBirthKey =
      Object.prototype.hasOwnProperty.call(req.body || {}, "dateOfBirth") ||
      Object.prototype.hasOwnProperty.call(req.body || {}, "birthDate") ||
      Object.prototype.hasOwnProperty.call(req.body || {}, "dob");
    if (!hasDateOfBirthKey) {
      return res.status(400).json({ error: "dateOfBirth is required" });
    }

    const rawDob = req.body?.dateOfBirth ?? req.body?.birthDate ?? req.body?.dob;
    const clearRequested = rawDob === null || normalize(rawDob) === "";
    const normalizedDob = clearRequested ? null : normalizeDateOfBirth(rawDob);
    if (!clearRequested && !normalizedDob) {
      return res.status(400).json({ error: "Date of birth must be DD-MM-YYYY or YYYY-MM-DD" });
    }

    const updated = await db.oneOrNone(
      `
      update members
      set date_of_birth = $2, updated_at = now()
      where id = $1
      returning
        id,
        full_name as "fullName",
        phone,
        email,
        role,
        church_id as "churchId",
        date_of_birth as "dateOfBirth",
        created_at as "createdAt",
        updated_at as "updatedAt"
      `,
      [memberId, normalizedDob]
    );

    if (!updated) return res.status(404).json({ error: "Member not found" });

    return res.json({ ok: true, member: updated });
  } catch (err) {
    console.error("[super/members/date-of-birth] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.patch("/members/:memberId", requireSuperAdmin, async (req, res) => {
  try {
    const memberId = req.params.memberId;
    if (!isUuid(memberId)) return res.status(400).json({ error: "Invalid member id" });

    const current = await db.oneOrNone(
      `
      select
        m.id,
        m.full_name as "fullName",
        m.phone,
        m.email,
        m.role,
        m.church_id as "churchId"
      from members m
      where m.id = $1
      limit 1
      `,
      [memberId]
    );
    if (!current) return res.status(404).json({ error: "Member not found" });
    if (String(current.role || "").toLowerCase() === "super") {
      return res.status(403).json({ error: "Super accounts cannot be edited here." });
    }

    const hasFullName = Object.prototype.hasOwnProperty.call(req.body || {}, "fullName");
    const hasPhone = Object.prototype.hasOwnProperty.call(req.body || {}, "phone");
    const hasEmail = Object.prototype.hasOwnProperty.call(req.body || {}, "email");
    const hasChurchId = Object.prototype.hasOwnProperty.call(req.body || {}, "churchId");
    const hasChurchJoinCode = Object.prototype.hasOwnProperty.call(req.body || {}, "churchJoinCode");

    if (!hasFullName && !hasPhone && !hasEmail && !hasChurchId && !hasChurchJoinCode) {
      return res.status(400).json({ error: "No updates supplied" });
    }

    const updates = [];
    const params = [];
    let idx = 1;

    if (hasFullName) {
      const fullName = normalize(req.body?.fullName);
      if (!fullName) return res.status(400).json({ error: "Full name is required" });
      updates.push(`full_name = $${idx++}`);
      params.push(fullName);
    }

    if (hasPhone) {
      const rawPhone = req.body?.phone;
      if (rawPhone === null || normalize(rawPhone) === "") {
        updates.push(`phone = null`);
      } else {
        const phone = normalizePhone(rawPhone);
        if (!phone) return res.status(400).json({ error: "Invalid phone number format" });
        updates.push(`phone = $${idx++}`);
        params.push(phone);
      }
    }

    if (hasEmail) {
      const rawEmail = req.body?.email;
      if (rawEmail === null || normalize(rawEmail) === "") {
        updates.push(`email = null`);
      } else {
        const email = normalizeEmail(rawEmail);
        if (!email) return res.status(400).json({ error: "Invalid email address format" });
        updates.push(`email = $${idx++}`);
        params.push(email);
      }
    }

    if (hasChurchId || hasChurchJoinCode) {
      let nextChurchId = null;
      if (hasChurchId) {
        const rawChurchId = normalize(req.body?.churchId);
        if (rawChurchId) {
          if (!isUuid(rawChurchId)) return res.status(400).json({ error: "Invalid churchId" });
          const church = await db.oneOrNone(`select id from churches where id = $1`, [rawChurchId]);
          if (!church) return res.status(404).json({ error: "Church not found" });
          nextChurchId = church.id;
        }
      } else if (hasChurchJoinCode) {
        const rawJoinCode = normalize(req.body?.churchJoinCode).toUpperCase();
        if (rawJoinCode) {
          const church = await db.oneOrNone(`select id from churches where upper(join_code) = $1`, [rawJoinCode]);
          if (!church) return res.status(404).json({ error: "Church join code not found" });
          nextChurchId = church.id;
        }
      }

      if (!nextChurchId && ["admin", "accountant"].includes(String(current.role || "").toLowerCase())) {
        return res.status(400).json({ error: "Admin/accountant must belong to a church" });
      }
      if (nextChurchId) {
        updates.push(`church_id = $${idx++}`);
        params.push(nextChurchId);
      } else if (hasChurchId || hasChurchJoinCode) {
        updates.push(`church_id = null`);
      }
    }

    if (!updates.length) return res.status(400).json({ error: "No updates supplied" });

    params.push(memberId);
    const updated = await db.one(
      `
      update members
      set
        ${updates.join(",\n        ")},
        updated_at = now()
      where id = $${idx}
      returning
        id,
        full_name as "fullName",
        phone,
        email,
        role,
        church_id as "churchId",
        date_of_birth as "dateOfBirth",
        created_at as "createdAt",
        updated_at as "updatedAt"
      `,
      params
    );

    return res.json({ ok: true, member: updated });
  } catch (err) {
    if (err?.code === "23505") {
      return res.status(409).json({ error: "Phone or email already belongs to another member" });
    }
    console.error("[super/members/patch] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.delete("/members/:memberId", requireSuperAdmin, async (req, res) => {
  try {
    const memberId = normalize(req.params.memberId);
    if (!isUuid(memberId)) return res.status(400).json({ error: "Invalid member id" });

    const rawMinInactiveYears = Number(req.query.minInactiveYears);
    const minInactiveYears = Number.isFinite(rawMinInactiveYears)
      ? Math.min(Math.max(rawMinInactiveYears, 0), 20)
      : 1;

    const hasLastActiveAt = await hasColumn("members", "last_active_at").catch(() => false);
    const lastSeenExpr = hasLastActiveAt ? "coalesce(m.last_active_at, m.created_at)" : "m.created_at";

    const member = await db.oneOrNone(
      `
      select
        m.id,
        m.full_name as "fullName",
        m.phone,
        m.email,
        m.role,
        m.church_id as "churchId",
        c.name as "churchName",
        ${hasLastActiveAt ? "m.last_active_at" : "null::timestamptz"} as "lastActiveAt",
        ${lastSeenExpr} as "lastSeenAt",
        coalesce(extract(epoch from (now() - ${lastSeenExpr})), 0)::bigint as "inactiveSeconds",
        coalesce(extract(epoch from (now() - ${lastSeenExpr})) / 31557600.0, 0)::numeric(12,4) as "inactiveYears",
        m.created_at as "createdAt"
      from members m
      left join churches c on c.id = m.church_id
      where m.id = $1
      limit 1
      `,
      [memberId]
    );
    if (!member) return res.status(404).json({ error: "Member not found" });

    const role = String(member.role || "member").toLowerCase();
    if (role === "super") {
      return res.status(403).json({ error: "Super accounts cannot be deleted here." });
    }
    if (role !== "member") {
      return res.status(409).json({ error: "Only role=member can be deleted from this action." });
    }

    const inactiveYears = Number(member.inactiveYears || 0);
    if (inactiveYears < minInactiveYears) {
      return res.status(409).json({
        error: `Member must be inactive for at least ${minInactiveYears} year(s) before deletion.`,
        meta: {
          minInactiveYears,
          inactiveYears,
          lastActiveAt: member.lastActiveAt || member.lastSeenAt || null,
        },
      });
    }

    const blockers = {
      paymentIntentsAsPayer: await safeCount(`select count(*)::int as count from payment_intents where member_id = $1`, [memberId]),
      paymentIntentsOnBehalf: await safeCount(
        `select count(*)::int as count from payment_intents where on_behalf_of_member_id = $1`,
        [memberId]
      ),
      transactionsOnBehalf: await safeCount(
        `select count(*)::int as count from transactions where on_behalf_of_member_id = $1`,
        [memberId]
      ),
      recurringGivings: await safeCount(`select count(*)::int as count from recurring_givings where member_id = $1`, [memberId]),
      givingLinksRequested: await safeCount(
        `select count(*)::int as count from giving_links where requester_member_id = $1`,
        [memberId]
      ),
    };

    const hasFinancialHistory = Object.values(blockers).some((value) => Number(value || 0) > 0);
    if (hasFinancialHistory) {
      return res.status(409).json({
        error: "Cannot delete this member because payment history exists. Deactivate the account instead.",
        meta: {
          blockers,
          inactiveYears,
          lastActiveAt: member.lastActiveAt || member.lastSeenAt || null,
        },
      });
    }

    await db.none(`delete from members where id = $1`, [memberId]);

    return res.json({
      ok: true,
      member: {
        id: member.id,
        fullName: member.fullName,
        phone: member.phone,
        email: member.email,
        role: member.role,
        churchId: member.churchId,
        churchName: member.churchName,
      },
      meta: {
        deletedAt: new Date().toISOString(),
        inactiveYears,
        lastActiveAt: member.lastActiveAt || member.lastSeenAt || null,
        minInactiveYears,
      },
    });
  } catch (err) {
    if (err?.code === "23503") {
      return res.status(409).json({ error: "Member cannot be deleted because related records still reference this account." });
    }
    console.error("[super/members/delete] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.patch("/members/:memberId/role", requireSuperAdmin, async (req, res) => {
  try {
    const memberId = req.params.memberId;
    if (!isUuid(memberId)) return res.status(400).json({ error: "Invalid member id" });

    const role = String(req.body?.role || "")
      .trim()
      .toLowerCase();
    const allowedRoles = new Set(["member", "admin", "accountant"]);
    if (!allowedRoles.has(role)) {
      return res.status(400).json({ error: "Role must be one of: member, admin, accountant" });
    }

    const current = await db.oneOrNone(
      `
      select id, role, church_id as "churchId"
      from members
      where id = $1
      limit 1
      `,
      [memberId]
    );
    if (!current) return res.status(404).json({ error: "Member not found" });
    if (String(current.role || "").toLowerCase() === "super") {
      return res.status(403).json({ error: "Super accounts cannot be changed here." });
    }
    if ((role === "admin" || role === "accountant") && !current.churchId) {
      return res.status(400).json({ error: "Assign member to a church before staff role changes." });
    }

    const updated = await db.one(
      `
      update members
      set role = $2, updated_at = now()
      where id = $1
      returning
        id,
        full_name as "fullName",
        phone,
        email,
        role,
        church_id as "churchId",
        date_of_birth as "dateOfBirth",
        created_at as "createdAt",
        updated_at as "updatedAt"
      `,
      [memberId, role]
    );

    return res.json({ ok: true, member: updated });
  } catch (err) {
    console.error("[super/members/role] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/members/:memberId/password-reset", requireSuperAdmin, async (req, res) => {
  try {
    const memberId = req.params.memberId;
    if (!isUuid(memberId)) return res.status(400).json({ error: "Invalid member id" });

    const member = await db.oneOrNone(
      `
      select id, full_name as "fullName", email
      from members
      where id = $1
      limit 1
      `,
      [memberId]
    );
    if (!member) return res.status(404).json({ error: "Member not found" });
    const email = normalizeEmail(member.email);
    if (!email) return res.status(400).json({ error: "Member has no email on file" });

    const challenge = createVerificationChallenge();
    try {
      await db.none(
        `
        update members
        set
          password_reset_token_hash = $2,
          password_reset_code_hash = $3,
          password_reset_expires_at = $4,
          password_reset_sent_at = now(),
          password_reset_used_at = null,
          updated_at = now()
        where id = $1
        `,
        [memberId, challenge.tokenHash, challenge.codeHash, challenge.expiresAt]
      );
    } catch (err) {
      // Missing columns (migration not applied yet)
      if (err?.code === "42703") {
        return res.status(503).json({ error: "Password reset upgrade in progress. Please retry shortly." });
      }
      throw err;
    }

    const subject = "Reset your Churpay password";
    const text = [
      `Hi ${member.fullName || "there"},`,
      "",
      "A Churpay admin requested a password reset for your account.",
      "",
      "Use this code in the app to reset your password:",
      challenge.code,
      "",
      `This code expires at ${challenge.expiresAt.toISOString()}.`,
    ].join("\n");

    const html = [
      `<p>Hi ${member.fullName || "there"},</p>`,
      "<p>A Churpay admin requested a password reset for your account.</p>",
      "<p>Use this code in the app to reset your password:</p>",
      `<p style="font-size:20px;font-weight:700;letter-spacing:2px">${challenge.code}</p>`,
      `<p>This code expires at ${challenge.expiresAt.toISOString()}.</p>`,
    ].join("");

    const delivery = await sendEmail({ to: email, subject, text, html });

    return res.json({
      ok: true,
      email,
      expiresAt: challenge.expiresAt.toISOString(),
      provider: delivery?.provider || "log",
    });
  } catch (err) {
    console.error("[super/members/reset] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/settings", requireSuperAdmin, async (_req, res) => {
  res.json({
    settings: {
      environment: process.env.NODE_ENV || "development",
      rateLimits: {
        globalWindowMs: Number(process.env.RATE_LIMIT_WINDOW_MS || 15 * 60 * 1000),
        globalMax: Number(process.env.RATE_LIMIT_MAX || 300),
        authWindowMs: Number(process.env.AUTH_RATE_LIMIT_WINDOW_MS || 15 * 60 * 1000),
        authMax: Number(process.env.AUTH_RATE_LIMIT_MAX || 30),
      },
      webhooks: {
        payfastNotifyUrl: process.env.PAYFAST_NOTIFY_URL || "",
      },
      maintenanceMode: process.env.MAINTENANCE_MODE === "1",
    },
  });
});

router.get("/legal-documents", requireSuperAdmin, async (_req, res) => {
  try {
    const rows = await db.manyOrNone(
      `
      select
        doc_key as "key",
        title,
        version,
        updated_by as "updatedBy",
        updated_at as "updatedAt"
      from legal_documents
      order by doc_key asc
      `
    );
    return res.json({ documents: rows, meta: { returned: rows.length } });
  } catch (err) {
    if (err?.code === "42P01") {
      return res.status(503).json({ error: "Legal documents upgrade in progress. Please retry shortly." });
    }
    console.error("[super/legal-documents] list error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/legal-documents/:docKey", requireSuperAdmin, async (req, res) => {
  try {
    const docKey = normalize(req.params.docKey).toLowerCase();
    if (!docKey) return res.status(400).json({ error: "Document key is required" });
    if (!/^[a-z0-9_-]{1,64}$/.test(docKey)) return res.status(400).json({ error: "Invalid document key" });

    const row = await db.oneOrNone(
      `
      select
        doc_key as "key",
        title,
        body,
        version,
        updated_by as "updatedBy",
        updated_at as "updatedAt"
      from legal_documents
      where doc_key = $1
      `,
      [docKey]
    );
    if (!row) return res.status(404).json({ error: "Document not found" });
    return res.json({ document: row });
  } catch (err) {
    if (err?.code === "42P01") {
      return res.status(503).json({ error: "Legal documents upgrade in progress. Please retry shortly." });
    }
    console.error("[super/legal-documents] get error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.patch("/legal-documents/:docKey", requireSuperAdmin, async (req, res) => {
  try {
    const docKey = normalize(req.params.docKey).toLowerCase();
    if (!docKey) return res.status(400).json({ error: "Document key is required" });
    if (!/^[a-z0-9_-]{1,64}$/.test(docKey)) return res.status(400).json({ error: "Invalid document key" });

    const title = normalize(req.body?.title);
    const body = normalize(req.body?.body);
    if (!title) return res.status(400).json({ error: "title is required" });
    if (!body) return res.status(400).json({ error: "body is required" });

    const updatedBy = normalize(req.superAdmin?.email || "super-admin");

    const row = await db.one(
      `
      insert into legal_documents (doc_key, title, body, version, updated_by, created_at, updated_at)
      values ($1, $2, $3, 1, $4, now(), now())
      on conflict (doc_key) do update set
        title = excluded.title,
        body = excluded.body,
        version = legal_documents.version + 1,
        updated_by = excluded.updated_by,
        updated_at = now()
      returning
        doc_key as "key",
        title,
        body,
        version,
        updated_by as "updatedBy",
        updated_at as "updatedAt"
      `,
      [docKey, title, body, updatedBy]
    );

    return res.json({ ok: true, document: row });
  } catch (err) {
    if (err?.code === "42P01") {
      return res.status(503).json({ error: "Legal documents upgrade in progress. Please retry shortly." });
    }
    console.error("[super/legal-documents] patch error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/audit-logs", requireSuperAdmin, async (_req, res) => {
  res.json({
    logs: [
      {
        id: "seed-1",
        actor: "system",
        action: "Super admin portal initialized",
        createdAt: new Date().toISOString(),
      },
    ],
    meta: { returned: 1 },
  });
});

export default router;
