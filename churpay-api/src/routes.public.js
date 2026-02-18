import express from "express";
import { db } from "./db.js";
import { buildPayfastRedirect } from "./payfast.js";
import { resolveChurchPayfastCredentials } from "./payfast-church.js";
import crypto from "node:crypto";
import bcrypt from "bcryptjs";
import { ensureUniqueJoinCode, joinCodePrefixFromChurchName, normalizeJoinCode } from "./join-code.js";
import { sendEmail } from "./email-delivery.js";
import { createNotification } from "./notifications.js";
import {
  createVerificationChallenge,
  verificationCodeMatches,
  verificationExpired,
  verificationTokenMatches,
} from "./email-verification.js";

const router = express.Router();

const DEFAULT_PLATFORM_FEE_FIXED = 2.5;
const DEFAULT_PLATFORM_FEE_PCT = 0.0075;
const DEFAULT_SUPERADMIN_CUT_PCT = 1.0;
const DEFAULT_CASH_RECORD_FEE_ENABLED = false;
const DEFAULT_CASH_RECORD_FEE_RATE = 0.0075;
// Onboarding documents are base64-encoded in JSON, so payload size is larger than raw files.
// Keep per-document size capped, but allow enough JSON headroom for two documents + metadata.
const MAX_ONBOARDING_DOC_BYTES = 10 * 1024 * 1024;
const MAX_JOB_CV_BYTES = 8 * 1024 * 1024;
const ONBOARDING_JSON_LIMIT = String(process.env.ONBOARDING_JSON_LIMIT || "50mb");
const onboardingJsonParser = express.json({ limit: ONBOARDING_JSON_LIMIT });
const ONBOARDING_EMAIL_VERIFICATION_COLUMNS = [
  "admin_email_verified",
  "admin_email_verified_at",
  "admin_email_verification_token_hash",
  "admin_email_verification_code_hash",
  "admin_email_verification_expires_at",
  "admin_email_verification_sent_at",
  "admin_email_verification_attempts",
];
const PUBLIC_JOB_EMPLOYMENT_TYPES = new Set(["FULL_TIME", "PART_TIME", "CONTRACT", "INTERNSHIP", "VOLUNTEER"]);
const allowedOnboardingMimeTypes = new Set([
  "application/pdf",
  "image/jpeg",
  "image/png",
  "image/webp",
]);
const allowedJobCvMimeTypes = new Set([
  "application/pdf",
  "application/msword",
  "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
]);
let onboardingColumnsPromise = null;

function normalize(value) {
  return String(value || "").trim();
}

function isUuid(value) {
  const v = normalize(value);
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(v);
}

function validateEmail(value) {
  const v = normalize(value).toLowerCase();
  if (!v) return "";
  const ok = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v);
  return ok ? v : "";
}

function normalizePhone(value) {
  const phone = normalize(value).replace(/\s+/g, "");
  if (!phone) return "";
  const ok = /^[+\d][\d-]{6,20}$/.test(phone);
  return ok ? phone : "";
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

function sha256Hex(value) {
  return crypto.createHash("sha256").update(String(value || ""), "utf8").digest("hex");
}

function toCurrencyNumber(value) {
  const rounded = Math.round(Number(value) * 100) / 100;
  return Number.isFinite(rounded) ? Number(rounded.toFixed(2)) : 0;
}

function parseAmount(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return toCurrencyNumber(n);
}

function parseBool(value, fallback = false) {
  if (typeof value === "undefined" || value === null || value === "") return fallback;
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
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
      return { error: `Each document must be ${Math.round(MAX_ONBOARDING_DOC_BYTES / (1024 * 1024))}MB or smaller` };
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

function parseJobCvDocument(raw, fallbackName) {
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
    if (buffer.length > MAX_JOB_CV_BYTES) {
      return { error: `CV must be ${Math.round(MAX_JOB_CV_BYTES / (1024 * 1024))}MB or smaller` };
    }
    if (!allowedJobCvMimeTypes.has(mime)) {
      return { error: "CV must be PDF or DOCX" };
    }
    return {
      buffer,
      mime,
      filename: sanitizeFilename(filename || fallbackName) || fallbackName,
    };
  } catch (_err) {
    return { error: "Invalid CV encoding" };
  }
}

function normalizeVerificationStatus(value) {
  const status = normalize(value).toLowerCase();
  if (!status) return "pending";
  return ["pending", "approved", "rejected"].includes(status) ? status : "pending";
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

async function getLegalDocumentVersion(docKey) {
  const key = normalize(docKey).toLowerCase();
  if (!key) return null;
  try {
    const row = await db.oneOrNone(`select version from legal_documents where doc_key = $1`, [key]);
    const version = row?.version;
    return Number.isFinite(Number(version)) ? Number(version) : null;
  } catch (err) {
    // Not migrated yet.
    if (err?.code === "42P01") return null;
    throw err;
  }
}

async function listPublicTableColumns(tableName) {
  const rows = await db.any(
    `
    select column_name
    from information_schema.columns
    where table_schema = 'public' and table_name = $1::text
    `,
    [tableName]
  );
  return new Set(rows.map((row) => row.column_name));
}

async function getOnboardingColumns() {
  if (!onboardingColumnsPromise) {
    onboardingColumnsPromise = listPublicTableColumns("church_onboarding_requests").catch((err) => {
      onboardingColumnsPromise = null;
      throw err;
    });
  }
  return onboardingColumnsPromise;
}

async function supportsOnboardingEmailVerification() {
  const columns = await getOnboardingColumns();
  return ONBOARDING_EMAIL_VERIFICATION_COLUMNS.every((name) => columns.has(name));
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

function normalizeApiBaseUrl() {
  const base = String(process.env.PUBLIC_BASE_URL || "https://api.churpay.com").trim();
  return base.replace(/\/+$/, "");
}

function normalizeWebBaseUrl() {
  const base = String(process.env.PUBLIC_WEB_BASE_URL || process.env.WEBSITE_BASE_URL || "https://churpay.com").trim();
  return base.replace(/\/+$/, "");
}

function normalizeShareWebBaseUrl() {
  // Prefer churpay.com for share links/QR, but allow env override.
  return normalizeWebBaseUrl();
}

function parseIsoDate(value) {
  const v = normalize(value);
  if (!v) return "";
  return /^\d{4}-\d{2}-\d{2}$/.test(v) ? v : "";
}

function normalizeAmountType(value) {
  const v = normalize(value).toUpperCase();
  return v === "OPEN" ? "OPEN" : "FIXED";
}

function normalizeEmploymentType(value) {
  const v = normalize(value).toUpperCase();
  return PUBLIC_JOB_EMPLOYMENT_TYPES.has(v) ? v : "";
}

function onboardingVerificationLink({ requestId, email, token }) {
  const base = normalizeWebBaseUrl();
  const params = new URLSearchParams();
  params.set("requestId", requestId);
  params.set("email", String(email || "").trim());
  params.set("token", String(token || "").trim());
  return `${base}/onboarding?${params.toString()}`;
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

function renderVerificationResultHtml({ title, message, ok = false }) {
  const bg = ok ? "#052e16" : "#3f1111";
  const border = ok ? "#10b981" : "#ef4444";
  const headline = ok ? "#a7f3d0" : "#fecaca";
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>${title}</title>
  <style>
    body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; background: #020617; color: #e2e8f0; }
    .wrap { min-height: 100vh; display: grid; place-items: center; padding: 20px; }
    .card { width: min(560px, 100%); border-radius: 16px; border: 1px solid ${border}; background: ${bg}; padding: 20px; box-sizing: border-box; }
    h1 { margin: 0 0 10px; font-size: 26px; color: ${headline}; }
    p { margin: 0; line-height: 1.45; font-size: 16px; }
  </style>
</head>
<body>
  <main class="wrap">
    <section class="card">
      <h1>${title}</h1>
      <p>${message}</p>
    </section>
  </main>
</body>
</html>`;
}

function getPayfastCallbackUrls(paymentIntentId, mPaymentId = null) {
  const baseUrl = normalizeApiBaseUrl();
  let returnUrl = `${baseUrl}/api/payfast/return`;
  let cancelUrl = `${baseUrl}/api/payfast/cancel`;
  const notifyUrl = `${baseUrl}/webhooks/payfast/itn`;

  returnUrl = appendQueryParam(returnUrl, "pi", paymentIntentId);
  cancelUrl = appendQueryParam(cancelUrl, "pi", paymentIntentId);
  returnUrl = appendQueryParam(returnUrl, "mp", mPaymentId);
  cancelUrl = appendQueryParam(cancelUrl, "mp", mPaymentId);

  return { returnUrl, cancelUrl, notifyUrl };
}

function makeMpaymentId() {
  return "CP-" + crypto.randomBytes(8).toString("hex").toUpperCase();
}

function makeCashReference() {
  return "CASH-" + crypto.randomBytes(8).toString("hex").toUpperCase();
}

function nextSundayIsoDate() {
  // Use UTC so results are stable across servers.
  const now = new Date();
  const day = now.getUTCDay(); // 0=Sun..6=Sat
  const daysUntilSunday = (7 - day) % 7 || 7; // always in the future
  const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  next.setUTCDate(next.getUTCDate() + daysUntilSunday);
  return next.toISOString().slice(0, 10);
}

function sanitizeItemName(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[^\x20-\x7E]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 100);
}

async function sendChurchOnboardingVerificationEmail({
  requestId,
  churchName,
  adminFullName,
  adminEmail,
  challenge,
}) {
  const verifyLink = onboardingVerificationLink({
    requestId,
    email: adminEmail,
    token: challenge.token,
  });
  const subject = "Verify your Churpay church onboarding email";
  const text = [
    `Hi ${adminFullName || "there"},`,
    "",
    `You started onboarding for ${churchName}.`,
    "Use this code to verify your admin email:",
    challenge.code,
    "",
    "Or click this secure verification link:",
    verifyLink,
    "",
    `This code expires at ${challenge.expiresAt.toISOString()}.`,
  ].join("\n");
  const html = [
    `<p>Hi ${adminFullName || "there"},</p>`,
    `<p>You started onboarding for <strong>${churchName}</strong>.</p>`,
    "<p>Use this code to verify your admin email:</p>",
    `<p style="font-size:20px;font-weight:700;letter-spacing:2px">${challenge.code}</p>`,
    `<p>Or click this secure link: <a href="${verifyLink}">${verifyLink}</a></p>`,
    `<p>This code expires at ${challenge.expiresAt.toISOString()}.</p>`,
  ].join("");
  const delivery = await sendEmail({ to: adminEmail, subject, text, html });
  return { delivery, verifyLink };
}

async function sendChurchOnboardingPendingReviewEmail({ requestId }) {
  const row = await db.oneOrNone(
    `
    select
      id,
      church_name,
      requested_join_code,
      admin_full_name,
      admin_email
    from church_onboarding_requests
    where id = $1
    `,
    [requestId]
  );
  if (!row) return { ok: false, skipped: true, reason: "not_found" };

  const churchName = normalize(row.church_name);
  const adminFullName = normalize(row.admin_full_name);
  const adminEmail = validateEmail(row.admin_email);
  const requestedJoinCode = normalize(row.requested_join_code).toUpperCase();
  if (!adminEmail) return { ok: false, skipped: true, reason: "missing_email" };

  const base = normalizeWebBaseUrl();
  const trackUrl = `${base}/onboarding?requestId=${encodeURIComponent(String(requestId))}`;
  const subject = "Churpay onboarding received (pending review)";
  const text = [
    `Hi ${adminFullName || "there"},`,
    "",
    "Thanks for submitting your church onboarding on Churpay.",
    "",
    `Church: ${churchName || "Your church"}`,
    requestedJoinCode ? `Requested join code: ${requestedJoinCode}` : null,
    "",
    "Status: Pending review.",
    "You will get an email once your application is approved or rejected.",
    "",
    "Track your application here:",
    trackUrl,
  ]
    .filter(Boolean)
    .join("\n");
  const html = [
    `<p>Hi ${adminFullName || "there"},</p>`,
    "<p>Thanks for submitting your church onboarding on Churpay.</p>",
    `<p><strong>Church:</strong> ${churchName || "Your church"}</p>`,
    requestedJoinCode ? `<p><strong>Requested join code:</strong> ${requestedJoinCode}</p>` : "",
    "<p><strong>Status:</strong> Pending review.</p>",
    "<p>You will get an email once your application is approved or rejected.</p>",
    `<p>Track your application here: <a href="${trackUrl}">${trackUrl}</a></p>`,
  ].join("");

  const delivery = await sendEmail({ to: adminEmail, subject, text, html });
  return { ok: true, delivery, trackUrl };
}

async function getOnboardingVerificationRow(requestId) {
  return db.oneOrNone(
    `
    select
      id,
      church_name,
      admin_full_name,
      admin_email,
      verification_status,
      coalesce(admin_email_verified, false) as admin_email_verified,
      admin_email_verified_at,
      admin_email_verification_token_hash,
      admin_email_verification_code_hash,
      admin_email_verification_expires_at,
      admin_email_verification_sent_at,
      coalesce(admin_email_verification_attempts, 0) as admin_email_verification_attempts
    from church_onboarding_requests
    where id = $1
    `,
    [requestId]
  );
}

function normalizeOnboardingVerificationPayload(row) {
  if (!row) return null;
  return {
    id: row.id,
    churchName: row.church_name || null,
    adminFullName: row.admin_full_name || null,
    adminEmail: row.admin_email || null,
    verificationStatus: normalizeVerificationStatus(row.verification_status),
    adminEmailVerified: !!row.admin_email_verified,
    adminEmailVerifiedAt: row.admin_email_verified_at || null,
    adminEmailVerificationSentAt: row.admin_email_verification_sent_at || null,
  };
}

async function resolveOnboardingVerificationRequest(requestId, providedEmail = "") {
  const row = await getOnboardingVerificationRow(requestId);
  if (!row) return { error: "not_found" };

  const normalizedProvided = validateEmail(providedEmail);
  const normalizedStored = validateEmail(row.admin_email);
  if (normalizedProvided && normalizedStored && normalizedProvided !== normalizedStored) {
    return { error: "email_mismatch" };
  }

  return { row };
}

async function verifyOnboardingAdminEmail({ requestId, providedEmail = "", code = "", token = "" }) {
  const resolved = await resolveOnboardingVerificationRequest(requestId, providedEmail);
  if (resolved.error) return resolved;

  const row = resolved.row;
  if (row.admin_email_verified) return { alreadyVerified: true, row };
  if (verificationExpired(row.admin_email_verification_expires_at)) {
    return { error: "expired" };
  }

  const codeOk = code ? verificationCodeMatches(code, row.admin_email_verification_code_hash) : false;
  const tokenOk = token ? verificationTokenMatches(token, row.admin_email_verification_token_hash) : false;
  if (!codeOk && !tokenOk) return { error: "invalid" };

  const updated = await db.one(
    `
    update church_onboarding_requests
    set
      admin_email_verified = true,
      admin_email_verified_at = now(),
      admin_email_verification_token_hash = null,
      admin_email_verification_code_hash = null,
      admin_email_verification_expires_at = null,
      admin_email_verification_sent_at = now(),
      updated_at = now()
    where id = $1
    returning
      id,
      church_name,
      admin_full_name,
      admin_email,
      verification_status,
      coalesce(admin_email_verified, false) as admin_email_verified,
      admin_email_verified_at,
      admin_email_verification_sent_at
    `,
    [requestId]
  );

  return { ok: true, row: updated };
}

router.post("/contact", async (req, res) => {
  try {
    const fullName = normalize(req.body?.fullName);
    const churchName = normalize(req.body?.churchName);
    const email = validateEmail(req.body?.email);
    const phone = normalize(req.body?.phone);
    const message = normalize(req.body?.message);

    if (!fullName || !email || !message) {
      return res.status(400).json({ error: "fullName, email, and message are required" });
    }

    if (message.length > 3000) {
      return res.status(400).json({ error: "message is too long" });
    }

    const row = await db.one(
      `
      insert into public_contact_messages (full_name, church_name, email, phone, message, source)
      values ($1, nullif($2, ''), $3, nullif($4, ''), $5, 'website')
      returning id, created_at
      `,
      [fullName, churchName, email, phone, message]
    );

    return res.status(201).json({
      data: {
        id: row.id,
        status: "received",
      },
      meta: {
        createdAt: row.created_at,
      },
    });
  } catch (err) {
    console.error("[public/contact]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to submit contact message" });
  }
});

router.post("/book-demo", async (req, res) => {
  try {
    const fullName = normalize(req.body?.fullName);
    const churchName = normalize(req.body?.churchName);
    const email = validateEmail(req.body?.email);
    const phone = normalizePhone(req.body?.phone);
    const preferredDate = normalize(req.body?.preferredDate || req.body?.preferredDay);
    const preferredTime = normalize(req.body?.preferredTime);
    const timezone = normalize(req.body?.timezone || req.body?.timeZone || "");
    const meetingType = normalize(req.body?.meetingType || req.body?.meetingChannel || "");
    const notes = normalize(req.body?.notes || req.body?.message || "");

    if (!fullName || !email) {
      return res.status(400).json({ error: "fullName and email are required" });
    }

    const lines = [];
    lines.push("Book demo request");
    lines.push("");
    lines.push(`Name: ${fullName}`);
    if (churchName) lines.push(`Church: ${churchName}`);
    lines.push(`Email: ${email}`);
    if (phone) lines.push(`Phone: ${phone}`);
    if (preferredDate || preferredTime) {
      lines.push(`Preferred slot: ${preferredDate || "?"} ${preferredTime || ""}`.trim());
    }
    if (timezone) lines.push(`Timezone: ${timezone}`);
    if (meetingType) lines.push(`Meeting type: ${meetingType}`);
    if (notes) {
      lines.push("");
      lines.push("Notes:");
      lines.push(notes);
    }

    const message = lines.join("\n").slice(0, 3000);

    const row = await db.one(
      `
      insert into public_contact_messages (full_name, church_name, email, phone, message, source)
      values ($1, nullif($2, ''), $3, nullif($4, ''), $5, 'book-demo')
      returning id, created_at
      `,
      [fullName, churchName, email, phone, message]
    );

    // Best-effort notify super admin.
    const superEmail = validateEmail(process.env.SUPER_ADMIN_EMAIL);
    if (superEmail) {
      try {
        await sendEmail({
          to: superEmail,
          subject: `Churpay demo booking request: ${churchName || fullName}`,
          text: message,
        });
      } catch (err) {
        console.error("[public/book-demo] notify email failed", err?.message || err, err?.stack);
      }
    }

    return res.status(201).json({
      data: {
        id: row.id,
        status: "received",
      },
      meta: {
        createdAt: row.created_at,
      },
    });
  } catch (err) {
    console.error("[public/book-demo]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to submit demo booking request" });
  }
});

router.get("/legal-documents/:docKey", async (req, res) => {
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
        updated_at as "updatedAt"
      from legal_documents
      where doc_key = $1
      `,
      [docKey]
    );
    if (!row) return res.status(404).json({ error: "Document not found" });
    return res.json({ data: row });
  } catch (err) {
    if (err?.code === "42P01") {
      return res.status(503).json({ error: "Legal documents upgrade in progress. Please retry shortly." });
    }
    console.error("[public/legal-documents]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to load legal document" });
  }
});

// Public church search for member onboarding (no auth).
// Returns only safe fields needed to select a join code.
router.get("/churches/search", async (req, res) => {
  try {
    const query = normalize(req.query?.query || req.query?.q || "");
    const limitRaw = Number(req.query?.limit || 10);
    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(25, Math.floor(limitRaw))) : 10;

    if (query.length < 2) {
      return res.json({ churches: [], meta: { query, limit, returned: 0 } });
    }

    const rows = await db.any(
      `
      select id, name, join_code as "joinCode"
      from churches
      where coalesce(active, true) = true
        and (
          name ilike $1
          or join_code ilike $1
        )
      order by name asc
      limit $2
      `,
      [`%${query}%`, limit]
    );

    return res.json({
      churches: rows,
      meta: { query, limit, returned: rows.length },
    });
  } catch (err) {
    console.error("[public/churches/search]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to search churches" });
  }
});

router.get("/jobs", async (req, res) => {
  try {
    const search = normalize(req.query?.search);
    const churchId = normalize(req.query?.churchId);
    const employmentType = normalizeEmploymentType(req.query?.employmentType);
    const limitRaw = Number(req.query?.limit || 20);
    const offsetRaw = Number(req.query?.offset || 0);
    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(100, Math.floor(limitRaw))) : 20;
    const offset = Number.isFinite(offsetRaw) ? Math.max(0, Math.floor(offsetRaw)) : 0;

    if (churchId && !isUuid(churchId)) {
      return res.status(400).json({ error: "churchId must be a valid UUID" });
    }

    const where = [
      "j.status = 'PUBLISHED'",
      "(j.published_at is null or j.published_at <= now())",
      "(j.expires_at is null or j.expires_at > now())",
    ];
    const params = [];
    let idx = 1;

    if (churchId) {
      where.push(`j.church_id = $${idx}`);
      params.push(churchId);
      idx += 1;
    }

    if (employmentType) {
      where.push(`j.employment_type = $${idx}`);
      params.push(employmentType);
      idx += 1;
    }

    if (search) {
      where.push(
        `(coalesce(j.title, '') ilike $${idx} or coalesce(j.summary, '') ilike $${idx} or coalesce(j.location, '') ilike $${idx} or coalesce(j.department, '') ilike $${idx} or coalesce(c.name, '') ilike $${idx})`
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
        j.published_at as "publishedAt",
        j.expires_at as "expiresAt",
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
    console.error("[public/jobs] list", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to load jobs" });
  }
});

router.get("/jobs/:slug", async (req, res) => {
  try {
    const slug = normalize(req.params?.slug);
    if (!slug) return res.status(400).json({ error: "slug is required" });

    const row = await db.oneOrNone(
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
        j.published_at as "publishedAt",
        j.expires_at as "expiresAt",
        j.created_at as "createdAt",
        j.updated_at as "updatedAt"
      from job_adverts j
      left join churches c on c.id = j.church_id
      where lower(j.slug) = lower($1)
        and j.status = 'PUBLISHED'
        and (j.published_at is null or j.published_at <= now())
        and (j.expires_at is null or j.expires_at > now())
      limit 1
      `,
      [slug]
    );
    if (!row) return res.status(404).json({ error: "Job not found" });

    return res.json({ job: row });
  } catch (err) {
    console.error("[public/jobs] detail", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to load job" });
  }
});

router.post("/jobs/:slug/apply", async (req, res) => {
  try {
    const slug = normalize(req.params?.slug);
    if (!slug) return res.status(400).json({ error: "slug is required" });

    const job = await db.oneOrNone(
      `
      select
        j.id,
        j.title,
        j.slug,
        j.application_email as "applicationEmail",
        c.name as "churchName"
      from job_adverts j
      left join churches c on c.id = j.church_id
      where lower(j.slug) = lower($1)
        and j.status = 'PUBLISHED'
        and (j.published_at is null or j.published_at <= now())
        and (j.expires_at is null or j.expires_at > now())
      limit 1
      `,
      [slug]
    );
    if (!job) return res.status(404).json({ error: "Job not found" });

    const fullName = normalize(req.body?.fullName);
    const email = validateEmail(req.body?.email);
    const phone = normalizePhone(req.body?.phone);
    const message = normalize(req.body?.message || req.body?.coverLetter || req.body?.notes);

    const cvDocument = parseJobCvDocument(req.body?.cvDocument, "cv");

    if (!fullName || !email) {
      return res.status(400).json({ error: "fullName and email are required" });
    }
    if (!cvDocument) {
      return res.status(400).json({ error: "cvDocument is required" });
    }
    if (cvDocument?.error) {
      return res.status(400).json({ error: `CV error: ${cvDocument.error}` });
    }
    if (message.length > 4000) {
      return res.status(400).json({ error: "message is too long" });
    }

    const token = crypto.randomBytes(24).toString("base64url");
    const tokenHash = sha256Hex(token);
    const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000);

    const row = await db.one(
      `
      insert into job_applications (
        job_id,
        full_name,
        email,
        phone,
        message,
        cv_document,
        cv_filename,
        cv_mime,
        cv_download_token_hash,
        cv_download_expires_at,
        created_at
      ) values (
        $1, $2, $3, nullif($4, ''), nullif($5, ''),
        $6, $7, $8, $9, $10, now()
      )
      returning id, created_at
      `,
      [
        job.id,
        fullName,
        email,
        phone,
        message,
        cvDocument.buffer,
        cvDocument.filename,
        cvDocument.mime,
        tokenHash,
        expiresAt,
      ]
    );

    const publicBase = normalize(process.env.PUBLIC_BASE_URL || "https://api.churpay.com").replace(/\/+$/, "");
    const cvDownloadUrl = `${publicBase}/api/public/job-applications/${encodeURIComponent(row.id)}/cv?token=${encodeURIComponent(token)}`;

    // Best-effort notify the job contact (or super admin fallback).
    const destinationEmail = validateEmail(job.applicationEmail) || validateEmail(process.env.SUPER_ADMIN_EMAIL);
    if (destinationEmail) {
      const title = normalize(job.title) || "Job";
      const churchName = normalize(job.churchName) || "Churpay";
      const notifyLines = [
        "New job application received",
        "",
        `Role: ${title}`,
        `Church: ${churchName}`,
        "",
        `Applicant: ${fullName}`,
        `Email: ${email}`,
        phone ? `Phone: ${phone}` : "",
        message ? "" : "",
        message ? "Message:" : "",
        message ? message : "",
        "",
        `CV download: ${cvDownloadUrl}`,
      ].filter(Boolean);

      try {
        await sendEmail({
          to: destinationEmail,
          subject: `Job application: ${title}`,
          text: notifyLines.join("\n"),
        });
      } catch (err) {
        console.error("[public/jobs] application notify failed", err?.message || err, err?.stack);
      }
    }

    // Best-effort applicant confirmation.
    try {
      await sendEmail({
        to: email,
        subject: "Churpay application received",
        text: `Hi ${fullName},\n\nWe received your application for "${job.title}". We will contact you if you are shortlisted.\n\nThanks,\nChurpay`,
      });
    } catch (err) {
      console.error("[public/jobs] application confirm failed", err?.message || err, err?.stack);
    }

    return res.status(201).json({
      ok: true,
      data: {
        id: row.id,
        status: "submitted",
      },
      meta: {
        createdAt: row.created_at,
      },
    });
  } catch (err) {
    console.error("[public/jobs] apply error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to submit job application" });
  }
});

router.get("/job-applications/:applicationId/cv", async (req, res) => {
  try {
    const applicationId = normalize(req.params?.applicationId);
    const token = normalize(req.query?.token);
    if (!applicationId) return res.status(400).json({ error: "applicationId is required" });
    if (!isUuid(applicationId)) return res.status(400).json({ error: "applicationId must be a valid UUID" });
    if (!token) return res.status(400).json({ error: "token is required" });

    const row = await db.oneOrNone(
      `
      select
        cv_document as document,
        cv_filename as filename,
        cv_mime as mime,
        cv_download_token_hash as token_hash,
        cv_download_expires_at as expires_at
      from job_applications
      where id = $1
      `,
      [applicationId]
    );
    if (!row || !row.document) return res.status(404).json({ error: "CV not found" });

    const expiresAt = row.expires_at ? new Date(row.expires_at) : null;
    if (expiresAt && Number.isFinite(expiresAt.getTime()) && expiresAt.getTime() < Date.now()) {
      return res.status(410).json({ error: "Download link has expired" });
    }

    if (sha256Hex(token) !== String(row.token_hash || "")) {
      return res.status(403).json({ error: "Invalid download token" });
    }

    const safeFilename = String(row.filename || "cv").replace(/"/g, "");
    res.setHeader("Content-Type", row.mime || "application/octet-stream");
    res.setHeader("Content-Disposition", `attachment; filename="${safeFilename}"`);
    return res.status(200).send(row.document);
  } catch (err) {
    console.error("[public/job-applications] cv download error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to download CV" });
  }
});

router.get("/church-onboarding/join-code-suggestion", async (req, res) => {
  try {
    const churchName = normalize(req.query?.churchName);
    if (!churchName) return res.status(400).json({ error: "churchName is required" });

    const suggestedJoinCode = await ensureUniqueJoinCode({
      db,
      churchName,
      desiredJoinCode: null,
    });

    return res.json({
      data: {
        churchName,
        prefix: joinCodePrefixFromChurchName(churchName),
        suggestedJoinCode,
      },
    });
  } catch (err) {
    console.error("[public/church-onboarding/suggest]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to generate join code suggestion" });
  }
});

router.post("/church-onboarding", onboardingJsonParser, async (req, res) => {
  try {
    const churchName = normalize(req.body?.churchName);
    const requestedJoinCodeRaw = normalizeJoinCode(req.body?.requestedJoinCode);
    const adminFullName = normalize(req.body?.adminFullName);
    const adminPhone = normalizePhone(req.body?.adminPhone);
    const adminEmail = validateEmail(req.body?.adminEmail);
    const adminEmailConfirm = validateEmail(req.body?.adminEmailConfirm || req.body?.adminEmailConfirmation || req.body?.confirmAdminEmail);
    const adminPassword = typeof req.body?.adminPassword === "string" ? req.body.adminPassword : "";
    const adminPasswordConfirm = typeof req.body?.adminPasswordConfirm === "string" ? req.body.adminPasswordConfirm : "";
    const termsAccepted = parseBool(
      req.body?.acceptTerms ?? req.body?.termsAccepted ?? req.body?.acceptedTerms,
      false
    );
    const cookiesAccepted = parseBool(
      req.body?.acceptCookies ?? req.body?.cookiesAccepted ?? req.body?.acceptedCookies ?? req.body?.cookieConsent,
      false
    );

    const cipcDocument = parseUploadedDocument(req.body?.cipcDocument, "cipc-document");
    const bankConfirmationDocument = parseUploadedDocument(req.body?.bankConfirmationDocument, "bank-confirmation");
    const bankAccountsRaw = Array.isArray(req.body?.bankAccounts) ? req.body.bankAccounts : [];
    const bankAccounts = [];

    for (const raw of bankAccountsRaw) {
      const bankName = normalize(raw?.bankName || raw?.bank || "");
      const accountName = normalize(raw?.accountName || raw?.accountHolder || raw?.accountHolderName || "");
      const accountNumber = normalizeAccountNumber(raw?.accountNumber || raw?.accountNo || raw?.account || "");
      const branchCode = normalizeBranchCode(raw?.branchCode || raw?.branch_code || "");
      const accountType = normalize(raw?.accountType || raw?.type || "");
      const isPrimary = parseBool(raw?.isPrimary, false);

      const hasAnyField = !!(bankName || accountName || accountNumber || branchCode || accountType);
      if (!hasAnyField) continue;

      if (!bankName || !accountName || !accountNumber) {
        return res.status(400).json({ error: "Each bank account must include bankName, accountName, and accountNumber" });
      }

      bankAccounts.push({
        bankName,
        accountName,
        accountNumber,
        branchCode,
        accountType,
        isPrimary,
      });
    }

    if (bankAccounts.length > 5) {
      return res.status(400).json({ error: "A maximum of 5 bank accounts is supported per onboarding request" });
    }

    const primaryCount = bankAccounts.reduce((acc, row) => acc + (row.isPrimary ? 1 : 0), 0);
    if (primaryCount > 1) {
      return res.status(400).json({ error: "Only one bank account can be marked as primary" });
    }
    if (bankAccounts.length && primaryCount === 0) {
      bankAccounts[0].isPrimary = true;
    }

    if (!churchName || !adminFullName || !adminPhone || !adminEmail) {
      return res.status(400).json({ error: "churchName, adminFullName, adminPhone, and adminEmail are required" });
    }
    if (!adminEmailConfirm) {
      return res.status(400).json({ error: "adminEmailConfirm is required" });
    }
    if (adminEmail !== adminEmailConfirm) {
      return res.status(400).json({ error: "Admin email confirmation does not match" });
    }
    if (!adminPassword || adminPassword.length < 8) {
      return res.status(400).json({ error: "adminPassword must be at least 8 characters" });
    }
    if (adminPassword !== adminPasswordConfirm) {
      return res.status(400).json({ error: "Admin password confirmation does not match" });
    }
    if (!termsAccepted) {
      return res.status(400).json({ error: "You must accept the Terms and Conditions to submit onboarding." });
    }
    if (!cookiesAccepted) {
      return res.status(400).json({ error: "You must accept cookie consent to submit onboarding." });
    }
    if (!cipcDocument || !bankConfirmationDocument) {
      return res.status(400).json({ error: "CIPC and bank confirmation documents are required" });
    }
    if (cipcDocument?.error) {
      return res.status(400).json({ error: `CIPC document error: ${cipcDocument.error}` });
    }
    if (bankConfirmationDocument?.error) {
      return res.status(400).json({ error: `Bank confirmation document error: ${bankConfirmationDocument.error}` });
    }

    const hasOnboardingPasswordHash = await hasColumn("church_onboarding_requests", "admin_password_hash");
    if (!hasOnboardingPasswordHash) {
      return res.status(503).json({ error: "Onboarding upgrade in progress. Please retry in a minute." });
    }
    if (!(await supportsOnboardingEmailVerification())) {
      return res.status(503).json({ error: "Onboarding verification upgrade in progress. Please retry shortly." });
    }
    if (bankAccounts.length && !(await hasTable("church_onboarding_bank_accounts"))) {
      return res.status(503).json({ error: "Onboarding bank account upgrade in progress. Please retry shortly." });
    }
    if (!(await hasTable("legal_documents"))) {
      return res.status(503).json({ error: "Terms upgrade in progress. Please retry shortly." });
    }

    const termsVersion = (await getLegalDocumentVersion("terms")) || 1;
    const cookieConsentVersion = (await getLegalDocumentVersion("privacy")) || termsVersion || 1;

    const requestedJoinCode =
      requestedJoinCodeRaw ||
      (await ensureUniqueJoinCode({
        db,
        churchName,
        desiredJoinCode: null,
      }));
    const adminPasswordHash = await bcrypt.hash(adminPassword, 10);
    const challenge = createVerificationChallenge();

    const row = await db.tx(async (t) => {
      const inserted = await t.one(
        `
        insert into church_onboarding_requests (
          church_name,
          requested_join_code,
          admin_full_name,
          admin_phone,
          admin_email,
          admin_password_hash,
          cipc_document,
          cipc_filename,
          cipc_mime,
          bank_confirmation_document,
          bank_confirmation_filename,
          bank_confirmation_mime,
          admin_email_verified,
          admin_email_verified_at,
          admin_email_verification_token_hash,
          admin_email_verification_code_hash,
          admin_email_verification_expires_at,
          admin_email_verification_sent_at,
          admin_email_verification_attempts,
          verification_status,
          created_at,
          updated_at
        ) values (
          $1, $2, $3, $4, $5, $6,
          $7, $8, $9,
          $10, $11, $12,
          false, null, $13, $14, $15, now(), 0,
          'pending',
          now(),
          now()
        )
        returning id, requested_join_code as "requestedJoinCode", verification_status as "verificationStatus", created_at as "createdAt"
        `,
        [
          churchName,
          requestedJoinCode,
          adminFullName,
          adminPhone,
          adminEmail,
          adminPasswordHash,
          cipcDocument.buffer,
          cipcDocument.filename,
          cipcDocument.mime,
          bankConfirmationDocument.buffer,
          bankConfirmationDocument.filename,
          bankConfirmationDocument.mime,
          challenge.tokenHash,
          challenge.codeHash,
          challenge.expiresAt,
        ]
      );

      try {
        await t.none(
          `
          update church_onboarding_requests
          set
            terms_accepted_at = now(),
            terms_version = $2,
            cookie_consent_at = now(),
            cookie_consent_version = $3
          where id = $1
          `,
          [inserted.id, termsVersion, cookieConsentVersion]
        );
      } catch (err) {
        // Backward-compatible if the terms columns aren't migrated yet.
        if (err?.code !== "42703" && err?.code !== "42P01") throw err;
      }

      for (const acct of bankAccounts) {
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
          ) values (
            $1, $2, $3, $4, nullif($5, ''), nullif($6, ''), $7, now(), now()
          )
          `,
          [
            inserted.id,
            acct.bankName,
            acct.accountName,
            acct.accountNumber,
            acct.branchCode,
            acct.accountType,
            acct.isPrimary,
          ]
        );
      }

      return inserted;
    });

    const emailDelivery = await sendChurchOnboardingVerificationEmail({
      requestId: row.id,
      churchName,
      adminFullName,
      adminEmail,
      challenge,
    });

    return res.status(201).json({
      data: {
        id: row.id,
        requestedJoinCode: row.requestedJoinCode,
        verificationStatus: row.verificationStatus,
        adminEmail,
        adminEmailVerified: false,
      },
      meta: {
        createdAt: row.createdAt,
        verificationRequired: true,
        verification: {
          channel: "email",
          email: adminEmail,
          expiresAt: challenge.expiresAt.toISOString(),
          provider: emailDelivery?.delivery?.provider || "log",
        },
      },
    });
  } catch (err) {
    console.error("[public/church-onboarding]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to submit church onboarding request" });
  }
});

router.post("/church-onboarding/:requestId/verify-email", async (req, res) => {
  try {
    if (!(await supportsOnboardingEmailVerification())) {
      return res.status(503).json({ error: "Onboarding verification upgrade in progress. Please retry shortly." });
    }

    const requestId = normalize(req.params?.requestId);
    const email = validateEmail(req.body?.email || req.query?.email);
    const code = normalize(req.body?.code || req.query?.code);
    const token = normalize(req.body?.token || req.query?.token);

    if (!requestId) return res.status(400).json({ error: "requestId is required" });
    if (!isUuid(requestId)) return res.status(400).json({ error: "requestId must be a valid UUID" });
    if (!code && !token) return res.status(400).json({ error: "code or token is required" });

    const verified = await verifyOnboardingAdminEmail({
      requestId,
      providedEmail: email,
      code,
      token,
    });

    if (verified.error === "not_found") return res.status(404).json({ error: "Onboarding request not found" });
    if (verified.error === "email_mismatch") return res.status(400).json({ error: "Provided email does not match onboarding request" });
    if (verified.error === "expired") return res.status(400).json({ error: "Verification code has expired. Request a new code." });
    if (verified.error === "invalid") return res.status(400).json({ error: "Invalid verification code or link." });

    if (verified.alreadyVerified) {
      return res.json({
        ok: true,
        alreadyVerified: true,
        data: normalizeOnboardingVerificationPayload(verified.row),
      });
    }

    // Send a "pending review" email once, immediately after successful verification.
    // Do not block the response if email fails (EMAIL_DELIVERY_REQUIRED may be false).
    try {
      await sendChurchOnboardingPendingReviewEmail({ requestId });
    } catch (err) {
      console.error(
        "[public/church-onboarding] pending-review email failed",
        err?.message || err,
        err?.stack
      );
    }

    return res.json({
      ok: true,
      data: normalizeOnboardingVerificationPayload(verified.row),
    });
  } catch (err) {
    console.error("[public/church-onboarding] verify-email", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to verify onboarding email" });
  }
});

router.get("/church-onboarding/:requestId/verify-email", async (req, res) => {
  try {
    if (!(await supportsOnboardingEmailVerification())) {
      if (req.accepts("html")) {
        return res.status(503).send(
          renderVerificationResultHtml({
            title: "Verification unavailable",
            message: "Church onboarding verification is temporarily unavailable. Please try again shortly.",
            ok: false,
          })
        );
      }
      return res.status(503).json({ error: "Onboarding verification upgrade in progress. Please retry shortly." });
    }

    const requestId = normalize(req.params?.requestId);
    const email = validateEmail(req.query?.email);
    const code = normalize(req.query?.code);
    const token = normalize(req.query?.token);

    if (!requestId) {
      const message = "requestId is required";
      if (req.accepts("html")) {
        return res.status(400).send(renderVerificationResultHtml({ title: "Verification failed", message, ok: false }));
      }
      return res.status(400).json({ error: message });
    }
    if (!isUuid(requestId)) {
      const message = "requestId must be a valid UUID";
      if (req.accepts("html")) {
        return res.status(400).send(renderVerificationResultHtml({ title: "Verification failed", message, ok: false }));
      }
      return res.status(400).json({ error: message });
    }
    if (!code && !token) {
      const message = "code or token is required";
      if (req.accepts("html")) {
        return res.status(400).send(renderVerificationResultHtml({ title: "Verification failed", message, ok: false }));
      }
      return res.status(400).json({ error: message });
    }

    const verified = await verifyOnboardingAdminEmail({
      requestId,
      providedEmail: email,
      code,
      token,
    });

    if (verified.error === "not_found") {
      const message = "Onboarding request not found";
      if (req.accepts("html")) {
        return res.status(404).send(renderVerificationResultHtml({ title: "Verification failed", message, ok: false }));
      }
      return res.status(404).json({ error: message });
    }
    if (verified.error === "email_mismatch") {
      const message = "Provided email does not match onboarding request";
      if (req.accepts("html")) {
        return res.status(400).send(renderVerificationResultHtml({ title: "Verification failed", message, ok: false }));
      }
      return res.status(400).json({ error: message });
    }
    if (verified.error === "expired") {
      const message = "Verification code has expired. Request a new code.";
      if (req.accepts("html")) {
        return res.status(400).send(renderVerificationResultHtml({ title: "Verification expired", message, ok: false }));
      }
      return res.status(400).json({ error: message });
    }
    if (verified.error === "invalid") {
      const message = "Invalid verification code or link.";
      if (req.accepts("html")) {
        return res.status(400).send(renderVerificationResultHtml({ title: "Verification failed", message, ok: false }));
      }
      return res.status(400).json({ error: message });
    }

    if (!verified.alreadyVerified) {
      try {
        await sendChurchOnboardingPendingReviewEmail({ requestId });
      } catch (err) {
        console.error(
          "[public/church-onboarding] pending-review email failed",
          err?.message || err,
          err?.stack
        );
      }
    }

    const payload = normalizeOnboardingVerificationPayload(verified.row);
    if (req.accepts("html")) {
      return res.status(200).send(
        renderVerificationResultHtml({
          title: verified.alreadyVerified ? "Already verified" : "Email verified",
          message: verified.alreadyVerified
            ? "Your admin email is already verified. Return to churpay.com/onboarding to track your request."
            : "Admin email verified successfully. Return to churpay.com/onboarding to track verification status.",
          ok: true,
        })
      );
    }

    return res.json({
      ok: true,
      alreadyVerified: !!verified.alreadyVerified,
      data: payload,
    });
  } catch (err) {
    console.error("[public/church-onboarding] verify-email-get", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to verify onboarding email" });
  }
});

router.post("/church-onboarding/:requestId/resend-verification", async (req, res) => {
  try {
    if (!(await supportsOnboardingEmailVerification())) {
      return res.status(503).json({ error: "Onboarding verification upgrade in progress. Please retry shortly." });
    }

    const requestId = normalize(req.params?.requestId);
    const email = validateEmail(req.body?.email || req.query?.email);
    if (!requestId) return res.status(400).json({ error: "requestId is required" });
    if (!isUuid(requestId)) return res.status(400).json({ error: "requestId must be a valid UUID" });

    const resolved = await resolveOnboardingVerificationRequest(requestId, email);
    if (resolved.error === "not_found") return res.status(404).json({ error: "Onboarding request not found" });
    if (resolved.error === "email_mismatch") return res.status(400).json({ error: "Provided email does not match onboarding request" });

    const current = resolved.row;
    if (current.admin_email_verified) {
      return res.json({
        ok: true,
        alreadyVerified: true,
        data: normalizeOnboardingVerificationPayload(current),
      });
    }

    const challenge = createVerificationChallenge();
    const updated = await db.one(
      `
      update church_onboarding_requests
      set
        admin_email_verification_token_hash = $2,
        admin_email_verification_code_hash = $3,
        admin_email_verification_expires_at = $4,
        admin_email_verification_sent_at = now(),
        admin_email_verification_attempts = coalesce(admin_email_verification_attempts, 0) + 1,
        updated_at = now()
      where id = $1
      returning
        id,
        church_name,
        admin_full_name,
        admin_email,
        verification_status,
        coalesce(admin_email_verified, false) as admin_email_verified,
        admin_email_verified_at,
        admin_email_verification_sent_at
      `,
      [requestId, challenge.tokenHash, challenge.codeHash, challenge.expiresAt]
    );

    const emailDelivery = await sendChurchOnboardingVerificationEmail({
      requestId: updated.id,
      churchName: updated.church_name,
      adminFullName: updated.admin_full_name,
      adminEmail: updated.admin_email,
      challenge,
    });

    return res.json({
      ok: true,
      data: {
        ...normalizeOnboardingVerificationPayload(updated),
        expiresAt: challenge.expiresAt.toISOString(),
      },
      meta: {
        provider: emailDelivery?.delivery?.provider || "log",
      },
    });
  } catch (err) {
    console.error("[public/church-onboarding] resend-verification", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to resend onboarding verification email" });
  }
});

router.get("/church-onboarding/:requestId", async (req, res) => {
  try {
    const requestId = normalize(req.params?.requestId);
    if (!requestId) return res.status(400).json({ error: "requestId is required" });
    if (!isUuid(requestId)) return res.status(400).json({ error: "requestId must be a valid UUID" });

    const row = await db.oneOrNone(
      `
      select
        id,
        church_name as "churchName",
        requested_join_code as "requestedJoinCode",
        admin_full_name as "adminFullName",
        admin_phone as "adminPhone",
        admin_email as "adminEmail",
        coalesce(admin_email_verified, false) as "adminEmailVerified",
        admin_email_verified_at as "adminEmailVerifiedAt",
        admin_email_verification_sent_at as "adminEmailVerificationSentAt",
        verification_status as "verificationStatus",
        verification_note as "verificationNote",
        approved_church_id as "approvedChurchId",
        approved_admin_member_id as "approvedAdminMemberId",
        cipc_filename as "cipcFilename",
        cipc_mime as "cipcMime",
        octet_length(cipc_document) as "cipcBytes",
        bank_confirmation_filename as "bankConfirmationFilename",
        bank_confirmation_mime as "bankConfirmationMime",
        octet_length(bank_confirmation_document) as "bankConfirmationBytes",
        created_at as "createdAt",
        updated_at as "updatedAt",
        verified_at as "verifiedAt"
      from church_onboarding_requests
      where id = $1
      `,
      [requestId]
    );

    if (!row) return res.status(404).json({ error: "Onboarding request not found" });

    return res.json({
      data: {
        ...row,
        verificationStatus: normalizeVerificationStatus(row.verificationStatus),
      },
    });
  } catch (err) {
    console.error("[public/church-onboarding] get", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to load onboarding request" });
  }
});

router.get("/give/context", async (req, res) => {
  try {
    const joinCode = normalize(req.query?.joinCode).toUpperCase();
    const fundCode = normalize(req.query?.fund).toLowerCase();
    const amount = req.query?.amount;
    if (!joinCode) return res.status(400).json({ error: "joinCode is required" });

    const church = await db.oneOrNone(
      `select id, name, join_code as "joinCode", created_at as "createdAt"
       from churches
       where upper(join_code)=upper($1)`,
      [joinCode]
    );
    if (!church) return res.status(404).json({ error: "Church not found" });

    let funds = await db.manyOrNone(
      `select id, code, name, coalesce(active, true) as active, created_at as "createdAt"
       from funds
       where church_id=$1 and coalesce(active, true)=true
       order by name asc`,
      [church.id]
    );

    if (!funds.length) {
      await db.none(
        `insert into funds (church_id, code, name, active)
         values ($1, 'general', 'General Offering', true)
         on conflict (church_id, code) do update set active=true`,
        [church.id]
      );
      funds = await db.manyOrNone(
        `select id, code, name, coalesce(active, true) as active, created_at as "createdAt"
         from funds
         where church_id=$1 and coalesce(active, true)=true
         order by name asc`,
        [church.id]
      );
    }

    const selectedFund =
      funds.find((f) => f.code === fundCode) ||
      funds.find((f) => f.code === "general") ||
      funds[0] ||
      null;

    const parsedAmount = amount ? parseAmount(amount) : null;

    res.json({
      data: {
        church,
        fund: selectedFund,
        funds,
        pricing: readFeeConfig(),
        cashPricing: readCashFeeConfig(),
        suggestedAmount: parsedAmount,
        webLink: `${normalizeWebBaseUrl()}/g/${encodeURIComponent(church.joinCode)}${selectedFund ? `?fund=${encodeURIComponent(selectedFund.code)}` : ""}`,
      },
      meta: {
        joinCode: church.joinCode,
      },
    });
  } catch (err) {
    console.error("[public/give/context]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to load giving context" });
  }
});

// Public context for shareable giving links (no login).
router.get("/giving-links/:token", async (req, res) => {
  try {
    const token = normalize(req.params?.token);
    if (!token) return res.status(400).json({ error: "token is required" });

    const row = await db.oneOrNone(
      `
      select
        gl.id,
        gl.token,
        gl.amount_type as "amountType",
        gl.amount_fixed as "amountFixed",
        gl.currency,
        gl.message,
        gl.status,
        gl.expires_at as "expiresAt",
        gl.max_uses as "maxUses",
        gl.use_count as "useCount",
        gl.created_at as "createdAt",
        c.id as "churchId",
        c.name as "churchName",
        c.join_code as "churchJoinCode",
        f.id as "fundId",
        f.code as "fundCode",
        f.name as "fundName",
        coalesce(f.active, true) as "fundActive"
      from giving_links gl
      join churches c on c.id = gl.church_id
      join funds f on f.id = gl.fund_id
      where gl.token = $1
      limit 1
      `,
      [token]
    );
    if (!row) return res.status(404).json({ error: "Link not found" });
    if (!row.fundActive) return res.status(400).json({ error: "Fund is inactive" });

    const expiresAtMs = new Date(row.expiresAt).getTime();
    const expired = Number.isFinite(expiresAtMs) && expiresAtMs <= Date.now();
    const usedOut = Number(row.useCount || 0) >= Number(row.maxUses || 1);
    const active = String(row.status || "").toUpperCase() === "ACTIVE";

    let status = String(row.status || "ACTIVE").toUpperCase();
    if (active && expired) status = "EXPIRED";
    if (active && usedOut) status = "PAID";

    return res.json({
      data: {
        id: row.id,
        token: row.token,
        amountType: normalizeAmountType(row.amountType),
        amountFixed: row.amountFixed === null ? null : Number(row.amountFixed),
        currency: row.currency || "ZAR",
        message: row.message || null,
        status: status.toLowerCase(),
        expiresAt: row.expiresAt,
        shareUrl: `${normalizeShareWebBaseUrl()}/l/${encodeURIComponent(row.token)}`,
        church: { id: row.churchId, name: row.churchName, joinCode: row.churchJoinCode },
        fund: { id: row.fundId, code: row.fundCode, name: row.fundName },
        pricing: readFeeConfig(),
      },
      meta: {
        token: row.token,
      },
    });
  } catch (err) {
    console.error("[public/giving-links/context]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to load giving link" });
  }
});

router.post("/giving-links/:token/pay", async (req, res) => {
  try {
    const token = normalize(req.params?.token);
    if (!token) return res.status(400).json({ error: "token is required" });

    const payerName = normalize(req.body?.payerName);
    const payerPhone = normalizePhone(req.body?.payerPhone);
    const payerEmail = validateEmail(req.body?.payerEmail);
    const notes = typeof req.body?.notes === "string" ? req.body.notes.trim().slice(0, 500) : null;
    const serviceDate = parseIsoDate(req.body?.serviceDate) || null;

    if (!payerName || !payerPhone) {
      return res.status(400).json({ error: "payerName and payerPhone are required" });
    }

    const link = await db.oneOrNone(
      `
      select
        gl.id,
        gl.requester_member_id as "requesterMemberId",
        gl.church_id as "churchId",
        gl.fund_id as "fundId",
        gl.amount_type as "amountType",
        gl.amount_fixed as "amountFixed",
        gl.status,
        gl.expires_at as "expiresAt",
        gl.max_uses as "maxUses",
        gl.use_count as "useCount",
        c.name as "churchName",
        c.join_code as "churchJoinCode",
        f.code as "fundCode",
        f.name as "fundName",
        coalesce(f.active,true) as "fundActive"
      from giving_links gl
      join churches c on c.id = gl.church_id
      join funds f on f.id = gl.fund_id
      where gl.token = $1
      limit 1
      `,
      [token]
    );
    if (!link) return res.status(404).json({ error: "Link not found" });
    if (!link.fundActive) return res.status(400).json({ error: "Fund is inactive" });

    const expiresAtMs = new Date(link.expiresAt).getTime();
    if (Number.isFinite(expiresAtMs) && expiresAtMs <= Date.now()) return res.status(400).json({ error: "Link expired" });
    if (String(link.status || "").toUpperCase() !== "ACTIVE") return res.status(400).json({ error: "Link is not active" });
    if (Number(link.useCount || 0) >= Number(link.maxUses || 1)) return res.status(400).json({ error: "Link already used" });

    const amountType = normalizeAmountType(link.amountType);
    const amount = amountType === "OPEN" ? parseAmount(req.body?.amount) : toCurrencyNumber(link.amountFixed);
    if (!amount || amount <= 0) return res.status(400).json({ error: "Invalid amount" });

    const payfastCreds = await resolveChurchPayfastCredentials(link.churchId);
    if (!payfastCreds?.merchantId || !payfastCreds?.merchantKey) {
      return res.status(503).json({
        error: "Payments are not activated for this church. Ask your church admin to connect PayFast.",
        code: "PAYFAST_NOT_CONNECTED",
      });
    }
    const mode = payfastCreds.mode;
    const merchantId = payfastCreds.merchantId;
    const merchantKey = payfastCreds.merchantKey;
    const passphrase = payfastCreds.passphrase || "";

    const pricing = buildFeeBreakdown(amount);
    const mPaymentId = makeMpaymentId();
    const itemName = sanitizeItemName(`${link.churchName} - ${link.fundName}`);
    const nameFirst = payerName.split(/\s+/).filter(Boolean)[0] || "Guest";

    const intent = await db.one(
      `insert into payment_intents (
        church_id, fund_id, amount, currency, status,
        member_name, member_phone,
        payer_name, payer_phone, payer_email, payer_type,
        channel, provider, provider_payment_id, m_payment_id, item_name,
        platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct,
        source, giving_link_id, on_behalf_of_member_id,
        service_date, notes,
        created_at, updated_at
      ) values (
        $1,$2,$3,'ZAR','PENDING',
        $4,$5,
        $6,$7,$8,'on_behalf',
        'web','payfast',null,$9,$10,
        $11,$12,$13,$14,$15,$16,
        'SHARE_LINK',$17,$18,
        $19,$20,
        now(),now()
      ) returning id`,
      [
        link.churchId,
        link.fundId,
        pricing.amount,
        payerName,
        payerPhone,
        payerName,
        payerPhone,
        payerEmail || null,
        mPaymentId,
        itemName,
        pricing.platformFeeAmount,
        pricing.platformFeePct,
        pricing.platformFeeFixed,
        pricing.amountGross,
        pricing.superadminCutAmount,
        pricing.superadminCutPct,
        link.id,
        link.requesterMemberId,
        serviceDate,
        notes,
      ]
    );

    const callbacks = getPayfastCallbackUrls(intent.id, mPaymentId);

    const checkoutUrl = buildPayfastRedirect({
      mode,
      merchantId,
      merchantKey,
      passphrase,
      mPaymentId,
      amount: pricing.amountGross,
      itemName,
      returnUrl: callbacks.returnUrl,
      cancelUrl: callbacks.cancelUrl,
      notifyUrl: callbacks.notifyUrl,
      customStr1: link.churchId,
      customStr2: link.fundId,
      nameFirst,
      emailAddress: payerEmail || undefined,
    });

    return res.status(201).json({
      data: {
        paymentIntentId: intent.id,
        mPaymentId,
        checkoutUrl,
        amount: pricing.amount,
        processingFee: pricing.platformFeeAmount,
        totalCharged: pricing.amountGross,
        currency: "ZAR",
        church: { id: link.churchId, name: link.churchName, joinCode: link.churchJoinCode },
        fund: { id: link.fundId, code: link.fundCode, name: link.fundName },
      },
      meta: {
        payerType: "on_behalf",
        provider: "payfast",
        source: "SHARE_LINK",
      },
    });
  } catch (err) {
    console.error("[public/giving-links/pay]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to start payment" });
  }
});

router.post("/give/payment-intents", async (req, res) => {
  try {
    const joinCode = normalize(req.body?.joinCode).toUpperCase();
    const fundCode = normalize(req.body?.fundCode).toLowerCase();
    const fundId = normalize(req.body?.fundId);
    const payerName = normalize(req.body?.payerName);
    const payerPhone = normalize(req.body?.payerPhone);
    const payerEmail = validateEmail(req.body?.payerEmail);
    const channel = "web";
    const amount = parseAmount(req.body?.amount);

    if (!joinCode || !payerName || !payerPhone || !amount) {
      return res.status(400).json({ error: "joinCode, payerName, payerPhone and amount are required" });
    }

    const church = await db.oneOrNone(
      `select id, name, join_code as "joinCode"
       from churches
       where upper(join_code)=upper($1)`,
      [joinCode]
    );
    if (!church) return res.status(404).json({ error: "Church not found" });

    let fund = null;
    if (fundId) {
      fund = await db.oneOrNone(
        `select id, code, name, coalesce(active, true) as active
         from funds where id=$1 and church_id=$2`,
        [fundId, church.id]
      );
    } else if (fundCode) {
      fund = await db.oneOrNone(
        `select id, code, name, coalesce(active, true) as active
         from funds where church_id=$1 and lower(code)=lower($2)`,
        [church.id, fundCode]
      );
    } else {
      fund = await db.oneOrNone(
        `select id, code, name, coalesce(active, true) as active
         from funds where church_id=$1 and lower(code)='general'`,
        [church.id]
      );
      if (!fund) {
        fund = await db.oneOrNone(
          `select id, code, name, coalesce(active, true) as active
           from funds where church_id=$1 and coalesce(active, true)=true
           order by name asc limit 1`,
          [church.id]
        );
      }
    }

    if (!fund) return res.status(404).json({ error: "Fund not found" });
    if (!fund.active) return res.status(400).json({ error: "Fund is inactive" });

    const payfastCreds = await resolveChurchPayfastCredentials(church.id);
    if (!payfastCreds?.merchantId || !payfastCreds?.merchantKey) {
      return res.status(503).json({
        error: "Payments are not activated for this church. Ask your church admin to connect PayFast.",
        code: "PAYFAST_NOT_CONNECTED",
      });
    }
    const mode = payfastCreds.mode;
    const merchantId = payfastCreds.merchantId;
    const merchantKey = payfastCreds.merchantKey;
    const passphrase = payfastCreds.passphrase || "";

    const pricing = buildFeeBreakdown(amount);
    const mPaymentId = makeMpaymentId();
    const itemName = sanitizeItemName(`${church.name} - ${fund.name}`);
    const nameFirst = payerName.split(/\s+/).filter(Boolean)[0] || "Guest";

    const intent = await db.one(
      `insert into payment_intents (
        church_id, fund_id, amount, currency, status,
        member_name, member_phone,
        payer_name, payer_phone, payer_email, payer_type,
        channel, provider, provider_payment_id, m_payment_id, item_name,
        platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct,
        created_at, updated_at
      ) values (
        $1,$2,$3,'ZAR','PENDING',
        $4,$5,
        $6,$7,$8,'visitor',
        $9,'payfast',null,$10,$11,
        $12,$13,$14,$15,$16,$17,
        now(),now()
      ) returning id`,
      [
        church.id,
        fund.id,
        pricing.amount,
        payerName,
        payerPhone,
        payerName,
        payerPhone,
        payerEmail || null,
        channel,
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

    const callbacks = getPayfastCallbackUrls(intent.id, mPaymentId);

    const checkoutUrl = buildPayfastRedirect({
      mode,
      merchantId,
      merchantKey,
      passphrase,
      mPaymentId,
      amount: pricing.amountGross,
      itemName,
      returnUrl: callbacks.returnUrl,
      cancelUrl: callbacks.cancelUrl,
      notifyUrl: callbacks.notifyUrl,
      customStr1: church.id,
      customStr2: fund.id,
      nameFirst,
      emailAddress: payerEmail || undefined,
    });

    return res.status(201).json({
      data: {
        paymentIntentId: intent.id,
        mPaymentId,
        checkoutUrl,
        amount: pricing.amount,
        processingFee: pricing.platformFeeAmount,
        totalCharged: pricing.amountGross,
        currency: "ZAR",
        church: { id: church.id, name: church.name, joinCode: church.joinCode },
        fund: { id: fund.id, code: fund.code, name: fund.name },
      },
      meta: {
        payerType: "visitor",
        provider: "payfast",
      },
    });
  } catch (err) {
    console.error("[public/give/payment-intents]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to start payment" });
  }
});

// Visitor cash records (no processor): visitor records a cash gift for admin confirmation.
router.post("/give/cash-givings", async (req, res) => {
  try {
    const joinCode = normalize(req.body?.joinCode).toUpperCase();
    const fundCode = normalize(req.body?.fundCode).toLowerCase();
    const fundId = normalize(req.body?.fundId);
    const payerName = normalize(req.body?.payerName);
    const payerPhone = normalizePhone(req.body?.payerPhone);
    const payerEmail = validateEmail(req.body?.payerEmail);
    const notes = normalize(req.body?.notes).slice(0, 500);
    const channel = "web";
    const amount = parseAmount(req.body?.amount);
    const serviceDate = parseIsoDate(req.body?.serviceDate) || nextSundayIsoDate();

    if (!joinCode || !payerName || !payerPhone || !amount) {
      return res.status(400).json({ error: "joinCode, payerName, payerPhone and amount are required" });
    }

    const church = await db.oneOrNone(
      `select id, name, join_code as "joinCode"
       from churches
       where upper(join_code)=upper($1)`,
      [joinCode]
    );
    if (!church) return res.status(404).json({ error: "Church not found" });

    let fund = null;
    if (fundId) {
      fund = await db.oneOrNone(
        `select id, code, name, coalesce(active, true) as active
         from funds where id=$1 and church_id=$2`,
        [fundId, church.id]
      );
    } else if (fundCode) {
      fund = await db.oneOrNone(
        `select id, code, name, coalesce(active, true) as active
         from funds where church_id=$1 and lower(code)=lower($2)`,
        [church.id, fundCode]
      );
    } else {
      fund = await db.oneOrNone(
        `select id, code, name, coalesce(active, true) as active
         from funds where church_id=$1 and lower(code)='general'`,
        [church.id]
      );
      if (!fund) {
        fund = await db.oneOrNone(
          `select id, code, name, coalesce(active, true) as active
           from funds where church_id=$1 and coalesce(active, true)=true
           order by name asc limit 1`,
          [church.id]
        );
      }
    }

    if (!fund) return res.status(404).json({ error: "Fund not found" });
    if (!fund.active) return res.status(400).json({ error: "Fund is inactive" });

    const pricing = buildCashFeeBreakdown(amount);
    const reference = makeCashReference();
    const itemName = sanitizeItemName(`${church.name} - ${fund.name} (Cash)`);

    const intent = await db.one(
      `insert into payment_intents (
        church_id, fund_id, amount, currency, status,
        member_name, member_phone,
        payer_name, payer_phone, payer_email, payer_type,
        channel, provider, provider_payment_id, m_payment_id, item_name,
        platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct,
        service_date, notes, cash_verified_by_admin,
        created_at, updated_at
      ) values (
        $1,$2,$3,'ZAR','PREPARED',
        $4,$5,
        $6,$7,$8,'visitor',
        $9,'cash',null,$10,$11,
        $12,$13,$14,$15,$16,$17,
        $18,$19,false,
        now(),now()
      ) returning id, status, amount, amount_gross, platform_fee_amount, created_at`,
      [
        church.id,
        fund.id,
        pricing.amount,
        payerName,
        payerPhone,
        payerName,
        payerPhone,
        payerEmail || null,
        channel,
        reference,
        itemName,
        pricing.platformFeeAmount,
        pricing.platformFeePct,
        pricing.platformFeeFixed,
        pricing.amountGross,
        pricing.superadminCutAmount,
        pricing.superadminCutPct,
        serviceDate,
        notes || null,
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
        $11,$12,$13,'visitor',
        $14,$15,'cash',null,now()
      ) returning id, reference, created_at
      `,
      [
        church.id,
        fund.id,
        intent.id,
        pricing.amount,
        pricing.platformFeeAmount,
        pricing.platformFeePct,
        pricing.platformFeeFixed,
        pricing.amountGross,
        pricing.superadminCutAmount,
        pricing.superadminCutPct,
        payerName,
        payerPhone,
        payerEmail || null,
        reference,
        channel,
      ]
    );

    // Best-effort: notify church staff to confirm the cash record.
    try {
      const staff = await db.manyOrNone(
        `
        select id
        from members
        where church_id=$1 and lower(role) in ('admin','accountant')
        `,
        [church.id]
      );
      const amount = toCurrencyNumber(pricing.amount || 0);
      for (const staffMember of staff) {
        await createNotification({
          memberId: staffMember.id,
          type: "CASH_RECORDED",
          title: "Cash giving recorded",
          body: `${payerName || "A visitor"} recorded R ${amount.toFixed(2)} cash to ${fund.name} (PREPARED).`,
          data: {
            paymentIntentId: intent.id,
            transactionId: txRow.id,
            reference,
            churchId: church.id,
            fundId: fund.id,
            amount,
            status: "PREPARED",
            provider: "cash",
            payerType: "visitor",
            serviceDate,
            requiresAdminConfirmation: true,
          },
        });
      }
    } catch (err) {
      console.error("[public/give/cash-givings] notify staff failed", err?.message || err);
    }

    return res.status(201).json({
      data: {
        paymentIntentId: intent.id,
        transactionId: txRow.id,
        reference: txRow.reference,
        method: "CASH",
        status: intent.status,
        amount: Number(intent.amount),
        pricing: {
          donationAmount: pricing.amount,
          churpayFee: pricing.platformFeeAmount,
          totalCharged: pricing.amountGross,
          feeEnabled: pricing.cashFeeEnabled,
          feeRate: pricing.platformFeePct,
        },
        serviceDate,
        church: { id: church.id, name: church.name, joinCode: church.joinCode },
        fund: { id: fund.id, code: fund.code, name: fund.name },
        createdAt: txRow.created_at,
      },
      meta: {
        payerType: "visitor",
        provider: "cash",
        requiresAdminConfirmation: true,
      },
    });
  } catch (err) {
    console.error("[public/give/cash-givings]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to record cash giving" });
  }
});

export default router;
