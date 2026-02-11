import express from "express";
import { buildPayfastRedirect } from "./payfast.js";
import { db } from "./db.js";
import { requireAuth, requireAdmin } from "./auth.js";
import { handlePayfastItn, payfastItnRawParser } from "./routes.webhooks.js";
import { createNotification } from "./notifications.js";
import crypto from "node:crypto";

const router = express.Router();
const isProduction = (process.env.NODE_ENV || "").toLowerCase() === "production";
const isAdminRole = (role) => role === "admin" || role === "super";
const DEFAULT_PLATFORM_FEE_FIXED = 2.5;
const DEFAULT_PLATFORM_FEE_PCT = 0.0075;
const DEFAULT_SUPERADMIN_CUT_PCT = 1.0;
const DEFAULT_CASH_RECORD_FEE_ENABLED = false;
const DEFAULT_CASH_RECORD_FEE_RATE = 0.0075;
const RECURRING_FREQUENCY_CODES = new Set([1, 2, 3, 4, 5, 6]);
const RECURRING_DEFAULT_FREQUENCY = 3; // monthly
const RECURRING_DEFAULT_CYCLES = 0; // 0 = indefinite in PayFast

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

const UUID_REGEX = /^[0-9a-fA-F-]{36}$/;

function csvEscape(value) {
  if (value === null || typeof value === "undefined") return "";
  const str = String(value);
  if (/[",\n\r]/.test(str)) return `"${str.replace(/"/g, '""')}"`;
  return str;
}

const TX_STATUS_EXPR =
  "upper(coalesce(pi.status, case when t.provider in ('payfast','simulated','manual') then 'PAID' when t.provider is null then 'PAID' else t.provider end))";
const STATEMENT_DEFAULT_STATUSES = ["PAID", "RECORDED", "CONFIRMED"];

function toBoolean(val) {
  if (typeof val === "undefined" || val === null) return undefined;
  if (typeof val === "boolean") return val;
  if (typeof val === "number") return !!val;
  const str = String(val).toLowerCase();
  return ["1", "true", "yes", "on"].includes(str);
}

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
          t.provider_payment_id as "providerPaymentId"
        from transactions t
        join funds f on f.id = t.fund_id
        left join payment_intents pi on pi.id = t.payment_intent_id
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
  return db.one(
    `select m.id, m.full_name, m.phone, m.role, m.church_id, c.name as church_name
     from members m
     left join churches c on c.id = m.church_id
     where m.id=$1`,
    [userId]
  );
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

function normalizeBaseUrl() {
  const base = process.env.PUBLIC_BASE_URL || process.env.BASE_URL;
  if (!base) return null;
  return base.endsWith("/") ? base.slice(0, -1) : base;
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
  const baseUrl = normalizeBaseUrl() || "";
  const defaultReturn = `${baseUrl}/api/payfast/return`;
  const defaultCancel = `${baseUrl}/api/payfast/cancel`;

  let returnUrl = String(process.env.PAYFAST_RETURN_URL || defaultReturn).trim();
  let cancelUrl = String(process.env.PAYFAST_CANCEL_URL || defaultCancel).trim();
  const notifyUrl = String(process.env.PAYFAST_NOTIFY_URL || `${baseUrl}/webhooks/payfast/itn`).trim();

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
router.get("/churches/me/qr", requireAdmin, async (req, res) => {
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
        coalesce(pi.status, case when t.provider in ('payfast','simulated','manual') then 'PAID' when t.provider is null then 'PAID' else upper(t.provider) end) as status,
        pi.service_date as "serviceDate",
        t.created_at as "createdAt",
        coalesce(pi.payer_name, pi.member_name) as "memberName",
        coalesce(pi.payer_phone, pi.member_phone) as "memberPhone",
        coalesce(pi.payer_email, null) as "memberEmail",
        coalesce(pi.payer_type, 'member') as "payerType",
        f.id as "fundId",
        f.code as "fundCode",
        f.name as "fundName"
      from transactions t
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
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
    let { fundId, amount, channel = "app" } = req.body || {};

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
        (church_id, fund_id, amount, status, provider, member_name, member_phone, payer_name, payer_phone, payer_type, item_name, m_payment_id, platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct)
      values ($1,$2,$3,'PENDING','payfast',$4,$5,$6,$7,'member',$8,$9,$10,$11,$12,$13,$14,$15)
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
        itemName,
        mPaymentId,
        pricing.platformFeeAmount,
        pricing.platformFeePct,
        pricing.platformFeeFixed,
        pricing.amountGross,
        pricing.superadminCutAmount,
        pricing.superadminCutPct,
      ]
    );

    const { returnUrl, cancelUrl, notifyUrl } = getPayfastCallbackUrls(intent.id, mPaymentId);

    const checkoutUrl = buildPayfastRedirect({
      mode: process.env.PAYFAST_MODE,
      merchantId: process.env.PAYFAST_MERCHANT_ID,
      merchantKey: process.env.PAYFAST_MERCHANT_KEY,
      passphrase: process.env.PAYFAST_PASSPHRASE,
      mPaymentId,
      amount: intent.amount_gross || pricing.amountGross,
      itemName,
      returnUrl,
      cancelUrl,
      notifyUrl,
      customStr1: churchId,
      customStr2: fundId,
    });

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

router.post("/recurring-givings", requireAuth, async (req, res) => {
  try {
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

    const mode = (process.env.PAYFAST_MODE || "sandbox").toLowerCase() === "live" ? "live" : "sandbox";
    const merchantId = process.env.PAYFAST_MERCHANT_ID;
    const merchantKey = process.env.PAYFAST_MERCHANT_KEY;
    const passphrase = process.env.PAYFAST_PASSPHRASE || "";
    if (!merchantId || !merchantKey) {
      return res.status(500).json({ error: "PayFast is not configured" });
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
    const checkoutUrl = buildPayfastRedirect({
      mode,
      merchantId,
      merchantKey,
      passphrase,
      mPaymentId: created.intent.mPaymentId || mPaymentId,
      amount: pricing.amountGross,
      itemName,
      returnUrl: callbacks.returnUrl,
      cancelUrl: callbacks.cancelUrl,
      notifyUrl: callbacks.notifyUrl,
      customStr1: churchId,
      customStr2: fundId,
      customStr3: created.recurring.id,
      nameFirst: member.full_name || undefined,
      emailAddress: member.email || undefined,
      subscriptionType: 1,
      billingDate,
      recurringAmount: pricing.amountGross,
      frequency,
      cycles,
    });

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
        provider: "payfast",
        mode,
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

    const mode = (process.env.PAYFAST_MODE || "sandbox").toLowerCase() === "live" ? "live" : "sandbox";
    const merchantId = process.env.PAYFAST_MERCHANT_ID;
    const merchantKey = process.env.PAYFAST_MERCHANT_KEY;
    const passphrase = process.env.PAYFAST_PASSPHRASE;

    if (!merchantId || !merchantKey) {
      return res.status(500).json({ error: "Server misconfigured: PayFast merchant keys missing" });
    }

    const callbackUrls = getPayfastCallbackUrls(intent.id, intent.m_payment_id || intent.id);
    const returnUrl = callbackUrls.returnUrl || `${baseUrl}/give?success=true`;
    const cancelUrl = callbackUrls.cancelUrl || `${baseUrl}/give?cancelled=true`;
    const notifyUrl = callbackUrls.notifyUrl || `${baseUrl}/webhooks/payfast/itn`;

    const paymentUrl = buildPayfastRedirect({
      mode,
      merchantId,
      merchantKey,
      passphrase,
      mPaymentId: intent.m_payment_id || intent.id,
      amount: intent.amount_gross || pricing.amountGross,
      itemName: intent.item_name || fund.name,
      returnUrl,
      cancelUrl,
      notifyUrl,
      customStr1: churchId,
      customStr2: fundId,
      nameFirst: member.full_name,
      emailAddress: undefined,
    });

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
    if (!isAdmin && (!ownChurchId || ownChurchId !== pi.church_id)) {
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
    console.error("[totals] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/admin/dashboard/totals", requireAdmin, async (req, res) => {
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

router.get("/admin/dashboard/transactions/recent", requireAdmin, async (req, res) => {
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
        t.created_at as "createdAt",
        coalesce(pi.payer_name, pi.member_name) as "memberName",
        coalesce(pi.payer_phone, pi.member_phone) as "memberPhone",
        coalesce(pi.payer_email, null) as "memberEmail",
        coalesce(pi.payer_type, 'member') as "payerType",
        f.id as "fundId",
        f.code as "fundCode",
        f.name as "fundName"
      from transactions t
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
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

router.get("/admin/dashboard/transactions/export", requireAdmin, async (req, res) => {
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
        f.code as "fundCode",
        f.name as "fundName"
      from transactions t
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
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

router.get("/admin/statements/summary", requireAdmin, async (req, res) => {
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

router.get("/admin/statements/export", requireAdmin, async (req, res) => {
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

router.get("/admin/statements/print", requireAdmin, async (req, res) => {
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
    const logoUrl = `${assetBase}/assets/brand/churpay-logo-500x250.png`;
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
    th, td { text-align: left; padding: 9px 10px; border-bottom: 1px solid var(--line); vertical-align: top; font-size: 12px; }
    th { font-size: 12px; color: var(--muted); font-weight: 700; }
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
    </section>

    <section class="section">
      <h3>By method</h3>
      <div class="card">
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
    </section>

    <section class="section">
      <h3>Transactions (most recent first)</h3>
      <div class="card">
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

router.get("/admin/members", requireAdmin, async (req, res) => {
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
        m.role,
        m.created_at as "createdAt",
        m.updated_at as "updatedAt"
      from members m
      where ${where.join(" and ")}
      order by case when m.role in ('admin','super') then 0 else 1 end, m.created_at desc
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

// Admin verification for cash records (prepared/recorded).
router.post("/admin/cash-givings/:paymentIntentId/confirm", requireAdmin, async (req, res) => {
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
});

router.post("/admin/cash-givings/:paymentIntentId/reject", requireAdmin, async (req, res) => {
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
});

export default router;
