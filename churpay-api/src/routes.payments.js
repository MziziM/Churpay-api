import express from "express";
import { buildPayfastRedirect, generateSignature } from "./payfast.js";
import { db } from "./db.js";
import { requireAuth, requireAdmin } from "./auth.js";
import crypto from "node:crypto";

const router = express.Router();
const isProduction = (process.env.NODE_ENV || "").toLowerCase() === "production";
const isAdminRole = (role) => role === "admin" || role === "super";
const DEFAULT_PLATFORM_FEE_FIXED = 2.5;
const DEFAULT_PLATFORM_FEE_PCT = 0.0075;
const DEFAULT_SUPERADMIN_CUT_PCT = 1.0;

function makeMpaymentId() {
  return "CP-" + crypto.randomBytes(8).toString("hex").toUpperCase();
}

function toCurrencyNumber(value) {
  const rounded = Math.round(Number(value) * 100) / 100;
  return Number.isFinite(rounded) ? Number(rounded.toFixed(2)) : 0;
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

const UUID_REGEX = /^[0-9a-fA-F-]{36}$/;

function csvEscape(value) {
  if (value === null || typeof value === "undefined") return "";
  const str = String(value);
  if (/[",\n\r]/.test(str)) return `"${str.replace(/"/g, '""')}"`;
  return str;
}

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
  const where = includeInactive ? "church_id=$1" : "church_id=$1 and active=true";
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
      `upper(coalesce(pi.status, case when t.provider in ('payfast','simulated','manual') then 'PAID' when t.provider is null then 'PAID' else t.provider end)) = $${paramIndex}`
    );
    paramIndex++;
  }

  if (typeof search === "string" && search.trim()) {
    const term = `%${search.trim()}%`;
    params.push(term);
    where.push(
      `(t.reference ilike $${paramIndex} or coalesce(pi.member_name, '') ilike $${paramIndex} or coalesce(pi.member_phone, '') ilike $${paramIndex})`
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

async function loadMember(userId) {
  return db.one(
    `select m.id, m.full_name, m.phone, m.role, m.church_id, c.name as church_name
     from members m
     left join churches c on c.id = m.church_id
     where m.id=$1`,
    [userId]
  );
}

function requireChurch(req, res) {
  if (!req.user?.church_id) {
    res.status(400).json({ error: "Join a church first" });
    return null;
  }
  return req.user.church_id;
}

async function ensurePaymentIntentsTable() {
  await db.none(`
    create extension if not exists pgcrypto;
    create table if not exists payment_intents (
      id uuid primary key default gen_random_uuid(),
      church_id uuid not null,
      fund_id uuid not null,
      amount numeric(12,2) not null,
      currency text default 'ZAR',
      status text not null default 'PENDING',
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      member_name text,
      member_phone text,
      channel text,
      provider text,
      provider_payment_id text,
      m_payment_id text,
      item_name text,
      platform_fee_amount numeric(12,2),
      platform_fee_pct numeric(6,4),
      platform_fee_fixed numeric(12,2),
      amount_gross numeric(12,2),
      superadmin_cut_amount numeric(12,2),
      superadmin_cut_pct numeric(6,4)
    );
    alter table payment_intents
      add column if not exists currency text default 'ZAR',
      add column if not exists updated_at timestamptz default now(),
      add column if not exists provider_payment_id text,
      add column if not exists m_payment_id text,
      add column if not exists item_name text,
      add column if not exists provider text,
      add column if not exists channel text,
      add column if not exists member_name text,
      add column if not exists member_phone text,
      add column if not exists platform_fee_amount numeric(12,2),
      add column if not exists platform_fee_pct numeric(6,4),
      add column if not exists platform_fee_fixed numeric(12,2),
      add column if not exists amount_gross numeric(12,2),
      add column if not exists superadmin_cut_amount numeric(12,2),
      add column if not exists superadmin_cut_pct numeric(6,4);
    create index if not exists idx_payment_intents_church on payment_intents (church_id);
    create index if not exists idx_payment_intents_fund on payment_intents (fund_id);
    alter table if exists transactions
      add column if not exists platform_fee_amount numeric(12,2),
      add column if not exists platform_fee_pct numeric(6,4),
      add column if not exists platform_fee_fixed numeric(12,2),
      add column if not exists amount_gross numeric(12,2),
      add column if not exists superadmin_cut_amount numeric(12,2),
      add column if not exists superadmin_cut_pct numeric(6,4);
  `);
}

function normalizeBaseUrl() {
  const base = process.env.PUBLIC_BASE_URL || process.env.BASE_URL;
  if (!base) return null;
  return base.endsWith("/") ? base.slice(0, -1) : base;
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

    const qrPayload = {
      type: "churpay_donation",
      churchId,
      fundId: fund.id,
      fundCode: fund.code,
    };
    if (amount !== null) qrPayload.amount = Number(amount.toFixed(2));

    const qrValue = JSON.stringify(qrPayload);
    const deepLinkBase = String(process.env.APP_DEEP_LINK_BASE || "churpaydemo://donate")
      .trim()
      .replace(/\/+$/, "");
    let deepLink = deepLinkBase;
    deepLink = appendQueryParam(deepLink, "churchId", churchId);
    deepLink = appendQueryParam(deepLink, "fundId", fund.id);
    deepLink = appendQueryParam(deepLink, "fundCode", fund.code);
    if (amount !== null) {
      deepLink = appendQueryParam(deepLink, "amount", Number(amount.toFixed(2)));
    }

    const base = normalizeBaseUrl();
    let webLink = base ? `${base}/give` : "";
    webLink = appendQueryParam(webLink, "churchId", churchId);
    webLink = appendQueryParam(webLink, "fundId", fund.id);
    webLink = appendQueryParam(webLink, "fundCode", fund.code);
    if (amount !== null) {
      webLink = appendQueryParam(webLink, "amount", Number(amount.toFixed(2)));
    }

    return res.json({
      qr: {
        value: qrValue,
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
        coalesce(t.amount_gross, t.amount)::numeric(12,2) as "amountGross",
        coalesce(t.superadmin_cut_amount,0)::numeric(12,2) as "superadminCutAmount",
        t.channel,
        t.provider,
        t.provider_payment_id as "providerPaymentId",
        coalesce(pi.status, case when t.provider in ('payfast','simulated','manual') then 'PAID' when t.provider is null then 'PAID' else upper(t.provider) end) as status,
        t.created_at as "createdAt",
        pi.member_name as "memberName",
        pi.member_phone as "memberPhone",
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
    await ensurePaymentIntentsTable();

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
             id, church_id, fund_id, amount, currency, status, member_name, member_phone, channel, provider, provider_payment_id, m_payment_id, item_name, platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct, created_at, updated_at
           ) values (
             $1,$2,$3,$4,'ZAR','PENDING',$5,$6,$7,'manual',null,$8,$9,$10,$11,$12,$13,$14,$15,now(),now()
           ) returning id`,
          [
            intentId,
            churchId,
            fundId,
            pricing.amount,
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
            church_id, fund_id, payment_intent_id, amount, platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct, reference, channel, provider, provider_payment_id, created_at
          ) values (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'manual',null,now()
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
        (church_id, fund_id, amount, status, provider, member_name, member_phone, item_name, m_payment_id, platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct)
      values ($1,$2,$3,'PENDING','payfast',$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
      returning *
    `,
      [
        churchId,
        fundId,
        pricing.amount,
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

    await ensurePaymentIntentsTable();

    const itemNameRaw = `${fund.name}`;
    const itemName = itemNameRaw
      .normalize("NFKD")
      .replace(/[^\x20-\x7E]/g, "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 100);

    const intent = await db.one(
      `insert into payment_intents (
         church_id, fund_id, amount, status, member_name, member_phone, channel, provider, m_payment_id, item_name, platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct
       ) values (
         $1, $2, $3, 'PENDING', $4, $5, $6, 'payfast', gen_random_uuid(), $7, $8, $9, $10, $11, $12, $13
       ) returning id, amount, church_id, fund_id, m_payment_id, item_name, amount_gross, platform_fee_amount, superadmin_cut_amount`,
      [
        churchId,
        fundId,
        pricing.amount,
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
    const notifyUrl = callbackUrls.notifyUrl || `${baseUrl}/api/payfast/itn`;

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

// ==========================
// PayFast ITN handler
// ==========================
router.post("/payfast/itn", async (req, res) => {
  try {
    const body = req.body || {};

    const merchantId = process.env.PAYFAST_MERCHANT_ID;
    const passphrase = process.env.PAYFAST_PASSPHRASE;

    if (!merchantId) return res.status(400).send("missing merchant");

    const { signature: incomingSig, ...rest } = body;
    const computedSig = generateSignature(rest, passphrase);
    if (!incomingSig || incomingSig !== computedSig) {
      console.warn("[payfast/itn] signature mismatch", { incomingSig, computedSig });
      return res.status(400).send("bad signature");
    }

    if (rest.merchant_id !== merchantId) {
      console.warn("[payfast/itn] merchant mismatch", rest.merchant_id);
      return res.status(400).send("bad merchant");
    }

    const paymentId = String(rest.m_payment_id || "").trim();
    const pfPaymentId = rest.pf_payment_id;
    const paymentStatus = rest.payment_status;
    const amount = Number(rest.amount);

    if (!paymentId) {
      console.warn("[payfast/itn] missing payment id");
      return res.status(400).send("bad payment id");
    }

    // PayFast sends back whatever we provided as m_payment_id.
    // In our system it can be either the intent UUID OR a human-readable string like CP-XXXX.
    let intent = null;
    if (UUID_REGEX.test(paymentId)) {
      intent = await db.oneOrNone(
        "select id, church_id, fund_id, amount, amount_gross, platform_fee_amount, platform_fee_pct, platform_fee_fixed, superadmin_cut_amount, superadmin_cut_pct, status, channel from payment_intents where id=$1 or m_payment_id=$1",
        [paymentId]
      );
    } else {
      intent = await db.oneOrNone(
        "select id, church_id, fund_id, amount, amount_gross, platform_fee_amount, platform_fee_pct, platform_fee_fixed, superadmin_cut_amount, superadmin_cut_pct, status, channel from payment_intents where m_payment_id=$1",
        [paymentId]
      );
    }
    if (!intent) {
      console.warn("[payfast/itn] intent not found", paymentId);
      return res.status(404).send("not found");
    }

    const expectedAmount = Number(intent.amount_gross ?? intent.amount);
    if (expectedAmount.toFixed(2) !== amount.toFixed(2)) {
      console.warn("[payfast/itn] amount mismatch", expectedAmount, amount);
      return res.status(400).send("amount mismatch");
    }

    if (paymentStatus !== "COMPLETE") {
      console.warn("[payfast/itn] status not complete", paymentStatus);
      return res.status(400).send("status not complete");
    }

    await db.tx(async (t) => {
      await t.none(
        "update payment_intents set status='PAID', provider='payfast', provider_payment_id=$2, updated_at=now() where id=$1",
        [intent.id, pfPaymentId]
      );

      await t.none(
        `insert into transactions (
          church_id, fund_id, payment_intent_id, amount, platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct, reference, channel, provider, provider_payment_id, created_at
        ) values (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'payfast', $13, now()
        )`,
        [
          intent.church_id,
          intent.fund_id,
          intent.id,
          intent.amount,
          intent.platform_fee_amount || 0,
          intent.platform_fee_pct || readFeeConfig().pct,
          intent.platform_fee_fixed || readFeeConfig().fixed,
          intent.amount_gross || intent.amount,
          intent.superadmin_cut_amount || 0,
          intent.superadmin_cut_pct || readFeeConfig().superPct,
          rest.item_name || paymentId,
          intent.channel || "payfast",
          pfPaymentId,
        ]
      );
    });

    return res.status(200).send("OK");
  } catch (err) {
    console.error("[payfast/itn] error", err?.message || err, err?.stack);
    res.status(200).send("OK");
  }
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
      await ensurePaymentIntentsTable();
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
            (church_id, fund_id, amount, currency, member_name, member_phone, status, provider, provider_payment_id, m_payment_id, item_name, platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct, created_at, updated_at)
          values
            ($1,$2,$3,'ZAR',$4,$5,'PAID','simulated',$6,$7,$8,$9,$10,$11,$12,$13,$14,now(),now())
          returning *
          `,
          [
            churchId,
            fundId,
            pricing.amount,
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
            (church_id, fund_id, payment_intent_id, amount, platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct, reference, channel, provider, provider_payment_id, created_at)
          values
            ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'simulated',$13,now())
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
        coalesce(t.amount_gross, t.amount)::numeric(12,2) as "amountGross",
        coalesce(t.superadmin_cut_amount,0)::numeric(12,2) as "superadminCutAmount",
        t.channel,
        t.provider,
        t.provider_payment_id as "providerPaymentId",
        coalesce(pi.status, case when t.provider in ('payfast','simulated','manual') then 'PAID' when t.provider is null then 'PAID' else upper(t.provider) end) as status,
        t.created_at as "createdAt",
        pi.member_name as "memberName",
        pi.member_phone as "memberPhone",
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
        coalesce(t.amount_gross, t.amount)::numeric(12,2) as "amountGross",
        coalesce(t.superadmin_cut_amount,0)::numeric(12,2) as "superadminCutAmount",
        t.channel,
        t.provider,
        coalesce(pi.status, case when t.provider in ('payfast','simulated','manual') then 'PAID' when t.provider is null then 'PAID' else upper(t.provider) end) as status,
        t.created_at as "createdAt",
        pi.member_name as "memberName",
        pi.member_phone as "memberPhone",
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

export default router;
