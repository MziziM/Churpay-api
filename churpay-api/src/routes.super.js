import express from "express";
import crypto from "node:crypto";
import { signSuperToken, requireSuperAdmin } from "./auth.js";
import { db } from "./db.js";

const router = express.Router();

const SUPER_EMAIL = (process.env.SUPER_ADMIN_EMAIL || "").toLowerCase().trim();
const SUPER_PASS = process.env.SUPER_ADMIN_PASSWORD || "";

function parseDate(value, endOfDay = false) {
  if (!value) return null;
  const suffix = endOfDay ? "T23:59:59.999Z" : "T00:00:00.000Z";
  const parsed = new Date(`${value}${suffix}`);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function csvEscape(value) {
  if (value === null || typeof value === "undefined") return "";
  const text = String(value);
  if (/[",\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

function joinCodePrefixFromChurchName(name) {
  const raw = String(name || "")
    .normalize("NFKD")
    .replace(/[^\x20-\x7E]/g, "")
    .replace(/[^A-Za-z0-9\s]/g, " ")
    .trim();
  const words = raw.split(/\s+/).filter(Boolean);
  if (!words.length) return "CH";

  let prefix = words.map((w) => w[0]).join("").toUpperCase();
  if (!prefix) prefix = "CH";
  if (prefix.length < 2) prefix = (words[0] || "CH").slice(0, 2).toUpperCase();
  return prefix.slice(0, 8);
}

function generateJoinCode(churchName) {
  const prefix = joinCodePrefixFromChurchName(churchName);
  const suffix = String(crypto.randomInt(0, 10000)).padStart(4, "0");
  return `${prefix}-${suffix}`;
}

async function ensureUniqueJoinCode(name, requestedJoinCode) {
  const candidates = [];
  if (requestedJoinCode) candidates.push(String(requestedJoinCode).trim().toUpperCase());
  candidates.push(generateJoinCode(name));
  candidates.push(generateJoinCode(name));
  candidates.push(generateJoinCode(name));

  const seen = new Set();
  for (const code of candidates) {
    if (!code || seen.has(code)) continue;
    seen.add(code);
    const exists = await db.oneOrNone("select id from churches where upper(join_code)=upper($1)", [code]);
    if (!exists) return code;
  }
  throw new Error("Unable to generate unique join code");
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
      `(coalesce(t.reference, '') ilike $${idx} or coalesce(pi.member_name, '') ilike $${idx} or coalesce(pi.member_phone, '') ilike $${idx} or coalesce(c.name, '') ilike $${idx} or coalesce(f.name, '') ilike $${idx})`
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
    const identifier = String(req.body?.identifier || req.body?.email || "").toLowerCase().trim();
    const password = String(req.body?.password || "");
    if (!identifier || !password) return res.status(400).json({ error: "Missing credentials" });
    if (!SUPER_EMAIL || !SUPER_PASS) return res.status(500).json({ error: "Super admin not configured" });

    const matchEmail = identifier === SUPER_EMAIL;
    const matchPass = password === SUPER_PASS;
    if (!matchEmail || !matchPass) return res.status(401).json({ error: "Invalid credentials" });

    const token = signSuperToken(SUPER_EMAIL);
    return res.json({
      ok: true,
      token,
      profile: { role: "super", email: SUPER_EMAIL, fullName: "Super Admin" },
    });
  } catch (err) {
    console.error("[super/login] error", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/me", requireSuperAdmin, (_req, res) => {
  return res.json({
    profile: {
      role: "super",
      email: SUPER_EMAIL || "super@churpay.com",
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
        coalesce(sum(coalesce(t.platform_fee_amount,0)), 0)::numeric(12,2) as fee_total,
        coalesce(sum(coalesce(t.superadmin_cut_amount,0)), 0)::numeric(12,2) as superadmin_cut_total,
        count(*)::int as total_transactions,
        count(distinct nullif(coalesce(pi.member_phone, pi.member_name), ''))::int as total_donors,
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
        pi.member_name as "memberName",
        pi.member_phone as "memberPhone"
      from transactions t
      join churches c on c.id = t.church_id
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
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
        totalFeesCollected: Number(totalRow.fee_total || 0).toFixed(2),
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

    const joinCode = await ensureUniqueJoinCode(name, requestedJoinCode || null);
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

router.get("/churches/:churchId", requireSuperAdmin, async (req, res) => {
  try {
    const churchId = String(req.params.churchId || "").trim();
    if (!churchId) return res.status(400).json({ error: "Church ID is required" });
    const hasActive = await hasColumn("churches", "active");

    const church = await db.oneOrNone(
      `
      select
        c.id,
        c.name,
        c.join_code as "joinCode",
        ${hasActive ? "coalesce(c.active, true)" : "true"} as active,
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
        pi.member_name as "memberName",
        pi.member_phone as "memberPhone"
      from transactions t
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
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
      where m.church_id = $1 and lower(m.role) in ('admin', 'super')
      order by m.created_at desc
      `,
      [churchId]
    );

    res.json({ church, funds, transactions, admins });
  } catch (err) {
    console.error("[super/church] detail error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
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
        t.created_at as "createdAt",
        c.id as "churchId",
        c.name as "churchName",
        f.id as "fundId",
        f.code as "fundCode",
        f.name as "fundName",
        pi.member_name as "memberName",
        pi.member_phone as "memberPhone"
      from transactions t
      join churches c on c.id = t.church_id
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
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
        coalesce(t.amount_gross, t.amount)::numeric(12,2) as "amountGross",
        coalesce(t.superadmin_cut_amount,0)::numeric(12,2) as "superadminCutAmount",
        t.channel,
        t.provider,
        coalesce(pi.status, case when t.provider in ('payfast','simulated','manual') then 'PAID' when t.provider is null then 'PAID' else upper(t.provider) end) as status,
        t.created_at as "createdAt",
        c.name as "churchName",
        f.code as "fundCode",
        f.name as "fundName",
        pi.member_name as "memberName",
        pi.member_phone as "memberPhone"
      from transactions t
      join churches c on c.id = t.church_id
      join funds f on f.id = t.fund_id
      left join payment_intents pi on pi.id = t.payment_intent_id
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
    ];
    const lines = [header.join(",")];
    for (const row of rows) {
      lines.push(
        [
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
          csvEscape(row.churchName),
          csvEscape(row.fundCode),
          csvEscape(row.fundName),
          csvEscape(row.memberName),
          csvEscape(row.memberPhone),
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

    const church = await db.oneOrNone(`select id, name from churches where id = $1`, [churchId]);
    if (!church) return res.status(404).json({ error: "Church not found" });
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
      fundId: fund.id,
      fundCode: fund.code,
    };
    if (amount !== null) payload.amount = amount;
    const qrValue = JSON.stringify(payload);
    const deepLink = `churpaydemo://donate?churchId=${encodeURIComponent(church.id)}&fundId=${encodeURIComponent(fund.id)}&fundCode=${encodeURIComponent(fund.code)}${amount !== null ? `&amount=${encodeURIComponent(amount)}` : ""}`;
    const baseUrl = process.env.PUBLIC_BASE_URL || "https://api.churpay.com";
    const webLink = `${baseUrl.replace(/\/$/, "")}/give?churchId=${encodeURIComponent(church.id)}&fundId=${encodeURIComponent(fund.id)}&fundCode=${encodeURIComponent(fund.code)}${amount !== null ? `&amount=${encodeURIComponent(amount)}` : ""}`;

    res.json({
      qr: { value: qrValue, payload },
      qrPayload: payload,
      deepLink,
      webLink,
      church: { id: church.id, name: church.name },
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
        m.role,
        m.church_id as "churchId",
        c.name as "churchName",
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
