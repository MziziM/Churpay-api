import express from "express";
import { buildPayfastRedirect } from "./payfast.js";
import { db } from "./db.js";
import crypto from "node:crypto";

const router = express.Router();

function makeMpaymentId() {
  return "CP-" + crypto.randomBytes(8).toString("hex").toUpperCase();
}
router.get("/churches/:churchId/funds", async (req, res) => {
  try {
    const { churchId } = req.params;
    const funds = await db.manyOrNone(
      "select id, code, name, active from funds where church_id=$1 and active=true order by name asc",
      [churchId]
    );
    res.json({ funds });
  } catch (err) {
    console.error("[funds] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Get transactions for a church with filters
router.get("/churches/:churchId/transactions", async (req, res) => {
  try {
    const { churchId } = req.params;

    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const fundId = req.query.fundId || null;
    const channel = req.query.channel || null;
    const from = req.query.from ? new Date(req.query.from + "T00:00:00.000Z") : null;
    const to = req.query.to ? new Date(req.query.to + "T23:59:59.999Z") : null;

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
        t.channel,
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

    res.json({
      transactions: rows,
      meta: { limit, offset, count: rows.length },
    });
  } catch (err) {
    console.error("[transactions] GET error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Failed to load transactions" });
  }
});

// Update fund (rename / toggle active)
router.patch("/funds/:id", async (req, res) => {
  try {
    const { id } = req.params;
    let { churchId, name, active } = req.body || {};

    churchId = typeof churchId === "string" ? churchId.trim() : churchId;

    if (!churchId) return res.status(400).json({ error: "Missing churchId" });

    // normalize active if provided
    if (typeof active === "string") {
      active = active === "true" || active === "1";
    } else if (typeof active === "number") {
      active = !!active;
    }

    const existing = await db.oneOrNone("select id from funds where id=$1 and church_id=$2", [id, churchId]);
    if (!existing) return res.status(404).json({ error: "Fund not found" });

    const updated = await db.one(
      `update funds set name = coalesce($1, name), active = coalesce($2, active) where id=$3 and church_id=$4 returning id, code, name, active`,
      [typeof name === "string" ? name : null, typeof active === "undefined" ? null : active, id, churchId]
    );

    res.json({ fund: updated });
  } catch (err) {
    console.error("[funds] PATCH /funds/:id error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Create a new fund
router.post("/funds", async (req, res) => {
  try {
    let { churchId, code, name, active = true } = req.body || {};

    churchId = typeof churchId === "string" ? churchId.trim() : churchId;
    name = typeof name === "string" ? name.trim() : name;

    if (!churchId || !name) {
      return res.status(400).json({ error: "Missing churchId or name" });
    }

    // generate a simple code if not provided
    if (!code || typeof code !== "string" || !code.trim()) {
      code = name
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/(^-|-$)/g, "")
        .slice(0, 40);
    }

    // validate churchId looks like a UUID to avoid DB type errors
    if (typeof churchId !== "string" || !/^[0-9a-fA-F-]{36}$/.test(churchId)) {
      return res.status(400).json({ error: "Invalid churchId" });
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

    // ensure uniqueness for this church
    const existing = await db.oneOrNone("select id from funds where church_id=$1 and code=$2", [churchId, code]);
    if (existing) {
      return res.status(409).json({ error: "Fund code already exists" });
    }

    const row = await db.one(
      `insert into funds (church_id, code, name, active) values ($1,$2,$3,$4) returning id, code, name, active`,
      [churchId, code, name, !!active]
    );

    res.json({ fund: row });
  } catch (err) {
    console.error("[funds] POST /funds error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Latest transactions for member/admin history
router.get("/churches/:churchId/transactions", async (req, res) => {
  try {
    const { churchId } = req.params;
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);

    const rows = await db.manyOrNone(
      `
      select
        t.id,
        t.amount,
        t.reference,
        t.channel,
        t.provider,
        t.provider_payment_id,
        t.created_at,
        f.id as fund_id,
        f.code as fund_code,
        f.name as fund_name
      from transactions t
      join funds f on f.id = t.fund_id
      where t.church_id = $1
      order by t.created_at desc
      limit $2
      `,
      [churchId, limit]
    );

    res.json({ transactions: rows });
  } catch (err) {
    console.error("[transactions] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/payment-intents", async (req, res) => {
  try {
    let { churchId, fundId, amount, memberName, memberPhone, channel = "app" } = req.body || {};

    if (!process.env.PUBLIC_BASE_URL) {
      return res.status(500).json({ error: "Server misconfigured: PUBLIC_BASE_URL missing" });
    }

    const amt = Number(amount);
    if (!Number.isFinite(amt) || amt <= 0) {
      return res.status(400).json({ error: "Invalid amount" });
    }

    churchId = typeof churchId === "string" ? churchId.trim() : churchId;
    fundId = typeof fundId === "string" ? fundId.trim() : fundId;

    if (!churchId || !fundId) {
      return res.status(400).json({ error: "Missing churchId/fundId/amount" });
    }

    let fund, church;
    try {
      fund = await db.one("select id, name from funds where id=$1 and church_id=$2", [fundId, churchId]);
      church = await db.one("select id, name from churches where id=$1", [churchId]);
    } catch (err) {
      if (err.message.includes("Expected 1 row, got 0")) {
        return res.status(404).json({ error: "Church or fund not found" });
      }
      console.error("[payments] DB error fetching church/fund", err);
      return res.status(500).json({ error: "Internal server error" });
    }

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

    const intent = await db.one(`
      insert into payment_intents
        (church_id, fund_id, amount, member_name, member_phone, item_name, m_payment_id)
      values ($1,$2,$3,$4,$5,$6,$7)
      returning *
    `, [churchId, fundId, amt, memberName || "", memberPhone || "", itemName, mPaymentId]);

    const returnUrl = `${process.env.PUBLIC_BASE_URL}/payfast/return?pi=${intent.id}`;
    const cancelUrl = `${process.env.PUBLIC_BASE_URL}/payfast/cancel?pi=${intent.id}`;
    const notifyUrl = `${process.env.PUBLIC_BASE_URL}/webhooks/payfast/itn`;

    const checkoutUrl = buildPayfastRedirect({
      mode: process.env.PAYFAST_MODE,
      merchantId: process.env.PAYFAST_MERCHANT_ID,
      merchantKey: process.env.PAYFAST_MERCHANT_KEY,
      passphrase: process.env.PAYFAST_PASSPHRASE,
      mPaymentId,
      amount: intent.amount,
      itemName,
      returnUrl,
      cancelUrl,
      notifyUrl,
      customStr1: churchId,
      customStr2: fundId,
    });

    res.json({ paymentIntentId: intent.id, checkoutUrl });
  } catch (err) {
    console.error("[payments] POST /payment-intents error", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/payment-intents/:id", async (req, res) => {
  try {
    const pi = await db.one("select * from payment_intents where id=$1", [req.params.id]);
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
router.post("/simulate-payment", async (req, res) => {
  try {
    let { churchId, fundId, amount, memberName, memberPhone, channel = "app" } = req.body || {};

    const amt = Number(amount);
    if (!Number.isFinite(amt) || amt <= 0) {
      return res.status(400).json({ error: "Invalid amount" });
    }

    churchId = typeof churchId === "string" ? churchId.trim() : churchId;
    fundId = typeof fundId === "string" ? fundId.trim() : fundId;

    if (!churchId || !fundId) {
      return res.status(400).json({ error: "Missing churchId/fundId/amount" });
    }

    // Validate church + fund exist
    let fund, church;
    try {
      fund = await db.one("select id, name from funds where id=$1 and church_id=$2", [fundId, churchId]);
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
          (church_id, fund_id, amount, currency, member_name, member_phone, status, provider, provider_payment_id, m_payment_id, item_name, created_at, updated_at)
        values
          ($1,$2,$3,'ZAR',$4,$5,'PAID','simulated',$6,$7,$8,now(),now())
        returning *
        `,
        [churchId, fundId, amt, memberName || "", memberPhone || "", providerPaymentId, mPaymentId, itemName]
      );

      // Insert ledger transaction row
      const txRow = await t.one(
        `
        insert into transactions
          (church_id, fund_id, payment_intent_id, amount, reference, channel, provider, provider_payment_id, created_at)
        values
          ($1,$2,$3,$4,$5,$6,'simulated',$7,now())
        returning *
        `,
        [churchId, fundId, intent.id, intent.amount, intent.m_payment_id, channel || "app", providerPaymentId]
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
router.get("/churches/:churchId/totals", async (req, res) => {
  try {
    const { churchId } = req.params;

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

export default router;