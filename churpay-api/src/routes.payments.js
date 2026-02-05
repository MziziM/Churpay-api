import express from "express";
import { buildPayfastRedirect, generateSignature } from "./payfast.js";
import { db } from "./db.js";
import crypto from "node:crypto";

const router = express.Router();

function makeMpaymentId() {
  return "CP-" + crypto.randomBytes(8).toString("hex").toUpperCase();
}

const UUID_REGEX = /^[0-9a-fA-F-]{36}$/;

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
      item_name text
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
      add column if not exists member_phone text;
    create index if not exists idx_payment_intents_church on payment_intents (church_id);
    create index if not exists idx_payment_intents_fund on payment_intents (fund_id);
  `);
}

function normalizeBaseUrl() {
  const base = process.env.PUBLIC_BASE_URL || process.env.BASE_URL;
  if (!base) return null;
  return base.endsWith("/") ? base.slice(0, -1) : base;
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


router.post("/payment-intents", async (req, res) => {
  try {
    let { churchId, fundId, amount, memberName, memberPhone, channel = "app" } = req.body || {};
    await ensurePaymentIntentsTable();

    const amt = Number(amount);
    if (!Number.isFinite(amt) || amt <= 0) {
      return res.status(400).json({ error: "Invalid amount" });
    }

    churchId = typeof churchId === "string" ? churchId.trim() : churchId;
    fundId = typeof fundId === "string" ? fundId.trim() : fundId;

    if (!churchId || !fundId) {
      return res.status(400).json({ error: "Missing churchId/fundId/amount" });
    }

    channel = typeof channel === "string" ? channel.trim() : channel;

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
             id, church_id, fund_id, amount, currency, status, member_name, member_phone, channel, provider, provider_payment_id, m_payment_id, item_name, created_at, updated_at
           ) values (
             $1,$2,$3,$4,'ZAR','PENDING',$5,$6,$7,'manual',null,$8,$9,now(),now()
           ) returning id`,
          [intentId, churchId, fundId, amt, memberName || "", memberPhone || "", channel || "manual", reference, itemName]
        );

        const txRow = await db.one(
          `insert into transactions (
            church_id, fund_id, payment_intent_id, amount, reference, channel, provider, provider_payment_id, created_at
          ) values (
            $1,$2,$3,$4,$5,$6,'manual',null,now()
          ) returning id, reference, created_at`,
          [churchId, fundId, intent.id, amt, reference, channel || "manual"]
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

// ==========================
// PayFast: initiate payment
// ==========================
router.post("/payfast/initiate", async (req, res) => {
  try {
    let { churchId, fundId, amount, memberName, memberPhone, channel = "app" } = req.body || {};

    const baseUrl = normalizeBaseUrl();
    if (!baseUrl) return res.status(500).json({ error: "Server misconfigured: BASE_URL missing" });

    const amt = Number(amount);
    if (!Number.isFinite(amt) || amt <= 0) return res.status(400).json({ error: "Invalid amount" });

    churchId = typeof churchId === "string" ? churchId.trim() : churchId;
    fundId = typeof fundId === "string" ? fundId.trim() : fundId;
    memberName = typeof memberName === "string" ? memberName.trim() : memberName;
    memberPhone = typeof memberPhone === "string" ? memberPhone.trim() : memberPhone;
    channel = typeof channel === "string" ? channel.trim() : channel;

    if (!churchId || !UUID_REGEX.test(churchId)) return res.status(400).json({ error: "Invalid churchId" });
    if (!fundId || !UUID_REGEX.test(fundId)) return res.status(400).json({ error: "Invalid fundId" });

    const fund = await db.oneOrNone("select id, name from funds where id=$1 and church_id=$2", [fundId, churchId]);
    if (!fund) return res.status(404).json({ error: "Fund not found" });

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
         church_id, fund_id, amount, status, member_name, member_phone, channel, provider, m_payment_id, item_name
       ) values (
         $1, $2, $3, 'PENDING', $4, $5, $6, 'payfast', gen_random_uuid(), $7
       ) returning id, amount, church_id, fund_id, m_payment_id, item_name`,
      [churchId, fundId, amt, memberName || null, memberPhone || null, channel || null, itemName]
    );

    const mode = (process.env.PAYFAST_MODE || "sandbox").toLowerCase() === "live" ? "live" : "sandbox";
    const merchantId = process.env.PAYFAST_MERCHANT_ID;
    const merchantKey = process.env.PAYFAST_MERCHANT_KEY;
    const passphrase = process.env.PAYFAST_PASSPHRASE;

    if (!merchantId || !merchantKey) {
      return res.status(500).json({ error: "Server misconfigured: PayFast merchant keys missing" });
    }

    const returnUrl = `${baseUrl}/give?success=true`;
    const cancelUrl = `${baseUrl}/give?cancelled=true`;
    const notifyUrl = `${baseUrl}/api/payfast/itn`;

    const paymentUrl = buildPayfastRedirect({
      mode,
      merchantId,
      merchantKey,
      passphrase,
      mPaymentId: intent.m_payment_id || intent.id,
      amount: amt,
      itemName: intent.item_name || fund.name,
      returnUrl,
      cancelUrl,
      notifyUrl,
      customStr1: churchId,
      customStr2: fundId,
      nameFirst: memberName,
      emailAddress: undefined,
    });

    return res.json({ paymentUrl, id: intent.id });
  } catch (err) {
    console.error("[payfast/initiate] error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
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
        "select id, church_id, fund_id, amount, status, channel from payment_intents where id=$1 or m_payment_id=$1",
        [paymentId]
      );
    } else {
      intent = await db.oneOrNone(
        "select id, church_id, fund_id, amount, status, channel from payment_intents where m_payment_id=$1",
        [paymentId]
      );
    }
    if (!intent) {
      console.warn("[payfast/itn] intent not found", paymentId);
      return res.status(404).send("not found");
    }

    if (Number(intent.amount).toFixed(2) !== amount.toFixed(2)) {
      console.warn("[payfast/itn] amount mismatch", intent.amount, amount);
      return res.status(400).send("amount mismatch");
    }

    if (paymentStatus !== "COMPLETE") {
      console.warn("[payfast/itn] status not complete", paymentStatus);
      return res.status(400).send("status not complete");
    }

    await db.tx(async (t) => {
      await t.none(
        "update payment_intents set status='PAID', provider_payment_id=$2, updated_at=now() where id=$1",
        [intent.id, pfPaymentId]
      );

      await t.none(
        `insert into transactions (
          church_id, fund_id, payment_intent_id, amount, reference, channel, provider, provider_payment_id, created_at
        ) values (
          $1, $2, $3, $4, $5, $6, 'payfast', $7, now()
        )`,
        [
          intent.church_id,
          intent.fund_id,
          intent.id,
          intent.amount,
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