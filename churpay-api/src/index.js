import "dotenv/config";
import express from "express";
import cors from "cors";
import payments from "./routes.payments.js";
import webhooks from "./routes.webhooks.js";
import { db } from "./db.js";

const app = express();
app.use(cors());
app.use(express.json());

app.get("/health", (_, res) => res.json({ ok: true }));

app.use("/api", payments);
app.use("/webhooks", webhooks);
// GET /api/churches/:id/transactions
app.get("/api/churches/:id/transactions", async (req, res) => {
  try {
    const churchId = req.params.id;

    const limit = Math.min(Number(req.query.limit || 50), 200);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const fundId = req.query.fundId || null;
    const channel = req.query.channel || null;

    // from/to are optional YYYY-MM-DD
    const from = req.query.from
      ? new Date(req.query.from + "T00:00:00.000Z")
      : null;
    const to = req.query.to
      ? new Date(req.query.to + "T23:59:59.999Z")
      : null;

    const where = [`t.church_id = $1`];
    const params = [churchId];
    let i = params.length;

    if (fundId) {
      params.push(fundId);
      i++;
      where.push(`t.fund_id = $${i}`);
    }

    if (channel) {
      params.push(channel);
      i++;
      where.push(`t.channel = $${i}`);
    }

    if (from && !Number.isNaN(from.getTime())) {
      params.push(from);
      i++;
      where.push(`t.created_at >= $${i}`);
    }

    if (to && !Number.isNaN(to.getTime())) {
      params.push(to);
      i++;
      where.push(`t.created_at <= $${i}`);
    }

    // pagination
    params.push(limit);
    const limitIdx = params.length;
    params.push(offset);
    const offsetIdx = params.length;

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
      limit $${limitIdx} offset $${offsetIdx};
    `;

    const { rows } = await db.query(sql, params);

    res.json({
      transactions: rows,
      meta: { limit, offset, count: rows.length },
    });
  } catch (err) {
    console.error("[transactions] GET error", err);
    res.status(500).json({ error: "Failed to load transactions" });
  }
});
const port = process.env.PORT || 3001;
app.listen(port, "0.0.0.0", () => {
  console.log(`Churpay API running on 0.0.0.0:${port}`);
});