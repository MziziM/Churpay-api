import express from "express";
import { db } from "./db.js";
import { generateSignature } from "./payfast.js";

const router = express.Router();

// PayFast sends application/x-www-form-urlencoded POST (ITN)
// Debug helper: PayFast will POST ITN, but this helps confirm the route is mounted.
router.get("/payfast/itn", (req, res) => {
  res.status(200).json({ ok: true, route: "webhooks/payfast/itn" });
});
router.post(
  "/payfast/itn",
  express.urlencoded({ extended: false }),
  async (req, res) => {
    try {
      // PayFast passphrase may legitimately be empty (especially in sandbox).
      // Treat only `undefined` as misconfiguration.
      if (process.env.PAYFAST_PASSPHRASE === undefined) {
        console.error("[itn] PAYFAST_PASSPHRASE env var is not set (can be empty string)");
        return res.status(500).send("server misconfigured");
      }
      
console.log("[itn] hit", {
  m_payment_id: req.body?.m_payment_id,
  payment_status: req.body?.payment_status,
  pf_payment_id: req.body?.pf_payment_id,
});
      const data = { ...req.body };

      const receivedSig = data.signature;
      if (!receivedSig) return res.status(400).send("missing signature");
      delete data.signature;

      const expectedSig = generateSignature(data, process.env.PAYFAST_PASSPHRASE);
      if (receivedSig !== expectedSig) {
        console.warn("[itn] invalid signature", { m_payment_id: data.m_payment_id });
        return res.status(400).send("invalid signature");
      }

      const mPaymentId = String(data.m_payment_id || "").trim();
      if (!mPaymentId) return res.status(400).send("missing m_payment_id");

      const intent = await db.oneOrNone(
        "select * from payment_intents where m_payment_id=$1",
        [mPaymentId]
      );
      if (!intent) return res.status(404).send("unknown m_payment_id");

      const grossRaw = data.amount_gross ?? data.amount ?? "0";
      const gross = Number(grossRaw);
      const expected = Number(intent.amount);

      if (!Number.isFinite(gross) || !Number.isFinite(expected)) {
        console.warn("[itn] invalid amounts", { grossRaw, expected });
        return res.status(400).send("invalid amount");
      }

      if (Number(gross.toFixed(2)) !== Number(expected.toFixed(2))) {
        console.warn("[itn] amount mismatch", { m_payment_id: mPaymentId, gross, expected });
        return res.status(400).send("amount mismatch");
      }

      const status = String(data.payment_status || "").toUpperCase();
      const pfPaymentId = (data.pf_payment_id && String(data.pf_payment_id)) || null;

      if (status === "COMPLETE") {
        await db.tx(async (t) => {
          await t.none(
            "update payment_intents set status='PAID', provider_payment_id=$2, updated_at=now() where id=$1 and status <> 'PAID'",
            [intent.id, pfPaymentId]
          );

          const existing = await t.oneOrNone(
            "select id from transactions where payment_intent_id=$1 limit 1",
            [intent.id]
          );

          if (!existing) {
            await t.none(
              `insert into transactions (
                 church_id,
                 fund_id,
                 payment_intent_id,
                 amount,
                 reference,
                 channel,
                 provider,
                 provider_payment_id
               ) values ($1,$2,$3,$4,$5,$6,$7,$8)`,
              [
                intent.church_id,
                intent.fund_id,
                intent.id,
                intent.amount,
                intent.m_payment_id,
                "app",
                "payfast",
                pfPaymentId,
              ]
            );
          }
        });

        return res.status(200).send("OK");
      }

      if (status === "FAILED") {
        await db.none(
          "update payment_intents set status='FAILED', provider_payment_id=$2, updated_at=now() where id=$1 and status <> 'PAID'",
          [intent.id, pfPaymentId]
        );
        return res.status(200).send("OK");
      }

      if (status === "CANCELLED") {
        await db.none(
          "update payment_intents set status='CANCELLED', provider_payment_id=$2, updated_at=now() where id=$1 and status <> 'PAID'",
          [intent.id, pfPaymentId]
        );
        return res.status(200).send("OK");
      }

      return res.status(200).send("OK");
    } catch (err) {
      console.error("[itn] server error", err);
      return res.status(500).send("server error");
    }
  }
);

export default router;
