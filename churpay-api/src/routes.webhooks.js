import express from "express";
import crypto from "node:crypto";
import { db } from "./db.js";

const router = express.Router();

// PayFast sends application/x-www-form-urlencoded POST (ITN)
// Debug helper: PayFast will POST ITN, but this helps confirm the route is mounted.
router.get("/payfast/itn", (req, res) => {
  res.status(200).json({ ok: true, route: "webhooks/payfast/itn" });
});

// Route-specific raw body capture to rebuild the signature exactly as PayFast expects.
router.post(
  "/payfast/itn",
  // Accept charset variants; ensure raw body available for signature.
  express.raw({
    type: (req) =>
      (req.headers["content-type"] || "")
        .toLowerCase()
        .startsWith("application/x-www-form-urlencoded"),
  }),
  async (req, res) => {
    try {
      const debug = String(process.env.PAYFAST_DEBUG || "").toLowerCase() === "1";
      const passphrase = process.env.PAYFAST_PASSPHRASE || "";

      const rawBody =
        typeof req.rawBody === "string"
          ? req.rawBody
          : Buffer.isBuffer(req.body)
          ? req.body.toString("utf8")
          : typeof req.body === "string"
          ? req.body
          : "";

      if (debug) {
        console.log("[itn] raw body", {
          raw: rawBody.slice(0, 2000),
          contentType: req.headers["content-type"],
          length: rawBody.length,
        });
      }

      // Decode values for business logic
      const parseForm = (raw) => Object.fromEntries(new URLSearchParams(raw));
      const parsed = parseForm(rawBody);
      const params = parsed; // ensure params is always defined for downstream logic

      // Rebuild signature using raw pairs (no decode/re-encode)
      const rawPairs = rawBody
        .split("&")
        .filter(Boolean)
        .map((pair) => {
          const idx = pair.indexOf("=");
          return idx === -1
            ? [pair, ""]
            : [pair.slice(0, idx), pair.slice(idx + 1)];
        });

      const receivedSig = params.signature;
      if (!receivedSig) return res.status(400).send("missing signature");

      const sigPairs = rawPairs.filter(([key]) => key !== "signature");

      if (debug) {
        console.log("[itn] parsed keys", sigPairs.map(([k]) => k));
      }

      sigPairs.sort((a, b) => a[0].localeCompare(b[0]));

      let sigBase = sigPairs.map(([k, v]) => `${k}=${v}`).join("&");
      if (passphrase) {
        const encodePF = (v) => encodeURIComponent(v).replace(/%20/g, "+");
        sigBase += `&passphrase=${encodePF(passphrase)}`;
      }

      const computedSig = crypto.createHash("md5").update(sigBase).digest("hex");
      const match = computedSig.toLowerCase() === String(receivedSig).toLowerCase();

      if (debug) {
        const maskedBase = passphrase
          ? sigBase.replace(
              encodeURIComponent(passphrase).replace(/%20/g, "+"),
              "***"
            )
          : sigBase;
        console.log("[itn] sig debug", {
          submitted: receivedSig,
          computed: computedSig,
          base: maskedBase,
        });
      }

      if (!match) {
        console.warn("[itn] invalid signature", { m_payment_id: params.m_payment_id });
        return res.status(400).send("invalid signature");
      }

      const mPaymentId = String(params.m_payment_id || "").trim();
      if (!mPaymentId) return res.status(400).send("missing m_payment_id");

      const intent = await db.oneOrNone(
        "select * from payment_intents where m_payment_id=$1",
        [mPaymentId]
      );
      if (!intent) return res.status(404).send("unknown m_payment_id");

      const grossRaw = params.amount_gross ?? params.amount ?? "0";
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

      const status = String(params.payment_status || "").toUpperCase();
      const pfPaymentId = (params.pf_payment_id && String(params.pf_payment_id)) || null;

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
