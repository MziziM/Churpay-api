import express from "express";
import crypto from "node:crypto";
import { db } from "./db.js";

const router = express.Router();
const BUILD = "v2026-02-05-1105";
const DEFAULT_PLATFORM_FEE_FIXED = 2.5;
const DEFAULT_PLATFORM_FEE_PCT = 0.0075;
const DEFAULT_SUPERADMIN_CUT_PCT = 1.0;

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

// PayFast sends application/x-www-form-urlencoded POST (ITN)
// Debug helper: PayFast will POST ITN, but this helps confirm the route is mounted.
router.get("/build", (req, res) => {
  res.json({ ok: true, build: BUILD });
});
router.get("/payfast/itn", (req, res) => {
  res.status(200).json({ ok: true, route: "webhooks/payfast/itn", build: BUILD });
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

      // Decode values for business logic (always define params)
      const parseForm = (raw) => Object.fromEntries(new URLSearchParams(raw));
      const params = parseForm(rawBody);

      // Always log build marker and passphrase presence to confirm deployed code + env
      console.log("[itn] build marker", BUILD, {
        debug,
        passphrasePresent: Boolean(passphrase),
      });

      const receivedSig = String(params.signature || "").trim();
      if (!receivedSig) return res.status(400).send("missing signature");

      // Build signature base exactly as PayFast sent it (raw form body):
      // remove signature=, keep order/encoding, append passphrase only if present.
      const parts = rawBody.split("&").filter(Boolean);
      const unsignedParts = parts.filter((p) => !p.startsWith("signature="));
      let sigBase = unsignedParts.join("&");

      const trimmedPass = String(process.env.PAYFAST_PASSPHRASE || "").trim();
      if (trimmedPass) {
        const passEnc = encodeURIComponent(trimmedPass).replace(/%20/g, "+");
        sigBase += `&passphrase=${passEnc}`;
      }

      const computedSig = crypto.createHash("md5").update(sigBase).digest("hex");
      const match = computedSig.toLowerCase() === receivedSig.toLowerCase();

      const masked = trimmedPass
        ? sigBase.replace(encodeURIComponent(trimmedPass).replace(/%20/g, "+"), "***")
        : sigBase;

      if (debug) {
        console.log("[itn] sig debug", {
          submitted: receivedSig,
          computed: computedSig,
          base: masked,
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
      const expected = Number(intent.amount_gross ?? intent.amount);

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
            "update payment_intents set status='PAID', provider='payfast', provider_payment_id=$2, updated_at=now() where id=$1 and coalesce(status,'') <> 'PAID'",
            [intent.id, pfPaymentId]
          );

          const existing = await t.oneOrNone(
            "select id from transactions where payment_intent_id=$1 limit 1",
            [intent.id]
          );

          if (!existing) {
            const feeCfg = readFeeConfig();
            const platformFeeAmount = toCurrencyNumber(
              intent.platform_fee_amount ?? feeCfg.fixed + Number(intent.amount || 0) * feeCfg.pct
            );
            const amountGross = toCurrencyNumber(intent.amount_gross ?? Number(intent.amount || 0) + platformFeeAmount);
            const superadminCutPct = Number.isFinite(Number(intent.superadmin_cut_pct))
              ? Number(intent.superadmin_cut_pct)
              : feeCfg.superPct;
            const superadminCutAmount = toCurrencyNumber(
              intent.superadmin_cut_amount ?? platformFeeAmount * superadminCutPct
            );
            await t.none(
              `insert into transactions (
                 church_id,
                 fund_id,
                 payment_intent_id,
                 amount,
                 platform_fee_amount,
                 platform_fee_pct,
                 platform_fee_fixed,
                 amount_gross,
                 superadmin_cut_amount,
                 superadmin_cut_pct,
                 reference,
                 channel,
                 provider,
                 provider_payment_id
               ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
              [
                intent.church_id,
                intent.fund_id,
                intent.id,
                intent.amount,
                platformFeeAmount,
                Number(intent.platform_fee_pct ?? feeCfg.pct),
                Number(intent.platform_fee_fixed ?? feeCfg.fixed),
                amountGross,
                superadminCutAmount,
                superadminCutPct,
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
          "update payment_intents set status='FAILED', provider='payfast', provider_payment_id=$2, updated_at=now() where id=$1 and coalesce(status,'') <> 'PAID'",
          [intent.id, pfPaymentId]
        );
        return res.status(200).send("OK");
      }

      if (status === "CANCELLED") {
        await db.none(
          "update payment_intents set status='CANCELLED', provider='payfast', provider_payment_id=$2, updated_at=now() where id=$1 and coalesce(status,'') <> 'PAID'",
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
