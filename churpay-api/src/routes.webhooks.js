import express from "express";
import crypto from "node:crypto";
import { db } from "./db.js";
import { createNotification } from "./notifications.js";

const router = express.Router();
const BUILD = "v2026-02-05-1105";
const DEFAULT_PLATFORM_FEE_FIXED = 2.5;
const DEFAULT_PLATFORM_FEE_PCT = 0.0075;
const DEFAULT_SUPERADMIN_CUT_PCT = 1.0;
const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function toCurrencyNumber(value) {
  const rounded = Math.round(Number(value) * 100) / 100;
  return Number.isFinite(rounded) ? Number(rounded.toFixed(2)) : 0;
}

function makeRecurringMpaymentId() {
  return "SUB-" + crypto.randomBytes(8).toString("hex").toUpperCase();
}

function extractPayfastToken(params) {
  const candidates = [
    params?.token,
    params?.subscription_id,
    params?.subscriptionId,
    params?.subscription_token,
  ];
  for (const value of candidates) {
    const token = String(value || "").trim();
    if (token) return token;
  }
  return "";
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
export const payfastItnRawParser = express.raw({
  type: (req) =>
    (req.headers["content-type"] || "")
      .toLowerCase()
      .startsWith("application/x-www-form-urlencoded"),
});

export async function handlePayfastItn(req, res) {
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

    const rawMPaymentId = String(params.m_payment_id || "").trim();
    const status = String(params.payment_status || "").toUpperCase();
    const pfPaymentId = (params.pf_payment_id && String(params.pf_payment_id)) || null;
    const payfastToken = extractPayfastToken(params);
    const recurringIdFromCustom = String(params.custom_str3 || "").trim();
    if (!rawMPaymentId && !payfastToken && !UUID_REGEX.test(recurringIdFromCustom)) {
      return res.status(400).send("missing m_payment_id");
    }
    const mPaymentId = rawMPaymentId || makeRecurringMpaymentId();

    let intent = await db.oneOrNone(
      "select * from payment_intents where m_payment_id=$1",
      [mPaymentId]
    );
    let recurring = null;

    // If this ITN is for a recurring cycle, PayFast can call back with a token and
    // a cycle m_payment_id that may not exist in our local table yet.
    if (!intent && (payfastToken || (UUID_REGEX.test(recurringIdFromCustom) && recurringIdFromCustom))) {
      recurring =
        (payfastToken
          ? await db.oneOrNone(
              `
              select *
              from recurring_givings
              where payfast_token=$1
              limit 1
              `,
              [payfastToken]
            )
          : null) ||
        (UUID_REGEX.test(recurringIdFromCustom)
          ? await db.oneOrNone("select * from recurring_givings where id=$1 limit 1", [recurringIdFromCustom])
          : null);

      if (recurring && String(status || "").toUpperCase() === "COMPLETE") {
        if (pfPaymentId) {
          const alreadyTx = await db.oneOrNone(
            "select id from transactions where provider='payfast' and provider_payment_id=$1 limit 1",
            [pfPaymentId]
          );
          if (alreadyTx) {
            return res.status(200).send("OK");
          }
        }

        const nextCycle = Math.max(1, Number(recurring.cycles_completed || 0) + 1);
        const recurringAmount = toCurrencyNumber(recurring.donation_amount || 0);
        const recurringFee = toCurrencyNumber(recurring.platform_fee_amount || 0);
        const recurringGross = toCurrencyNumber(recurring.gross_amount || recurringAmount + recurringFee);
        const recurringMPaymentId = mPaymentId || makeRecurringMpaymentId();
        const feeCfg = readFeeConfig();

        intent = await db.one(
          `
          insert into payment_intents (
            church_id, fund_id, amount, currency, status,
            member_name, member_phone, payer_name, payer_phone, payer_email, payer_type,
            channel, provider, provider_payment_id, m_payment_id, item_name,
            platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct,
            source, recurring_giving_id, recurring_cycle_no,
            service_date, notes,
            created_at, updated_at
          ) values (
            $1,$2,$3,'ZAR','PENDING',
            null,null,$4,$5,$6,'member',
            'app','payfast',null,$7,$8,
            $9,$10,$11,$12,$13,$14,
            'RECURRING',$15,$16,
            now()::date,null,
            now(),now()
          )
          returning *
          `,
          [
            recurring.church_id,
            recurring.fund_id,
            recurringAmount,
            String([params.name_first, params.name_last].filter(Boolean).join(" ").trim() || "Recurring donor"),
            String(params.cell_number || params.payer_phone || "").trim() || null,
            String(params.email_address || "").trim() || null,
            recurringMPaymentId,
            String(params.item_name || `Recurring giving #${nextCycle}`).slice(0, 100),
            recurringFee,
            feeCfg.pct,
            feeCfg.fixed,
            recurringGross,
            toCurrencyNumber(recurringFee * feeCfg.superPct),
            feeCfg.superPct,
            recurring.id,
            nextCycle,
          ]
        );
      }
    }

    if (!intent) return res.status(404).send("unknown m_payment_id");
    if (!recurring && intent.recurring_giving_id) {
      recurring = await db.oneOrNone("select * from recurring_givings where id=$1", [intent.recurring_giving_id]);
    }

    const grossRaw = params.amount_gross ?? params.amount ?? "0";
    const gross = Number(grossRaw);
    const amountBase = Number(intent.amount || 0);
    const feeCfg = readFeeConfig();
    const feeFromIntent = Number(intent.platform_fee_amount);
    const computedFee = Number.isFinite(feeFromIntent)
      ? feeFromIntent
      : feeCfg.fixed + amountBase * feeCfg.pct;
    const expectedGross = Number(intent.amount_gross);
    const expected = Number.isFinite(expectedGross)
      ? expectedGross
      : toCurrencyNumber(amountBase + computedFee);

    if (!Number.isFinite(gross) || !Number.isFinite(expected)) {
      console.warn("[itn] invalid amounts", { grossRaw, expected });
      return res.status(400).send("invalid amount");
    }

    if (Number(gross.toFixed(2)) !== Number(expected.toFixed(2))) {
      console.warn("[itn] amount mismatch", { m_payment_id: mPaymentId, gross, expected });
      return res.status(400).send("amount mismatch");
    }

    const payfastFeeAmount = toCurrencyNumber(Math.abs(Number(params.amount_fee || 0)));

    if (status === "COMPLETE") {
      let createdTransactionId = null;
      await db.tx(async (t) => {
        await t.none(
          `
          update payment_intents
          set
            status='PAID',
            provider='payfast',
            provider_payment_id=$2,
            payfast_fee_amount=$3,
            church_net_amount=$4,
            updated_at=now()
          where id=$1
          `,
          [
            intent.id,
            pfPaymentId,
            payfastFeeAmount,
            toCurrencyNumber(Math.max(0, Number(intent.amount || 0) - payfastFeeAmount)),
          ]
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
          const churchNetAmount = toCurrencyNumber(Math.max(0, Number(intent.amount || 0) - payfastFeeAmount));
          const superadminCutPct = Number.isFinite(Number(intent.superadmin_cut_pct))
            ? Number(intent.superadmin_cut_pct)
            : feeCfg.superPct;
          const superadminCutAmount = toCurrencyNumber(
            intent.superadmin_cut_amount ?? platformFeeAmount * superadminCutPct
          );
          const insertedTx = await t.one(
            `insert into transactions (
               church_id,
               fund_id,
               payment_intent_id,
               giving_link_id,
               on_behalf_of_member_id,
               recurring_giving_id,
               recurring_cycle_no,
               amount,
               platform_fee_amount,
               platform_fee_pct,
               platform_fee_fixed,
               payfast_fee_amount,
               church_net_amount,
               amount_gross,
               superadmin_cut_amount,
               superadmin_cut_pct,
               payer_name,
               payer_phone,
               payer_email,
               payer_type,
               reference,
               channel,
               provider,
               provider_payment_id
             ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
             returning id`,
            [
              intent.church_id,
              intent.fund_id,
              intent.id,
              intent.giving_link_id || null,
              intent.on_behalf_of_member_id || null,
              intent.recurring_giving_id || null,
              intent.recurring_cycle_no || null,
              intent.amount,
              platformFeeAmount,
              Number(intent.platform_fee_pct ?? feeCfg.pct),
              Number(intent.platform_fee_fixed ?? feeCfg.fixed),
              payfastFeeAmount,
              churchNetAmount,
              amountGross,
              superadminCutAmount,
              superadminCutPct,
              intent.payer_name || intent.member_name || null,
              intent.payer_phone || intent.member_phone || null,
              intent.payer_email || null,
              intent.payer_type || "member",
              intent.m_payment_id,
              intent.channel || "app",
              "payfast",
              pfPaymentId,
            ]
          );
          createdTransactionId = insertedTx?.id || null;

          if (intent.giving_link_id) {
            await t.none(
              `
              update giving_links
              set
                use_count = least(coalesce(use_count,0) + 1, coalesce(max_uses, 1)),
                status = case
                  when least(coalesce(use_count,0) + 1, coalesce(max_uses, 1)) >= coalesce(max_uses, 1) then 'PAID'
                  else 'ACTIVE'
                end,
                paid_at = case
                  when least(coalesce(use_count,0) + 1, coalesce(max_uses, 1)) >= coalesce(max_uses, 1) then coalesce(paid_at, now())
                  else paid_at
                end,
                paid_payment_intent_id = case
                  when least(coalesce(use_count,0) + 1, coalesce(max_uses, 1)) >= coalesce(max_uses, 1) then coalesce(paid_payment_intent_id, $2)
                  else paid_payment_intent_id
                end
              where id = $1
              `,
              [intent.giving_link_id, intent.id]
            );
          }
        } else {
          await t.none(
            `
            update transactions
            set
              provider_payment_id = coalesce(provider_payment_id, $2),
              payfast_fee_amount = $3,
              church_net_amount = $4
            where payment_intent_id = $1
            `,
            [
              intent.id,
              pfPaymentId,
              payfastFeeAmount,
              toCurrencyNumber(Math.max(0, Number(intent.amount || 0) - payfastFeeAmount)),
            ]
          );
        }

        if (intent.recurring_giving_id) {
          const cycleNo = Math.max(1, Number(intent.recurring_cycle_no || 1));
          await t.none(
            `
            update recurring_givings
            set
              payfast_token = coalesce(payfast_token, nullif($2, '')),
              cycles_completed = greatest(coalesce(cycles_completed, 0), $3),
              status = case
                when coalesce(status, '') in ('CANCELLED', 'FAILED') then status
                when coalesce(cycles, 0) > 0 and greatest(coalesce(cycles_completed, 0), $3) >= cycles then 'COMPLETED'
                else 'ACTIVE'
              end,
              last_charged_at = now(),
              updated_at = now()
            where id = $1
            `,
            [intent.recurring_giving_id, payfastToken || null, cycleNo]
          );
        }
      });

      // Post-commit best-effort notification (never block ITN processing).
      if (createdTransactionId && intent.on_behalf_of_member_id) {
        try {
          const fund = await db.oneOrNone("select name from funds where id=$1", [intent.fund_id]);
          const amount = toCurrencyNumber(intent.amount || 0);
          const payerName = String(intent.payer_name || "").trim() || "Someone";
          const fundName = String(fund?.name || "").trim() || "a fund";
          await createNotification({
            memberId: intent.on_behalf_of_member_id,
            type: "GIVING_LINK_PAID",
            title: "Someone gave for you",
            body: `${payerName} gave R ${amount.toFixed(2)} to ${fundName}.`,
            data: {
              paymentIntentId: intent.id,
              transactionId: createdTransactionId,
              reference: intent.m_payment_id,
              churchId: intent.church_id,
              fundId: intent.fund_id,
              amount,
              source: intent.source || null,
            },
          });
        } catch (err) {
          console.error("[itn] notify requester failed", err?.message || err);
        }
      }

      return res.status(200).send("OK");
    }

    if (status === "FAILED") {
      await db.none(
        "update payment_intents set status='FAILED', provider='payfast', provider_payment_id=$2, updated_at=now() where id=$1 and coalesce(status,'') <> 'PAID'",
        [intent.id, pfPaymentId]
      );
      if (intent.recurring_giving_id) {
        await db.none(
          `
          update recurring_givings
          set
            payfast_token = coalesce(payfast_token, nullif($2, '')),
            status = case
              when coalesce(cycles_completed, 0) > 0 then status
              else 'FAILED'
            end,
            updated_at = now()
          where id = $1
          `,
          [intent.recurring_giving_id, payfastToken || null]
        );
      }
      return res.status(200).send("OK");
    }

    if (status === "CANCELLED") {
      await db.none(
        "update payment_intents set status='CANCELLED', provider='payfast', provider_payment_id=$2, updated_at=now() where id=$1 and coalesce(status,'') <> 'PAID'",
        [intent.id, pfPaymentId]
      );
      if (intent.recurring_giving_id) {
        await db.none(
          `
          update recurring_givings
          set status='CANCELLED', cancelled_at=coalesce(cancelled_at, now()), updated_at=now()
          where id = $1 and status <> 'COMPLETED'
          `,
          [intent.recurring_giving_id]
        );
      }
      return res.status(200).send("OK");
    }

    return res.status(200).send("OK");
  } catch (err) {
    console.error("[itn] server error", err);
    return res.status(500).send("server error");
  }
}

router.post("/payfast/itn", payfastItnRawParser, handlePayfastItn);

export default router;
