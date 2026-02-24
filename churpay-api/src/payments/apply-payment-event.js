import crypto from "node:crypto";
import { db } from "../db.js";
import { createNotification } from "../notifications.js";
import { shouldTrackExternalDonorIntent, upsertChurchDonor } from "../church-donors.js";
import {
  applySubscriptionPaymentEvent,
  buildSubscriptionEventKey,
  normalizeChurchSubscriptionPlanCode,
} from "../church-subscriptions.js";

const DEFAULT_PLATFORM_FEE_FIXED = 2.5;
const DEFAULT_PLATFORM_FEE_PCT = 0.0075;
const DEFAULT_SUPERADMIN_CUT_PCT = 1.0;
const CHURPAY_GROWTH_SUBSCRIPTION_SOURCE = "CHURPAY_GROWTH_SUBSCRIPTION";
const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function normalize(value) {
  return String(value || "").trim();
}

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
    const token = normalize(value);
    if (token) return token;
  }
  return "";
}

function isGrowthSubscriptionSource(value) {
  return normalize(value).toUpperCase() === CHURPAY_GROWTH_SUBSCRIPTION_SOURCE;
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

function toIntentStatus(normalizedStatus) {
  const status = normalize(normalizedStatus).toLowerCase();
  if (status === "paid") return "PAID";
  if (status === "failed") return "FAILED";
  if (status === "cancelled" || status === "canceled") return "CANCELLED";
  if (status === "refunded") return "REFUNDED";
  if (status === "created") return "CREATED";
  return "PENDING";
}

function normalizeOccurredAt(value) {
  const raw = normalize(value);
  const parsed = raw ? Date.parse(raw) : NaN;
  if (Number.isFinite(parsed)) return new Date(parsed);
  return new Date();
}

function payloadHash(value) {
  return crypto.createHash("sha256").update(JSON.stringify(value || {})).digest("hex");
}

async function listChurchStaffMembers(churchId) {
  const id = normalize(churchId);
  if (!id) return [];
  return db.manyOrNone(
    `
    select id, full_name as "fullName", role
    from members
    where church_id=$1 and lower(role) in ('admin','accountant','finance','pastor','volunteer','usher')
    order by created_at asc
    `,
    [id]
  );
}

function makeEventType(event, status) {
  if (normalize(event?.type)) return normalize(event.type).toUpperCase();
  if (status === "PAID") return "PAYMENT_COMPLETED";
  if (status === "FAILED") return "PAYMENT_FAILED";
  if (status === "CANCELLED") return "PAYMENT_CANCELLED";
  if (status === "REFUNDED") return "PAYMENT_REFUNDED";
  return "PAYMENT_UPDATED";
}

async function recordPaymentEventTx(t, event, intent, status, occurredAt, params) {
  const provider = normalize(event?.provider).toLowerCase() || "payfast";
  const providerEventId = normalize(event?.providerEventId || event?.providerRef || "");
  const eventType = makeEventType(event, status);
  const bodyPayload = params && typeof params === "object" ? params : {};
  const metadata = event?.metadata && typeof event.metadata === "object" ? event.metadata : {};
  const hash = payloadHash(bodyPayload);
  const amount = Number.isFinite(Number(event?.amount)) ? toCurrencyNumber(event.amount) : null;
  const currency = normalize(event?.currency).toUpperCase() || "ZAR";
  const churchId = normalize(event?.churchId || intent?.church_id) || null;
  const source = normalize(event?.source || intent?.source).toUpperCase() || null;

  const inserted = await t.oneOrNone(
    `
    insert into payment_events (
      intent_id,
      provider,
      provider_event_id,
      type,
      status,
      amount,
      currency,
      church_id,
      source,
      payload,
      metadata,
      payload_hash,
      occurred_at
    )
    values (
      $1,$2,nullif($3,''),$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12,$13
    )
    on conflict do nothing
    returning id
    `,
    [
      intent?.id || null,
      provider,
      providerEventId || null,
      eventType,
      status,
      amount,
      currency,
      churchId,
      source,
      JSON.stringify(bodyPayload),
      JSON.stringify(metadata),
      hash,
      occurredAt,
    ]
  );

  return {
    inserted: Boolean(inserted?.id),
    idempotent: !inserted?.id,
    paymentEventId: inserted?.id || null,
  };
}

function makeApplyError(message, statusCode = 400) {
  const err = new Error(message);
  err.statusCode = statusCode;
  err.publicMessage = message;
  return err;
}

async function sendPostCommitNotifications(result) {
  const requesterNotification = result?.requesterNotification || null;
  if (requesterNotification?.memberId) {
    try {
      await createNotification(requesterNotification);
    } catch (err) {
      console.error("[payment-events] notify requester failed", err?.message || err);
    }
  }

  const staffNotifications = Array.isArray(result?.staffNotifications) ? result.staffNotifications : [];
  for (const payload of staffNotifications) {
    if (!payload?.memberId) continue;
    try {
      await createNotification(payload);
    } catch (err) {
      console.error("[payment-events] notify staff failed", err?.message || err);
    }
  }
}

export async function applyPaymentEvent(event) {
  if (!event || typeof event !== "object") {
    throw makeApplyError("payment event is required", 400);
  }
  if (normalize(event.provider).toLowerCase() !== "payfast") {
    throw makeApplyError("unsupported payment provider", 400);
  }

  const payloadParams =
    (event.payload && typeof event.payload === "object" ? event.payload : null) ||
    (event.metadata?.rawParams && typeof event.metadata.rawParams === "object" ? event.metadata.rawParams : {});
  const normalizedStatus = toIntentStatus(event.status);
  const occurredAt = normalizeOccurredAt(event.occurredAt);
  const providerRef = normalize(event.providerRef || payloadParams?.pf_payment_id) || null;
  const payfastToken = extractPayfastToken(payloadParams);
  const planCode = normalizeChurchSubscriptionPlanCode(payloadParams?.custom_str2 || null);

  const txResult = await db.tx(async (t) => {
    let intent = null;
    if (normalize(event.intentId) && UUID_REGEX.test(normalize(event.intentId))) {
      intent = await t.oneOrNone("select * from payment_intents where id=$1 limit 1", [normalize(event.intentId)]);
    }
    if (!intent && normalize(event.providerIntentRef)) {
      intent = await t.oneOrNone("select * from payment_intents where m_payment_id=$1 limit 1", [
        normalize(event.providerIntentRef),
      ]);
    }

    let recurring =
      event?.metadata?.recurringSnapshot && typeof event.metadata.recurringSnapshot === "object"
        ? event.metadata.recurringSnapshot
        : null;

    if (!recurring) {
      const recurringIdFromCustom = normalize(payloadParams?.custom_str3);
      if (UUID_REGEX.test(recurringIdFromCustom)) {
        recurring = await t.oneOrNone("select * from recurring_givings where id=$1 limit 1", [recurringIdFromCustom]);
      } else if (payfastToken) {
        recurring = await t.oneOrNone("select * from recurring_givings where payfast_token=$1 limit 1", [payfastToken]);
      }
    }

    const mPaymentId = normalize(event.providerIntentRef || payloadParams?.m_payment_id || "") || makeRecurringMpaymentId();
    const growthMarker = normalize(payloadParams?.custom_str4).toUpperCase();
    const isGrowthSubscription =
      growthMarker === CHURPAY_GROWTH_SUBSCRIPTION_SOURCE || isGrowthSubscriptionSource(intent?.source || event?.source);

    if (!intent && recurring && normalizedStatus === "PAID") {
      if (providerRef) {
        const alreadyTx = await t.oneOrNone(
          "select id from transactions where provider='payfast' and provider_payment_id=$1 limit 1",
          [providerRef]
        );
        if (alreadyTx) {
          const evt = await recordPaymentEventTx(t, event, null, normalizedStatus, occurredAt, payloadParams);
          return {
            ok: true,
            idempotent: true,
            skipped: true,
            reason: "provider_payment_already_processed",
            paymentEventId: evt.paymentEventId,
          };
        }
      }

      const nextCycle = Math.max(1, Number(recurring.cycles_completed || 0) + 1);
      const recurringAmount = toCurrencyNumber(recurring.donation_amount || 0);
      const recurringFee = toCurrencyNumber(recurring.platform_fee_amount || 0);
      const recurringGross = toCurrencyNumber(recurring.gross_amount || recurringAmount + recurringFee);
      const feeCfg = readFeeConfig();

      intent = await t.one(
        `
        insert into payment_intents (
          church_id, fund_id, amount, currency, status,
          member_name, member_phone, payer_name, payer_phone, payer_email, payer_type,
          channel, provider, provider_payment_id, m_payment_id, item_name,
          platform_fee_amount, platform_fee_pct, platform_fee_fixed, amount_gross, superadmin_cut_amount, superadmin_cut_pct,
          source, recurring_giving_id, recurring_cycle_no,
          service_date, notes, provider_intent_ref, metadata,
          created_at, updated_at
        ) values (
          $1,$2,$3,'ZAR','PENDING',
          null,null,$4,$5,$6,'member',
          'app','payfast',null,$7,$8,
          $9,$10,$11,$12,$13,$14,
          'RECURRING',$15,$16,
          now()::date,null,$7,$17::jsonb,
          now(),now()
        )
        returning *
        `,
        [
          recurring.church_id,
          recurring.fund_id,
          recurringAmount,
          normalize([payloadParams?.name_first, payloadParams?.name_last].filter(Boolean).join(" ")) || "Recurring donor",
          normalize(payloadParams?.cell_number || payloadParams?.payer_phone) || null,
          normalize(payloadParams?.email_address) || null,
          mPaymentId,
          normalize(payloadParams?.item_name || `Recurring giving #${nextCycle}`).slice(0, 100),
          recurringFee,
          feeCfg.pct,
          feeCfg.fixed,
          recurringGross,
          toCurrencyNumber(recurringFee * feeCfg.superPct),
          feeCfg.superPct,
          recurring.id,
          nextCycle,
          JSON.stringify({
            source: "RECURRING",
            recurringGivingId: recurring.id,
            cycleNo: nextCycle,
            generatedFromWebhook: true,
          }),
        ]
      );
    }

    const eventRow = await recordPaymentEventTx(t, event, intent, normalizedStatus, occurredAt, payloadParams);
    if (eventRow.idempotent) {
      return {
        ok: true,
        idempotent: true,
        intentId: intent?.id || null,
        paymentEventId: eventRow.paymentEventId,
      };
    }

    if (!intent) {
      return {
        ok: true,
        idempotent: false,
        ignored: true,
        reason: "unknown_m_payment_id",
        paymentEventId: eventRow.paymentEventId,
      };
    }

    if (!recurring && intent.recurring_giving_id) {
      recurring = await t.oneOrNone("select * from recurring_givings where id=$1", [intent.recurring_giving_id]);
    }

    const grossRaw = payloadParams?.amount_gross ?? payloadParams?.amount ?? event?.amount ?? "0";
    const gross = Number(grossRaw);
    const amountBase = Number(intent.amount || 0);
    const feeCfg = readFeeConfig();
    const feeFromIntent = Number(intent.platform_fee_amount);
    const computedFee = Number.isFinite(feeFromIntent) ? feeFromIntent : feeCfg.fixed + amountBase * feeCfg.pct;
    const expectedGross = Number(intent.amount_gross);
    const expected = Number.isFinite(expectedGross) ? expectedGross : toCurrencyNumber(amountBase + computedFee);

    if (!Number.isFinite(gross) || !Number.isFinite(expected)) {
      throw makeApplyError("invalid amount", 400);
    }
    if (Number(gross.toFixed(2)) !== Number(expected.toFixed(2))) {
      throw makeApplyError("amount mismatch", 400);
    }

    const payfastFeeAmount = toCurrencyNumber(Math.abs(Number(payloadParams?.amount_fee || 0)));
    const churchNetAmount = toCurrencyNumber(Math.max(0, Number(intent.amount || 0) - payfastFeeAmount));

    if (normalizedStatus === "PAID") {
      if (isGrowthSubscription) {
        await t.none(
          `
          update payment_intents
          set
            status='PAID',
            provider='payfast',
            provider_payment_id=$2,
            payfast_fee_amount=$3,
            church_net_amount=$4,
            provider_intent_ref = coalesce(provider_intent_ref, nullif($5, '')),
            metadata = coalesce(metadata, '{}'::jsonb) || $6::jsonb,
            updated_at=now()
          where id=$1
          `,
          [
            intent.id,
            providerRef,
            payfastFeeAmount,
            churchNetAmount,
            mPaymentId,
            JSON.stringify({ webhookSource: "payfast", providerEventId: event.providerEventId || null }),
          ]
        );

        await applySubscriptionPaymentEvent({
          churchId: intent.church_id,
          eventType: "PAYMENT_OK",
          planCode,
          provider: "payfast",
          providerPaymentId: providerRef || intent.id,
          providerSubscriptionId: payfastToken || null,
          eventKey: buildSubscriptionEventKey({
            eventType: "PAYMENT_OK",
            providerPaymentId: providerRef,
            paymentIntentId: intent.id,
            churchId: intent.church_id,
          }),
          payload: {
            mPaymentId,
            pfPaymentId: providerRef,
            customStr2: payloadParams?.custom_str2 || null,
            customStr3: payloadParams?.custom_str3 || null,
          },
          actorType: "SYSTEM",
          actorId: null,
        });

        return {
          ok: true,
          idempotent: false,
          intentId: intent.id,
          status: "PAID",
          paymentEventId: eventRow.paymentEventId,
        };
      }

      let createdTransactionId = null;
      await t.none(
        `
        update payment_intents
        set
          status='PAID',
          provider='payfast',
          provider_payment_id=$2,
          payfast_fee_amount=$3,
          church_net_amount=$4,
          provider_intent_ref = coalesce(provider_intent_ref, nullif($5, '')),
          metadata = coalesce(metadata, '{}'::jsonb) || $6::jsonb,
          updated_at=now()
        where id=$1
        `,
        [
          intent.id,
          providerRef,
          payfastFeeAmount,
          churchNetAmount,
          mPaymentId,
          JSON.stringify({ webhookSource: "payfast", providerEventId: event.providerEventId || null }),
        ]
      );

      const existing = await t.oneOrNone("select id from transactions where payment_intent_id=$1 limit 1", [intent.id]);
      if (!existing) {
        const platformFeeAmount = toCurrencyNumber(
          intent.platform_fee_amount ?? feeCfg.fixed + Number(intent.amount || 0) * feeCfg.pct
        );
        const amountGross = toCurrencyNumber(intent.amount_gross ?? Number(intent.amount || 0) + platformFeeAmount);
        const superadminCutPct = Number.isFinite(Number(intent.superadmin_cut_pct))
          ? Number(intent.superadmin_cut_pct)
          : feeCfg.superPct;
        const superadminCutAmount = toCurrencyNumber(intent.superadmin_cut_amount ?? platformFeeAmount * superadminCutPct);
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
            providerRef,
          ]
        );
        createdTransactionId = insertedTx?.id || null;

        if (shouldTrackExternalDonorIntent(intent)) {
          await upsertChurchDonor(
            {
              churchId: intent.church_id,
              payerName: intent.payer_name || intent.member_name || null,
              payerEmail: intent.payer_email || null,
              payerPhone: intent.payer_phone || intent.member_phone || null,
              amount: intent.amount || null,
              paymentIntentId: intent.id,
              transactionId: insertedTx?.id || null,
              source: intent.source || intent.payer_type || "PAYFAST_ITN",
            },
            t
          );
        }

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
          [intent.id, providerRef, payfastFeeAmount, churchNetAmount]
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

      if (intent.save_card_requested && payfastToken) {
        const email = normalize(intent.payer_email);
        const phone = normalize(intent.payer_phone || intent.member_phone);
        if (email || phone) {
          await t.none(
            `
            update members
            set
              payfast_adhoc_token = nullif($3, ''),
              payfast_adhoc_token_created_at = case
                when nullif($3, '') is null then payfast_adhoc_token_created_at
                when coalesce(payfast_adhoc_token,'') <> $3 then now()
                else payfast_adhoc_token_created_at
              end,
              payfast_adhoc_token_revoked_at = null,
              updated_at = now()
            where
              ($1 <> '' and lower(email) = lower($1))
              or ($2 <> '' and phone = $2)
            `,
            [email, phone, payfastToken]
          );
        }
      }

      const result = {
        ok: true,
        idempotent: false,
        intentId: intent.id,
        status: "PAID",
        paymentEventId: eventRow.paymentEventId,
        createdTransactionId: createdTransactionId || null,
      };

      if (createdTransactionId && intent.on_behalf_of_member_id) {
        const fund = await t.oneOrNone("select name from funds where id=$1", [intent.fund_id]);
        const amountValue = toCurrencyNumber(intent.amount || 0);
        const payerName = normalize(intent.payer_name) || "Someone";
        const fundName = normalize(fund?.name) || "a fund";
        result.requesterNotification = {
          memberId: intent.on_behalf_of_member_id,
          type: "GIVING_LINK_PAID",
          title: "Someone gave for you",
          body: `${payerName} gave R ${amountValue.toFixed(2)} to ${fundName}.`,
          data: {
            paymentIntentId: intent.id,
            transactionId: createdTransactionId,
            reference: intent.m_payment_id,
            churchId: intent.church_id,
            fundId: intent.fund_id,
            amount: amountValue,
            source: intent.source || null,
          },
        };
      }

      if (createdTransactionId) {
        const staff = await listChurchStaffMembers(intent.church_id);
        if (staff.length) {
          const fund = await t.oneOrNone("select name from funds where id=$1", [intent.fund_id]);
          const amountValue = toCurrencyNumber(intent.amount || 0);
          const payerName = normalize(intent.payer_name || intent.member_name) || "Someone";
          const fundName = normalize(fund?.name) || "a fund";
          const payerType = normalize(intent.payer_type || "member").toLowerCase();

          result.staffNotifications = staff.map((staffMember) => ({
            memberId: staffMember.id,
            type: "GIVING_RECEIVED",
            title: "New giving received",
            body: `${payerName} gave R ${amountValue.toFixed(2)} to ${fundName}.`,
            data: {
              paymentIntentId: intent.id,
              transactionId: createdTransactionId,
              reference: intent.m_payment_id,
              churchId: intent.church_id,
              fundId: intent.fund_id,
              amount: amountValue,
              payerType,
              provider: "payfast",
              source: intent.source || null,
            },
          }));
        }
      }

      return result;
    }

    if (normalizedStatus === "FAILED") {
      await t.none(
        `
        update payment_intents
        set
          status='FAILED',
          provider='payfast',
          provider_payment_id=$2,
          provider_intent_ref = coalesce(provider_intent_ref, nullif($3, '')),
          metadata = coalesce(metadata, '{}'::jsonb) || $4::jsonb,
          updated_at=now()
        where id=$1 and coalesce(status,'') <> 'PAID'
        `,
        [
          intent.id,
          providerRef,
          mPaymentId,
          JSON.stringify({ webhookSource: "payfast", providerEventId: event.providerEventId || null }),
        ]
      );
      if (isGrowthSubscription) {
        await applySubscriptionPaymentEvent({
          churchId: intent.church_id,
          eventType: "PAYMENT_FAILED",
          planCode,
          provider: "payfast",
          providerPaymentId: providerRef || intent.id,
          providerSubscriptionId: payfastToken || null,
          eventKey: buildSubscriptionEventKey({
            eventType: "PAYMENT_FAILED",
            providerPaymentId: providerRef,
            paymentIntentId: intent.id,
            churchId: intent.church_id,
          }),
          payload: {
            mPaymentId,
            pfPaymentId: providerRef,
            customStr2: payloadParams?.custom_str2 || null,
          },
          actorType: "SYSTEM",
          actorId: null,
        });
      }
      if (intent.recurring_giving_id) {
        await t.none(
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
      return {
        ok: true,
        idempotent: false,
        intentId: intent.id,
        status: "FAILED",
        paymentEventId: eventRow.paymentEventId,
      };
    }

    if (normalizedStatus === "CANCELLED") {
      await t.none(
        `
        update payment_intents
        set
          status='CANCELLED',
          provider='payfast',
          provider_payment_id=$2,
          provider_intent_ref = coalesce(provider_intent_ref, nullif($3, '')),
          metadata = coalesce(metadata, '{}'::jsonb) || $4::jsonb,
          updated_at=now()
        where id=$1 and coalesce(status,'') <> 'PAID'
        `,
        [
          intent.id,
          providerRef,
          mPaymentId,
          JSON.stringify({ webhookSource: "payfast", providerEventId: event.providerEventId || null }),
        ]
      );
      if (isGrowthSubscription) {
        await applySubscriptionPaymentEvent({
          churchId: intent.church_id,
          eventType: "CANCELED",
          planCode,
          provider: "payfast",
          providerPaymentId: providerRef || intent.id,
          providerSubscriptionId: payfastToken || null,
          eventKey: buildSubscriptionEventKey({
            eventType: "CANCELED",
            providerPaymentId: providerRef,
            paymentIntentId: intent.id,
            churchId: intent.church_id,
          }),
          payload: {
            mPaymentId,
            pfPaymentId: providerRef,
            customStr2: payloadParams?.custom_str2 || null,
          },
          actorType: "SYSTEM",
          actorId: null,
        });
      }
      if (intent.recurring_giving_id) {
        await t.none(
          `
          update recurring_givings
          set status='CANCELLED', cancelled_at=coalesce(cancelled_at, now()), updated_at=now()
          where id = $1 and status <> 'COMPLETED'
          `,
          [intent.recurring_giving_id]
        );
      }
      return {
        ok: true,
        idempotent: false,
        intentId: intent.id,
        status: "CANCELLED",
        paymentEventId: eventRow.paymentEventId,
      };
    }

    await t.none("update payment_intents set updated_at=now() where id=$1", [intent.id]);
    return {
      ok: true,
      idempotent: false,
      intentId: intent.id,
      status: normalizedStatus,
      paymentEventId: eventRow.paymentEventId,
    };
  });

  await sendPostCommitNotifications(txResult);
  return txResult;
}

export default applyPaymentEvent;
