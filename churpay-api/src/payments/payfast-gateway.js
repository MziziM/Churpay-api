import crypto from "node:crypto";
import { db } from "../db.js";
import { buildPayfastRedirect } from "../payfast.js";
import { normalizePayfastMode, resolveChurchPayfastCredentials } from "../payfast-church.js";
import { PaymentGateway, makeGatewayError, normalizeGatewayStatus } from "./payment-gateway.js";

const CHURPAY_GROWTH_SUBSCRIPTION_SOURCE = "CHURPAY_GROWTH_SUBSCRIPTION";
const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function normalize(value) {
  return String(value || "").trim();
}

function toCurrencyNumber(value) {
  const rounded = Math.round(Number(value) * 100) / 100;
  return Number.isFinite(rounded) ? Number(rounded.toFixed(2)) : 0;
}

function extractPayfastToken(params) {
  const candidates = [params?.token, params?.subscription_id, params?.subscriptionId, params?.subscription_token];
  for (const value of candidates) {
    const token = normalize(value);
    if (token) return token;
  }
  return "";
}

function isGrowthSubscriptionSource(value) {
  return normalize(value).toUpperCase() === CHURPAY_GROWTH_SUBSCRIPTION_SOURCE;
}

function parseOccurredAt(params) {
  const raw = normalize(params?.payment_date || params?.paymentDate || params?.timestamp || "");
  if (!raw) return new Date().toISOString();
  const normalized = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(raw) ? `${raw.replace(" ", "T")}Z` : raw;
  const ms = Date.parse(normalized);
  if (!Number.isFinite(ms)) return new Date().toISOString();
  return new Date(ms).toISOString();
}

function mapWebhookStatus(rawStatus) {
  const status = normalize(rawStatus).toUpperCase();
  if (status === "COMPLETE") return { status: "paid", type: "PAYMENT_COMPLETED" };
  if (status === "FAILED") return { status: "failed", type: "PAYMENT_FAILED" };
  if (status === "CANCELLED") return { status: "cancelled", type: "PAYMENT_CANCELLED" };
  return { status: "pending", type: "PAYMENT_UPDATED" };
}

function makeWebhookError(message, { code = "PAYFAST_WEBHOOK_INVALID", statusCode = 400 } = {}) {
  const err = makeGatewayError(message, { code, statusCode });
  err.publicMessage = message;
  return err;
}

function readGlobalPayfastCredentials() {
  return {
    source: "global",
    mode: normalizePayfastMode(process.env.PAYFAST_MODE),
    merchantId: normalize(process.env.PAYFAST_MERCHANT_ID),
    merchantKey: normalize(process.env.PAYFAST_MERCHANT_KEY),
    passphrase: normalize(process.env.PAYFAST_PASSPHRASE),
  };
}

function hasRequiredCredentials(creds) {
  return Boolean(normalize(creds?.merchantId) && normalize(creds?.merchantKey));
}

export class PayFastGateway extends PaymentGateway {
  constructor(options = {}) {
    super();
    this.provider = "payfast";
    this.logger = options.logger || console;
  }

  async resolveCredentials({ churchId, preferGlobal = false, allowGlobalFallback = true } = {}) {
    const globalCreds = readGlobalPayfastCredentials();
    if (preferGlobal && hasRequiredCredentials(globalCreds)) {
      return globalCreds;
    }

    const churchCreds = await resolveChurchPayfastCredentials(churchId, { allowGlobalFallback });
    if (hasRequiredCredentials(churchCreds)) {
      return churchCreds;
    }

    if (!preferGlobal && hasRequiredCredentials(globalCreds) && allowGlobalFallback) {
      return globalCreds;
    }

    return null;
  }

  async createIntent(input = {}) {
    const churchId = normalize(input.churchId);
    const amount = Number(input.amount);
    const currency = normalize(input.currency || "ZAR").toUpperCase() || "ZAR";
    const providerIntentRef = normalize(input.providerIntentRef || input.mPaymentId || input.intentId);
    const itemName = normalize(input.itemName || "ChurPay payment");
    const returnUrl = normalize(input.returnUrl);
    const cancelUrl = normalize(input.cancelUrl);
    const notifyUrl = normalize(input.notifyUrl);

    if (!churchId) {
      throw makeGatewayError("churchId is required", { code: "PAYFAST_CHURCH_REQUIRED", statusCode: 400 });
    }
    if (!providerIntentRef) {
      throw makeGatewayError("providerIntentRef is required", { code: "PAYFAST_INTENT_REF_REQUIRED", statusCode: 400 });
    }
    if (!Number.isFinite(amount) || amount <= 0) {
      throw makeGatewayError("amount must be a positive number", { code: "PAYFAST_AMOUNT_INVALID", statusCode: 400 });
    }
    if (!returnUrl || !cancelUrl || !notifyUrl) {
      throw makeGatewayError("returnUrl, cancelUrl and notifyUrl are required", {
        code: "PAYFAST_CALLBACK_URLS_REQUIRED",
        statusCode: 400,
      });
    }

    const creds = await this.resolveCredentials({
      churchId,
      preferGlobal: Boolean(input.preferGlobalCredentials),
      allowGlobalFallback: input.allowGlobalFallback !== false,
    });

    if (!hasRequiredCredentials(creds)) {
      throw makeGatewayError("Payments are not activated for this church. Ask your church admin to connect PayFast.", {
        code: "PAYFAST_NOT_CONNECTED",
        statusCode: 503,
      });
    }

    const checkoutUrl = buildPayfastRedirect({
      mode: creds.mode,
      merchantId: creds.merchantId,
      merchantKey: creds.merchantKey,
      passphrase: creds.passphrase || "",
      mPaymentId: providerIntentRef,
      amount,
      itemName,
      returnUrl,
      cancelUrl,
      notifyUrl,
      customStr1: input.reference1,
      customStr2: input.reference2,
      customStr3: input.reference3,
      customStr4: input.reference4,
      customStr5: input.reference5,
      nameFirst: input.payerName,
      emailAddress: input.payerEmail,
      subscriptionType: input.subscriptionType,
      billingDate: input.billingDate,
      recurringAmount: input.recurringAmount,
      frequency: input.frequency,
      cycles: input.cycles,
      subscriptionNotifyEmail: input.subscriptionNotifyEmail,
      subscriptionNotifyWebhook: input.subscriptionNotifyWebhook,
      subscriptionNotifyBuyer: input.subscriptionNotifyBuyer,
      token: input.token,
    });

    return {
      ok: true,
      provider: this.provider,
      checkoutUrl,
      paymentUrl: checkoutUrl,
      amount,
      currency,
      providerIntentRef,
      credentialSource: creds.source || "unknown",
      mode: creds.mode || "sandbox",
      metadata: input.metadata || {},
    };
  }

  async getIntentStatus({ intentId } = {}) {
    const id = normalize(intentId);
    if (!id) {
      throw makeGatewayError("intentId is required", { code: "PAYMENT_INTENT_ID_REQUIRED", statusCode: 400 });
    }

    const row = await db.oneOrNone(
      `
      select
        id,
        provider,
        coalesce(provider_intent_ref, m_payment_id) as "providerIntentRef",
        provider_payment_id as "providerRef",
        status,
        church_id as "churchId",
        amount,
        currency,
        source,
        metadata,
        created_at as "createdAt",
        updated_at as "updatedAt"
      from payment_intents
      where id = $1
      limit 1
      `,
      [id]
    );

    if (!row) {
      throw makeGatewayError("Payment intent not found", { code: "PAYMENT_INTENT_NOT_FOUND", statusCode: 404 });
    }

    return {
      ok: true,
      provider: this.provider,
      intentId: row.id,
      providerIntentRef: row.providerIntentRef || null,
      providerRef: row.providerRef || null,
      status: normalizeGatewayStatus(row.status),
      churchId: row.churchId,
      source: row.source || null,
      amount: Number(row.amount || 0),
      currency: normalize(row.currency || "ZAR").toUpperCase(),
      metadata: row.metadata || null,
      createdAt: row.createdAt || null,
      updatedAt: row.updatedAt || null,
    };
  }

  async handleWebhook(_input = {}) {
    const input = _input || {};
    const debug = String(process.env.PAYFAST_DEBUG || "").toLowerCase() === "1";
    const rawBody =
      typeof input.rawBody === "string"
        ? input.rawBody
        : Buffer.isBuffer(input.rawBody)
        ? input.rawBody.toString("utf8")
        : Buffer.isBuffer(input.body)
        ? input.body.toString("utf8")
        : typeof input.body === "string"
        ? input.body
        : "";

    const params = Object.fromEntries(new URLSearchParams(rawBody));
    const receivedSig = normalize(params.signature);
    if (!receivedSig) {
      throw makeWebhookError("missing signature", { code: "PAYFAST_SIGNATURE_MISSING", statusCode: 400 });
    }

    const rawMPaymentId = normalize(params.m_payment_id);
    const payfastToken = extractPayfastToken(params);
    const recurringIdFromCustom = normalize(params.custom_str3);
    if (!rawMPaymentId && !payfastToken && !UUID_REGEX.test(recurringIdFromCustom)) {
      throw makeWebhookError("missing m_payment_id", { code: "PAYFAST_M_PAYMENT_ID_MISSING", statusCode: 400 });
    }

    const mPaymentId = rawMPaymentId;
    const pfPaymentId = normalize(params.pf_payment_id) || null;
    let intent = rawMPaymentId
      ? await db.oneOrNone("select * from payment_intents where m_payment_id=$1 limit 1", [rawMPaymentId])
      : null;
    let recurring = null;

    if (!intent && (payfastToken || UUID_REGEX.test(recurringIdFromCustom))) {
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
    }

    const growthMarker = normalize(params.custom_str4).toUpperCase();
    const isGrowthSubscription =
      growthMarker === CHURPAY_GROWTH_SUBSCRIPTION_SOURCE || isGrowthSubscriptionSource(intent?.source);
    const churchIdFromParam = normalize(params.custom_str1);
    const churchIdForValidation = normalize(intent?.church_id || recurring?.church_id || churchIdFromParam);
    if (!churchIdForValidation) {
      throw makeWebhookError("missing church context", { code: "PAYFAST_CHURCH_CONTEXT_MISSING", statusCode: 400 });
    }

    const creds = await this.resolveCredentials({
      churchId: churchIdForValidation,
      preferGlobal: isGrowthSubscription,
      allowGlobalFallback: true,
    });
    if (!hasRequiredCredentials(creds)) {
      throw makeWebhookError("invalid merchant context", {
        code: "PAYFAST_MERCHANT_CONTEXT_INVALID",
        statusCode: 400,
      });
    }

    const merchantIdFromItn = normalize(params.merchant_id);
    if (merchantIdFromItn && merchantIdFromItn !== normalize(creds.merchantId)) {
      throw makeWebhookError("invalid merchant", { code: "PAYFAST_MERCHANT_ID_MISMATCH", statusCode: 400 });
    }
    const merchantKeyFromItn = normalize(params.merchant_key);
    if (merchantKeyFromItn && merchantKeyFromItn !== normalize(creds.merchantKey)) {
      throw makeWebhookError("invalid merchant", { code: "PAYFAST_MERCHANT_KEY_MISMATCH", statusCode: 400 });
    }

    const passphrase = normalize(creds.passphrase);
    const parts = rawBody.split("&").filter(Boolean);
    const unsignedParts = parts.filter((part) => !part.startsWith("signature="));
    let sigBase = unsignedParts.join("&");
    if (passphrase) {
      sigBase += `&passphrase=${encodeURIComponent(passphrase).replace(/%20/g, "+")}`;
    }
    const computedSig = crypto.createHash("md5").update(sigBase).digest("hex");
    if (computedSig.toLowerCase() !== receivedSig.toLowerCase()) {
      if (debug) {
        this.logger.warn?.("[payfast-gateway] signature mismatch", {
          submitted: receivedSig,
          computed: computedSig,
          m_payment_id: rawMPaymentId || null,
        });
      }
      throw makeWebhookError("invalid signature", { code: "PAYFAST_SIGNATURE_INVALID", statusCode: 400 });
    }

    const normalized = mapWebhookStatus(params.payment_status);
    const providerEventId =
      pfPaymentId ||
      [normalize(rawMPaymentId || recurringIdFromCustom), normalize(params.payment_status), normalize(params.payment_date)]
        .filter(Boolean)
        .join(":") ||
      null;

    const amountRaw = params.amount_gross ?? params.amount ?? null;
    const amount = amountRaw == null ? null : toCurrencyNumber(amountRaw);
    const currency = normalize(params.currency || "ZAR").toUpperCase() || "ZAR";
    const source = normalize(intent?.source || (isGrowthSubscription ? CHURPAY_GROWTH_SUBSCRIPTION_SOURCE : "")).toUpperCase() || null;

    return {
      provider: this.provider,
      providerEventId,
      type: normalized.type,
      intentId: intent?.id || null,
      providerRef: pfPaymentId || null,
      providerIntentRef: rawMPaymentId || null,
      status: normalized.status,
      amount,
      currency,
      churchId: churchIdForValidation,
      source,
      metadata: {
        rawParams: params,
        mPaymentId: rawMPaymentId || null,
        payfastToken: payfastToken || null,
        recurringIdFromCustom: recurringIdFromCustom || null,
        intentSnapshot: intent || null,
        recurringSnapshot: recurring || null,
        credentialSource: creds.source || "church",
        growthMarker: growthMarker || null,
      },
      occurredAt: parseOccurredAt(params),
      payload: params,
    };
  }

  async refund(_input = {}) {
    throw makeGatewayError("PayFast refund is not implemented", {
      code: "PAYFAST_REFUND_NOT_IMPLEMENTED",
      statusCode: 501,
    });
  }
}
