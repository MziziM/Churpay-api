import { db } from "../db.js";
import { buildPayfastRedirect } from "../payfast.js";
import { normalizePayfastMode, resolveChurchPayfastCredentials } from "../payfast-church.js";
import { PaymentGateway, makeGatewayError, normalizeGatewayStatus } from "./payment-gateway.js";

function normalize(value) {
  return String(value || "").trim();
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
    throw makeGatewayError("PayFast webhook normalization is not wired yet", {
      code: "PAYFAST_WEBHOOK_NOT_IMPLEMENTED",
      statusCode: 501,
    });
  }

  async refund(_input = {}) {
    throw makeGatewayError("PayFast refund is not implemented", {
      code: "PAYFAST_REFUND_NOT_IMPLEMENTED",
      statusCode: 501,
    });
  }
}
