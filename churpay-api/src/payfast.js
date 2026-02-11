import crypto from "crypto";

export function payfastProcessUrl(mode) {
  return mode === "live"
    ? "https://www.payfast.co.za/eng/process"
    : "https://sandbox.payfast.co.za/eng/process";
}

// PayFast redirect signature rules (strict):
// - drop undefined/null/empty-string params
// - sort keys A-Z
// - encodeURIComponent then %20 -> '+'
// - join k=v with '&'
// - append passphrase only if non-empty
// - MD5 hex lowercase
function pfEncode(v) {
  return encodeURIComponent(String(v))
    .replace(/[!'()*~]/g, (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase())
    .replace(/%20/g, "+");
}

function encodeFormQuery(params) {
  const entries = Object.entries(params).filter(([, v]) => v !== undefined && v !== null && String(v) !== "");
  entries.sort(([a], [b]) => a.localeCompare(b));
  return entries.map(([k, v]) => `${k}=${pfEncode(v)}`).join("&");
}

export function generateSignature(params, passphrase) {
  const base = encodeFormQuery(params);
  const withPass = passphrase ? `${base}&passphrase=${pfEncode(passphrase)}` : base;
  return crypto.createHash("md5").update(withPass).digest("hex");
}

export function buildPayfastRedirect({
  mode,
  merchantId,
  merchantKey,
  passphrase,
  mPaymentId,
  amount,
  itemName,
  returnUrl,
  cancelUrl,
  notifyUrl,
  customStr1,
  customStr2,
  customStr3,
  customStr4,
  customStr5,
  nameFirst,
  emailAddress,
  subscriptionType,
  billingDate,
  recurringAmount,
  frequency,
  cycles,
  subscriptionNotifyEmail,
  subscriptionNotifyWebhook,
  subscriptionNotifyBuyer,
  token,
}) {
  const base = {
    merchant_id: merchantId,
    merchant_key: merchantKey,
    return_url: returnUrl,
    cancel_url: cancelUrl,
    notify_url: notifyUrl,
    m_payment_id: mPaymentId,
    amount: Number(amount).toFixed(2),
    item_name: itemName,
    custom_str1: customStr1,
    custom_str2: customStr2,
    custom_str3: customStr3,
    custom_str4: customStr4,
    custom_str5: customStr5,
    name_first: nameFirst,
    email_address: emailAddress,
    subscription_type: subscriptionType,
    billing_date: billingDate,
    recurring_amount:
      typeof recurringAmount === "number" && Number.isFinite(recurringAmount)
        ? Number(recurringAmount).toFixed(2)
        : recurringAmount,
    frequency,
    cycles,
    subscription_notify_email: subscriptionNotifyEmail,
    subscription_notify_webhook: subscriptionNotifyWebhook,
    subscription_notify_buyer: subscriptionNotifyBuyer,
    token,
  };

  const signature = generateSignature(base, passphrase);
  const qs = encodeFormQuery({ ...base, signature });

  if (String(process.env.PAYFAST_DEBUG || "").toLowerCase() === "1") {
    const maskedBase = passphrase ? qs.replace(pfEncode(passphrase), "***") : qs;
    console.log("[payfast] redirect sig", { base: maskedBase, signature });
  }

  return `${payfastProcessUrl(mode)}?${qs}`;
}
