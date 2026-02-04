import crypto from "crypto";

export function payfastProcessUrl(mode) {
  return mode === "live"
    ? "https://www.payfast.co.za/eng/process"
    : "https://sandbox.payfast.co.za/eng/process";
}

// PayFast signature rules (important):
// - keys sorted alphabetically
// - spaces encoded as '+'
// - RFC3986 tweaks on encodeURIComponent
// - ignore empty values
function formEncodeComponent(value) {
  return encodeURIComponent(String(value))
    .replace(/%20/g, "+")
    .replace(/[!'()*]/g, (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase());
}

function encodeFormQuery(params) {
  const entries = Object.entries(params)
    .filter(([, v]) => v !== undefined && v !== null && String(v) !== "")
    .map(([k, v]) => [String(k), String(v).trim()]);

  entries.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));

  return entries.map(([k, v]) => `${k}=${formEncodeComponent(v)}`).join("&");
}

export function generateSignature(params, passphrase) {
  const base = encodeFormQuery(params);
  const withPass = passphrase
    ? `${base}&passphrase=${formEncodeComponent(passphrase)}`
    : base;

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
  nameFirst,
  emailAddress,
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
    name_first: nameFirst,
    email_address: emailAddress,
  };

  const signature = generateSignature(base, passphrase);
  const qs = encodeFormQuery({ ...base, signature });

  return `${payfastProcessUrl(mode)}?${qs}`;
}