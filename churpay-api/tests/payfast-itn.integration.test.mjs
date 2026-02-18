import assert from "node:assert/strict";
import crypto from "node:crypto";
import test from "node:test";

const BASE_URL = process.env.TEST_BASE_URL || "";
const JOIN_CODE = process.env.TEST_JOIN_CODE || "";
const FUND_CODE = process.env.TEST_FUND_CODE || "general";
const TEST_ITN_ENABLED = String(process.env.TEST_ITN_ENABLED || "0") === "1";
const PAYFAST_PASSPHRASE = process.env.TEST_PAYFAST_PASSPHRASE || process.env.PAYFAST_PASSPHRASE || "";

function toMoney(value) {
  return Math.round(Number(value) * 100) / 100;
}

function encodeForm(value) {
  return encodeURIComponent(String(value ?? ""))
    .replace(/%20/g, "+");
}

function buildSignedItnBody(fields) {
  const orderedEntries = Object.entries(fields);
  const unsigned = orderedEntries.map(([key, value]) => `${encodeForm(key)}=${encodeForm(value)}`).join("&");
  const passphrase = String(PAYFAST_PASSPHRASE || "").trim();
  const signatureBase = passphrase
    ? `${unsigned}&passphrase=${encodeForm(passphrase)}`
    : unsigned;
  const signature = crypto.createHash("md5").update(signatureBase).digest("hex");
  return `${unsigned}&signature=${signature}`;
}

async function createVisitorIntent(amount = 10) {
  const response = await fetch(`${BASE_URL}/api/public/give/payment-intents`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      joinCode: JOIN_CODE,
      fundCode: FUND_CODE,
      amount,
      payerName: "ITN Integration Visitor",
      payerPhone: "0715550000",
      payerEmail: "itn-integration@churpay.test",
    }),
  });
  const json = await response.json();
  assert.equal(response.status, 201);
  assert.ok(json?.data?.mPaymentId);
  assert.ok(json?.data?.totalCharged);
  return json.data;
}

if (!BASE_URL || !JOIN_CODE || !TEST_ITN_ENABLED) {
  test("payfast ITN integration skipped (set TEST_BASE_URL, TEST_JOIN_CODE, TEST_ITN_ENABLED=1)", { skip: true }, () => {});
} else {
  test("ITN rejects signed gross amount mismatch against amount_gross", async () => {
    const intent = await createVisitorIntent(10);
    const mismatchedGross = toMoney(Number(intent.totalCharged) + 1).toFixed(2);

    const body = buildSignedItnBody({
      m_payment_id: intent.mPaymentId,
      pf_payment_id: `TEST-${Date.now()}`,
      payment_status: "COMPLETE",
      item_name: "Integration Test Giving",
      item_description: "",
      amount_gross: mismatchedGross,
      amount_fee: "-2.00",
      amount_net: "8.00",
      custom_str1: "",
      custom_str2: "",
      custom_str3: "",
      custom_str4: "",
      custom_str5: "",
      custom_int1: "",
      custom_int2: "",
      custom_int3: "",
      custom_int4: "",
      custom_int5: "",
      name_first: "ITN",
      name_last: "Visitor",
      email_address: "itn-integration@churpay.test",
      merchant_id: process.env.TEST_PAYFAST_MERCHANT_ID || process.env.PAYFAST_MERCHANT_ID || "",
    });

    const response = await fetch(`${BASE_URL}/webhooks/payfast/itn`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body,
    });

    const text = await response.text();
    assert.equal(response.status, 400);
    assert.match(text, /amount mismatch/i);
  });
}

