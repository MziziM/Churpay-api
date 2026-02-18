import assert from "node:assert/strict";
import test from "node:test";

const BASE_URL = process.env.TEST_BASE_URL || "";
const JOIN_CODE = process.env.TEST_JOIN_CODE || "";
const FUND_CODE = process.env.TEST_FUND_CODE || "general";

function toMoney(value) {
  return Math.round(Number(value) * 100) / 100;
}

if (!BASE_URL || !JOIN_CODE) {
  test("public give integration skipped (set TEST_BASE_URL and TEST_JOIN_CODE)", { skip: true }, () => {});
} else {
  test("public give context returns church/fund/pricing", async () => {
    const response = await fetch(
      `${BASE_URL}/api/public/give/context?joinCode=${encodeURIComponent(JOIN_CODE)}&fund=${encodeURIComponent(FUND_CODE)}`
    );
    assert.equal(response.status, 200);
    const json = await response.json();
    assert.ok(json?.data?.church?.id);
    assert.ok(json?.data?.fund?.id);
    assert.ok(Number.isFinite(Number(json?.data?.pricing?.fixed)));
    assert.ok(Number.isFinite(Number(json?.data?.pricing?.pct)));
  });

  test("public give payment intent uses fee formula and visitor metadata", async () => {
    const amount = 10;
    const response = await fetch(`${BASE_URL}/api/public/give/payment-intents`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        joinCode: JOIN_CODE,
        fundCode: FUND_CODE,
        amount,
        payerName: "Integration Visitor",
        payerPhone: "0712345678",
        payerEmail: "integration-visitor@churpay.test",
      }),
    });
    assert.equal(response.status, 201);
    const json = await response.json();
    assert.ok(json?.data?.paymentIntentId);
    assert.ok(json?.data?.checkoutUrl);
    const expectedFee = toMoney(2.5 + amount * 0.0075);
    const expectedTotal = toMoney(amount + expectedFee);
    assert.equal(toMoney(json?.data?.processingFee), expectedFee);
    assert.equal(toMoney(json?.data?.totalCharged), expectedTotal);
    assert.equal(json?.meta?.payerType, "visitor");
  });
}
