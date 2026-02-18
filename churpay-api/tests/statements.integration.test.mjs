import assert from "node:assert/strict";
import test from "node:test";

const BASE_URL = process.env.TEST_BASE_URL || "";
const ADMIN_IDENTIFIER = process.env.TEST_ADMIN_IDENTIFIER || "";
const ADMIN_PASSWORD = process.env.TEST_ADMIN_PASSWORD || "";

async function postJson(path, payload) {
  const response = await fetch(`${BASE_URL}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const text = await response.text();
  const json = text ? JSON.parse(text) : null;
  return { response, json };
}

if (!BASE_URL || !ADMIN_IDENTIFIER || !ADMIN_PASSWORD) {
  test("statements integration skipped (set TEST_BASE_URL, TEST_ADMIN_IDENTIFIER, TEST_ADMIN_PASSWORD)", { skip: true }, () => {});
} else {
  test("admin statements summary endpoint responds with totals and breakdown", async () => {
    const login = await postJson("/api/auth/login/admin", {
      identifier: ADMIN_IDENTIFIER,
      password: ADMIN_PASSWORD,
    });
    assert.equal(login.response.status, 200);
    const token = login.json?.token;
    assert.ok(token);

    const res = await fetch(`${BASE_URL}/api/admin/statements/summary`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    assert.equal(res.status, 200);
    const json = await res.json();
    assert.ok(json?.summary);
    assert.ok(Object.prototype.hasOwnProperty.call(json.summary, "donationTotal"));
    assert.ok(Object.prototype.hasOwnProperty.call(json.summary, "feeTotal"));
    assert.ok(Object.prototype.hasOwnProperty.call(json.summary, "payfastFeeTotal"));
    assert.ok(Object.prototype.hasOwnProperty.call(json.summary, "netReceivedTotal"));
    assert.ok(Object.prototype.hasOwnProperty.call(json.summary, "totalCharged"));
    assert.ok(Object.prototype.hasOwnProperty.call(json.summary, "transactionCount"));
    assert.ok(json?.breakdown?.byFund);
    assert.ok(json?.breakdown?.byMethod);
  });

  test("admin statements export returns CSV with totals row", async () => {
    const login = await postJson("/api/auth/login/admin", {
      identifier: ADMIN_IDENTIFIER,
      password: ADMIN_PASSWORD,
    });
    assert.equal(login.response.status, 200);
    const token = login.json?.token;
    assert.ok(token);

    const res = await fetch(`${BASE_URL}/api/admin/statements/export`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    assert.equal(res.status, 200);
    const csv = await res.text();
    assert.ok(
      csv.includes(
        "reference,status,provider,channel,donationAmount,churpayFeeAmount,payfastFeeAmount,netReceivedAmount,totalCharged"
      )
    );
    assert.ok(csv.includes("\nTOTAL,"));
  });

  test("admin statements print returns HTML containing Churpay branding", async () => {
    const login = await postJson("/api/auth/login/admin", {
      identifier: ADMIN_IDENTIFIER,
      password: ADMIN_PASSWORD,
    });
    assert.equal(login.response.status, 200);
    const token = login.json?.token;
    assert.ok(token);

    const res = await fetch(`${BASE_URL}/api/admin/statements/print?from=2026-02-01&to=2026-02-29`, {
      headers: { Authorization: `Bearer ${token}`, Accept: "text/html" },
    });
    assert.equal(res.status, 200);
    const contentType = res.headers.get("content-type") || "";
    assert.ok(contentType.includes("text/html"));
    const html = await res.text();
    assert.ok(html.toLowerCase().includes("churpay statement"));
    assert.ok(html.includes("churpay-logo.svg"));
  });
}
