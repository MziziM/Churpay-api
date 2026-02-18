import assert from "node:assert/strict";
import test from "node:test";

const BASE_URL = process.env.TEST_BASE_URL || "";
const SUPER_IDENTIFIER = process.env.TEST_SUPER_IDENTIFIER || "";
const SUPER_PASSWORD = process.env.TEST_SUPER_PASSWORD || "";

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

if (!BASE_URL || !SUPER_IDENTIFIER || !SUPER_PASSWORD) {
  test("super dashboard integration skipped (set TEST_BASE_URL, TEST_SUPER_IDENTIFIER, TEST_SUPER_PASSWORD)", { skip: true }, () => {});
} else {
  test("super login canonical and alias return compatible payloads", async () => {
    const canonical = await postJson("/api/super/login", {
      identifier: SUPER_IDENTIFIER,
      password: SUPER_PASSWORD,
    });
    assert.equal(canonical.response.status, 200);
    assert.ok(canonical.json?.token);
    assert.equal(canonical.json?.ok, true);
    assert.equal(String(canonical.json?.profile?.role || "").toLowerCase(), "super");

    const alias = await postJson("/api/auth/login/super", {
      identifier: SUPER_IDENTIFIER,
      password: SUPER_PASSWORD,
    });
    assert.equal(alias.response.status, 200);
    assert.ok(alias.json?.token);
    assert.equal(alias.json?.ok, true);
    assert.equal(String(alias.json?.profile?.role || "").toLowerCase(), "super");
  });

  test("super dashboard exposes platform fees + payfast fees + processed totals", async () => {
    const login = await postJson("/api/super/login", {
      identifier: SUPER_IDENTIFIER,
      password: SUPER_PASSWORD,
    });
    assert.equal(login.response.status, 200);
    const token = login.json?.token;
    assert.ok(token);

    const summaryRes = await fetch(`${BASE_URL}/api/super/dashboard/summary`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    assert.equal(summaryRes.status, 200);
    const summaryJson = await summaryRes.json();

    assert.ok(summaryJson?.summary);
    assert.ok(Object.prototype.hasOwnProperty.call(summaryJson.summary, "totalProcessed"));
    assert.ok(Object.prototype.hasOwnProperty.call(summaryJson.summary, "totalFeesCollected"));
    assert.ok(Object.prototype.hasOwnProperty.call(summaryJson.summary, "totalPayfastFees"));
    assert.ok(Object.prototype.hasOwnProperty.call(summaryJson.summary, "totalSuperadminCut"));
    assert.ok(Object.prototype.hasOwnProperty.call(summaryJson.summary, "netPlatformRevenue"));
    assert.ok(Array.isArray(summaryJson?.recentTransactions));

    const txRes = await fetch(`${BASE_URL}/api/super/transactions?limit=5`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    assert.equal(txRes.status, 200);
    const txJson = await txRes.json();
    assert.ok(Array.isArray(txJson?.transactions));
    if (txJson.transactions.length > 0) {
      const tx = txJson.transactions[0];
      assert.ok(Object.prototype.hasOwnProperty.call(tx, "platformFeeAmount"));
      assert.ok(Object.prototype.hasOwnProperty.call(tx, "payfastFeeAmount"));
      assert.ok(Object.prototype.hasOwnProperty.call(tx, "churchNetAmount"));
      assert.ok(Object.prototype.hasOwnProperty.call(tx, "amountGross"));
      assert.ok(Object.prototype.hasOwnProperty.call(tx, "superadminCutAmount"));
    }
  });
}
