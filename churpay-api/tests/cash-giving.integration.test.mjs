import assert from "node:assert/strict";
import test from "node:test";

const BASE_URL = process.env.TEST_BASE_URL || "";
const MEMBER_IDENTIFIER = process.env.TEST_MEMBER_IDENTIFIER || "";
const MEMBER_PASSWORD = process.env.TEST_MEMBER_PASSWORD || "";
const ADMIN_IDENTIFIER = process.env.TEST_ADMIN_IDENTIFIER || "";
const ADMIN_PASSWORD = process.env.TEST_ADMIN_PASSWORD || "";

async function postJson(path, payload, token = null) {
  const headers = { "Content-Type": "application/json" };
  if (token) headers.Authorization = `Bearer ${token}`;
  const response = await fetch(`${BASE_URL}${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  });
  const text = await response.text();
  const json = text ? JSON.parse(text) : null;
  return { response, json };
}

async function getJson(path, token) {
  const response = await fetch(`${BASE_URL}${path}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const text = await response.text();
  const json = text ? JSON.parse(text) : null;
  return { response, json };
}

if (!BASE_URL) {
  test("cash giving integration skipped (set TEST_BASE_URL)", { skip: true }, () => {});
} else {
  test("member can create prepared + recorded cash giving and fee is 0 by default", { skip: !MEMBER_IDENTIFIER || !MEMBER_PASSWORD }, async () => {
    const login = await postJson("/api/auth/login/member", {
      identifier: MEMBER_IDENTIFIER,
      password: MEMBER_PASSWORD,
    });
    assert.equal(login.response.status, 200);
    assert.ok(login.json?.token);
    const memberToken = login.json.token;

    const funds = await getJson("/api/funds", memberToken);
    assert.equal(funds.response.status, 200);
    assert.ok(Array.isArray(funds.json?.funds));
    assert.ok(funds.json.funds.length > 0);
    const fundId = funds.json.funds[0].id;
    assert.ok(fundId);

    const prepared = await postJson(
      "/api/cash-givings",
      { fundId, amount: 10, flow: "prepared", serviceDate: "2026-02-15", notes: "Prepared test" },
      memberToken
    );
    assert.equal(prepared.response.status, 201);
    assert.equal(String(prepared.json?.method || ""), "CASH");
    assert.equal(String(prepared.json?.status || "").toUpperCase(), "PREPARED");
    assert.equal(Number(prepared.json?.pricing?.churpayFee || 0), 0);
    assert.equal(Number(prepared.json?.pricing?.totalCharged || 0), 10);
    assert.ok(prepared.json?.paymentIntentId);

    const recorded = await postJson(
      "/api/cash-givings",
      { fundId, amount: 10, flow: "recorded", serviceDate: "2026-02-15", notes: "Recorded test" },
      memberToken
    );
    assert.equal(recorded.response.status, 201);
    assert.equal(String(recorded.json?.status || "").toUpperCase(), "RECORDED");
    assert.equal(Number(recorded.json?.pricing?.churpayFee || 0), 0);
    assert.equal(Number(recorded.json?.pricing?.totalCharged || 0), 10);
    assert.ok(recorded.json?.paymentIntentId);
  });

  test("only admin can confirm cash giving", { skip: !MEMBER_IDENTIFIER || !MEMBER_PASSWORD || !ADMIN_IDENTIFIER || !ADMIN_PASSWORD }, async () => {
    const memberLogin = await postJson("/api/auth/login/member", {
      identifier: MEMBER_IDENTIFIER,
      password: MEMBER_PASSWORD,
    });
    assert.equal(memberLogin.response.status, 200);
    const memberToken = memberLogin.json.token;

    const adminLogin = await postJson("/api/auth/login/admin", {
      identifier: ADMIN_IDENTIFIER,
      password: ADMIN_PASSWORD,
    });
    assert.equal(adminLogin.response.status, 200);
    const adminToken = adminLogin.json.token;

    const funds = await getJson("/api/funds", memberToken);
    const fundId = funds.json.funds[0].id;

    const prepared = await postJson("/api/cash-givings", { fundId, amount: 10, flow: "prepared" }, memberToken);
    assert.equal(prepared.response.status, 201);
    const pi = prepared.json.paymentIntentId;

    const memberAttempt = await postJson(`/api/admin/cash-givings/${pi}/confirm`, {}, memberToken);
    assert.notEqual(memberAttempt.response.status, 200);

    const adminConfirm = await postJson(`/api/admin/cash-givings/${pi}/confirm`, {}, adminToken);
    assert.equal(adminConfirm.response.status, 200);
    assert.equal(String(adminConfirm.json?.cashGiving?.status || "").toUpperCase(), "CONFIRMED");
  });
}

