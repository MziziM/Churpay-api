import assert from "node:assert/strict";
import test from "node:test";

const BASE_URL = process.env.TEST_BASE_URL || "";
const MEMBER_IDENTIFIER = process.env.TEST_MEMBER_IDENTIFIER || "";
const MEMBER_PASSWORD = process.env.TEST_MEMBER_PASSWORD || "";

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

async function getJson(path) {
  const response = await fetch(`${BASE_URL}${path}`);
  const text = await response.text();
  const json = text ? JSON.parse(text) : null;
  return { response, json };
}

async function getJsonAuth(path, token) {
  const response = await fetch(`${BASE_URL}${path}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const text = await response.text();
  const json = text ? JSON.parse(text) : null;
  return { response, json };
}

if (!BASE_URL) {
  test("giving links integration skipped (set TEST_BASE_URL)", { skip: true }, () => {});
} else {
  test("member can create giving link + public payer flow returns checkout url", { skip: !MEMBER_IDENTIFIER || !MEMBER_PASSWORD }, async () => {
    const login = await postJson("/api/auth/login/member", {
      identifier: MEMBER_IDENTIFIER,
      password: MEMBER_PASSWORD,
    });
    assert.equal(login.response.status, 200);
    assert.ok(login.json?.token);
    const memberToken = login.json.token;

    const funds = await getJsonAuth("/api/funds", memberToken);
    assert.equal(funds.response.status, 200);
    assert.ok(Array.isArray(funds.json?.funds));
    assert.ok(funds.json.funds.length > 0);
    const fundId = funds.json.funds[0].id;
    assert.ok(fundId);

    const linkRes = await postJson(
      "/api/giving-links",
      {
        fundId,
        amountType: "FIXED",
        amountFixed: 10,
        message: "Integration share link",
        expiresInHours: 48,
        maxUses: 1,
      },
      memberToken
    );
    assert.equal(linkRes.response.status, 201);
    const token = linkRes.json?.data?.givingLink?.token;
    const shareUrl = linkRes.json?.data?.shareUrl;
    assert.ok(token);
    assert.ok(shareUrl);
    assert.ok(String(shareUrl).includes(`/l/${token}`));

    const context = await getJson(`/api/public/giving-links/${encodeURIComponent(token)}`);
    assert.equal(context.response.status, 200);
    assert.equal(String(context.json?.data?.amountType || "").toUpperCase(), "FIXED");
    assert.equal(Number(context.json?.data?.amountFixed || 0), 10);
    assert.ok(context.json?.data?.church?.id);
    assert.ok(context.json?.data?.fund?.id);

    const pay = await postJson(`/api/public/giving-links/${encodeURIComponent(token)}/pay`, {
      payerName: "Integration Payer",
      payerPhone: "0712340000",
      payerEmail: "integration-payer@churpay.test",
    });
    assert.equal(pay.response.status, 201);
    assert.ok(pay.json?.data?.paymentIntentId);
    assert.ok(pay.json?.data?.checkoutUrl);
    assert.equal(String(pay.json?.meta?.source || ""), "SHARE_LINK");
  });

  test("open amount link requires payer amount", { skip: !MEMBER_IDENTIFIER || !MEMBER_PASSWORD }, async () => {
    const login = await postJson("/api/auth/login/member", {
      identifier: MEMBER_IDENTIFIER,
      password: MEMBER_PASSWORD,
    });
    assert.equal(login.response.status, 200);
    const memberToken = login.json.token;

    const funds = await getJsonAuth("/api/funds", memberToken);
    const fundId = funds.json.funds[0].id;

    const linkRes = await postJson(
      "/api/giving-links",
      {
        fundId,
        amountType: "OPEN",
        message: "Open amount link",
        expiresInHours: 48,
        maxUses: 1,
      },
      memberToken
    );
    assert.equal(linkRes.response.status, 201);
    const token = linkRes.json?.data?.givingLink?.token;
    assert.ok(token);

    const payMissing = await postJson(`/api/public/giving-links/${encodeURIComponent(token)}/pay`, {
      payerName: "Integration Payer",
      payerPhone: "0712340000",
    });
    assert.equal(payMissing.response.status, 400);

    const payOk = await postJson(`/api/public/giving-links/${encodeURIComponent(token)}/pay`, {
      payerName: "Integration Payer",
      payerPhone: "0712340000",
      amount: 15,
    });
    assert.equal(payOk.response.status, 201);
    assert.ok(payOk.json?.data?.checkoutUrl);
  });
}

