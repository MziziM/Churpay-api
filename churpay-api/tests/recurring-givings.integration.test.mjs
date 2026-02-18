import assert from "node:assert/strict";
import test from "node:test";

const BASE_URL = process.env.TEST_BASE_URL || "";
const MEMBER_IDENTIFIER = process.env.TEST_MEMBER_IDENTIFIER || "";
const MEMBER_PASSWORD = process.env.TEST_MEMBER_PASSWORD || "";
const RECURRING_ENABLED =
  ["1", "true", "yes", "on"].includes(String(process.env.RECURRING_GIVING_ENABLED ?? "").toLowerCase());

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
  test("recurring givings integration skipped (set TEST_BASE_URL)", { skip: true }, () => {});
} else {
  test(
    "member can create/list/cancel recurring giving",
    { skip: !MEMBER_IDENTIFIER || !MEMBER_PASSWORD || !RECURRING_ENABLED },
    async () => {
    const login = await postJson("/api/auth/login/member", {
      identifier: MEMBER_IDENTIFIER,
      password: MEMBER_PASSWORD,
    });
    assert.equal(login.response.status, 200);
    assert.ok(login.json?.token);
    const token = login.json.token;

    const funds = await getJson("/api/funds", token);
    assert.equal(funds.response.status, 200);
    assert.ok(Array.isArray(funds.json?.funds));
    assert.ok(funds.json.funds.length > 0);
    const fundId = funds.json.funds[0].id;
    assert.ok(fundId);

    const create = await postJson(
      "/api/recurring-givings",
      {
        fundId,
        amount: 15,
        frequency: "monthly",
        cycles: 0,
        notes: "Recurring integration test",
      },
      token
    );
    assert.equal(create.response.status, 201);
    const recurringId = create.json?.data?.recurringGiving?.id;
    assert.ok(recurringId);
    assert.ok(create.json?.data?.checkoutUrl);
    assert.equal(Number(create.json?.data?.pricing?.donationAmount || 0), 15);

    const list = await getJson("/api/recurring-givings?limit=10", token);
    assert.equal(list.response.status, 200);
    assert.ok(Array.isArray(list.json?.recurringGivings));
    assert.ok(list.json.recurringGivings.find((r) => r.id === recurringId));

    const cancel = await postJson(`/api/recurring-givings/${recurringId}/cancel`, {}, token);
    assert.equal(cancel.response.status, 200);
    assert.equal(String(cancel.json?.recurringGiving?.status || "").toUpperCase(), "CANCELLED");
  });

  test(
    "invalid recurring frequency is rejected",
    { skip: !MEMBER_IDENTIFIER || !MEMBER_PASSWORD || !RECURRING_ENABLED },
    async () => {
    const login = await postJson("/api/auth/login/member", {
      identifier: MEMBER_IDENTIFIER,
      password: MEMBER_PASSWORD,
    });
    assert.equal(login.response.status, 200);
    const token = login.json.token;

    const funds = await getJson("/api/funds", token);
    const fundId = funds.json.funds[0].id;

    const create = await postJson(
      "/api/recurring-givings",
      {
        fundId,
        amount: 10,
        frequency: "every-minute",
      },
      token
    );
    assert.equal(create.response.status, 400);
    }
  );
}
