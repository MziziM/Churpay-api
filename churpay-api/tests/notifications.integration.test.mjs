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
  test("notifications integration skipped (set TEST_BASE_URL)", { skip: true }, () => {});
} else {
  test(
    "admin confirming cash giving creates an unread notification for member",
    { skip: !MEMBER_IDENTIFIER || !MEMBER_PASSWORD || !ADMIN_IDENTIFIER || !ADMIN_PASSWORD },
    async () => {
      const memberLogin = await postJson("/api/auth/login/member", {
        identifier: MEMBER_IDENTIFIER,
        password: MEMBER_PASSWORD,
      });
      assert.equal(memberLogin.response.status, 200);
      assert.ok(memberLogin.json?.token);
      const memberToken = memberLogin.json.token;

      // Make the test deterministic.
      const readAll = await postJson("/api/notifications/read-all", {}, memberToken);
      assert.ok([200, 404].includes(readAll.response.status)); // 404 if notifications not enabled on that env
      if (readAll.response.status === 404) return;

      const unread0 = await getJson("/api/notifications/unread-count", memberToken);
      assert.equal(unread0.response.status, 200);
      assert.equal(Number(unread0.json?.count || 0), 0);

      const funds = await getJson("/api/funds", memberToken);
      assert.equal(funds.response.status, 200);
      assert.ok(Array.isArray(funds.json?.funds));
      assert.ok(funds.json.funds.length > 0);
      const fundId = funds.json.funds[0].id;
      assert.ok(fundId);

      const prepared = await postJson("/api/cash-givings", { fundId, amount: 10, flow: "prepared" }, memberToken);
      assert.equal(prepared.response.status, 201);
      const pi = prepared.json.paymentIntentId;
      assert.ok(pi);

      const adminLogin = await postJson("/api/auth/login/admin", {
        identifier: ADMIN_IDENTIFIER,
        password: ADMIN_PASSWORD,
      });
      assert.equal(adminLogin.response.status, 200);
      assert.ok(adminLogin.json?.token);
      const adminToken = adminLogin.json.token;

      const confirm = await postJson(`/api/admin/cash-givings/${pi}/confirm`, {}, adminToken);
      assert.equal(confirm.response.status, 200);
      assert.equal(String(confirm.json?.cashGiving?.status || "").toUpperCase(), "CONFIRMED");

      const unread1 = await getJson("/api/notifications/unread-count", memberToken);
      assert.equal(unread1.response.status, 200);
      assert.equal(Number(unread1.json?.count || 0), 1);

      const list = await getJson("/api/notifications?limit=5", memberToken);
      assert.equal(list.response.status, 200);
      assert.ok(Array.isArray(list.json?.notifications));
      assert.ok(list.json.notifications.length >= 1);
      assert.equal(String(list.json.notifications[0]?.type || ""), "CASH_CONFIRMED");

      const notificationId = list.json.notifications[0].id;
      assert.ok(notificationId);

      const markRead = await postJson(`/api/notifications/${notificationId}/read`, {}, memberToken);
      assert.equal(markRead.response.status, 200);
      assert.ok(markRead.json?.notification?.readAt);

      const unread2 = await getJson("/api/notifications/unread-count", memberToken);
      assert.equal(unread2.response.status, 200);
      assert.equal(Number(unread2.json?.count || 0), 0);
    }
  );
}

