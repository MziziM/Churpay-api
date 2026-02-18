import assert from "node:assert/strict";
import test from "node:test";

const BASE_URL = process.env.TEST_BASE_URL || "";
const MEMBER_IDENTIFIER = process.env.TEST_MEMBER_IDENTIFIER || "";
const MEMBER_PASSWORD = process.env.TEST_MEMBER_PASSWORD || "";
const SUPER_IDENTIFIER = process.env.TEST_SUPER_IDENTIFIER || "";
const SUPER_PASSWORD = process.env.TEST_SUPER_PASSWORD || "";

async function requestJson(path, { method = "GET", token = "", body } = {}) {
  const response = await fetch(`${BASE_URL}${path}`, {
    method,
    headers: {
      ...(body ? { "Content-Type": "application/json" } : {}),
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const text = await response.text();
  const json = text ? JSON.parse(text) : null;
  return { response, json };
}

async function login(path, identifier, password) {
  const { response, json } = await requestJson(path, {
    method: "POST",
    body: { identifier, password },
  });
  assert.equal(response.status, 200);
  assert.ok(json?.token);
  return json.token;
}

if (!BASE_URL) {
  test("date-of-birth permissions integration skipped (set TEST_BASE_URL)", { skip: true }, () => {});
} else {
  test("member cannot update date of birth from /api/auth/profile/me", { skip: !MEMBER_IDENTIFIER || !MEMBER_PASSWORD }, async () => {
    const memberToken = await login("/api/auth/login/member", MEMBER_IDENTIFIER, MEMBER_PASSWORD);
    const { response, json } = await requestJson("/api/auth/profile/me", {
      method: "PATCH",
      token: memberToken,
      body: { dateOfBirth: "1994-05-27" },
    });
    assert.equal(response.status, 403);
    assert.match(String(json?.error || ""), /super admin/i);
  });

  test(
    "super can update member date of birth",
    { skip: !MEMBER_IDENTIFIER || !MEMBER_PASSWORD || !SUPER_IDENTIFIER || !SUPER_PASSWORD },
    async () => {
      const memberToken = await login("/api/auth/login/member", MEMBER_IDENTIFIER, MEMBER_PASSWORD);
      const me = await requestJson("/api/auth/me", { token: memberToken });
      assert.equal(me.response.status, 200);
      const memberId = me.json?.profile?.id;
      assert.ok(memberId);

      const superToken = await login("/api/super/login", SUPER_IDENTIFIER, SUPER_PASSWORD);
      const updated = await requestJson(`/api/super/members/${encodeURIComponent(memberId)}/date-of-birth`, {
        method: "PATCH",
        token: superToken,
        body: { dateOfBirth: "1994-05-27" },
      });
      assert.equal(updated.response.status, 200);
      assert.equal(String(updated.json?.member?.id || ""), memberId);
      assert.ok(updated.json?.member?.dateOfBirth);
    }
  );
}
