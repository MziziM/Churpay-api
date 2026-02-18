import assert from "node:assert/strict";
import test from "node:test";

const BASE_URL = process.env.TEST_BASE_URL || "";
const ADMIN_IDENTIFIER = process.env.TEST_ADMIN_IDENTIFIER || "";
const ADMIN_PASSWORD = process.env.TEST_ADMIN_PASSWORD || "";
const MEMBER_IDENTIFIER = process.env.TEST_MEMBER_IDENTIFIER || "";
const MEMBER_PASSWORD = process.env.TEST_MEMBER_PASSWORD || "";
const SUPER_IDENTIFIER = process.env.TEST_SUPER_IDENTIFIER || "";
const SUPER_PASSWORD = process.env.TEST_SUPER_PASSWORD || "";
const ADMIN_2FA_CODE = process.env.TEST_ADMIN_2FA_CODE || "";
const SUPER_2FA_CODE = process.env.TEST_SUPER_2FA_CODE || "";

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

if (!BASE_URL) {
  test("auth integration skipped (set TEST_BASE_URL)", { skip: true }, () => {});
} else {
  test("health endpoint returns ok", async () => {
    const response = await fetch(`${BASE_URL}/health`);
    assert.equal(response.status, 200);
    const json = await response.json();
    assert.equal(json.ok, true);
  });

  test("admin login and /api/auth/me work", { skip: !ADMIN_IDENTIFIER || !ADMIN_PASSWORD }, async () => {
    const login = await postJson("/api/auth/login/admin", {
      identifier: ADMIN_IDENTIFIER,
      password: ADMIN_PASSWORD,
    });
    assert.equal(login.response.status, 200, JSON.stringify(login.json || {}));

    let token = login.json?.token || "";
    if (!token && login.json?.requiresTwoFactor) {
      assert.ok(login.json?.twoFactor?.challengeId);
      if (ADMIN_2FA_CODE) {
        const verify = await postJson("/api/auth/login/admin/verify-2fa", {
          challengeId: login.json.twoFactor.challengeId,
          code: ADMIN_2FA_CODE,
        });
        assert.equal(verify.response.status, 200, JSON.stringify(verify.json || {}));
        token = verify.json?.token || "";
      } else {
        return;
      }
    }

    assert.ok(token);
    const me = await fetch(`${BASE_URL}/api/auth/me`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    assert.equal(me.status, 200);
    const meJson = await me.json();
    assert.ok(meJson?.profile?.id);
    assert.equal(String(meJson?.profile?.role || "").toLowerCase(), "admin");
  });

  test("member login works", { skip: !MEMBER_IDENTIFIER || !MEMBER_PASSWORD }, async () => {
    const login = await postJson("/api/auth/login/member", {
      identifier: MEMBER_IDENTIFIER,
      password: MEMBER_PASSWORD,
    });
    assert.equal(login.response.status, 200);
    assert.ok(login.json?.token);
    assert.equal(String(login.json?.profile?.role || "").toLowerCase(), "member");
  });

  test("super login alias works", { skip: !SUPER_IDENTIFIER || !SUPER_PASSWORD }, async () => {
    const login = await postJson("/api/auth/login/super", {
      identifier: SUPER_IDENTIFIER,
      password: SUPER_PASSWORD,
    });
    assert.equal(login.response.status, 200, JSON.stringify(login.json || {}));

    let token = login.json?.token || "";
    let profile = login.json?.profile || null;
    if (!token && login.json?.requiresTwoFactor) {
      assert.ok(login.json?.twoFactor?.challengeId);
      if (SUPER_2FA_CODE) {
        const verify = await postJson("/api/auth/login/super/verify-2fa", {
          challengeId: login.json.twoFactor.challengeId,
          code: SUPER_2FA_CODE,
        });
        assert.equal(verify.response.status, 200, JSON.stringify(verify.json || {}));
        token = verify.json?.token || "";
        profile = verify.json?.profile || null;
      } else {
        return;
      }
    }

    assert.ok(token);
    assert.equal(String(profile?.role || "").toLowerCase(), "super");
  });
}
