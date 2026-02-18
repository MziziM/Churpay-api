import assert from "node:assert/strict";
import test from "node:test";

const BASE_URL = process.env.TEST_BASE_URL || "";
const IDENTIFIER = process.env.TEST_PASSWORD_RESET_IDENTIFIER || process.env.TEST_MEMBER_IDENTIFIER || "";

async function postJson(path, payload) {
  const response = await fetch(`${BASE_URL}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const text = await response.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch (_) {}
  return { response, json, text };
}

if (!BASE_URL) {
  test("password reset integration skipped (set TEST_BASE_URL)", { skip: true }, () => {});
} else {
  test("password reset request endpoint exists and returns ok", { skip: !IDENTIFIER }, async () => {
    const res = await postJson("/api/auth/password-reset/request", { identifier: IDENTIFIER });
    assert.equal(res.response.status, 200);
    assert.equal(res.json?.ok, true);
  });
}

