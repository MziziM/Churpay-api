import { db } from "./db.js";
import { sendEmail } from "./email-delivery.js";
import {
  createVerificationChallenge,
  verificationCodeMatches,
  verificationExpired,
  verificationTokenMatches,
} from "./email-verification.js";

const LOGIN_2FA_COLUMNS = [
  "id",
  "role",
  "member_id",
  "email",
  "code_hash",
  "token_hash",
  "attempts",
  "max_attempts",
  "expires_at",
  "consumed_at",
  "updated_at",
];

let loginTwoFactorSupportPromise = null;

function parseBool(value, fallback = false) {
  if (typeof value === "undefined" || value === null || value === "") return fallback;
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

function normalizeEmail(value) {
  if (!value) return null;
  const normalized = String(value).trim().toLowerCase();
  return normalized || null;
}

function parsePositiveInt(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

export function adminTwoFactorEnabled() {
  return parseBool(process.env.ADMIN_LOGIN_2FA_ENABLED, true);
}

export function superTwoFactorEnabled() {
  return parseBool(process.env.SUPER_LOGIN_2FA_ENABLED, true);
}

export async function supportsLoginTwoFactor() {
  if (!loginTwoFactorSupportPromise) {
    loginTwoFactorSupportPromise = (async () => {
      const rows = await db.any(
        `
        select column_name
        from information_schema.columns
        where table_schema = 'public' and table_name = 'auth_login_challenges'
        `
      );
      const columns = new Set(rows.map((row) => row.column_name));
      return LOGIN_2FA_COLUMNS.every((name) => columns.has(name));
    })().catch((err) => {
      loginTwoFactorSupportPromise = null;
      throw err;
    });
  }
  return loginTwoFactorSupportPromise;
}

export function maskEmail(email) {
  const normalized = normalizeEmail(email);
  if (!normalized) return "";
  const [local, domain] = normalized.split("@");
  if (!domain) return normalized;
  const visible = local.length <= 2 ? local[0] || "*" : `${local.slice(0, 2)}***`;
  return `${visible}@${domain}`;
}

function twoFactorTtlMinutes() {
  return parsePositiveInt(process.env.LOGIN_2FA_TTL_MINUTES, 15);
}

function twoFactorMaxAttempts() {
  return parsePositiveInt(process.env.LOGIN_2FA_MAX_ATTEMPTS, 5);
}

function roleLabel(role) {
  return role === "super" ? "super admin" : "admin";
}

export async function issueLoginTwoFactorChallenge({ role, memberId = null, identifier = "", email, recipientName = "" }) {
  const normalizedRole = String(role || "").toLowerCase();
  if (normalizedRole !== "admin" && normalizedRole !== "super") {
    throw new Error("Invalid two-factor role");
  }

  const normalizedEmail = normalizeEmail(email);
  if (!normalizedEmail) {
    throw new Error("Two-factor sign-in requires an email address");
  }

  if (!(await supportsLoginTwoFactor())) {
    throw new Error("Two-factor sign-in upgrade in progress. Please retry shortly.");
  }

  const challenge = createVerificationChallenge();
  const expiresAt = new Date(Date.now() + twoFactorTtlMinutes() * 60 * 1000);
  const maxAttempts = twoFactorMaxAttempts();

  await db.none(
    `
    update auth_login_challenges
    set consumed_at = now(), updated_at = now()
    where role = $1
      and consumed_at is null
      and (
        ($2::uuid is not null and member_id = $2::uuid)
        or ($2::uuid is null and lower(email) = lower($3))
      )
    `,
    [normalizedRole, memberId, normalizedEmail]
  );

  const inserted = await db.one(
    `
    insert into auth_login_challenges (
      role, member_id, identifier, email, code_hash, token_hash, attempts, max_attempts, expires_at, created_at, updated_at
    ) values (
      $1, $2, nullif($3, ''), $4, $5, $6, 0, $7, $8, now(), now()
    )
    returning id, role, email, expires_at
    `,
    [normalizedRole, memberId, String(identifier || "").trim(), normalizedEmail, challenge.codeHash, challenge.tokenHash, maxAttempts, expiresAt]
  );

  const subject = `Your Churpay ${roleLabel(normalizedRole)} sign-in code`;
  const text = [
    `Hi ${recipientName || "there"},`,
    "",
    `Use this code to complete your ${roleLabel(normalizedRole)} sign-in:`,
    challenge.code,
    "",
    `This code expires at ${expiresAt.toISOString()}.`,
  ].join("\n");
  const html = [
    `<p>Hi ${recipientName || "there"},</p>`,
    `<p>Use this code to complete your ${roleLabel(normalizedRole)} sign-in:</p>`,
    `<p style="font-size:20px;font-weight:700;letter-spacing:2px">${challenge.code}</p>`,
    `<p>This code expires at ${expiresAt.toISOString()}.</p>`,
  ].join("");

  const delivery = await sendEmail({ to: normalizedEmail, subject, text, html });

  return {
    challengeId: inserted.id,
    role: inserted.role,
    channel: "email",
    email: inserted.email,
    emailMasked: maskEmail(inserted.email),
    expiresAt: expiresAt.toISOString(),
    provider: delivery?.provider || "log",
  };
}

export async function verifyLoginTwoFactorChallenge({ challengeId, role, code, token }) {
  const normalizedRole = String(role || "").toLowerCase();
  if (normalizedRole !== "admin" && normalizedRole !== "super") {
    return { ok: false, status: 400, code: "TWO_FACTOR_INVALID_ROLE", error: "Invalid two-factor role." };
  }

  const normalizedChallengeId = String(challengeId || "").trim();
  const normalizedCode = typeof code === "string" ? code.trim() : "";
  const normalizedToken = typeof token === "string" ? token.trim() : "";

  if (!normalizedChallengeId) {
    return { ok: false, status: 400, code: "TWO_FACTOR_CHALLENGE_REQUIRED", error: "Two-factor challenge is required." };
  }
  if (!normalizedCode && !normalizedToken) {
    return { ok: false, status: 400, code: "TWO_FACTOR_CODE_REQUIRED", error: "Two-factor code is required." };
  }

  if (!(await supportsLoginTwoFactor())) {
    return { ok: false, status: 503, code: "TWO_FACTOR_UPGRADE", error: "Two-factor sign-in upgrade in progress. Please retry shortly." };
  }

  const row = await db.oneOrNone(
    `
    select
      id,
      role,
      member_id,
      email,
      code_hash,
      token_hash,
      attempts,
      max_attempts,
      expires_at,
      consumed_at
    from auth_login_challenges
    where id = $1::uuid and role = $2
    `,
    [normalizedChallengeId, normalizedRole]
  );

  if (!row) {
    return { ok: false, status: 400, code: "TWO_FACTOR_INVALID", error: "Invalid two-factor challenge." };
  }
  if (row.consumed_at) {
    return { ok: false, status: 400, code: "TWO_FACTOR_ALREADY_USED", error: "This two-factor code has already been used." };
  }
  if (verificationExpired(row.expires_at)) {
    return { ok: false, status: 400, code: "TWO_FACTOR_EXPIRED", error: "Two-factor code has expired. Please sign in again." };
  }
  if (Number(row.attempts || 0) >= Number(row.max_attempts || 5)) {
    return { ok: false, status: 429, code: "TWO_FACTOR_ATTEMPTS_EXCEEDED", error: "Too many failed attempts. Please sign in again." };
  }

  const codeOk = normalizedCode ? verificationCodeMatches(normalizedCode, row.code_hash) : false;
  const tokenOk = normalizedToken ? verificationTokenMatches(normalizedToken, row.token_hash) : false;

  if (!codeOk && !tokenOk) {
    await db.none(
      `
      update auth_login_challenges
      set attempts = attempts + 1, updated_at = now()
      where id = $1::uuid
      `,
      [row.id]
    );
    return { ok: false, status: 400, code: "TWO_FACTOR_INVALID_CODE", error: "Invalid two-factor code." };
  }

  const consumed = await db.oneOrNone(
    `
    update auth_login_challenges
    set consumed_at = now(), updated_at = now()
    where id = $1::uuid and consumed_at is null
    returning id, role, member_id, email
    `,
    [row.id]
  );

  if (!consumed) {
    return { ok: false, status: 400, code: "TWO_FACTOR_ALREADY_USED", error: "This two-factor code has already been used." };
  }

  return {
    ok: true,
    challenge: {
      id: consumed.id,
      role: consumed.role,
      memberId: consumed.member_id || null,
      email: consumed.email || null,
    },
  };
}
