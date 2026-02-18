import express from "express";
import bcrypt from "bcryptjs";
import { db } from "./db.js";
import { authenticateSuperAdmin, getSuperAdminConfig, requireAuth, requireAdmin, signSuperToken, signUserToken } from "./auth.js";
import { ensureUniqueJoinCode, normalizeJoinCode } from "./join-code.js";
import { sendEmail } from "./email-delivery.js";
import {
  createVerificationChallenge,
  verificationCodeMatches,
  verificationExpired,
  verificationTokenMatches,
} from "./email-verification.js";
import {
  adminTwoFactorEnabled,
  issueLoginTwoFactorChallenge,
  superTwoFactorEnabled,
  verifyLoginTwoFactorChallenge,
} from "./login-two-factor.js";

const router = express.Router();

function normalizeEmail(email) {
  if (!email) return null;
  const trimmed = String(email).trim().toLowerCase();
  return trimmed || null;
}

function normalizePhone(phone) {
  if (!phone) return null;
  const trimmed = String(phone).trim();
  return trimmed || null;
}

function parseBool(value, fallback = false) {
  if (typeof value === "undefined" || value === null || value === "") return fallback;
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

function normalizeDateOfBirth(value) {
  if (!value) return null;
  const raw = String(value).trim();
  if (!raw) return null;

  let year = "";
  let month = "";
  let day = "";
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    [year, month, day] = raw.split("-");
  } else if (/^\d{2}[-/]\d{2}[-/]\d{4}$/.test(raw)) {
    const [d, m, y] = raw.split(/[-/]/);
    year = y;
    month = m;
    day = d;
  } else {
    return null;
  }

  const y = Number.parseInt(year, 10);
  const m = Number.parseInt(month, 10);
  const d = Number.parseInt(day, 10);
  if (!Number.isInteger(y) || !Number.isInteger(m) || !Number.isInteger(d)) return null;
  if (y < 1900 || m < 1 || m > 12 || d < 1 || d > 31) return null;

  const parsed = new Date(Date.UTC(y, m - 1, d));
  if (parsed.getUTCFullYear() !== y || parsed.getUTCMonth() !== m - 1 || parsed.getUTCDate() !== d) return null;

  const today = new Date();
  const todayUtc = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()));
  if (parsed > todayUtc) return null;

  return `${String(y).padStart(4, "0")}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}

async function getLegalDocumentVersion(docKey) {
  const key = String(docKey || "").trim().toLowerCase();
  if (!key) return null;
  try {
    const row = await db.oneOrNone(`select version from legal_documents where doc_key = $1`, [key]);
    const version = row?.version;
    return Number.isFinite(Number(version)) ? Number(version) : null;
  } catch (err) {
    if (err?.code === "42P01") return null; // not migrated yet
    throw err;
  }
}

function serializeDateOfBirth(value) {
  if (!value) return null;

  if (value instanceof Date && Number.isFinite(value.getTime())) {
    return value.toISOString().slice(0, 10);
  }

  const raw = String(value).trim();
  if (!raw) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;

  const normalized = normalizeDateOfBirth(raw);
  if (normalized) return normalized;

  const parsed = new Date(raw);
  if (!Number.isFinite(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

function isAdminRole(role) {
  return role === "admin" || role === "accountant" || role === "super";
}

function requiresAdminTwoFactor(role) {
  const normalizedRole = String(role || "").toLowerCase();
  return normalizedRole === "admin" || normalizedRole === "super";
}

function toChurchProfile(row) {
  if (!row) return null;
  return {
    id: row.id,
    name: row.name || null,
    joinCode: row.join_code || null,
    createdAt: row.created_at || null,
  };
}

function toProfile(row) {
  if (!row) return null;
  const dob = serializeDateOfBirth(row.date_of_birth || row.dateOfBirth || null);
  const token = String(row.payfast_adhoc_token || "").trim();
  return {
    id: row.id,
    fullName: row.full_name || row.fullName || row.name || null,
    phone: row.phone,
    email: row.email,
    role: row.role,
    churchId: row.church_id,
    churchName: row.church_name || null,
    dateOfBirth: dob,
    hasSavedCard: Boolean(token) && !row.payfast_adhoc_token_revoked_at,
  };
}

function isRecoverableSqlError(err) {
  return err?.code === "42P01" || err?.code === "42703" || err?.code === "42P18";
}

function isUniqueViolation(err) {
  return err?.code === "23505";
}

function internalErrorPayload(err) {
  const payload = { error: "Internal server error" };
  if (process.env.NODE_ENV !== "production") {
    payload.detail = err?.message || String(err);
  }
  return payload;
}

function twoFactorResponsePayload(challenge) {
  return {
    ok: true,
    requiresTwoFactor: true,
    code: "TWO_FACTOR_REQUIRED",
    twoFactor: {
      challengeId: challenge.challengeId,
      channel: challenge.channel,
      emailMasked: challenge.emailMasked,
      expiresAt: challenge.expiresAt,
      role: challenge.role,
    },
  };
}

let membersColumnsPromise = null;
const MEMBER_EMAIL_VERIFICATION_COLUMNS = [
  "email_verified",
  "email_verified_at",
  "email_verification_token_hash",
  "email_verification_code_hash",
  "email_verification_expires_at",
  "email_verification_sent_at",
];
const MEMBER_PASSWORD_RESET_COLUMNS = [
  "password_reset_token_hash",
  "password_reset_code_hash",
  "password_reset_expires_at",
  "password_reset_sent_at",
  "password_reset_used_at",
];
const MEMBER_ACTIVITY_COLUMNS = ["last_active_at"];

function normalizeAuthRow(row, source) {
  if (!row) return null;
  return {
    id: row.id,
    full_name: row.full_name || row.name || null,
    phone: row.phone || null,
    email: row.email || null,
    role: row.role || "member",
    church_id: row.church_id || null,
    date_of_birth: row.date_of_birth || null,
    password_hash: row.password_hash || null,
    password: row.password || null,
    auth_source: source,
  };
}

async function findAuthMember({ normalizedPhone, normalizedEmail }) {
  try {
    const row = await db.oneOrNone(
      `select id, full_name, phone, email, role, church_id, password_hash
       from members
       where (coalesce($1::text, '') <> '' and phone::text = $1::text)
          or (coalesce($2::text, '') <> '' and lower(email::text) = lower($2::text))
       limit 1`,
      [normalizedPhone, normalizedEmail]
    );
    if (row) return normalizeAuthRow(row, "members");
  } catch (err) {
    if (!isRecoverableSqlError(err)) throw err;
  }

  if (normalizedPhone) {
    try {
      const row = await db.oneOrNone("select * from users where phone=$1 limit 1", [normalizedPhone]);
      if (row) return normalizeAuthRow(row, "users");
    } catch (err) {
      if (!isRecoverableSqlError(err)) throw err;
    }
  }

  if (normalizedEmail) {
    try {
      const row = await db.oneOrNone("select * from users where lower(email)=lower($1) limit 1", [normalizedEmail]);
      if (row) return normalizeAuthRow(row, "users");
    } catch (err) {
      if (!isRecoverableSqlError(err)) throw err;
    }
  }

  return null;
}

async function checkPassword(member, plainPassword) {
  if (!member || typeof plainPassword !== "string" || !plainPassword) return false;

  if (typeof member.password_hash === "string" && member.password_hash) {
    const match = await bcrypt.compare(plainPassword, member.password_hash).catch(() => false);
    if (match) return true;
  }

  // Backwards compatibility for legacy users table that may store plain text passwords.
  if (member.auth_source === "users" && typeof member.password === "string" && member.password) {
    const bcryptMatch = await bcrypt.compare(plainPassword, member.password).catch(() => false);
    if (bcryptMatch) return true;
    return member.password === plainPassword;
  }

  return false;
}

async function listPublicTableColumns(tableName) {
  const rows = await db.any(
    `select column_name
     from information_schema.columns
     where table_schema = 'public' and table_name = $1::text`,
    [tableName]
  );
  return new Set(rows.map((r) => r.column_name));
}

async function getMembersTableColumns() {
  if (!membersColumnsPromise) {
    membersColumnsPromise = listPublicTableColumns("members").catch((err) => {
      membersColumnsPromise = null;
      throw err;
    });
  }
  return membersColumnsPromise;
}

async function supportsMemberEmailVerification() {
  const columns = await getMembersTableColumns();
  return MEMBER_EMAIL_VERIFICATION_COLUMNS.every((name) => columns.has(name));
}

async function supportsMemberPasswordReset() {
  const columns = await getMembersTableColumns();
  return MEMBER_PASSWORD_RESET_COLUMNS.every((name) => columns.has(name));
}

async function supportsMemberActivityTracking() {
  const columns = await getMembersTableColumns();
  return MEMBER_ACTIVITY_COLUMNS.every((name) => columns.has(name));
}

async function touchMemberLastActive(memberId) {
  const id = String(memberId || "").trim();
  if (!id) return false;

  try {
    if (!(await supportsMemberActivityTracking())) return false;
  } catch (_err) {
    return false;
  }

  try {
    await db.none(`update members set last_active_at = now() where id = $1`, [id]);
    return true;
  } catch (_err) {
    return false;
  }
}

function normalizePublicApiBase() {
  const base = String(process.env.PUBLIC_BASE_URL || "https://api.churpay.com").trim();
  return base.replace(/\/+$/, "");
}

function memberVerificationLink(identifier, token) {
  const base = normalizePublicApiBase();
  return `${base}/api/auth/verify-email?identifier=${encodeURIComponent(String(identifier || "").trim())}&token=${encodeURIComponent(token)}`;
}

async function issueMemberEmailVerification(member) {
  const memberId = member?.id;
  const recipientEmail = normalizeEmail(member?.email);
  if (!memberId || !recipientEmail) {
    return {
      verificationRequired: false,
      delivery: { ok: false, provider: "none" },
      expiresAt: null,
    };
  }

  if (!(await supportsMemberEmailVerification())) {
    throw new Error("Email verification upgrade in progress. Please retry shortly.");
  }

  const challenge = createVerificationChallenge();
  await db.none(
    `
    update members
    set
      email_verified = false,
      email_verified_at = null,
      email_verification_token_hash = $2,
      email_verification_code_hash = $3,
      email_verification_expires_at = $4,
      email_verification_sent_at = now(),
      updated_at = now()
    where id = $1
    `,
    [memberId, challenge.tokenHash, challenge.codeHash, challenge.expiresAt]
  );

  const verifyLink = memberVerificationLink(recipientEmail, challenge.token);
  const subject = "Verify your Churpay email";
  const text = [
    `Hi ${member?.full_name || "there"},`,
    "",
    "Use this code to verify your Churpay account:",
    challenge.code,
    "",
    "Or click this secure link:",
    verifyLink,
    "",
    `This code expires at ${challenge.expiresAt.toISOString()}.`,
  ].join("\n");
  const html = [
    `<p>Hi ${member?.full_name || "there"},</p>`,
    "<p>Use this code to verify your Churpay account:</p>",
    `<p style="font-size:20px;font-weight:700;letter-spacing:2px">${challenge.code}</p>`,
    `<p>Or click this secure link: <a href="${verifyLink}">${verifyLink}</a></p>`,
    `<p>This code expires at ${challenge.expiresAt.toISOString()}.</p>`,
  ].join("");

  const delivery = await sendEmail({ to: recipientEmail, subject, text, html });

  return {
    verificationRequired: true,
    delivery,
    expiresAt: challenge.expiresAt.toISOString(),
  };
}

async function issueMemberPasswordReset(member) {
  const memberId = member?.id;
  const recipientEmail = normalizeEmail(member?.email);
  if (!memberId || !recipientEmail) {
    return { ok: false, skipped: true, provider: "none", expiresAt: null };
  }

  if (!(await supportsMemberPasswordReset())) {
    throw new Error("Password reset upgrade in progress. Please retry shortly.");
  }

  const challenge = createVerificationChallenge();
  await db.none(
    `
    update members
    set
      password_reset_token_hash = $2,
      password_reset_code_hash = $3,
      password_reset_expires_at = $4,
      password_reset_sent_at = now(),
      password_reset_used_at = null,
      updated_at = now()
    where id = $1
    `,
    [memberId, challenge.tokenHash, challenge.codeHash, challenge.expiresAt]
  );

  const subject = "Reset your Churpay password";
  const text = [
    `Hi ${member?.full_name || "there"},`,
    "",
    "You requested a password reset for your Churpay account.",
    "",
    "Use this code in the app to reset your password:",
    challenge.code,
    "",
    `This code expires at ${challenge.expiresAt.toISOString()}.`,
    "",
    "If you did not request this, you can ignore this email.",
  ].join("\n");

  const html = [
    `<p>Hi ${member?.full_name || "there"},</p>`,
    "<p>You requested a password reset for your Churpay account.</p>",
    "<p>Use this code in the app to reset your password:</p>",
    `<p style="font-size:20px;font-weight:700;letter-spacing:2px">${challenge.code}</p>`,
    `<p>This code expires at ${challenge.expiresAt.toISOString()}.</p>`,
    "<p>If you did not request this, you can ignore this email.</p>",
  ].join("");

  const delivery = await sendEmail({ to: recipientEmail, subject, text, html });
  return { ok: true, provider: delivery?.provider || "log", expiresAt: challenge.expiresAt.toISOString() };
}

async function getMemberVerificationRow(memberId) {
  return db.oneOrNone(
    `
    select
      id,
      full_name,
      phone,
      email,
      role,
      church_id,
      coalesce(email_verified, false) as email_verified,
      email_verified_at,
      email_verification_token_hash,
      email_verification_code_hash,
      email_verification_expires_at
    from members
    where id = $1
    `,
    [memberId]
  );
}

async function resolveMemberForVerification(identifierOrEmail, explicitEmail) {
  const identifier = normalizeEmail(explicitEmail) || normalizeEmail(identifierOrEmail);
  const phone = normalizePhone(identifierOrEmail && !String(identifierOrEmail).includes("@") ? identifierOrEmail : null);
  const row = await findAuthMember({ normalizedPhone: phone, normalizedEmail: identifier });
  if (!row || row.auth_source !== "members") return null;
  return getMemberVerificationRow(row.id);
}

async function insertLegacyUser({ full_name, normalizedPhone, normalizedEmail, password_hash, church_id = null, normalizedDateOfBirth = null }) {
  const columns = await listPublicTableColumns("users");
  if (!columns.size) return null;

  const insertCols = [];
  const placeholders = [];
  const params = [];
  const add = (col, value) => {
    insertCols.push(col);
    params.push(value);
    placeholders.push(`$${params.length}`);
  };

  if (columns.has("full_name")) add("full_name", full_name);
  else if (columns.has("name")) add("name", full_name);

  if (columns.has("phone")) add("phone", normalizedPhone);
  if (columns.has("email")) add("email", normalizedEmail);
  if (columns.has("role")) add("role", "member");

  if (columns.has("password_hash")) add("password_hash", password_hash);
  else if (columns.has("password")) add("password", password_hash);
  else throw new Error("users table has no password/password_hash column");

  if (church_id && columns.has("church_id")) add("church_id", church_id);
  if (normalizedDateOfBirth && columns.has("date_of_birth")) add("date_of_birth", normalizedDateOfBirth);

  if (!insertCols.length) throw new Error("users table has no compatible columns for register");

  let inserted = null;
  if (columns.has("id")) {
    inserted = await db.one(
      `insert into users (${insertCols.join(", ")})
       values (${placeholders.join(", ")})
       returning *`,
      params
    );
  } else {
    await db.none(
      `insert into users (${insertCols.join(", ")})
       values (${placeholders.join(", ")})`,
      params
    );
    inserted = await db.oneOrNone(
      `select * from users
       where (coalesce($1::text, '') <> '' and phone::text = $1::text)
          or (coalesce($2::text, '') <> '' and lower(email::text) = lower($2::text))
       limit 1`,
      [normalizedPhone, normalizedEmail]
    );
  }

  return normalizeAuthRow(inserted, "users");
}

async function createAuthMember({
  full_name,
  normalizedPhone,
  normalizedEmail,
  password_hash,
  church_id = null,
  normalizedDateOfBirth = null,
  termsAcceptedAt = null,
  termsVersion = null,
  cookieConsentAt = null,
  cookieConsentVersion = null,
}) {
  try {
    const columns = await getMembersTableColumns();
    const insertColumns = ["full_name", "phone", "email", "password_hash", "role", "church_id"];
    const values = [full_name, normalizedPhone, normalizedEmail, password_hash, "member", church_id];
    if (normalizedDateOfBirth && columns.has("date_of_birth")) {
      insertColumns.push("date_of_birth");
      values.push(normalizedDateOfBirth);
    }
    if (termsAcceptedAt && columns.has("terms_accepted_at")) {
      insertColumns.push("terms_accepted_at");
      values.push(termsAcceptedAt);
    }
    if (termsVersion && columns.has("terms_version")) {
      insertColumns.push("terms_version");
      values.push(Number(termsVersion));
    }
    if (cookieConsentAt && columns.has("cookie_consent_at")) {
      insertColumns.push("cookie_consent_at");
      values.push(cookieConsentAt);
    }
    if (cookieConsentVersion && columns.has("cookie_consent_version")) {
      insertColumns.push("cookie_consent_version");
      values.push(Number(cookieConsentVersion));
    }

    const placeholders = values.map((_, index) => `$${index + 1}`);
    const row = await db.one(
      `insert into members (${insertColumns.join(", ")})
       values (${placeholders.join(", ")})
       returning id, full_name, phone, email, role, church_id${columns.has("date_of_birth") ? ", date_of_birth" : ""}`,
      values
    );
    return normalizeAuthRow(row, "members");
  } catch (err) {
    if (!isRecoverableSqlError(err)) throw err;
  }

  try {
    return await insertLegacyUser({ full_name, normalizedPhone, normalizedEmail, password_hash, church_id, normalizedDateOfBirth });
  } catch (err) {
    if (!isRecoverableSqlError(err)) throw err;
    return null;
  }
}

async function fetchMember(memberId) {
  try {
    const sql = `
      select
        m.id,
        m.full_name,
        m.phone,
        m.email,
        m.role,
        m.church_id,
        m.date_of_birth,
        m.payfast_adhoc_token,
        m.payfast_adhoc_token_revoked_at,
        c.name as church_name
      from members m
      left join churches c on c.id = m.church_id
      where m.id = $1
    `;
    return await db.one(sql, [memberId]);
  } catch (err) {
    // Backward compatible fallback if saved-card columns aren't migrated yet.
    if (err?.code === "42703") {
      const sql = `
        select m.id, m.full_name, m.phone, m.email, m.role, m.church_id, m.date_of_birth, c.name as church_name
        from members m
        left join churches c on c.id = m.church_id
        where m.id = $1
      `;
      return db.one(sql, [memberId]);
    }
    throw err;
  }
}

async function fetchChurch(churchId) {
  return db.oneOrNone(
    `select id, name, join_code, created_at
     from churches
     where id = $1`,
    [churchId]
  );
}

async function handleLogin(req, res, expectedRole = null) {
  const { phone, email, password, identifier } = req.body || {};
  const normalizedPhone = normalizePhone(phone || (identifier && !String(identifier).includes("@") ? identifier : null));
  const normalizedEmail = normalizeEmail(email || (identifier && String(identifier).includes("@") ? identifier : null));
  const pwd = typeof password === "string" ? password : "";

  if (!normalizedPhone && !normalizedEmail) {
    return res.status(400).json({ error: "Phone or email is required" });
  }
  if (!pwd) return res.status(400).json({ error: "Password is required" });

  const row = await findAuthMember({ normalizedPhone, normalizedEmail });
  if (!row) return res.status(401).json({ error: "Invalid credentials" });

  const match = await checkPassword(row, pwd);
  if (!match) return res.status(401).json({ error: "Invalid credentials" });

  if (row.auth_source === "members" && row.id && row.email && row.role === "member") {
    try {
      if (await supportsMemberEmailVerification()) {
        const verification = await getMemberVerificationRow(row.id);
        if (verification?.email && !verification?.email_verified) {
          return res.status(403).json({
            error: "Email verification required",
            code: "EMAIL_VERIFICATION_REQUIRED",
            email: verification.email,
          });
        }
      }
    } catch (err) {
      if (!isRecoverableSqlError(err)) throw err;
    }
  }

  if (expectedRole === "admin" && !isAdminRole(row.role)) {
    return res.status(403).json({ error: "Admin login required" });
  }
  if (expectedRole === "member" && isAdminRole(row.role)) {
    return res.status(403).json({ error: "Use admin login" });
  }

  const shouldRequireAdminTwoFactor =
    adminTwoFactorEnabled() &&
    requiresAdminTwoFactor(row.role) &&
    expectedRole !== "member";

  if (shouldRequireAdminTwoFactor) {
    const destinationEmail = normalizeEmail(row.email);
    if (!destinationEmail) {
      return res.status(400).json({
        error: "Admin account needs an email address before two-factor sign-in can be used.",
        code: "TWO_FACTOR_EMAIL_REQUIRED",
      });
    }

    const challenge = await issueLoginTwoFactorChallenge({
      role: "admin",
      memberId: row.id,
      identifier: normalizedEmail || normalizedPhone || destinationEmail,
      email: destinationEmail,
      recipientName: row.full_name || "there",
    });

    return res.json(twoFactorResponsePayload(challenge));
  }

  if (row.auth_source === "members" && row.id) {
    await touchMemberLastActive(row.id);
  }

  const profile = toProfile(row);
  const token = signUserToken(row);
  return res.json({ token, profile, member: profile });
}

router.post("/register", async (req, res) => {
  try {
    const {
      fullName,
      phone,
      email,
      emailConfirm,
      confirmEmail,
      emailConfirmation,
      password,
      joinCode,
      churchJoinCode,
      churchCode,
      dateOfBirth,
      birthDate,
      dob,
      acceptTerms,
      termsAccepted,
      acceptedTerms,
      acceptCookies,
      cookiesAccepted,
      acceptedCookies,
      cookieConsent,
    } = req.body || {};

    const full_name = typeof fullName === "string" ? fullName.trim() : "";
    const normalizedPhone = normalizePhone(phone);
    const normalizedEmail = normalizeEmail(email);
    const normalizedEmailConfirm = normalizeEmail(emailConfirm || confirmEmail || emailConfirmation);
    const requestedJoinCode = normalizeJoinCode(joinCode || churchJoinCode || churchCode);
    const normalizedDateOfBirth = normalizeDateOfBirth(dateOfBirth || birthDate || dob);
    const didAcceptTerms = parseBool(acceptTerms ?? termsAccepted ?? acceptedTerms, false);
    const didAcceptCookies = parseBool(
      acceptCookies ?? cookiesAccepted ?? acceptedCookies ?? cookieConsent,
      false
    );
    const pwd = typeof password === "string" ? password : "";

    if (!full_name) return res.status(400).json({ error: "Full name is required" });
    if (!normalizedDateOfBirth) return res.status(400).json({ error: "Date of birth is required (DD-MM-YYYY)" });
    if (!pwd || pwd.length < 6) return res.status(400).json({ error: "Password must be at least 6 characters" });
    if (!normalizedPhone) return res.status(400).json({ error: "Phone is required" });
    if (!normalizedEmail) return res.status(400).json({ error: "Email is required" });
    if (!normalizedEmailConfirm) return res.status(400).json({ error: "Email confirmation is required" });
    if (normalizedEmail !== normalizedEmailConfirm) return res.status(400).json({ error: "Email addresses do not match" });
    if (!requestedJoinCode) return res.status(400).json({ error: "Join code is required" });
    if (!didAcceptTerms) return res.status(400).json({ error: "You must accept the Terms and Conditions to create an account." });
    if (!didAcceptCookies) {
      return res.status(400).json({ error: "You must accept cookie consent to create an account." });
    }

    const church = await db.oneOrNone("select id, name, join_code from churches where upper(join_code)=upper($1) limit 1", [requestedJoinCode]);
    if (!church) return res.status(404).json({ error: "Invalid join code" });

    const existing = await findAuthMember({ normalizedPhone, normalizedEmail });
    if (existing) {
      if (normalizedPhone && existing.phone === normalizedPhone) {
        return res.status(409).json({ error: "Phone already registered" });
      }
      if (normalizedEmail && existing.email && String(existing.email).toLowerCase() === normalizedEmail) {
        return res.status(409).json({ error: "Email already registered" });
      }
      return res.status(409).json({ error: "Phone or email already registered" });
    }

    const password_hash = await bcrypt.hash(pwd, 10);
    const termsVersion = (await getLegalDocumentVersion("terms")) || 1;
    const cookieConsentVersion = (await getLegalDocumentVersion("privacy")) || termsVersion || 1;

    const row = await createAuthMember({
      full_name,
      normalizedPhone,
      normalizedEmail,
      password_hash,
      church_id: church.id,
      normalizedDateOfBirth,
      termsAcceptedAt: new Date(),
      termsVersion,
      cookieConsentAt: new Date(),
      cookieConsentVersion,
    });
    if (!row?.id) throw new Error("Failed to create user record");

    const created = row.auth_source === "members" ? await fetchMember(row.id) : row;
    const profile = toProfile(created);
    if (profile && !profile.churchName) profile.churchName = church.name || null;
    if (row.auth_source === "members") {
      const verification = await issueMemberEmailVerification(row);
      return res.status(201).json({
        ok: true,
        verificationRequired: true,
        verification: {
          channel: "email",
          email: normalizedEmail,
          expiresAt: verification.expiresAt,
          provider: verification.delivery?.provider || "log",
        },
        profile,
        member: profile,
      });
    }

    const token = signUserToken(row);
    return res.json({ token, profile, member: profile });
  } catch (err) {
    if (isUniqueViolation(err)) {
      return res.status(409).json({ error: "Phone or email already registered" });
    }
    if (String(err?.message || "").toLowerCase().includes("verification upgrade")) {
      return res.status(503).json({ error: err.message });
    }
    console.error("[auth/register]", err?.message || err, err?.stack);
    res.status(500).json(internalErrorPayload(err));
  }
});

router.post("/login", async (req, res) => {
  try {
    return await handleLogin(req, res, null);
  } catch (err) {
    if (String(err?.message || "").toLowerCase().includes("two-factor sign-in upgrade")) {
      return res.status(503).json({ error: err.message });
    }
    console.error("[auth/login]", err?.message || err, err?.stack);
    return res.status(500).json(internalErrorPayload(err));
  }
});

router.post("/login/member", async (req, res) => {
  try {
    return await handleLogin(req, res, "member");
  } catch (err) {
    console.error("[auth/login/member]", err?.message || err, err?.stack);
    return res.status(500).json(internalErrorPayload(err));
  }
});

router.post("/login/admin", async (req, res) => {
  try {
    return await handleLogin(req, res, "admin");
  } catch (err) {
    if (String(err?.message || "").toLowerCase().includes("two-factor sign-in upgrade")) {
      return res.status(503).json({ error: err.message });
    }
    console.error("[auth/login/admin]", err?.message || err, err?.stack);
    return res.status(500).json(internalErrorPayload(err));
  }
});

router.post("/login/admin/verify-2fa", async (req, res) => {
  try {
    const verification = await verifyLoginTwoFactorChallenge({
      challengeId: req.body?.challengeId,
      role: "admin",
      code: req.body?.code,
      token: req.body?.token,
    });
    if (!verification.ok) {
      return res.status(verification.status || 400).json({ error: verification.error, code: verification.code });
    }

    const memberId = verification.challenge?.memberId;
    if (!memberId) {
      return res.status(400).json({ error: "Invalid two-factor challenge.", code: "TWO_FACTOR_INVALID" });
    }

    const member = await fetchMember(memberId).catch(() => null);

    if (!member || !isAdminRole(member.role)) {
      return res.status(403).json({ error: "Admin login required" });
    }

    await touchMemberLastActive(member.id);

    const profile = toProfile(member);
    const token = signUserToken(member);
    return res.json({ ok: true, token, profile, member: profile });
  } catch (err) {
    if (String(err?.message || "").toLowerCase().includes("two-factor sign-in upgrade")) {
      return res.status(503).json({ error: err.message });
    }
    console.error("[auth/login/admin/verify-2fa]", err?.message || err, err?.stack);
    return res.status(500).json(internalErrorPayload(err));
  }
});

router.post("/login/super", async (req, res) => {
  try {
    const result = authenticateSuperAdmin(req.body?.identifier || req.body?.email, req.body?.password);
    if (!result.ok) {
      return res.status(result.status).json({ error: result.error });
    }

    if (!superTwoFactorEnabled()) {
      return res.json({
        ok: true,
        token: result.token,
        profile: result.profile,
      });
    }

    const destinationEmail = normalizeEmail(result.profile?.email);
    if (!destinationEmail) {
      return res.status(400).json({
        error: "Super admin account needs an email address before two-factor sign-in can be used.",
        code: "TWO_FACTOR_EMAIL_REQUIRED",
      });
    }

    const challenge = await issueLoginTwoFactorChallenge({
      role: "super",
      memberId: null,
      identifier: destinationEmail,
      email: destinationEmail,
      recipientName: result.profile?.fullName || "there",
    });
    return res.json(twoFactorResponsePayload(challenge));
  } catch (err) {
    if (String(err?.message || "").toLowerCase().includes("two-factor sign-in upgrade")) {
      return res.status(503).json({ error: err.message });
    }
    console.error("[auth/login/super]", err?.message || err, err?.stack);
    return res.status(500).json(internalErrorPayload(err));
  }
});

router.post("/login/super/verify-2fa", async (req, res) => {
  try {
    const verification = await verifyLoginTwoFactorChallenge({
      challengeId: req.body?.challengeId,
      role: "super",
      code: req.body?.code,
      token: req.body?.token,
    });
    if (!verification.ok) {
      return res.status(verification.status || 400).json({ error: verification.error, code: verification.code });
    }

    const email = normalizeEmail(verification.challenge?.email);
    const config = getSuperAdminConfig();
    if (!email || !config?.email || email !== config.email) {
      return res.status(401).json({ error: "Invalid credentials" });
    }

    return res.json({
      ok: true,
      token: signSuperToken(email),
      profile: { role: "super", email, fullName: "Super Admin" },
    });
  } catch (err) {
    if (String(err?.message || "").toLowerCase().includes("two-factor sign-in upgrade")) {
      return res.status(503).json({ error: err.message });
    }
    console.error("[auth/login/super/verify-2fa]", err?.message || err, err?.stack);
    return res.status(500).json(internalErrorPayload(err));
  }
});

function renderVerificationResultHtml({ title, message, ok = false }) {
  const bg = ok ? "#052e16" : "#3f1111";
  const border = ok ? "#10b981" : "#ef4444";
  const headline = ok ? "#a7f3d0" : "#fecaca";
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>${title}</title>
  <style>
    body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; background: #020617; color: #e2e8f0; }
    .wrap { min-height: 100vh; display: grid; place-items: center; padding: 20px; }
    .card { width: min(560px, 100%); border-radius: 16px; border: 1px solid ${border}; background: ${bg}; padding: 20px; box-sizing: border-box; }
    h1 { margin: 0 0 10px; font-size: 26px; color: ${headline}; }
    p { margin: 0; line-height: 1.45; font-size: 16px; }
  </style>
</head>
<body>
  <main class="wrap">
    <section class="card">
      <h1>${title}</h1>
      <p>${message}</p>
    </section>
  </main>
</body>
</html>`;
}

router.post("/verify-email", async (req, res) => {
  try {
    if (!(await supportsMemberEmailVerification())) {
      return res.status(503).json({ error: "Email verification upgrade in progress. Please retry shortly." });
    }

    const identifier = typeof req.body?.identifier === "string" ? req.body.identifier : req.body?.email;
    const email = typeof req.body?.email === "string" ? req.body.email : "";
    const code = typeof req.body?.code === "string" ? req.body.code.trim() : "";
    const token = typeof req.body?.token === "string" ? req.body.token.trim() : "";

    if (!identifier && !email) {
      return res.status(400).json({ error: "identifier or email is required" });
    }
    if (!code && !token) {
      return res.status(400).json({ error: "code or token is required" });
    }

    const member = await resolveMemberForVerification(identifier, email);
    if (!member) return res.status(404).json({ error: "Account not found" });
    if (member.email_verified) {
      const profile = toProfile(member);
      const signed = signUserToken(member);
      return res.json({ ok: true, alreadyVerified: true, token: signed, profile, member: profile });
    }

    if (verificationExpired(member.email_verification_expires_at)) {
      return res.status(400).json({ error: "Verification code has expired. Request a new code." });
    }

    const codeOk = code ? verificationCodeMatches(code, member.email_verification_code_hash) : false;
    const tokenOk = token ? verificationTokenMatches(token, member.email_verification_token_hash) : false;
    if (!codeOk && !tokenOk) {
      return res.status(400).json({ error: "Invalid verification code or link." });
    }

    const updated = await db.one(
      `
      update members
      set
        email_verified = true,
        email_verified_at = now(),
        email_verification_token_hash = null,
        email_verification_code_hash = null,
        email_verification_expires_at = null,
        email_verification_sent_at = now(),
        updated_at = now()
      where id = $1
      returning id, full_name, phone, email, role, church_id
      `,
      [member.id]
    );

    const profile = toProfile(updated);
    const signed = signUserToken(updated);
    return res.json({ ok: true, token: signed, profile, member: profile });
  } catch (err) {
    if (String(err?.message || "").toLowerCase().includes("verification upgrade")) {
      return res.status(503).json({ error: err.message });
    }
    console.error("[auth/verify-email]", err?.message || err, err?.stack);
    return res.status(500).json(internalErrorPayload(err));
  }
});

router.get("/verify-email", async (req, res) => {
  try {
    if (!(await supportsMemberEmailVerification())) {
      if (req.accepts("html")) {
        return res.status(503).send(
          renderVerificationResultHtml({
            title: "Verification unavailable",
            message: "Email verification is temporarily unavailable. Please try again shortly.",
            ok: false,
          })
        );
      }
      return res.status(503).json({ error: "Email verification upgrade in progress. Please retry shortly." });
    }

    const identifier = typeof req.query?.identifier === "string" ? req.query.identifier : req.query?.email;
    const email = typeof req.query?.email === "string" ? req.query.email : "";
    const code = typeof req.query?.code === "string" ? req.query.code.trim() : "";
    const token = typeof req.query?.token === "string" ? req.query.token.trim() : "";

    if (!identifier && !email) {
      const msg = "identifier or email is required";
      if (req.accepts("html")) {
        return res.status(400).send(renderVerificationResultHtml({ title: "Verification failed", message: msg, ok: false }));
      }
      return res.status(400).json({ error: msg });
    }
    if (!code && !token) {
      const msg = "code or token is required";
      if (req.accepts("html")) {
        return res.status(400).send(renderVerificationResultHtml({ title: "Verification failed", message: msg, ok: false }));
      }
      return res.status(400).json({ error: msg });
    }

    const member = await resolveMemberForVerification(identifier, email);
    if (!member) {
      const msg = "Account not found";
      if (req.accepts("html")) {
        return res.status(404).send(renderVerificationResultHtml({ title: "Verification failed", message: msg, ok: false }));
      }
      return res.status(404).json({ error: msg });
    }

    if (member.email_verified) {
      const message = "Your email is already verified. You can return to the app and sign in.";
      if (req.accepts("html")) {
        return res.status(200).send(renderVerificationResultHtml({ title: "Already verified", message, ok: true }));
      }
      const profile = toProfile(member);
      const signed = signUserToken(member);
      return res.json({ ok: true, alreadyVerified: true, token: signed, profile, member: profile });
    }

    if (verificationExpired(member.email_verification_expires_at)) {
      const message = "Verification code has expired. Request a new code.";
      if (req.accepts("html")) {
        return res.status(400).send(renderVerificationResultHtml({ title: "Verification expired", message, ok: false }));
      }
      return res.status(400).json({ error: message });
    }

    const codeOk = code ? verificationCodeMatches(code, member.email_verification_code_hash) : false;
    const tokenOk = token ? verificationTokenMatches(token, member.email_verification_token_hash) : false;
    if (!codeOk && !tokenOk) {
      const message = "Invalid verification code or link.";
      if (req.accepts("html")) {
        return res.status(400).send(renderVerificationResultHtml({ title: "Verification failed", message, ok: false }));
      }
      return res.status(400).json({ error: message });
    }

    const updated = await db.one(
      `
      update members
      set
        email_verified = true,
        email_verified_at = now(),
        email_verification_token_hash = null,
        email_verification_code_hash = null,
        email_verification_expires_at = null,
        email_verification_sent_at = now(),
        updated_at = now()
      where id = $1
      returning id, full_name, phone, email, role, church_id
      `,
      [member.id]
    );

    const successMessage = "Email verified successfully. You can now continue in Churpay.";
    if (req.accepts("html")) {
      return res.status(200).send(renderVerificationResultHtml({ title: "Email verified", message: successMessage, ok: true }));
    }

    const profile = toProfile(updated);
    const signed = signUserToken(updated);
    return res.json({ ok: true, token: signed, profile, member: profile });
  } catch (err) {
    if (String(err?.message || "").toLowerCase().includes("verification upgrade")) {
      return res.status(503).json({ error: err.message });
    }
    console.error("[auth/verify-email-get]", err?.message || err, err?.stack);
    return res.status(500).json(internalErrorPayload(err));
  }
});

router.post("/resend-verification", async (req, res) => {
  try {
    if (!(await supportsMemberEmailVerification())) {
      return res.status(503).json({ error: "Email verification upgrade in progress. Please retry shortly." });
    }

    const identifier = typeof req.body?.identifier === "string" ? req.body.identifier : req.body?.email;
    const email = typeof req.body?.email === "string" ? req.body.email : "";
    if (!identifier && !email) {
      return res.status(400).json({ error: "identifier or email is required" });
    }

    const member = await resolveMemberForVerification(identifier, email);
    if (!member) return res.status(404).json({ error: "Account not found" });
    if (member.email_verified) {
      return res.json({
        ok: true,
        alreadyVerified: true,
        email: member.email,
      });
    }

    const verification = await issueMemberEmailVerification(member);
    return res.json({
      ok: true,
      verificationRequired: true,
      email: member.email,
      expiresAt: verification.expiresAt,
      provider: verification.delivery?.provider || "log",
    });
  } catch (err) {
    if (String(err?.message || "").toLowerCase().includes("verification upgrade")) {
      return res.status(503).json({ error: err.message });
    }
    console.error("[auth/resend-verification]", err?.message || err, err?.stack);
    return res.status(500).json(internalErrorPayload(err));
  }
});

router.post("/password-reset/request", async (req, res) => {
  try {
    const identifier = typeof req.body?.identifier === "string" ? req.body.identifier : req.body?.email;
    const normalizedPhone = normalizePhone(identifier);
    const normalizedEmail = normalizeEmail(identifier);
    if (!normalizedPhone && !normalizedEmail) {
      return res.status(400).json({ error: "Phone or email is required" });
    }

    // Always return ok to avoid account enumeration.
    const member = await findAuthMember({ normalizedPhone, normalizedEmail });
    if (member?.auth_source === "members" && normalizeEmail(member?.email)) {
      try {
        await issueMemberPasswordReset(member);
      } catch (err) {
        if (String(err?.message || "").toLowerCase().includes("upgrade in progress")) {
          return res.status(503).json({ error: err.message });
        }
        console.error("[auth/password-reset/request] issue error", err?.message || err, err?.stack);
      }
    }

    return res.json({ ok: true });
  } catch (err) {
    if (String(err?.message || "").toLowerCase().includes("upgrade in progress")) {
      return res.status(503).json({ error: err.message });
    }
    console.error("[auth/password-reset/request] error", err?.message || err, err?.stack);
    return res.status(500).json(internalErrorPayload(err));
  }
});

router.post("/password-reset/confirm", async (req, res) => {
  try {
    if (!(await supportsMemberPasswordReset())) {
      return res.status(503).json({ error: "Password reset upgrade in progress. Please retry shortly." });
    }

    const identifier = typeof req.body?.identifier === "string" ? req.body.identifier : req.body?.email;
    const normalizedPhone = normalizePhone(identifier);
    const normalizedEmail = normalizeEmail(identifier);
    const code = typeof req.body?.code === "string" ? req.body.code.trim() : "";
    const token = typeof req.body?.token === "string" ? req.body.token.trim() : "";
    const newPassword = typeof req.body?.newPassword === "string" ? req.body.newPassword : "";
    const newPasswordConfirm =
      typeof req.body?.newPasswordConfirm === "string"
        ? req.body.newPasswordConfirm
        : typeof req.body?.confirmPassword === "string"
          ? req.body.confirmPassword
          : "";

    if (!normalizedPhone && !normalizedEmail) {
      return res.status(400).json({ error: "Phone or email is required" });
    }
    if (!code && !token) {
      return res.status(400).json({ error: "Verification code is required" });
    }
    if (!newPassword || newPassword.length < 6) {
      return res.status(400).json({ error: "Password must be at least 6 characters" });
    }
    if (newPassword !== newPasswordConfirm) {
      return res.status(400).json({ error: "Passwords do not match" });
    }

    const member = await db.oneOrNone(
      `
      select
        id,
        password_reset_token_hash,
        password_reset_code_hash,
        password_reset_expires_at,
        password_reset_used_at
      from members
      where (coalesce($1::text,'') <> '' and phone::text = $1::text)
         or (coalesce($2::text,'') <> '' and lower(email::text) = lower($2::text))
      limit 1
      `,
      [normalizedPhone, normalizedEmail]
    );

    if (!member) {
      return res.status(400).json({ error: "Invalid verification code or expired." });
    }

    if (member.password_reset_used_at) {
      return res.status(400).json({ error: "This reset code has already been used. Request a new code." });
    }

    if (verificationExpired(member.password_reset_expires_at)) {
      return res.status(400).json({ error: "Reset code has expired. Request a new code." });
    }

    const codeOk = code ? verificationCodeMatches(code, member.password_reset_code_hash) : false;
    const tokenOk = token ? verificationTokenMatches(token, member.password_reset_token_hash) : false;
    if (!codeOk && !tokenOk) {
      return res.status(400).json({ error: "Invalid verification code or expired." });
    }

    const hash = await bcrypt.hash(newPassword, 10);
    await db.none(
      `
      update members
      set
        password_hash = $2,
        password_reset_token_hash = null,
        password_reset_code_hash = null,
        password_reset_expires_at = null,
        password_reset_sent_at = now(),
        password_reset_used_at = now(),
        updated_at = now()
      where id = $1
      `,
      [member.id, hash]
    );

    return res.json({ ok: true });
  } catch (err) {
    console.error("[auth/password-reset/confirm] error", err?.message || err, err?.stack);
    return res.status(500).json(internalErrorPayload(err));
  }
});

async function handleGetMe(req, res) {
  try {
    await touchMemberLastActive(req.user.id);
    const row = await fetchMember(req.user.id);
    const profile = toProfile(row);
    return res.json({ profile, member: profile });
  } catch (err) {
    console.error("[profile/me]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
}

router.get("/me", requireAuth, handleGetMe);
router.get("/profile/me", requireAuth, handleGetMe);

router.patch("/profile/me", requireAuth, async (req, res) => {
  try {
    const { fullName, phone, email, password } = req.body || {};
    const hasDobInput =
      Object.prototype.hasOwnProperty.call(req.body || {}, "dateOfBirth") ||
      Object.prototype.hasOwnProperty.call(req.body || {}, "birthDate") ||
      Object.prototype.hasOwnProperty.call(req.body || {}, "dob");
    if (hasDobInput) {
      return res.status(403).json({ error: "Date of birth can only be updated by super admin" });
    }
    const updates = [];
    const params = [];
    let idx = 1;

    if (typeof fullName === "string" && fullName.trim()) {
      updates.push(`full_name = $${idx++}`);
      params.push(fullName.trim());
    }

    if (typeof phone !== "undefined") {
      const normalized = normalizePhone(phone);
      if (normalized) {
        const existing = await db.oneOrNone("select id from members where phone=$1 and id<>$2", [normalized, req.user.id]);
        if (existing) return res.status(409).json({ error: "Phone already registered" });
        updates.push(`phone = $${idx++}`);
        params.push(normalized);
      } else {
        updates.push("phone = null");
      }
    }

    if (typeof email !== "undefined") {
      const normalized = normalizeEmail(email);
      if (normalized) {
        const existing = await db.oneOrNone("select id from members where lower(email)=lower($1) and id<>$2", [normalized, req.user.id]);
        if (existing) return res.status(409).json({ error: "Email already registered" });
        updates.push(`email = $${idx++}`);
        params.push(normalized);
      } else {
        return res.status(400).json({ error: "Email is required" });
      }
    }

    if (typeof password === "string" && password.trim()) {
      if (password.trim().length < 6) return res.status(400).json({ error: "Password must be at least 6 characters" });
      const hash = await bcrypt.hash(password.trim(), 10);
      updates.push(`password_hash = $${idx++}`);
      params.push(hash);
    }

    if (!updates.length) return res.status(400).json({ error: "No updates supplied" });

    updates.push("updated_at = now()");
    params.push(req.user.id);

    await db.none(`update members set ${updates.join(", ")} where id = $${idx}`, params);

    const fresh = await fetchMember(req.user.id);
    const profile = toProfile(fresh);
    res.json({ profile, member: profile });
  } catch (err) {
    console.error("[profile/update]", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/profile/church", requireAuth, async (req, res) => {
  try {
    const joinCode = typeof req.body?.joinCode === "string" ? req.body.joinCode.trim() : "";
    if (!joinCode) return res.status(400).json({ error: "Join code is required" });

    const church = await db.oneOrNone("select id, name, join_code from churches where upper(join_code)=upper($1)", [joinCode]);
    if (!church) return res.status(404).json({ error: "Invalid join code" });

    await db.none("update members set church_id=$1, updated_at=now() where id=$2", [church.id, req.user.id]);

    const fresh = await fetchMember(req.user.id);
    const profile = toProfile(fresh);
    // Church is encoded into the JWT payload (church_id), so re-issue a token after switching
    // to avoid the client having to log out/in for the new church to take effect.
    const token = signUserToken(fresh);
    res.json({ ok: true, token, profile, member: profile, church: { id: church.id, name: church.name, joinCode: church.join_code } });
  } catch (err) {
    console.error("[profile/church]", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/churches/join", async (req, res) => {
  try {
    const joinCode = typeof req.body?.joinCode === "string" ? req.body.joinCode.trim() : "";
    if (!joinCode) return res.status(400).json({ error: "Join code is required" });

    const church = await db.oneOrNone("select id, name, join_code from churches where upper(join_code)=upper($1)", [joinCode]);
    if (!church) return res.status(404).json({ error: "Invalid join code" });

    res.json({ church: { id: church.id, name: church.name, joinCode: church.join_code } });
  } catch (err) {
    console.error("[churches/join]", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/church/me", requireAuth, async (req, res) => {
  try {
    const member = await fetchMember(req.user.id);
    if (!member?.church_id) {
      return res.status(404).json({ error: "No church assigned" });
    }

    const church = await fetchChurch(member.church_id);
    if (!church) {
      return res.status(404).json({ error: "Church not found" });
    }

    return res.json({ church: toChurchProfile(church) });
  } catch (err) {
    console.error("[church/me]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/church/me", requireAdmin, async (req, res) => {
  try {
    const name = typeof req.body?.name === "string" ? req.body.name.trim() : "";
    const requestedJoinCode = normalizeJoinCode(req.body?.joinCode);
    if (!name) return res.status(400).json({ error: "Church name is required" });

    const member = await fetchMember(req.user.id);
    if (member?.church_id) {
      return res.status(409).json({ error: "Admin already linked to a church" });
    }

    if (requestedJoinCode) {
      const existingCode = await db.oneOrNone("select id from churches where upper(join_code)=upper($1)", [requestedJoinCode]);
      if (existingCode) {
        return res.status(409).json({ error: "Join code already in use" });
      }
    }

    const joinCode = await ensureUniqueJoinCode({
      db,
      desiredJoinCode: requestedJoinCode,
      churchName: name,
    });
    const church = await db.one(
      `insert into churches (name, join_code)
       values ($1, $2)
       returning id, name, join_code, created_at`,
      [name, joinCode]
    );

    await db.none("update members set church_id=$1, updated_at=now() where id=$2", [church.id, req.user.id]);
    const freshMember = await fetchMember(req.user.id);
    const profile = toProfile(freshMember);

    return res.status(201).json({
      church: toChurchProfile(church),
      member: profile,
      profile,
    });
  } catch (err) {
    if (isUniqueViolation(err)) {
      return res.status(409).json({ error: "Join code already in use" });
    }
    console.error("[church/create]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.patch("/church/me", requireAdmin, async (req, res) => {
  try {
    const member = await fetchMember(req.user.id);
    if (!member?.church_id) {
      return res.status(404).json({ error: "No church assigned" });
    }

    const updates = [];
    const params = [];
    let idx = 1;

    if (typeof req.body?.name === "string") {
      const name = req.body.name.trim();
      if (!name) return res.status(400).json({ error: "Church name is required" });
      updates.push(`name = $${idx++}`);
      params.push(name);
    }

    if (typeof req.body?.joinCode !== "undefined") {
      const joinCode = normalizeJoinCode(req.body.joinCode);
      if (!joinCode) return res.status(400).json({ error: "Join code is required" });
      updates.push(`join_code = $${idx++}`);
      params.push(joinCode);
    }

    if (!updates.length) {
      return res.status(400).json({ error: "No updates supplied" });
    }

    params.push(member.church_id);
    const church = await db.one(
      `update churches
       set ${updates.join(", ")}
       where id = $${idx}
       returning id, name, join_code, created_at`,
      params
    );

    return res.json({ church: toChurchProfile(church) });
  } catch (err) {
    if (isUniqueViolation(err)) {
      return res.status(409).json({ error: "Join code already in use" });
    }
    console.error("[church/update]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

export { handleGetMe };
export default router;
