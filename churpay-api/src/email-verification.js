import crypto from "node:crypto";

const DEFAULT_CODE_LENGTH = 6;
const DEFAULT_TOKEN_BYTES = 24;

function parsePositiveInt(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

export function verificationTtlMinutes() {
  return parsePositiveInt(process.env.EMAIL_VERIFICATION_TTL_MINUTES, 20);
}

export function hashVerificationValue(value) {
  return crypto.createHash("sha256").update(String(value || "")).digest("hex");
}

export function createVerificationChallenge() {
  const codeLength = parsePositiveInt(process.env.EMAIL_VERIFICATION_CODE_LENGTH, DEFAULT_CODE_LENGTH);
  const tokenBytes = parsePositiveInt(process.env.EMAIL_VERIFICATION_TOKEN_BYTES, DEFAULT_TOKEN_BYTES);

  const max = 10 ** Math.min(Math.max(codeLength, 4), 9);
  const code = String(crypto.randomInt(0, max)).padStart(codeLength, "0");
  const token = crypto.randomBytes(tokenBytes).toString("hex");
  const expiresAt = new Date(Date.now() + verificationTtlMinutes() * 60 * 1000);

  return {
    code,
    token,
    codeHash: hashVerificationValue(code),
    tokenHash: hashVerificationValue(token),
    expiresAt,
  };
}

export function verificationCodeMatches(inputCode, storedCodeHash) {
  if (!storedCodeHash || typeof inputCode !== "string") return false;
  return hashVerificationValue(inputCode.trim()) === storedCodeHash;
}

export function verificationTokenMatches(inputToken, storedTokenHash) {
  if (!storedTokenHash || typeof inputToken !== "string") return false;
  return hashVerificationValue(inputToken.trim()) === storedTokenHash;
}

export function verificationExpired(expiresAt) {
  if (!expiresAt) return true;
  const when = new Date(expiresAt);
  if (!Number.isFinite(when.getTime())) return true;
  return when.getTime() < Date.now();
}

