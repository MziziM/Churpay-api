import crypto from "node:crypto";

const ENCRYPTION_ENV_KEY = "PAYFAST_CREDENTIAL_ENCRYPTION_KEY";
const ENCRYPTION_PREFIX = "enc:v1";
const ENCRYPTION_ALGO = "aes-256-gcm";
const IV_BYTES = 12;

let cachedKey = null;

function readRawKey() {
  return String(process.env[ENCRYPTION_ENV_KEY] || "").trim();
}

function deriveKeyBuffer(raw) {
  if (!raw) return null;

  if (/^[0-9a-f]{64}$/i.test(raw)) {
    const fromHex = Buffer.from(raw, "hex");
    if (fromHex.length === 32) return fromHex;
  }

  if (/^[A-Za-z0-9+/=]+$/.test(raw)) {
    try {
      const fromBase64 = Buffer.from(raw, "base64");
      if (fromBase64.length === 32) return fromBase64;
    } catch (_err) {
      // Ignore parse errors and fall through to deterministic hashing.
    }
  }

  return crypto.createHash("sha256").update(raw, "utf8").digest();
}

function getKeyBuffer() {
  if (cachedKey) return cachedKey;
  const raw = readRawKey();
  if (!raw) return null;
  cachedKey = deriveKeyBuffer(raw);
  return cachedKey;
}

export function hasSecretEncryptionKey() {
  return !!getKeyBuffer();
}

export function requireSecretEncryptionKey() {
  const key = getKeyBuffer();
  if (key) return key;
  const err = new Error(`${ENCRYPTION_ENV_KEY} is required to encrypt/decrypt church PayFast credentials`);
  err.code = "PAYFAST_CREDENTIAL_ENCRYPTION_KEY_MISSING";
  throw err;
}

export function encryptSecret(value) {
  const plain = String(value || "");
  if (!plain) return "";

  const key = requireSecretEncryptionKey();
  const iv = crypto.randomBytes(IV_BYTES);
  const cipher = crypto.createCipheriv(ENCRYPTION_ALGO, key, iv);
  const encrypted = Buffer.concat([cipher.update(Buffer.from(plain, "utf8")), cipher.final()]);
  const tag = cipher.getAuthTag();

  return [
    ENCRYPTION_PREFIX,
    iv.toString("base64"),
    tag.toString("base64"),
    encrypted.toString("base64"),
  ].join(":");
}

export function decryptSecret(value) {
  const raw = String(value || "");
  if (!raw) return "";

  // Backward compatibility for legacy plain values.
  if (!raw.startsWith(`${ENCRYPTION_PREFIX}:`)) {
    return raw;
  }

  const parts = raw.split(":");
  if (parts.length !== 4) {
    const err = new Error("Invalid encrypted secret payload format");
    err.code = "PAYFAST_CREDENTIAL_DECRYPT_INVALID_FORMAT";
    throw err;
  }

  const [, ivB64, tagB64, dataB64] = parts;
  const iv = Buffer.from(ivB64, "base64");
  const tag = Buffer.from(tagB64, "base64");
  const encrypted = Buffer.from(dataB64, "base64");

  const key = requireSecretEncryptionKey();
  const decipher = crypto.createDecipheriv(ENCRYPTION_ALGO, key, iv);
  decipher.setAuthTag(tag);
  const decrypted = Buffer.concat([decipher.update(encrypted), decipher.final()]);
  return decrypted.toString("utf8");
}

export function maskSecret(value, { prefix = 2, suffix = 2 } = {}) {
  const raw = String(value || "");
  if (!raw) return "";
  if (raw.length <= prefix + suffix) return "*".repeat(raw.length);
  const start = raw.slice(0, Math.max(0, prefix));
  const end = raw.slice(Math.max(0, raw.length - suffix));
  return `${start}${"*".repeat(raw.length - start.length - end.length)}${end}`;
}
