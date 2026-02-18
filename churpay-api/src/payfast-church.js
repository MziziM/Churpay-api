import { db } from "./db.js";
import { buildPayfastRedirect } from "./payfast.js";
import {
  decryptSecret,
  encryptSecret,
  hasSecretEncryptionKey,
  maskSecret,
  requireSecretEncryptionKey,
} from "./secrets-crypto.js";

const PAYFAST_CHURCH_COLUMNS = [
  "payfast_merchant_id",
  "payfast_merchant_key",
  "payfast_passphrase",
  "payfast_connected",
  "payfast_connected_at",
  "payfast_last_connect_attempt_at",
  "payfast_last_connect_status",
  "payfast_last_connect_error",
  "payfast_disconnected_at",
];

let churchColumnsPromise = null;

function normalize(value) {
  return String(value || "").trim();
}

function parseBool(value, fallback = false) {
  if (typeof value === "undefined" || value === null || value === "") return fallback;
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

export function normalizePayfastMode(raw) {
  return String(raw || "").toLowerCase() === "live" ? "live" : "sandbox";
}

function normalizedApiBaseUrl() {
  const base = normalize(process.env.PUBLIC_BASE_URL) || "https://api.churpay.com";
  return base.replace(/\/+$/, "");
}

function fallbackEnabled() {
  return parseBool(process.env.PAYFAST_GLOBAL_FALLBACK_ENABLED, false);
}

function readGlobalPayfastCredentials() {
  return {
    source: "global",
    mode: normalizePayfastMode(process.env.PAYFAST_MODE),
    merchantId: normalize(process.env.PAYFAST_MERCHANT_ID),
    merchantKey: normalize(process.env.PAYFAST_MERCHANT_KEY),
    passphrase: normalize(process.env.PAYFAST_PASSPHRASE),
    connected: false,
    connectedAt: null,
  };
}

function hasRequiredCredentials(creds) {
  return Boolean(normalize(creds?.merchantId) && normalize(creds?.merchantKey));
}

async function getChurchColumns() {
  if (!churchColumnsPromise) {
    churchColumnsPromise = db
      .any(
        `
        select column_name
        from information_schema.columns
        where table_schema = 'public' and table_name = 'churches'
        `
      )
      .then((rows) => new Set((rows || []).map((row) => row.column_name)))
      .catch((err) => {
        churchColumnsPromise = null;
        throw err;
      });
  }
  return churchColumnsPromise;
}

export async function hasChurchPayfastColumns() {
  try {
    const columns = await getChurchColumns();
    return PAYFAST_CHURCH_COLUMNS.every((name) => columns.has(name));
  } catch (_err) {
    return false;
  }
}

async function getChurchPayfastRow(churchId) {
  if (!churchId) return null;
  if (!(await hasChurchPayfastColumns())) return null;
  return db.oneOrNone(
    `
    select
      payfast_merchant_id,
      payfast_merchant_key,
      payfast_passphrase,
      payfast_connected,
      payfast_connected_at,
      payfast_last_connect_attempt_at,
      payfast_last_connect_status,
      payfast_last_connect_error,
      payfast_disconnected_at
    from churches
    where id = $1
    `,
    [churchId]
  );
}

function decryptChurchSecrets(row) {
  if (!row) return null;
  const merchantKey = decryptSecret(row.payfast_merchant_key);
  const passphrase = decryptSecret(row.payfast_passphrase);
  return {
    merchantId: normalize(row.payfast_merchant_id),
    merchantKey: normalize(merchantKey),
    passphrase: normalize(passphrase),
    connected: !!row.payfast_connected,
    connectedAt: row.payfast_connected_at || null,
    lastAttemptAt: row.payfast_last_connect_attempt_at || null,
    lastAttemptStatus: normalize(row.payfast_last_connect_status) || null,
    lastAttemptError: normalize(row.payfast_last_connect_error) || null,
    disconnectedAt: row.payfast_disconnected_at || null,
  };
}

export async function getChurchPayfastStatus(churchId) {
  const storageReady = await hasChurchPayfastColumns();
  const mode = normalizePayfastMode(process.env.PAYFAST_MODE);

  if (!storageReady) {
    return {
      storageReady: false,
      encryptionKeyConfigured: hasSecretEncryptionKey(),
      connected: false,
      connectedAt: null,
      mode,
      merchantIdMasked: "",
      merchantKeyMasked: "",
      lastAttemptAt: null,
      lastAttemptStatus: null,
      lastAttemptError: null,
      fallbackEnabled: fallbackEnabled(),
    };
  }

  const row = await getChurchPayfastRow(churchId);
  if (!row) {
    return {
      storageReady: true,
      encryptionKeyConfigured: hasSecretEncryptionKey(),
      connected: false,
      connectedAt: null,
      mode,
      merchantIdMasked: "",
      merchantKeyMasked: "",
      lastAttemptAt: null,
      lastAttemptStatus: null,
      lastAttemptError: null,
      fallbackEnabled: fallbackEnabled(),
    };
  }

  let decrypted = null;
  let decryptError = "";
  try {
    decrypted = decryptChurchSecrets(row);
  } catch (err) {
    decryptError = String(err?.message || err || "Failed to decrypt credentials");
  }

  const merchantId = normalize(decrypted?.merchantId || row.payfast_merchant_id);
  const merchantKey = normalize(decrypted?.merchantKey || "");
  const connected =
    Boolean(row.payfast_connected) &&
    Boolean(merchantId) &&
    Boolean(row.payfast_merchant_key) &&
    Boolean(!decryptError || merchantKey);

  return {
    storageReady: true,
    encryptionKeyConfigured: hasSecretEncryptionKey(),
    connected,
    connectedAt: row.payfast_connected_at || null,
    mode,
    merchantIdMasked: merchantId ? maskSecret(merchantId, { prefix: 3, suffix: 2 }) : "",
    merchantKeyMasked: merchantKey ? maskSecret(merchantKey, { prefix: 2, suffix: 2 }) : "",
    lastAttemptAt: row.payfast_last_connect_attempt_at || null,
    lastAttemptStatus: normalize(row.payfast_last_connect_status) || null,
    lastAttemptError: normalize(row.payfast_last_connect_error) || null,
    decryptError: decryptError || null,
    fallbackEnabled: fallbackEnabled(),
  };
}

export async function resolveChurchPayfastCredentials(churchId, options = {}) {
  const allowGlobalFallback = options.allowGlobalFallback !== false;
  const globalCreds = readGlobalPayfastCredentials();

  const row = await getChurchPayfastRow(churchId);
  if (row) {
    const connected = !!row.payfast_connected;
    if (connected && row.payfast_merchant_id && row.payfast_merchant_key) {
      const decrypted = decryptChurchSecrets(row);
      if (hasRequiredCredentials(decrypted)) {
        return {
          source: "church",
          mode: normalizePayfastMode(process.env.PAYFAST_MODE),
          merchantId: decrypted.merchantId,
          merchantKey: decrypted.merchantKey,
          passphrase: decrypted.passphrase,
          connected: true,
          connectedAt: decrypted.connectedAt,
        };
      }
    }
  }

  if (allowGlobalFallback && fallbackEnabled() && hasRequiredCredentials(globalCreds)) {
    return globalCreds;
  }

  return null;
}

export async function recordChurchPayfastConnectAttempt({
  churchId,
  status = "failed",
  error = "",
}) {
  if (!churchId) return;
  if (!(await hasChurchPayfastColumns())) return;
  const normalizedStatus = ["connected", "failed", "disconnected"].includes(String(status || "").toLowerCase())
    ? String(status || "").toLowerCase()
    : "failed";
  const normalizedError = normalize(error).slice(0, 1000) || null;

  await db.none(
    `
    update churches
    set
      payfast_last_connect_attempt_at = now(),
      payfast_last_connect_status = $2,
      payfast_last_connect_error = $3
    where id = $1
    `,
    [churchId, normalizedStatus, normalizedError]
  );
}

export async function connectChurchPayfastCredentials({
  churchId,
  merchantId,
  merchantKey,
  passphrase = "",
}) {
  if (!churchId) throw new Error("churchId is required");
  if (!(await hasChurchPayfastColumns())) {
    const err = new Error("PayFast church credential storage is not available yet. Run migrations and retry.");
    err.code = "PAYFAST_STORAGE_NOT_READY";
    throw err;
  }

  requireSecretEncryptionKey();

  const cleanMerchantId = normalize(merchantId);
  const cleanMerchantKey = normalize(merchantKey);
  const cleanPassphrase = normalize(passphrase);
  if (!cleanMerchantId || !cleanMerchantKey) {
    const err = new Error("merchantId and merchantKey are required");
    err.code = "PAYFAST_CONNECT_FIELDS_REQUIRED";
    throw err;
  }

  const encryptedMerchantKey = encryptSecret(cleanMerchantKey);
  const encryptedPassphrase = cleanPassphrase ? encryptSecret(cleanPassphrase) : "";

  const result = await db.query(
    `
    update churches
    set
      payfast_merchant_id = $2,
      payfast_merchant_key = $3,
      payfast_passphrase = nullif($4, ''),
      payfast_connected = true,
      payfast_connected_at = now(),
      payfast_last_connect_attempt_at = now(),
      payfast_last_connect_status = 'connected',
      payfast_last_connect_error = null,
      payfast_disconnected_at = null
    where id = $1
    `,
    [churchId, cleanMerchantId, encryptedMerchantKey, encryptedPassphrase]
  );

  if (!result?.rowCount) {
    const err = new Error("Church not found");
    err.code = "PAYFAST_CONNECT_CHURCH_NOT_FOUND";
    throw err;
  }
}

export async function disconnectChurchPayfastCredentials(churchId) {
  if (!churchId) throw new Error("churchId is required");
  if (!(await hasChurchPayfastColumns())) {
    const err = new Error("PayFast church credential storage is not available yet. Run migrations and retry.");
    err.code = "PAYFAST_STORAGE_NOT_READY";
    throw err;
  }

  const result = await db.query(
    `
    update churches
    set
      payfast_merchant_id = null,
      payfast_merchant_key = null,
      payfast_passphrase = null,
      payfast_connected = false,
      payfast_connected_at = null,
      payfast_disconnected_at = now(),
      payfast_last_connect_attempt_at = now(),
      payfast_last_connect_status = 'disconnected',
      payfast_last_connect_error = null
    where id = $1
    `,
    [churchId]
  );

  if (!result?.rowCount) {
    const err = new Error("Church not found");
    err.code = "PAYFAST_DISCONNECT_CHURCH_NOT_FOUND";
    throw err;
  }
}

export async function validatePayfastCredentialConnection({
  merchantId,
  merchantKey,
  passphrase = "",
  mode,
}) {
  const cleanMerchantId = normalize(merchantId);
  const cleanMerchantKey = normalize(merchantKey);
  const cleanPassphrase = normalize(passphrase);
  if (!cleanMerchantId || !cleanMerchantKey) {
    return { ok: false, error: "Invalid Merchant Credentials" };
  }

  const liveMode = normalizePayfastMode(mode || process.env.PAYFAST_MODE);
  const baseUrl = normalizedApiBaseUrl();
  const testRef = `CP-CONNECT-${Date.now()}`;
  const checkoutUrl = buildPayfastRedirect({
    mode: liveMode,
    merchantId: cleanMerchantId,
    merchantKey: cleanMerchantKey,
    passphrase: cleanPassphrase,
    mPaymentId: testRef,
    amount: 1,
    itemName: "Churpay PayFast connection test",
    returnUrl: `${baseUrl}/api/payfast/return?connect_test=1`,
    cancelUrl: `${baseUrl}/api/payfast/cancel?connect_test=1`,
    notifyUrl: `${baseUrl}/webhooks/payfast/itn`,
    nameFirst: "Churpay",
    emailAddress: "support@churpay.com",
  });

  const timeoutMs = 8000;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(checkoutUrl, {
      method: "GET",
      redirect: "manual",
      signal: controller.signal,
    });
    const bodyText = String(await response.text()).toLowerCase();

    const invalidSnippets = [
      "invalid merchant",
      "merchant key",
      "merchant id",
      "merchant account",
      "incorrectly configured",
      "account has not been configured",
      "could not find merchant",
    ];

    if (response.status >= 500) {
      return { ok: false, error: "Invalid Merchant Credentials" };
    }

    if (invalidSnippets.some((snippet) => bodyText.includes(snippet))) {
      return { ok: false, error: "Invalid Merchant Credentials" };
    }

    return { ok: true };
  } catch (err) {
    return {
      ok: false,
      code: "PAYFAST_VALIDATION_UNAVAILABLE",
      error: "Could not validate Merchant Credentials right now. Please retry.",
      detail: String(err?.message || err || "validation request failed"),
    };
  } finally {
    clearTimeout(timer);
  }
}

