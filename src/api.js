import AsyncStorage from "@react-native-async-storage/async-storage";
import { API_BASE_URL } from "../config/api";

const TOKEN_KEY = "churpay.auth.token";
const LEGACY_TOKEN_KEY = "authToken";
const BIOMETRIC_ENABLED_KEY = "churpay.auth.biometric.enabled";
const NOTIFICATIONS_ENABLED_KEY = "churpay.notifications.enabled";
const SECURE_TOKEN_KEY = "churpay.auth.token.secure";
let cachedToken = null;
let tokenLoadPromise = null;
const DEFAULT_TIMEOUT = 8000;

const DEFAULT_API_BASE = "https://api.churpay.com/api";

let biometricModulesPromise = null;
async function loadBiometricModules() {
  if (biometricModulesPromise) return biometricModulesPromise;
  biometricModulesPromise = (async () => {
    try {
      // Metro bundler will fail if we import missing native modules. Use eval-require to keep the app bootable
      // even before the modules are installed. Once installed, this resolves normally.
      // eslint-disable-next-line no-eval
      const req = eval("require");
      const SecureStore = req("expo-secure-store");
      const LocalAuthentication = req("expo-local-authentication");
      return { SecureStore, LocalAuthentication };
    } catch (_) {
      return null;
    }
  })();
  return biometricModulesPromise;
}

function normalizeApiBase(rawBase) {
  let base = String(rawBase || "").trim();
  if (!base) base = DEFAULT_API_BASE;

  base = base.replace(/\/+$/, "");

  // Some older builds/configs used https://api.churpay.com (without /api).
  // The backend mounts most routes under /api, while /auth is also exposed for legacy clients.
  // Normalize here so notifications/funds/transactions don't 404 with HTML.
  if (!/\/api$/i.test(base)) base = `${base}/api`;

  // Avoid accidental /api/api after normalization.
  base = base.replace(/\/api\/api$/i, "/api");

  return base;
}

const NORMALIZED_API_BASE = normalizeApiBase(API_BASE_URL);

function resolveUrl(path) {
  const base = NORMALIZED_API_BASE;
  const normalized = path.startsWith("/") ? path : `/${path}`;
  return `${base}${normalized}`;
}

async function fetchWithTimeout(url, options = {}, ms = DEFAULT_TIMEOUT) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), ms);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(id);
  }
}

export function getToken() {
  return cachedToken;
}

export async function getBiometricEnabled() {
  const raw = await AsyncStorage.getItem(BIOMETRIC_ENABLED_KEY);
  return raw === "1";
}

export async function getNotificationsEnabled() {
  const raw = await AsyncStorage.getItem(NOTIFICATIONS_ENABLED_KEY);
  // Default to enabled so existing users keep receiving alerts unless they opt out.
  if (raw === null || typeof raw === "undefined") return true;
  return raw === "1";
}

export async function setNotificationsEnabled(enabled) {
  await AsyncStorage.setItem(NOTIFICATIONS_ENABLED_KEY, enabled ? "1" : "0");
  return !!enabled;
}

export async function canUseBiometrics() {
  try {
    const mods = await loadBiometricModules();
    if (!mods) return { ok: false, reason: "Biometric modules not installed" };
    const { LocalAuthentication } = mods;
    const hasHardware = await LocalAuthentication.hasHardwareAsync();
    if (!hasHardware) return { ok: false, reason: "No biometric hardware" };
    const enrolled = await LocalAuthentication.isEnrolledAsync();
    if (!enrolled) return { ok: false, reason: "No biometrics enrolled" };
    return { ok: true };
  } catch (err) {
    return { ok: false, reason: err?.message || "Biometrics unavailable" };
  }
}

async function readSecureToken() {
  try {
    const mods = await loadBiometricModules();
    if (!mods) return null;
    const { SecureStore } = mods;
    // If stored with requireAuthentication, this will prompt automatically.
    const token = await SecureStore.getItemAsync(SECURE_TOKEN_KEY);
    return token || null;
  } catch (_) {
    // User cancelled biometric prompt or device auth unavailable.
    return null;
  }
}

async function writeSecureToken(token) {
  const mods = await loadBiometricModules();
  if (!mods) {
    throw new Error('Biometric modules not installed. Run: "npx expo install expo-secure-store expo-local-authentication"');
  }
  const { SecureStore } = mods;

  if (!token) {
    await SecureStore.deleteItemAsync(SECURE_TOKEN_KEY).catch(() => {});
    return;
  }

  // Store in secure enclave/keystore; prompt required to read token later.
  await SecureStore.setItemAsync(SECURE_TOKEN_KEY, token, {
    requireAuthentication: true,
  });
}

async function clearSecureToken() {
  const mods = await loadBiometricModules();
  if (!mods) return;
  const { SecureStore } = mods;
  await SecureStore.deleteItemAsync(SECURE_TOKEN_KEY).catch(() => {});
}

async function readTokenFromStorage() {
  const entries = await AsyncStorage.multiGet([TOKEN_KEY, LEGACY_TOKEN_KEY]);
  const tokenByKey = Object.fromEntries(entries);
  const current = tokenByKey[TOKEN_KEY] || null;
  const legacy = tokenByKey[LEGACY_TOKEN_KEY] || null;

  if (current) return current;
  if (!legacy) return null;

  // Migrate legacy key transparently.
  await AsyncStorage.setItem(TOKEN_KEY, legacy);
  await AsyncStorage.removeItem(LEGACY_TOKEN_KEY);
  return legacy;
}

export async function loadSessionToken() {
  const biometricEnabled = await getBiometricEnabled().catch(() => false);
  if (biometricEnabled) {
    // Prefer secure token. If missing but legacy token exists, migrate it into SecureStore.
    const secure = await readSecureToken();
    if (secure) {
      cachedToken = secure;
      return cachedToken;
    }

    const legacy = await readTokenFromStorage();
    if (legacy) {
      await writeSecureToken(legacy);
      // Remove plaintext token after migration.
      await AsyncStorage.multiRemove([TOKEN_KEY, LEGACY_TOKEN_KEY]);
      cachedToken = legacy;
      return cachedToken;
    }

    cachedToken = null;
    return null;
  }

  const stored = await readTokenFromStorage();
  cachedToken = stored || null;
  return cachedToken;
}

async function ensureSessionTokenLoaded() {
  if (cachedToken) return cachedToken;
  if (!tokenLoadPromise) {
    tokenLoadPromise = loadSessionToken().finally(() => {
      tokenLoadPromise = null;
    });
  }
  return tokenLoadPromise;
}

export async function setSessionToken(token) {
  cachedToken = token || null;
  const biometricEnabled = await getBiometricEnabled().catch(() => false);

  if (!token) {
    await AsyncStorage.multiRemove([TOKEN_KEY, LEGACY_TOKEN_KEY]);
    await clearSecureToken();
    return;
  }

  if (biometricEnabled) {
    await writeSecureToken(token);
    await AsyncStorage.multiRemove([TOKEN_KEY, LEGACY_TOKEN_KEY]);
    return;
  }

  await clearSecureToken();
  await AsyncStorage.setItem(TOKEN_KEY, token);
  await AsyncStorage.removeItem(LEGACY_TOKEN_KEY);
}

export async function setBiometricEnabled(enabled) {
  const next = !!enabled;

  if (next) {
    const capability = await canUseBiometrics();
    if (!capability.ok) throw new Error(capability.reason || "Biometrics unavailable");

    const mods = await loadBiometricModules();
    if (!mods) {
      throw new Error('Biometric modules not installed. Run: "npx expo install expo-secure-store expo-local-authentication"');
    }
    const { LocalAuthentication } = mods;

    // Require a biometric prompt now so user understands what's happening.
    const auth = await LocalAuthentication.authenticateAsync({
      promptMessage: "Enable biometric unlock",
      cancelLabel: "Cancel",
      disableDeviceFallback: false,
    });
    if (!auth?.success) throw new Error("Biometric authentication cancelled");

    await AsyncStorage.setItem(BIOMETRIC_ENABLED_KEY, "1");

    // Migrate existing token into SecureStore if present.
    const legacy = await readTokenFromStorage();
    if (legacy) {
      await writeSecureToken(legacy);
      await AsyncStorage.multiRemove([TOKEN_KEY, LEGACY_TOKEN_KEY]);
      cachedToken = legacy;
    }
    return true;
  }

  await AsyncStorage.setItem(BIOMETRIC_ENABLED_KEY, "0");
  const secure = await readSecureToken();
  await clearSecureToken();
  if (secure) {
    // Fall back to plaintext session storage.
    await AsyncStorage.setItem(TOKEN_KEY, secure);
    cachedToken = secure;
  }
  return false;
}

async function request(path, { method = "GET", body, auth = true, timeoutMs = DEFAULT_TIMEOUT } = {}) {
  const headers = { "Content-Type": "application/json" };
  if (auth) {
    if (!cachedToken) {
      await ensureSessionTokenLoaded();
    }
    if (cachedToken) {
      headers.Authorization = `Bearer ${cachedToken}`;
    }
  }

  let res;
  const url = resolveUrl(path);
  try {
    res = await fetchWithTimeout(
      url,
      {
        method,
        headers,
        body: typeof body === "undefined" ? undefined : JSON.stringify(body),
      },
      timeoutMs
    );
  } catch (e) {
    if (e?.name === "AbortError") throw new Error("Request timed out");
    throw e;
  }

  const text = await res.text().catch(() => "");
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch (_) {}

  if (!res.ok) {
    const ct = String(res.headers?.get?.("content-type") || "");
    const isHtml = ct.includes("text/html") || /^\s*</.test(text || "");
    const urlNoQuery = (() => {
      try {
        const u = new URL(url);
        u.search = "";
        u.hash = "";
        return u.toString();
      } catch (_err) {
        return url;
      }
    })();
    const fallback = isHtml
      ? `HTTP ${res.status}: API returned HTML from ${urlNoQuery} (route missing or wrong API base URL)`
      : `HTTP ${res.status}: ${text}`;
    const err = new Error(json?.error || fallback);
    // Preserve structured error details for UI flows (e.g. email verification required).
    err.status = res.status;
    if (json && typeof json === "object") {
      if (json.code) err.code = json.code;
      if (json.email) err.email = json.email;
      err.data = json;
    }
    throw err;
  }

  return json;
}

export async function registerMember(payload) {
  const data = await request("/auth/register", { method: "POST", body: payload, auth: false });
  if (data?.token) await setSessionToken(data.token);
  return data;
}

export async function loginMember(payload) {
  const data = await request("/auth/login/member", { method: "POST", body: payload, auth: false });
  if (data?.token) await setSessionToken(data.token);
  return data;
}

export async function loginAdmin(payload) {
  const data = await request("/auth/login/admin", { method: "POST", body: payload, auth: false });
  if (data?.token) await setSessionToken(data.token);
  return data;
}

export async function verifyAdminTwoFactor({ challengeId, code } = {}) {
  const data = await request("/auth/login/admin/verify-2fa", {
    method: "POST",
    body: { challengeId, code },
    auth: false,
  });
  if (data?.token) await setSessionToken(data.token);
  return data;
}

export async function verifyMemberEmail({ identifier, email, code, token } = {}) {
  const body = {};
  if (identifier) body.identifier = identifier;
  if (email) body.email = email;
  if (code) body.code = code;
  if (token) body.token = token;
  const data = await request("/auth/verify-email", { method: "POST", body, auth: false });
  if (data?.token) await setSessionToken(data.token);
  return data;
}

export async function resendMemberVerification({ identifier, email } = {}) {
  const body = {};
  if (identifier) body.identifier = identifier;
  if (email) body.email = email;
  return request("/auth/resend-verification", { method: "POST", body, auth: false });
}

export function requestPasswordReset({ identifier } = {}) {
  return request("/auth/password-reset/request", { method: "POST", body: { identifier }, auth: false });
}

export function confirmPasswordReset({ identifier, code, token, newPassword, newPasswordConfirm } = {}) {
  const body = { identifier, newPassword, newPasswordConfirm };
  if (code) body.code = code;
  if (token) body.token = token;
  return request("/auth/password-reset/confirm", { method: "POST", body, auth: false });
}

export async function logout() {
  await setSessionToken(null);
}

export function getProfile() {
  return request("/auth/me");
}

export function updateProfile(body) {
  return request("/auth/profile/me", { method: "PATCH", body });
}

export async function joinChurch(joinCode) {
  const data = await request("/auth/profile/church", { method: "POST", body: { joinCode } });
  // Server may re-issue a token to keep church_id in sync after switching churches.
  if (data?.token) await setSessionToken(data.token);
  return data;
}

export function getMyChurchProfile() {
  return request("/auth/church/me");
}

export function createMyChurchProfile({ name, joinCode }) {
  const body = { name };
  if (joinCode) body.joinCode = joinCode;
  return request("/auth/church/me", { method: "POST", body });
}

export function updateMyChurchProfile({ name, joinCode }) {
  const body = {};
  if (typeof name !== "undefined") body.name = name;
  if (joinCode) body.joinCode = joinCode;
  return request("/auth/church/me", { method: "PATCH", body });
}

export function lookupJoinCode(joinCode) {
  return request("/auth/churches/join", { method: "POST", body: { joinCode }, auth: false });
}

export function searchChurchesPublic(query, { limit = 10 } = {}) {
  const q = String(query || "").trim();
  const params = new URLSearchParams();
  if (q) params.append("query", q);
  if (limit) params.append("limit", String(limit));
  const qs = params.toString();
  return request(`/public/churches/search${qs ? `?${qs}` : ""}`, { auth: false });
}

export function listFunds(includeInactive = false) {
  const query = includeInactive ? "?includeInactive=1" : "";
  return request(`/funds${query}`);
}

export function getChurchQr({ fundId, amount } = {}) {
  const params = new URLSearchParams();
  if (fundId) params.append("fundId", fundId);
  if (typeof amount !== "undefined" && amount !== null && amount !== "") {
    params.append("amount", amount);
  }
  const qs = params.toString();
  return request(`/churches/me/qr${qs ? `?${qs}` : ""}`);
}

export function createFund({ name, code, active = true }) {
  return request("/funds", { method: "POST", body: { name, code, active } });
}

export function updateFund({ fundId, name, active }) {
  return request(`/funds/${fundId}`, { method: "PATCH", body: { name, active } });
}

export function createPaymentIntent({ fundId, amount, saveCard, useSavedCard } = {}) {
  const body = { fundId, amount };
  if (saveCard) body.saveCard = true;
  if (useSavedCard) body.useSavedCard = true;
  return request("/payment-intents", { method: "POST", body });
}

export function createGivingLink({ fundId, amountType = "FIXED", amountFixed, message, expiresInHours = 48, maxUses = 1 } = {}) {
  const body = {
    fundId,
    amountType,
    expiresInHours,
    maxUses,
  };
  if (typeof amountFixed !== "undefined" && amountFixed !== null) body.amountFixed = amountFixed;
  if (message) body.message = message;
  return request("/giving-links", { method: "POST", body });
}

export function createCashGiving({ fundId, amount, flow, serviceDate, notes } = {}) {
  const body = { fundId, amount };
  if (flow) body.flow = flow; // "prepared" | "recorded"
  if (serviceDate) body.serviceDate = serviceDate; // YYYY-MM-DD
  if (notes) body.notes = notes;
  return request("/cash-givings", { method: "POST", body });
}

export function confirmAdminCashGiving(paymentIntentId) {
  const id = String(paymentIntentId || "").trim();
  if (!id) return Promise.reject(new Error("paymentIntentId is required"));
  return request(`/admin/cash-givings/${encodeURIComponent(id)}/confirm`, { method: "POST", body: {} });
}

export function rejectAdminCashGiving(paymentIntentId, note) {
  const id = String(paymentIntentId || "").trim();
  const n = String(note || "").trim();
  if (!id) return Promise.reject(new Error("paymentIntentId is required"));
  if (!n) return Promise.reject(new Error("note is required"));
  return request(`/admin/cash-givings/${encodeURIComponent(id)}/reject`, { method: "POST", body: { note: n } });
}

export function registerPushToken({ token, platform, deviceId } = {}) {
  return request("/push-tokens/register", { method: "POST", body: { token, platform, deviceId } });
}

export function unregisterPushToken({ token } = {}) {
  return request("/push-tokens/unregister", { method: "POST", body: { token } });
}

export async function getUnreadNotificationCount() {
  try {
    return await request("/notifications/unread-count");
  } catch (err) {
    // Older backend deployments may not expose notifications yet.
    if (Number(err?.status) === 404) return { count: 0, unavailable: true };
    throw err;
  }
}

export async function listNotifications({ limit = 50, offset = 0, unread = false } = {}) {
  const params = new URLSearchParams();
  if (limit) params.append("limit", String(limit));
  if (offset) params.append("offset", String(offset));
  if (unread) params.append("unread", "1");
  const qs = params.toString();
  try {
    return await request(`/notifications${qs ? `?${qs}` : ""}`);
  } catch (err) {
    // Older backend deployments may not expose notifications yet.
    if (Number(err?.status) === 404) {
      return { notifications: [], meta: { limit, offset, count: 0, returned: 0 }, unavailable: true };
    }
    throw err;
  }
}

export async function markNotificationRead(notificationId) {
  try {
    return await request(`/notifications/${notificationId}/read`, { method: "POST", body: {} });
  } catch (err) {
    if (Number(err?.status) === 404) return { ok: true, unavailable: true };
    throw err;
  }
}

export async function markAllNotificationsRead() {
  try {
    return await request("/notifications/read-all", { method: "POST", body: {} });
  } catch (err) {
    if (Number(err?.status) === 404) return { ok: true, unavailable: true };
    throw err;
  }
}

export function getPaymentIntent(paymentIntentId) {
  return request(`/payment-intents/${paymentIntentId}`);
}

export function listTransactions({ limit = 50, offset = 0, fundId, channel, from, to } = {}) {
  const params = new URLSearchParams();
  if (limit) params.append("limit", limit);
  if (offset) params.append("offset", offset);
  if (fundId) params.append("fundId", fundId);
  if (channel) params.append("channel", channel);
  if (from) params.append("from", from);
  if (to) params.append("to", to);
  const qs = params.toString();
  return request(`/churches/me/transactions${qs ? `?${qs}` : ""}`);
}

export function getAdminDashboardTotals() {
  return request("/admin/dashboard/totals");
}

export function getAdminRecentTransactions({ limit = 20, offset = 0, fundId, channel, from, to } = {}) {
  const params = new URLSearchParams();
  if (limit) params.append("limit", limit);
  if (offset) params.append("offset", offset);
  if (fundId) params.append("fundId", fundId);
  if (channel) params.append("channel", channel);
  if (from) params.append("from", from);
  if (to) params.append("to", to);
  const qs = params.toString();
  return request(`/admin/dashboard/transactions/recent${qs ? `?${qs}` : ""}`);
}

export async function exportAdminTransactionsCsv({ limit = 5000, fundId, channel, from, to } = {}) {
  if (!cachedToken) {
    await ensureSessionTokenLoaded();
  }
  if (!cachedToken) throw new Error("Unauthorized");

  const params = new URLSearchParams();
  if (limit) params.append("limit", limit);
  if (fundId) params.append("fundId", fundId);
  if (channel) params.append("channel", channel);
  if (from) params.append("from", from);
  if (to) params.append("to", to);
  const qs = params.toString();
  const url = resolveUrl(`/admin/dashboard/transactions/export${qs ? `?${qs}` : ""}`);

  const res = await fetchWithTimeout(
    url,
    {
      method: "GET",
      headers: { Authorization: `Bearer ${cachedToken}` },
    },
    DEFAULT_TIMEOUT
  );

  const text = await res.text().catch(() => "");
  if (!res.ok) {
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch (_) {}
    throw new Error(json?.error || `HTTP ${res.status}: ${text}`);
  }

  return text;
}
