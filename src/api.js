import AsyncStorage from "@react-native-async-storage/async-storage";
import { API_BASE_URL } from "../config/api";

const TOKEN_KEY = "churpay.auth.token";
const LEGACY_TOKEN_KEY = "authToken";
let cachedToken = null;
let tokenLoadPromise = null;
const DEFAULT_TIMEOUT = 8000;

function resolveUrl(path) {
  const base = API_BASE_URL.replace(/\/$/, "");
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
  if (!token) {
    await AsyncStorage.multiRemove([TOKEN_KEY, LEGACY_TOKEN_KEY]);
  } else {
    await AsyncStorage.setItem(TOKEN_KEY, token);
    await AsyncStorage.removeItem(LEGACY_TOKEN_KEY);
  }
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
  try {
    res = await fetchWithTimeout(
      resolveUrl(path),
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
    throw new Error(json?.error || `HTTP ${res.status}: ${text}`);
  }

  return json;
}

export async function registerMember(payload) {
  const data = await request("/auth/register", { method: "POST", body: payload, auth: false });
  if (data?.token) await setSessionToken(data.token);
  return data;
}

export async function loginMember(payload) {
  const data = await request("/auth/login", { method: "POST", body: payload, auth: false });
  if (data?.token) await setSessionToken(data.token);
  return data;
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

export function joinChurch(joinCode) {
  return request("/auth/profile/church", { method: "POST", body: { joinCode } });
}

export function getMyChurchProfile() {
  return request("/auth/church/me");
}

export function createMyChurchProfile({ name, joinCode }) {
  return request("/auth/church/me", { method: "POST", body: { name, joinCode } });
}

export function updateMyChurchProfile({ name, joinCode }) {
  return request("/auth/church/me", { method: "PATCH", body: { name, joinCode } });
}

export function lookupJoinCode(joinCode) {
  return request("/auth/churches/join", { method: "POST", body: { joinCode }, auth: false });
}

export function listFunds(includeInactive = false) {
  const query = includeInactive ? "?includeInactive=1" : "";
  return request(`/funds${query}`);
}

export function createFund({ name, code, active = true }) {
  return request("/funds", { method: "POST", body: { name, code, active } });
}

export function updateFund({ fundId, name, active }) {
  return request(`/funds/${fundId}`, { method: "PATCH", body: { name, active } });
}

export function createPaymentIntent({ fundId, amount }) {
  return request("/payment-intents", { method: "POST", body: { fundId, amount } });
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
