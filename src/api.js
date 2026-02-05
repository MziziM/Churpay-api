import AsyncStorage from "@react-native-async-storage/async-storage";
import { API_BASE_URL } from "../config/api";

const TOKEN_KEY = "authToken";
let cachedToken = null;

function resolveUrl(path) {
  const base = API_BASE_URL.replace(/\/$/, "");
  const normalized = path.startsWith("/") ? path : `/${path}`;
  return `${base}${normalized}`;
}

export function getToken() {
  return cachedToken;
}

export async function loadSessionToken() {
  const stored = await AsyncStorage.getItem(TOKEN_KEY);
  cachedToken = stored || null;
  return cachedToken;
}

export async function setSessionToken(token) {
  cachedToken = token || null;
  if (!token) {
    await AsyncStorage.removeItem(TOKEN_KEY);
  } else {
    await AsyncStorage.setItem(TOKEN_KEY, token);
  }
}

async function request(path, { method = "GET", body, auth = true } = {}) {
  const headers = { "Content-Type": "application/json" };
  if (auth && cachedToken) headers.Authorization = `Bearer ${cachedToken}`;

  const res = await fetch(resolveUrl(path), {
    method,
    headers,
    body: typeof body === "undefined" ? undefined : JSON.stringify(body),
  });

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
  return request("/profile/me");
}

export function updateProfile(body) {
  return request("/profile/me", { method: "PATCH", body });
}

export function joinChurch(joinCode) {
  return request("/profile/church", { method: "POST", body: { joinCode } });
}

export function lookupJoinCode(joinCode) {
  return request("/churches/join", { method: "POST", body: { joinCode }, auth: false });
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
