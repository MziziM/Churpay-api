import { API_BASE_URL as API_BASE, PILOT_CHURCH_ID as CHURCH_ID } from "../config/api";

async function safeFetch(url, options) {
  const res = await fetch(url, options);
  const text = await res.text().catch(() => "");
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch {}
  if (!res.ok) throw new Error(json?.error || `HTTP ${res.status}: ${text}`);
  return json;
}

export function apiGetFunds() {
  const url = `${API_BASE}/churches/${CHURCH_ID}/funds`;
  return safeFetch(url);
}

export function apiCreateFund({ name, active = true }) {
  const url = `${API_BASE}/funds`;
  return safeFetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ churchId: CHURCH_ID, name, active }),
  });
}

export function apiPatchFund({ fundId, name, active }) {
  const url = `${API_BASE}/funds/${fundId}`;
  return safeFetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ churchId: CHURCH_ID, name, active }),
  });
}

export function apiGetTransactions({ limit = 50, offset = 0, fundId, channel, from, to } = {}) {
  const url = new URL(`${API_BASE}/churches/${CHURCH_ID}/transactions`);
  url.searchParams.append("limit", limit);
  url.searchParams.append("offset", offset);
  if (fundId) url.searchParams.append("fundId", fundId);
  if (channel) url.searchParams.append("channel", channel);
  if (from) url.searchParams.append("from", from);
  if (to) url.searchParams.append("to", to);
  return safeFetch(url.toString());
}
