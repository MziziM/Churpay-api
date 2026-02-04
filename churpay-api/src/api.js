const API_BASE = "https://www.churpay.com/api";
const CHURCH_ID = "09f9c0f2-c1b0-481b-8058-67853fb9b9dd";

export async function apiGetTransactions({ limit = 50, offset = 0, channel, fundId, from, to } = {}) {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  params.set("offset", String(offset));
  if (channel) params.set("channel", channel);
  if (fundId) params.set("fundId", fundId);
  if (from) params.set("from", from); // YYYY-MM-DD
  if (to) params.set("to", to);       // YYYY-MM-DD

  const url = `${API_BASE}/churches/${CHURCH_ID}/transactions?${params.toString()}`;
  const res = await fetch(url);

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`Failed: ${res.status} ${t}`);
  }
  return res.json();
}