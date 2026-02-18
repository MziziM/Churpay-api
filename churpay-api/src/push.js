const EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send";
const MAX_EXPO_MESSAGES_PER_REQUEST = 100;

function chunk(array, size) {
  const out = [];
  for (let i = 0; i < array.length; i += size) out.push(array.slice(i, i + size));
  return out;
}

export function isExpoPushToken(token) {
  const t = String(token || "").trim();
  if (!t) return false;
  // Common formats:
  // - ExponentPushToken[xxxxxxxxxxxxxxxxxxxxxx]
  // - ExpoPushToken[xxxxxxxxxxxxxxxxxxxxxx]
  return /^(ExponentPushToken|ExpoPushToken)\[[^\]]+\]$/.test(t);
}

export function getPushProvider() {
  const provider = String(process.env.PUSH_PROVIDER || "").trim().toLowerCase();
  if (provider === "expo") return "expo";
  return "log";
}

export async function sendExpoPushMessages(messages) {
  const provider = getPushProvider();
  if (provider !== "expo") {
    console.log("[push/log]", messages);
    return { ok: true, tickets: [], invalidTokens: [] };
  }

  const accessToken = String(process.env.EXPO_ACCESS_TOKEN || "").trim();
  const headers = {
    Accept: "application/json",
    "Accept-Encoding": "gzip, deflate",
    "Content-Type": "application/json",
  };
  if (accessToken) headers.Authorization = `Bearer ${accessToken}`;

  const invalidTokens = [];
  const tickets = [];

  for (const batch of chunk(messages, MAX_EXPO_MESSAGES_PER_REQUEST)) {
    let response;
    let json;
    try {
      response = await fetch(EXPO_PUSH_URL, {
        method: "POST",
        headers,
        body: JSON.stringify(batch),
      });
      json = await response.json().catch(() => null);
    } catch (err) {
      console.error("[push/expo] send failed", err?.message || err);
      continue;
    }

    if (!response.ok) {
      console.error("[push/expo] http error", response.status, json || null);
      continue;
    }

    const data = Array.isArray(json?.data) ? json.data : [];
    for (let i = 0; i < data.length; i += 1) {
      const ticket = data[i] || {};
      tickets.push(ticket);

      if (ticket.status === "error") {
        const details = ticket.details || {};
        const err = String(details.error || ticket.message || "").toLowerCase();
        if (err.includes("device") || err.includes("notregistered") || err.includes("invalid")) {
          const to = batch[i]?.to;
          if (to) invalidTokens.push(String(to));
        }
      }
    }
  }

  return { ok: true, tickets, invalidTokens };
}

