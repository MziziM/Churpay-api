import { db } from "./db.js";
import { isExpoPushToken, sendExpoPushMessages } from "./push.js";

function truncate(str, max) {
  const s = String(str || "");
  return s.length > max ? s.slice(0, max) : s;
}

async function getActivePushTokensForMember(memberId) {
  const rows = await db.manyOrNone(
    `
    select token
    from push_tokens
    where member_id=$1 and revoked_at is null
    order by last_seen_at desc
    `,
    [memberId]
  );
  return rows.map((r) => String(r.token || "").trim()).filter(Boolean);
}

export async function createNotification({ memberId, type, title, body, data = null, sendPush = true }) {
  const notification = await db.one(
    `
    insert into notifications (member_id, type, title, body, data, created_at)
    values ($1,$2,$3,$4,$5,now())
    returning
      id,
      member_id as "memberId",
      type,
      title,
      body,
      data,
      created_at as "createdAt",
      read_at as "readAt",
      push_sent_at as "pushSentAt"
    `,
    [memberId, String(type || "GENERIC"), truncate(title, 120), truncate(body, 500), data]
  );

  if (!sendPush) return notification;

  try {
    await sendPushForNotification(notification);
  } catch (err) {
    console.error("[notifications] push failed", err?.message || err);
  }

  return notification;
}

export async function sendPushForNotification(notification) {
  const memberId = notification?.memberId;
  if (!memberId) return { ok: false, error: "memberId missing" };

  const tokens = await getActivePushTokensForMember(memberId);
  const expoTokens = tokens.filter(isExpoPushToken);

  if (expoTokens.length === 0) {
    return { ok: true, sent: 0, invalid: 0 };
  }

  const payloadData = {
    notificationId: notification.id,
    type: notification.type,
    ...(notification.data || {}),
  };

  const messages = expoTokens.map((to) => ({
    to,
    sound: "default",
    title: notification.title,
    body: notification.body,
    data: payloadData,
  }));

  const result = await sendExpoPushMessages(messages);
  const invalidTokens = Array.isArray(result?.invalidTokens) ? result.invalidTokens : [];

  if (invalidTokens.length) {
    await db.none("update push_tokens set revoked_at=now() where token = any($1::text[])", [invalidTokens]);
  }

  await db.none("update notifications set push_sent_at=coalesce(push_sent_at, now()) where id=$1", [notification.id]);

  return { ok: true, sent: expoTokens.length, invalid: invalidTokens.length };
}

