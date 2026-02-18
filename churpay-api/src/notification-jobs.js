import { db } from "./db.js";
import { createNotification } from "./notifications.js";

function parseBool(value, fallback = false) {
  if (typeof value === "undefined" || value === null || value === "") return fallback;
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

function parseIntEnv(name, fallback) {
  const n = Number(process.env[name]);
  return Number.isFinite(n) ? n : fallback;
}

function firstName(fullName) {
  const normalized = String(fullName || "").trim();
  if (!normalized) return "";
  return normalized.split(/\s+/)[0] || "";
}

function resolveTimezone() {
  return String(process.env.NOTIFICATION_TIMEZONE || "Africa/Johannesburg").trim() || "Africa/Johannesburg";
}

export async function runBirthdayNotifications({ tz } = {}) {
  const timezone = tz || resolveTimezone();
  const enabled = parseBool(process.env.BIRTHDAY_NOTIFICATIONS_ENABLED, true);
  if (!enabled) return { ok: true, skipped: true, sent: 0 };

  const rows = await db.manyOrNone(
    `
    select m.id, m.full_name as "fullName"
    from members m
    where
      m.date_of_birth is not null
      and extract(month from m.date_of_birth) = extract(month from (now() at time zone $1)::date)
      and extract(day from m.date_of_birth) = extract(day from (now() at time zone $1)::date)
      and not exists (
        select 1
        from notifications n
        where
          n.member_id = m.id
          and n.type = 'BIRTHDAY'
          and (n.created_at at time zone $1)::date = (now() at time zone $1)::date
      )
    `,
    [timezone]
  );

  let sent = 0;
  for (const row of rows) {
    const name = firstName(row.fullName);
    await createNotification({
      memberId: row.id,
      type: "BIRTHDAY",
      title: "Happy birthday",
      body: name ? `Happy birthday ${name}! Have a blessed day.` : "Happy birthday! Have a blessed day.",
      data: { timezone, kind: "birthday" },
    });
    sent += 1;
  }

  return { ok: true, sent };
}

export async function runSaturdayCashReminder({ tz } = {}) {
  const timezone = tz || resolveTimezone();
  const enabled = parseBool(process.env.CASH_SATURDAY_REMINDER_ENABLED, false);
  if (!enabled) return { ok: true, skipped: true, sent: 0 };

  const hourLocal = Math.min(Math.max(parseIntEnv("CASH_SATURDAY_REMINDER_HOUR_LOCAL", 19), 0), 23);

  const local = await db.one(
    `
    select
      extract(dow from (now() at time zone $1))::int as dow,
      extract(hour from (now() at time zone $1))::int as hour
    `,
    [timezone]
  );

  if (Number(local.dow) !== 6 || Number(local.hour) < hourLocal) {
    return { ok: true, skipped: true, reason: "not_due", sent: 0 };
  }

  const members = await db.manyOrNone(
    `
    select m.id, m.full_name as "fullName"
    from members m
    where
      m.church_id is not null
      and lower(m.role) = 'member'
      and not exists (
        select 1
        from notifications n
        where
          n.member_id = m.id
          and n.type = 'CASH_REMINDER'
          and (n.created_at at time zone $1)::date = (now() at time zone $1)::date
      )
    `,
    [timezone]
  );

  let sent = 0;
  for (const member of members) {
    const name = firstName(member.fullName);
    await createNotification({
      memberId: member.id,
      type: "CASH_REMINDER",
      title: "Cash giving reminder",
      body: name
        ? `Hi ${name}, if you're giving cash tomorrow, remember to record it in Churpay.`
        : "If you're giving cash tomorrow, remember to record it in Churpay.",
      data: { timezone, kind: "cash_reminder" },
    });
    sent += 1;
  }

  return { ok: true, sent, hourLocal };
}

export async function runNotificationJobs({ tz } = {}) {
  const timezone = tz || resolveTimezone();
  const results = { ok: true, timezone, jobs: {} };

  results.jobs.birthdays = await runBirthdayNotifications({ tz: timezone });
  results.jobs.cashReminder = await runSaturdayCashReminder({ tz: timezone });

  return results;
}

