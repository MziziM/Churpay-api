import express from "express";
import { db } from "./db.js";
import { requireAuth } from "./auth.js";

const router = express.Router();
const UUID_REGEX = /^[0-9a-fA-F-]{36}$/;

router.post("/push-tokens/register", requireAuth, async (req, res) => {
  try {
    const token = typeof req.body?.token === "string" ? req.body.token.trim() : "";
    const platform = typeof req.body?.platform === "string" ? req.body.platform.trim().toLowerCase() : null;
    const deviceId = typeof req.body?.deviceId === "string" ? req.body.deviceId.trim() : null;

    if (!token) return res.status(400).json({ error: "token is required" });
    if (token.length > 512) return res.status(400).json({ error: "token is too long" });

    await db.none(
      `
      insert into push_tokens (member_id, token, platform, device_id, created_at, last_seen_at, revoked_at)
      values ($1,$2,$3,$4,now(),now(),null)
      on conflict (token) do update set
        member_id = excluded.member_id,
        platform = coalesce(excluded.platform, push_tokens.platform),
        device_id = coalesce(excluded.device_id, push_tokens.device_id),
        last_seen_at = now(),
        revoked_at = null
      `,
      [req.user.id, token, platform, deviceId]
    );

    res.json({ ok: true });
  } catch (err) {
    if (err?.code === "42P01") return res.status(404).json({ error: "Push tokens not enabled" });
    console.error("[push-tokens] register error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/push-tokens/unregister", requireAuth, async (req, res) => {
  try {
    const token = typeof req.body?.token === "string" ? req.body.token.trim() : "";
    if (!token) return res.status(400).json({ error: "token is required" });

    await db.none(
      `
      update push_tokens
      set revoked_at=now()
      where member_id=$1 and token=$2 and revoked_at is null
      `,
      [req.user.id, token]
    );

    res.json({ ok: true });
  } catch (err) {
    if (err?.code === "42P01") return res.status(404).json({ error: "Push tokens not enabled" });
    console.error("[push-tokens] unregister error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/notifications/unread-count", requireAuth, async (req, res) => {
  try {
    const row = await db.one(
      `
      select count(*)::int as count
      from notifications
      where member_id=$1 and read_at is null
      `,
      [req.user.id]
    );
    res.json({ count: Number(row.count || 0) });
  } catch (err) {
    if (err?.code === "42P01") return res.status(404).json({ error: "Notifications not enabled" });
    console.error("[notifications] unread-count error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/notifications", requireAuth, async (req, res) => {
  try {
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);
    const offset = Math.max(Number(req.query.offset || 0), 0);
    const unreadOnly = ["1", "true", "yes", "on"].includes(String(req.query.unread || "").toLowerCase());

    const where = ["member_id=$1"];
    const params = [req.user.id];
    if (unreadOnly) where.push("read_at is null");

    const countRow = await db.one(
      `
      select count(*)::int as count
      from notifications
      where ${where.join(" and ")}
      `,
      params
    );

    const rows = await db.manyOrNone(
      `
      select
        id,
        type,
        title,
        body,
        data,
        created_at as "createdAt",
        read_at as "readAt"
      from notifications
      where ${where.join(" and ")}
      order by created_at desc
      limit $2 offset $3
      `,
      [...params, limit, offset]
    );

    res.json({
      notifications: rows,
      meta: { limit, offset, count: Number(countRow.count || 0), returned: rows.length },
    });
  } catch (err) {
    if (err?.code === "42P01") return res.status(404).json({ error: "Notifications not enabled" });
    console.error("[notifications] list error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/notifications/:id/read", requireAuth, async (req, res) => {
  try {
    const id = req.params?.id;
    if (!id || !UUID_REGEX.test(id)) return res.status(400).json({ error: "Valid notification id is required" });

    const row = await db.oneOrNone(
      `
      update notifications
      set read_at = coalesce(read_at, now())
      where id=$1 and member_id=$2
      returning id, read_at as "readAt"
      `,
      [id, req.user.id]
    );
    if (!row) return res.status(404).json({ error: "Notification not found" });
    res.json({ ok: true, notification: row });
  } catch (err) {
    if (err?.code === "42P01") return res.status(404).json({ error: "Notifications not enabled" });
    console.error("[notifications] read error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/notifications/read-all", requireAuth, async (req, res) => {
  try {
    await db.none(
      `
      update notifications
      set read_at = now()
      where member_id=$1 and read_at is null
      `,
      [req.user.id]
    );
    res.json({ ok: true });
  } catch (err) {
    if (err?.code === "42P01") return res.status(404).json({ error: "Notifications not enabled" });
    console.error("[notifications] read-all error", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;

