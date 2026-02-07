import express from "express";
import { db } from "./db.js";

const router = express.Router();

let tableReady = false;

function normalize(value) {
  return String(value || "").trim();
}

function validateEmail(value) {
  const v = normalize(value).toLowerCase();
  if (!v) return "";
  const ok = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v);
  return ok ? v : "";
}

async function ensureTable() {
  if (tableReady) return;
  await db.none(`
    create table if not exists public_contact_messages (
      id uuid primary key default gen_random_uuid(),
      full_name text not null,
      church_name text,
      email text not null,
      phone text,
      message text not null,
      source text not null default 'website',
      created_at timestamptz not null default now()
    )
  `);
  tableReady = true;
}

router.post("/contact", async (req, res) => {
  try {
    const fullName = normalize(req.body?.fullName);
    const churchName = normalize(req.body?.churchName);
    const email = validateEmail(req.body?.email);
    const phone = normalize(req.body?.phone);
    const message = normalize(req.body?.message);

    if (!fullName || !email || !message) {
      return res.status(400).json({ error: "fullName, email, and message are required" });
    }

    if (message.length > 3000) {
      return res.status(400).json({ error: "message is too long" });
    }

    await ensureTable();

    const row = await db.one(
      `
      insert into public_contact_messages (full_name, church_name, email, phone, message, source)
      values ($1, nullif($2, ''), $3, nullif($4, ''), $5, 'website')
      returning id, created_at
      `,
      [fullName, churchName, email, phone, message]
    );

    return res.status(201).json({
      data: {
        id: row.id,
        status: "received",
      },
      meta: {
        createdAt: row.created_at,
      },
    });
  } catch (err) {
    console.error("[public/contact]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Failed to submit contact message" });
  }
});

export default router;
