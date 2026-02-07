#!/usr/bin/env node
import "dotenv/config";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import bcrypt from "bcryptjs";
import { db } from "../src/db.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const raw = argv[i];
    if (!raw.startsWith("--")) continue;

    const keyValue = raw.slice(2);
    const eq = keyValue.indexOf("=");
    if (eq >= 0) {
      const key = keyValue.slice(0, eq);
      const value = keyValue.slice(eq + 1);
      out[key] = value;
      continue;
    }

    const key = keyValue;
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) {
      out[key] = "true";
      continue;
    }

    out[key] = next;
    i += 1;
  }
  return out;
}

function normalizeEmail(input) {
  if (!input) return null;
  const normalized = String(input).trim().toLowerCase();
  return normalized || null;
}

function normalizePhone(input) {
  if (!input) return null;
  const normalized = String(input).trim();
  return normalized || null;
}

function normalizeJoinCode(input) {
  if (!input) return null;
  const normalized = String(input).trim().toUpperCase();
  return normalized || null;
}

async function runMigrations() {
  const migrationsDir = path.resolve(__dirname, "..", "migrations");
  const files = (await fs.readdir(migrationsDir)).filter((file) => file.endsWith(".sql")).sort();

  for (const file of files) {
    const sql = await fs.readFile(path.join(migrationsDir, file), "utf8");
    if (!sql.trim()) continue;
    await db.none(sql);
    console.log(`[seed-admin] applied migration: ${file}`);
  }
}

async function upsertChurch(t, churchName, joinCode) {
  const existing = await t.oneOrNone(
    "select id, name, join_code from churches where lower(name)=lower($1::text) limit 1",
    [churchName]
  );

  if (!existing) {
    return t.one(
      `insert into churches (name, join_code)
       values ($1::text, coalesce($2::text, 'CH' || upper(substr(md5(random()::text), 1, 6))))
       returning id, name, join_code`,
      [churchName, joinCode]
    );
  }

  if (joinCode && existing.join_code !== joinCode) {
    return t.one(
      "update churches set join_code=$1::text where id=$2::uuid returning id, name, join_code",
      [joinCode, existing.id]
    );
  }

  if (!existing.join_code) {
    return t.one(
      `update churches
       set join_code = coalesce($1::text, 'CH' || upper(substr(md5(random()::text), 1, 6)))
       where id = $2::uuid
       returning id, name, join_code`,
      [joinCode, existing.id]
    );
  }

  return existing;
}

async function upsertAdminMember(t, { adminName, adminPhone, adminEmail, adminPasswordHash, churchId }) {
  const existing = await t.oneOrNone(
    `select id, full_name, phone, email, role, church_id
     from members
     where (coalesce($1::text, '') <> '' and phone::text = $1::text)
        or (coalesce($2::text, '') <> '' and lower(email::text) = lower($2::text))
     limit 1`,
    [adminPhone, adminEmail]
  );

  if (!existing) {
    return t.one(
      `insert into members (full_name, phone, email, password_hash, role, church_id)
       values ($1::text, $2::text, $3::text, $4::text, 'admin', $5::uuid)
       returning id, full_name, phone, email, role, church_id`,
      [adminName, adminPhone, adminEmail, adminPasswordHash, churchId]
    );
  }

  return t.one(
    `update members
     set full_name = $1::text,
         phone = $2::text,
         email = $3::text,
         password_hash = $4::text,
         role = 'admin',
         church_id = $5::uuid
     where id = $6::uuid
     returning id, full_name, phone, email, role, church_id`,
    [adminName, adminPhone, adminEmail, adminPasswordHash, churchId, existing.id]
  );
}

function printUsage() {
  console.log(`
Usage:
  npm run seed:admin -- \\
    --church-name "Great Commission Church of Christ" \\
    --join-code "GCCOC-1234" \\
    --admin-name "Admin Test" \\
    --admin-phone "0710000000" \\
    --admin-email "admin@test.com" \\
    --admin-password "test123"

Options:
  --church-name      Required church name (or SEED_CHURCH_NAME)
  --join-code        Optional join code (or SEED_JOIN_CODE)
  --admin-name       Required admin full name (or SEED_ADMIN_NAME)
  --admin-phone      Optional if admin-email is provided (or SEED_ADMIN_PHONE)
  --admin-email      Optional if admin-phone is provided (or SEED_ADMIN_EMAIL)
  --admin-password   Required password >= 6 chars (or SEED_ADMIN_PASSWORD)
  --skip-migrations  Use "true" to skip running migrations first
  --help             Show this help
`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  if (args.help === "true" || args.help === true) {
    printUsage();
    return;
  }

  const churchName = String(args["church-name"] || process.env.SEED_CHURCH_NAME || "").trim();
  const joinCode = normalizeJoinCode(args["join-code"] || process.env.SEED_JOIN_CODE || null);
  const adminName = String(args["admin-name"] || process.env.SEED_ADMIN_NAME || "").trim();
  const adminPhone = normalizePhone(args["admin-phone"] || process.env.SEED_ADMIN_PHONE || null);
  const adminEmail = normalizeEmail(args["admin-email"] || process.env.SEED_ADMIN_EMAIL || null);
  const adminPassword = String(args["admin-password"] || process.env.SEED_ADMIN_PASSWORD || "");
  const skipMigrations = String(args["skip-migrations"] || "").toLowerCase() === "true";

  if (!churchName) throw new Error("church-name is required");
  if (!adminName) throw new Error("admin-name is required");
  if (!adminPhone && !adminEmail) throw new Error("admin-phone or admin-email is required");
  if (!adminPassword || adminPassword.length < 6) throw new Error("admin-password must be at least 6 characters");

  if (!skipMigrations) {
    await runMigrations();
  } else {
    console.log("[seed-admin] skipping migrations by request");
  }

  const adminPasswordHash = await bcrypt.hash(adminPassword, 10);

  const result = await db.tx(async (t) => {
    const church = await upsertChurch(t, churchName, joinCode);
    const admin = await upsertAdminMember(t, {
      adminName,
      adminPhone,
      adminEmail,
      adminPasswordHash,
      churchId: church.id,
    });

    return { church, admin };
  });

  console.log("\n[seed-admin] done");
  console.log(`[seed-admin] church: ${result.church.name} (${result.church.id})`);
  console.log(`[seed-admin] join code: ${result.church.join_code || "(none)"}`);
  console.log(`[seed-admin] admin: ${result.admin.full_name} (${result.admin.id})`);
  console.log(`[seed-admin] admin role: ${result.admin.role}`);
  console.log(`[seed-admin] admin church_id: ${result.admin.church_id}`);
}

main()
  .catch((err) => {
    console.error("[seed-admin] failed", err?.message || err);
    printUsage();
    process.exitCode = 1;
  })
  .finally(async () => {
    await db.close().catch(() => {});
  });
