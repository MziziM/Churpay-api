#!/usr/bin/env node
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import crypto from "node:crypto";
import dotenv from "dotenv";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const migrationsDir = path.resolve(__dirname, "../migrations");
const mode = process.argv.includes("--check") ? "check" : "apply";
const defaultEnvFile = "/etc/churpay/churpay-api.env";
let db;

function loadRuntimeEnv() {
  const candidates = [
    process.env.CHURPAY_ENV_FILE,
    defaultEnvFile,
    path.resolve(process.cwd(), ".env"),
    path.resolve(process.cwd(), ".env.local"),
  ].filter(Boolean);

  const seen = new Set();
  for (const file of candidates) {
    if (seen.has(file)) continue;
    seen.add(file);
    if (!fs.existsSync(file)) continue;
    dotenv.config({ path: file, override: false });
  }
}

async function ensureMigrationsTable(db) {
  await db.none(
    `
    create table if not exists schema_migrations (
      id bigserial primary key,
      filename text not null unique,
      checksum text not null,
      applied_at timestamptz not null default now()
    )
    `
  );
}

function checksum(content) {
  return crypto.createHash("sha256").update(content).digest("hex");
}

async function loadMigrationFiles() {
  const entries = await fsp.readdir(migrationsDir, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(".sql"))
    .map((entry) => entry.name)
    .sort((a, b) => a.localeCompare(b));
}

async function run() {
  loadRuntimeEnv();
  ({ db } = await import("../src/db.js"));
  await ensureMigrationsTable(db);
  const files = await loadMigrationFiles();
  const existingRows = await db.manyOrNone(
    "select filename, checksum from schema_migrations order by filename asc"
  );
  const applied = new Map(existingRows.map((row) => [row.filename, row.checksum]));

  let pending = 0;
  for (const filename of files) {
    const fullPath = path.join(migrationsDir, filename);
    const content = await fsp.readFile(fullPath, "utf8");
    const fileChecksum = checksum(content);
    const appliedChecksum = applied.get(filename);

    if (appliedChecksum) {
      if (appliedChecksum !== fileChecksum) {
        throw new Error(`Checksum mismatch for already-applied migration: ${filename}`);
      }
      continue;
    }

    pending += 1;
    if (mode === "check") continue;

    process.stdout.write(`[migrate] applying ${filename}\n`);
    await db.tx(async (t) => {
      await t.none(content);
      await t.none(
        "insert into schema_migrations (filename, checksum) values ($1, $2)",
        [filename, fileChecksum]
      );
    });
  }

  if (mode === "check") {
    process.stdout.write(`[migrate] pending migrations: ${pending}\n`);
  } else {
    process.stdout.write(`[migrate] completed. applied=${pending}\n`);
  }
}

run()
  .then(async () => {
    if (db) await db.close().catch(() => {});
    process.exit(0);
  })
  .catch(async (err) => {
    console.error("[migrate] failed:", err?.message || err);
    if (db) await db.close().catch(() => {});
    process.exit(1);
  });
