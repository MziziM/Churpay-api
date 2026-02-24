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
const defaultEnvFile = "/etc/churpay/churpay-api.env";
let db;

function checksum(content) {
  return crypto.createHash("sha256").update(content).digest("hex");
}

function fail(message) {
  console.error(`[migrate:drift] ${message}`);
  process.exit(1);
}

function loadRuntimeEnv() {
  const explicitEnvFile = process.env.CHURPAY_ENV_FILE;
  const isProd = String(process.env.NODE_ENV || "").toLowerCase() === "production";
  const localEnv = [path.resolve(process.cwd(), ".env"), path.resolve(process.cwd(), ".env.local")];
  const candidates = explicitEnvFile
    ? [explicitEnvFile, ...localEnv]
    : isProd
    ? [defaultEnvFile, ...localEnv]
    : [...localEnv, defaultEnvFile];

  const seen = new Set();
  for (const file of candidates) {
    if (seen.has(file)) continue;
    seen.add(file);
    if (!fs.existsSync(file)) continue;
    dotenv.config({ path: file, override: false });
  }
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

  const files = await loadMigrationFiles();
  if (!files.length) fail("no migration files found");

  const hasMigrationsTable = await db.one(
    `
    select exists (
      select 1
      from information_schema.tables
      where table_schema = 'public' and table_name = 'schema_migrations'
    ) as ok
    `
  );
  if (!hasMigrationsTable?.ok) {
    fail("schema_migrations table is missing");
  }

  const rows = await db.manyOrNone("select filename, checksum from schema_migrations order by filename asc");
  const appliedMap = new Map(rows.map((row) => [row.filename, row.checksum]));
  const fileSet = new Set(files);

  const missing = [];
  const checksumMismatches = [];
  for (const filename of files) {
    const fullPath = path.join(migrationsDir, filename);
    const content = await fsp.readFile(fullPath, "utf8");
    const expectedChecksum = checksum(content);
    const appliedChecksum = appliedMap.get(filename);
    if (!appliedChecksum) {
      missing.push(filename);
      continue;
    }
    if (expectedChecksum !== appliedChecksum) {
      checksumMismatches.push(filename);
    }
  }

  const unknownApplied = rows
    .map((row) => row.filename)
    .filter((filename) => !fileSet.has(filename));

  if (missing.length || checksumMismatches.length || unknownApplied.length) {
    if (missing.length) {
      console.error(`[migrate:drift] pending/unapplied migrations: ${missing.join(", ")}`);
    }
    if (checksumMismatches.length) {
      console.error(`[migrate:drift] checksum mismatches: ${checksumMismatches.join(", ")}`);
    }
    if (unknownApplied.length) {
      console.error(`[migrate:drift] applied migrations missing from repo: ${unknownApplied.join(", ")}`);
    }
    process.exit(1);
  }

  console.log(`[migrate:drift] ok (${files.length} files, 0 drift)`);
}

run()
  .then(async () => {
    if (db) await db.close().catch(() => {});
    process.exit(0);
  })
  .catch(async (err) => {
    console.error("[migrate:drift] failed:", err?.message || err);
    if (db) await db.close().catch(() => {});
    process.exit(1);
  });
