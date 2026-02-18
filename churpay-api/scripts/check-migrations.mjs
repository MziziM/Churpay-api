#!/usr/bin/env node
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const migrationsDir = path.resolve(__dirname, "../migrations");

function fail(message) {
  console.error(`[migrate:lint] ${message}`);
  process.exit(1);
}

const allowedPattern = /^\d{8}_\d{4}__[a-z0-9_]+\.sql$|^\d+_[a-z0-9_]+\.sql$/i;

async function run() {
  let entries;
  try {
    entries = await fs.readdir(migrationsDir, { withFileTypes: true });
  } catch (err) {
    fail(`unable to read migrations dir: ${err?.message || err}`);
  }

  const files = entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(".sql"))
    .map((entry) => entry.name)
    .sort((a, b) => a.localeCompare(b));

  if (files.length === 0) {
    fail("no migration files found");
  }

  let previous = "";
  for (const file of files) {
    if (!allowedPattern.test(file)) {
      fail(`invalid migration filename format: ${file}`);
    }
    if (previous && file.localeCompare(previous) <= 0) {
      fail(`migration order is not strictly increasing: ${previous} -> ${file}`);
    }
    previous = file;

    const fullPath = path.join(migrationsDir, file);
    const content = await fs.readFile(fullPath, "utf8");
    if (!content.trim()) {
      fail(`migration file is empty: ${file}`);
    }
    if (!content.includes(";")) {
      fail(`migration file has no SQL terminator ';': ${file}`);
    }
  }

  console.log(`[migrate:lint] ok (${files.length} files)`);
}

run().catch((err) => fail(err?.message || String(err)));

