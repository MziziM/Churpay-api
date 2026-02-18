import { Pool } from "pg";

const connectionString = process.env.DATABASE_URL;
const insecureFlag = (process.env.PGSSLINSECURE || "").toLowerCase();
const insecure = ["1", "true", "yes", "on"].includes(insecureFlag);
const isProduction = (process.env.NODE_ENV || "").toLowerCase() === "production";
const rawCa = process.env.DATABASE_CA_CERT || process.env.DATABASE_SSL_CA || "";
const ca = rawCa ? String(rawCa).replace(/\\n/g, "\n") : "";

if (!connectionString) throw new Error("DATABASE_URL is missing");

let dbUrl;
try {
  dbUrl = new URL(connectionString);
} catch (e) {
  throw new Error(`Invalid DATABASE_URL: ${e?.message || e}`);
}

if (isProduction && insecure) {
  throw new Error("PGSSLINSECURE is not allowed in production; configure DATABASE_CA_CERT for strict TLS");
}

// Explicitly build config to avoid sslmode quirks.
const ssl = isProduction
  ? ca
    ? { ca, rejectUnauthorized: true, minVersion: "TLSv1.2" }
    : { rejectUnauthorized: true, minVersion: "TLSv1.2" }
  : ca
  ? { ca, rejectUnauthorized: true, minVersion: "TLSv1.2" }
  : insecure
  ? { rejectUnauthorized: false }
  : undefined;

const pool = new Pool({
  host: dbUrl.hostname,
  port: dbUrl.port ? Number(dbUrl.port) : 5432,
  user: decodeURIComponent(dbUrl.username || ""),
  password: decodeURIComponent(dbUrl.password || ""),
  database: (dbUrl.pathname || "/").replace(/^\//, ""),
  ssl,
});

console.log("DB SSL:", {
  insecure,
  hasCA: !!ca,
  strictTLS: !!ssl?.rejectUnauthorized,
  usingSystemCAStore: isProduction && !ca && !!ssl?.rejectUnauthorized,
  host: dbUrl.hostname,
  port: dbUrl.port || 5432,
});

pool.on("error", (err) => {
  console.error("[db] unexpected pool error", err?.message || err, err?.stack);
});

async function query(text, params) {
  return pool.query(text, params);
}

async function any(text, params) {
  const res = await query(text, params);
  return res.rows;
}

// Common pg-promise style name; returns an array of rows.
async function manyOrNone(text, params) {
  return any(text, params);
}

async function one(text, params) {
  const res = await query(text, params);
  if (res.rows.length !== 1) throw new Error(`Expected 1 row, got ${res.rows.length}`);
  return res.rows[0];
}

async function oneOrNone(text, params) {
  const res = await query(text, params);
  if (res.rows.length === 0) return null;
  if (res.rows.length > 1) throw new Error(`Expected 0 or 1 row, got ${res.rows.length}`);
  return res.rows[0];
}

async function none(text, params) {
  await query(text, params);
}

async function tx(fn) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const t = {
      query: (text, params) => client.query(text, params),
      any: async (text, params) => {
        const res = await client.query(text, params);
        return res.rows;
      },
      manyOrNone: async (text, params) => {
        const res = await client.query(text, params);
        return res.rows;
      },
      one: async (text, params) => {
        const res = await client.query(text, params);
        if (res.rows.length !== 1) throw new Error(`Expected 1 row, got ${res.rows.length}`);
        return res.rows[0];
      },
      oneOrNone: async (text, params) => {
        const res = await client.query(text, params);
        if (res.rows.length === 0) return null;
        if (res.rows.length > 1) throw new Error(`Expected 0 or 1 row, got ${res.rows.length}`);
        return res.rows[0];
      },
      none: async (text, params) => client.query(text, params),
    };
    const result = await fn(t);
    await client.query("COMMIT");
    return result;
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

async function close() {
  await pool.end();
}

export const db = { query, any, manyOrNone, one, oneOrNone, none, tx, close };
export default db;
