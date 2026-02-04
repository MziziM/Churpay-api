import pg from "pg";
const { Pool } = pg;

// SSL handling for managed Postgres (DigitalOcean) and local dev.
// - If PGSSLMODE is set (require/verify-full), enable SSL.
// - Prefer DATABASE_CA_CERT (PEM string). If missing and PGSSLINSECURE=1, allow insecure.
const sslmode = (process.env.PGSSLMODE || "").toLowerCase();
const hasSslMode = !!sslmode && sslmode !== "disable";
const caFromEnv = process.env.DATABASE_CA_CERT;
const insecure = String(process.env.PGSSLINSECURE || "").trim() === "1";

let ssl;
if (hasSslMode) {
  if (caFromEnv && caFromEnv.trim().length > 0) {
    ssl = { ca: caFromEnv, rejectUnauthorized: true };
  } else if (insecure) {
    ssl = { rejectUnauthorized: false };
  } else {
    ssl = { rejectUnauthorized: true };
  }
}

console.log("DB SSL:", { hasCA: !!caFromEnv, insecure, sslmode: sslmode || null });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ...(ssl ? { ssl } : {}),
  max: Number(process.env.PG_POOL_MAX || 10),
  idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS || 30000),
  connectionTimeoutMillis: Number(process.env.PG_CONN_TIMEOUT_MS || 10000),
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

export const db = { query, any, manyOrNone, one, oneOrNone, none, tx };
export default db;