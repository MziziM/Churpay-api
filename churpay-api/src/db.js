import { Pool } from "pg";

const connectionString = process.env.DATABASE_URL;
const insecureFlag = (process.env.PGSSLINSECURE || "").toLowerCase();
const insecure = insecureFlag === "1" || insecureFlag === "true" || insecureFlag === "yes" || insecureFlag === "on";

if (!connectionString) throw new Error("DATABASE_URL is missing");

let sslmode = null;
try {
  const u = new URL(connectionString);
  sslmode = u.searchParams.get("sslmode");
} catch {
  // ignore parse issues
}

const hasCA = !!process.env.DATABASE_CA_CERT;
const shouldAllowSelfSigned = insecure || (!!sslmode && !hasCA);

const pool = new Pool({
  connectionString,
  ssl: shouldAllowSelfSigned ? { rejectUnauthorized: false } : undefined,
});

console.log("DB SSL:", { sslmode, insecure: shouldAllowSelfSigned, hasCA });

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