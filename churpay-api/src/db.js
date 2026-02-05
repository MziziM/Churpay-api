import { Pool } from "pg";

const connectionString = process.env.DATABASE_URL;

function getSslConfig() {
  // Pull sslmode from env OR from DATABASE_URL (?sslmode=require)
  let sslmode = process.env.PGSSLMODE || null;
  let uselibpqcompat = false;

  try {
    if (connectionString) {
      const u = new URL(connectionString);
      sslmode = sslmode || u.searchParams.get("sslmode");
      uselibpqcompat = (u.searchParams.get("uselibpqcompat") || "").toLowerCase() === "true";
    }
  } catch {
    // ignore parse errors
  }

  const insecure = process.env.PGSSLINSECURE === "1";
  const ca = process.env.DATABASE_CA_CERT;

  // If no sslmode is set anywhere, return no SSL config.
  if (!sslmode) {
    return { ssl: undefined, sslmode: null, insecure };
  }

  // Normalize common values
  const mode = String(sslmode).toLowerCase();

  // Preferred secure path: provide CA cert
  if (ca && ca.trim().length > 0) {
    return {
      ssl: { ca, rejectUnauthorized: true },
      sslmode: mode,
      insecure,
    };
  }

  // Practical DO Managed PG path: if CA not provided, allow insecure SSL for now.
  if (insecure || mode === "require" || mode === "verify-full" || mode === "verify-ca" || mode === "prefer") {
    return {
      ssl: { rejectUnauthorized: false },
      sslmode: mode,
      insecure: true,
    };
  }

  return { ssl: undefined, sslmode: mode, insecure };
}

const sslInfo = getSslConfig();
console.log("DB SSL:", { hasCA: !!process.env.DATABASE_CA_CERT, insecure: sslInfo.insecure, sslmode: sslInfo.sslmode });

let pgConfig = {
  max: Number(process.env.PG_POOL_MAX || 10),
  idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS || 30000),
  connectionTimeoutMillis: Number(process.env.PG_CONN_TIMEOUT_MS || 10000),
  ssl: sslInfo.ssl,
};

if (!connectionString) throw new Error("DATABASE_URL is missing");

let dbUrl;
try {
  dbUrl = new URL(connectionString);
} catch (e) {
  throw new Error(`Invalid DATABASE_URL: ${e?.message || e}`);
}

pgConfig = {
  ...pgConfig,
  host: dbUrl.hostname,
  port: dbUrl.port ? Number(dbUrl.port) : 5432,
  user: decodeURIComponent(dbUrl.username || ""),
  password: decodeURIComponent(dbUrl.password || ""),
  database: (dbUrl.pathname || "/").replace(/^\//, ""),
};

console.log("DB Target:", { host: pgConfig.host, port: pgConfig.port, db: pgConfig.database });

const pool = new Pool(pgConfig);

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