import "dotenv/config";
import express from "express";
import path from "path";
import cors from "cors";
import payments from "./routes.payments.js";
import webhooks from "./routes.webhooks.js";
import superRoutes from "./routes.super.js";
import authRoutes from "./routes.auth.js";
import { db } from "./db.js";

process.on("unhandledRejection", (e) => console.error("UNHANDLED REJECTION", e));
process.on("uncaughtException", (e) => console.error("UNCAUGHT EXCEPTION", e));

const app = express();
const isProduction = process.env.NODE_ENV === "production";

function parseBool(value, fallback = false) {
  if (typeof value === "undefined" || value === null || value === "") return fallback;
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

function parseIntEnv(name, fallback) {
  const n = Number(process.env[name]);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

const trustProxyEnabled = parseBool(process.env.TRUST_PROXY, isProduction);
if (trustProxyEnabled) {
  app.set("trust proxy", parseIntEnv("TRUST_PROXY_HOPS", 1));
}

function buildAllowedOrigins() {
  const origins = new Set();
  const fromEnv = String(process.env.CORS_ORIGINS || "")
    .split(",")
    .map((v) => v.trim())
    .filter(Boolean);
  for (const origin of fromEnv) origins.add(origin);

  const base = process.env.PUBLIC_BASE_URL;
  if (base) {
    try {
      origins.add(new URL(base).origin);
    } catch (_) {}
  }

  return origins;
}

const allowedOrigins = buildAllowedOrigins();
if (isProduction && allowedOrigins.size === 0) {
  console.warn("[cors] NODE_ENV=production but CORS_ORIGINS/PUBLIC_BASE_URL allowlist is empty; only non-browser clients will work.");
}

const corsOptions = {
  origin(origin, cb) {
    // Non-browser clients (curl/mobile native fetch) have no Origin header.
    if (!origin) return cb(null, true);
    if (!isProduction) return cb(null, true);
    return cb(null, allowedOrigins.has(origin));
  },
  credentials: true,
  methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
  maxAge: 86400,
};

app.use(cors(corsOptions));
app.options("*", cors(corsOptions));
app.use(express.json());

// Do not pre-parse PayFast ITN as urlencoded; that route captures raw body for signature.
app.use((req, res, next) => {
  if (req.originalUrl.startsWith("/webhooks/payfast/itn")) return next();
  return express.urlencoded({ extended: false })(req, res, next);
});

function createRateLimiter({ windowMs, max, keyPrefix = "", skip }) {
  const buckets = new Map();
  let gcCounter = 0;

  return (req, res, next) => {
    if (typeof skip === "function" && skip(req)) return next();

    const now = Date.now();
    const ip = req.ip || req.socket?.remoteAddress || "unknown";
    const key = `${keyPrefix}${ip}`;
    let bucket = buckets.get(key);

    if (!bucket || bucket.resetAt <= now) {
      bucket = { count: 0, resetAt: now + windowMs };
    }

    bucket.count += 1;
    buckets.set(key, bucket);

    gcCounter += 1;
    if (gcCounter % 200 === 0) {
      for (const [bucketKey, value] of buckets.entries()) {
        if (value.resetAt <= now) buckets.delete(bucketKey);
      }
    }

    const remaining = Math.max(max - bucket.count, 0);
    res.setHeader("X-RateLimit-Limit", String(max));
    res.setHeader("X-RateLimit-Remaining", String(remaining));
    res.setHeader("X-RateLimit-Reset", String(Math.ceil(bucket.resetAt / 1000)));

    if (bucket.count > max) {
      const retryAfter = Math.max(1, Math.ceil((bucket.resetAt - now) / 1000));
      res.setHeader("Retry-After", String(retryAfter));
      return res.status(429).json({ error: "Too many requests" });
    }

    return next();
  };
}

const globalRateWindowMs = parseIntEnv("RATE_LIMIT_WINDOW_MS", 15 * 60 * 1000);
const globalRateMax = parseIntEnv("RATE_LIMIT_MAX", 300);
const authRateWindowMs = parseIntEnv("AUTH_RATE_LIMIT_WINDOW_MS", 15 * 60 * 1000);
const authRateMax = parseIntEnv("AUTH_RATE_LIMIT_MAX", 30);

app.use(
  createRateLimiter({
    windowMs: globalRateWindowMs,
    max: globalRateMax,
    skip: (req) => req.path === "/health" || req.path === "/api/health",
  })
);

const authRateLimiter = createRateLimiter({
  windowMs: authRateWindowMs,
  max: authRateMax,
  keyPrefix: "auth:",
});

app.use("/api/auth", authRateLimiter);
app.use("/auth", authRateLimiter);
app.use("/api/super/login", authRateLimiter);

app.get("/health", (_, res) => res.json({ ok: true }));
app.get("/api/health", (_, res) => res.json({ ok: true }));

// Build marker + route sanity check
const BUILD_MARKER = process.env.BUILD_MARKER || "dev";

app.get(["/build", "/api/build"], (_, res) => {
  res.json({ ok: true, build: BUILD_MARKER });
});

app.get(["/routes", "/api/routes"], (_, res) => {
  const routes = [];
  const stack = app?._router?.stack || [];
  for (const layer of stack) {
    if (layer?.route?.path) {
      const methods = Object.keys(layer.route.methods || {}).filter(Boolean);
      routes.push({ path: layer.route.path, methods });
    }
  }
  res.json({ ok: true, build: BUILD_MARKER, routes });
});

// Serve static files from public directory
app.use(express.static("public"));

// Serve the give landing page
app.get("/give", (req, res) => {
  res.sendFile(path.join(process.cwd(), "public", "give.html"));
});

// Auth routes exposed under multiple mounts to survive external prefixing
// Auth routes (keep mounts narrow so they don't intercept MVP open routes)
app.use("/api/auth", authRoutes);
app.use("/auth", authRoutes);
const demoModeEnabled = parseBool(process.env.DEMO_MODE, false);
if (demoModeEnabled) {
  // Demo-only open transactions routes intentionally mounted before protected payments router.
  app.get("/api/churches/me/transactions", async (req, res) => {
    try {
      const churchId = "09f9c0f2-c1b0-481b-8058-67853fb9b9dd";
      const limit = Math.min(Number(req.query.limit || 50), 200);
      const offset = Math.max(Number(req.query.offset || 0), 0);

      const sql = `
        select
          t.id,
          t.reference,
          t.amount,
          t.channel,
          t.created_at as "createdAt",
          pi.member_name as "memberName",
          pi.member_phone as "memberPhone",
          f.id as "fundId",
          f.code as "fundCode",
          f.name as "fundName"
        from transactions t
        join funds f on f.id = t.fund_id
        left join payment_intents pi on pi.id = t.payment_intent_id
        where t.church_id = $1
        order by t.created_at desc
        limit $2 offset $3;
      `;

      const { rows } = await db.query(sql, [churchId, limit, offset]);

      res.json({
        transactions: rows,
        meta: { limit, offset, count: rows.length },
      });
    } catch (err) {
      console.error("[transactions:me] demo error", err);
      res.status(500).json({ error: "Failed to load transactions" });
    }
  });

  app.get("/api/churches/:id/transactions", async (req, res) => {
    try {
      const churchId = req.params.id;

      // Demo safety: allow only demo church
      if (churchId !== "09f9c0f2-c1b0-481b-8058-67853fb9b9dd") {
        return res.status(403).json({ error: "Demo only" });
      }

      const limit = Math.min(Number(req.query.limit || 50), 200);
      const offset = Math.max(Number(req.query.offset || 0), 0);

      const sql = `
        select
          t.id,
          t.reference,
          t.amount,
          t.channel,
          t.created_at as "createdAt",
          pi.member_name as "memberName",
          pi.member_phone as "memberPhone",
          f.id as "fundId",
          f.code as "fundCode",
          f.name as "fundName"
        from transactions t
        join funds f on f.id = t.fund_id
        left join payment_intents pi on pi.id = t.payment_intent_id
        where t.church_id = $1
        order by t.created_at desc
        limit $2 offset $3;
      `;

      const { rows } = await db.query(sql, [churchId, limit, offset]);

      res.json({
        transactions: rows,
        meta: { limit, offset, count: rows.length },
      });
    } catch (err) {
      console.error("[transactions] demo error", err);
      res.status(500).json({ error: "Failed to load transactions" });
    }
  });
}
app.use("/api", payments);
app.use("/api/super", superRoutes);
app.use("/webhooks", webhooks);
const PORT = process.env.PORT || 3001;

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Churpay API running on port ${PORT}`);
});
