import "dotenv/config";
import express from "express";
import path from "path";
import cors from "cors";
import payments from "./routes.payments.js";
import webhooks from "./routes.webhooks.js";
import superRoutes from "./routes.super.js";
import publicRoutes from "./routes.public.js";
import authRoutes from "./routes.auth.js";
import notificationRoutes from "./routes.notifications.js";
import { runNotificationJobs } from "./notification-jobs.js";
import { db } from "./db.js";

process.on("unhandledRejection", (e) => console.error("UNHANDLED REJECTION", e));
process.on("uncaughtException", (e) => console.error("UNCAUGHT EXCEPTION", e));

const app = express();
const isProduction = process.env.NODE_ENV === "production";
const jsonBodyLimit = String(process.env.JSON_BODY_LIMIT || "35mb");

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
const superRoutesEnabled = parseBool(process.env.SUPER_ROUTES_ENABLED, true);

if (isProduction) {
  if (!process.env.JWT_SECRET) {
    throw new Error("JWT_SECRET is required in production");
  }
  if (allowedOrigins.size === 0) {
    throw new Error("CORS_ORIGINS/PUBLIC_BASE_URL must include at least one allowed browser origin in production");
  }
  if (superRoutesEnabled) {
    const missingSuperCreds = !String(process.env.SUPER_ADMIN_EMAIL || "").trim() || !String(process.env.SUPER_ADMIN_PASSWORD || "").trim();
    if (missingSuperCreds) {
      throw new Error("SUPER_ADMIN_EMAIL and SUPER_ADMIN_PASSWORD are required in production when SUPER_ROUTES_ENABLED=true");
    }
  }
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
app.use((req, res, next) => {
  // Lightweight security-header baseline without external middleware dependency.
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "SAMEORIGIN");
  res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
  res.setHeader("X-DNS-Prefetch-Control", "off");
  if (isProduction) {
    res.setHeader("Strict-Transport-Security", "max-age=31536000; includeSubDomains");
  }
  next();
});
app.use(express.json({ limit: jsonBodyLimit }));

// Do not pre-parse PayFast ITN as urlencoded; that route captures raw body for signature.
app.use((req, res, next) => {
  if (req.originalUrl.startsWith("/webhooks/payfast/itn")) return next();
  return express.urlencoded({ extended: false })(req, res, next);
});

// Ensure API callers get JSON (not an HTML error page) for common body/parser failures.
// This is important for onboarding (base64 docs) and mobile clients that parse JSON.
app.use((err, req, res, next) => {
  if (!err) return next();

  const wantsJson = req.originalUrl?.startsWith("/api/") || req.originalUrl?.startsWith("/auth") || req.originalUrl?.startsWith("/webhooks");
  if (!wantsJson) return next(err);

  const status = Number(err.status || err.statusCode) || 500;
  const type = String(err.type || "");

  if (status === 413 || type === "entity.too.large") {
    return res.status(413).json({
      error: "Request is too large. Please upload smaller documents (or contact support to increase upload limits).",
    });
  }

  // Preserve existing error handling for other errors; return a safe message.
  console.error("[api] middleware error:", err?.message || err);
  return res.status(status >= 400 && status < 600 ? status : 500).json({ error: "Request failed" });
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

// Backward-compatible aliases for legacy PayFast callback URLs.
// Old links might still target /payfast/* without the /api prefix.
app.get("/payfast/return", (req, res) => {
  const query = req.url.includes("?") ? req.url.slice(req.url.indexOf("?")) : "";
  res.redirect(302, `/api/payfast/return${query}`);
});

app.get("/payfast/cancel", (req, res) => {
  const query = req.url.includes("?") ? req.url.slice(req.url.indexOf("?")) : "";
  res.redirect(302, `/api/payfast/cancel${query}`);
});

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

// App Links / Universal Links metadata (must not redirect).
// NOTE: express.static ignores dotfiles by default, so serve `.well-known/*` explicitly.
const WELL_KNOWN_DIR = path.join(process.cwd(), "public", ".well-known");

app.get("/.well-known/assetlinks.json", (_req, res) => {
  res.type("application/json");
  res.sendFile(path.join(WELL_KNOWN_DIR, "assetlinks.json"), (err) => {
    if (err) res.status(err?.statusCode || 404).end();
  });
});

app.get(["/.well-known/apple-app-site-association", "/apple-app-site-association"], (_req, res) => {
  res.type("application/json");
  res.sendFile(path.join(WELL_KNOWN_DIR, "apple-app-site-association"), (err) => {
    if (err) res.status(err?.statusCode || 404).end();
  });
});

// Serve static files from public directory
app.use(express.static("public"));

// Convenience redirect for admin portal root.
app.get("/admin", (_req, res) => {
  res.redirect(302, "/admin/");
});

// Super admin portal routes
if (superRoutesEnabled) {
  app.get("/super", (_req, res) => {
    res.redirect(302, "/super/");
  });

  app.get("/super/login", (_req, res) => {
    res.redirect(302, "/super/login/");
  });

  app.get("/super/login/", (_req, res) => {
    res.sendFile(path.join(process.cwd(), "public", "super", "login", "index.html"));
  });

  app.get(["/super/", "/super/dashboard", "/super/churches", "/super/onboarding", "/super/jobs", "/super/transactions", "/super/funds", "/super/members", "/super/settings", "/super/audit-logs"], (_req, res) => {
    res.sendFile(path.join(process.cwd(), "public", "super", "index.html"));
  });

  app.get("/super/churches/:churchId", (_req, res) => {
    res.sendFile(path.join(process.cwd(), "public", "super", "index.html"));
  });
}

// Serve the give landing page
app.get("/give", (req, res) => {
  res.sendFile(path.join(process.cwd(), "public", "give.html"));
});
app.get("/g/:joinCode", (_req, res) => {
  res.sendFile(path.join(process.cwd(), "public", "give.html"));
});

// Shareable giving links page (payer flow)
app.get("/l/:token", (_req, res) => {
  res.sendFile(path.join(process.cwd(), "public", "giving-link.html"));
});

// Auth routes exposed under multiple mounts to survive external prefixing
// Auth routes (keep mounts narrow so they don't intercept MVP open routes)
app.use("/api/auth", authRoutes);
app.use("/auth", authRoutes);
app.use("/api/public", publicRoutes);
app.use("/api", notificationRoutes);

function startNotificationJobsLoop() {
  const enabled = parseBool(process.env.NOTIFICATION_JOBS_ENABLED, false);
  if (!enabled) return;

  const tz = String(process.env.NOTIFICATION_TIMEZONE || "Africa/Johannesburg").trim() || "Africa/Johannesburg";
  const intervalMs = parseIntEnv("NOTIFICATION_JOBS_INTERVAL_MS", 15 * 60 * 1000);

  const run = async () => {
    try {
      const result = await runNotificationJobs({ tz });
      const birthdaysSent = Number(result?.jobs?.birthdays?.sent || 0);
      const cashSent = Number(result?.jobs?.cashReminder?.sent || 0);
      console.log("[jobs] notifications ran", { tz, birthdaysSent, cashSent });
    } catch (err) {
      console.error("[jobs] notifications failed", err?.message || err);
    }
  };

  // Kick once at boot, then periodically.
  run();
  setInterval(run, intervalMs).unref();
}

startNotificationJobsLoop();
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
if (superRoutesEnabled) {
  app.use("/api/super", superRoutes);
}
app.use("/webhooks", webhooks);
const PORT = process.env.PORT || 3001;

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Churpay API running on port ${PORT}`);
});
