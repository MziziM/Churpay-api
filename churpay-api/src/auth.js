import jwt from "jsonwebtoken";

const isProduction = process.env.NODE_ENV === "production";
const JWT_SECRET = process.env.JWT_SECRET || (isProduction ? null : "churpay-dev-insecure-secret-change-me");
if (!JWT_SECRET) {
  throw new Error("JWT_SECRET is required in production");
}

if (!process.env.JWT_SECRET) {
  console.warn("[auth] JWT_SECRET missing; using insecure development fallback secret");
}

export function signSuperToken(email) {
  return jwt.sign({ role: "super", email }, JWT_SECRET, { expiresIn: "12h" });
}

export function getSuperAdminConfig() {
  return {
    email: String(process.env.SUPER_ADMIN_EMAIL || "").toLowerCase().trim(),
    password: String(process.env.SUPER_ADMIN_PASSWORD || ""),
  };
}

export function authenticateSuperAdmin(identifier, password) {
  const normalizedIdentifier = String(identifier || "").toLowerCase().trim();
  const normalizedPassword = String(password || "");

  if (!normalizedIdentifier || !normalizedPassword) {
    return { ok: false, status: 400, error: "Missing credentials" };
  }

  const config = getSuperAdminConfig();
  if (!config.email || !config.password) {
    return { ok: false, status: 500, error: "Super admin not configured" };
  }

  if (normalizedIdentifier !== config.email || normalizedPassword !== config.password) {
    return { ok: false, status: 401, error: "Invalid credentials" };
  }

  return {
    ok: true,
    token: signSuperToken(config.email),
    profile: { role: "super", email: config.email, fullName: "Super Admin" },
  };
}

export function signUserToken(member) {
  const payload = {
    id: member.id,
    role: member.role || "member",
    church_id: member.church_id || null,
    phone: member.phone || null,
    email: member.email || null,
    fullName: member.full_name || member.fullName || null,
  };
  return jwt.sign(payload, JWT_SECRET, { expiresIn: "12h" });
}

export function requireAuth(req, res, next) {
  try {
    const auth = req.headers["authorization"] || "";
    const [, token] = auth.split(" ");
    if (!token) return res.status(401).json({ error: "Missing token" });
    const payload = jwt.verify(token, JWT_SECRET);
    if (!payload || !payload.id) return res.status(401).json({ error: "Unauthorized" });
    req.user = payload;
    return next();
  } catch (e) {
    return res.status(401).json({ error: "Unauthorized" });
  }
}

export function requireAdmin(req, res, next) {
  return requireAuth(req, res, () => {
    if (req.user?.role !== "admin" && req.user?.role !== "super") {
      return res.status(403).json({ error: "Admin only" });
    }
    return next();
  });
}

export function requireStaff(req, res, next) {
  return requireAuth(req, res, () => {
    const role = String(req.user?.role || "").toLowerCase();
    if (role !== "admin" && role !== "accountant" && role !== "super") {
      return res.status(403).json({ error: "Staff only" });
    }
    return next();
  });
}

export function requireSuperAdmin(req, res, next) {
  try {
    const auth = req.headers["authorization"] || "";
    const [, token] = auth.split(" ");
    if (!token) return res.status(401).json({ error: "Missing token" });
    const payload = jwt.verify(token, JWT_SECRET);
    // Super tokens are signed by `signSuperToken()` and intentionally omit `id`.
    // Guard against privilege escalation if a member token is ever mis-issued with role "super".
    if (!payload || payload.id || !payload.email || (payload.role !== "super" && payload.role !== "super_admin")) {
      return res.status(403).json({ error: "Forbidden" });
    }
    req.superAdmin = payload;
    return next();
  } catch (e) {
    return res.status(401).json({ error: "Unauthorized" });
  }
}
