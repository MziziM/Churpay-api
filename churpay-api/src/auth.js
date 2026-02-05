import jwt from "jsonwebtoken";

const JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET) {
  throw new Error("JWT_SECRET is required");
}

export function signSuperToken(email) {
  return jwt.sign({ role: "super", email }, JWT_SECRET, { expiresIn: "12h" });
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
    if (req.user?.role !== "admin") {
      return res.status(403).json({ error: "Admin only" });
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
    if (!payload || payload.role !== "super") return res.status(403).json({ error: "Forbidden" });
    req.superAdmin = payload;
    return next();
  } catch (e) {
    return res.status(401).json({ error: "Unauthorized" });
  }
}
