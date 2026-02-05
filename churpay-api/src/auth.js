import jwt from "jsonwebtoken";

const JWT_SECRET = process.env.JWT_SECRET || "dev-super-secret";

export function signSuperToken(email) {
  return jwt.sign({ role: "super", email }, JWT_SECRET, { expiresIn: "12h" });
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
