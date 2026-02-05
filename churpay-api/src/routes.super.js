import express from "express";
import { signSuperToken, requireSuperAdmin } from "./auth.js";

const router = express.Router();

const SUPER_EMAIL = (process.env.SUPER_ADMIN_EMAIL || "").toLowerCase().trim();
const SUPER_PASS = process.env.SUPER_ADMIN_PASSWORD || "";

router.post("/login", async (req, res) => {
  try {
    const { email, password } = req.body || {};
    if (!email || !password) return res.status(400).json({ error: "Missing credentials" });
    if (!SUPER_EMAIL || !SUPER_PASS) return res.status(500).json({ error: "Super admin not configured" });

    const matchEmail = String(email || "").toLowerCase().trim() === SUPER_EMAIL;
    const matchPass = String(password) === SUPER_PASS;
    if (!matchEmail || !matchPass) return res.status(401).json({ error: "Invalid credentials" });

    const token = signSuperToken(SUPER_EMAIL);
    return res.json({ ok: true, token });
  } catch (err) {
    console.error("[super/login] error", err?.message || err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/ping", requireSuperAdmin, (req, res) => {
  return res.json({ ok: true, super: req.superAdmin?.email });
});

export default router;
