import express from "express";
import bcrypt from "bcryptjs";
import { db } from "./db.js";
import { requireAuth, signUserToken } from "./auth.js";

const router = express.Router();

function normalizeEmail(email) {
  if (!email) return null;
  const trimmed = String(email).trim().toLowerCase();
  return trimmed || null;
}

function normalizePhone(phone) {
  if (!phone) return null;
  const trimmed = String(phone).trim();
  return trimmed || null;
}

function toProfile(row) {
  if (!row) return null;
  return {
    id: row.id,
    fullName: row.full_name,
    phone: row.phone,
    email: row.email,
    role: row.role,
    churchId: row.church_id,
    churchName: row.church_name || null,
  };
}

async function fetchMember(memberId) {
  const sql = `
    select m.id, m.full_name, m.phone, m.email, m.role, m.church_id, c.name as church_name
    from members m
    left join churches c on c.id = m.church_id
    where m.id = $1
  `;
  return db.one(sql, [memberId]);
}

router.post("/register", async (req, res) => {
  try {
    const { fullName, phone, email, password } = req.body || {};

    const full_name = typeof fullName === "string" ? fullName.trim() : "";
    const normalizedPhone = normalizePhone(phone);
    const normalizedEmail = normalizeEmail(email);
    const pwd = typeof password === "string" ? password : "";

    if (!full_name) return res.status(400).json({ error: "Full name is required" });
    if (!pwd || pwd.length < 6) return res.status(400).json({ error: "Password must be at least 6 characters" });
    if (!normalizedPhone && !normalizedEmail) return res.status(400).json({ error: "Phone or email is required" });

    if (normalizedPhone) {
      const existingPhone = await db.oneOrNone("select id from members where phone=$1", [normalizedPhone]);
      if (existingPhone) return res.status(409).json({ error: "Phone already registered" });
    }

    if (normalizedEmail) {
      const existingEmail = await db.oneOrNone("select id from members where lower(email)=lower($1)", [normalizedEmail]);
      if (existingEmail) return res.status(409).json({ error: "Email already registered" });
    }

    const password_hash = await bcrypt.hash(pwd, 10);

    const row = await db.one(
      `insert into members (full_name, phone, email, password_hash, role)
       values ($1,$2,$3,$4,'member')
       returning id, full_name, phone, email, role, church_id`,
      [full_name, normalizedPhone, normalizedEmail, password_hash]
    );

    const profile = toProfile(row);
    const token = signUserToken(row);

    res.json({ token, profile, member: profile });
  } catch (err) {
    console.error("[auth/register]", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/login", async (req, res) => {
  try {
    const { phone, email, password, identifier } = req.body || {};
    const normalizedPhone = normalizePhone(phone || (identifier && !String(identifier).includes("@") ? identifier : null));
    const normalizedEmail = normalizeEmail(email || (identifier && String(identifier).includes("@") ? identifier : null));
    const pwd = typeof password === "string" ? password : "";

    if (!normalizedPhone && !normalizedEmail) {
      return res.status(400).json({ error: "Phone or email is required" });
    }
    if (!pwd) return res.status(400).json({ error: "Password is required" });

    const row = await db.oneOrNone(
      `select id, full_name, phone, email, role, church_id, password_hash
       from members
       where ($1 is not null and phone=$1) or ($2 is not null and lower(email)=lower($2))
       limit 1`,
      [normalizedPhone, normalizedEmail]
    );

    if (!row) return res.status(401).json({ error: "Invalid credentials" });

    const match = await bcrypt.compare(pwd, row.password_hash || "");
    if (!match) return res.status(401).json({ error: "Invalid credentials" });

    const profile = toProfile(row);
    const token = signUserToken(row);

    res.json({ token, profile, member: profile });
  } catch (err) {
    console.error("[auth/login]", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

async function handleGetMe(req, res) {
  try {
    const row = await fetchMember(req.user.id);
    const profile = toProfile(row);
    return res.json({ profile, member: profile });
  } catch (err) {
    console.error("[profile/me]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
}

router.get("/me", requireAuth, handleGetMe);
router.get("/profile/me", requireAuth, handleGetMe);

router.patch("/profile/me", requireAuth, async (req, res) => {
  try {
    const { fullName, phone, email, password } = req.body || {};
    const updates = [];
    const params = [];
    let idx = 1;

    if (typeof fullName === "string" && fullName.trim()) {
      updates.push(`full_name = $${idx++}`);
      params.push(fullName.trim());
    }

    if (typeof phone !== "undefined") {
      const normalized = normalizePhone(phone);
      if (normalized) {
        const existing = await db.oneOrNone("select id from members where phone=$1 and id<>$2", [normalized, req.user.id]);
        if (existing) return res.status(409).json({ error: "Phone already registered" });
        updates.push(`phone = $${idx++}`);
        params.push(normalized);
      } else {
        updates.push(`phone = null`);
      }
    }

    if (typeof email !== "undefined") {
      const normalized = normalizeEmail(email);
      if (normalized) {
        const existing = await db.oneOrNone("select id from members where lower(email)=lower($1) and id<>$2", [normalized, req.user.id]);
        if (existing) return res.status(409).json({ error: "Email already registered" });
        updates.push(`email = $${idx++}`);
        params.push(normalized);
      } else {
        updates.push(`email = null`);
      }
    }

    if (typeof password === "string" && password.trim()) {
      if (password.trim().length < 6) return res.status(400).json({ error: "Password must be at least 6 characters" });
      const hash = await bcrypt.hash(password.trim(), 10);
      updates.push(`password_hash = $${idx++}`);
      params.push(hash);
    }

    if (!updates.length) return res.status(400).json({ error: "No updates supplied" });

    updates.push(`updated_at = now()`);
    params.push(req.user.id);

    await db.none(`update members set ${updates.join(", ")} where id = $${idx}`, params);

    const fresh = await fetchMember(req.user.id);
    const profile = toProfile(fresh);
    res.json({ profile, member: profile });
  } catch (err) {
    console.error("[profile/update]", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/profile/church", requireAuth, async (req, res) => {
  try {
    const joinCode = typeof req.body?.joinCode === "string" ? req.body.joinCode.trim() : "";
    if (!joinCode) return res.status(400).json({ error: "Join code is required" });

    const church = await db.oneOrNone("select id, name, join_code from churches where upper(join_code)=upper($1)", [joinCode]);
    if (!church) return res.status(404).json({ error: "Invalid join code" });

    await db.none("update members set church_id=$1, updated_at=now() where id=$2", [church.id, req.user.id]);

    const fresh = await fetchMember(req.user.id);
    const profile = toProfile(fresh);
    res.json({ profile, member: profile, church: { id: church.id, name: church.name, joinCode: church.join_code } });
  } catch (err) {
    console.error("[profile/church]", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/churches/join", async (req, res) => {
  try {
    const joinCode = typeof req.body?.joinCode === "string" ? req.body.joinCode.trim() : "";
    if (!joinCode) return res.status(400).json({ error: "Join code is required" });

    const church = await db.oneOrNone("select id, name, join_code from churches where upper(join_code)=upper($1)", [joinCode]);
    if (!church) return res.status(404).json({ error: "Invalid join code" });

    res.json({ church: { id: church.id, name: church.name, joinCode: church.join_code } });
  } catch (err) {
    console.error("[churches/join]", err?.message || err, err?.stack);
    res.status(500).json({ error: "Internal server error" });
  }
});

export { handleGetMe };
export default router;
