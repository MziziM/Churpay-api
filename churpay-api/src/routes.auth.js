import express from "express";
import bcrypt from "bcryptjs";
import crypto from "node:crypto";
import { db } from "./db.js";
import { requireAuth, signUserToken } from "./auth.js";

const router = express.Router();
let ensureMembersStoragePromise = null;

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

function normalizeJoinCode(value) {
  if (!value) return null;
  const normalized = String(value).trim().toUpperCase();
  return normalized || null;
}

function generateJoinCode() {
  return `CH${crypto.randomBytes(3).toString("hex").toUpperCase()}`;
}

function toChurchProfile(row) {
  if (!row) return null;
  return {
    id: row.id,
    name: row.name || null,
    joinCode: row.join_code || null,
    createdAt: row.created_at || null,
  };
}

function toProfile(row) {
  if (!row) return null;
  return {
    id: row.id,
    fullName: row.full_name || row.fullName || row.name || null,
    phone: row.phone,
    email: row.email,
    role: row.role,
    churchId: row.church_id,
    churchName: row.church_name || null,
  };
}

function isRecoverableSqlError(err) {
  return err?.code === "42P01" || err?.code === "42703" || err?.code === "42P18";
}

function isUniqueViolation(err) {
  return err?.code === "23505";
}

function internalErrorPayload(err) {
  const payload = { error: "Internal server error" };
  if (process.env.NODE_ENV !== "production") {
    payload.detail = err?.message || String(err);
  }
  return payload;
}

async function ensureMembersAuthStorage() {
  if (ensureMembersStoragePromise) return ensureMembersStoragePromise;

  ensureMembersStoragePromise = (async () => {
    await db.none(
      `create table if not exists members (
        id uuid primary key default (md5(random()::text || clock_timestamp()::text)::uuid),
        full_name text,
        phone text,
        email text,
        password_hash text,
        role text not null default 'member',
        church_id uuid,
        created_at timestamptz not null default now(),
        updated_at timestamptz not null default now()
      )`
    );
    await db.none("alter table members add column if not exists full_name text");
    await db.none("alter table members add column if not exists phone text");
    await db.none("alter table members add column if not exists email text");
    await db.none("alter table members add column if not exists password_hash text");
    await db.none("alter table members add column if not exists role text");
    await db.none("alter table members add column if not exists church_id uuid");
    await db.none("alter table members add column if not exists created_at timestamptz not null default now()");
    await db.none("alter table members add column if not exists updated_at timestamptz not null default now()");
    await db.none("update members set role='member' where role is null");
    await db.none("alter table members alter column role set default 'member'");
    await db.none("create unique index if not exists idx_members_phone_unique on members (phone) where phone is not null");
    await db.none("create unique index if not exists idx_members_email_unique on members (lower(email)) where email is not null");
  })().catch((err) => {
    ensureMembersStoragePromise = null;
    throw err;
  });

  return ensureMembersStoragePromise;
}

function normalizeAuthRow(row, source) {
  if (!row) return null;
  return {
    id: row.id,
    full_name: row.full_name || row.name || null,
    phone: row.phone || null,
    email: row.email || null,
    role: row.role || "member",
    church_id: row.church_id || null,
    password_hash: row.password_hash || null,
    password: row.password || null,
    auth_source: source,
  };
}

async function findAuthMember({ normalizedPhone, normalizedEmail }) {
  try {
    await ensureMembersAuthStorage();
  } catch (err) {
    // If schema bootstrap is blocked, keep legacy users fallback path.
  }

  try {
    const row = await db.oneOrNone(
      `select id, full_name, phone, email, role, church_id, password_hash
       from members
       where (coalesce($1::text, '') <> '' and phone::text = $1::text)
          or (coalesce($2::text, '') <> '' and lower(email::text) = lower($2::text))
       limit 1`,
      [normalizedPhone, normalizedEmail]
    );
    if (row) return normalizeAuthRow(row, "members");
  } catch (err) {
    if (!isRecoverableSqlError(err)) throw err;
  }

  if (normalizedPhone) {
    try {
      const row = await db.oneOrNone("select * from users where phone=$1 limit 1", [normalizedPhone]);
      if (row) return normalizeAuthRow(row, "users");
    } catch (err) {
      if (!isRecoverableSqlError(err)) throw err;
    }
  }

  if (normalizedEmail) {
    try {
      const row = await db.oneOrNone("select * from users where lower(email)=lower($1) limit 1", [normalizedEmail]);
      if (row) return normalizeAuthRow(row, "users");
    } catch (err) {
      if (!isRecoverableSqlError(err)) throw err;
    }
  }

  return null;
}

async function checkPassword(member, plainPassword) {
  if (!member || typeof plainPassword !== "string" || !plainPassword) return false;

  if (typeof member.password_hash === "string" && member.password_hash) {
    const match = await bcrypt.compare(plainPassword, member.password_hash).catch(() => false);
    if (match) return true;
  }

  // Backwards compatibility for legacy users table that may store plain text passwords.
  if (member.auth_source === "users" && typeof member.password === "string" && member.password) {
    const bcryptMatch = await bcrypt.compare(plainPassword, member.password).catch(() => false);
    if (bcryptMatch) return true;
    return member.password === plainPassword;
  }

  return false;
}

async function listPublicTableColumns(tableName) {
  const rows = await db.any(
    `select column_name
     from information_schema.columns
     where table_schema = 'public' and table_name = $1::text`,
    [tableName]
  );
  return new Set(rows.map((r) => r.column_name));
}

async function insertLegacyUser({ full_name, normalizedPhone, normalizedEmail, password_hash }) {
  const columns = await listPublicTableColumns("users");
  if (!columns.size) return null;

  const insertCols = [];
  const placeholders = [];
  const params = [];
  const add = (col, value) => {
    insertCols.push(col);
    params.push(value);
    placeholders.push(`$${params.length}`);
  };

  if (columns.has("full_name")) add("full_name", full_name);
  else if (columns.has("name")) add("name", full_name);

  if (columns.has("phone")) add("phone", normalizedPhone);
  if (columns.has("email")) add("email", normalizedEmail);
  if (columns.has("role")) add("role", "member");

  if (columns.has("password_hash")) add("password_hash", password_hash);
  else if (columns.has("password")) add("password", password_hash);
  else throw new Error("users table has no password/password_hash column");

  if (!insertCols.length) throw new Error("users table has no compatible columns for register");

  let inserted = null;
  if (columns.has("id")) {
    inserted = await db.one(
      `insert into users (${insertCols.join(", ")})
       values (${placeholders.join(", ")})
       returning *`,
      params
    );
  } else {
    await db.none(
      `insert into users (${insertCols.join(", ")})
       values (${placeholders.join(", ")})`,
      params
    );
    inserted = await db.oneOrNone(
      `select * from users
       where (coalesce($1::text, '') <> '' and phone::text = $1::text)
          or (coalesce($2::text, '') <> '' and lower(email::text) = lower($2::text))
       limit 1`,
      [normalizedPhone, normalizedEmail]
    );
  }

  return normalizeAuthRow(inserted, "users");
}

async function createAuthMember({ full_name, normalizedPhone, normalizedEmail, password_hash }) {
  let bootstrapError = null;
  try {
    await ensureMembersAuthStorage();
  } catch (err) {
    bootstrapError = err;
  }

  try {
    const row = await db.one(
      `insert into members (full_name, phone, email, password_hash, role)
       values ($1,$2,$3,$4,'member')
       returning id, full_name, phone, email, role, church_id`,
      [full_name, normalizedPhone, normalizedEmail, password_hash]
    );
    return normalizeAuthRow(row, "members");
  } catch (err) {
    if (!isRecoverableSqlError(err)) throw err;
  }

  try {
    return await insertLegacyUser({ full_name, normalizedPhone, normalizedEmail, password_hash });
  } catch (err) {
    if (!isRecoverableSqlError(err)) throw err;
    if (bootstrapError) throw bootstrapError;
    return null;
  }
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

async function fetchChurch(churchId) {
  return db.oneOrNone(
    `select id, name, join_code, created_at
     from churches
     where id = $1`,
    [churchId]
  );
}

async function ensureUniqueJoinCode(desired) {
  let joinCode = normalizeJoinCode(desired) || generateJoinCode();
  for (let i = 0; i < 5; i += 1) {
    const exists = await db.oneOrNone("select id from churches where upper(join_code)=upper($1)", [joinCode]);
    if (!exists) return joinCode;
    joinCode = generateJoinCode();
  }
  throw new Error("Unable to generate unique join code");
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

    const existing = await findAuthMember({ normalizedPhone, normalizedEmail });
    if (existing) {
      if (normalizedPhone && existing.phone === normalizedPhone) {
        return res.status(409).json({ error: "Phone already registered" });
      }
      if (normalizedEmail && existing.email && String(existing.email).toLowerCase() === normalizedEmail) {
        return res.status(409).json({ error: "Email already registered" });
      }
      return res.status(409).json({ error: "Phone or email already registered" });
    }

    const password_hash = await bcrypt.hash(pwd, 10);

    const row = await createAuthMember({ full_name, normalizedPhone, normalizedEmail, password_hash });
    if (!row?.id) throw new Error("Failed to create user record");

    const profile = toProfile(row);
    const token = signUserToken(row);

    res.json({ token, profile, member: profile });
  } catch (err) {
    if (isUniqueViolation(err)) {
      return res.status(409).json({ error: "Phone or email already registered" });
    }
    console.error("[auth/register]", err?.message || err, err?.stack);
    res.status(500).json(internalErrorPayload(err));
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

    const row = await findAuthMember({ normalizedPhone, normalizedEmail });
    if (!row) return res.status(401).json({ error: "Invalid credentials" });

    const match = await checkPassword(row, pwd);
    if (!match) return res.status(401).json({ error: "Invalid credentials" });

    const profile = toProfile(row);
    const token = signUserToken(row);

    res.json({ token, profile, member: profile });
  } catch (err) {
    console.error("[auth/login]", err?.message || err, err?.stack);
    res.status(500).json(internalErrorPayload(err));
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
        updates.push("phone = null");
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
        updates.push("email = null");
      }
    }

    if (typeof password === "string" && password.trim()) {
      if (password.trim().length < 6) return res.status(400).json({ error: "Password must be at least 6 characters" });
      const hash = await bcrypt.hash(password.trim(), 10);
      updates.push(`password_hash = $${idx++}`);
      params.push(hash);
    }

    if (!updates.length) return res.status(400).json({ error: "No updates supplied" });

    updates.push("updated_at = now()");
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

router.get("/church/me", requireAuth, async (req, res) => {
  try {
    const member = await fetchMember(req.user.id);
    if (!member?.church_id) {
      return res.status(404).json({ error: "No church assigned" });
    }

    const church = await fetchChurch(member.church_id);
    if (!church) {
      return res.status(404).json({ error: "Church not found" });
    }

    return res.json({ church: toChurchProfile(church) });
  } catch (err) {
    console.error("[church/me]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/church/me", requireAuth, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ error: "Admin only" });
    }

    const name = typeof req.body?.name === "string" ? req.body.name.trim() : "";
    const requestedJoinCode = normalizeJoinCode(req.body?.joinCode);
    if (!name) return res.status(400).json({ error: "Church name is required" });

    const member = await fetchMember(req.user.id);
    if (member?.church_id) {
      return res.status(409).json({ error: "Admin already linked to a church" });
    }

    if (requestedJoinCode) {
      const existingCode = await db.oneOrNone("select id from churches where upper(join_code)=upper($1)", [requestedJoinCode]);
      if (existingCode) {
        return res.status(409).json({ error: "Join code already in use" });
      }
    }

    const joinCode = await ensureUniqueJoinCode(requestedJoinCode);
    const church = await db.one(
      `insert into churches (name, join_code)
       values ($1, $2)
       returning id, name, join_code, created_at`,
      [name, joinCode]
    );

    await db.none("update members set church_id=$1, updated_at=now() where id=$2", [church.id, req.user.id]);
    const freshMember = await fetchMember(req.user.id);
    const profile = toProfile(freshMember);

    return res.status(201).json({
      church: toChurchProfile(church),
      member: profile,
      profile,
    });
  } catch (err) {
    if (isUniqueViolation(err)) {
      return res.status(409).json({ error: "Join code already in use" });
    }
    console.error("[church/create]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.patch("/church/me", requireAuth, async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ error: "Admin only" });
    }

    const member = await fetchMember(req.user.id);
    if (!member?.church_id) {
      return res.status(404).json({ error: "No church assigned" });
    }

    const updates = [];
    const params = [];
    let idx = 1;

    if (typeof req.body?.name === "string") {
      const name = req.body.name.trim();
      if (!name) return res.status(400).json({ error: "Church name is required" });
      updates.push(`name = $${idx++}`);
      params.push(name);
    }

    if (typeof req.body?.joinCode !== "undefined") {
      const joinCode = normalizeJoinCode(req.body.joinCode);
      if (!joinCode) return res.status(400).json({ error: "Join code is required" });
      updates.push(`join_code = $${idx++}`);
      params.push(joinCode);
    }

    if (!updates.length) {
      return res.status(400).json({ error: "No updates supplied" });
    }

    params.push(member.church_id);
    const church = await db.one(
      `update churches
       set ${updates.join(", ")}
       where id = $${idx}
       returning id, name, join_code, created_at`,
      params
    );

    return res.json({ church: toChurchProfile(church) });
  } catch (err) {
    if (isUniqueViolation(err)) {
      return res.status(409).json({ error: "Join code already in use" });
    }
    console.error("[church/update]", err?.message || err, err?.stack);
    return res.status(500).json({ error: "Internal server error" });
  }
});

export { handleGetMe };
export default router;
