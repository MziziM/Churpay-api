function normalizeJoinCode(value) {
  if (!value) return null;
  const normalized = String(value).trim().toUpperCase();
  return normalized || null;
}

function joinCodePrefixFromChurchName(name) {
  const raw = String(name || "")
    .normalize("NFKD")
    .replace(/[^\x20-\x7E]/g, "")
    .replace(/[^A-Za-z0-9\s]/g, " ")
    .trim();
  const words = raw.split(/\s+/).filter(Boolean);
  if (!words.length) return "CH";

  let prefix = words.map((word) => word[0]).join("").toUpperCase();
  if (!prefix) prefix = "CH";

  if (prefix.length < 2) {
    prefix = (words[0] || "CH").replace(/[^A-Za-z0-9]/g, "").slice(0, 2).toUpperCase();
  }

  if (prefix.length < 2) {
    prefix = `${prefix}H`;
  }

  return prefix.slice(0, 8);
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

async function listExistingJoinCodesByPrefix(dbOrTx, prefix) {
  const rows = await dbOrTx.any(
    `
    select join_code as code
    from churches
    where upper(join_code) like upper($1)
    union
    select requested_join_code as code
    from church_onboarding_requests
    where requested_join_code is not null
      and upper(requested_join_code) like upper($1)
    `,
    [`${prefix}-%`]
  );
  return rows
    .map((row) => String(row.code || "").trim().toUpperCase())
    .filter(Boolean);
}

async function joinCodeExists(dbOrTx, joinCode) {
  const row = await dbOrTx.oneOrNone(
    `
    select 1 as ok
    from churches
    where upper(join_code) = upper($1)
    union
    select 1 as ok
    from church_onboarding_requests
    where requested_join_code is not null
      and upper(requested_join_code) = upper($1)
    limit 1
    `,
    [joinCode]
  );
  return !!row?.ok;
}

async function generateSequentialJoinCode(dbOrTx, prefix, { minSequence = 1, pad = 5 } = {}) {
  const normalizedPrefix = String(prefix || "").trim().toUpperCase() || "CH";
  const existing = await listExistingJoinCodesByPrefix(dbOrTx, normalizedPrefix);
  // Accept any numeric suffix length so legacy codes like PREFIX-1234 still advance the sequence.
  // Newly generated codes are normalized to the configured pad length.
  const matcher = new RegExp(`^${escapeRegExp(normalizedPrefix)}-(\\d+)$`);
  let maxSuffix = Math.max(Number(minSequence) - 1, 0);

  for (const code of existing) {
    const match = code.match(matcher);
    if (!match) continue;
    const numeric = Number(match[1]);
    if (Number.isFinite(numeric) && numeric > maxSuffix) {
      maxSuffix = numeric;
    }
  }

  for (let attempt = 0; attempt < 100000; attempt += 1) {
    const seq = maxSuffix + 1 + attempt;
    const candidate = `${normalizedPrefix}-${String(seq).padStart(pad, "0")}`;
    const exists = await joinCodeExists(dbOrTx, candidate);
    if (!exists) return candidate;
  }

  throw new Error("Unable to generate unique join code");
}

async function ensureUniqueJoinCode({ db: dbOrTx, churchName, desiredJoinCode = null }) {
  const desired = normalizeJoinCode(desiredJoinCode);
  if (desired) {
    const exists = await joinCodeExists(dbOrTx, desired);
    if (!exists) return desired;
  }

  const prefix = joinCodePrefixFromChurchName(churchName);
  return generateSequentialJoinCode(dbOrTx, prefix, { minSequence: 1, pad: 5 });
}

export { ensureUniqueJoinCode, generateSequentialJoinCode, joinCodePrefixFromChurchName, normalizeJoinCode };
