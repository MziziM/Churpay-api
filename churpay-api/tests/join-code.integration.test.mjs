import assert from "node:assert/strict";
import test from "node:test";

import { ensureUniqueJoinCode } from "../src/join-code.js";

function makeMockDb({ churches = [], onboardingRequests = [] } = {}) {
  const churchCodes = new Set(churches.map((c) => String(c || "").trim().toUpperCase()).filter(Boolean));
  const onboardingCodes = new Set(
    onboardingRequests.map((c) => String(c || "").trim().toUpperCase()).filter(Boolean)
  );

  function allCodes() {
    return [...churchCodes, ...onboardingCodes];
  }

  return {
    any: async (_sql, params) => {
      const pattern = String(params?.[0] || "").toUpperCase(); // e.g. "GCCOC-%"
      const prefix = pattern.endsWith("%") ? pattern.slice(0, -1) : pattern;
      const codes = allCodes().filter((code) => code.startsWith(prefix));
      return codes.map((code) => ({ code }));
    },
    oneOrNone: async (_sql, params) => {
      const joinCode = String(params?.[0] || "").trim().toUpperCase();
      const exists = churchCodes.has(joinCode) || onboardingCodes.has(joinCode);
      return exists ? { ok: 1 } : null;
    },
  };
}

test("join codes increment based on onboarding requests too (GCCOC-00001 -> GCCOC-00002)", async () => {
  const db1 = makeMockDb();
  const code1 = await ensureUniqueJoinCode({ db: db1, churchName: "Great Commission Church of Christ" });
  assert.equal(code1, "GCCOC-00001");

  const db2 = makeMockDb({ onboardingRequests: [code1] });
  const code2 = await ensureUniqueJoinCode({ db: db2, churchName: "Great Commission Church of Christ" });
  assert.equal(code2, "GCCOC-00002");
});

test("join codes take the highest existing sequence across churches + onboarding requests", async () => {
  const db = makeMockDb({
    churches: ["GCCOC-00007"],
    onboardingRequests: ["GCCOC-00002", "GCCOC-00003"],
  });
  const code = await ensureUniqueJoinCode({ db, churchName: "Great Commission Church of Christ" });
  assert.equal(code, "GCCOC-00008");
});

test("desired join code is rejected if already used in onboarding requests", async () => {
  const db = makeMockDb({ onboardingRequests: ["GCCOC-00002"] });
  const code = await ensureUniqueJoinCode({
    db,
    churchName: "Great Commission Church of Christ",
    desiredJoinCode: "gccoc-00002",
  });
  assert.equal(code, "GCCOC-00003");
});

