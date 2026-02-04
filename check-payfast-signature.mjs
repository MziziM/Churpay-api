import crypto from "crypto";

function formEncodeComponent(value) {
  return encodeURIComponent(String(value))
    .replace(/%20/g, "+")
    .replace(/[!'()*]/g, (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase());
}

function encodeFormQuery(params) {
  const entries = Object.entries(params)
    .filter(([, v]) => v !== undefined && v !== null && String(v) !== "")
    .map(([k, v]) => [String(k), String(v).trim()]);

  entries.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));

  return entries.map(([k, v]) => `${k}=${formEncodeComponent(v)}`).join("&");
}

function md5(s) {
  return crypto.createHash("md5").update(s).digest("hex");
}

const url = process.argv[2];
const passphrase = process.argv[3] ?? ""; // empty in your sandbox env

if (!url) {
  console.log("Usage: node check-payfast-signature.mjs '<checkoutUrl>' ''");
  process.exit(1);
}

const u = new URL(url);
const params = Object.fromEntries(u.searchParams.entries());

const receivedSig = params.signature;
delete params.signature;

const base = encodeFormQuery(params);
const str = passphrase ? `${base}&passphrase=${formEncodeComponent(passphrase)}` : base;
const expectedSig = md5(str);

console.log("Received:", receivedSig);
console.log("Expected:", expectedSig);
console.log("Match:", receivedSig === expectedSig);
