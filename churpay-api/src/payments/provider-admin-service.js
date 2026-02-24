import { connectChurchPayfastCredentials, validatePayfastCredentialConnection } from "../payfast-church.js";

function normalizeProvider(value) {
  const provider = String(value || "payfast").trim().toLowerCase();
  return provider || "payfast";
}

export async function connectChurchPaymentProviderCredentials({
  provider = "payfast",
  churchId,
  merchantId,
  merchantKey,
  passphrase = "",
}) {
  const normalizedProvider = normalizeProvider(provider);
  if (normalizedProvider !== "payfast") {
    const err = new Error(`Unsupported payment provider: ${normalizedProvider}`);
    err.code = "PAYMENT_PROVIDER_UNSUPPORTED";
    throw err;
  }
  return connectChurchPayfastCredentials({
    churchId,
    merchantId,
    merchantKey,
    passphrase,
  });
}

export async function validatePaymentProviderConnection({
  provider = "payfast",
  churchId,
  merchantId,
  merchantKey,
  passphrase = "",
}) {
  const normalizedProvider = normalizeProvider(provider);
  if (normalizedProvider !== "payfast") {
    const err = new Error(`Unsupported payment provider: ${normalizedProvider}`);
    err.code = "PAYMENT_PROVIDER_UNSUPPORTED";
    throw err;
  }
  return validatePayfastCredentialConnection({
    churchId,
    merchantId,
    merchantKey,
    passphrase,
  });
}
