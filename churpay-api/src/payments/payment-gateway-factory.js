import { db } from "../db.js";
import { PayFastGateway } from "./payfast-gateway.js";

const DEFAULT_PROVIDER = "payfast";

let hasPaymentProviderColumnPromise = null;
const gatewayCache = new Map();

function normalizeProvider(value) {
  const provider = String(value || "").trim().toLowerCase();
  if (provider === "payfast") return "payfast";
  return DEFAULT_PROVIDER;
}

async function hasChurchPaymentProviderColumn() {
  if (!hasPaymentProviderColumnPromise) {
    hasPaymentProviderColumnPromise = db
      .one(
        `
        select exists (
          select 1
          from information_schema.columns
          where table_schema = 'public' and table_name = 'churches' and column_name = 'payment_provider'
        ) as ok
        `
      )
      .then((row) => Boolean(row?.ok))
      .catch((err) => {
        hasPaymentProviderColumnPromise = null;
        throw err;
      });
  }
  return hasPaymentProviderColumnPromise;
}

async function resolveProviderForChurch(churchId, providerHint) {
  const normalizedHint = normalizeProvider(providerHint);
  if (normalizedHint && normalizedHint !== DEFAULT_PROVIDER) return normalizedHint;

  if (!churchId) return DEFAULT_PROVIDER;
  if (!(await hasChurchPaymentProviderColumn())) return DEFAULT_PROVIDER;

  const row = await db.oneOrNone("select payment_provider as provider from churches where id=$1 limit 1", [churchId]);
  return normalizeProvider(row?.provider || process.env.DEFAULT_PAYMENT_PROVIDER || DEFAULT_PROVIDER);
}

function getGatewayInstance(provider) {
  const normalizedProvider = normalizeProvider(provider);
  if (gatewayCache.has(normalizedProvider)) {
    return gatewayCache.get(normalizedProvider);
  }
  let gateway = null;
  if (normalizedProvider === "payfast") {
    gateway = new PayFastGateway();
  } else {
    gateway = new PayFastGateway();
  }
  gatewayCache.set(normalizedProvider, gateway);
  return gateway;
}

export const PaymentGatewayFactory = {
  async get(churchId, options = {}) {
    const provider = await resolveProviderForChurch(churchId, options.providerHint);
    return getGatewayInstance(provider);
  },
  getByProvider(provider) {
    return getGatewayInstance(provider);
  },
};

export default PaymentGatewayFactory;
