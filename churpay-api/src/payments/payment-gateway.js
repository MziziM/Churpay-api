export class PaymentGateway {
  async createIntent(_input) {
    throw new Error("createIntent not implemented");
  }

  async getIntentStatus(_input) {
    throw new Error("getIntentStatus not implemented");
  }

  async handleWebhook(_input) {
    throw new Error("handleWebhook not implemented");
  }

  async refund(_input) {
    throw new Error("refund not implemented");
  }
}

export function normalizeGatewayStatus(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (raw === "created") return "created";
  if (raw === "paid") return "paid";
  if (raw === "failed") return "failed";
  if (raw === "cancelled" || raw === "canceled") return "cancelled";
  if (raw === "refunded") return "refunded";
  return "pending";
}

export function makeGatewayError(message, { code, statusCode = 500, details } = {}) {
  const err = new Error(message || "Payment gateway error");
  err.code = code || "PAYMENT_GATEWAY_ERROR";
  err.statusCode = statusCode;
  if (typeof details !== "undefined") err.details = details;
  return err;
}
