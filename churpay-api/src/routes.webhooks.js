import express from "express";
import { PaymentGatewayFactory } from "./payments/index.js";
import { applyPaymentEvent } from "./payments/apply-payment-event.js";

const router = express.Router();
const BUILD = "v2026-02-24-payment-gateway";

// PayFast sends application/x-www-form-urlencoded POST (ITN)
export const payfastItnRawParser = express.raw({
  type: (req) =>
    (req.headers["content-type"] || "")
      .toLowerCase()
      .startsWith("application/x-www-form-urlencoded"),
});

function toRawBody(input) {
  if (typeof input === "string") return input;
  if (Buffer.isBuffer(input)) return input.toString("utf8");
  return "";
}

// Debug helpers to confirm route mounts.
router.get("/build", (_req, res) => {
  res.json({ ok: true, build: BUILD });
});

router.get("/payfast/itn", (_req, res) => {
  res.status(200).json({ ok: true, route: "webhooks/payfast/itn", build: BUILD });
});

router.get("/payfast/subscription", (_req, res) => {
  res.status(200).json({ ok: true, route: "webhooks/payfast/subscription", build: BUILD });
});

export async function handlePayfastItn(req, res) {
  try {
    const rawBody = toRawBody(req.rawBody) || toRawBody(req.body);
    const gateway = PaymentGatewayFactory.getByProvider("payfast");
    const normalizedEvent = await gateway.handleWebhook({
      headers: req.headers,
      rawBody,
      query: req.query || {},
      body: req.body,
    });
    await applyPaymentEvent(normalizedEvent);
    return res.status(200).send("OK");
  } catch (err) {
    const statusCode = Number(err?.statusCode || 500);
    if (statusCode >= 400 && statusCode < 500) {
      return res.status(statusCode).send(String(err?.publicMessage || err?.message || "invalid webhook"));
    }
    console.error("[webhooks/payfast/itn] server error", err?.message || err, err?.stack);
    return res.status(500).send("server error");
  }
}

router.post("/payfast/itn", payfastItnRawParser, handlePayfastItn);
router.post("/payfast/subscription", payfastItnRawParser, handlePayfastItn);

export default router;
