function normalize(value) {
  return String(value || "").trim();
}

function parseBool(value, fallback = false) {
  if (typeof value === "undefined" || value === null || value === "") return fallback;
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

function resolveMailMode() {
  const explicit = normalize(process.env.MAIL_DELIVERY_MODE).toLowerCase();
  if (explicit) return explicit;
  if (normalize(process.env.RESEND_API_KEY)) return "resend";
  if (normalize(process.env.EMAIL_WEBHOOK_URL)) return "webhook";
  return "log";
}

function mailFromAddress() {
  const value = normalize(process.env.MAIL_FROM);
  return value || "Churpay <no-reply@churpay.com>";
}

async function sendByResend({ to, subject, html, text }) {
  const apiKey = normalize(process.env.RESEND_API_KEY);
  if (!apiKey) throw new Error("RESEND_API_KEY is missing");

  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      from: mailFromAddress(),
      to: [to],
      subject,
      html,
      text,
    }),
  });

  const raw = await response.text().catch(() => "");
  if (!response.ok) {
    throw new Error(`Resend email failed: ${response.status} ${raw}`);
  }
  return { ok: true, provider: "resend", detail: raw || "ok" };
}

async function sendByWebhook({ to, subject, html, text }) {
  const url = normalize(process.env.EMAIL_WEBHOOK_URL);
  if (!url) throw new Error("EMAIL_WEBHOOK_URL is missing");

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      from: mailFromAddress(),
      to,
      subject,
      html,
      text,
    }),
  });

  const raw = await response.text().catch(() => "");
  if (!response.ok) {
    throw new Error(`Webhook email failed: ${response.status} ${raw}`);
  }
  return { ok: true, provider: "webhook", detail: raw || "ok" };
}

async function sendByLog({ to, subject, text }) {
  console.log("[email/log]", {
    to,
    subject,
    text,
  });
  return { ok: true, provider: "log", detail: "logged" };
}

export async function sendEmail({ to, subject, html, text }) {
  const recipient = normalize(to).toLowerCase();
  if (!recipient) throw new Error("Email recipient is required");
  const emailSubject = normalize(subject);
  if (!emailSubject) throw new Error("Email subject is required");

  const mode = resolveMailMode();
  const mustDeliver = parseBool(process.env.EMAIL_DELIVERY_REQUIRED, false);

  try {
    if (mode === "resend") return await sendByResend({ to: recipient, subject: emailSubject, html, text });
    if (mode === "webhook") return await sendByWebhook({ to: recipient, subject: emailSubject, html, text });
    return await sendByLog({ to: recipient, subject: emailSubject, html, text });
  } catch (err) {
    if (mustDeliver) throw err;
    console.error("[email] delivery failed, falling back to log:", err?.message || err);
    return await sendByLog({ to: recipient, subject: emailSubject, html, text });
  }
}

