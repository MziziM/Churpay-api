import React, { useEffect, useMemo, useState } from "react";
import { Link, NavLink, Route, Routes } from "react-router-dom";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "https://api.churpay.com";
const APP_LINK = import.meta.env.VITE_APP_LINK || "https://expo.dev";
const WHATSAPP_LINK = "https://wa.me/27830000000?text=Hi%20Churpay%2C%20I%20want%20to%20onboard%20my%20church";

function useAnalyticsPlaceholders() {
  useEffect(() => {
    // Placeholder: Google Analytics snippet entry point.
    if (window.__CHURPAY_GA_INIT__) return;
    window.__CHURPAY_GA_INIT__ = true;
  }, []);

  useEffect(() => {
    // Placeholder: Meta Pixel snippet entry point.
    if (window.__CHURPAY_META_INIT__) return;
    window.__CHURPAY_META_INIT__ = true;
  }, []);
}

function Layout({ children }) {
  return (
    <div className="site-shell">
      <header className="top-nav">
        <Link className="brand-link" to="/">
          <img src="/assets/churpay-logo.png" alt="Churpay" className="brand-logo" />
        </Link>
        <nav className="nav-links">
          <NavLink to="/churches">For Churches</NavLink>
          <NavLink to="/members">For Members</NavLink>
          <NavLink to="/pricing">Pricing</NavLink>
          <NavLink to="/security">Security</NavLink>
          <NavLink to="/about">About</NavLink>
          <NavLink to="/contact">Contact</NavLink>
          <NavLink to="/status">Status</NavLink>
        </nav>
        <div className="nav-cta-group">
          <a href="https://api.churpay.com/admin/" className="btn btn-ghost">Admin</a>
          <a href="https://api.churpay.com/super/" className="btn btn-ghost">Super</a>
          <a href="/contact" className="btn btn-primary">Book onboarding</a>
        </div>
      </header>

      <main>{children}</main>

      <footer className="site-footer">
        <div className="footer-brand">
          <img src="/assets/churpay-logo.png" alt="Churpay" className="footer-logo" />
          <p>Giving made easy.</p>
        </div>
        <div className="footer-links">
          <Link to="/legal/terms">Terms</Link>
          <Link to="/legal/privacy">Privacy</Link>
          <a href="https://api.churpay.com/admin/">Admin portal</a>
          <a href={APP_LINK} target="_blank" rel="noreferrer">App download</a>
        </div>
      </footer>
    </div>
  );
}

function Hero() {
  return (
    <section className="hero section">
      <div className="hero-copy">
        <span className="eyebrow">Built for South African churches</span>
        <h1>Fast, transparent church giving with fintech-grade trust.</h1>
        <p>
          Churpay helps churches onboard quickly, generate fund QR codes, and track every payment in real time.
          Members give in seconds with clear processing fee breakdown before checkout.
        </p>
        <div className="hero-cta">
          <Link className="btn btn-primary" to="/contact">Book demo</Link>
          <Link className="btn btn-ghost" to="/members">How giving works</Link>
        </div>
      </div>
      <div className="hero-card card">
        <h3>What churches get</h3>
        <ul>
          <li>Church onboarding with join code</li>
          <li>Fund setup (Tithe, Offering, Building)</li>
          <li>Secure PayFast checkout flow</li>
          <li>Admin dashboard and CSV exports</li>
          <li>Super admin reporting across churches</li>
        </ul>
      </div>
    </section>
  );
}

function FeatureBlocks() {
  return (
    <section className="section grid-3">
      <article className="card">
        <h3>Church onboarding</h3>
        <p>Launch your giving stack fast: church profile, join code, fund setup, and admin controls.</p>
      </article>
      <article className="card">
        <h3>Member-first giving</h3>
        <p>Members see amount, processing fee, and total charged before they confirm payment.</p>
      </article>
      <article className="card">
        <h3>Operations visibility</h3>
        <p>Track transactions, provider status, and exports from admin and super admin dashboards.</p>
      </article>
    </section>
  );
}

function TrustSection() {
  return (
    <section className="section trust">
      <div className="card">
        <h2>Trusted payment infrastructure</h2>
        <p>
          Checkout is powered by PayFast with signed callbacks, audited transaction records, and role-based access
          control across admin surfaces.
        </p>
        <div className="chip-row">
          <span className="chip">PayFast live flow</span>
          <span className="chip">JWT auth</span>
          <span className="chip">Rate-limited APIs</span>
          <span className="chip">PostgreSQL audit trail</span>
        </div>
      </div>
    </section>
  );
}

function HomePage() {
  return (
    <>
      <Hero />
      <FeatureBlocks />
      <TrustSection />
    </>
  );
}

function ChurchesPage() {
  return (
    <section className="section page">
      <h1>For Churches</h1>
      <p className="lead">From onboarding to reconciliation, Churpay gives your team full control with less admin work.</p>
      <div className="grid-2">
        <article className="card">
          <h3>1. Onboard your church</h3>
          <p>Create your profile, get a unique join code, and assign your admin team.</p>
        </article>
        <article className="card">
          <h3>2. Create your funds</h3>
          <p>Set up Tithe, Offering, Building projects, and any campaign-specific collection funds.</p>
        </article>
        <article className="card">
          <h3>3. Generate QR codes</h3>
          <p>Create QR codes per fund and share on screens, posters, and WhatsApp groups.</p>
        </article>
        <article className="card">
          <h3>4. Track performance</h3>
          <p>View transactions by fund/date and export CSV for finance and audit workflows.</p>
        </article>
      </div>
      <div className="cta-inline">
        <Link to="/contact" className="btn btn-primary">Start church onboarding</Link>
      </div>
    </section>
  );
}

function MembersPage() {
  return (
    <section className="section page">
      <h1>For Members</h1>
      <p className="lead">Give in seconds with transparent pricing and secure checkout.</p>
      <div className="grid-2">
        <article className="card">
          <h3>Choose your fund</h3>
          <p>Select the fund your church supports: Tithe, Offering, Building, or campaign-specific giving.</p>
        </article>
        <article className="card">
          <h3>See costs before paying</h3>
          <p>Before checkout, members see Amount, Processing fee, and Total charged.</p>
        </article>
        <article className="card">
          <h3>Secure PayFast checkout</h3>
          <p>Payment is processed via PayFast with callback confirmation and receipt-ready transaction references.</p>
        </article>
        <article className="card notice">
          <h3>Processing fee disclosure</h3>
          <p>
            Processing fee is <strong>R2.50 + 0.75% of amount</strong>.<br />
            Total charged = donation amount + processing fee.
          </p>
        </article>
      </div>
    </section>
  );
}

function PricingPage() {
  const examples = [50, 100, 500, 1000].map((amount) => {
    const fee = +(2.5 + amount * 0.0075).toFixed(2);
    const total = +(amount + fee).toFixed(2);
    return { amount, fee, total };
  });

  return (
    <section className="section page">
      <h1>Pricing</h1>
      <p className="lead">Churpay uses a transparent processing fee model. No subscription framing on this page.</p>
      <article className="card pricing-formula">
        <h3>Processing fee formula</h3>
        <p><strong>fee = R2.50 + (amount × 0.75%)</strong></p>
        <p><strong>total charged = amount + fee</strong></p>
      </article>
      <article className="card">
        <h3>Worked examples (ZAR)</h3>
        <table className="table">
          <thead>
            <tr>
              <th>Amount</th>
              <th>Processing fee</th>
              <th>Total charged</th>
            </tr>
          </thead>
          <tbody>
            {examples.map((row) => (
              <tr key={row.amount}>
                <td>R {row.amount.toFixed(2)}</td>
                <td>R {row.fee.toFixed(2)}</td>
                <td>R {row.total.toFixed(2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </article>
    </section>
  );
}

function SecurityPage() {
  return (
    <section className="section page">
      <h1>Security & Trust</h1>
      <p className="lead">Built with practical controls required for payment operations.</p>
      <div className="grid-2">
        <article className="card">
          <h3>Payment processing</h3>
          <p>PayFast live mode with signature-validated callback flow and intent-to-transaction reconciliation.</p>
        </article>
        <article className="card">
          <h3>Transport security</h3>
          <p>TLS/HTTPS via Let’s Encrypt certificates and Nginx reverse proxy hardening.</p>
        </article>
        <article className="card">
          <h3>Access control</h3>
          <p>JWT-based authentication with role gates for member, admin, and super admin operations.</p>
        </article>
        <article className="card">
          <h3>Operational safety</h3>
          <p>Rate limiting, CORS allowlist, structured logs, and production runbook incident playbooks.</p>
        </article>
      </div>
    </section>
  );
}

function AboutPage() {
  return (
    <section className="section page">
      <h1>About Churpay</h1>
      <p className="lead">Our mission is to make church giving simple, transparent, and accountable.</p>
      <article className="card">
        <p>
          Churpay is purpose-built for churches in South Africa. We focus on practical tools that help ministry teams run
          reliable giving operations while keeping member experience clear and fast.
        </p>
      </article>
    </section>
  );
}

function ContactPage() {
  const [status, setStatus] = useState({ type: "", message: "" });
  const [submitting, setSubmitting] = useState(false);

  async function onSubmit(event) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const payload = {
      fullName: String(form.get("fullName") || "").trim(),
      churchName: String(form.get("churchName") || "").trim(),
      email: String(form.get("email") || "").trim(),
      phone: String(form.get("phone") || "").trim(),
      message: String(form.get("message") || "").trim(),
    };

    if (!payload.fullName || !payload.email || !payload.message) {
      setStatus({ type: "error", message: "Please complete full name, email, and message." });
      return;
    }

    setSubmitting(true);
    setStatus({ type: "", message: "" });

    try {
      const response = await fetch(`${API_BASE}/api/public/contact`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const json = await response.json().catch(() => null);
      if (!response.ok) throw new Error(json?.error || "Could not send message");

      setStatus({ type: "success", message: "Thanks. We received your message and will contact you shortly." });
      event.currentTarget.reset();
    } catch (err) {
      setStatus({ type: "error", message: err.message || "Submission failed. Use WhatsApp below." });
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <section className="section page">
      <h1>Contact</h1>
      <p className="lead">Book onboarding, ask product questions, or request support.</p>
      <div className="grid-2">
        <form className="card form" onSubmit={onSubmit}>
          <label>
            Full name
            <input name="fullName" type="text" required />
          </label>
          <label>
            Church name
            <input name="churchName" type="text" />
          </label>
          <label>
            Email
            <input name="email" type="email" required />
          </label>
          <label>
            Phone
            <input name="phone" type="text" />
          </label>
          <label>
            Message
            <textarea name="message" rows={4} required />
          </label>
          <button type="submit" className="btn btn-primary" disabled={submitting}>
            {submitting ? "Sending..." : "Send message"}
          </button>
          {status.message ? <p className={`form-status ${status.type}`}>{status.message}</p> : null}
        </form>
        <article className="card">
          <h3>Direct channels</h3>
          <p>Email: <a href="mailto:hello@churpay.com">hello@churpay.com</a></p>
          <p>WhatsApp: <a href={WHATSAPP_LINK} target="_blank" rel="noreferrer">Start chat</a></p>
          <p>Admin portal: <a href="https://api.churpay.com/admin/">api.churpay.com/admin</a></p>
          <p>Super portal: <a href="https://api.churpay.com/super/">api.churpay.com/super</a></p>
        </article>
      </div>
    </section>
  );
}

function LegalPage({ title, children }) {
  return (
    <section className="section page">
      <h1>{title}</h1>
      <article className="card legal-copy">{children}</article>
    </section>
  );
}

function StatusPage() {
  const [loading, setLoading] = useState(true);
  const [result, setResult] = useState({ ok: false, statusCode: 0, checkedAt: "" });

  useEffect(() => {
    let alive = true;
    async function check() {
      try {
        const res = await fetch(`${API_BASE}/health`, { cache: "no-store" });
        const json = await res.json().catch(() => null);
        if (!alive) return;
        setResult({
          ok: !!json?.ok,
          statusCode: res.status,
          checkedAt: new Date().toISOString(),
        });
      } catch (_err) {
        if (!alive) return;
        setResult({ ok: false, statusCode: 0, checkedAt: new Date().toISOString() });
      } finally {
        if (alive) setLoading(false);
      }
    }
    check();
    return () => {
      alive = false;
    };
  }, []);

  const humanTime = useMemo(() => {
    if (!result.checkedAt) return "-";
    const d = new Date(result.checkedAt);
    return Number.isNaN(d.getTime()) ? result.checkedAt : d.toLocaleString();
  }, [result.checkedAt]);

  return (
    <section className="section page">
      <h1>Status</h1>
      <article className="card">
        <p><strong>API endpoint:</strong> {API_BASE}/health</p>
        {loading ? <p>Checking API health...</p> : null}
        {!loading ? (
          <>
            <p><strong>Healthy:</strong> {result.ok ? "Yes" : "No"}</p>
            <p><strong>HTTP status:</strong> {result.statusCode || "N/A"}</p>
            <p><strong>Checked at:</strong> {humanTime}</p>
          </>
        ) : null}
      </article>
    </section>
  );
}

function NotFoundPage() {
  return (
    <section className="section page">
      <h1>Page not found</h1>
      <p className="lead">The page you requested does not exist.</p>
      <Link to="/" className="btn btn-primary">Go home</Link>
    </section>
  );
}

export default function App() {
  useAnalyticsPlaceholders();

  return (
    <Layout>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/churches" element={<ChurchesPage />} />
        <Route path="/members" element={<MembersPage />} />
        <Route path="/pricing" element={<PricingPage />} />
        <Route path="/security" element={<SecurityPage />} />
        <Route path="/about" element={<AboutPage />} />
        <Route path="/contact" element={<ContactPage />} />
        <Route
          path="/legal/terms"
          element={(
            <LegalPage title="Terms of Use">
              <p>Churpay provides payment facilitation and giving operations tooling for churches.</p>
              <p>By using this service, users agree to lawful use, accurate account information, and payment provider terms.</p>
              <p>Platform terms may be updated as product, legal, or regulatory requirements change.</p>
            </LegalPage>
          )}
        />
        <Route
          path="/legal/privacy"
          element={(
            <LegalPage title="Privacy Policy">
              <p>Churpay processes personal and transaction data only for giving operations, reconciliation, support, and compliance.</p>
              <p>Data is protected using role-based access, encrypted transport, and operational controls.</p>
              <p>For privacy requests, contact hello@churpay.com.</p>
            </LegalPage>
          )}
        />
        <Route path="/status" element={<StatusPage />} />
        <Route path="*" element={<NotFoundPage />} />
      </Routes>
    </Layout>
  );
}
