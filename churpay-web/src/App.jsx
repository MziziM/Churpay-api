import React, { useEffect, useMemo, useState } from "react";
import { Link, NavLink, Route, Routes, useLocation, useParams } from "react-router-dom";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "https://api.churpay.com";
const APP_LINK = (import.meta.env.VITE_APP_LINK || "").trim();
const IOS_APP_LINK = (import.meta.env.VITE_IOS_APP_LINK || "").trim() || APP_LINK;
const ANDROID_APP_LINK = (import.meta.env.VITE_ANDROID_APP_LINK || "").trim() || APP_LINK;
const WHATSAPP_NUMBER_DISPLAY = "+27 63 092 8649";
const WHATSAPP_LINK = "https://wa.me/27630928649?text=Hi%20Churpay%2C%20I%20want%20to%20onboard%20my%20church";
const JOB_EMPLOYMENT_TYPE_LABELS = {
  FULL_TIME: "Full-time",
  PART_TIME: "Part-time",
  CONTRACT: "Contract",
  INTERNSHIP: "Internship",
  VOLUNTEER: "Volunteer",
};
const COOKIE_CONSENT_STORAGE_KEY = "churpay.cookie-consent.v1";
const COOKIE_CONSENT_ACCEPTED = "accepted";
const COOKIE_CONSENT_ESSENTIAL_ONLY = "essential_only";

function parseJsonSafe(raw) {
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch (_err) {
    return null;
  }
}

function looksLikeHtml(raw) {
  const s = String(raw || "").trim().toLowerCase();
  return s.startsWith("<!doctype html") || s.startsWith("<html") || s.startsWith("<head") || s.startsWith("<body");
}

function getStoredCookieConsent() {
  try {
    const value = String(window.localStorage.getItem(COOKIE_CONSENT_STORAGE_KEY) || "").trim();
    if (value === COOKIE_CONSENT_ACCEPTED || value === COOKIE_CONSENT_ESSENTIAL_ONLY) return value;
    return "";
  } catch (_err) {
    return "";
  }
}

function persistCookieConsent(value) {
  const normalized = String(value || "").trim();
  if (normalized !== COOKIE_CONSENT_ACCEPTED && normalized !== COOKIE_CONSENT_ESSENTIAL_ONLY) return;
  try {
    window.localStorage.setItem(COOKIE_CONSENT_STORAGE_KEY, normalized);
    window.localStorage.setItem(`${COOKIE_CONSENT_STORAGE_KEY}.updatedAt`, new Date().toISOString());
  } catch (_err) {
    // Best-effort persistence only.
  }
}

function linkifyText(raw) {
  const text = String(raw || "");
  const re = /(https?:\/\/[^\s]+)/g;
  const parts = [];
  let lastIndex = 0;

  for (;;) {
    const match = re.exec(text);
    if (!match) break;
    const url = match[1];
    const start = match.index;
    const end = start + url.length;

    if (start > lastIndex) parts.push(text.slice(lastIndex, start));
    parts.push(
      <a key={`link-${start}`} href={url} target="_blank" rel="noreferrer">
        {url}
      </a>
    );
    lastIndex = end;
  }

  if (lastIndex < text.length) parts.push(text.slice(lastIndex));
  return parts.length ? parts : text;
}

function renderPlainTextBody(raw) {
  const blocks = String(raw || "")
    .split(/\n\s*\n/g)
    .map((block) => block.trim())
    .filter(Boolean);

  return blocks.map((block, index) => {
    const lines = block
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean);

    const isList = lines.length > 1 && lines.every((line) => line.startsWith("- "));
    if (isList) {
      return (
        <ul key={`list-${index}`}>
          {lines.map((line, idx) => (
            <li key={`li-${index}-${idx}`}>{linkifyText(line.slice(2).trim())}</li>
          ))}
        </ul>
      );
    }

    return <p key={`p-${index}`}>{linkifyText(block)}</p>;
  });
}

function fileToPayload(file, { label = "Document", maxBytes = 10 * 1024 * 1024 } = {}) {
  return new Promise((resolve, reject) => {
    if (!(file instanceof File) || !file.size) return reject(new Error(`Missing required ${label.toLowerCase()}`));
    if (file.size > maxBytes) {
      return reject(new Error(`${label} must be ${(maxBytes / (1024 * 1024)).toFixed(0)}MB or smaller`));
    }

    const reader = new FileReader();
    reader.onload = () => {
      const dataUrl = typeof reader.result === "string" ? reader.result : "";
      const match = dataUrl.match(/^data:([^;]+);base64,(.+)$/i);
      if (!match) return reject(new Error("Invalid document encoding"));
      return resolve({
        filename: file.name,
        mimeType: match[1],
        base64: match[2],
      });
    };
    reader.onerror = () => reject(new Error("Could not read selected file"));
    reader.readAsDataURL(file);
  });
}

async function fetchLegalDocument(docKey) {
  const key = String(docKey || "").trim().toLowerCase();
  if (!key) throw new Error("Missing legal document key");

  const res = await fetch(`${API_BASE}/api/public/legal-documents/${encodeURIComponent(key)}`, {
    method: "GET",
    cache: "no-store",
  });
  const raw = await res.text();
  const json = parseJsonSafe(raw);
  if (!res.ok) throw new Error(json?.error || `Failed to load legal document (HTTP ${res.status})`);
  if (!json?.data) throw new Error("Invalid legal document response");
  return json.data;
}

function formatEmploymentType(value) {
  const key = String(value || "").trim().toUpperCase();
  if (!key) return "Role";
  return JOB_EMPLOYMENT_TYPE_LABELS[key] || key.replaceAll("_", " ");
}

function formatJobDate(value) {
  if (!value) return "Open";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleDateString();
}

function useAnalyticsPlaceholders() {
  useEffect(() => {
    const applyConsent = () => {
      if (getStoredCookieConsent() !== COOKIE_CONSENT_ACCEPTED) return;
      // Placeholder: Google Analytics snippet entry point.
      if (window.__CHURPAY_GA_INIT__) return;
      window.__CHURPAY_GA_INIT__ = true;
    };
    applyConsent();
    window.addEventListener("churpay-cookie-consent", applyConsent);
    return () => window.removeEventListener("churpay-cookie-consent", applyConsent);
  }, []);

  useEffect(() => {
    const applyConsent = () => {
      if (getStoredCookieConsent() !== COOKIE_CONSENT_ACCEPTED) return;
      // Placeholder: Meta Pixel snippet entry point.
      if (window.__CHURPAY_META_INIT__) return;
      window.__CHURPAY_META_INIT__ = true;
    };
    applyConsent();
    window.addEventListener("churpay-cookie-consent", applyConsent);
    return () => window.removeEventListener("churpay-cookie-consent", applyConsent);
  }, []);
}

function ExternalCta({ href, className, children }) {
  if (!href) {
    return (
      <span className={`${className} is-disabled`} role="link" aria-disabled="true" title="Download link coming soon">
        {children}
      </span>
    );
  }

  return (
    <a href={href} className={className} target="_blank" rel="noreferrer">
      {children}
    </a>
  );
}

function Layout({ children }) {
  const location = useLocation();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [cookieConsent, setCookieConsent] = useState(() => getStoredCookieConsent());

  useEffect(() => {
    setMobileMenuOpen(false);
  }, [location.pathname]);

  const showCookieBanner = !cookieConsent;
  const onAcceptCookies = () => {
    persistCookieConsent(COOKIE_CONSENT_ACCEPTED);
    setCookieConsent(COOKIE_CONSENT_ACCEPTED);
    window.dispatchEvent(new Event("churpay-cookie-consent"));
  };
  const onRejectNonEssentialCookies = () => {
    persistCookieConsent(COOKIE_CONSENT_ESSENTIAL_ONLY);
    setCookieConsent(COOKIE_CONSENT_ESSENTIAL_ONLY);
    window.dispatchEvent(new Event("churpay-cookie-consent"));
  };

  return (
    <div className="site-shell">
      <header className={`top-nav${mobileMenuOpen ? " is-open" : ""}`}>
        <div className="top-nav-inner">
          <Link className="brand-link" to="/">
            <img src="/assets/churpay-logo.svg" alt="Churpay" className="brand-logo" />
          </Link>
          <button
            type="button"
            className={`menu-toggle${mobileMenuOpen ? " is-open" : ""}`}
            aria-expanded={mobileMenuOpen}
            aria-controls="topNavPanel"
            aria-label="Toggle navigation"
            onClick={() => setMobileMenuOpen((value) => !value)}
          >
            <span></span>
            <span></span>
            <span></span>
          </button>
          <div id="topNavPanel" className={`top-nav-panel${mobileMenuOpen ? " is-open" : ""}`}>
            <nav className="nav-links">
              <NavLink to="/churches">For Churches</NavLink>
              <NavLink to="/members">For Members</NavLink>
              <NavLink to="/pricing">Pricing</NavLink>
              <NavLink to="/about">About</NavLink>
              <NavLink to="/contact">Contact</NavLink>
            </nav>
            <div className="nav-cta-group">
              <a href="https://api.churpay.com/admin/" className="btn btn-ghost">Admin</a>
              <Link to="/book-demo" className="btn btn-primary">Book demo</Link>
            </div>
          </div>
        </div>
      </header>

      <main>{children}</main>

      <footer className="site-footer">
        <div className="footer-main">
          <div className="footer-brand">
            <img src="/assets/churpay-logo.svg" alt="Churpay" className="footer-logo" />
            <p>Modern giving infrastructure for churches in South Africa.</p>
            <a href={WHATSAPP_LINK} target="_blank" rel="noreferrer" className="footer-contact-link">
              WhatsApp support: {WHATSAPP_NUMBER_DISPLAY}
            </a>
          </div>
          <div className="footer-columns">
            <section className="footer-column" aria-label="Platform links">
              <h4>Platform</h4>
              <Link to="/churches">For Churches</Link>
              <Link to="/members">For Members</Link>
              <Link to="/jobs">Jobs</Link>
              <Link to="/pricing">Pricing</Link>
            </section>
            <section className="footer-column" aria-label="Company links">
              <h4>Company</h4>
              <Link to="/about">About</Link>
              <Link to="/security">Security</Link>
              <Link to="/contact">Contact</Link>
              <Link to="/status">Status</Link>
            </section>
            <section className="footer-column" aria-label="Legal and access links">
              <h4>Legal & Access</h4>
              <Link to="/legal/terms">Terms</Link>
              <Link to="/legal/privacy">Privacy</Link>
              <Link to="/delete-account">Delete account</Link>
              <a href="https://api.churpay.com/admin/">Admin portal</a>
              <ExternalCta href={APP_LINK} className="footer-download-link">App download</ExternalCta>
            </section>
          </div>
        </div>
        <div className="footer-bottom">
          <p>© {new Date().getFullYear()} Churpay. All rights reserved.</p>
          <div className="footer-bottom-links">
            <a href="https://api.churpay.com/super/">Super Admin</a>
            <Link to="/jobs">Open roles</Link>
          </div>
        </div>
      </footer>

      {showCookieBanner ? (
        <aside className="cookie-banner" role="region" aria-label="Cookie consent">
          <div className="cookie-banner__content">
            <h4>Cookie consent</h4>
            <p>
              We use essential cookies for security and reliability, plus optional analytics cookies to improve Churpay.
              See our <Link to="/legal/privacy">Privacy Policy</Link> and <Link to="/legal/terms">Terms</Link>.
            </p>
          </div>
          <div className="cookie-banner__actions">
            <button type="button" className="btn btn-ghost" onClick={onRejectNonEssentialCookies}>
              Essential only
            </button>
            <button type="button" className="btn btn-primary" onClick={onAcceptCookies}>
              Accept all
            </button>
          </div>
        </aside>
      ) : null}
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
          <Link className="btn btn-primary" to="/book-demo">Book demo</Link>
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

function AppPreviewSection() {
  function renderDeviceScreen(deviceKey) {
    if (deviceKey === "ios") {
      return (
        <div className="app-screen ios-ui">
          <div className="app-status-row">
            <span>09:41</span>
            <span>5G • 100%</span>
          </div>
          <div className="app-screen-header">
            <strong>Give</strong>
            <span>The Great Commission Church of Christ</span>
          </div>
          <div className="screen-card-list">
            <div className="fund-row active">
              <span className="fund-dot"></span>
              <div>
                <strong>General Offering</strong>
                <span>Transparent checkout</span>
              </div>
              <em>Selected</em>
            </div>
            <div className="fund-row">
              <span className="fund-dot"></span>
              <div>
                <strong>Tithes</strong>
                <span>Recurring giving coming soon</span>
              </div>
              <em>R200</em>
            </div>
          </div>
          <div className="checkout-card">
            <h4>Checkout summary</h4>
            <div className="fee-line"><span>Amount</span><strong>R 200.00</strong></div>
            <div className="fee-line"><span>Processing fee</span><strong>R 4.00</strong></div>
            <div className="fee-line total"><span>Total charged</span><strong>R 204.00</strong></div>
            <button type="button">Continue to PayFast</button>
          </div>
        </div>
      );
    }

    return (
      <div className="app-screen android-ui">
        <div className="app-status-row">
          <span>09:43</span>
          <span>LTE • 98%</span>
        </div>
        <div className="success-card">
          <span className="success-badge">PAYMENT COMPLETE</span>
          <p>Reference</p>
          <strong>CP-5470B78CA750F3BE</strong>
          <div className="success-meta">
            <span>Gross charged: R 204.00</span>
            <span>Fund: General</span>
          </div>
        </div>
        <div className="history-card">
          <h4>Recent history</h4>
          <div className="history-row"><span>Cash (Recorded)</span><strong>R 100.00</strong></div>
          <div className="history-row"><span>PayFast (Paid)</span><strong>R 204.00</strong></div>
          <div className="history-row"><span>Prepared cash</span><strong>R 50.00</strong></div>
        </div>
      </div>
    );
  }

  const previewDevices = useMemo(
    () => [
      {
        key: "ios",
        title: "iPhone experience",
        kicker: "iOS giving flow",
        frameClass: "phone-iphone",
        notes: [
          "Clear fee transparency before checkout.",
          "Fast navigation from fund to secure confirmation.",
          "Consistent church and giving history context.",
        ],
      },
      {
        key: "android",
        title: "Android experience",
        kicker: "Android giving flow",
        frameClass: "phone-android",
        notes: [
          "Large, readable controls for one-hand use.",
          "Strong contrast on forms and transaction summaries.",
          "Consistent giving flow optimized for mid-range devices.",
        ],
      },
    ],
    []
  );
  const [activePreview, setActivePreview] = useState(previewDevices[0].key);
  const activeDevice = previewDevices.find((device) => device.key === activePreview) || previewDevices[0];

  return (
    <section className="section app-preview-section" id="app-download">
      <div className="app-preview-copy">
        <span className="eyebrow">Mobile experience</span>
        <h2>Premium mobile giving across iPhone and Android</h2>
        <p>
          Churpay is designed for real church moments: fast fund selection, clear processing fees, and trustworthy
          checkout feedback. The same polished flow works across iPhone and Android.
        </p>
        <div className="hero-cta">
          <ExternalCta className="btn btn-primary" href={IOS_APP_LINK}>Download for iPhone</ExternalCta>
          <ExternalCta className="btn btn-ghost" href={ANDROID_APP_LINK}>Download for Android</ExternalCta>
        </div>
      </div>

      <div className="preview-stage">
        <div className="preview-switch" role="tablist" aria-label="Choose device preview">
          {previewDevices.map((device) => (
            <button
              key={device.key}
              type="button"
              role="tab"
              aria-selected={activePreview === device.key}
              className={`preview-switch-btn${activePreview === device.key ? " is-active" : ""}`}
              onClick={() => setActivePreview(device.key)}
            >
              {device.key === "ios" ? "iPhone" : "Android"}
            </button>
          ))}
        </div>

        <article className="device-card spotlight-card">
          <header className="device-card-head">
            <h3>{activeDevice.title}</h3>
            <span>{activeDevice.kicker}</span>
          </header>
          <div className={`device-frame ${activeDevice.frameClass}`}>
            {activeDevice.frameClass === "phone-iphone" ? <div className="device-notch"></div> : <div className="device-camera"></div>}
            {renderDeviceScreen(activeDevice.key)}
          </div>
        </article>

        <div className="preview-notes" aria-live="polite">
          {activeDevice.notes.map((note) => (
            <p key={note} className="preview-note">{note}</p>
          ))}
        </div>
      </div>
    </section>
  );
}

function HomePage() {
  return (
    <>
      <Hero />
      <AppPreviewSection />
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
        <Link to="/onboarding" className="btn btn-primary">Start church onboarding</Link>
      </div>
    </section>
  );
}

function ChurchOnboardingPage() {
  const location = useLocation();
  const [submitting, setSubmitting] = useState(false);
  const [status, setStatus] = useState({ type: "", message: "", requestId: "" });
  const [trackId, setTrackId] = useState("");
  const [tracking, setTracking] = useState(false);
  const [trackedRequest, setTrackedRequest] = useState(null);
  const [trackError, setTrackError] = useState("");
  const [verifyStatus, setVerifyStatus] = useState({ type: "", message: "" });
  const [verifyCode, setVerifyCode] = useState("");
  const [verifying, setVerifying] = useState(false);
  const [resendingVerification, setResendingVerification] = useState(false);
  const [verificationContext, setVerificationContext] = useState({
    requestId: "",
    email: "",
    expiresAt: "",
    provider: "",
  });
  const [churchName, setChurchName] = useState("");
  const [requestedJoinCode, setRequestedJoinCode] = useState("");
  const [joinCodeLoading, setJoinCodeLoading] = useState(false);
  const [joinCodeError, setJoinCodeError] = useState("");
  const [adminFullName, setAdminFullName] = useState("");
  const [adminPhone, setAdminPhone] = useState("");
  const [adminEmail, setAdminEmail] = useState("");
  const [adminEmailConfirm, setAdminEmailConfirm] = useState("");
  const [adminPassword, setAdminPassword] = useState("");
  const [adminPasswordConfirm, setAdminPasswordConfirm] = useState("");
  const [bankAccounts, setBankAccounts] = useState([
    {
      bankName: "",
      accountName: "",
      accountNumber: "",
      branchCode: "",
      accountType: "",
      isPrimary: true,
    },
  ]);
  const [acceptTerms, setAcceptTerms] = useState(false);
  const [acceptCookies, setAcceptCookies] = useState(false);
  const [payfastFeesDoc, setPayfastFeesDoc] = useState(null);

  function updateBankAccount(index, patch) {
    setBankAccounts((prev) =>
      prev.map((row, idx) => (idx === index ? { ...row, ...patch } : row))
    );
  }

  function setPrimaryBankAccount(index) {
    setBankAccounts((prev) =>
      prev.map((row, idx) => ({ ...row, isPrimary: idx === index }))
    );
  }

  function addBankAccount() {
    setBankAccounts((prev) => ([
      ...prev,
      {
        bankName: "",
        accountName: "",
        accountNumber: "",
        branchCode: "",
        accountType: "",
        isPrimary: false,
      },
    ]));
  }

  function removeBankAccount(index) {
    setBankAccounts((prev) => {
      if (prev.length <= 1) return prev;
      const next = prev.filter((_row, idx) => idx !== index);
      const hasPrimary = next.some((row) => row.isPrimary);
      if (!hasPrimary && next.length) next[0] = { ...next[0], isPrimary: true };
      return next;
    });
  }

  useEffect(() => {
    let alive = true;
    fetchLegalDocument("payfast_fees")
      .then((doc) => {
        if (alive) setPayfastFeesDoc(doc);
      })
      .catch(() => {
        if (alive) setPayfastFeesDoc(null);
      });

    return () => {
      alive = false;
    };
  }, []);

  useEffect(() => {
    const normalizedChurchName = churchName.trim();
    if (!normalizedChurchName) {
      setRequestedJoinCode("");
      setJoinCodeError("");
      return;
    }

    let active = true;
    const timerId = window.setTimeout(async () => {
      setJoinCodeLoading(true);
      setJoinCodeError("");
      try {
        const response = await fetch(
          `${API_BASE}/api/public/church-onboarding/join-code-suggestion?churchName=${encodeURIComponent(normalizedChurchName)}`,
          { method: "GET", cache: "no-store" }
        );
        const raw = await response.text();
        const json = parseJsonSafe(raw);
        if (!response.ok) throw new Error(json?.error || "Could not generate join code");

        if (active) {
          setRequestedJoinCode(String(json?.data?.suggestedJoinCode || ""));
        }
      } catch (err) {
        if (active) {
          setRequestedJoinCode("");
          setJoinCodeError(err?.message || "Could not generate join code right now");
        }
      } finally {
        if (active) setJoinCodeLoading(false);
      }
    }, 250);

    return () => {
      active = false;
      window.clearTimeout(timerId);
    };
  }, [churchName]);

  async function onSubmit(event) {
    event.preventDefault();
    const formEl = event.currentTarget;
    const form = new FormData(formEl);

    const normalizedChurchName = churchName.trim();
    const normalizedJoinCode = requestedJoinCode.trim();
    const normalizedAdminFullName = adminFullName.trim();
    const normalizedAdminPhone = adminPhone.trim();
    const normalizedAdminEmail = adminEmail.trim().toLowerCase();
    const normalizedAdminEmailConfirm = adminEmailConfirm.trim().toLowerCase();
    const cipcDocument = form.get("cipcDocument");
    const bankConfirmationDocument = form.get("bankConfirmationDocument");
    const normalizedBankAccounts = bankAccounts
      .map((row) => ({
        bankName: String(row.bankName || "").trim(),
        accountName: String(row.accountName || "").trim(),
        accountNumber: String(row.accountNumber || "").replace(/\s+/g, "").trim(),
        branchCode: String(row.branchCode || "").trim(),
        accountType: String(row.accountType || "").trim(),
        isPrimary: !!row.isPrimary,
      }))
      .filter((row) => row.bankName || row.accountName || row.accountNumber || row.branchCode || row.accountType);

    if (!normalizedChurchName || !normalizedAdminFullName || !normalizedAdminPhone || !normalizedAdminEmail) {
      setStatus({ type: "error", message: "Complete all required onboarding fields.", requestId: "" });
      return;
    }
    if (!normalizedJoinCode) {
      setStatus({ type: "error", message: "Generated join code is required.", requestId: "" });
      return;
    }
    if (normalizedAdminEmail !== normalizedAdminEmailConfirm) {
      setStatus({ type: "error", message: "Admin email confirmation does not match.", requestId: "" });
      return;
    }
    if (!adminPassword || adminPassword.length < 8) {
      setStatus({ type: "error", message: "Admin password must be at least 8 characters.", requestId: "" });
      return;
    }
    if (adminPassword !== adminPasswordConfirm) {
      setStatus({ type: "error", message: "Admin password confirmation does not match.", requestId: "" });
      return;
    }
    if (!acceptTerms) {
      setStatus({ type: "error", message: "You must accept the Terms and Conditions to submit onboarding.", requestId: "" });
      return;
    }
    if (!acceptCookies) {
      setStatus({ type: "error", message: "You must accept cookie consent to submit onboarding.", requestId: "" });
      return;
    }

    if (!normalizedBankAccounts.length) {
      setStatus({ type: "error", message: "Add at least one bank account for payouts and verification.", requestId: "" });
      return;
    }
    if (normalizedBankAccounts.length > 5) {
      setStatus({ type: "error", message: "A maximum of 5 bank accounts is supported.", requestId: "" });
      return;
    }

    for (const acct of normalizedBankAccounts) {
      if (!acct.bankName || !acct.accountName || !acct.accountNumber) {
        setStatus({ type: "error", message: "Each bank account must include bank name, account name, and account number.", requestId: "" });
        return;
      }
    }
    const primaryCount = normalizedBankAccounts.reduce((acc, row) => acc + (row.isPrimary ? 1 : 0), 0);
    if (primaryCount > 1) {
      setStatus({ type: "error", message: "Choose only one primary bank account.", requestId: "" });
      return;
    }
    if (primaryCount === 0) normalizedBankAccounts[0].isPrimary = true;

    if (!(cipcDocument instanceof File) || !cipcDocument.size || !(bankConfirmationDocument instanceof File) || !bankConfirmationDocument.size) {
      setStatus({ type: "error", message: "Upload both CIPC and bank confirmation documents.", requestId: "" });
      return;
    }

    setSubmitting(true);
    setStatus({ type: "", message: "", requestId: "" });
    setTrackedRequest(null);
    setTrackError("");

    try {
      const [cipcPayload, bankPayload] = await Promise.all([
        fileToPayload(cipcDocument, { label: "CIPC document" }),
        fileToPayload(bankConfirmationDocument, { label: "Bank confirmation letter" }),
      ]);

      const payload = {
        churchName: normalizedChurchName,
        requestedJoinCode: normalizedJoinCode,
        adminFullName: normalizedAdminFullName,
        adminPhone: normalizedAdminPhone,
        adminEmail: normalizedAdminEmail,
        adminEmailConfirm: normalizedAdminEmailConfirm,
        adminPassword,
        adminPasswordConfirm,
        acceptTerms: true,
        acceptCookies: true,
        bankAccounts: normalizedBankAccounts,
        cipcDocument: cipcPayload,
        bankConfirmationDocument: bankPayload,
      };

      const response = await fetch(`${API_BASE}/api/public/church-onboarding`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const raw = await response.text();
      const json = parseJsonSafe(raw);
      if (!response.ok) {
        if (!json && looksLikeHtml(raw)) {
          throw new Error(
            `Could not submit onboarding request (HTTP ${response.status}). ` +
              "The server returned HTML instead of JSON (often upload too large or proxy error)."
          );
        }
        throw new Error(json?.error || `Could not submit onboarding request (HTTP ${response.status})`);
      }
      if (!json) {
        throw new Error("Unexpected response from server. Please retry.");
      }

      const requestId = String(json?.data?.id || "");
      const verificationEmail = String(json?.data?.adminEmail || normalizedAdminEmail || "").trim().toLowerCase();
      const verificationExpiresAt = String(json?.meta?.verification?.expiresAt || "");
      const verificationProvider = String(json?.meta?.verification?.provider || "").trim().toLowerCase();
      setStatus({
        type: "success",
        message: "Onboarding submitted. Verify admin email to move this request into review.",
        requestId,
      });
      setTrackId(requestId);
      setVerificationContext({
        requestId,
        email: verificationEmail,
        expiresAt: verificationExpiresAt,
        provider: verificationProvider,
      });
      setVerifyCode("");
      setVerifyStatus({
        type: verificationProvider === "log" ? "error" : "success",
        message:
          verificationProvider === "log"
            ? "Email delivery is not configured yet, so we could not send your verification code. Please contact support and try resending once email delivery is enabled."
            : verificationEmail
            ? `Verification code sent to ${verificationEmail}. Check your inbox/spam folder.`
            : "Verification code sent to admin email. Check your inbox/spam folder.",
      });
      // Don't access SyntheticEvent fields after awaits; hold DOM element ref.
      if (formEl && typeof formEl.reset === "function") formEl.reset();
      setChurchName("");
      setRequestedJoinCode("");
      setJoinCodeError("");
      setAdminFullName("");
      setAdminPhone("");
      setAdminEmail("");
      setAdminEmailConfirm("");
      setAdminPassword("");
      setAdminPasswordConfirm("");
      setAcceptTerms(false);
      setAcceptCookies(false);
      setBankAccounts([
        {
          bankName: "",
          accountName: "",
          accountNumber: "",
          branchCode: "",
          accountType: "",
          isPrimary: true,
        },
      ]);
    } catch (err) {
      setStatus({
        type: "error",
        message: err?.message || "Onboarding submission failed",
        requestId: "",
      });
    } finally {
      setSubmitting(false);
    }
  }

  async function onTrackStatus(event) {
    event.preventDefault();
    const requestId = trackId.trim();
    if (!requestId) {
      setTrackError("Enter your onboarding request ID.");
      setTrackedRequest(null);
      return;
    }

    setTracking(true);
    setTrackError("");
    setTrackedRequest(null);

    try {
      const response = await fetch(`${API_BASE}/api/public/church-onboarding/${encodeURIComponent(requestId)}`, {
        method: "GET",
        cache: "no-store",
      });
      const raw = await response.text();
      const json = parseJsonSafe(raw);
      if (!response.ok) throw new Error(json?.error || "Could not fetch request status");

      setTrackedRequest(json?.data || null);
      if (json?.data?.id) {
        setVerificationContext((prev) => ({
          requestId: String(json.data.id || prev.requestId || ""),
          email: String(json.data.adminEmail || prev.email || "").trim().toLowerCase(),
          expiresAt: String(prev.expiresAt || ""),
          provider: String(prev.provider || ""),
        }));
      }
    } catch (err) {
      setTrackError(err?.message || "Status lookup failed");
    } finally {
      setTracking(false);
    }
  }

  async function verifyAdminEmail({ requestId, email, code, token }) {
    const response = await fetch(`${API_BASE}/api/public/church-onboarding/${encodeURIComponent(requestId)}/verify-email`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        email,
        code,
        token,
      }),
    });
    const raw = await response.text();
    const json = parseJsonSafe(raw);
    if (!response.ok) throw new Error(json?.error || "Could not verify admin email");
    return json;
  }

  async function onVerifyCode(event) {
    if (event?.preventDefault) event.preventDefault();
    const requestId = verificationContext.requestId.trim();
    const email = verificationContext.email.trim().toLowerCase();
    const code = verifyCode.trim();

    if (!requestId) {
      setVerifyStatus({ type: "error", message: "Onboarding request ID is required for verification." });
      return;
    }
    if (!email) {
      setVerifyStatus({ type: "error", message: "Admin email is required for verification." });
      return;
    }
    if (!code) {
      setVerifyStatus({ type: "error", message: "Enter the verification code sent to your email." });
      return;
    }

    setVerifying(true);
    setVerifyStatus({ type: "", message: "" });
    try {
      const json = await verifyAdminEmail({ requestId, email, code, token: "" });
      const payload = json?.data || null;
      setTrackedRequest((prev) => ({
        ...(prev || {}),
        ...(payload || {}),
      }));
      setVerifyStatus({
        type: "success",
        message: json?.alreadyVerified ? "Admin email is already verified." : "Admin email verified successfully.",
      });
    } catch (err) {
      setVerifyStatus({ type: "error", message: err?.message || "Email verification failed" });
    } finally {
      setVerifying(false);
    }
  }

  async function onResendVerification() {
    const requestId = verificationContext.requestId.trim();
    const email = verificationContext.email.trim().toLowerCase();
    if (!requestId) {
      setVerifyStatus({ type: "error", message: "Onboarding request ID is required before resending." });
      return;
    }
    if (!email) {
      setVerifyStatus({ type: "error", message: "Admin email is required before resending." });
      return;
    }

    setResendingVerification(true);
    setVerifyStatus({ type: "", message: "" });
    try {
      const response = await fetch(`${API_BASE}/api/public/church-onboarding/${encodeURIComponent(requestId)}/resend-verification`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email }),
      });
      const raw = await response.text();
      const json = parseJsonSafe(raw);
      if (!response.ok) throw new Error(json?.error || "Could not resend verification email");

      setVerificationContext((prev) => ({
        ...prev,
        expiresAt: String(json?.data?.expiresAt || prev.expiresAt || ""),
        provider: String(json?.meta?.provider || prev.provider || ""),
      }));
      setVerifyStatus({
        type: String(json?.meta?.provider || "").trim().toLowerCase() === "log" ? "error" : "success",
        message:
          String(json?.meta?.provider || "").trim().toLowerCase() === "log"
            ? "Email delivery is not configured yet, so we could not send a verification email. Please contact support and try again later."
            : "Verification email sent again. Check your inbox/spam folder.",
      });
    } catch (err) {
      setVerifyStatus({ type: "error", message: err?.message || "Resend failed" });
    } finally {
      setResendingVerification(false);
    }
  }

  useEffect(() => {
    const params = new URLSearchParams(location.search || "");
    const requestId = (params.get("requestId") || "").trim();
    const email = (params.get("email") || "").trim().toLowerCase();
    const token = (params.get("token") || "").trim();
    if (!requestId || !token) return;

    setTrackId(requestId);
    setVerificationContext((prev) => ({
      requestId,
      email: email || prev.email || "",
      expiresAt: prev.expiresAt || "",
      provider: prev.provider || "",
    }));

    let active = true;
    setVerifying(true);
    setVerifyStatus({ type: "", message: "" });

    verifyAdminEmail({ requestId, email, code: "", token })
      .then((json) => {
        if (!active) return;
        const payload = json?.data || null;
        if (payload) setTrackedRequest((prev) => ({ ...(prev || {}), ...payload }));
        setVerifyStatus({
          type: "success",
          message: json?.alreadyVerified
            ? "Admin email was already verified."
            : "Admin email verified from your secure link.",
        });
      })
      .catch((err) => {
        if (!active) return;
        setVerifyStatus({ type: "error", message: err?.message || "Could not verify email from link" });
      })
      .finally(() => {
        if (active) setVerifying(false);
      });

    return () => {
      active = false;
    };
  }, [location.search]);

  const verificationStatus = String(trackedRequest?.verificationStatus || "").toLowerCase();

  return (
    <section className="section page">
      <h1>Church Onboarding</h1>
      <p className="lead">Submit verification documents and we will review your church profile for approval.</p>
      <div className="grid-2">
        <form className="card form onboarding-form" onSubmit={onSubmit}>
          <label>
            Church name
            <input name="churchName" type="text" required value={churchName} onChange={(event) => setChurchName(event.target.value)} />
          </label>
          <label>
            Generated join code
            <input
              name="requestedJoinCode"
              type="text"
              value={requestedJoinCode}
              readOnly
              required
              placeholder={joinCodeLoading ? "Generating..." : "Auto-generated from church name"}
            />
          </label>
          {joinCodeError ? <p className="form-status error">{joinCodeError}</p> : null}
          <label>
            Admin full name
            <input name="adminFullName" type="text" required value={adminFullName} onChange={(event) => setAdminFullName(event.target.value)} />
          </label>
          <label>
            Admin phone number
            <input name="adminPhone" type="text" required value={adminPhone} onChange={(event) => setAdminPhone(event.target.value)} />
          </label>
          <label>
            Admin email
            <input name="adminEmail" type="email" required value={adminEmail} onChange={(event) => setAdminEmail(event.target.value)} />
          </label>
          <label>
            Confirm admin email
            <input
              name="adminEmailConfirm"
              type="email"
              required
              value={adminEmailConfirm}
              onChange={(event) => setAdminEmailConfirm(event.target.value)}
            />
          </label>
          <label>
            Admin password
            <input
              name="adminPassword"
              type="password"
              minLength={8}
              required
              value={adminPassword}
              onChange={(event) => setAdminPassword(event.target.value)}
            />
          </label>
          <label>
            Confirm admin password
            <input
              name="adminPasswordConfirm"
              type="password"
              minLength={8}
              required
              value={adminPasswordConfirm}
              onChange={(event) => setAdminPasswordConfirm(event.target.value)}
            />
          </label>
          <fieldset className="bank-accounts">
            <legend>Bank account details</legend>
            <p className="form-hint">Add one or more church bank accounts and mark one as primary.</p>
            {bankAccounts.map((acct, index) => (
              <div className="bank-account" key={`${index}`}>
                <div className="bank-account-head">
                  <strong>Account {index + 1}</strong>
                  {bankAccounts.length > 1 ? (
                    <button type="button" className="btn btn-ghost btn-small" onClick={() => removeBankAccount(index)}>
                      Remove
                    </button>
                  ) : null}
                </div>
                <div className="bank-account-grid">
                  <label>
                    Bank name
                    <input
                      type="text"
                      value={acct.bankName}
                      onChange={(event) => updateBankAccount(index, { bankName: event.target.value })}
                      required
                      placeholder="e.g. Capitec"
                    />
                  </label>
                  <label>
                    Account name
                    <input
                      type="text"
                      value={acct.accountName}
                      onChange={(event) => updateBankAccount(index, { accountName: event.target.value })}
                      required
                      placeholder="Account holder name"
                    />
                  </label>
                  <label>
                    Account number
                    <input
                      type="text"
                      inputMode="numeric"
                      value={acct.accountNumber}
                      onChange={(event) => updateBankAccount(index, { accountNumber: event.target.value })}
                      required
                      placeholder="Digits only"
                    />
                  </label>
                  <label>
                    Branch code (optional)
                    <input
                      type="text"
                      inputMode="numeric"
                      value={acct.branchCode}
                      onChange={(event) => updateBankAccount(index, { branchCode: event.target.value })}
                      placeholder="e.g. 470010"
                    />
                  </label>
                  <label>
                    Account type (optional)
                    <input
                      type="text"
                      value={acct.accountType}
                      onChange={(event) => updateBankAccount(index, { accountType: event.target.value })}
                      placeholder="Cheque / Savings"
                    />
                  </label>
                  <label className="bank-account-primary">
                    Primary account
                    <input
                      type="radio"
                      name="primaryBankAccount"
                      checked={!!acct.isPrimary}
                      onChange={() => setPrimaryBankAccount(index)}
                    />
                  </label>
                </div>
              </div>
            ))}
            <button type="button" className="btn btn-ghost" onClick={addBankAccount}>
              Add another bank account
            </button>
          </fieldset>
          <label>
            CIPC document (PDF/JPG/PNG/WEBP)
            <input name="cipcDocument" type="file" accept=".pdf,image/jpeg,image/png,image/webp" required />
          </label>
          <label>
            Bank confirmation letter (PDF/JPG/PNG/WEBP)
            <input name="bankConfirmationDocument" type="file" accept=".pdf,image/jpeg,image/png,image/webp" required />
          </label>

          <div className="inline-callout notice">
            <h4>PayFast fees on payouts</h4>
            {payfastFeesDoc?.body ? renderPlainTextBody(payfastFeesDoc.body) : (
              <p className="form-hint">
                PayFast charges transaction fees and payout/withdrawal fees. Churches are responsible for PayFast fees.
                See: <a href="https://payfast.io/fees" target="_blank" rel="noreferrer">https://payfast.io/fees</a>
              </p>
            )}
          </div>

          <label className="terms-check">
            <input
              type="checkbox"
              checked={acceptTerms}
              onChange={(event) => setAcceptTerms(!!event.target.checked)}
              required
            />
            <span>
              I agree to the{" "}
              <a href="/legal/terms" target="_blank" rel="noreferrer">Terms and Conditions</a>.
            </span>
          </label>
          <label className="terms-check">
            <input
              type="checkbox"
              checked={acceptCookies}
              onChange={(event) => setAcceptCookies(!!event.target.checked)}
              required
            />
            <span>
              I agree to cookie consent and the{" "}
              <a href="/legal/privacy" target="_blank" rel="noreferrer">Privacy Policy</a>.
            </span>
          </label>
          <p className="form-hint">
            All onboarding requests are reviewed as <strong>pending</strong> before approval. Your admin login is activated only
            after approval and verified email confirmation.
          </p>
          <button type="submit" className="btn btn-primary" disabled={submitting || joinCodeLoading}>
            {submitting ? "Submitting..." : "Submit onboarding"}
          </button>
          {status.message ? <p className={`form-status ${status.type}`}>{status.message}</p> : null}
          {status.requestId ? <p className="form-hint">Request ID: <code>{status.requestId}</code></p> : null}

          {verificationContext.requestId ? (
            <div className="verification-box">
              <h4>Verify admin email</h4>
              <p className="form-hint">
                Enter the code sent to <strong>{verificationContext.email || "your admin email"}</strong>.
              </p>
              {verificationContext.expiresAt ? (
                <p className="form-hint">Code expires at: {new Date(verificationContext.expiresAt).toLocaleString()}</p>
              ) : null}
              <div className="verification-form">
                <label>
                  Email verification code
                  <input
                    type="text"
                    inputMode="numeric"
                    autoComplete="one-time-code"
                    placeholder="6-digit code"
                    value={verifyCode}
                    onChange={(event) => setVerifyCode(event.target.value)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter") {
                        event.preventDefault();
                        onVerifyCode();
                      }
                    }}
                  />
                </label>
                <div className="verification-actions">
                  <button
                    type="button"
                    className="btn btn-primary"
                    disabled={verifying || !verifyCode.trim()}
                    onClick={onVerifyCode}
                  >
                    {verifying ? "Verifying..." : "Verify email"}
                  </button>
                  <button
                    type="button"
                    className="btn btn-ghost"
                    onClick={onResendVerification}
                    disabled={resendingVerification || verifying}
                  >
                    {resendingVerification ? "Sending..." : "Resend code"}
                  </button>
                </div>
              </div>
              {verifyStatus.message ? <p className={`form-status ${verifyStatus.type}`}>{verifyStatus.message}</p> : null}
            </div>
          ) : null}
        </form>

        <article className="card">
          <h3>Verification status</h3>
          <p>Track your request with the ID returned after submission.</p>
          <form className="form" onSubmit={onTrackStatus}>
            <label>
              Onboarding request ID
              <input
                name="requestId"
                type="text"
                value={trackId}
                onChange={(event) => setTrackId(event.target.value)}
                placeholder="e.g. 2c8c3d02-..."
              />
            </label>
            <button type="submit" className="btn btn-ghost" disabled={tracking}>
              {tracking ? "Checking..." : "Check status"}
            </button>
          </form>
          {trackError ? <p className="form-status error">{trackError}</p> : null}
          {trackedRequest ? (
            <div className="status-block">
              <p className={`status-badge ${verificationStatus || "pending"}`}>{trackedRequest.verificationStatus}</p>
              <div className="status-list">
                <p><strong>Church:</strong> {trackedRequest.churchName}</p>
                <p><strong>Admin:</strong> {trackedRequest.adminFullName}</p>
                <p><strong>Admin email verified:</strong> {trackedRequest.adminEmailVerified ? "Yes" : "Pending"}</p>
                <p><strong>CIPC document:</strong> {trackedRequest.cipcFilename || "Missing"}</p>
                <p><strong>Bank confirmation:</strong> {trackedRequest.bankConfirmationFilename || "Missing"}</p>
                {trackedRequest.adminEmailVerificationSentAt ? (
                  <p><strong>Verification sent:</strong> {new Date(trackedRequest.adminEmailVerificationSentAt).toLocaleString()}</p>
                ) : null}
                <p><strong>Verified at:</strong> {trackedRequest.verifiedAt ? new Date(trackedRequest.verifiedAt).toLocaleString() : "Pending review"}</p>
                {trackedRequest.verificationNote ? <p><strong>Note:</strong> {trackedRequest.verificationNote}</p> : null}
              </div>
              {verificationStatus === "rejected" && trackedRequest.verificationNote ? (
                <p className="form-status error"><strong>Rejection reason:</strong> {trackedRequest.verificationNote}</p>
              ) : null}
            </div>
          ) : null}
          <p className="form-hint">Need assistance? Use <a href={WHATSAPP_LINK} target="_blank" rel="noreferrer">WhatsApp support ({WHATSAPP_NUMBER_DISPLAY})</a>.</p>
        </article>
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

function JobsPage() {
  const [jobs, setJobs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [meta, setMeta] = useState({ count: 0, returned: 0 });
  const [searchDraft, setSearchDraft] = useState("");
  const [search, setSearch] = useState("");
  const [employmentType, setEmploymentType] = useState("");

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setError("");

    const params = new URLSearchParams();
    params.set("limit", "30");
    if (search) params.set("search", search);
    if (employmentType) params.set("employmentType", employmentType);

    fetch(`${API_BASE}/api/public/jobs?${params.toString()}`, { cache: "no-store" })
      .then(async (response) => {
        const raw = await response.text();
        const json = parseJsonSafe(raw);
        if (!response.ok) throw new Error(json?.error || `Could not load jobs (HTTP ${response.status})`);
        if (!alive) return;
        setJobs(Array.isArray(json?.jobs) ? json.jobs : []);
        setMeta(json?.meta || { count: 0, returned: 0 });
      })
      .catch((err) => {
        if (!alive) return;
        setJobs([]);
        setMeta({ count: 0, returned: 0 });
        setError(err?.message || "Could not load jobs");
      })
      .finally(() => {
        if (alive) setLoading(false);
      });

    return () => {
      alive = false;
    };
  }, [search, employmentType]);

  function onApplyFilter(event) {
    event.preventDefault();
    setSearch(searchDraft.trim());
  }

  return (
    <section className="section page jobs-page">
      <div className="jobs-page-head">
        <h1>Jobs</h1>
        <p className="lead">Church and platform opportunities across South Africa.</p>
      </div>

      <form className="card jobs-filter-form" onSubmit={onApplyFilter}>
        <label>
          Search
          <input
            type="text"
            value={searchDraft}
            onChange={(event) => setSearchDraft(event.target.value)}
            placeholder="Role, church, location, department"
          />
        </label>
        <label>
          Employment type
          <select value={employmentType} onChange={(event) => setEmploymentType(event.target.value)}>
            <option value="">All types</option>
            <option value="FULL_TIME">Full-time</option>
            <option value="PART_TIME">Part-time</option>
            <option value="CONTRACT">Contract</option>
            <option value="INTERNSHIP">Internship</option>
            <option value="VOLUNTEER">Volunteer</option>
          </select>
        </label>
        <button type="submit" className="btn btn-primary">Apply</button>
      </form>

      <p className="jobs-meta">Showing {meta.returned || jobs.length} of {meta.count || 0}</p>

      {error ? <p className="form-status error">{error}</p> : null}
      {loading ? <p className="form-hint">Loading jobs...</p> : null}

      {!loading && !error && !jobs.length ? (
        <article className="card jobs-empty">
          <h3>No open roles right now</h3>
          <p>Check again later or contact us for partnership opportunities.</p>
        </article>
      ) : null}

      {!loading && !error && jobs.length ? (
        <div className="jobs-grid">
          {jobs.map((job) => (
            <article className="card job-card" key={job.id}>
              <p className="job-chip">{formatEmploymentType(job.employmentType)}</p>
              <h3>{job.title}</h3>
              <p className="job-subtitle">{job.churchName || "Churpay"}{job.location ? ` • ${job.location}` : ""}</p>
              {job.summary ? <p>{job.summary}</p> : null}
              <div className="job-card-meta">
                <span>Closes: {formatJobDate(job.expiresAt)}</span>
                <span>Published: {formatJobDate(job.publishedAt || job.createdAt)}</span>
              </div>
              <div className="job-card-actions">
                <Link to={`/jobs/${encodeURIComponent(job.slug)}`} className="btn btn-primary">View role</Link>
                {job.applicationUrl ? (
                  <a href={job.applicationUrl} className="btn btn-ghost" target="_blank" rel="noreferrer">Apply</a>
                ) : job.applicationEmail ? (
                  <a href={`mailto:${job.applicationEmail}`} className="btn btn-ghost">Apply by email</a>
                ) : null}
              </div>
            </article>
          ))}
        </div>
      ) : null}
    </section>
  );
}

function JobDetailPage() {
  const { slug = "" } = useParams();
  const [job, setJob] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [applyStatus, setApplyStatus] = useState({ type: "", message: "" });
  const [applying, setApplying] = useState(false);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setError("");
    setJob(null);
    setApplyStatus({ type: "", message: "" });

    fetch(`${API_BASE}/api/public/jobs/${encodeURIComponent(slug)}`, { cache: "no-store" })
      .then(async (response) => {
        const raw = await response.text();
        const json = parseJsonSafe(raw);
        if (!response.ok) throw new Error(json?.error || `Could not load job (HTTP ${response.status})`);
        if (!alive) return;
        setJob(json?.job || null);
      })
      .catch((err) => {
        if (!alive) return;
        setError(err?.message || "Could not load job details");
      })
      .finally(() => {
        if (alive) setLoading(false);
      });

    return () => {
      alive = false;
    };
  }, [slug]);

  async function onApplyOnline(event) {
    event.preventDefault();
    const formEl = event.currentTarget;
    const form = new FormData(formEl);
    const fullName = String(form.get("fullName") || "").trim();
    const email = String(form.get("email") || "").trim();
    const phone = String(form.get("phone") || "").trim();
    const message = String(form.get("message") || "").trim();
    const cvFile = form.get("cvFile");

    if (!fullName || !email) {
      setApplyStatus({ type: "error", message: "Please provide your full name and email." });
      return;
    }
    if (!(cvFile instanceof File) || !cvFile.size) {
      setApplyStatus({ type: "error", message: "Please attach your CV (PDF or DOCX)." });
      return;
    }

    setApplying(true);
    setApplyStatus({ type: "", message: "" });

    try {
      const cvPayload = await fileToPayload(cvFile, { label: "CV", maxBytes: 8 * 1024 * 1024 });
      const payload = {
        fullName,
        email,
        phone,
        message,
        cvDocument: cvPayload,
      };

      const response = await fetch(`${API_BASE}/api/public/jobs/${encodeURIComponent(slug)}/apply`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const raw = await response.text();
      const json = parseJsonSafe(raw);
      if (!response.ok) {
        throw new Error(json?.error || `Application failed (HTTP ${response.status})`);
      }

      setApplyStatus({ type: "success", message: "Application submitted. Check your email for confirmation." });
      // Don't access SyntheticEvent fields after awaits; hold DOM element ref.
      if (formEl && typeof formEl.reset === "function") formEl.reset();
    } catch (err) {
      setApplyStatus({ type: "error", message: err?.message || "Could not submit application" });
    } finally {
      setApplying(false);
    }
  }

  if (loading) {
    return (
      <section className="section page">
        <p className="form-hint">Loading job details...</p>
      </section>
    );
  }

  if (error || !job) {
    return (
      <section className="section page">
        <h1>Job not found</h1>
        <p className="lead">{error || "The role may have closed or moved."}</p>
        <Link to="/jobs" className="btn btn-primary">Back to jobs</Link>
      </section>
    );
  }

  return (
    <section className="section page job-detail-page">
      <Link to="/jobs" className="btn btn-ghost">Back to jobs</Link>
      <article className="card job-detail-card">
        <p className="job-chip">{formatEmploymentType(job.employmentType)}</p>
        <h1>{job.title}</h1>
        <p className="lead">{job.churchName || "Churpay"}{job.location ? ` • ${job.location}` : ""}</p>
        <div className="job-card-meta">
          <span>Published: {formatJobDate(job.publishedAt || job.createdAt)}</span>
          <span>Closes: {formatJobDate(job.expiresAt)}</span>
        </div>
        {job.summary ? <p className="job-detail-summary">{job.summary}</p> : null}

        <section className="job-detail-block">
          <h3>Role description</h3>
          <p className="job-pre">{job.description}</p>
        </section>

        {job.requirements ? (
          <section className="job-detail-block">
            <h3>Requirements</h3>
            <p className="job-pre">{job.requirements}</p>
          </section>
        ) : null}

        <div className="job-card-actions">
          {job.applicationUrl ? (
            <a href={job.applicationUrl} className="btn btn-primary" target="_blank" rel="noreferrer">Apply now</a>
          ) : null}
          {job.applicationEmail ? (
            <a href={`mailto:${job.applicationEmail}`} className="btn btn-ghost">Apply by email</a>
          ) : null}
        </div>
      </article>

      <article className="card job-apply-card">
        <h3>Apply online</h3>
        <p className="form-hint">Fill in your details and upload your CV. We will send it to the role owner.</p>
        <form className="form" onSubmit={onApplyOnline}>
          <label>
            Full name
            <input name="fullName" type="text" required />
          </label>
          <label>
            Email
            <input name="email" type="email" required />
          </label>
          <label>
            Phone (optional)
            <input name="phone" type="text" />
          </label>
          <label>
            Message (optional)
            <textarea name="message" rows={4} />
          </label>
          <label>
            CV (PDF or DOCX)
            <input name="cvFile" type="file" accept=".pdf,.doc,.docx,application/pdf,application/msword,application/vnd.openxmlformats-officedocument.wordprocessingml.document" required />
          </label>
          <button type="submit" className="btn btn-primary" disabled={applying}>
            {applying ? "Submitting..." : "Submit application"}
          </button>
          {applyStatus.message ? <p className={`form-status ${applyStatus.type}`}>{applyStatus.message}</p> : null}
        </form>
      </article>
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
        <div className="table-wrap">
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
        </div>
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
      <div className="grid-2">
        <article className="card">
          <p>
            Churpay is purpose-built for churches in South Africa. We focus on practical tools that help ministry teams run
            reliable giving operations while keeping member experience clear and fast.
          </p>
        </article>

        <article className="card app-download-card">
          <h3>Get the Churpay app</h3>
          <p>Available for iPhone and Android. Download and start giving in minutes.</p>
          <div className="store-buttons">
            <ExternalCta href={IOS_APP_LINK} className="store-btn">
              <span className="store-icon" aria-hidden="true">iOS</span>
              <span>
                <strong>Download on the</strong>
                <small>App Store</small>
              </span>
            </ExternalCta>
            <ExternalCta href={ANDROID_APP_LINK} className="store-btn">
              <span className="store-icon" aria-hidden="true">▶</span>
              <span>
                <strong>Get it on</strong>
                <small>Google Play</small>
              </span>
            </ExternalCta>
          </div>
        </article>
      </div>
    </section>
  );
}

function BookDemoPage() {
  const [status, setStatus] = useState({ type: "", message: "" });
  const [submitting, setSubmitting] = useState(false);
  const timezone = useMemo(() => {
    try {
      return Intl.DateTimeFormat().resolvedOptions().timeZone || "";
    } catch (_err) {
      return "";
    }
  }, []);

  async function onSubmit(event) {
    event.preventDefault();
    const formEl = event.currentTarget;
    const form = new FormData(formEl);
    const payload = {
      fullName: String(form.get("fullName") || "").trim(),
      churchName: String(form.get("churchName") || "").trim(),
      email: String(form.get("email") || "").trim(),
      phone: String(form.get("phone") || "").trim(),
      preferredDate: String(form.get("preferredDate") || "").trim(),
      preferredTime: String(form.get("preferredTime") || "").trim(),
      meetingType: String(form.get("meetingType") || "").trim(),
      notes: String(form.get("notes") || "").trim(),
      timezone,
    };

    if (!payload.fullName || !payload.churchName || !payload.email || !payload.preferredDate || !payload.preferredTime) {
      setStatus({ type: "error", message: "Please complete name, church, email, and your preferred date/time." });
      return;
    }

    setSubmitting(true);
    setStatus({ type: "", message: "" });

    try {
      const response = await fetch(`${API_BASE}/api/public/book-demo`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const raw = await response.text();
      const json = parseJsonSafe(raw);
      if (!response.ok) {
        throw new Error(json?.error || `Could not submit booking (HTTP ${response.status})`);
      }

      setStatus({ type: "success", message: "Booking request received. We will contact you to confirm the meeting." });
      // Don't access SyntheticEvent fields after awaits; hold DOM element ref.
      if (formEl && typeof formEl.reset === "function") formEl.reset();
    } catch (err) {
      setStatus({ type: "error", message: err.message || "Booking request failed. Use WhatsApp below." });
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <section className="section page">
      <h1>Book a demo</h1>
      <p className="lead">Choose a time and we will confirm the meeting with your church team.</p>
      <div className="grid-2">
        <form className="card form" onSubmit={onSubmit}>
          <label>
            Full name
            <input name="fullName" type="text" required />
          </label>
          <label>
            Church name
            <input name="churchName" type="text" required />
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
            Preferred date
            <input name="preferredDate" type="date" required />
          </label>
          <label>
            Preferred time
            <input name="preferredTime" type="time" required />
          </label>
          <label>
            Meeting type
            <select name="meetingType" defaultValue="Google Meet">
              <option value="Google Meet">Google Meet</option>
              <option value="Phone call">Phone call</option>
              <option value="WhatsApp call">WhatsApp call</option>
            </select>
          </label>
          <label>
            Notes (optional)
            <textarea name="notes" rows={4} />
          </label>
          <button type="submit" className="btn btn-primary" disabled={submitting}>
            {submitting ? "Submitting..." : "Request booking"}
          </button>
          {status.message ? <p className={`form-status ${status.type}`}>{status.message}</p> : null}
          {timezone ? <p className="form-hint">Your timezone: {timezone}</p> : null}
        </form>

        <article className="card notice">
          <h3>Prefer WhatsApp?</h3>
          <p>
            You can also message us directly on WhatsApp and we will schedule the demo with you.
          </p>
          <p>
            <a className="btn btn-ghost" href={WHATSAPP_LINK} target="_blank" rel="noreferrer">WhatsApp support</a>
          </p>
        </article>
      </div>
    </section>
  );
}

function ContactPage() {
  const [status, setStatus] = useState({ type: "", message: "" });
  const [submitting, setSubmitting] = useState(false);

  async function onSubmit(event) {
    event.preventDefault();
    const formEl = event.currentTarget;
    const form = new FormData(formEl);
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
      // Don't access SyntheticEvent fields after awaits; hold DOM element ref.
      if (formEl && typeof formEl.reset === "function") formEl.reset();
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
          <p>WhatsApp: <a href={WHATSAPP_LINK} target="_blank" rel="noreferrer">{WHATSAPP_NUMBER_DISPLAY}</a></p>
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

function DeleteAccountPage() {
  return (
    <LegalPage title="Churpay Account Deletion Request">
      <p>
        Users may request deletion of their Churpay account and associated personal data by emailing:
      </p>
      <p>
        <a href="mailto:support@churpay.co.za">support@churpay.co.za</a>
      </p>
      <p>Include:</p>
      <ul>
        <li>Full name</li>
        <li>Registered email address</li>
        <li>Phone number (if applicable)</li>
      </ul>
      <p>What will be deleted:</p>
      <ul>
        <li>Profile information</li>
        <li>Login credentials</li>
        <li>Donation history visibility</li>
        <li>Linked devices</li>
      </ul>
      <p>What may be retained:</p>
      <ul>
        <li>
          Transaction records required for financial compliance and auditing purposes (retained according to
          South African financial regulations)
        </li>
      </ul>
      <p>Deletion requests are processed within 7-14 business days.</p>
    </LegalPage>
  );
}

function LegalDocumentPage({ docKey, fallbackTitle, fallbackBody }) {
  const [loading, setLoading] = useState(true);
  const [doc, setDoc] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setError("");
    setDoc(null);

    fetchLegalDocument(docKey)
      .then((data) => {
        if (!alive) return;
        setDoc(data || null);
      })
      .catch((err) => {
        if (!alive) return;
        setError(err?.message || "Could not load document");
        setDoc(null);
      })
      .finally(() => {
        if (alive) setLoading(false);
      });

    return () => {
      alive = false;
    };
  }, [docKey]);

  return (
    <LegalPage title={doc?.title || fallbackTitle}>
      {loading ? <p className="form-hint">Loading…</p> : null}
      {!loading && doc?.body ? renderPlainTextBody(doc.body) : null}
      {!loading && !doc?.body ? fallbackBody : null}
      {!loading && error && !doc?.body ? <p className="form-hint">{error}</p> : null}
    </LegalPage>
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
        <Route path="/jobs" element={<JobsPage />} />
        <Route path="/jobs/:slug" element={<JobDetailPage />} />
        <Route path="/book-demo" element={<BookDemoPage />} />
        <Route path="/onboarding" element={<ChurchOnboardingPage />} />
        <Route path="/pricing" element={<PricingPage />} />
        <Route path="/security" element={<SecurityPage />} />
        <Route path="/about" element={<AboutPage />} />
        <Route path="/contact" element={<ContactPage />} />
        <Route
          path="/legal/terms"
          element={(
            <LegalDocumentPage
              docKey="terms"
              fallbackTitle="Terms and Conditions"
              fallbackBody={(
                <>
                  <p>Churpay provides payment facilitation and giving operations tooling for churches.</p>
                  <p>By using this service, users agree to lawful use, accurate account information, and payment provider terms.</p>
                  <p>Platform terms may be updated as product, legal, or regulatory requirements change.</p>
                </>
              )}
            />
          )}
        />
        <Route
          path="/legal/privacy"
          element={(
            <LegalDocumentPage
              docKey="privacy"
              fallbackTitle="Privacy Policy"
              fallbackBody={(
                <>
                  <p>Churpay processes personal and transaction data only for giving operations, reconciliation, support, and compliance.</p>
                  <p>Data is protected using role-based access, encrypted transport, and operational controls.</p>
                  <p>Website cookie preferences can be managed from the cookie consent banner.</p>
                  <p>For privacy requests, contact hello@churpay.com.</p>
                </>
              )}
            />
          )}
        />
        <Route path="/delete-account" element={<DeleteAccountPage />} />
        <Route path="/status" element={<StatusPage />} />
        <Route path="*" element={<NotFoundPage />} />
      </Routes>
    </Layout>
  );
}
