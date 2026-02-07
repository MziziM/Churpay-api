# QA Checklist

## Functional
- [ ] All routes render correctly on desktop and mobile.
- [ ] Contact form submits successfully and returns success state.
- [ ] Contact form handles failures and WhatsApp fallback remains available.
- [ ] `/status` shows API healthy when `/health` returns `{ "ok": true }`.
- [ ] Pricing page formula and examples are accurate:
  - fee = R2.50 + 0.75% of amount
  - total charged = amount + fee

## Content
- [ ] Processing fee wording is used everywhere (not donation wording).
- [ ] Church and member journeys are clear.
- [ ] Legal pages exist and are linked.

## Visual
- [ ] Logo appears in header and footer.
- [ ] Branding colors and spacing are consistent.
- [ ] No layout breakage at mobile sizes.

## Technical
- [ ] No console errors on key pages.
- [ ] Sitemap and robots are accessible.
- [ ] Meta tags and OpenGraph values present.
- [ ] Lighthouse target >= 90 for Performance/Best Practices/SEO on Home.
