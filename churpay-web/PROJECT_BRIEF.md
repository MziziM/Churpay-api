# Churpay Public Website Plan

## Hosting recommendation
- **Recommended:** Vercel for `churpay.com` + keep API on droplet at `api.churpay.com`.
  - Why: simpler SSL/CDN, preview deployments, instant rollback, lower operational risk for marketing changes.
- **Alternative:** same droplet static hosting with Nginx.
  - Why: single infra control and no extra platform dependency.

## Sitemap
- `/` Home
- `/churches` For Churches
- `/members` For Members
- `/pricing` Pricing (processing fee model)
- `/security` Security & Trust
- `/about` About
- `/contact` Contact
- `/legal/terms` Terms
- `/legal/privacy` Privacy
- `/status` API status

## Wireframes (structure)

### Home
- Top nav + logo + CTAs
- Hero (headline, value proposition, two CTAs)
- 3 feature cards
- Trust section + chips
- Footer

### Churches
- Heading + lead
- 4 step cards
- CTA strip

### Members
- Heading + lead
- Giving flow cards
- Processing fee disclosure card

### Pricing
- Formula card
- Example table

### Security
- 4 trust cards

### Contact
- Form card
- Direct channels card (email/WhatsApp/admin links)

### Status
- Health endpoint + last check result

## UI direction
- Reuse Churpay brand colors and token feel:
  - Navy `#1E3A5F`
  - Teal `#0EA5B7`
  - Clean light background for public marketing pages
- Rounded cards, soft shadows, generous spacing.
- Strong typography hierarchy (hero > section > body).

## Build sequence
1. Scaffold Vite React app.
2. Implement shared layout and all required pages.
3. Add copy with SA-friendly wording and processing fee transparency.
4. Add SEO and crawler files.
5. Add contact form integration to API endpoint.
6. Add status page to read `/health`.
7. Prepare deployment guides and QA checklist.
