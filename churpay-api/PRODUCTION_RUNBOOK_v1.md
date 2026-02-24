# Churpay Production Runbook v1.6 (Growth + Church Life Phases 1-6)

Last updated: February 23, 2026

## 1) Overview

### Production architecture
- Host: DigitalOcean Droplet (Ubuntu 24.04 LTS)
- Runtime: Node.js 20
- Process manager: PM2 (`churpay-api`)
- Reverse proxy: Nginx
- TLS: Let's Encrypt (Certbot)
- Database: DigitalOcean Managed PostgreSQL
- Payments: PayFast live ITN

### Public URLs
- API: `https://api.churpay.com`
- Public website: `https://churpay.com`
- Admin portal: `https://api.churpay.com/admin/`
- Super admin portal: `https://api.churpay.com/super/`

### Internal ports
- `80` and `443`: Nginx
- `8080`: Node/Express upstream behind Nginx

### Canonical payment/webhook routes
- Health: `GET /health`
- PayFast ITN canonical endpoint: `POST /webhooks/payfast/itn`
- PayFast return bridge: `GET /api/payfast/return`
- PayFast cancel bridge: `GET /api/payfast/cancel`
- QR/public visitor give page: `GET /g/:joinCode`

### Auth routes
- Member login: `POST /api/auth/login/member`
- Admin login: `POST /api/auth/login/admin`
- Admin login 2FA verify: `POST /api/auth/login/admin/verify-2fa`
- Super login canonical: `POST /api/super/login`
- Super login canonical 2FA verify: `POST /api/super/login/verify-2fa`
- Super login compatibility alias: `POST /api/auth/login/super` (deprecated, do not remove yet)
- Super login compatibility 2FA verify: `POST /api/auth/login/super/verify-2fa`

### Church onboarding state (updated February 18, 2026)
- Public onboarding requires church/admin details, terms + cookie consent, and a CIPC document.
- Public onboarding no longer requires bank-account fields or bank-confirmation upload in active web/super onboarding UI.
- PayFast activation during onboarding is optional via `Activate payments (Powered by PayFast)`.
- Backward compatibility: API still writes legacy bank-confirmation columns using the CIPC file until schema cleanup migration removes/relaxes those columns.

### Current production state (updated February 23, 2026)
- Phase 1: complete.
- Phase 2: complete.
- Phase 3: complete (foundation scope shipped).
- Phase 4: complete (small groups meetings/attendance).
- Phase 5: complete (events registration/cancel + poster support).
- Phase 6: complete (volunteer governance + family tree + mobile polish).
- Key migrations confirmed on production:
  - `20260221_0100__church_subscriptions_hardening.sql`
  - `20260221_0700__church_donors_external_giving.sql`
  - `20260221_0800__volunteer_governance_foundation.sql`
  - `20260221_0900__church_event_registrations.sql`
  - `20260221_1000__church_group_meetings_attendance.sql`
  - `20260223_1100__church_family_relationships.sql`
- Family + household CRM linking is live:
  - household member-candidate lookup
  - family relationship graph endpoints
  - address-share support from parent/member profile link flow
- Mobile Church Life polish shipped:
  - Family Tree grouped lanes (spouse/parents/children/siblings/extended)
  - non-critical Church Life modules now fail soft (no full-screen global crash from one failing endpoint)
- Login routing rule (mandatory):
  - `member` role uses `POST /api/auth/login/member`
  - `volunteer`, `pastor`, `admin`, `super` use `POST /api/auth/login/admin` (+ OTP challenge verify when returned)
- Operational guardrail:
  - Prefer SSH + `tmux` for all production operations. DigitalOcean web console can disconnect during long/interactive tasks.
- Account-holder recommendation for church onboarding:
  - Use a stable church-owned email and a pastor/admin role as the church account holder; avoid personal throwaway addresses for long-term ownership continuity.

### Canonical deploy from Mac (mandatory path)
- Always deploy via `/Users/mzizimzwakhe/Documents/Churpay-demo/churpay-api/scripts/deploy-prod-from-mac.sh`.
- This script prevents drift by syncing the full API tree (including new migrations/scripts), then running:
  - `npm run migrate`
  - `npm run -s migrate:drift`
  - PM2 restart + local/public health checks
- Run from your Mac:
```bash
cd /Users/mzizimzwakhe/Documents/Churpay-demo/churpay-api
bash scripts/deploy-prod-from-mac.sh
```
- Optional safety flags:
```bash
# Preview sync only
DRY_RUN=1 bash scripts/deploy-prod-from-mac.sh

# Fail if local git tree is dirty
REQUIRE_CLEAN_GIT=1 bash scripts/deploy-prod-from-mac.sh
```

## 2) Environment Variables

### Env file path
- `/etc/churpay/churpay-api.env`

### Required (production)
- `NODE_ENV=production`
- `PORT=8080`
- `DATABASE_URL=...`
- `DATABASE_CA_CERT=-----BEGIN CERTIFICATE-----...` (recommended for explicit CA pinning)
- `JWT_SECRET=...`
- `PUBLIC_BASE_URL=https://api.churpay.com`
- `PUBLIC_WEB_BASE_URL=https://churpay.com`
- `CORS_ORIGINS=https://churpay.com,https://www.churpay.com`
- `TRUST_PROXY=true`
- `TRUST_PROXY_HOPS=1`
- `PAYFAST_MODE=live`
- `PAYFAST_CREDENTIAL_ENCRYPTION_KEY=...` (32-byte+ secret for encrypting church PayFast keys at rest)
- `PAYFAST_RETURN_URL=https://api.churpay.com/api/payfast/return`
- `PAYFAST_CANCEL_URL=https://api.churpay.com/api/payfast/cancel`
- `PAYFAST_NOTIFY_URL=https://api.churpay.com/webhooks/payfast/itn`
- `APP_DEEP_LINK_BASE=churpaydemo://give`
- `SUPER_ROUTES_ENABLED=true`
- `SUPER_ADMIN_EMAIL=...`
- `SUPER_ADMIN_PASSWORD=...`

### Required fee config
- `PLATFORM_FEE_FIXED=2.50`
- `PLATFORM_FEE_PCT=0.0075`
- `SUPERADMIN_CUT_PCT=1.0`

### Optional
- `PAYFAST_PASSPHRASE=`
- `PAYFAST_DEBUG=0`
- `PAYFAST_MERCHANT_ID=...` (legacy global fallback only)
- `PAYFAST_MERCHANT_KEY=...` (legacy global fallback only)
- `PAYFAST_GLOBAL_FALLBACK_ENABLED=false`
- `PAYFAST_APP_FALLBACK_URL=https://www.churpay.com`
- `ADMIN_LOGIN_2FA_ENABLED=true`
- `SUPER_LOGIN_2FA_ENABLED=true`
- `LOGIN_2FA_TTL_MINUTES=15`
- `LOGIN_2FA_MAX_ATTEMPTS=5`
- `PUSH_PROVIDER=log` (set to `expo` to send real push notifications)
- `EXPO_ACCESS_TOKEN=` (optional, recommended when using Expo push)
- `NOTIFICATION_JOBS_ENABLED=false` (set to `true` to run birthday + cash reminder jobs)
- `NOTIFICATION_JOBS_INTERVAL_MS=900000`
- `NOTIFICATION_TIMEZONE=Africa/Johannesburg`
- `BIRTHDAY_NOTIFICATIONS_ENABLED=true`
- `CASH_SATURDAY_REMINDER_ENABLED=false`
- `CASH_SATURDAY_REMINDER_HOUR_LOCAL=19`
- `RATE_LIMIT_WINDOW_MS=900000`
- `RATE_LIMIT_MAX=300`
- `AUTH_RATE_LIMIT_WINDOW_MS=900000`
- `AUTH_RATE_LIMIT_MAX=30`
- `DEMO_MODE=false`

### Security notes
- In production, app boot fails if `JWT_SECRET` is missing.
- In production, app boot fails if CORS allowlist is empty.
- In production, app boot fails if super routes are enabled but super credentials are missing.
- In production, DB boot fails if `PGSSLINSECURE=true`.
- In production, DB uses strict TLS verification; if `DATABASE_CA_CERT` is missing it uses the system CA trust store.

### Credential handling (mandatory)
- Store final production secrets in a password manager/vault (do not keep only in shell history).
- Minimum secrets to store immediately after rotation:
  - `SUPER_ADMIN_EMAIL`
  - `SUPER_ADMIN_PASSWORD`
  - `JWT_SECRET`
  - `DATABASE_URL`
  - `PAYFAST_CREDENTIAL_ENCRYPTION_KEY`
  - `PAYFAST_MERCHANT_ID`
  - `PAYFAST_MERCHANT_KEY`
- After rotating `SUPER_ADMIN_PASSWORD`, validate both endpoints:
  - `POST /api/super/login`
  - `POST /api/auth/login/super`
- Redact secrets from screenshots, tickets, and chat logs.

### Super admin password rotation (safe procedure)
Run on droplet:
```bash
set -euo pipefail
ENV=/etc/churpay/churpay-api.env

# 1) Backup env before change
cp -a "$ENV" "/root/churpay-api.env.$(date -u +%Y%m%d_%H%M%S).bak"

# 2) Rotate password
NEW_SUPER_PASSWORD="$(LC_ALL=C tr -dc 'A-Za-z0-9@#%_+=.-' </dev/urandom | head -c 40)"
if grep -q '^SUPER_ADMIN_PASSWORD=' "$ENV"; then
  sed -i "s|^SUPER_ADMIN_PASSWORD=.*|SUPER_ADMIN_PASSWORD=${NEW_SUPER_PASSWORD}|" "$ENV"
else
  echo "SUPER_ADMIN_PASSWORD=${NEW_SUPER_PASSWORD}" >> "$ENV"
fi

# 3) Restart with updated env
pm2 restart /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save

# 4) Lock backup permissions
chmod 600 /root/churpay-api.env.*.bak

echo "Store NEW_SUPER_PASSWORD in vault/password manager immediately."
```

Validate canonical + compatibility auth routes:
```bash
set -a; source /etc/churpay/churpay-api.env; set +a

curl -sS -X POST https://api.churpay.com/api/super/login \
  -H "Content-Type: application/json" \
  -d "$(jq -nc --arg identifier "$SUPER_ADMIN_EMAIL" --arg password "$SUPER_ADMIN_PASSWORD" '{identifier:$identifier,password:$password}')" | jq

curl -sS -X POST https://api.churpay.com/api/auth/login/super \
  -H "Content-Type: application/json" \
  -d "$(jq -nc --arg identifier "$SUPER_ADMIN_EMAIL" --arg password "$SUPER_ADMIN_PASSWORD" '{identifier:$identifier,password:$password}')" | jq
```

### Env backup policy
- Keep timestamped backups of `/etc/churpay/churpay-api.env` before and after high-risk changes.
- Use:
```bash
cp -a /etc/churpay/churpay-api.env "/root/churpay-api.env.$(date -u +%Y%m%d_%H%M%S).bak"
chmod 600 /root/churpay-api.env.*.bak
```
- Restore (if needed):
```bash
cp -a /root/churpay-api.env.YYYYMMDD_HHMMSS.bak /etc/churpay/churpay-api.env
pm2 restart /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save
```

## 3) First-Time Production Setup

Run on droplet:

```bash
sudo apt update
sudo apt install -y nginx certbot python3-certbot-nginx curl git jq
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
sudo npm i -g pm2
```

```bash
sudo mkdir -p /var/www/churpay
sudo chown -R "$USER":"$USER" /var/www/churpay
cd /var/www/churpay
git clone YOUR_REPO_URL repo
cd /var/www/churpay/repo/churpay-api
npm ci --omit=dev
```

```bash
sudo mkdir -p /etc/churpay
sudo cp .env.example /etc/churpay/churpay-api.env
sudo chown root:root /etc/churpay/churpay-api.env
sudo chmod 600 /etc/churpay/churpay-api.env
sudo nano /etc/churpay/churpay-api.env
```

Run migrations before first start:

```bash
cd /var/www/churpay/repo/churpay-api
npm run migrate
```

Start PM2:

```bash
pm2 start /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save
```

Install Nginx site and TLS:

```bash
sudo cp /var/www/churpay/repo/churpay-api/deploy/nginx.churpay-api.conf /etc/nginx/sites-available/churpay-api
sudo ln -sf /etc/nginx/sites-available/churpay-api /etc/nginx/sites-enabled/churpay-api
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl restart nginx
sudo certbot --nginx -d api.churpay.com -m YOUR_EMAIL_ADDRESS --agree-tos --no-eff-email
sudo systemctl reload nginx
```

## 4) Standard Deploy / Update Steps

```bash
cd /var/www/churpay/repo
git fetch --all --tags
git checkout main
git pull --ff-only

cd /var/www/churpay/repo/churpay-api
if ! npm ci --omit=dev; then
  # Fallback when package-lock drift exists on server copy.
  npm install --omit=dev
fi
# Migration script auto-loads /etc/churpay/churpay-api.env (or CHURPAY_ENV_FILE if set).
npm run migrate

pm2 restart /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save

sudo nginx -t
sudo systemctl reload nginx
```

### Exact server deploy/restart + curl (authoritative)
Use this exact sequence for backend rollout on production:

```bash
ssh -o ServerAliveInterval=20 -o ServerAliveCountMax=6 root@178.62.81.206
tmux new -As churpay
cd /var/www/churpay/repo/churpay-api
set -euo pipefail

npm install --omit=dev
npm run migrate
pm2 restart deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save

for i in {1..90}; do
  curl -fsS http://127.0.0.1:8080/health >/dev/null && echo "up in ${i}s" && break
  sleep 1
done

curl -i -sS https://api.churpay.com/health | sed -n '1,20p'
```

If your terminal is unstable, run deploy detached and follow log output:

```bash
cd /var/www/churpay/repo/churpay-api

nohup bash -lc '
  cd /var/www/churpay/repo/churpay-api
  SMOKE_ENFORCE=0 \
  BASE_URL=https://api.churpay.com \
  API_IP=178.62.81.206 \
  UPSTREAM_WAIT_RETRIES=90 \
  PUBLIC_WAIT_RETRIES=90 \
  bash scripts/deploy-prod.sh
' >/tmp/churpay-deploy.log 2>&1 < /dev/null &

echo "DEPLOY_PID=$!"
tail -n 200 /tmp/churpay-deploy.log
grep -E "^\[deploy\]|^\[fail\]" /tmp/churpay-deploy.log | tail -n 120
```

### Static UI deploy (portal/web only)
Use this when only frontend static files changed (`churpay-api/public/*` and/or `churpay-web/dist/*`).

From Mac (project workspace):
```bash
cd /Users/mzizimzwakhe/Documents/Churpay-demo

# Admin/Super portal static files
rsync -avz churpay-api/public/admin/index.html root@178.62.81.206:/var/www/churpay/repo/churpay-api/public/admin/index.html
rsync -avz churpay-api/public/admin/styles.css root@178.62.81.206:/var/www/churpay/repo/churpay-api/public/admin/styles.css
rsync -avz churpay-api/public/admin/app.js root@178.62.81.206:/var/www/churpay/repo/churpay-api/public/admin/app.js
rsync -avz churpay-api/public/super/index.html root@178.62.81.206:/var/www/churpay/repo/churpay-api/public/super/index.html
rsync -avz churpay-api/public/super/login/index.html root@178.62.81.206:/var/www/churpay/repo/churpay-api/public/super/login/index.html
rsync -avz churpay-api/public/super/app.js root@178.62.81.206:/var/www/churpay/repo/churpay-api/public/super/app.js

# Shared brand assets (logo/favicon source)
rsync -avz churpay-api/public/assets/brand/churpay-logo.svg root@178.62.81.206:/var/www/churpay/repo/churpay-api/public/assets/brand/churpay-logo.svg
rsync -avz churpay-api/public/assets/brand/churpay-logo-bold.svg root@178.62.81.206:/var/www/churpay/repo/churpay-api/public/assets/brand/churpay-logo-bold.svg

# Public website build output
rsync -avz --delete churpay-web/dist/ root@178.62.81.206:/var/www/churpay/repo/churpay-web/dist/
```

On droplet:
```bash
pm2 restart /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save
nginx -t && systemctl reload nginx
```

### Full deploy from Mac (when server repo is not a git clone)
Use this flow if `/var/www/churpay/repo` is missing `.git` or droplet console is unstable.

From Mac:
```bash
cd /Users/mzizimzwakhe/Documents/Churpay-demo

# 1) Sync API source to server
rsync -az --delete \
  --exclude '.git' \
  --exclude 'node_modules' \
  /Users/mzizimzwakhe/Documents/Churpay-demo/churpay-api/ \
  root@178.62.81.206:/var/www/churpay/repo/churpay-api/

# 2) Build web locally and sync static output
npm --prefix /Users/mzizimzwakhe/Documents/Churpay-demo/churpay-web run build
rsync -az --delete \
  /Users/mzizimzwakhe/Documents/Churpay-demo/churpay-web/dist/ \
  root@178.62.81.206:/var/www/churpay/repo/churpay-web/dist/
```

On droplet:
```bash
set -euo pipefail
API=/var/www/churpay/repo/churpay-api
APP_CFG=/var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs

cd "$API"
npm install --omit=dev
npm run migrate
pm2 restart "$APP_CFG" --only churpay-api --update-env
pm2 save

# Wait until Node upstream is healthy before testing public URL.
for i in {1..20}; do
  if curl -fsS http://127.0.0.1:8080/health >/dev/null; then
    echo "upstream ok"
    break
  fi
  sleep 1
done

nginx -t && systemctl reload nginx
curl -i https://api.churpay.com/health
```

Quick verify:
```bash
# Confirm website root path
sudo nginx -T 2>/dev/null | sed -n '/server_name churpay.com/,+80p' | grep -E 'server_name|root '

# Confirm new cache-busted assets are referenced
curl -sS https://api.churpay.com/admin/ | grep -Eo 'styles.css\\?v=[^"]+|app.js\\?v=[^"]+|churpay-logo[^"]+\\.svg\\?v=[^"]+' | head
curl -sS https://api.churpay.com/super/login/ | grep -Eo 'styles.css\\?v=[^"]+|churpay-logo[^"]+\\.svg\\?v=[^"]+' | head
curl -sS https://churpay.com/ | grep -Eo '/assets/index-[^"]+\\.js|/assets/index-[^"]+\\.css'
```

### Static cache-busting rule (mandatory)
- When changing portal/web static files, bump query versions in HTML (`?v=...`) for:
  - `/admin/styles.css`
  - `/admin/app.js`
  - `/super/app.js`
  - brand logo/favicons (`churpay-logo.svg`, `churpay-logo-bold.svg`)
- Use a monotonic token like `v=20260216e`.
- After deploy, do a hard refresh in browser (`Cmd+Shift+R` / `Ctrl+F5`).

### Near-zero downtime preflight
```bash
pm2 status
systemctl status nginx --no-pager
ss -ltnp | egrep ':80|:443|:8080'
curl -i https://api.churpay.com/health
bash /var/www/churpay/repo/churpay-api/scripts/ops-dns-check.sh churpay.com 178.62.81.206
```

## 5) Smoke Tests

### Automated smoke
```bash
cd /var/www/churpay/repo/churpay-api
BASE_URL=https://api.churpay.com \
API_IP=178.62.81.206 \
ADMIN_IDENTIFIER=YOUR_ADMIN_EMAIL_OR_PHONE \
ADMIN_PASSWORD=YOUR_ADMIN_PASSWORD \
ADMIN_OTP=YOUR_ADMIN_OTP_IF_REQUIRED \
MEMBER_IDENTIFIER=YOUR_MEMBER_EMAIL_OR_PHONE \
MEMBER_PASSWORD=YOUR_MEMBER_PASSWORD \
MEMBER_OTP=YOUR_MEMBER_OTP_IF_REQUIRED \
EXPECTED_ADMIN_APP_VERSION=20260220e \
EXPECTED_ADMIN_STYLE_VERSION=20260220h \
bash scripts/smoke-prod.sh
```

Pass criteria for the automated smoke script:
- `onboarding` submit + fetch succeeds (`201` then `200`).
- `auth` succeeds for admin and member.
- `giving` context + payment intent creation succeeds.
- `ITN` endpoints are reachable and validate malformed payloads (`400/401/403`, never `404/503`).
- `Growth gating` returns `hasAccess=true` and member `/api/church-life/status` returns `active=true`.
- `Church Life endpoints` return `200` (not `404/503`).
- `broadcast templates/audiences` return `200`.
- `/admin/` contains cache-busted `/admin/app.js?v=...` and `/admin/styles.css?v=...`, and those assets return `200`.

### Manual critical checks
```bash
curl -i https://api.churpay.com/health
```

```bash
curl -i -X POST https://api.churpay.com/api/auth/login/member \
  -H "Content-Type: application/json" \
  -d '{"identifier":"0710000000","password":"test123"}'

curl -i -X POST https://api.churpay.com/api/auth/login/admin \
  -H "Content-Type: application/json" \
  -d '{"identifier":"0710000000","password":"test123"}'

curl -i -X POST https://api.churpay.com/api/super/login \
  -H "Content-Type: application/json" \
  -d '{"identifier":"super@churpay.com","password":"YOUR_SUPER_PASSWORD"}'
```

```bash
TOKEN=$(curl -sS -X POST https://api.churpay.com/api/auth/login/admin \
  -H "Content-Type: application/json" \
  -d '{"identifier":"0710000000","password":"test123"}' | jq -r '.token // empty')

curl -i https://api.churpay.com/api/auth/me -H "Authorization: Bearer $TOKEN"
curl -i "https://api.churpay.com/api/churches/me/transactions?limit=5" -H "Authorization: Bearer $TOKEN"
```

```bash
# PayFast callback reachability + bridge pages
curl -i https://api.churpay.com/webhooks/payfast/itn
curl -i "https://api.churpay.com/api/payfast/return?pi=test123&mp=CP-TEST123"
curl -i "https://api.churpay.com/api/payfast/cancel?pi=test123&mp=CP-TEST123"
```

```bash
# Public give flow
curl -i "https://api.churpay.com/api/public/give/context?joinCode=GCCOC-1234&fund=general"
curl -i "https://churpay.com/g/GCCOC-1234?fund=general"
```

### Live PayFast acceptance test
1. Create a payment intent (member or visitor flow).
2. Complete checkout on PayFast.
3. Verify DB:
```bash
set -a; source /etc/churpay/churpay-api.env; set +a
psql "$DATABASE_URL" -c "
select m_payment_id,status,provider_payment_id,updated_at
from payment_intents
where m_payment_id='YOUR_M_PAYMENT_ID';
"

psql "$DATABASE_URL" -c "
select reference,provider,provider_payment_id,amount,platform_fee_amount,amount_gross,superadmin_cut_amount,created_at
from transactions
where reference='YOUR_M_PAYMENT_ID';
"
```
4. Confirm admin and super dashboards include the transaction and fee fields.

## 6) Rollback Steps

Rollback to previous release tag:

```bash
cd /var/www/churpay/repo/churpay-api
bash scripts/rollback-prod.sh PROD-v1.1-stabilization
```

If you need manual rollback:

```bash
cd /var/www/churpay/repo/churpay-api
git fetch --all --tags
git checkout tags/PROD-v1.1-stabilization -b rollback-$(date +%Y%m%d%H%M%S)
npm ci --omit=dev
pm2 restart /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save
sudo nginx -t
sudo systemctl reload nginx
curl -i https://api.churpay.com/health
```

## 7) Daily Ops Checklist

- `curl -i https://api.churpay.com/health` returns 200.
- `pm2 status` shows `churpay-api` online.
- `systemctl status nginx` is active/running.
- Check error logs (no spikes):
  - PM2: `/root/.pm2/logs/churpay-api-error-0.log`
  - PM2 out: `/root/.pm2/logs/churpay-api-out-0.log`
  - Nginx access: `/var/log/nginx/access.log`
  - Nginx error: `/var/log/nginx/error.log`
- Verify PayFast ITN traffic in logs.
- Verify at least one admin and one super portal login.
- Run DNS consistency check during incidents:
  - `bash scripts/ops-dns-check.sh churpay.com 178.62.81.206`

## 8) Incident Troubleshooting Playbooks

### A) SSL handshake issues (`curl: (60)` / cert mismatch)
1. Validate active cert hosts:
```bash
echo | openssl s_client -servername churpay.com -connect churpay.com:443 2>/dev/null | openssl x509 -noout -subject -issuer -dates
```
2. Check Nginx server blocks:
```bash
sudo nginx -T | sed -n '/server_name churpay.com/,+80p'
```
3. Re-issue cert if needed:
```bash
sudo certbot --nginx -d churpay.com -d www.churpay.com -m YOUR_EMAIL_ADDRESS --agree-tos --no-eff-email
sudo nginx -t && sudo systemctl reload nginx
```

### B) 502/504 gateway
1. Check listeners:
```bash
ss -ltnp | egrep ':80|:443|:8080'
```
2. Check upstream health (local) before public health:
```bash
curl -i http://127.0.0.1:8080/health
curl -i https://api.churpay.com/health
```
3. Check PM2 + app logs:
```bash
pm2 status
pm2 logs churpay-api --lines 120 --nostream
```
4. Restart safely:
```bash
pm2 restart /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save
sudo nginx -t && sudo systemctl reload nginx
```
5. If public `https://api.churpay.com/health` briefly returns `502` right after restart, wait for upstream warm-up:
```bash
for i in {1..20}; do
  curl -fsS http://127.0.0.1:8080/health >/dev/null && break
  sleep 1
done
curl -i https://api.churpay.com/health
```

### C) PayFast ITN not updating payment intent
1. Confirm canonical ITN endpoint is reachable:
```bash
curl -i https://api.churpay.com/webhooks/payfast/itn
```
2. Check logs for signature/amount mismatch:
```bash
pm2 logs churpay-api --lines 200 --nostream | grep -Ei 'itn|amount mismatch|invalid signature|m_payment_id'
```
3. Verify callback URLs in generated checkout URL are on `api.churpay.com`.
4. Verify amount check expects `payment_intents.amount_gross`.
5. Confirm `payment_intents.m_payment_id` exists and is unique.

### D) DB connectivity / SSL mode
1. Validate required env in process:
```bash
pm2 env 0 | egrep 'DATABASE_URL|DATABASE_CA_CERT|PGSSLINSECURE|NODE_ENV'
```
2. Production requires CA and disallows insecure SSL.
3. Validate DB connectivity:
```bash
set -a; source /etc/churpay/churpay-api.env; set +a
psql "$DATABASE_URL" -c 'select now();'
```

### E) Rate limit false positives
1. Inspect current limits:
```bash
pm2 env 0 | egrep 'RATE_LIMIT|AUTH_RATE_LIMIT'
```
2. Check response headers (`X-RateLimit-*`, `Retry-After`).
3. Adjust env values and restart PM2 with `--update-env`.

### F) CORS issues
1. Confirm production allowlist:
```bash
pm2 env 0 | egrep '^CORS_ORIGINS=|^PUBLIC_BASE_URL='
```
2. Ensure browser origin exactly matches one of configured origins.
3. Restart PM2 after env updates.

### G) Web give page shows `Unrecognized token '<'`
This means frontend expected JSON but received HTML. In practice, this indicates API base mismatch or an HTML fallback/error page returned for a JSON endpoint.

1. Confirm API endpoint returns JSON directly:
```bash
curl -i "https://api.churpay.com/api/public/give/context?joinCode=GCCOC-1234&fund=general"
```
2. Confirm give page points API calls to `https://api.churpay.com` (not relative `/api/...` on `churpay.com`).
3. Confirm `/g/:joinCode` is routed to Node and not rewritten to website `index.html`.
4. Hard refresh mobile browser (or reopen tab) to clear cached JS.
5. If still failing, inspect HTML response body for `<!doctype html>` to identify which layer is returning HTML:
   - Nginx website fallback
   - Express 404/500 HTML error page

### H) Website/portal changes not visible after deploy
1. Confirm files were uploaded from the correct local path (watch for `rsync` "No such file or directory").
2. Confirm Nginx web root is correct:
```bash
sudo nginx -T 2>/dev/null | sed -n '/server_name churpay.com/,+80p' | grep -E 'server_name|root '
```
3. Confirm live HTML references new asset versions:
```bash
curl -sS https://api.churpay.com/admin/ | grep -Eo 'styles.css\\?v=[^"]+|app.js\\?v=[^"]+|churpay-logo[^"]+\\.svg\\?v=[^"]+' | head
curl -sS https://api.churpay.com/super/login/ | grep -Eo 'styles.css\\?v=[^"]+|churpay-logo[^"]+\\.svg\\?v=[^"]+' | head
curl -sS https://churpay.com/ | grep -Eo '/assets/index-[^"]+\\.js|/assets/index-[^"]+\\.css'
```
4. If versions in live HTML did not change, redeploy the exact file again.
5. If versions changed but UI is stale, hard refresh and clear browser/site data.
6. Do not run `npm ci` inside `/var/www/churpay/repo/churpay-web` unless `package.json` exists there; for static-only web deploy, upload `dist/` directly.

### I) ChurPay Growth page shows `formatDateTimeLocal is not defined`
This is a stale frontend bundle/runtime mismatch issue, not a DB migration issue.

1. Confirm current live admin asset versions:
```bash
curl -sS https://api.churpay.com/admin/ | grep -Eo 'app.js\\?v=[0-9a-z]+|styles.css\\?v=[0-9a-z]+' | sort -u
```
2. Deploy only admin static files (`public/admin/index.html`, `public/admin/app.js`, `public/admin/styles.css`) and restart PM2.
3. Verify `/health` is `200` and new `app.js?v=...` is visible in live HTML.
4. Hard refresh browser (`Cmd+Shift+R`) or close/reopen installed PWA instance.
5. If still present, clear site data for `api.churpay.com` and re-open.

### J) ChurPay Growth UI is blurred/frozen and forms are not clickable
This is usually a frontend runtime exception leaving modal/overlay state open.

1. Confirm API is healthy:
```bash
curl -i https://api.churpay.com/health | sed -n '1,20p'
```
2. Check for recent JS/runtime-induced empty states by validating key Growth endpoints with admin token:
```bash
BASE=https://api.churpay.com
for p in \
"/api/admin/operations/overview" \
"/api/admin/operations/insights?weeks=12" \
"/api/admin/church-life/services" \
"/api/admin/church-life/followups"
do
  printf "%-65s " "$p"
  curl -s -o /dev/null -w "%{http_code}\n" "$BASE$p" -H "Authorization: Bearer $ADMIN_TOKEN"
done
```
3. If status codes are healthy (`200/401/403` and not `404/503`), redeploy admin static files and cache-bust.
4. If only public is broken but local `127.0.0.1:8080` is healthy, inspect Nginx override rules before touching app code.

## 9) ChurPay Growth (Phase 1) Runbook

### 9.1 Deploy commands (API + admin/super static)
From Mac:
```bash
HOST=root@178.62.81.206
API_LOCAL=/Users/mzizimzwakhe/Documents/Churpay-demo/churpay-api
WEB_LOCAL=/Users/mzizimzwakhe/Documents/Churpay-demo/churpay-web/dist
API_REMOTE=/var/www/churpay/repo/churpay-api
WEB_REMOTE=/var/www/churpay/repo/churpay-web/dist

# Sync API source (exclude env + node_modules)
rsync -az --delete \
  --exclude '.git' \
  --exclude 'node_modules' \
  --exclude '.env' \
  "$API_LOCAL/" "$HOST:$API_REMOTE/"

# Sync public website bundle
rsync -az --delete \
  "$WEB_LOCAL/" "$HOST:$WEB_REMOTE/"
```

On droplet:
```bash
set -euo pipefail
API=/var/www/churpay/repo/churpay-api
APP_CFG=$API/deploy/ecosystem.config.cjs

cd "$API"
npm install --omit=dev
npm run migrate
pm2 restart "$APP_CFG" --only churpay-api --update-env
pm2 save

# wait for upstream before reloading nginx/public checks
for i in {1..40}; do
  curl -fsS http://127.0.0.1:8080/health >/dev/null && break
  sleep 1
done

nginx -t
systemctl reload nginx
curl -i https://api.churpay.com/health
```

Fast static hotfix (admin portal only; no API sync):
```bash
HOST=root@178.62.81.206
LOCAL=/Users/mzizimzwakhe/Documents/Churpay-demo/churpay-api/public/admin
REMOTE=/var/www/churpay/repo/churpay-api/public/admin

rsync -avz "$LOCAL/index.html" "$LOCAL/app.js" "$LOCAL/styles.css" "$HOST:$REMOTE/"

ssh "$HOST" 'bash -se' <<'EOF'
set -euo pipefail
cd /var/www/churpay/repo/churpay-api
pm2 restart deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save
for i in {1..40}; do curl -fsS http://127.0.0.1:8080/health >/dev/null && break; sleep 1; done
curl -sS https://api.churpay.com/admin/ | grep -Eo 'app.js\\?v=[0-9a-z]+|styles.css\\?v=[0-9a-z]+' | sort -u
EOF
```

Notes:
- `curl: (7) Failed to connect to 127.0.0.1:8080` immediately after restart can be transient during PM2 warm-up.
- If the final `/health` is `200`, do not treat that transient error as failure.

### 9.2 Church Life access model (canonical hardening)
- Church Life/growth gating is **access-based** and must use canonical subscription state.
- Canonical statuses:
  - `TRIALING`
  - `ACTIVE`
  - `PAST_DUE`
  - `GRACE`
  - `SUSPENDED`
  - `CANCELED`
- Decisions must use:
  - `subscription.hasAccess` (`true` = allow Church Life)
  - `active` from `GET /api/church-life/status` (member-facing, derived from `hasAccess`)
- Do not gate by legacy string checks like `PENDING/ACTIVE` directly.

### 9.3 Trial + subscription flow (fully automated)
- Admin starts/continues Growth via:
  - `POST /api/admin/church-operations/subscription/request`
- Behavior:
  - If no subscription row exists: create `TRIALING` immediately with 14-day window.
  - Return checkout URL for PayFast.
  - If trial expired and not active: keep `hasAccess` from state machine and return renewal checkout.
- No manual super-admin toggles in normal flow.

### 9.4 Webhook + reconcile rules
- PayFast webhook endpoints:
  - `POST /webhooks/payfast/itn`
  - `POST /webhooks/payfast/subscription` (alias)
- Expected transition behavior:
  - Payment success => `ACTIVE`
  - Payment failed => `PAST_DUE` (grace window starts from `past_due_at`)
  - Canceled => `CANCELED`
- Reconcile job (every 15m default) enforces time-based transitions:
  - `TRIALING` expired => `SUSPENDED`
  - `PAST_DUE` => `GRACE` (while grace window open)
  - `PAST_DUE/GRACE` expired => `SUSPENDED`
- Env:
  - `SUBSCRIPTION_JOBS_ENABLED=true`
  - `SUBSCRIPTION_JOBS_INTERVAL_MS=900000`
  - `SUBSCRIPTION_JOBS_LIMIT=1000`
- Migration/files to confirm on droplet:
  - `migrations/20260221_0100__church_subscriptions_hardening.sql`
  - `src/church-subscriptions.js`
  - `src/subscription-jobs.js`
- Auditing:
  - `church_subscription_audit_logs` records transitions (`TRIAL_STARTED`, `PAYMENT_OK`, `PAYMENT_FAILED`, `GRACE_STARTED`, `SUSPENDED`, `CANCELED`, `REACTIVATED`, `PLAN_CHANGED`)
  - Webhook idempotency enforced in `church_subscription_webhook_events` (`provider,event_key` unique).

### 9.5 Phase 1 smoke tests (Growth + Church Life)
1. Get admin token, request Growth subscription/trial:
```bash
BASE=https://api.churpay.com
ADMIN_TOKEN="YOUR_ADMIN_TOKEN"
curl -sS -X POST "$BASE/api/admin/church-operations/subscription/request" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{}' | jq
```

2. Member status must show Church Life access:
```bash
MEMBER_TOKEN="YOUR_MEMBER_TOKEN"
curl -sS "$BASE/api/church-life/status" \
  -H "Authorization: Bearer $MEMBER_TOKEN" | jq
```

3. Member services endpoint must be `200` (not `403/404`) when access is active:
```bash
curl -i -sS "$BASE/api/church-life/services" \
  -H "Authorization: Bearer $MEMBER_TOKEN" | sed -n '1,25p'
```

4. Admin creates service, then member check-in:
```bash
SERVICE_ID="$(curl -sS -X POST "$BASE/api/admin/church-life/services" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"serviceName":"Sunday Main Service","serviceDate":"'"$(date -u +%F)"'","published":true}' | jq -r '.service.id')"

curl -sS -X POST "$BASE/api/church-life/check-ins" \
  -H "Authorization: Bearer $MEMBER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"serviceId":"'"$SERVICE_ID"'","method":"TAP"}' | jq
```

5. Growth insights endpoint should return `200` with overview + weekly trend:
```bash
curl -sS "$BASE/api/admin/operations/insights?weeks=12" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq '{overview, sampleWeek:.weeklyTrend[0]}'
```

### 9.6 Nginx guardrails (recurring + church-life false 503/404 prevention)
- There must be no hardcoded return blocks overriding API routes, for example:
  - `location ^~ /api/recurring-givings { return 503 ... }`
- Ensure only one `location /g/` block exists in `/etc/nginx/sites-enabled/churpay-web`.
- Check active config:
```bash
nginx -T 2>/dev/null | grep -nE 'server_name api\\.churpay\\.com|/api/recurring-givings|/api/church-life'
```
- Check duplicate `/g/` locations:
```bash
grep -n 'location /g/' /etc/nginx/sites-enabled/churpay-web
```
- If conflicting server blocks exist for `api.churpay.com`, disable duplicates in `/etc/nginx/sites-enabled`, then:
```bash
nginx -t && systemctl reload nginx
```

### 9.7 Roadmap progress matrix (Church CRM + Church Life)
Status key: `DONE`, `PARTIAL`, `NOT_STARTED`

#### Phase 1 (fast win): services + attendance check-in + attendance dashboard
- `DONE` Subscription gating for Church Life (`/api/church-life/status`, `hasAccess`, 403 guard when locked).
- `DONE` Member Church Life flow in app (Check In, Prayer Request, Events, Apologies).
- `DONE` Church services CRUD + publish in admin.
- `DONE` Check-ins:
  - Member tap check-in
  - QR/usher method support in API
  - Usher-assisted check-in in admin
  - Kiosk mode and CSV import in admin
- `DONE` Attendance records + operations attendance table/chart in admin.
- `DONE` Events with poster support (`poster_url` + `poster_data_url`) and member event list.
- `DONE` Prayer inbox with category/visibility/assignment.
- `DONE` Attendance + giving insights endpoint/UI:
  - weekly attendance trend
  - first-time vs returning visitors
  - 4-week retention rate
  - donor participation rate
  - giving per attendance by fund/campus/service
  - baseline at-risk member list (attendance + giving drop)

#### Phase 2: member directory + follow-up tasks + segmentation
- `DONE` CRM schema foundation:
  - `church_member_profiles`
  - `church_groups`
  - `church_group_members`
  - `church_followups`
  - `church_followup_tasks`
- `DONE` Church CRM admin APIs:
  - `GET/PUT /api/admin/church-life/member-profiles...`
  - `GET/POST/PATCH /api/admin/church-life/groups...`
  - `GET/POST/PATCH /api/admin/church-life/followups...`
  - `GET/POST/PATCH /api/admin/church-life/followups/:followupId/tasks...`
- `PARTIAL` Member directory base exists (admin members list + role updates).
- `DONE` CRM profile extensions:
  - household + address editor
  - age-band enrichment + children count
  - alternate phone + WhatsApp + occupation
  - emergency contact fields (name/phone/relation)
- `PARTIAL` Follow-up/task board model (CRUD + admin task board shipped; workflow automations pending).
- `DONE` Visitor intake form in admin (Visitors tab):
  - volunteer/usher capture
  - creates `VISITOR_CALL` follow-up with optional assignment/service binding
- `PARTIAL` Groups/small groups + memberships (group + membership CRUD + admin manager shipped; campaigns/automation pending).
- `DONE` Segments + in-app broadcast campaigns:
  - Segment catalog + audience preview endpoints
  - In-app broadcast send endpoint with recipient delivery rows
  - Admin UI panel for segment preview + send + broadcast history
- `DONE` Follow-up automation actions:
  - Auto-followup preview (first-time visitors, missed 3 weeks)
  - Run-now endpoint + scheduler job hook

#### Phase 3: predictive insights + automated campaigns
- `PARTIAL` At-risk baseline exists (attendance + giving drop). Predictive scoring still pending.
- `NOT_STARTED` Predictive/automated campaigns.

#### Compliance and trust
- `DONE` Full Church Life auth role matrix shipped: `super`, `admin`, `accountant`, `finance`, `pastor`, `volunteer`, `usher`, `teacher`, `member`.
- `DONE` Field-level redaction/enforcement shipped for Church Life:
  - prayer sensitive content
  - member profile consent + notes
  - follow-up sensitive contact/details
  - check-in live contact fields
- `DONE` Children's Church flows shipped:
  - teacher/usher/admin check-in from Operations tab
  - walk-in child check-in support (no parent profile required)
  - parent pickup + staff checkout fallback when parent is not present
- `PARTIAL` POPIA/privacy baseline present (privacy docs + consent timestamp fields).
- `DONE` Audit logs for profile edits and attendance overrides:
  - DB table: `church_life_audit_logs`
  - Write hooks shipped for:
    - member profile create/update
    - attendance auto-mark / attendance override
    - usher check-in override
    - CSV attendance import batches
  - API endpoint shipped: `GET /api/admin/church-life/audit-logs`

#### Required next build slice (current)
1. Add per-church staff policy overrides UI (keep global defaults as fallback baseline).
2. Add phase-3 predictive scoring for at-risk members and automated campaign triggers.
3. Add WhatsApp provider integration behind consent + template approval controls (optional, feature-flagged).

### 9.8 Phase 2 CRM API smoke tests
1. Ensure migration applied:
```bash
cd /var/www/churpay/repo/churpay-api
npm run migrate
```

2. List member profiles:
```bash
BASE=https://api.churpay.com
ADMIN_TOKEN="YOUR_ADMIN_TOKEN"
curl -sS "$BASE/api/admin/church-life/member-profiles?limit=20" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq '{count:(.profiles|length), sample:.profiles[0]}'
```

3. Create a group, then assign members:
```bash
GROUP_ID="$(curl -sS -X POST "$BASE/api/admin/church-life/groups" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"Young Adults","groupType":"SMALL_GROUP","active":true}' | jq -r '.group.id')"

curl -sS -X PUT "$BASE/api/admin/church-life/groups/$GROUP_ID/members" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"members":[{"memberPk":"YOUR_MEMBER_UUID","memberRole":"MEMBER","active":true}]}' | jq
```

4. Create a follow-up and add a task:
```bash
FOLLOWUP_ID="$(curl -sS -X POST "$BASE/api/admin/church-life/followups" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"memberRef":"0658760444","followupType":"VISITOR_CALL","priority":"MEDIUM","title":"First-time visitor call"}' \
  | jq -r '.followup.id')"

curl -sS -X POST "$BASE/api/admin/church-life/followups/$FOLLOWUP_ID/tasks" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"title":"Call by Friday","status":"TODO"}' | jq
```

5. Preview segment audience + send broadcast:
```bash
curl -sS "$BASE/api/admin/church-life/broadcast-audience?segmentKey=ALL_MEMBERS&limit=20" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq '{segmentKey,count,sample:.audience[0]}'

curl -sS -X POST "$BASE/api/admin/church-life/broadcasts" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"title":"Welcome to ChurPay Growth","body":"This is a test in-app broadcast.","segmentKey":"ALL_MEMBERS"}' \
  | jq '{broadcast,summary}'
```

6. Preview and run auto follow-ups:
```bash
curl -sS "$BASE/api/admin/church-life/auto-followups/preview?sampleLimit=25" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq '{first:(.firstTimeVisitors|length),missed:(.missedThreeWeeks|length)}'

curl -sS -X POST "$BASE/api/admin/church-life/auto-followups/run" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"limitPerRule":120}' | jq
```

### 9.9 Compliance audit log smoke tests
1. Verify audit endpoint:
```bash
BASE=https://api.churpay.com
ADMIN_TOKEN="YOUR_ADMIN_TOKEN"
curl -sS "$BASE/api/admin/church-life/audit-logs?limit=50" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq '{count:.meta.count, sample:.logs[0]}'
```

2. Filter profile edits only:
```bash
curl -sS "$BASE/api/admin/church-life/audit-logs?action=PROFILE_UPDATED&entityType=MEMBER_PROFILE&limit=20" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq '{count:.meta.count, sample:.logs[0]}'
```

3. Filter attendance overrides only:
```bash
curl -sS "$BASE/api/admin/church-life/audit-logs?action=ATTENDANCE_OVERRIDE&limit=20" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq '{count:.meta.count, sample:.logs[0]}'
```

### 9.10 Church Life role matrix (strict baseline)

Use this as the canonical production baseline for Church Life access.

| Role | Church Life scope | Sensitive fields |
|---|---|---|
| `super` | Full access (`*`) | Full read/write |
| `admin` | Full access (`*`) | Full read/write |
| `pastor` | Full operations + services + check-ins + prayer + events + profiles + groups + followups + broadcasts + auto-followups | Can read/write consent and notes; can read sensitive prayer/follow-up content |
| `finance` | Read-only operational visibility (overview/attendance/insights/services/check-ins/apologies/prayer/events/profiles/groups/followups/tasks/audit/broadcasts preview) | Profile consent and notes are redacted; follow-up sensitive contact/details redacted; prayer sensitive content redacted; no profile writes |
| `volunteer` | Services + live check-ins + usher check-in + events + followups/task board + children check-in/pickup + visitor forms | No profile consent/notes read; no prayer sensitive content |
| `usher` | Door flow + visitor flow: services read, usher check-in, children check-in/pickup, followups/task board | No profile access; no prayer sensitive content |
| `teacher` | Children's Church flow: services read + children check-in/pickup/contact read | No general profile/group/followup management |
| `accountant` | Alias to `finance` behavior | Same as finance |
| `member` | Member Church Life endpoints only (`/api/church-life/*`) when subscription access is active | No admin Church Life endpoints |

Field-level enforcement rules:
- Profile consent write requires `profiles.consent.write`.
- Profile notes read requires `profiles.notes.read`.
- Follow-up sensitive fields require `followups.sensitive.read`.
- Prayer sensitive content requires `prayer.sensitive.read`.
- Check-in contact details in live feed require `checkins.contact.read`.

### 9.11 Role matrix QA checklist (production)

Pre-check:
```bash
BASE="https://api.churpay.com"
curl -i -sS "$BASE/health" | sed -n '1,15p'
```

1) Assign test user to finance:
```bash
curl -sS -X PATCH "$BASE/api/admin/members/$TARGET_MEMBER_ID/role" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"role":"finance"}' | jq '{ok,error,member:(.member|{id,role})}'
```
- Expected: `ok=true`, role=`finance`.

2) Finance redaction + write-deny:
```bash
curl -sS "$BASE/api/admin/church-life/member-profiles?limit=500" \
  -H "Authorization: Bearer $FINANCE_TOKEN" \
  | jq -r --arg id "$TARGET_MEMBER_ID" '.profiles[] | select(.memberPk==$id) | {memberId,consentData,consentContact,notes}'

curl -i -sS -X PUT "$BASE/api/admin/church-life/member-profiles/$TARGET_MEMBER_ID" \
  -H "Authorization: Bearer $FINANCE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"consentData":false}' | sed -n '1,25p'
```
- Expected:
  - redacted fields (`consentData=false`, `consentContact=false`, `notes=null`)
  - `403` with `code=CHURCH_LIFE_PERMISSION_DENIED`.

3) Pastor consent/profile write:
```bash
curl -sS -X PATCH "$BASE/api/admin/members/$TARGET_MEMBER_ID/role" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"role":"pastor"}' | jq '{ok,error,member:(.member|{id,role})}'

curl -sS -X PUT "$BASE/api/admin/church-life/member-profiles/$TARGET_MEMBER_ID" \
  -H "Authorization: Bearer $PASTOR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"consentData":false,"consentContact":false,"notes":"PASTOR_UPDATE_OK"}' \
  | jq '{ok,error,profile:(.profile|{memberId,consentData,consentContact,notes})}'
```
- Expected: `ok=true`, profile update succeeds.

4) Volunteer limited access:
```bash
curl -sS -X PATCH "$BASE/api/admin/members/$TARGET_MEMBER_ID/role" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"role":"volunteer"}' | jq '{ok,error,member:(.member|{id,role})}'

curl -i -sS "$BASE/api/admin/church-life/member-profiles?limit=1" \
  -H "Authorization: Bearer $VOL_TOKEN" | sed -n '1,20p'
```
- Expected: profile endpoint returns `403 CHURCH_LIFE_PERMISSION_DENIED`.

5) Usher visitor-form/check-in scope:
```bash
curl -sS -X PATCH "$BASE/api/admin/members/$TARGET_MEMBER_ID/role" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"role":"usher"}' | jq '{ok,error,member:(.member|{id,role})}'

curl -i -sS "$BASE/api/admin/church-life/followups?limit=10" \
  -H "Authorization: Bearer $USHER_TOKEN" | sed -n '1,20p'
```
- Expected: usher can access followups/check-in endpoints; profile endpoints remain blocked.

6) Teacher children flow scope:
```bash
curl -sS -X PATCH "$BASE/api/admin/members/$TARGET_MEMBER_ID/role" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"role":"teacher"}' | jq '{ok,error,member:(.member|{id,role})}'

curl -i -sS "$BASE/api/admin/church-life/children/check-ins?limit=20" \
  -H "Authorization: Bearer $TEACHER_TOKEN" | sed -n '1,20p'
```
- Expected: teacher can access children endpoints; general profile/group/followup management stays restricted.

7) Restore test role:
```bash
curl -sS -X PATCH "$BASE/api/admin/members/$TARGET_MEMBER_ID/role" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"role":"member"}' | jq '{ok,error,member:(.member|{id,role})}'
```

### 9.12 Broadcast template/audience 503 recovery
Symptom:
- `GET /api/admin/church-life/broadcast-templates?...` returns `503`
- `GET /api/admin/church-life/broadcast-audiences?...` returns `503`
- but `broadcast-segments` and `broadcast-audience` can still return `200`

Root cause:
- DB broadcast preset tables missing or not migrated.

Recovery:
```bash
set -euo pipefail
cd /var/www/churpay/repo/churpay-api
DATABASE_URL="$(sed -n 's/^DATABASE_URL=//p' /etc/churpay/churpay-api.env | head -n1)"
[ -n "$DATABASE_URL" ] || { echo "DATABASE_URL missing"; exit 1; }

psql "$DATABASE_URL" -Atc "select to_regclass('public.church_broadcast_templates');"
psql "$DATABASE_URL" -Atc "select to_regclass('public.church_broadcast_saved_audiences');"

npm run migrate
pm2 restart deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save
for i in {1..40}; do curl -fsS http://127.0.0.1:8080/health >/dev/null && break; sleep 1; done
```

Validation with fresh admin token:
```bash
BASE="https://api.churpay.com"
for p in \
"/api/admin/church-life/broadcast-templates?includeInactive=0&limit=300" \
"/api/admin/church-life/broadcast-audiences?includeInactive=0&limit=300" \
"/api/admin/church-life/broadcast-segments" \
"/api/admin/church-life/broadcast-audience?segmentKey=ALL_MEMBERS&limit=20"
do
  printf "%-95s " "$p"
  curl -s -o /dev/null -w "%{http_code}\n" "$BASE$p" -H "Authorization: Bearer $ADMIN_TOKEN"
done
```

Expected:
- all endpoints return `200` (never `404`, never `503`).

### 9.13 Token sanity checks (avoid false 401 during smoke tests)
Before role/CRM tests in a new shell:
```bash
echo "BASE=${BASE:-unset}"
echo "ADMIN_TOKEN_LEN=${#ADMIN_TOKEN}"
```

Rules:
- `401 Missing token` with `TOKEN_LEN=0` is a test-shell issue, not API logic failure.
- Always log in again in the same shell before running protected endpoint tests.

## 10) Migration Hygiene and Startup Rules

- Never run schema DDL in request handlers.
- All schema changes must be SQL migrations in `/var/www/churpay/repo/churpay-api/migrations`.
- Use:
  - `npm run migrate` to apply
  - `npm run migrate:check` for pending checks (requires DB)
  - `npm run migrate:lint` for static CI validation

## 11) Release Tagging

After successful smoke + one live PayFast transaction verification:

```bash
cd /var/www/churpay/repo/churpay-api
git tag -a PROD-v1.1-stabilization -m "Production stabilization release"
git push origin PROD-v1.1-stabilization
```

## 12) Phase Delivery Ledger (Production)

This section is the consolidated delivery state across phases for production operations.

### Phase 1 (Growth access + Church Life baseline)
- Subscription access gating implemented (`hasAccess` canonical enforcement).
- Member Church Life base endpoints live.
- Service/check-in/prayer/events baseline shipped.

### Phase 2 (CRM + follow-ups + broadcast foundations)
- Member profiles, groups, follow-ups, task board APIs shipped.
- Segment audiences + broadcast templates/audiences/send endpoints live.

### Phase 3 (Volunteer governance foundation)
- Ministry model shipped:
  - `church_ministries`
  - `church_ministry_roles`
  - `volunteer_role_terms`
  - `ministry_schedules`
  - `ministry_schedule_assignments`
- Attention summary endpoint for volunteer review/load surfaced in admin.

### Phase 4 (Small groups meetings + attendance)
- Group meetings scheduling API live.
- Meeting attendance capture API live with summary totals.
- Member group-meeting visibility + personal attendance exposure shipped.

### Phase 5 (Church events hardening)
- Event registration/cancel member flow shipped and validated in smoke.
- Event registration schema migration applied.
- Member poster payload support (`posterUrl`/`posterDataUrl`) active.

### Phase 6 (Household + Family Tree + app polish)
- Household member-candidates lookup endpoint shipped.
- Household linking from CRM/member directory shipped.
- Family relationship graph shipped (`/api/church-life/family/*`).
- Family links validated in production (spouse + parent relationships).
- Mobile Church Life polish:
  - Family Tree grouped lane presentation.
  - Resilient loading for optional Church Life modules to reduce global error banners.

### Phase verification status
- API health: `200 OK` on `https://api.churpay.com/health`
- Strict smoke: required checks pass (health, auth, giving, ITN, growth gating, church life, groups, broadcasts, admin cache-bust)
- Remaining rollout item: continue mobile release cadence (OTA/store) as app polish increments are cut.
