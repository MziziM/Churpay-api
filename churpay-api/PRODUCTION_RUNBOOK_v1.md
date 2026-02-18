# Churpay Production Runbook v1.2 (Stabilization + Static Deploy)

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
ADMIN_IDENTIFIER=0710000000 \
ADMIN_PASSWORD=test123 \
SUPER_IDENTIFIER=super@churpay.com \
SUPER_PASSWORD=YOUR_SUPER_PASSWORD \
bash scripts/smoke-prod.sh
```

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
2. Check PM2 + app logs:
```bash
pm2 status
pm2 logs churpay-api --lines 120 --nostream
```
3. Restart safely:
```bash
pm2 restart /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save
sudo nginx -t && sudo systemctl reload nginx
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

## 9) Migration Hygiene and Startup Rules

- Never run schema DDL in request handlers.
- All schema changes must be SQL migrations in `/var/www/churpay/repo/churpay-api/migrations`.
- Use:
  - `npm run migrate` to apply
  - `npm run migrate:check` for pending checks (requires DB)
  - `npm run migrate:lint` for static CI validation

## 10) Release Tagging

After successful smoke + one live PayFast transaction verification:

```bash
cd /var/www/churpay/repo/churpay-api
git tag -a PROD-v1.1-stabilization -m "Production stabilization release"
git push origin PROD-v1.1-stabilization
```
