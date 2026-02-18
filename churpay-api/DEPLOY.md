# Churpay Deploy Guide (Stabilized)

This guide is for droplet-only production:
- API: `https://api.churpay.com`
- Website: `https://churpay.com`
- Admin: `https://api.churpay.com/admin/`
- Super Admin: `https://api.churpay.com/super/`

## 1) Prerequisites

- Ubuntu 24.04 droplet
- Node.js 20 + npm 10
- PM2 installed globally
- Nginx + Certbot installed
- `/etc/churpay/churpay-api.env` populated

## 2) Fast Production Deploy

```bash
cd /var/www/churpay/repo
git fetch --all --tags
git checkout main
git pull --ff-only

cd /var/www/churpay/repo/churpay-api
if ! npm ci --omit=dev; then
  npm install --omit=dev
fi
# Migration script auto-loads /etc/churpay/churpay-api.env (or CHURPAY_ENV_FILE).
npm run migrate

pm2 restart /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save

sudo nginx -t
sudo systemctl reload nginx
```

## 3) Post-Deploy Verification

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

### DNS consistency (authoritative)
```bash
cd /var/www/churpay/repo/churpay-api
bash scripts/ops-dns-check.sh churpay.com 178.62.81.206
```

### Manual must-pass checks
```bash
curl -i https://api.churpay.com/health
curl -i https://api.churpay.com/admin/
curl -i https://api.churpay.com/super/
curl -i "https://churpay.com/g/GCCOC-1234?fund=general"
```

## 4) Migration Workflow

```bash
cd /var/www/churpay/repo/churpay-api
npm run migrate:lint
npm run migrate:check
npm run migrate
```

Notes:
- `migrate:check` and `migrate` require DB connectivity.
- Schema changes must be migration-only; no request-time DDL.

## 5) Super Login Contract

Canonical:
- `POST /api/super/login`

Compatibility alias (kept, deprecated):
- `POST /api/auth/login/super`

Both return same shape:
- `{ ok, token, profile }`

## 6) PayFast Contract Checks

- Canonical ITN: `POST /webhooks/payfast/itn`
- ITN amount validation compares PayFast gross to `payment_intents.amount_gross`
- Return/cancel bridge pages:
  - `/api/payfast/return`
  - `/api/payfast/cancel`

Quick check:
```bash
pm2 logs churpay-api --lines 200 --nostream | grep -Ei 'itn|amount mismatch|invalid signature|m_payment_id'
```

## 7) Rollback

Preferred:
```bash
cd /var/www/churpay/repo/churpay-api
bash scripts/rollback-prod.sh PROD-v1.1-stabilization
```

Manual:
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

## 8) Logs and Runtime Diagnostics

- PM2 error log: `/root/.pm2/logs/churpay-api-error-0.log`
- PM2 out log: `/root/.pm2/logs/churpay-api-out-0.log`
- Nginx access log: `/var/log/nginx/access.log`
- Nginx error log: `/var/log/nginx/error.log`

Quick diagnostic:
```bash
pm2 status
systemctl status nginx --no-pager
ss -ltnp | egrep ':80|:443|:8080'
```
