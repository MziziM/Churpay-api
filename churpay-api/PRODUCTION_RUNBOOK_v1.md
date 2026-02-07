# Churpay Production Runbook v1

## 1) Overview

### Production architecture
- **Host OS:** Ubuntu 24.04 LTS (DigitalOcean Droplet)
- **Runtime:** Node.js 20
- **Process manager:** PM2 (`churpay-api`)
- **Reverse proxy:** Nginx
- **TLS:** Let's Encrypt certificates (Certbot-managed)
- **Database:** DigitalOcean Managed PostgreSQL
- **Payments:** PayFast (live mode)
- **API base:** `https://api.churpay.com`
- **Admin web portal:** `https://api.churpay.com/admin/`
- **Super admin web portal:** `https://api.churpay.com/super/`

### Network and ports
- `80/tcp` -> Nginx (HTTP, redirects to HTTPS)
- `443/tcp` -> Nginx (HTTPS)
- `8080/tcp` -> Node/Express app (internal upstream for Nginx)

### Main route groups
- Health: `GET /health`
- Auth: `/api/auth/*`
- Super auth/API: `/api/super/*`
- PayFast ITN callback: `/webhooks/payfast/itn`
- PayFast return bridge: `/api/payfast/return`
- PayFast cancel bridge: `/api/payfast/cancel`

## 2) Environment variables

### Env file path
- **System env file:** `/etc/churpay/churpay-api.env`
- PM2 ecosystem reads this file directly.

### Required variables
- `NODE_ENV=production`
- `PORT=8080`
- `DATABASE_URL=postgresql://...`
- `JWT_SECRET=...`
- `PUBLIC_BASE_URL=https://api.churpay.com`
- `CORS_ORIGINS=https://www.churpay.com,https://churpay.com`
- `TRUST_PROXY=true`
- `TRUST_PROXY_HOPS=1`
- `PAYFAST_MODE=live`
- `PAYFAST_MERCHANT_ID=...`
- `PAYFAST_MERCHANT_KEY=...`
- `PAYFAST_RETURN_URL=https://api.churpay.com/api/payfast/return`
- `PAYFAST_CANCEL_URL=https://api.churpay.com/api/payfast/cancel`
- `PAYFAST_NOTIFY_URL=https://api.churpay.com/webhooks/payfast/itn`
- `PGSSLINSECURE=0` (or `1` when DO SSL chain validation/network requires it)

### Super admin variables
- `SUPER_ADMIN_EMAIL=super@churpay.com`
- `SUPER_ADMIN_PASSWORD=...`

### Optional variables
- `PAYFAST_PASSPHRASE=`
- `PAYFAST_DEBUG=0`
- `APP_DEEP_LINK_BASE=churpaydemo://payfast`
- `PAYFAST_APP_FALLBACK_URL=https://www.churpay.com`
- `RATE_LIMIT_WINDOW_MS=900000`
- `RATE_LIMIT_MAX=300`
- `AUTH_RATE_LIMIT_WINDOW_MS=900000`
- `AUTH_RATE_LIMIT_MAX=30`
- `DEMO_MODE=false`

### File ownership and permissions
```bash
sudo chown root:root /etc/churpay/churpay-api.env
sudo chmod 600 /etc/churpay/churpay-api.env
```

## 3) First-time production setup steps

### 3.1 System packages
```bash
sudo apt update
sudo apt install -y nginx certbot python3-certbot-nginx curl git jq
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
sudo npm i -g pm2
```

### 3.2 App checkout
```bash
sudo mkdir -p /var/www/churpay
sudo chown -R "$USER":"$USER" /var/www/churpay
cd /var/www/churpay
git clone YOUR_REPO_URL repo
cd /var/www/churpay/repo/churpay-api
npm ci --omit=dev
```

### 3.3 Environment file
```bash
sudo mkdir -p /etc/churpay
sudo cp /var/www/churpay/repo/churpay-api/.env.example /etc/churpay/churpay-api.env
sudo chown root:root /etc/churpay/churpay-api.env
sudo chmod 600 /etc/churpay/churpay-api.env
sudo nano /etc/churpay/churpay-api.env
```

### 3.4 PM2 start
```bash
pm2 start /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save
pm2 startup systemd -u "$USER" --hp "$HOME"
```

### 3.5 Nginx config
```bash
sudo cp /var/www/churpay/repo/churpay-api/deploy/nginx.churpay-api.conf /etc/nginx/sites-available/churpay-api
sudo ln -sf /etc/nginx/sites-available/churpay-api /etc/nginx/sites-enabled/churpay-api
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl daemon-reload
sudo systemctl restart nginx
```

### 3.6 SSL certificate
```bash
sudo certbot --nginx -d api.churpay.com -m YOUR_EMAIL_ADDRESS --agree-tos --no-eff-email
sudo systemctl reload nginx
```

### 3.7 Firewall
```bash
sudo ufw allow OpenSSH
sudo ufw allow 'Nginx Full'
sudo ufw --force enable
sudo ufw status
```

## 4) Standard deploy/update steps

```bash
cd /var/www/churpay/repo

git fetch --all --tags
git checkout main
git pull --ff-only

cd /var/www/churpay/repo/churpay-api
npm ci --omit=dev

pm2 restart /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save

sudo nginx -t
sudo systemctl reload nginx
```

### Deploy verification quick check
```bash
pm2 status
ss -ltnp | egrep ':80|:443|:8080'
curl -i https://api.churpay.com/health
```

## 5) Smoke tests

### 5.1 Health
```bash
curl -i https://api.churpay.com/health
```

### 5.2 Member/Admin/Super login
```bash
curl -i -X POST https://api.churpay.com/api/auth/login/member \
  -H "Content-Type: application/json" \
  -d '{"identifier":"0710000000","password":"test123"}'

curl -i -X POST https://api.churpay.com/api/auth/login/admin \
  -H "Content-Type: application/json" \
  -d '{"identifier":"0710000000","password":"test123"}'

# Current production super endpoint:
curl -i -X POST https://api.churpay.com/api/super/login \
  -H "Content-Type: application/json" \
  -d '{"identifier":"super@churpay.com","password":"ChangeThisToAStrongLongPassword123"}'

# If your client contract expects this path, verify whether alias is configured:
curl -i -X POST https://api.churpay.com/api/auth/login/super \
  -H "Content-Type: application/json" \
  -d '{"identifier":"super@churpay.com","password":"ChangeThisToAStrongLongPassword123"}'
```

### 5.3 Token + /me + transactions
```bash
TOKEN=$(curl -sS -X POST https://api.churpay.com/api/auth/login/admin \
  -H "Content-Type: application/json" \
  -d '{"identifier":"0710000000","password":"test123"}' | jq -r '.token // empty')

echo "TOKEN_LEN=${#TOKEN}"

curl -i https://api.churpay.com/api/auth/me \
  -H "Authorization: Bearer $TOKEN"

curl -i "https://api.churpay.com/api/churches/me/transactions?limit=10&offset=0" \
  -H "Authorization: Bearer $TOKEN"
```

### 5.4 Payment intent + status check
```bash
# 1) create payment intent
PAYLOAD=$(curl -sS -X POST https://api.churpay.com/api/payment-intents \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"fundId":"883e0ec0-b687-4d31-9ac6-de6a76c2dcd7","amount":10}')

echo "$PAYLOAD"

# 2) inspect checkout URL callback parameters (pi/mp)
echo "$PAYLOAD" | node -e 'let s="";process.stdin.on("data",d=>s+=d);process.stdin.on("end",()=>{const j=JSON.parse(s);const u=new URL(j.checkoutUrl);console.log("return_url=",decodeURIComponent(u.searchParams.get("return_url")));console.log("cancel_url=",decodeURIComponent(u.searchParams.get("cancel_url")));console.log("notify_url=",decodeURIComponent(u.searchParams.get("notify_url")));})'
```

### 5.5 PayFast callback and deep-link bridge checks
```bash
# ITN endpoint reachability (method should be POST from PayFast; GET is still useful as route smoke)
curl -i https://api.churpay.com/webhooks/payfast/itn

# Browser return/cancel bridge endpoints
curl -i "https://api.churpay.com/api/payfast/return?pi=test123&mp=CP-TEST123"
curl -i "https://api.churpay.com/api/payfast/cancel?pi=test123&mp=CP-TEST123"

# Optional legacy short paths (should redirect to /api/payfast/...)
curl -i "https://api.churpay.com/payfast/return?pi=test123&mp=CP-TEST123"
curl -i "https://api.churpay.com/payfast/cancel?pi=test123&mp=CP-TEST123"
```

## 6) Rollback steps

### 6.1 Roll back to a known git tag
```bash
cd /var/www/churpay/repo

git fetch --all --tags
git checkout tags/PROD-v1 -b rollback-PROD-v1

cd /var/www/churpay/repo/churpay-api
npm ci --omit=dev

pm2 restart /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save

sudo nginx -t
sudo systemctl reload nginx
```

### 6.2 Validate after rollback
```bash
curl -i https://api.churpay.com/health
pm2 status
pm2 logs churpay-api --lines 80 --nostream
```

## 7) Daily ops checklist

- Confirm health endpoint is `200 OK`.
- Check PM2 process is online and stable (`restarts` not climbing).
- Review API error log for auth/payment/DB errors.
- Review Nginx error log for `502`, `504`, upstream timeouts, TLS errors.
- Verify at least one admin login and `/api/auth/me` works.
- Verify transactions endpoint returns live data.
- Verify PayFast ITN events are being processed (new PAID records appear).
- Check disk, memory, and DB connectivity.
- Confirm SSL cert is valid and not near expiry.

Useful commands:
```bash
pm2 status
pm2 logs churpay-api --lines 120 --nostream
sudo tail -n 120 /var/log/nginx/error.log
sudo tail -n 120 /var/log/nginx/access.log
df -h
free -m
```

## 8) Incident troubleshooting playbooks

### A) SSL handshake issues
Symptoms:
- `curl: (35) ... sslv3 alert handshake failure`

Checks:
```bash
dig +short A api.churpay.com @1.1.1.1
dig +short A api.churpay.com @8.8.8.8
sudo nginx -t
sudo systemctl status nginx --no-pager
ss -ltnp | egrep ':443|:80'
openssl s_client -connect api.churpay.com:443 -servername api.churpay.com </dev/null | head -n 40
```

Fixes:
- Ensure DNS points to droplet IP.
- Ensure nginx listens on 443 and cert paths are valid.
- Reload/restart nginx.
- Reissue cert if missing/invalid:
```bash
sudo certbot --nginx -d api.churpay.com -m YOUR_EMAIL_ADDRESS --agree-tos --no-eff-email
sudo systemctl reload nginx
```

### B) 502/504 gateway
Symptoms:
- Nginx returns `502 Bad Gateway` or `504 Gateway Timeout`

Checks:
```bash
pm2 status
ss -ltnp | egrep ':8080'
pm2 logs churpay-api --lines 200 --nostream
sudo tail -n 200 /var/log/nginx/error.log
curl -i http://127.0.0.1:8080/health
```

Fixes:
- Restart app with env:
```bash
pm2 restart /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save
```
- If app is down due to env/database failure, fix env and restart.
- Validate nginx upstream routes still point to `127.0.0.1:8080`.

### C) PayFast ITN not updating payment intent
Symptoms:
- User paid, but status remains pending

Checks:
```bash
pm2 logs churpay-api --lines 300 --nostream | grep -i "itn"
curl -i https://api.churpay.com/webhooks/payfast/itn
```

Verify env URLs:
```bash
grep -E '^(PAYFAST_RETURN_URL|PAYFAST_CANCEL_URL|PAYFAST_NOTIFY_URL|PUBLIC_BASE_URL|APP_DEEP_LINK_BASE)=' /etc/churpay/churpay-api.env
```

Fixes:
- Ensure PayFast notify URL is exactly `https://api.churpay.com/webhooks/payfast/itn`.
- Ensure merchant credentials and passphrase match PayFast live config.
- Ensure signature verification passes (check app logs).
- Ensure `m_payment_id` maps to intent in DB.

### D) DB connectivity / SSL mode
Symptoms:
- `ETIMEDOUT`, `connect()` failure, `Invalid DATABASE_URL`, SSL errors

Checks:
```bash
grep '^DATABASE_URL=' /etc/churpay/churpay-api.env
node -e 'new URL(process.env.DATABASE_URL);console.log("DATABASE_URL OK")' < /dev/null
pm2 logs churpay-api --lines 150 --nostream
```

Load env for shell and test DB quickly:
```bash
set -a; source /etc/churpay/churpay-api.env; set +a
psql "$DATABASE_URL" -c 'select now();'
```

Fixes:
- Correct malformed `DATABASE_URL`.
- Toggle `PGSSLINSECURE` as needed for DO-managed Postgres pathing:
```bash
sudo sed -i 's/^PGSSLINSECURE=.*/PGSSLINSECURE=1/' /etc/churpay/churpay-api.env
pm2 restart /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
```

### E) Rate limit false positives
Symptoms:
- Valid users receive frequent `429 Too Many Requests`

Checks:
```bash
grep -E '^(RATE_LIMIT_WINDOW_MS|RATE_LIMIT_MAX|AUTH_RATE_LIMIT_WINDOW_MS|AUTH_RATE_LIMIT_MAX)=' /etc/churpay/churpay-api.env
```

Fixes:
- Increase limits conservatively:
```bash
sudo sed -i 's/^AUTH_RATE_LIMIT_MAX=.*/AUTH_RATE_LIMIT_MAX=60/' /etc/churpay/churpay-api.env
sudo sed -i 's/^RATE_LIMIT_MAX=.*/RATE_LIMIT_MAX=500/' /etc/churpay/churpay-api.env
pm2 restart /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
```
- Ensure load balancer/proxy chain is correctly trusted via `TRUST_PROXY` and `TRUST_PROXY_HOPS`.

### F) CORS issues
Symptoms:
- Browser requests blocked by CORS policy

Checks:
```bash
grep -E '^(CORS_ORIGINS|PUBLIC_BASE_URL)=' /etc/churpay/churpay-api.env
curl -i https://api.churpay.com/health -H 'Origin: https://www.churpay.com'
```

Fixes:
- Add all real frontend origins to `CORS_ORIGINS` (comma-separated).
- Restart with updated env:
```bash
pm2 restart /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
```

## Logs and diagnostics locations

### PM2
- `/root/.pm2/logs/churpay-api-out-0.log`
- `/root/.pm2/logs/churpay-api-error-0.log`
- `pm2 logs churpay-api --lines 200 --nostream`
- `pm2 env 0`

### Nginx
- `/var/log/nginx/access.log`
- `/var/log/nginx/error.log`
- `sudo journalctl -u nginx -n 200 --no-pager`

### System
- `sudo journalctl -xe --no-pager`
- `sudo systemctl status nginx --no-pager`
- `sudo systemctl status pm2-root --no-pager`

---

This runbook describes the current Churpay production stack on Ubuntu 24.04 with Node 20 + PM2 + Nginx + Let's Encrypt + DO Postgres + PayFast + Admin/Super portals.
