# Churpay API Deploy Runbook

## 1) Seed Admin + Church in One Command

From `/Users/mzizimzwakhe/Documents/Churpay-demo/churpay-api`:

```bash
npm run seed:admin -- \
  --church-name "Great Commission Church of Christ" \
  --join-code "GCCOC-1234" \
  --admin-name "Admin Test" \
  --admin-phone "0710000000" \
  --admin-email "admin@test.com" \
  --admin-password "test123"
```

Script: `scripts/seed-admin.mjs`.

## 2) Required Environment Variables

Set these on Droplet or App Platform:

- `DATABASE_URL`
- `JWT_SECRET`
- `PUBLIC_BASE_URL`
- `PORT` (default: `8080`)
- `PAYFAST_MODE`
- `PAYFAST_MERCHANT_ID`
- `PAYFAST_MERCHANT_KEY`
- `PAYFAST_PASSPHRASE`
- `PAYFAST_RETURN_URL`
- `PAYFAST_CANCEL_URL`
- `PAYFAST_NOTIFY_URL`
- `PGSSLINSECURE`
- `TRUST_PROXY`
- `TRUST_PROXY_HOPS`
- `CORS_ORIGINS`
- `RATE_LIMIT_WINDOW_MS`
- `RATE_LIMIT_MAX`
- `AUTH_RATE_LIMIT_WINDOW_MS`
- `AUTH_RATE_LIMIT_MAX`

Optional:

- `SUPER_ADMIN_EMAIL`
- `SUPER_ADMIN_PASSWORD`

## 2A) Security Quick Wins (Production)

Run on droplet:

```bash
# 1) rotate JWT secret (strong random)
NEW_JWT_SECRET="$(openssl rand -base64 48)"
sudo sed -i "s|^JWT_SECRET=.*|JWT_SECRET=${NEW_JWT_SECRET}|" /etc/churpay/churpay-api.env

# 2) required production values
sudo grep -E "^(NODE_ENV|PUBLIC_BASE_URL|CORS_ORIGINS)=" /etc/churpay/churpay-api.env || true
# expected:
# NODE_ENV=production
# PUBLIC_BASE_URL=https://api.churpay.com
# CORS_ORIGINS=https://www.churpay.com,https://churpay.com

# 3) restart with refreshed env
pm2 restart /var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs --only churpay-api --update-env
pm2 save
```

Notes:

- `JWT_SECRET` is required in production by `src/auth.js`.
- keep `/etc/churpay/churpay-api.env` at mode `600`.
- include every browser origin that will call the API in `CORS_ORIGINS`.

## 2B) Post-Deploy Production Test Checklist

Run these five checks after every deploy:

```bash
# 1) health
curl -i https://api.churpay.com/health

# 2) login returns token
TOKEN=$(curl -sS -X POST https://api.churpay.com/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"phone":"0710000000","password":"test123"}' \
  | node -e 'let s="";process.stdin.on("data",d=>s+=d);process.stdin.on("end",()=>{try{process.stdout.write(JSON.parse(s).token||"")}catch{}})')
echo "TOKEN_LEN=${#TOKEN}"

# 3) /api/auth/me works with token
curl -i https://api.churpay.com/api/auth/me \
  -H "Authorization: Bearer $TOKEN"

# 4) /api/churches/me/transactions returns live data
curl -i "https://api.churpay.com/api/churches/me/transactions?limit=10" \
  -H "Authorization: Bearer $TOKEN"

# 5) PayFast webhook endpoint reachable (route/mount check)
curl -i https://api.churpay.com/webhooks/payfast/itn
```

## 3) Droplet Deploy (PM2)

```bash
# 0) bootstrap
sudo apt update
sudo apt install -y git curl nginx certbot python3-certbot-nginx
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
sudo npm i -g pm2

# 1) checkout
sudo mkdir -p /var/www/churpay
sudo chown -R $USER:$USER /var/www/churpay
git clone <your-repo-url> /var/www/churpay/repo
cd /var/www/churpay/repo/churpay-api
npm ci --omit=dev

# 2) env
sudo mkdir -p /etc/churpay
sudo cp .env.example /etc/churpay/churpay-api.env
sudo chown root:root /etc/churpay/churpay-api.env
sudo chmod 600 /etc/churpay/churpay-api.env
sudo nano /etc/churpay/churpay-api.env

# 3) seed admin (optional but recommended)
set -a; source /etc/churpay/churpay-api.env; set +a
npm run seed:admin -- \
  --church-name "Great Commission Church of Christ" \
  --join-code "GCCOC-1234" \
  --admin-name "Admin Test" \
  --admin-phone "0710000000" \
  --admin-email "admin@test.com" \
  --admin-password "test123"

# 4) start with PM2
cp deploy/ecosystem.config.cjs /var/www/churpay/ecosystem.config.cjs
pm2 start /var/www/churpay/ecosystem.config.cjs --update-env
pm2 save
pm2 startup

# 5) nginx reverse proxy
sudo cp deploy/nginx.churpay-api.conf /etc/nginx/sites-available/churpay-api
sudo ln -sf /etc/nginx/sites-available/churpay-api /etc/nginx/sites-enabled/churpay-api
sudo nginx -t
sudo systemctl reload nginx

# sanity check: nginx must listen on 80 + 443
ss -ltnp | egrep ':80|:443|:8080' || true
# expected to include:
# - nginx on :80
# - nginx on :443
# - node on :8080

# 6) TLS (after DNS points to droplet IP)
sudo certbot --nginx -d api.churpay.com

# 7) verify
curl -i http://127.0.0.1:8080/health
curl -i https://api.churpay.com/health
```

Update deploy:

```bash
cd /var/www/churpay/repo
git pull
cd churpay-api
npm ci --omit=dev
pm2 restart churpay-api --update-env
```

## 4) Droplet Deploy (systemd Alternative)

```bash
# create service user once
sudo useradd --system --create-home --shell /usr/sbin/nologin churpay

# copy app
sudo mkdir -p /var/www/churpay
sudo cp -R /path/to/repo/churpay-api /var/www/churpay/churpay-api
sudo chown -R churpay:churpay /var/www/churpay/churpay-api

# env
sudo mkdir -p /etc/churpay
sudo cp /var/www/churpay/churpay-api/.env.example /etc/churpay/churpay-api.env
sudo chown root:root /etc/churpay/churpay-api.env
sudo chmod 600 /etc/churpay/churpay-api.env
sudo nano /etc/churpay/churpay-api.env

# install + start service
sudo cp /var/www/churpay/churpay-api/deploy/churpay-api.service /etc/systemd/system/churpay-api.service
sudo systemctl daemon-reload
sudo systemctl enable churpay-api
sudo systemctl start churpay-api
sudo systemctl status churpay-api
```

## 5) DigitalOcean App Platform Deploy

Spec file: `.do/app.yaml`.

```bash
# 0) install/auth doctl (one time)
# macOS: brew install doctl
# linux: https://docs.digitalocean.com/reference/doctl/how-to/install/
doctl auth init

# 1) go to api directory
cd /Users/mzizimzwakhe/Documents/Churpay-demo/churpay-api

# 2) edit placeholders in .do/app.yaml
# - github.repo
# - domain values (api.example.com -> api.churpay.com)
# - secret placeholders (${...})

# 3) create app
doctl apps create --spec .do/app.yaml

# 4) for updates
doctl apps list
doctl apps update <APP_ID> --spec .do/app.yaml

# 5) inspect health check status
doctl apps get <APP_ID>
```

Health check path in spec: `/health`.

## 6) Templates Included

- PM2: `deploy/ecosystem.config.cjs`
- systemd: `deploy/churpay-api.service`
- nginx: `deploy/nginx.churpay-api.conf`
- App Platform: `.do/app.yaml`

## 7) Nginx 443 Recovery (If HTTPS Stops Working)

If `curl https://api.churpay.com/health` fails and `ss -ltnp` shows nginx only on `:80`, re-apply the nginx template and restart:

```bash
cd /var/www/churpay/repo/churpay-api
sudo cp deploy/nginx.churpay-api.conf /etc/nginx/sites-available/churpay-api
sudo ln -sf /etc/nginx/sites-available/churpay-api /etc/nginx/sites-enabled/churpay-api
sudo nginx -t
sudo systemctl daemon-reload
sudo systemctl restart nginx
ss -ltnp | egrep ':80|:443|:8080' || true
curl -i https://api.churpay.com/health
```

Quick validation:

```bash
grep -n "listen 443" /etc/nginx/sites-available/churpay-api
```
