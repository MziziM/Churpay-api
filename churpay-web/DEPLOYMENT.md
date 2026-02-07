# Churpay Website Deployment

## Option A (Recommended): Vercel + API on droplet

1. Push `churpay-web` to GitHub.
2. Import project into Vercel.
3. Set env vars:

```bash
VITE_API_BASE_URL=https://api.churpay.com
VITE_APP_LINK=https://expo.dev
```

4. Set domain:
- `churpay.com`
- `www.churpay.com` (redirect to root)

5. Verify:
- Home + all pages load
- `/status` reads API health
- Contact form submits to `https://api.churpay.com/api/public/contact`

## Option B: same droplet with Nginx static hosting

1. Build site locally:

```bash
cd /var/www/churpay/repo/churpay-web
npm ci
npm run build
```

2. Copy build output to static directory:

```bash
sudo mkdir -p /var/www/churpay/site
sudo rsync -av --delete dist/ /var/www/churpay/site/
```

3. Nginx server block (separate from api block):

```nginx
server {
  listen 80;
  server_name churpay.com www.churpay.com;
  return 301 https://churpay.com$request_uri;
}

server {
  listen 443 ssl http2;
  server_name churpay.com www.churpay.com;

  ssl_certificate /etc/letsencrypt/live/churpay.com/fullchain.pem;
  ssl_certificate_key /etc/letsencrypt/live/churpay.com/privkey.pem;

  root /var/www/churpay/site;
  index index.html;

  location / {
    try_files $uri /index.html;
  }
}
```

4. Reload Nginx:

```bash
sudo nginx -t && sudo systemctl reload nginx
```
