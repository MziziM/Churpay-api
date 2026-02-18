#!/usr/bin/env bash
set -euo pipefail

# Production deploy helper for the API droplet.
# Assumes:
# - repo at /var/www/churpay/repo/churpay-api
# - PM2 app name: churpay-api

APP_CFG="${APP_CFG:-/var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs}"
APP_NAME="${APP_NAME:-churpay-api}"

cd "$(dirname "$0")/.."

echo "[deploy] cwd=$(pwd)"
echo "[deploy] install prod dependencies"
if ! npm ci --omit=dev; then
  echo "[deploy] npm ci failed; falling back to npm install"
  npm install --omit=dev
fi

echo "[deploy] run migrations"
npm run migrate

echo "[deploy] restart pm2 ($APP_NAME)"
pm2 restart "$APP_CFG" --only "$APP_NAME" --update-env
pm2 save

echo "[deploy] reload nginx"
nginx -t
systemctl reload nginx

echo "[deploy] done"

