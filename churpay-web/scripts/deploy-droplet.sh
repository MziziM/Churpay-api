#!/usr/bin/env bash
set -euo pipefail

# Deploy churpay-web static site to a droplet that serves churpay.com via Nginx.
#
# Usage (run from your laptop, not on the droplet):
#   bash churpay-web/scripts/deploy-droplet.sh
#
# Optional env overrides:
#   DEPLOY_HOST=root@178.62.81.206
#   DEPLOY_DIR=/var/www/churpay/repo/churpay-web/dist

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_HOST="${DEPLOY_HOST:-root@178.62.81.206}"
DEPLOY_DIR="${DEPLOY_DIR:-/var/www/churpay/repo/churpay-web/dist}"

cd "$ROOT_DIR"

if [ ! -d dist ]; then
  echo "[deploy] dist/ missing; building..."
  npm ci
  npm run build
fi

echo "[deploy] ensuring remote dir exists: ${DEPLOY_HOST}:${DEPLOY_DIR}"
ssh "$DEPLOY_HOST" "mkdir -p '$DEPLOY_DIR'"

echo "[deploy] syncing dist/ -> ${DEPLOY_HOST}:${DEPLOY_DIR}"
rsync -avz --delete dist/ "${DEPLOY_HOST}:${DEPLOY_DIR}/"

echo "[deploy] reloading nginx"
ssh "$DEPLOY_HOST" "nginx -t && systemctl reload nginx"

echo "[deploy] done"
