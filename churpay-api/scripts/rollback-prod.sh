#!/usr/bin/env bash
set -euo pipefail

TAG="${1:-}"
APP_DIR="${APP_DIR:-/var/www/churpay/repo/churpay-api}"
ECOSYSTEM="${ECOSYSTEM:-/var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs}"

if [[ -z "${TAG}" ]]; then
  echo "Usage: $0 <git-tag>"
  echo "Example: $0 PROD-v1.1-stabilization"
  exit 1
fi

if [[ ! -d "${APP_DIR}" ]]; then
  echo "[rollback] app dir not found: ${APP_DIR}"
  exit 1
fi

cd "${APP_DIR}"
echo "[rollback] fetching tags"
git fetch --all --tags
git checkout "tags/${TAG}" -b "rollback-${TAG}-$(date +%Y%m%d%H%M%S)"

echo "[rollback] install dependencies"
npm ci --omit=dev

echo "[rollback] restart pm2"
pm2 restart "${ECOSYSTEM}" --only churpay-api --update-env
pm2 save

echo "[rollback] reload nginx"
nginx -t
systemctl reload nginx

echo "[rollback] health check"
curl -fsS https://api.churpay.com/health

echo "[rollback] done"
