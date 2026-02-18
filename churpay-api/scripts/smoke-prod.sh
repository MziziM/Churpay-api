#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-https://api.churpay.com}"
ADMIN_IDENTIFIER="${ADMIN_IDENTIFIER:-0710000000}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-test123}"
SUPER_IDENTIFIER="${SUPER_IDENTIFIER:-super@churpay.com}"
SUPER_PASSWORD="${SUPER_PASSWORD:-}"
CONTACT_EMAIL="${CONTACT_EMAIL:-prod-smoke@churpay.com}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[smoke] missing required command: $1"
    exit 1
  fi
}

require_cmd curl
require_cmd jq

echo "[smoke] BASE_URL=${BASE_URL}"
echo "[smoke] health"
curl -fsS "${BASE_URL}/health" | jq .

echo "[smoke] admin login"
ADMIN_TOKEN="$(curl -fsS -X POST "${BASE_URL}/api/auth/login/admin" \
  -H "Content-Type: application/json" \
  -d "{\"identifier\":\"${ADMIN_IDENTIFIER}\",\"password\":\"${ADMIN_PASSWORD}\"}" | jq -r '.token // empty')"

if [[ -z "${ADMIN_TOKEN}" ]]; then
  echo "[smoke] failed to obtain admin token"
  exit 1
fi

echo "[smoke] /api/auth/me"
curl -fsS "${BASE_URL}/api/auth/me" -H "Authorization: Bearer ${ADMIN_TOKEN}" | jq .

echo "[smoke] /api/churches/me/transactions"
curl -fsS "${BASE_URL}/api/churches/me/transactions?limit=3" -H "Authorization: Bearer ${ADMIN_TOKEN}" | jq .

echo "[smoke] /api/admin/dashboard/transactions/recent"
curl -fsS "${BASE_URL}/api/admin/dashboard/transactions/recent?limit=3" -H "Authorization: Bearer ${ADMIN_TOKEN}" | jq .

echo "[smoke] /api/public/contact"
curl -fsS -X POST "${BASE_URL}/api/public/contact" \
  -H "Content-Type: application/json" \
  -d "{\"fullName\":\"Prod Smoke\",\"email\":\"${CONTACT_EMAIL}\",\"message\":\"smoke test\"}" | jq .

if [[ -n "${SUPER_PASSWORD}" ]]; then
  echo "[smoke] /api/super/login"
  SUPER_TOKEN="$(curl -fsS -X POST "${BASE_URL}/api/super/login" \
    -H "Content-Type: application/json" \
    -d "{\"identifier\":\"${SUPER_IDENTIFIER}\",\"password\":\"${SUPER_PASSWORD}\"}" | jq -r '.token // empty')"

  if [[ -z "${SUPER_TOKEN}" ]]; then
    echo "[smoke] failed to obtain super token"
    exit 1
  fi

  echo "[smoke] /api/auth/login/super alias"
  curl -fsS -X POST "${BASE_URL}/api/auth/login/super" \
    -H "Content-Type: application/json" \
    -d "{\"identifier\":\"${SUPER_IDENTIFIER}\",\"password\":\"${SUPER_PASSWORD}\"}" | jq .

  echo "[smoke] /api/super/me"
  curl -fsS "${BASE_URL}/api/super/me" -H "Authorization: Bearer ${SUPER_TOKEN}" | jq .
fi

echo "[smoke] payfast bridge pages"
curl -fsS -I "${BASE_URL}/api/payfast/return?pi=test123&mp=CP-TEST123" | head -n 1
curl -fsS -I "${BASE_URL}/api/payfast/cancel?pi=test123&mp=CP-TEST123" | head -n 1

echo "[smoke] done"
