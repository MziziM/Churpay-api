#!/usr/bin/env bash
set -euo pipefail

# Canonical production deploy runner from macOS/dev machine.
# This script syncs the entire API repo subtree to the server, then performs:
# install -> migrate -> drift check -> pm2 restart -> health checks.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_API_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

SERVER_HOST="${SERVER_HOST:-root@178.62.81.206}"
REMOTE_DIR="${REMOTE_DIR:-/var/www/churpay/repo/churpay-api}"
APP_CFG="${APP_CFG:-/var/www/churpay/repo/churpay-api/deploy/ecosystem.config.cjs}"
APP_NAME="${APP_NAME:-churpay-api}"
API_BASE_URL="${API_BASE_URL:-https://api.churpay.com}"
UPSTREAM_PORT="${UPSTREAM_PORT:-8080}"
UPSTREAM_WAIT_RETRIES="${UPSTREAM_WAIT_RETRIES:-90}"
REQUIRE_CLEAN_GIT="${REQUIRE_CLEAN_GIT:-0}"
DRY_RUN="${DRY_RUN:-0}"

REQUIRED_FILES=(
  "scripts/run-migrations.mjs"
  "scripts/check-migrations.js"
  "src/payments/payment-drift-guard.js"
  "src/routes.webhooks.js"
  "src/payments/webhook-inbox.js"
  "src/payment-webhook-jobs.js"
  "migrations/20260224_0100__payment_events_audit.sql"
  "migrations/20260224_0200__payment_intents_provider_normalization.sql"
  "migrations/20260224_0300__webhook_inbox.sql"
)

SSH_OPTS=(
  -o ServerAliveInterval=20
  -o ServerAliveCountMax=6
)

echo "[mac-deploy] local_api_dir=${LOCAL_API_DIR}"
echo "[mac-deploy] server_host=${SERVER_HOST}"
echo "[mac-deploy] remote_dir=${REMOTE_DIR}"

for cmd in rsync ssh npm; do
  command -v "${cmd}" >/dev/null 2>&1 || {
    echo "[mac-deploy] missing required command: ${cmd}" >&2
    exit 1
  }
done

for rel in "${REQUIRED_FILES[@]}"; do
  if [[ ! -f "${LOCAL_API_DIR}/${rel}" ]]; then
    echo "[mac-deploy] missing required file: ${LOCAL_API_DIR}/${rel}" >&2
    exit 1
  fi
done

if git -C "${LOCAL_API_DIR}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  GIT_SHA="$(git -C "${LOCAL_API_DIR}" rev-parse --short HEAD)"
  GIT_DIRTY_COUNT="$(git -C "${LOCAL_API_DIR}" status --porcelain | wc -l | awk '{print $1}')"
  echo "[mac-deploy] git_sha=${GIT_SHA}"
  echo "[mac-deploy] git_dirty_files=${GIT_DIRTY_COUNT}"
  if [[ "${REQUIRE_CLEAN_GIT}" == "1" && "${GIT_DIRTY_COUNT}" != "0" ]]; then
    echo "[mac-deploy] refusing deploy because REQUIRE_CLEAN_GIT=1 and working tree is dirty" >&2
    exit 1
  fi
fi

echo "[mac-deploy] local migration lint"
npm --prefix "${LOCAL_API_DIR}" run -s migrate:lint

RSYNC_ARGS=(
  -az
  --delete
  --exclude ".git"
  --exclude "node_modules"
  --exclude ".env"
)
if [[ "${DRY_RUN}" == "1" ]]; then
  RSYNC_ARGS+=(--dry-run)
fi

echo "[mac-deploy] syncing files to production"
rsync "${RSYNC_ARGS[@]}" "${LOCAL_API_DIR}/" "${SERVER_HOST}:${REMOTE_DIR}/"

if [[ "${DRY_RUN}" == "1" ]]; then
  echo "[mac-deploy] dry-run complete"
  exit 0
fi

echo "[mac-deploy] running remote deploy steps"
ssh "${SSH_OPTS[@]}" "${SERVER_HOST}" \
  "REMOTE_DIR='${REMOTE_DIR}' APP_CFG='${APP_CFG}' APP_NAME='${APP_NAME}' API_BASE_URL='${API_BASE_URL}' UPSTREAM_PORT='${UPSTREAM_PORT}' UPSTREAM_WAIT_RETRIES='${UPSTREAM_WAIT_RETRIES}' bash -s" <<'EOF'
set -euo pipefail

echo "[remote] cwd=${REMOTE_DIR}"
cd "${REMOTE_DIR}"

echo "[remote] install production dependencies"
if ! npm ci --omit=dev; then
  echo "[remote] npm ci failed; falling back to npm install"
  npm install --omit=dev
fi

echo "[remote] migrate"
npm run migrate

echo "[remote] drift check"
npm run -s migrate:drift

echo "[remote] restart pm2"
pm2 restart "${APP_CFG}" --only "${APP_NAME}" --update-env
pm2 save

echo "[remote] wait for local upstream health"
upstream_ok=0
for i in $(seq 1 "${UPSTREAM_WAIT_RETRIES}"); do
  if curl -fsS "http://127.0.0.1:${UPSTREAM_PORT}/health" >/dev/null; then
    echo "[remote] local upstream healthy in ${i}s"
    upstream_ok=1
    break
  fi
  sleep 1
done
if [[ "${upstream_ok}" != "1" ]]; then
  echo "[remote] local upstream did not become healthy" >&2
  exit 1
fi

echo "[remote] public health check"
curl -fsS "${API_BASE_URL}/health" >/dev/null

echo "[remote] deployed successfully"
EOF

echo "[mac-deploy] deploy complete"
