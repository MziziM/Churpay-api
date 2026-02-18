#!/usr/bin/env bash
set -euo pipefail

DOMAIN="${1:-churpay.com}"
EXPECTED_IP="${2:-178.62.81.206}"
NS_RECORDS=$(dig +short NS "${DOMAIN}")

if [[ -z "${NS_RECORDS}" ]]; then
  echo "[dns-check] No NS records found for ${DOMAIN}"
  exit 1
fi

echo "[dns-check] domain=${DOMAIN} expected_ip=${EXPECTED_IP}"
echo "[dns-check] authoritative nameservers:"
echo "${NS_RECORDS}" | sed 's/^/ - /'
echo

FAILED=0
while IFS= read -r ns; do
  [[ -z "${ns}" ]] && continue
  ns="${ns%.}"
  echo "=== ${ns} ==="
  ROOT_A=$(dig +short A "${DOMAIN}" @"${ns}" | tr '\n' ' ')
  WWW_A=$(dig +short A "www.${DOMAIN}" @"${ns}" | tr '\n' ' ')
  API_A=$(dig +short A "api.${DOMAIN}" @"${ns}" | tr '\n' ' ')
  echo "root A: ${ROOT_A:-<none>}"
  echo "www  A/CNAME target: ${WWW_A:-<none>}"
  echo "api  A: ${API_A:-<none>}"

  if ! dig +short A "${DOMAIN}" @"${ns}" | grep -qx "${EXPECTED_IP}"; then
    echo "[warn] ${ns} root record mismatch"
    FAILED=1
  fi

  if ! dig +short A "www.${DOMAIN}" @"${ns}" | grep -qx "${EXPECTED_IP}"; then
    echo "[warn] ${ns} www record mismatch"
    FAILED=1
  fi

  if ! dig +short A "api.${DOMAIN}" @"${ns}" | grep -qx "${EXPECTED_IP}"; then
    echo "[warn] ${ns} api record mismatch"
    FAILED=1
  fi

  echo
done <<< "${NS_RECORDS}"

if [[ "${FAILED}" -ne 0 ]]; then
  echo "[dns-check] FAILED"
  exit 1
fi

echo "[dns-check] OK"
