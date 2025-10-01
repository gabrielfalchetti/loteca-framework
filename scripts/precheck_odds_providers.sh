#!/usr/bin/env bash
set -euo pipefail

RODADA="${RODADA:-}"
DEBUG="${DEBUG:-false}"
THEODDS_API_KEY="${THEODDS_API_KEY:-}"
X_RAPIDAPI_KEY="${X_RAPIDAPI_KEY:-}"

echo "[precheck] iniciando…"

# ---------- TheOddsAPI ----------
if [[ -z "${THEODDS_API_KEY}" ]]; then
  echo "[precheck] ERRO: THEODDS_API_KEY ausente nos secrets."
  exit 10
fi

THEODDS_URL="https://api.the-odds-api.com/v4/sports?apiKey=${THEODDS_API_KEY}"
set +e
resp_toa=$(curl -sS -w " HTTP_CODE:%{http_code}" "${THEODDS_URL}")
curl_rc=$?
set -e
if [[ $curl_rc -ne 0 ]]; then
  echo "[precheck] ERRO: falha de rede ao consultar TheOddsAPI."
  exit 10
fi

toa_code="${resp_toa##*HTTP_CODE:}"
toa_body="${resp_toa% HTTP_CODE:*}"

if [[ "${toa_code}" != "200" ]]; then
  echo "[precheck] ERRO: TheOddsAPI HTTP ${toa_code}."
  if [[ "${toa_code}" == "401" ]]; then
    echo "[precheck] Dica: cheque validade da chave, projeto e cota."
  fi
  [[ "${DEBUG}" == "true" ]] && echo "[precheck] body: ${toa_body}"
  exit 10
fi
echo "[precheck] TheOddsAPI ok."

# ---------- API-Football (RapidAPI) ----------
if [[ -z "${X_RAPIDAPI_KEY}" ]]; then
  echo "[precheck] ERRO: X_RAPIDAPI_KEY ausente nos secrets."
  exit 10
fi

# endpoint leve que valida assinatura e headers
APIFOOT_HOST="api-football-v1.p.rapidapi.com"
APIFOOT_URL="https://${APIFOOT_HOST}/v3/status"

set +e
resp_api=$(curl -sS -w " HTTP_CODE:%{http_code}" \
  -H "x-rapidapi-key: ${X_RAPIDAPI_KEY}" \
  -H "x-rapidapi-host: ${APIFOOT_HOST}" \
  "${APIFOOT_URL}")
curl_rc=$?
set -e
if [[ $curl_rc -ne 0 ]]; then
  echo "[precheck] ERRO: falha de rede ao consultar API-Football."
  exit 10
fi

api_code="${resp_api##*HTTP_CODE:}"
api_body="${resp_api% HTTP_CODE:*}"

if [[ "${api_code}" != "200" ]]; then
  echo "[precheck] ERRO: API-Football HTTP ${api_code}."
  if [[ "${api_code}" == "403" ]]; then
    echo "[precheck] Dica: assine a API no RapidAPI (mesmo plano free) e use a chave da conta assinada."
  fi
  [[ "${DEBUG}" == "true" ]] && echo "[precheck] body: ${api_body}"
  exit 10
fi
echo "[precheck] API-Football ok."

echo "[precheck] provedores OK — pode prosseguir."