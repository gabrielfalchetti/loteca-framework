#!/usr/bin/env python3
# scripts/check_rapidapi_key.py
"""
Valida a RAPIDAPI_KEY para a API-Football e dá diagnóstico amigável.

Uso:
  python scripts/check_rapidapi_key.py
  python scripts/check_rapidapi_key.py --key SEU_TOKEN_AQUI
  python scripts/check_rapidapi_key.py --host api-football-v1.p.rapidapi.com

Códigos de saída:
  0 = OK
  2 = Chave ausente
  3 = Não autorizado (401/403) ou chave inválida
  4 = Não assinado na API (subscription necessária)
  5 = Timeout / rede
  6 = Erro inesperado
"""
from __future__ import annotations
import os, sys, json, argparse, textwrap
from typing import Tuple
import requests

DEFAULT_HOST = "api-football-v1.p.rapidapi.com"
STATUS_URL = "https://api-football-v1.p.rapidapi.com/v3/status"
SMOKE_URL  = "https://api-football-v1.p.rapidapi.com/v3/leagues"

def pretty(obj) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        return str(obj)

def classify_error(status: int, payload: dict) -> Tuple[int, str]:
    """
    Mapeia (status, payload) em (exit_code, mensagem curta).
    """
    # Mensagens típicas que a API-Football retorna via RapidAPI
    msg = ""
    if isinstance(payload, dict):
        msg = (payload.get("message")
               or payload.get("errors")
               or payload.get("response")
               or "")
        if isinstance(msg, (list, dict)): msg = pretty(msg)

    if status in (401, 403):
        # Tentamos diferenciar "invalid key" de "not subscribed"
        lower = str(msg).lower()
        if "not subscribed" in lower:
            return 4, "Você não está assinando este produto no RapidAPI (Subscribe)."
        if "invalid api key" in lower or "invalid key" in lower:
            return 3, "Chave inválida. Verifique o valor de RAPIDAPI_KEY."
        return 3, f"Não autorizado ({status}). Revise chave/assinatura/limites."
    if status == 429:
        return 3, "Rate limit excedido (429). Aguarde ou aumente o plano."
    if 500 <= status < 600:
        return 6, f"Erro do provedor ({status})."
    # Qualquer outra coisa inesperada
    return 6, f"Falha inesperada (HTTP {status})."

def call(url: str, key: str, host: str, params=None, timeout=20) -> requests.Response:
    headers = {
        "x-rapidapi-key": key,
        "x-rapidapi-host": host,
    }
    return requests.get(url, headers=headers, params=params or {}, timeout=timeout)

def main() -> int:
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Checker de RAPIDAPI_KEY (API-Football)",
        epilog=textwrap.dedent(
            f"""\
            Exemplo rápido:
              python {__file__} --key SEU_TOKEN
              python {__file__} --host {DEFAULT_HOST}
            """
        ),
    )
    ap.add_argument("--key", help="Override da RAPIDAPI_KEY (senão usa env).")
    ap.add_argument("--host", default=DEFAULT_HOST,
                    help=f"Header x-rapidapi-host (default: {DEFAULT_HOST})")
    ap.add_argument("--timeout", type=int, default=20, help="Timeout em segundos (default: 20)")
    args = ap.parse_args()

    key = args.key or os.getenv("RAPIDAPI_KEY", "").strip()
    host = args.host.strip()

    print("[check-rapidapi] host esperado:", host)
    if not key:
        print("[check-rapidapi] ERRO: RAPIDAPI_KEY ausente. Defina em Settings→Secrets do GitHub ou export no shell.")
        return 2

    # 1) Ping básico em /status
    try:
        print("[check-rapidapi] Testando /v3/status…")
        r = call(STATUS_URL, key, host, timeout=args.timeout)
        ct = r.headers.get("content-type", "")
        payload = {}
        try:
            payload = r.json()
        except Exception:
            pass

        print(f"[check-rapidapi] HTTP {r.status_code} content-type={ct}")
        if r.ok:
            print("[check-rapidapi] OK em /status ✅")
        else:
            code, msg = classify_error(r.status_code, payload)
            print("[check-rapidapi] Resposta:", pretty(payload)[:1000])
            print(f"[check-rapidapi] DIAGNÓSTICO: {msg}")
            return code
    except requests.exceptions.Timeout:
        print("[check-rapidapi] Timeout ao chamar /status.")
        return 5
    except requests.exceptions.RequestException as e:
        print(f"[check-rapidapi] Erro de rede: {e}")
        return 5
    except Exception as e:
        print(f"[check-rapidapi] Erro inesperado: {e}")
        return 6

    # 2) Smoke test simples em /leagues (opcional, mas ajuda a pegar throttle/assinatura)
    try:
        print("[check-rapidapi] Testando /v3/leagues?season=2025&country=Brazil…")
        r2 = call(SMOKE_URL, key, host, params={"season": 2025, "country": "Brazil"}, timeout=args.timeout)
        payload2 = {}
        try:
            payload2 = r2.json()
        except Exception:
            pass

        print(f"[check-rapidapi] HTTP {r2.status_code} em /leagues")
        if r2.ok:
            # mostra só um resumo
            results = payload2.get("results")
            print(f"[check-rapidapi] OK em /leagues ✅ results={results}")
            print("[check-rapidapi] Tudo certo com sua RAPIDAPI_KEY! 🎉")
            return 0
        else:
            code, msg = classify_error(r2.status_code, payload2)
            print("[check-rapidapi] Resposta:", pretty(payload2)[:1000])
            print(f"[check-rapidapi] DIAGNÓSTICO: {msg}")
            return code
    except requests.exceptions.Timeout:
        print("[check-rapidapi] Timeout ao chamar /leagues.")
        return 5
    except requests.exceptions.RequestException as e:
        print(f"[check-rapidapi] Erro de rede: {e}")
        return 5
    except Exception as e:
        print(f"[check-rapidapi] Erro inesperado: {e}")
        return 6

if __name__ == "__main__":
    sys.exit(main())
