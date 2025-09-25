#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
api_smoketest.py — checa rapidamente se os APIs principais estão OK:
- RapidAPI (API-Football): fixtures e standings
- Open-Meteo: clima no dia de um jogo
Requer: config/config.yaml e os secrets no ambiente.
"""

import os, sys, json
from pathlib import Path
import yaml, requests, pandas as pd

EXIT = 0  # acumulador de falhas

def fail(msg):
    global EXIT
    print(f"[FAIL] {msg}", file=sys.stderr)
    EXIT = 1

def ok(msg):
    print(f"[OK] {msg}")

def cfg():
    return yaml.safe_load(open("config/config.yaml", "r", encoding="utf-8"))

def get_env_clean(name):
    val = (os.getenv(name) or "").strip().replace("\r","").replace("\n","")
    return val

def check_secret(name):
    v = get_env_clean(name)
    if not v:
        fail(f"Secret ausente: {name}")
    elif any(c in (os.getenv(name) or "") for c in "\r\n\t"):
        fail(f"Secret {name} contém quebra de linha/tab (limpe no Settings>Secrets).")
    else:
        ok(f"Secret {name} presente (len={len(v)})")
    return v

def http_get(url, headers=None, params=None, timeout=25):
    r = requests.get(url, headers=headers or {}, params=params or {}, timeout=timeout)
    status = r.status_code
    if status >= 400:
        snippet = r.text[:300].replace("\n"," ")
        raise RuntimeError(f"HTTP {status} {url} :: {snippet}")
    return r

def main():
    global EXIT
    C = cfg()
    rodada = C.get("rodada_id") or "2025-09-20_21"
    prov = C.get("provider", {})
    base = prov.get("base_url") or ""
    league = prov.get("league")
    season = prov.get("season")

    # 1) Secrets
    rapid_key = check_secret("RAPIDAPI_KEY")
    # (opcional) outras chaves:
    # check_secret("STANDINGS_API_TOKEN")
    # check_secret("WANDB_API_KEY")

    # montar headers RapidAPI padrão
    rapid_headers = {
        "X-RapidAPI-Key": rapid_key,
        "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com",
    }

    # 2) Fixtures (usando round = "Regular Season - NN" se rodada tiver "_NN")
    def round_api_from(rod):
        parts = rod.split("_", 1)
        rn = parts[1] if len(parts) > 1 else ""
        rn = "".join([c for c in rn if c.isdigit()])
        return f"Regular Season - {rn}" if rn else None

    round_api = round_api_from(rodada)
    params_fix = {"league": league, "season": season}
    if round_api:
        params_fix["round"] = round_api
    else:
        params_fix["date"] = (rodada.split("_",1)[0])

    try:
        url_fix = f"{base}/fixtures"
        r = http_get(url_fix, headers=rapid_headers, params=params_fix)
        j = r.json()
        rows = j.get("response") if isinstance(j, dict) else j
        n = len(rows or [])
        if n == 0:
            fail("Fixtures respondeu 200 mas sem jogos (confira league/season/round/data).")
        else:
            ok(f"Fixtures 200 OK — {n} jogos")
    except Exception as e:
        fail(f"Fixtures falhou: {e}")

    # 3) Standings
    try:
        url_std = f"{base}/standings"
        params_std = {"league": league, "season": season}
        r = http_get(url_std, headers=rapid_headers, params=params_std)
        j = r.json()
        # estruturas variam, mas normalmente vem 'response'
        payload = j.get("response") if isinstance(j, dict) else j
        n = len(payload or [])
        if n == 0:
            fail("Standings 200 OK mas sem payload.")
        else:
            ok(f"Standings 200 OK — payload tamanho {n}")
    except Exception as e:
        fail(f"Standings falhou: {e}")

    # 4) Open-Meteo para 1 jogo (pega 1º da sua matches.csv)
    matches_path = C["paths"]["matches_csv"].replace("${rodada}", rodada)
    stadiums_path = C["paths"]["stadiums_csv"]

    if Path(matches_path).exists() and Path(stadiums_path).exists():
        try:
            dfm = pd.read_csv(matches_path)
            dfs = pd.read_csv(stadiums_path)
            if dfm.empty:
                fail("matches.csv existe, mas está vazio.")
            else:
                # normalizar stadium_id para join
                def norm(x):
                    import pandas as pd
                    if pd.isna(x): return "unknown"
                    s = str(x).strip()
                    if s.endswith(".0") and s.replace(".","",1).isdigit():
                        s = s[:-2]
                    return s.lower().replace(" ","_") or "unknown"
                dfm["stadium_id_norm"] = dfm.get("stadium_id", pd.Series([None]*len(dfm))).apply(norm)
                dfs["stadium_id_norm"] = dfs.get("stadium_id", pd.Series([None]*len(dfs))).apply(norm)
                one = dfm.iloc[0]
                mid = one.get("match_id")
                stid = one.get("stadium_id_norm")
                row = dfs[dfs["stadium_id_norm"] == stid]
                if row.empty:
                    fail(f"Open-Meteo: estádio '{stid}' não mapeado em stadiums_latlon.csv.")
                else:
                    lat, lon = float(row.iloc[0]["lat"]), float(row.iloc[0]["lon"])
                    date_iso = str(pd.to_datetime(one.get("kickoff_utc")).date())
                    url = "https://api.open-meteo.com/v1/forecast"
                    params = {
                        "latitude": lat,
                        "longitude": lon,
                        "hourly": "temperature_2m,precipitation_probability,precipitation,wind_speed_10m",
                        "start_date": date_iso,
                        "end_date": date_iso,
                        "timezone": "UTC",
                    }
                    rr = http_get(url, params=params)
                    jj = rr.json()
                    if "hourly" not in jj:
                        fail("Open-Meteo 200 OK mas sem 'hourly'.")
                    else:
                        ok(f"Open-Meteo 200 OK — match_id {mid} ({lat},{lon}) {date_iso}")
        except Exception as e:
            fail(f"Open-Meteo falhou: {e}")
    else:
        print("[WARN] Pulei Open-Meteo: matches.csv ou stadiums_latlon.csv não encontrado.")

    # resultado final
    if EXIT == 0:
        ok("SMOKE TEST: TODOS OK")
    else:
        print("SMOKE TEST: houve falhas (veja [FAIL] acima).", file=sys.stderr)
    sys.exit(EXIT)

if __name__ == "__main__":
    main()
