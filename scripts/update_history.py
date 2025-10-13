#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
update_history.py
-----------------
Atualiza (ou cria) um histórico de resultados em CSV.

Interface (compatível com o workflow):
    python -m scripts.update_history --since_days 14 --out data/history/results.csv

Comportamento:
- Se houver variável de ambiente API_FOOTBALL_KEY, tenta baixar jogos FINALIZADOS
  dos últimos `since_days` dias via API-FOOTBALL.
- Se a chamada falhar (sem chave, sem internet, erro HTTP etc.), o script ainda
  garante a existência do arquivo de saída com cabeçalho válido.
- Faz merge com um CSV existente (se existir) e remove duplicatas.

Colunas de saída (todas em minúsculas):
    date            (ISO 8601, UTC)
    league          (nome da liga, quando disponível)
    season          (ano/temporada)
    home
    away
    home_goals
    away_goals
    source          (ex.: 'api_football' ou 'manual/unknown')
    match_id        (id do provedor, quando disponível; caso contrário, hash estável)

Requisitos:
    pandas, requests
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import os
import sys
from typing import List, Dict, Any

import pandas as pd
import requests


API_FOOTBALL_BASE = "https://v3.football.api-sports.io/fixtures"


def _iso_date(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")


def _safe_int(x) -> int:
    try:
        return int(x)
    except Exception:
        return 0


def _stable_match_id(row: Dict[str, Any]) -> str:
    """
    Constrói um hash estável quando não houver ID do provedor.
    Usa (date, season, league, home, away).
    """
    key = "|".join(
        [
            str(row.get("date", "")),
            str(row.get("season", "")),
            str(row.get("league", "")),
            str(row.get("home", "")),
            str(row.get("away", "")),
        ]
    )
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def fetch_api_football(since_days: int) -> List[Dict[str, Any]]:
    key = os.environ.get("API_FOOTBALL_KEY", "").strip()
    if not key:
        print("[history][WARN] API_FOOTBALL_KEY ausente; pulando fetch da API-FOOTBALL.")
        return []

    end = dt.date.today()
    start = end - dt.timedelta(days=int(since_days))

    params = {
        "from": _iso_date(start),
        "to": _iso_date(end),
        "timezone": "UTC",
        # status 'FT' => partidas finalizadas; 'AET' e 'PEN' também podem ocorrer:
        "status": "FT-AET-PEN",
    }
    headers = {"x-apisports-key": key}

    print(
        f"[history][INFO] Buscando fixtures finalizados via API-FOOTBALL "
        f"de {params['from']} a {params['to']}."
    )

    try:
        r = requests.get(API_FOOTBALL_BASE, params=params, headers=headers, timeout=25)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[history][ERROR] Falha ao chamar API-FOOTBALL: {e}")
        return []

    resp = data.get("response", [])
    out: List[Dict[str, Any]] = []
    for fx in resp:
        try:
            league = (fx.get("league") or {}).get("name") or ""
            season = (fx.get("league") or {}).get("season") or ""
            fixture = fx.get("fixture") or {}
            teams = fx.get("teams") or {}
            goals = fx.get("goals") or {}

            date_iso = fixture.get("date") or ""
            # Normaliza para YYYY-MM-DD
            try:
                dts = pd.to_datetime(date_iso, utc=True)
                date_short = dts.strftime("%Y-%m-%d")
            except Exception:
                date_short = (date_iso or "")[:10]

            home = (teams.get("home") or {}).get("name") or ""
            away = (teams.get("away") or {}).get("name") or ""
            hg = _safe_int(goals.get("home"))
            ag = _safe_int(goals.get("away"))

            row = {
                "date": date_short,
                "league": league,
                "season": season,
                "home": home,
                "away": away,
                "home_goals": hg,
                "away_goals": ag,
                "source": "api_football",
                "match_id": str((fixture.get("id") or "")),
            }

            if not row["match_id"]:
                row["match_id"] = _stable_match_id(row)

            out.append(row)
        except Exception:
            # Se algum item vier malformado, ignora apenas aquele
            continue

    print(f"[history][INFO] Registros coletados da API-FOOTBALL: {len(out)}")
    return out


def ensure_parent(path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)


def read_existing_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(
            columns=[
                "date",
                "league",
                "season",
                "home",
                "away",
                "home_goals",
                "away_goals",
                "source",
                "match_id",
            ]
        )
    try:
        return pd.read_csv(path, dtype=str)
    except Exception:
        # Se o arquivo estiver corrompido, recomeça do zero (mas não falha)
        print("[history][WARN] results.csv existente não pôde ser lido; recriando.")
        return pd.DataFrame(
            columns=[
                "date",
                "league",
                "season",
                "home",
                "away",
                "home_goals",
                "away_goals",
                "source",
                "match_id",
            ]
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--since_days", type=int, default=14)
    parser.add_argument("--out", type=str, required=True)
    args = parser.parse_args()

    out_path = args.out
    ensure_parent(out_path)

    # 1) coleta (se possível)
    new_rows = fetch_api_football(args.since_days)

    # 2) lê existente
    df_old = read_existing_csv(out_path)

    # 3) concatena e normaliza tipos
    df_new = pd.DataFrame(new_rows)
    all_df = pd.concat([df_old, df_new], ignore_index=True)

    # dtypes básicos
    for c in ["date", "league", "season", "home", "away", "source", "match_id"]:
        if c not in all_df.columns:
            all_df[c] = ""
        all_df[c] = all_df[c].astype(str)

    for c in ["home_goals", "away_goals"]:
        if c not in all_df.columns:
            all_df[c] = 0
        all_df[c] = pd.to_numeric(all_df[c], errors="coerce").fillna(0).astype(int)

    # 4) remove duplicatas
    all_df["dedup_key"] = (
        all_df["match_id"].where(all_df["match_id"].astype(bool), None)
    )
    # Se não houver match_id, usa um hash estável:
    no_id_mask = all_df["dedup_key"].isna()
    if no_id_mask.any():
        subset = all_df.loc[no_id_mask, ["date", "season", "league", "home", "away"]]
        hashes = subset.apply(lambda r: _stable_match_id(r.to_dict()), axis=1)
        all_df.loc[no_id_mask, "dedup_key"] = hashes

    all_df = all_df.drop_duplicates(subset=["dedup_key"]).drop(columns=["dedup_key"])

    # 5) ordena por data
    try:
        all_df["_d"] = pd.to_datetime(all_df["date"], errors="coerce")
    except Exception:
        all_df["_d"] = pd.NaT
    all_df = all_df.sort_values(by=["_d", "league", "home", "away"], ascending=True).drop(columns=["_d"])

    # 6) salva CSV sempre (mesmo vazio com cabeçalho) — não falhar!
    all_df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL, encoding="utf-8")
    print(f"[history][OK] Histórico salvo em: {out_path}  (linhas={len(all_df)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())