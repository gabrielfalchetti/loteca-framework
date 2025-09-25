#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_weather.py — Open-Meteo real + merge seguro por stadium_id

O que este script faz:
1) Lê config/config.yaml (variáveis/horas a coletar).
2) Lê matches.csv da rodada e stadiums_latlon.csv (lat/lon).
3) Padroniza o campo stadium_id nas duas tabelas (string, sem espaços, minúsculo).
4) Faz merge sem erro de tipo.
5) Para cada partida com lat/lon, consulta a Open-Meteo (hourly) em uma
   janela de 24h do dia do jogo e agrega médias dos campos definidos no config.
6) Salva um CSV por partida em data/processed/weather_<rodada>.csv
   com colunas: match_id, stadium_id, <métricas de clima...>.
"""

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
import yaml

# -------------------- utilidades --------------------

def load_cfg() -> dict:
    with open("config/config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def normalize_id(x) -> str:
    """
    Transforma qualquer ID em string "normalizada" para join:
    - NaN -> "unknown"
    - string: strip, baixa, troca espaços por "_"
    - numérico: vira string sem ".0"
    """
    if pd.isna(x):
        return "unknown"
    s = str(x).strip()
    # remove .0 comum de floats vindos de CSV
    if s.endswith(".0") and s.replace(".","",1).isdigit():
        s = s[:-2]
    s = s.lower().replace(" ", "_")
    return s if s else "unknown"

def parse_kickoff_date(kickoff_utc: str) -> str:
    """
    Recebe '2025-09-20T19:00:00Z' ou '2025-09-20 19:00:00+00:00' e devolve 'YYYY-MM-DD'.
    Se falhar, devolve apenas a parte antes do espaço.
    """
    if not isinstance(kickoff_utc, str):
        return ""
    try:
        # tenta ISO completo
        return pd.to_datetime(kickoff_utc, utc=True).strftime("%Y-%m-%d")
    except Exception:
        # fallback: pega só a parte de data
        return kickoff_utc.split("T")[0].split(" ")[0]

def open_meteo_hourly(lat: float, lon: float, hourly_vars: List[str], date_iso: str) -> Optional[dict]:
    """
    Chama a Open-Meteo para o dia (UTC) do jogo e retorna JSON com arrays hourly.
    Docs: https://open-meteo.com/en/docs
    """
    base = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(hourly_vars),
        "start_date": date_iso,
        "end_date": date_iso,
        "timezone": "UTC",
    }
    r = requests.get(base, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def aggregate_hourly(payload: dict, hourly_vars: List[str]) -> Dict[str, float]:
    """
    Recebe JSON da Open-Meteo e calcula a MÉDIA do dia para cada variável pedida.
    Se a variável não existir no retorno, devolve NaN.
    """
    out = {}
    if not payload or "hourly" not in payload:
        for v in hourly_vars:
            out[v] = float("nan")
        return out

    hourly = payload["hourly"]
    n = len(hourly.get("time", []))
    for v in hourly_vars:
        vals = hourly.get(v, [])
        if not vals or len(vals) != n:
            out[v] = float("nan")
        else:
            # alguns campos vêm como inteiros; garantir float
            try:
                series = pd.to_numeric(pd.Series(vals), errors="coerce")
                out[v] = float(series.mean())
            except Exception:
                out[v] = float("nan")
    return out

# -------------------- pipeline --------------------

def main(rodada: str):
    cfg = load_cfg()

    # caminhos
    matches_path = cfg["paths"]["matches_csv"].replace("${rodada}", rodada)
    stadiums_path = cfg["paths"]["stadiums_csv"]
    weather_out = cfg["paths"]["weather_out"].replace("${rodada}", rodada)

    Path(weather_out).parent.mkdir(parents=True, exist_ok=True)

    # ler dados
    if not Path(matches_path).exists():
        raise SystemExit(f"[ERRO] matches.csv não encontrado: {matches_path}")
    if not Path(stadiums_path).exists():
        raise SystemExit(f"[ERRO] stadiums_latlon.csv não encontrado: {stadiums_path}")

    matches = pd.read_csv(matches_path)
    stadiums = pd.read_csv(stadiums_path)

    # normalizar as chaves para STRING compatível (evita o erro float64 vs object)
    matches["stadium_id_norm"] = matches.get("stadium_id", pd.Series([None]*len(matches))).apply(normalize_id)
    stadiums["stadium_id_norm"] = stadiums.get("stadium_id", pd.Series([None]*len(stadiums))).apply(normalize_id)

    # checar colunas de lat/lon
    for col in ("lat", "lon"):
        if col not in stadiums.columns:
            raise SystemExit(f"[ERRO] Coluna '{col}' ausente em {stadiums_path}. Esperado: stadium_id, stadium_name, lat, lon")

    # merge seguro
    df = matches.merge(
        stadiums[["stadium_id_norm", "lat", "lon"]],
        on="stadium_id_norm",
        how="left",
        validate="m:1",  # cada estádio deve aparecer uma vez (ou poucas) no mapeamento
    )

    # variáveis meteorológicas do config
    hourly_vars = cfg.get("weather", {}).get("hourly", [
        "temperature_2m", "precipitation_probability", "precipitation", "wind_speed_10m"
    ])

    # coletar clima por partida
    rows = []
    for rec in df.to_dict("records"):
        match_id = rec.get("match_id")
        stid = rec.get("stadium_id_norm", "unknown")
        lat = rec.get("lat")
        lon = rec.get("lon")
        kickoff = rec.get("kickoff_utc")

        date_iso = parse_kickoff_date(kickoff)
        if not date_iso or pd.isna(lat) or pd.isna(lon):
            # sem lat/lon ou data: registra NaN
            agg = {v: float("nan") for v in hourly_vars}
        else:
            try:
                payload = open_meteo_hourly(float(lat), float(lon), hourly_vars, date_iso)
                agg = aggregate_hourly(payload, hourly_vars)
            except Exception as e:
                print(f"[WARN] Open-Meteo falhou para match_id={match_id} ({lat},{lon}) em {date_iso}: {e}")
                agg = {v: float("nan") for v in hourly_vars}

        row = {
            "match_id": match_id,
            "stadium_id": stid,
            **{f"{k}": agg.get(k, float("nan")) for k in hourly_vars},
        }
        rows.append(row)

    out = pd.DataFrame(rows)

    # renomeações friendly (opcional): manter compatível com seu join_features.py
    rename_map = {
        "precipitation_probability": "precipitation_probability",
        "wind_speed_10m": "wind_speed_10m",
        "precipitation": "precipitation",
        "temperature_2m": "temperature_2m",
    }
    out = out.rename(columns=rename_map)

    out.to_csv(weather_out, index=False)
    print(f"[OK] weather → {weather_out}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Ex.: 2025-09-20_21")
    args = ap.parse_args()
    main(args.rodada)
