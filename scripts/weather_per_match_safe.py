#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
weather_per_match_safe.py (STRICT)

Consulta Open-Meteo por partida usando lat/lon vindos de data/in/matches_source.csv
e grava <OUT_DIR>/weather.csv

Regras:
- Requer colunas: match_id,lat,lon (string/num); falha se ausentes.
- Retentativas com backoff exponencial (p.ex. 3 tentativas).
- Se alguma partida falhar TODAS as tentativas → job falha (exit 17).
"""

import os
import sys
import argparse
import time
import csv
import requests
import pandas as pd


API = "https://api.open-meteo.com/v1/forecast"
TIMEOUT = 20
RETRIES = 3
BACKOFF = 5  # segundos base


def die(msg: str, code: int = 17):
    print(f"##[error]{msg}", file=sys.stderr, flush=True)
    sys.exit(code)


def fetch_weather(lat: float, lon: float) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": ["temperature_2m","apparent_temperature","wind_speed_10m",
                    "wind_gusts_10m","wind_direction_10m","precipitation","relative_humidity_2m",
                    "cloud_cover","surface_pressure"],
        "timezone": "UTC"
    }
    for attempt in range(1, RETRIES+1):
        try:
            r = requests.get(API, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            js = r.json()
            cur = js.get("current", {})
            return {
                "temp_c": cur.get("temperature_2m"),
                "apparent_temp_c": cur.get("apparent_temperature"),
                "wind_speed_kph": (cur.get("wind_speed_10m") or 0) * 3.6,
                "wind_gust_kph": (cur.get("wind_gusts_10m") or 0) * 3.6,
                "wind_dir_deg": cur.get("wind_direction_10m"),
                "precip_mm": cur.get("precipitation"),
                "relative_humidity": cur.get("relative_humidity_2m"),
                "cloud_cover": cur.get("cloud_cover"),
                "pressure_hpa": cur.get("surface_pressure"),
            }
        except Exception as e:
            if attempt == RETRIES:
                raise
            time.sleep(BACKOFF * attempt)
    # inatingível
    return {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "weather.csv")

    if not os.path.isfile(args.infile) or os.path.getsize(args.infile) == 0:
        die(f"Entrada {args.infile} ausente/vazia.")
    src = pd.read_csv(args.infile)
    need = ["match_id","lat","lon"]
    if not all(c in src.columns for c in need):
        die(f"Cabeçalhos ausentes em {args.infile}. Esperado: {need}")

    rows = []
    fails = []
    for _, r in src.iterrows():
        mid = str(r["match_id"])
        try:
            lat = float(r["lat"]); lon = float(r["lon"])
        except Exception:
            fails.append((mid,"lat/lon inválidos"))
            continue
        try:
            wx = fetch_weather(lat, lon)
            wx.update({"match_id": str(mid), "lat": lat, "lon": lon, "weather_source": "open-meteo"})
            rows.append(wx)
        except Exception as e:
            print(f"[weather] ERRO em match_id={mid}: {e}", flush=True)
            fails.append((mid, str(e)))

    if fails:
        # Falha estrita: qualquer partida sem clima derruba o job
        detail = "\n".join([f"match_id={m} | {msg}" for m,msg in fails])
        die(f"Falha ao obter clima para {len(fails)} partida(s):\n{detail}")

    pd.DataFrame(rows).to_csv(out_path, index=False)
    print(f"[weather] OK -> {out_path} (linhas={len(rows)})")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        die(f"Erro inesperado: {e}")