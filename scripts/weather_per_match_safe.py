#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
weather_per_match_safe.py
Coleta clima por partida usando lat/lon do arquivo data/in/matches_source.csv
e salva em <OUT_DIR>/weather.csv.

Regras:
- Falha dura se:
  * arquivo de entrada não existir
  * colunas obrigatórias ausentes: match_id,lat,lon
  * lat/lon inválidos
  * alguma requisição de clima falhar
- Não inventa dados. Se a API falhar, o script sai com código != 0.
- Usa Open-Meteo (sem necessidade de API key).

Uso:
  python scripts/weather_per_match_safe.py --in data/in/matches_source.csv --out-dir data/out/<ID> [--debug]
"""

import argparse
import csv
import json
import math
import os
import sys
import time
from typing import Dict, Any, List, Tuple

import requests

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

REQUIRED_INPUT_COLS = ["match_id", "lat", "lon"]
OUTPUT_COLS = [
    "match_id", "lat", "lon",
    "temp_c", "apparent_temp_c",
    "wind_speed_kph", "wind_gust_kph", "wind_dir_deg",
    "precip_mm", "precip_prob", "relative_humidity",
    "cloud_cover", "pressure_hpa",
    "weather_source", "fetched_at_utc"
]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", required=True, help="CSV com partidas (match_id, lat, lon)")
    ap.add_argument("--out-dir", dest="out_dir", required=True, help="Diretório de saída da rodada")
    ap.add_argument("--debug", action="store_true")
    return ap.parse_args()


def eprint(*a, **k):
    print(*a, file=sys.stderr, **k)


def read_matches_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Entrada {path} não encontrada.")
    with open(path, "r", newline="", encoding="utf-8") as fh:
        r = csv.DictReader(fh)
        header = [h.strip() for h in (r.fieldnames or [])]
        missing = [c for c in REQUIRED_INPUT_COLS if c.lower() not in [h.lower() for h in header]]
        if missing:
            raise ValueError(f"Cabeçalhos ausentes: {missing}. Precisamos de {REQUIRED_INPUT_COLS}")

        # normaliza chaves para lower e tira espaços
        rows = [{(k or "").strip().lower(): (v or "").strip() for k, v in row.items()} for row in r]

    # mapeia para chaves canônicas (exatamente REQUIRED_INPUT_COLS + quaisquer outras, mas guardamos as essenciais)
    norm: List[Dict[str, str]] = []
    for row in rows:
        item = {
            "match_id": row.get("match_id", ""),
            "lat": row.get("lat", ""),
            "lon": row.get("lon", "")
        }
        norm.append(item)
    if len(norm) == 0:
        raise ValueError("Nenhum jogo listado no CSV de entrada.")
    return norm


def valid_coord(v: str) -> bool:
    try:
        x = float(v)
    except Exception:
        return False
    # latitude: -90..90, longitude: -180..180 (aqui só checamos valor numérico; faixa será checada em run)
    return math.isfinite(x)


def kmh_from_ms(ms: float) -> float:
    return ms * 3.6 if (ms is not None) else None


def fetch_open_meteo(lat: float, lon: float, debug: bool = False) -> Dict[str, Any]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": ",".join([
            "temperature_2m",
            "apparent_temperature",
            "wind_speed_10m",
            "wind_gusts_10m",
            "wind_direction_10m",
            "precipitation",
            "relative_humidity_2m",
            "cloud_cover",
            "surface_pressure"
        ]),
        "timezone": "UTC",
    }
    if debug:
        eprint(f"[weather] GET {OPEN_METEO_URL} {params}")
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"Open-Meteo falhou HTTP {resp.status_code}: {resp.text[:200]}")
    try:
        data = resp.json()
    except json.JSONDecodeError:
        raise RuntimeError("Resposta Open-Meteo inválida (JSON).")
    if "current" not in data:
        raise RuntimeError("Open-Meteo sem bloco 'current'.")

    cur = data["current"]
    out = {
        "temp_c": cur.get("temperature_2m"),
        "apparent_temp_c": cur.get("apparent_temperature"),
        "wind_speed_kph": kmh_from_ms(cur.get("wind_speed_10m")),
        "wind_gust_kph": kmh_from_ms(cur.get("wind_gusts_10m")),
        "wind_dir_deg": cur.get("wind_direction_10m"),
        "precip_mm": cur.get("precipitation"),
        "relative_humidity": cur.get("relative_humidity_2m"),
        "cloud_cover": cur.get("cloud_cover"),
        "pressure_hpa": cur.get("surface_pressure"),
        "weather_source": "open-meteo",
        "fetched_at_utc": cur.get("time"),
    }
    return out


def ensure_output_dir(path: str):
    os.makedirs(path, exist_ok=True)


def main():
    args = parse_args()
    infile = args.infile
    out_dir = args.out_dir
    debug = args.debug

    ensure_output_dir(out_dir)
    out_path = os.path.join(out_dir, "weather.csv")

    matches = read_matches_csv(infile)

    results: List[Dict[str, Any]] = []
    for i, m in enumerate(matches, start=1):
        mid = m.get("match_id", "").strip()
        lat_s = m.get("lat", "").strip()
        lon_s = m.get("lon", "").strip()

        if not mid:
            raise ValueError(f"Linha {i}: match_id vazio.")
        if not (valid_coord(lat_s) and valid_coord(lon_s)):
            raise ValueError(f"match_id={mid}: lat/lon inválidos. Recebido lat='{lat_s}' lon='{lon_s}'.")

        lat = float(lat_s)
        lon = float(lon_s)
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            raise ValueError(f"match_id={mid}: lat/lon fora da faixa. lat={lat} lon={lon}")

        # Respeita API pública (pode ser bom um pequeno intervalo entre chamadas)
        try:
            wx = fetch_open_meteo(lat, lon, debug=debug)
        except Exception as e:
            eprint(f"[weather] ERRO em match_id={mid}: {e}")
            # Falha dura: não inventar dados
            raise

        row = {
            "match_id": mid,
            "lat": lat,
            "lon": lon,
            **wx
        }
        # Checagem de sanidade mínima (sem NaN/None nas chaves mais importantes)
        critical = ["temp_c", "wind_speed_kph", "precip_mm"]
        if any(row.get(k) is None for k in critical):
            raise RuntimeError(f"[weather] Dados incompletos para match_id={mid}: { {k: row.get(k) for k in critical} }")

        results.append(row)
        if debug:
            eprint(f"[weather] OK match_id={mid}: temp={row['temp_c']}C wind={row['wind_speed_kph']}km/h precip={row['precip_mm']}mm")
        # pausa leve para ser amigável com a API pública
        time.sleep(0.3)

    # Escreve saída
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=OUTPUT_COLS)
        w.writeheader()
        for r in results:
            w.writerow({k: r.get(k, "") for k in OUTPUT_COLS})

    if debug:
        eprint(f"[weather] Salvo {len(results)} linhas em {out_path}")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        eprint(f"[weather] FALHA: {exc}")
        sys.exit(1)