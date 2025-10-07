#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
weather_snapshot_safe.py
Consulta snapshot de clima real via Open-Meteo quando LAT/LON são fornecidos.
- Gera SEMPRE data/out/<ID>/weather.csv (com cabeçalho).
- Se LAT/LON ausentes ou erro de rede, grava apenas cabeçalho e avisa no log (sem dados sintéticos).
Uso:
  python scripts/weather_snapshot_safe.py --out-dir data/out/<RODADA_ID> --lat <LAT> --lon <LON>
"""

import os
import sys
import csv
import argparse
from typing import Dict, Any

try:
    import requests
except Exception:
    requests = None

def parse_args():
    p = argparse.ArgumentParser(description="Snapshot de clima (Open-Meteo) com tolerância a falhas")
    p.add_argument("--out-dir", required=True, help="Diretório de saída data/out/<RODADA_ID>")
    p.add_argument("--lat", default="", help="Latitude (ex: -23.55)")
    p.add_argument("--lon", default="", help="Longitude (ex: -46.63)")
    p.add_argument("--timeout", type=int, default=12, help="Timeout da requisição (s)")
    return p.parse_args()

def _safe_get(d: Dict[str, Any], k: str, default=None):
    try:
        v = d.get(k, default)
        return v if v is not None else default
    except Exception:
        return default

def fetch_weather(lat: str, lon: str, timeout: int) -> Dict[str, Any]:
    """
    Consulta Open-Meteo. Retorna dicionário com campos de interesse.
    Se falhar, retorna {} (sem inventar).
    """
    if requests is None:
        return {}

    if not lat or not lon:
        return {}

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": "true",
        "hourly": "relative_humidity_2m,precipitation,cloud_cover,wind_speed_10m",
        "forecast_days": 1,
        "timezone": "UTC",
    }
    try:
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code != 200:
            print(f"[weather] aviso: HTTP {r.status_code} de Open-Meteo", file=sys.stderr)
            return {}
        data = r.json() if r.content else {}

        current = _safe_get(data, "current_weather", {}) or {}
        hourly = _safe_get(data, "hourly", {}) or {}

        # Monta um snapshot simples
        out = {
            "lat": lat,
            "lon": lon,
            "time": _safe_get(current, "time", ""),
            "temperature_c": _safe_get(current, "temperature", ""),
            "windspeed_kmh": _safe_get(current, "windspeed", ""),
            "winddir_deg": _safe_get(current, "winddirection", ""),
            "weathercode": _safe_get(current, "weathercode", ""),
        }

        # Alguns médios agregados (se tiver série horária)
        # Não inventa: se não existir, fica vazio.
        def _first(arr):
            try:
                return arr[0]
            except Exception:
                return ""

        out["humidity_pct"] = _first(_safe_get(hourly, "relative_humidity_2m", []))
        out["precip_mm"]   = _first(_safe_get(hourly, "precipitation", []))
        out["cloud_pct"]   = _first(_safe_get(hourly, "cloud_cover", []))
        out["wind10m_ms"]  = _first(_safe_get(hourly, "wind_speed_10m", []))
        return out
    except Exception as e:
        print(f"[weather] aviso: falha ao consultar Open-Meteo: {e}", file=sys.stderr)
        return {}

def write_csv(path: str, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "lat","lon","time","temperature_c",
            "windspeed_kmh","winddir_deg","weathercode",
            "humidity_pct","precip_mm","cloud_pct","wind10m_ms"
        ])
        for r in rows:
            w.writerow([
                r.get("lat",""), r.get("lon",""), r.get("time",""),
                r.get("temperature_c",""), r.get("windspeed_kmh",""),
                r.get("winddir_deg",""), r.get("weathercode",""),
                r.get("humidity_pct",""), r.get("precip_mm",""),
                r.get("cloud_pct",""), r.get("wind10m_ms",""),
            ])

def main():
    args = parse_args()
    out_csv = os.path.join(args.out_dir, "weather.csv")

    # Caso não tenha lat/lon, não inventa dado. Apenas cabeçalho + aviso.
    if not args.lat or not args.lon:
        print("::warning::[weather] WEATHER_LAT/LON não fornecidos — gerando apenas cabeçalho (sem dados).", file=sys.stderr)
        write_csv(out_csv, [])
        print(f"[weather] OK (sem dados). Arquivo: {out_csv}")
        sys.exit(0)

    snap = fetch_weather(args.lat, args.lon, args.timeout)
    if snap:
        write_csv(out_csv, [snap])
        print(f"[weather] OK (dados reais). Arquivo: {out_csv}")
    else:
        # ainda assim gera o arquivo com cabeçalho (sem dados)
        print("::warning::[weather] Não foi possível obter clima real — gerando apenas cabeçalho.", file=sys.stderr)
        write_csv(out_csv, [])
        print(f"[weather] OK (sem dados). Arquivo: {out_csv}")

if __name__ == "__main__":
    main()