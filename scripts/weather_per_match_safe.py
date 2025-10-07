#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
weather_per_match_safe.py
- Lê os jogos em data/in/matches_source.csv (match_id,home,away,source).
- Cruza com data/static/stadium_coords.csv (team,lat,lon,stadium,city).
- Consulta Open-Meteo por (lat,lon) e salva 1 linha por jogo em <OUT_DIR>/weather_per_match.csv.
- Sem dados sintéticos. Se --strict e faltar coordenada de algum time, sai com erro !=0.
Uso:
  python scripts/weather_per_match_safe.py --out-dir data/out/<ID> \
      --matches data/in/matches_source.csv \
      --coords data/static/stadium_coords.csv \
      --strict
"""
import os, sys, csv, argparse, time
from typing import Dict, Tuple, Optional

try:
    import requests
except Exception:
    requests = None

HEAD_MATCHES = ["match_id","home","away","source"]
HEAD_COORDS  = ["team","lat","lon","stadium","city"]
HEAD_OUT     = [
    "match_id","home","away","team_lat","team_lon","time_utc",
    "temperature_c","windspeed_kmh","winddir_deg","weathercode",
    "humidity_pct","precip_mm","cloud_pct","wind10m_ms"
]

def die(msg: str, code: int = 1):
    print(f"::error::{msg}", file=sys.stderr)
    sys.exit(code)

def warn(msg: str):
    print(f"::warning::{msg}", file=sys.stderr)

def parse_args():
    p = argparse.ArgumentParser(description="Clima granular por jogo (Open-Meteo), sem dados fictícios.")
    p.add_argument("--out-dir", required=True, help="Diretório de saída (ex: data/out/<ID>)")
    p.add_argument("--matches", default="data/in/matches_source.csv", help="CSV de partidas (default: data/in/matches_source.csv)")
    p.add_argument("--coords", default="data/static/stadium_coords.csv", help="CSV de coordenadas (default: data/static/stadium_coords.csv)")
    p.add_argument("--timeout", type=int, default=12, help="Timeout HTTP (s)")
    p.add_argument("--strict", action="store_true", help="Falha se faltar coordenada para QUALQUER jogo")
    p.add_argument("--sleep", type=float, default=0.5, help="Sleep entre chamadas (evitar rate-limit)")
    return p.parse_args()

def read_csv(path: str) -> list:
    if not os.path.isfile(path):
        die(f"Arquivo obrigatório não encontrado: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        sniffer = csv.Sniffer()
        sample = f.read(2048)
        f.seek(0)
        dialect = sniffer.sniff(sample) if sample else csv.excel
        r = csv.DictReader(f, dialect=dialect)
        rows = [dict((k or "").strip(): (v or "").strip() for k,v in row.items()) for row in r]
    return rows

def ensure_header(path: str, needed: list):
    # Retorna True se tem todos os campos
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            die(f"{path} está vazio.")
    header = [ (h or "").strip().lower() for h in header ]
    miss = [c for c in needed if c.lower() not in header]
    if miss:
        die(f"{path} sem colunas obrigatórias: {miss}")
    return True

def load_coords_map(path: str) -> Dict[str, Tuple[str,str,dict]]:
    ensure_header(path, HEAD_COORDS)
    rows = read_csv(path)
    mp = {}
    for r in rows:
        team = (r.get("team","") or "").strip().lower()
        lat  = (r.get("lat","")  or "").strip()
        lon  = (r.get("lon","")  or "").strip()
        extra = {"stadium": r.get("stadium",""), "city": r.get("city","")}
        if team:
            mp[team] = (lat, lon, extra)
    return mp

def fetch_weather(lat: str, lon: str, timeout: int) -> Optional[dict]:
    if not requests:
        warn("[weather] 'requests' não disponível no ambiente.")
        return None
    if not lat or not lon:
        return None
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat, "longitude": lon,
        "current_weather": "true",
        "hourly": "relative_humidity_2m,precipitation,cloud_cover,wind_speed_10m",
        "forecast_days": 1, "timezone": "UTC",
    }
    try:
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code != 200:
            warn(f"[weather] HTTP {r.status_code} para lat={lat}, lon={lon}")
            return None
        data = r.json()
        current = (data or {}).get("current_weather") or {}
        hourly  = (data or {}).get("hourly") or {}
        def _first(arr):
            try: return arr[0]
            except Exception: return ""
        return {
            "time_utc": current.get("time",""),
            "temperature_c": current.get("temperature",""),
            "windspeed_kmh": current.get("windspeed",""),
            "winddir_deg": current.get("winddirection",""),
            "weathercode": current.get("weathercode",""),
            "humidity_pct": _first(hourly.get("relative_humidity_2m", [])),
            "precip_mm":   _first(hourly.get("precipitation", [])),
            "cloud_pct":   _first(hourly.get("cloud_cover", [])),
            "wind10m_ms":  _first(hourly.get("wind_speed_10m", [])),
        }
    except Exception as e:
        warn(f"[weather] falha ao consultar Open-Meteo: {e}")
        return None

def write_out(path: str, rows: list):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEAD_OUT)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k,"") for k in HEAD_OUT})

def main():
    args = parse_args()

    # matches input
    ensure_header(args.matches, HEAD_MATCHES)
    matches = read_csv(args.matches)

    # coords mapping
    coords_map = load_coords_map(args.coords)

    # valida coordenadas
    missing = []
    resolved = []
    for m in matches:
        home = (m.get("home","") or "").strip()
        away = (m.get("away","") or "").strip()
        if not home or not away or not m.get("match_id"):
            missing.append(m.get("match_id","<sem id>"))
            continue
        home_k = home.lower().strip()
        away_k = away.lower().strip()
        h_info = coords_map.get(home_k)
        a_info = coords_map.get(away_k)
        if not h_info or not h_info[0] or not h_info[1]:
            missing.append(f"{m['match_id']}: {home} (lat/lon ausentes)")
        if not a_info or not a_info[0] or not a_info[1]:
            missing.append(f"{m['match_id']}: {away} (lat/lon ausentes)")
        resolved.append((m, h_info, a_info))

    if missing:
        if args.strict:
            die(f"Coordenadas faltando para jogos/equipes: {missing}", code=31)
        else:
            warn(f"Coordenadas faltando (serão IGNORADOS no clima): {missing}")

    out_rows = []
    for (m, h_info, a_info) in resolved:
        # Coleta somente para o mandante (clima do estádio do mandante)
        if not h_info or not h_info[0] or not h_info[1]:
            # se não tem coordenada do mandante, pula (em modo não estrito)
            continue
        lat, lon, _extra = h_info
        wx = fetch_weather(lat, lon, args.timeout) or {}
        out_rows.append({
            "match_id": m["match_id"],
            "home": m["home"],
            "away": m["away"],
            "team_lat": lat, "team_lon": lon,
            "time_utc": wx.get("time_utc",""),
            "temperature_c": wx.get("temperature_c",""),
            "windspeed_kmh": wx.get("windspeed_kmh",""),
            "winddir_deg": wx.get("winddir_deg",""),
            "weathercode": wx.get("weathercode",""),
            "humidity_pct": wx.get("humidity_pct",""),
            "precip_mm": wx.get("precip_mm",""),
            "cloud_pct": wx.get("cloud_pct",""),
            "wind10m_ms": wx.get("wind10m_ms",""),
        })
        time.sleep(args.sleep)

    out_csv = os.path.join(args.out_dir, "weather_per_match.csv")
    write_out(out_csv, out_rows)
    if not out_rows:
        warn("[weather] Nenhum jogo teve clima coletado (provável falta de coordenadas).")
    print(f"[weather] OK. Arquivo gerado: {out_csv} (linhas={len(out_rows)})")

if __name__ == "__main__":
    main()