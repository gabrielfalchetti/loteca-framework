#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, pandas as pd, requests, yaml
from pathlib import Path

def load_cfg():
    return yaml.safe_load(open("config/config.yaml","r",encoding="utf-8"))

def normalize_id(x):
    if pd.isna(x): return "unknown"
    s = str(x).strip()
    if s.endswith(".0") and s.replace(".","",1).isdigit():
        s = s[:-2]
    return (s.lower().replace(" ","_") or "unknown")

def parse_date(kickoff_utc: str) -> str:
    if not isinstance(kickoff_utc, str): return ""
    try:
        return pd.to_datetime(kickoff_utc, utc=True).strftime("%Y-%m-%d")
    except Exception:
        return kickoff_utc.split("T")[0].split(" ")[0]

def open_meteo(lat, lon, hourly_vars, date_iso):
    base = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": ",".join(hourly_vars),
        "start_date": date_iso, "end_date": date_iso,
        "timezone": "UTC",
    }
    r = requests.get(base, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def aggregate_hourly(payload, hourly_vars):
    out = {}
    if not payload or "hourly" not in payload:
        for v in hourly_vars: out[v] = float("nan"); return out
    hourly = payload["hourly"]; n = len(hourly.get("time", []))
    for v in hourly_vars:
        vals = hourly.get(v, [])
        if not vals or len(vals)!=n: out[v]=float("nan"); continue
        out[v] = float(pd.to_numeric(pd.Series(vals), errors="coerce").mean())
    return out

def main(rodada: str):
    cfg = load_cfg()
    matches_path = cfg["paths"]["matches_csv"].replace("${rodada}", rodada)
    stadiums_path = cfg["paths"]["stadiums_csv"]
    weather_out = cfg["paths"]["weather_out"].replace("${rodada}", rodada)
    Path(weather_out).parent.mkdir(parents=True, exist_ok=True)

    if not Path(matches_path).exists(): raise SystemExit(f"[ERRO] matches.csv não encontrado: {matches_path}")
    if not Path(stadiums_path).exists(): raise SystemExit(f"[ERRO] stadiums_latlon.csv não encontrado: {stadiums_path}")

    matches = pd.read_csv(matches_path)
    stadiums = pd.read_csv(stadiums_path)
    matches["stadium_id_norm"]  = matches.get("stadium_id", pd.Series([None]*len(matches))).apply(normalize_id)
    stadiums["stadium_id_norm"] = stadiums.get("stadium_id", pd.Series([None]*len(stadiums))).apply(normalize_id)

    for col in ("lat","lon"):
        if col not in stadiums.columns:
            raise SystemExit(f"[ERRO] Coluna '{col}' ausente em {stadiums_path}")

    df = matches.merge(stadiums[["stadium_id_norm","lat","lon"]], on="stadium_id_norm", how="left", validate="m:1")

    hourly_vars = cfg.get("weather", {}).get("hourly", ["temperature_2m","precipitation_probability","precipitation","wind_speed_10m"])

    rows=[]
    for rec in df.to_dict("records"):
        mid = rec.get("match_id"); lat = rec.get("lat"); lon = rec.get("lon"); kickoff = rec.get("kickoff_utc")
        date_iso = parse_date(kickoff)
        if not date_iso or pd.isna(lat) or pd.isna(lon):
            agg = {v: float("nan") for v in hourly_vars}
        else:
            try:
                payload = open_meteo(float(lat), float(lon), hourly_vars, date_iso)
                agg = aggregate_hourly(payload, hourly_vars)
            except Exception as e:
                print(f"[WARN] Open-Meteo falhou p/ match_id={mid}: {e}")
                agg = {v: float("nan") for v in hourly_vars}
        rows.append({"match_id": mid, **agg})

    out = pd.DataFrame(rows)
    out.to_csv(weather_out, index=False)
    print(f"[OK] weather → {weather_out}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
