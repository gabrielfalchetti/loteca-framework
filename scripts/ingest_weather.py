#!/usr/bin/env python3
import argparse, pandas as pd, requests, time, yaml
from pathlib import Path

BASE_URL = "https://api.open-meteo.com/v1/forecast"

def load_cfg():
    with open("config/config.yaml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def fetch_weather(lat, lon, hourly, tz, past_days=0, forecast_days=5):
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": ",".join(hourly),
        "timezone": tz,
        "past_days": past_days,
        "forecast_days": forecast_days
    }
    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def main(rodada):
    cfg = load_cfg()
    matches = pd.read_csv(cfg["paths"]["matches_csv"].replace("${rodada}", rodada))
    stadiums = pd.read_csv(cfg["paths"]["stadiums_csv"])
    df = matches.merge(stadiums, on="stadium_id", how="left")

    rows = []
    for m in df.to_dict("records"):
        j = fetch_weather(
            m["lat"], m["lon"],
            cfg["weather"]["hourly"],
            cfg["timezone"],
            cfg["weather"]["past_days"],
            cfg["weather"]["forecast_days"]
        )
        hourly = j.get("hourly", {})
        times = hourly.get("time", [])
        series = {k: hourly.get(k, []) for k in cfg["weather"]["hourly"]}
        for idx, t in enumerate(times):
            row = {"match_id": m["match_id"], "time": t}
            for k in cfg["weather"]["hourly"]:
                vals = series.get(k, [])
                row[k] = vals[idx] if idx < len(vals) else None
            rows.append(row)
        time.sleep(0.3)

    out = pd.DataFrame(rows)
    out_path = cfg["paths"]["weather_out"].replace("${rodada}", rodada)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"[OK] clima → {out_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
