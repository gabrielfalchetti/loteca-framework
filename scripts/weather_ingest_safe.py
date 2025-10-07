#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coleta previsão do tempo por cidade do mandante (OpenWeather) e grava OUT_DIR/weather.csv.
Falha sem chave/entrada/retorno.

Uso:
  python scripts/weather_ingest_safe.py --out-dir data/out/<ID> --debug
Requisitos:
  WEATHER_API_KEY no env (OpenWeatherMap)
  data/in/matches_source.csv com 'home' (nome da cidade/time -> simplificação)
"""

import argparse, csv, os, sys, time
try:
    import requests
except Exception as e:
    print(f"[weather] ERRO: 'requests' indisponível: {e}", file=sys.stderr); sys.exit(8)

def load_matches():
    p="data/in/matches_source.csv"
    if not os.path.isfile(p):
        print(f"::error::[weather] {p} ausente.", file=sys.stderr); sys.exit(8)
    rows=[]
    with open(p,"r",encoding="utf-8") as f:
        rd=csv.DictReader(f)
        for c in ("match_id","home","away"):
            if c not in rd.fieldnames:
                print(f"::error::[weather] cabeçalho '{c}' ausente", file=sys.stderr); sys.exit(8)
        for r in rd:
            if r.get("home") and r.get("away"):
                rows.append(r)
    if not rows:
        print("::error::[weather] nenhuma linha válida em matches_source.csv", file=sys.stderr); sys.exit(8)
    return rows

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--debug", action="store_true")
    args=ap.parse_args()

    out_dir=args.out_dir
    os.makedirs(out_dir, exist_ok=True)
    out_csv=os.path.join(out_dir,"weather.csv")

    key=os.getenv("WEATHER_API_KEY","").strip()
    if not key:
        print("::error::[weather] WEATHER_API_KEY não definido.", file=sys.stderr); sys.exit(8)

    matches=load_matches()
    base="https://api.openweathermap.org/data/2.5/weather"
    collected=[]
    hits=0
    for r in matches:
        city = r["home"]
        try:
            resp=requests.get(base, params={"q": city, "appid": key, "units":"metric","lang":"pt"}, timeout=15)
            resp.raise_for_status()
            js=resp.json()
            temp = (js.get("main") or {}).get("temp", "")
            cond = (js.get("weather") or [{}])[0].get("description","")
            collected.append({
                "match_id": r["match_id"], "home": r["home"], "away": r["away"],
                "temp_c": temp, "condition": cond
            })
            hits+=1
            time.sleep(0.3)
        except Exception as e:
            print(f"[weather] aviso: falha ao consultar {city}: {e}", file=sys.stderr)

    if hits==0:
        print("::error::[weather] Nenhum clima coletado (hits=0).", file=sys.stderr); sys.exit(8)

    with open(out_csv,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=["match_id","home","away","temp_c","condition"])
        w.writeheader()
        for r in collected: w.writerow(r)

    if not os.path.isfile(out_csv) or os.path.getsize(out_csv)==0:
        print("::error::[weather] weather.csv não gerado.", file=sys.stderr); sys.exit(8)

    print(f"[weather] OK -> {out_csv} linhas={len(collected)}")

if __name__=="__main__":
    main()