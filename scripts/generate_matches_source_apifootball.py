#!/usr/bin/env python3
import os, sys, json, argparse, pathlib, datetime
import requests
import pandas as pd

API_HOST = "api-football-v1.p.rapidapi.com"

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="ex.: 2025-10-04_1214 (usa a data do começo)")
    ap.add_argument("--season", required=True, help="ex.: 2025")
    ap.add_argument("--leagues", required=True, help="IDs separados por vírgula, ex.: 71,72")
    ap.add_argument("--limit", type=int, default=0, help="limite opcional de partidas")
    ap.add_argument("--debug", action="store_true")
    return ap.parse_args()

def rodada_to_date(rodada: str) -> str:
    # pega o prefixo YYYY-MM-DD antes de '_' (se tiver)
    d = rodada.split("_")[0]
    # valida formato simples
    datetime.date.fromisoformat(d)
    return d

def rapid_get(path: str, params: dict, key: str, debug=False):
    url = f"https://{API_HOST}/v3/{path}"
    headers = {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": API_HOST
    }
    if debug:
        print(f"[apifoot/generate][DEBUG] GET {url} params={params}")
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code == 403:
        print(f"[apifoot/generate] ERRO 403: verifique seu plano/assinatura na RapidAPI.")
    r.raise_for_status()
    return r.json()

def main():
    args = parse_args()
    key = os.environ.get("X_RAPIDAPI_KEY")
    if not key:
        print("[apifoot/generate] X_RAPIDAPI_KEY ausente. Saindo.")
        sys.exit(0)

    date_str = rodada_to_date(args.rodada)
    leagues = [x.strip() for x in args.leagues.split(",") if x.strip()]
    out_dir_in = pathlib.Path(f"data/in/{args.rodada}")
    out_dir_in.mkdir(parents=True, exist_ok=True)
    rows = []

    for lg in leagues:
        try:
            data = rapid_get("fixtures",
                             {"date": date_str, "league": lg, "season": args.season},
                             key, debug=args.debug)
        except Exception as e:
            print(f"[apifoot/generate] Falha lig {lg}: {e}")
            continue

        for item in data.get("response", []):
            home = item["teams"]["home"]["name"]
            away = item["teams"]["away"]["name"]
            match_key = f"{home}__vs__{away}".lower()
            rows.append({"team_home": home, "team_away": away, "match_key": match_key})

    if args.limit and len(rows) > args.limit:
        rows = rows[:args.limit]

    if not rows:
        print(f"[apifoot/generate] Nenhum fixture retornado para {date_str} nas ligas {leagues}.")
        sys.exit(0)

    df = pd.DataFrame(rows).drop_duplicates(subset=["match_key"])
    out_path = out_dir_in / "matches_source.csv"
    df.to_csv(out_path, index=False)
    print(f"[apifoot/generate] OK -> {out_path} ({len(df)} linhas)")

if __name__ == "__main__":
    main()