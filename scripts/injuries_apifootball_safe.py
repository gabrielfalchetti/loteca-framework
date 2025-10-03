#!/usr/bin/env python3
import os, sys, logging, requests, pandas as pd

API_HOST = "api-football-v1.p.rapidapi.com"
BASE_URL = f"https://{API_HOST}/v3"

def http_get(path, params, key, debug=False):
    headers = {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": API_HOST,
    }
    url = f"https://{API_HOST}/v3/{path.lstrip('/')}"
    if debug:
        logging.info(f"[inj] GET {url} params={params}")
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    debug = os.getenv("DEBUG","").lower()=="true"
    if debug:
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    key = os.getenv("X_RAPIDAPI_KEY","")
    if not key:
        print("[inj] SKIP: X_RAPIDAPI_KEY ausente.")
        return

    out_dir = os.path.join("data","out", os.getenv("RODADA",""))
    if not out_dir.strip():
        out_dir = os.path.join("data","out","_")
    os.makedirs(out_dir, exist_ok=True)

    # API-Football injuries precisa de liga/temporada/time ou data.
    # Para simplificar, deixamos 3 chamadas ilustrativas com resultados agregados (falha -> ignora).
    frames = []
    for league_id in [71,72]:
        try:
            js = http_get("injuries", {"league": league_id, "season": int(os.getenv("SEASON","2025"))}, key, debug)
            for it in js.get("response", []):
                t = it.get("team", {})
                p = it.get("player", {})
                f = it.get("fixture", {})
                frames.append({
                    "league_id": league_id,
                    "team": t.get("name"),
                    "player": p.get("name"),
                    "reason": p.get("reason"),
                    "fixture_id": f.get("id"),
                })
        except requests.HTTPError as e:
            print(f"[inj]  x : league={league_id} -> {e.response.status_code}/{e.response.reason}")
    df = pd.DataFrame(frames)
    outp = os.path.join(out_dir,"injuries.csv")
    (df if not df.empty else pd.DataFrame(columns=["league_id","team","player","reason","fixture_id"])).to_csv(outp, index=False)
    print(f"[inj] OK -> {outp} ({len(df)} linhas)")

if __name__ == "__main__":
    main()
