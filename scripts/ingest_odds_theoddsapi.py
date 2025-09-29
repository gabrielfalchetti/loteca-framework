#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, argparse, requests, pandas as pd
from unidecode import unidecode

API_BASE = "https://api.the-odds-api.com/v4"

# mapeamento auxiliar se o CSV não trouxer sport_key
SPORT_MAP = {
    ("Brazil","Série A"): "soccer_brazil_campeonato",
    ("Brazil","Serie A"): "soccer_brazil_campeonato",
    ("Brazil","Série B"): "soccer_brazil_serie_b",
    ("Brazil","Serie B"): "soccer_brazil_serie_b",
    ("Brazil","Copa do Brasil"): "soccer_brazil_cup",
    ("Brazil","Brazil Cup"): "soccer_brazil_cup",
}

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", default="uk,eu,us,au")
    ap.add_argument("--markets", default="h2h")
    ap.add_argument("--debug", action="store_true")
    return ap.parse_args()

def load_matches(rodada):
    fn = f"data/in/{rodada}/matches_source.csv"
    df = pd.read_csv(fn)
    req = ["match_id","home_team","away_team"]
    for c in req:
        if c not in df.columns:
            raise ValueError(f"Campo obrigatório ausente em {fn}: {c}")
    # tenta preencher sport_key se faltar
    if "sport_key_theoddsapi" not in df.columns:
        df["sport_key_theoddsapi"] = ""
    def fill_skey(row):
        if pd.notna(row.get("sport_key_theoddsapi","")) and str(row.get("sport_key_theoddsapi","")).strip():
            return str(row["sport_key_theoddsapi"]).strip()
        country = str(row.get("country","")).strip()
        league  = str(row.get("league_name","")).strip()
        return SPORT_MAP.get((country, league), "")
    df["sport_key_theoddsapi"] = df.apply(fill_skey, axis=1)
    return df

def norm(s: str) -> str:
    return unidecode(str(s).strip().lower())

def fetch_odds(apikey, sport_key, regions, markets):
    url = f"{API_BASE}/sports/{sport_key}/odds"
    params = {
        "apiKey": apikey,
        "regions": regions,
        "markets": markets,
        "oddsFormat": "decimal",
        "dateFormat": "iso"
    }
    r = requests.get(url, params=params, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"TheOddsAPI {r.status_code}: {r.text}")
    return r.json()

def rows_for_match(data, home, away):
    H, A = norm(home), norm(away)
    out = []
    for ev in data:
        if norm(ev.get("home_team","")) == H and norm(ev.get("away_team","")) == A:
            for bk in ev.get("bookmakers", []):
                for mk in bk.get("markets", []):
                    if mk.get("key") != "h2h":
                        continue
                    rec = {
                        "bookmaker": bk.get("title",""),
                        "last_update": mk.get("last_update",""),
                        "home_price": None, "draw_price": None, "away_price": None
                    }
                    for o in mk.get("outcomes", []):
                        nm = norm(o.get("name",""))
                        if nm == H:
                            rec["home_price"] = o.get("price")
                        elif nm == "draw":
                            rec["draw_price"] = o.get("price")
                        elif nm == A:
                            rec["away_price"] = o.get("price")
                    out.append(rec)
    return out

def main():
    args = parse_args()
    rodada = args.rodada
    outdir = f"data/out/{rodada}"
    os.makedirs(outdir, exist_ok=True)

    apikey = os.getenv("THEODDS_API_KEY", "").strip()
    if not apikey:
        print("[theoddsapi] ERRO: THEODDS_API_KEY não definido.", file=sys.stderr)
        sys.exit(2)

    dfm = load_matches(rodada)
    all_rows = []
    for _, m in dfm.iterrows():
        mid  = m["match_id"]
        home = m["home_team"]; away = m["away_team"]
        skey = str(m.get("sport_key_theoddsapi","")).strip()
        if not skey:
            # sem sport_key válida, não tenta (TheOddsAPI exige)
            if args.debug:
                print(f"[theoddsapi] {mid}: sem sport_key — pulando")
            continue
        try:
            data = fetch_odds(apikey, skey, args.regions, args.markets)
            rows = rows_for_match(data, home, away)
            for r in rows:
                r.update({"match_id": mid, "home_team": home, "away_team": away, "sport_key": skey})
                all_rows.append(r)
            if args.debug:
                print(f"[theoddsapi] {mid} -> {len(rows)} linhas")
        except Exception as e:
            print(f"[theoddsapi] AVISO {mid}: {e}", file=sys.stderr)

    out = f"{outdir}/odds_theoddsapi.csv"
    if not all_rows:
        pd.DataFrame(columns=["match_id","home_team","away_team","sport_key","bookmaker","last_update","home_price","draw_price","away_price"]).to_csv(out, index=False)
        print(f"[theoddsapi] OK -> {out} (0 linhas)")
        sys.exit(3)  # indica “sem odds” ao caller
    pd.DataFrame(all_rows).to_csv(out, index=False)
    print(f"[theoddsapi] OK -> {out} ({len(all_rows)} linhas)")

if __name__ == "__main__":
    main()
