from __future__ import annotations
import argparse, os, requests, time
import pandas as pd
from pathlib import Path
from utils_team_aliases import load_aliases, normalize_team

BASE_URL = "https://api.the-odds-api.com/v4/sports"

def api_get(path: str, params: dict) -> list[dict] | dict:
    r = requests.get(f"{BASE_URL}/{path}", params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"[theoddsapi] GET /{path} HTTP {r.status_code}: {r.text[:300]}")
    try:
        return r.json()
    except Exception:
        return {}

def main():
    ap = argparse.ArgumentParser(description="Ingest odds TheOddsAPI")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--sport", required=True, help="ex.: soccer_brazil_campeonato, soccer_epl, soccer_spain_la_liga, ...")
    ap.add_argument("--regions", default="uk,eu", help="ex.: uk,eu,us")
    ap.add_argument("--market", default="h2h")
    ap.add_argument("--allow-partial", action="store_true")
    ap.add_argument("--min-match", type=int, default=85)  # mantido pra compat
    args = ap.parse_args()

    api_key = os.environ.get("ODDS_API_KEY", "")
    if not api_key:
        raise RuntimeError("[theoddsapi] ODDS_API_KEY ausente nos Secrets.")

    base = Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)

    # matches (para mapear match_id/home/away normalizados)
    mpath = base / "matches.csv"
    if not mpath.exists() or mpath.stat().st_size == 0:
        raise RuntimeError(f"[theoddsapi] matches.csv ausente: {mpath}")
    matches = pd.read_csv(mpath).rename(columns=str.lower)

    alias_map = load_aliases()
    matches["home_n"] = matches["home"].astype(str).apply(lambda x: normalize_team(x, alias_map))
    matches["away_n"] = matches["away"].astype(str).apply(lambda x: normalize_team(x, alias_map))

    # consulta de odds do sport
    params = {
        "apiKey": api_key,
        "regions": args.regions,
        "markets": args.market,
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    resp = api_get(f"{args.sport}/odds", params=params)
    if not isinstance(resp, list):
        raise RuntimeError("[theoddsapi] Resposta inesperada (não-list).")

    rows = []
    for game in resp:
        try:
            co = game.get("commence_time")  # iso
            teams = game.get("teams", []) or []
            home_team = game.get("home_team", "")
            if home_team and teams and home_team in teams:
                away_team = [t for t in teams if t != home_team][0] if len(teams) == 2 else ""
            else:
                if len(teams) == 2:
                    home_team, away_team = teams[0], teams[1]
                else:
                    continue
            hn = normalize_team(home_team, alias_map)
            an = normalize_team(away_team, alias_map)

            markets = game.get("bookmakers", []) or []
            oh = od = oa = None
            for bk in markets:
                for mkt in bk.get("markets", []):
                    if mkt.get("key") == args.market:
                        for outc in mkt.get("outcomes", []):
                            name = str(outc.get("name","")).strip().upper()
                            price = outc.get("price")
                            try:
                                price = float(price)
                            except Exception:
                                continue
                            if name == "HOME":
                                oh = price if oh is None else min(oh, price)
                            elif name == "DRAW":
                                od = price if od is None else min(od, price)
                            elif name == "AWAY":
                                oa = price if oa is None else min(oa, price)

            if oh and od and oa:
                rows.append({"commence_time": co, "home": hn, "away": an, "odd_home": oh, "odd_draw": od, "odd_away": oa})
        except Exception:
            continue

        time.sleep(0.05)  # leve respiro

    if not rows:
        print("[theoddsapi] Aviso: nenhuma odd coletada para este sport.")
        if not args.allow_partial:
            raise SystemExit(0)

    out = pd.DataFrame(rows)
    # tenta casar com matches por nomes normalizados
    merged = matches.merge(out, left_on=["home_n","away_n"], right_on=["home","away"], how="left")
    have = merged[merged[["odd_home","odd_draw","odd_away"]].notna().all(axis=1)]
    have = have[["match_id","home","away","odd_home","odd_draw","odd_away"]].sort_values("match_id")
    if have.empty:
        print("[theoddsapi] Nenhuma odd casada com matches (talvez sport não cobre esses jogos).")
    else:
        # salva em arquivo específico por sport, permitindo múltiplos merges depois
        sport_key = args.sport.lower().replace("/", "_")
        (base / f"odds_{sport_key}.csv").write_text(have.to_csv(index=False), encoding="utf-8")
        print(f"[theoddsapi] OK -> {base/f'odds_{sport_key}.csv'} ({len(have)} jogos)")

if __name__ == "__main__":
    main()
