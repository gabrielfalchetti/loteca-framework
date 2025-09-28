from __future__ import annotations
import argparse, os, time
from datetime import datetime, timedelta
from pathlib import Path
import requests
import pandas as pd
from rapidfuzz import fuzz
from utils_team_aliases import load_aliases, normalize_team

# ---------- API-Football (RapidAPI) ----------
RAPIDAPI_HOST = "api-football-v1.p.rapidapi.com"
AFB_BASE = f"https://{RAPIDAPI_HOST}/v3"

def afb_get(path: str, params: dict) -> dict:
    headers = {
        "X-RapidAPI-Key": os.environ.get("RAPIDAPI_KEY", ""),
        "X-RapidAPI-Host": RAPIDAPI_HOST,
    }
    r = requests.get(AFB_BASE + path, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"[apifootball] GET {path} HTTP {r.status_code}: {r.text[:300]}")
    return r.json()

def afb_fixtures_by_date(d: str, country: str | None = None) -> list[dict]:
    params = {"date": d}
    if country:
        params["country"] = country
    return afb_get("/fixtures", params).get("response", [])

def afb_find_fixture_id(date_iso: str, home: str, away: str, season_year: int, days_window: int = 3, country_hint: str | None = None) -> int | None:
    dt = datetime.fromisoformat(date_iso)
    candidates = []
    countries = [c.strip() for c in (country_hint or "").split(",") if c.strip()] or [None]
    for off in range(-days_window, days_window+1):
        dstr = (dt + timedelta(days=off)).date().isoformat()
        for country in countries:
            resp = afb_fixtures_by_date(dstr, country)
            for fx in resp:
                league = fx.get("league") or {}
                season = league.get("season")
                if season and int(season) != int(season_year):
                    continue
                teams = fx.get("teams") or {}
                th = str(teams.get("home", {}).get("name",""))
                ta = str(teams.get("away", {}).get("name",""))
                score = max(min(fuzz.token_sort_ratio(home, th), fuzz.token_sort_ratio(away, ta)),
                            min(fuzz.token_sort_ratio(home, ta), fuzz.token_sort_ratio(away, th)))
                candidates.append((score, fx))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1].get("fixture", {}).get("id")

def afb_result_by_fixture(fid: int) -> tuple[int,int,str] | None:
    data = afb_get("/fixtures", {"ids": fid}).get("response", [])
    if not data:
        return None
    fx = data[0]
    goals = fx.get("goals") or {}
    gh, ga = goals.get("home"), goals.get("away")
    try:
        gh = int(gh); ga = int(ga)
    except Exception:
        return None
    if gh > ga: res = "1"
    elif gh < ga: res = "2"
    else: res = "X"
    return gh, ga, res

# ---------- TheOddsAPI Scores ----------
ODDS_BASE = "https://api.the-odds-api.com/v4/sports"

SPORTS_TRY = [
    "soccer_brazil_campeonato",
    "soccer_brazil_serie_b",
    "soccer_brazil_serie_c",
    "soccer_brazil_cup",
    "soccer_epl",
    "soccer_spain_la_liga",
]

def odds_scores(sport: str, days_from: int = 3) -> list[dict]:
    key = os.environ.get("ODDS_API_KEY", "")
    if not key:
        return []
    params = {"apiKey": key, "daysFrom": days_from, "dateFormat": "iso"}
    r = requests.get(f"{ODDS_BASE}/{sport}/scores", params=params, timeout=25)
    if r.status_code != 200:
        return []
    try:
        return r.json() or []
    except Exception:
        return []

# ---------- Util ----------
def infer_season(d: str | None) -> int:
    if not d:
        return datetime.utcnow().year
    try:
        return int(str(d)[:4])
    except Exception:
        return datetime.utcnow().year

def main():
    ap = argparse.ArgumentParser(description="Coleta resultados oficiais (API-Football + TheOddsAPI Scores) e reconcilia")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--days-window", type=int, default=3)
    ap.add_argument("--min-match", type=int, default=80)
    ap.add_argument("--country-hint", type=str, default="Brazil,England,Spain")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)

    mpath = base / "matches.csv"
    if not mpath.exists() or mpath.stat().st_size == 0:
        raise RuntimeError(f"[results] matches.csv ausente: {mpath}")
    dfm = pd.read_csv(mpath).rename(columns=str.lower)

    alias_map = load_aliases()
    dfm["home_n"] = dfm["home"].astype(str).apply(lambda x: normalize_team(x, alias_map))
    dfm["away_n"] = dfm["away"].astype(str).apply(lambda x: normalize_team(x, alias_map))

    # ---------- API-Football pass ----------
    rows_afb = []
    missing_afb = []
    for _, r in dfm.iterrows():
        mid = int(r["match_id"])
        home = r["home_n"]; away = r["away_n"]
        date_iso = str(r["date"]) if "date" in r and not pd.isna(r["date"]) else datetime.utcnow().date().isoformat()
        season = infer_season(date_iso)
        try:
            fid = afb_find_fixture_id(date_iso, home, away, season, days_window=args.days_window, country_hint=args.country_hint)
        except Exception:
            fid = None
        if fid is None:
            missing_afb.append(mid); continue
        try:
            res = afb_result_by_fixture(fid)
        except Exception:
            time.sleep(0.4)
            res = None
        if res is None:
            missing_afb.append(mid)
            continue
        gh, ga, out = res
        rows_afb.append({"match_id": mid, "home": home, "away": away, "ft_home": gh, "ft_away": ga, "result": out})

        time.sleep(0.25)

    df_afb = pd.DataFrame(rows_afb).sort_values("match_id")
    (base / "results_raw_apifootball.csv").write_text(df_afb.to_csv(index=False), encoding="utf-8")
    print(f"[results] API-Football OK: {len(df_afb)} resultados | faltando: {len(missing_afb)}")

    # ---------- TheOddsAPI pass (fallback) ----------
    rows_odds = []
    if missing_afb:
        # carrega índices por esporte
        cache_scores = {}
        for sport in SPORTS_TRY:
            try:
                cache_scores[sport] = odds_scores(sport, days_from=args.days_window)
            except Exception:
                cache_scores[sport] = []
            time.sleep(0.2)

        for _, r in dfm[dfm["match_id"].isin(missing_afb)].iterrows():
            mid = int(r["match_id"])
            home = r["home_n"]; away = r["away_n"]
            got = False
            for sport, arr in cache_scores.items():
                for g in arr:
                    # normalizar nomes
                    teams = g.get("teams") or []
                    if len(teams) != 2:
                        continue
                    ht, at = teams[0], teams[1]
                    # tentar mapear home/away coerente
                    if g.get("home_team") == ht:
                        t_home, t_away = ht, at
                    else:
                        t_home, t_away = at, ht
                    s1 = max(min(fuzz.token_sort_ratio(home, t_home), fuzz.token_sort_ratio(away, t_away)),
                             min(fuzz.token_sort_ratio(home, t_away), fuzz.token_sort_ratio(away, t_home)))
                    if s1 < args.min_match:
                        continue
                    # precisa estar finalizado
                    comm = g.get("completed", False)
                    scores = g.get("scores") or {}
                    sh = scores.get(t_home)
                    sa = scores.get(t_away)
                    try:
                        gh = int(sh); ga = int(sa)
                    except Exception:
                        continue
                    if gh > ga: out = "1"
                    elif gh < ga: out = "2"
                    else: out = "X"
                    rows_odds.append({"match_id": mid, "home": home, "away": away, "ft_home": gh, "ft_away": ga, "result": out, "sport": sport, "completed": bool(comm)})
                    got = True
                    break
                if got: break

    df_odds = pd.DataFrame(rows_odds).sort_values("match_id")
    (base / "results_raw_oddsapi.csv").write_text(df_odds.to_csv(index=False), encoding="utf-8")
    print(f"[results] OddsAPI OK: {len(df_odds)} resultados (fallback)")

    # ---------- Reconcile ----------
    # Regra: preferir API-Football; se ausente, usar OddsAPI; se houver conflito, manter API-Football e logar.
    merged = dfm[["match_id","home_n","away_n"]].rename(columns={"home_n":"home","away_n":"away"}).merge(
        df_afb[["match_id","ft_home","ft_away","result"]], on="match_id", how="left", suffixes=("","_afb")
    ).merge(
        df_odds[["match_id","ft_home","ft_away","result"]], on="match_id", how="left", suffixes=("","_odds")
    )

    out_rows = []
    for _, r in merged.iterrows():
        mid = int(r["match_id"]); home = r["home"]; away = r["away"]
        if pd.notna(r.get("ft_home")) and pd.notna(r.get("ft_away")):
            gh, ga, res = int(r["ft_home"]), int(r["ft_away"]), str(r["result"])
        elif pd.notna(r.get("ft_home_odds")) and pd.notna(r.get("ft_away_odds")):
            gh, ga, res = int(r["ft_home_odds"]), int(r["ft_away_odds"]), str(r["result_odds"])
        else:
            gh = ga = None; res = None
        out_rows.append({"match_id": mid, "home": home, "away": away, "ft_home": gh, "ft_away": ga, "result": res})

    out = pd.DataFrame(out_rows).sort_values("match_id")
    out_path = base / "results_reconciled.csv"
    out.to_csv(out_path, index=False)
    print(f"[results] Reconciliado -> {out_path}")

if __name__ == "__main__":
    main()
