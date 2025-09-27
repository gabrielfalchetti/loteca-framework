from __future__ import annotations
import argparse, os, time, requests
import pandas as pd
from pathlib import Path
from rapidfuzz import fuzz
from utils_team_aliases import load_aliases, normalize_team

RAPIDAPI_HOST = "api-football-v1.p.rapidapi.com"
BASE_URL = f"https://{RAPIDAPI_HOST}/v3"

def api_get(path: str, params: dict) -> dict:
    headers = {
        "X-RapidAPI-Key": os.environ.get("RAPIDAPI_KEY", ""),
        "X-RapidAPI-Host": RAPIDAPI_HOST,
    }
    r = requests.get(BASE_URL + path, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"[apifootball] GET {path} HTTP {r.status_code}: {r.text[:300]}")
    return r.json()

def fixtures_by_date(d: str, country: str | None = None) -> list[dict]:
    params = {"date": d}
    if country:
        params["country"] = country
    return api_get("/fixtures", params).get("response", [])

def odds_by_fixture(fid: int) -> list[dict]:
    return api_get("/odds", {"fixture": fid}).get("response", [])

def find_fixture_id(date_iso: str, home: str, away: str, season_year: int, days_window: int = 2, country_hint: str | None = None) -> int | None:
    # busca em janela +/- days_window
    from datetime import datetime, timedelta
    dt = datetime.fromisoformat(date_iso)
    candidates = []
    countries = [c.strip() for c in (country_hint or "").split(",") if c.strip()] or [None]

    alias_map = load_aliases()
    hN = normalize_team(home, alias_map)
    aN = normalize_team(away, alias_map)

    for off in range(-days_window, days_window + 1):
        dstr = (dt + timedelta(days=off)).date().isoformat()
        for country in countries:
            resp = fixtures_by_date(dstr, country)
            for fx in resp:
                teams = fx.get("teams", {}) or {}
                league = fx.get("league", {}) or {}
                season = league.get("season")
                if season and int(season) != int(season_year):
                    continue
                th = teams.get("home", {}).get("name", "")
                ta = teams.get("away", {}).get("name", "")
                score_h = fuzz.token_sort_ratio(hN, th)
                score_a = fuzz.token_sort_ratio(aN, ta)
                score_swap_h = fuzz.token_sort_ratio(hN, ta)
                score_swap_a = fuzz.token_sort_ratio(aN, th)
                best = max(min(score_h, score_a), min(score_swap_h, score_swap_a))
                candidates.append((best, fx))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    best_score, best_fx = candidates[0]
    # um threshold razoável; será filtrado por --min-match no chamador
    return best_fx.get("fixture", {}).get("id", None)

def main():
    ap = argparse.ArgumentParser(description="Ingest odds via API-Football (RapidAPI)")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--allow-partial", action="store_true")
    ap.add_argument("--days-window", type=int, default=2)
    ap.add_argument("--min-match", type=int, default=85)
    ap.add_argument("--country-hint", type=str, default="")
    ap.add_argument("--season-year", type=int, default=None, help="Override season year (opcional)")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)
    matches_path = base / "matches.csv"
    if not matches_path.exists() or matches_path.stat().st_size == 0:
        raise RuntimeError(f"[apifootball] matches.csv ausente: {matches_path}")

    dfm = pd.read_csv(matches_path).rename(columns=str.lower)
    need = {"match_id", "home", "away"}
    if not need.issubset(dfm.columns):
        raise RuntimeError("[apifootball] matches.csv sem colunas necessárias: match_id,home,away")

    # normalizar nomes antes de buscar fixtures
    alias_map = load_aliases()
    dfm["home_n"] = dfm["home"].astype(str).apply(lambda x: normalize_team(x, alias_map))
    dfm["away_n"] = dfm["away"].astype(str).apply(lambda x: normalize_team(x, alias_map))

    # season: tentar inferir do campo date; senão usar ano corrente
    def infer_season(d: str) -> int:
        try:
            return int(str(d)[:4])
        except Exception:
            from datetime import datetime
            return int(datetime.utcnow().year)

    rows = []
    missing = []

    for _, r in dfm.iterrows():
        mid = int(r["match_id"])
        home = r["home_n"]
        away = r["away_n"]
        # date opcional
        date_iso = str(r["date"]) if "date" in r and not pd.isna(r["date"]) else None
        if not date_iso:
            # se faltar date, usa hoje como referência (janela cobre +/- days_window)
            from datetime import datetime
            date_iso = datetime.utcnow().date().isoformat()

        season_year = args.season_year or infer_season(date_iso)
        try:
            fid = find_fixture_id(date_iso, home, away, season_year, days_window=args.days_window, country_hint=args.country_hint)
        except Exception:
            time.sleep(0.6)
            fid = None

        if fid is None:
            missing.append(mid)
            continue

        # pega odds do fixture
        try:
            res = odds_by_fixture(fid)
        except Exception:
            time.sleep(0.6)
            res = []

        # extrai mercado 1X2 (home/draw/away)
        oh = od = oa = None
        for item in res:
            bks = item.get("bookmakers") or []
            for bk in bks:
                for mv in bk.get("bets", []):
                    if str(mv.get("name","")).strip().lower() in {"match winner", "1x2", "winner", "fulltime result"}:
                        vals = mv.get("values") or []
                        for v in vals:
                            val = str(v.get("value","")).strip().upper()
                            odd = v.get("odd")
                            try:
                                odd = float(odd)
                            except Exception:
                                continue
                            if val in {"HOME","1"}:
                                oh = odd if oh is None else min(oh, odd)
                            elif val in {"DRAW","X"}:
                                od = odd if od is None else min(od, odd)
                            elif val in {"AWAY","2"}:
                                oa = odd if oa is None else min(oa, odd)

        if oh and od and oa:
            rows.append({"match_id": mid, "home": home, "away": away, "odd_home": oh, "odd_draw": od, "odd_away": oa})
        else:
            missing.append(mid)

        time.sleep(0.25)  # respeita rate-limit

    if rows:
        out = pd.DataFrame(rows).sort_values("match_id")
        out.to_csv(base / "odds_apifootball.csv", index=False)
        print(f"[apifootball] OK -> {base/'odds_apifootball.csv'} ({len(out)} linhas)")
    else:
        print("[apifootball] Aviso: nenhuma odd coletada.")

    if missing:
        msg = f"[ingest_odds] Aviso: {len(missing)} jogos sem odds (ids={sorted(set(missing))})"
        if not args.allow_partial:
            raise RuntimeError(msg)
        else:
            print(msg)

if __name__ == "__main__":
    main()
