#!/usr/bin/env python3
import argparse, os, sys, json, time, logging
from datetime import datetime, timedelta
import requests
import pandas as pd
from unidecode import unidecode
from rapidfuzz import fuzz, process

API_HOST = "api-football-v1.p.rapidapi.com"
BASE_URL = f"https://{API_HOST}/v3"

BET_NAME_CANDIDATES = {
    "Match Winner", "1X2", "Full Time Result", "Winner", "3Way Result", "Match Result"
}

def norm(s: str) -> str:
    if s is None: return ""
    return unidecode(s).lower().strip()

def make_match_key(home: str, away: str) -> str:
    return f"{norm(home)}__vs__{norm(away)}"

def http_get(path, params, key, debug=False):
    headers = {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": API_HOST,
    }
    url = f"{BASE_URL}/{path.lstrip('/')}"
    if debug:
        logging.info(f"[apifootball] GET {url} params={params}")
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code == 429:
        # simples backoff
        time.sleep(2)
        r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def collect_fixtures_by_dates(dates, season, leagues, key, debug=False):
    fixtures = []
    for d in dates:
        for lg in leagues:
            params = {"date": d.strftime("%Y-%m-%d"), "season": int(season), "league": int(lg)}
            try:
                data = http_get("fixtures", params, key, debug)
            except requests.HTTPError as e:
                logging.warning(f"[apifootball] fixtures HTTP {e.response.status_code} league={lg} date={params['date']}")
                continue
            for it in data.get("response", []):
                fxt = it.get("fixture", {})
                teams = it.get("teams", {})
                home = teams.get("home", {}).get("name")
                away = teams.get("away", {}).get("name")
                fid = fxt.get("id")
                if not home or not away or not fid:
                    continue
                fixtures.append({
                    "fixture_id": fid,
                    "team_home": home,
                    "team_away": away,
                    "match_key": make_match_key(home, away),
                    "timestamp": fxt.get("timestamp"),
                    "date": fxt.get("date"),
                    "league_id": lg,
                })
    return pd.DataFrame(fixtures).drop_duplicates(subset=["fixture_id"]).reset_index(drop=True)

def find_1x2_row(odds_item):
    # odds->bookmakers->[...]->bets->[...]; procurar bet.name em BET_NAME_CANDIDATES
    bms = odds_item.get("bookmakers", []) or []
    for bm in bms:
        for bet in bm.get("bets", []) or []:
            name = bet.get("name") or ""
            if name in BET_NAME_CANDIDATES:
                values = bet.get("values", []) or []
                # mapear selection: "Home", "Draw", "Away" (varia por feed)
                out = {"home": None, "draw": None, "away": None}
                for v in values:
                    sel = (v.get("value") or "").lower()
                    odd_str = v.get("odd")
                    try:
                        odd = float(odd_str)
                    except:
                        odd = None
                    if odd and odd > 1.0:
                        if "home" in sel or sel in ("1", "local"):
                            out["home"] = odd
                        elif "draw" in sel or sel in ("x", "empate"):
                            out["draw"] = odd
                        elif "away" in sel or sel in ("2", "visitante"):
                            out["away"] = odd
                if any(out.values()):
                    return out
    return None

def collect_odds_for_fixtures(fixture_ids, key, debug=False):
    rows = []
    for fid in fixture_ids:
        try:
            data = http_get("odds", {"fixture": fid}, key, debug)
        except requests.HTTPError as e:
            logging.warning(f"[apifootball] odds HTTP {e.response.status_code} fixture={fid}")
            continue
        for item in data.get("response", []):
            # Alguns retornam dentro de "fixture" e "league", com "teams"
            teams = item.get("teams", {})
            home = teams.get("home", {}).get("name")
            away = teams.get("away", {}).get("name")
            mk = make_match_key(home, away)
            pick = find_1x2_row(item)
            if not pick:
                continue
            rows.append({
                "team_home": home,
                "team_away": away,
                "match_key": mk,
                "odds_home": pick.get("home"),
                "odds_draw": pick.get("draw"),
                "odds_away": pick.get("away"),
                "fixture_id": item.get("fixture", {}).get("id") or fid
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.groupby(["team_home","team_away","match_key","fixture_id"], as_index=False).max()
    return df

def fuzzy_match_inputs(df_in, df_fx, fuzzy=0.90, debug=False):
    # cria chaves normalizadas dos fixtures
    df_fx = df_fx.copy()
    df_fx["__home_n"] = df_fx["team_home"].map(norm)
    df_fx["__away_n"] = df_fx["team_away"].map(norm)
    df_fx["__key_n"]  = df_fx["match_key"].map(str)

    matched = []
    unmatched = []

    for _, row in df_in.iterrows():
        home, away = row["team_home"], row["team_away"]
        key = row["match_key"]
        hn, an = norm(home), norm(away)
        candidates = df_fx

        # usa chave exata primeiro
        exact = candidates[candidates["match_key"] == key]
        if not exact.empty:
            pick = exact.iloc[0]
            matched.append({**row.to_dict(), "fixture_id": pick["fixture_id"]})
            continue

        # fallback: fuzzy por assinatura "home + ' vs ' + away"
        target1 = f"{hn} vs {an}"
        choices = (candidates["__home_n"] + " vs " + candidates["__away_n"]).tolist()
        best = process.extractOne(
            target1,
            choices,
            scorer=fuzz.WRatio
        )
        score = best[1] if best else 0
        if best and score >= fuzzy*100:
            idx = choices.index(best[0])
            pick = candidates.iloc[idx]
            matched.append({**row.to_dict(), "fixture_id": pick["fixture_id"]})
            if debug:
                logging.info(f"[match] '{home} x {away}' ~ '{pick['team_home']} x {pick['team_away']}' (score={score})")
        else:
            unmatched.append(row.to_dict())

    return pd.DataFrame(matched), pd.DataFrame(unmatched)

def load_aliases(path):
    if not path or not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def apply_aliases(df, aliases):
    if not aliases: return df
    def map_name(x):
        nx = norm(x)
        return aliases.get(nx, x)
    df = df.copy()
    df["team_home"] = df["team_home"].map(map_name)
    df["team_away"] = df["team_away"].map(map_name)
    df["match_key"] = [make_match_key(h,a) for h,a in zip(df["team_home"], df["team_away"])]
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", required=True)
    ap.add_argument("--leagues", default="71,72")  # Série A=71, Série B=72 (API-Football)
    ap.add_argument("--window", type=int, default=2)  # dias ±
    ap.add_argument("--fuzzy", type=float, default=0.90)
    ap.add_argument("--aliases", default="data/aliases_br.json")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO if args.debug else logging.WARNING,
                        format="%(message)s")

    api_key = os.getenv("X_RAPIDAPI_KEY", "")
    if not api_key:
        print("[apifootball-safe] SKIP: X_RAPIDAPI_KEY ausente.")
        return

    in_dir  = os.path.join("data","in", args.rodada)
    out_dir = os.path.join("data","out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)

    in_path = os.path.join(in_dir, "matches_source.csv")
    if not os.path.isfile(in_path):
        print(f"[apifootball-safe] ERRO: {in_path} não encontrado.")
        sys.exit(2)

    df_in = pd.read_csv(in_path)
    needed_cols = {"team_home","team_away","match_key"}
    if not needed_cols.issubset(set(map(str.lower, df_in.columns))):
        # normaliza headers
        df_in.columns = [c.strip() for c in df_in.columns]
    for c in needed_cols:
        if c not in df_in.columns:
            raise SystemExit(f"[apifootball-safe] ERRO: coluna ausente em matches_source.csv: {c}")

    # aliases (opcional)
    aliases = load_aliases(args.aliases)
    if aliases:
        df_in = apply_aliases(df_in, aliases)

    # datas a consultar
    date_str = args.rodada.split("_")[0]
    base_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    dates = [base_date + timedelta(days=delta) for delta in range(-args.window, args.window+1)]

    leagues = [x.strip() for x in args.leagues.split(",") if x.strip()]
    if args.debug:
        logging.info(f"[apifootball-safe] buscando fixtures: dates={','.join([d.isoformat() for d in dates])} season={args.season} leagues={leagues}")

    df_fx = collect_fixtures_by_dates(dates, args.season, leagues, api_key, args.debug)
    if df_fx.empty:
        # ainda assim escrever arquivos vazios para manter o pipeline estável
        pd.DataFrame(columns=["team_home","team_away","match_key","odds_home","odds_draw","odds_away"]).to_csv(
            os.path.join(out_dir, "odds_apifootball.csv"), index=False
        )
        pd.DataFrame(columns=df_in.columns).to_csv(
            os.path.join(out_dir, "unmatched_apifootball.csv"), index=False
        )
        print('[apifootball-safe] linhas -> {"odds_apifootball.csv": 0, "unmatched_apifootball.csv": %d}' % len(df_in))
        return

    # match inputs x fixtures
    df_matched, df_unmatched = fuzzy_match_inputs(df_in, df_fx, args.fuzzy, args.debug)

    # coletar odds de quem casou
    odds_df = pd.DataFrame(columns=["team_home","team_away","match_key","odds_home","odds_draw","odds_away"])
    if not df_matched.empty:
        fx_ids = df_matched["fixture_id"].dropna().astype(int).unique().tolist()
        df_od = collect_odds_for_fixtures(fx_ids, api_key, args.debug)
        if not df_od.empty:
            # O CSV final deve manter o match_key do input
            odds_df = (df_matched[["match_key","fixture_id"]]
                       .merge(df_od, on="fixture_id", how="left")
                       [["team_home","team_away","match_key","odds_home","odds_draw","odds_away"]])
            # Deduplicate keeping best (maior odd)
            odds_df = odds_df.groupby(["team_home","team_away","match_key"], as_index=False).max()

    # salvar
    odds_path = os.path.join(out_dir, "odds_apifootball.csv")
    unmatched_path = os.path.join(out_dir, "unmatched_apifootball.csv")
    odds_df.to_csv(odds_path, index=False)
    (df_unmatched if not df_unmatched.empty else pd.DataFrame(columns=df_in.columns)).to_csv(unmatched_path, index=False)

    print(f'[apifootball-safe] linhas -> {{"odds_apifootball.csv": {len(odds_df)}, "unmatched_apifootball.csv": {len(df_unmatched)}}}')

if __name__ == "__main__":
    main()
