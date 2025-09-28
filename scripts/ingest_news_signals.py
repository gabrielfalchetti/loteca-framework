from __future__ import annotations
import argparse, os, time, re, json
from datetime import datetime, timedelta
from pathlib import Path
import requests
import pandas as pd
from utils_team_aliases import load_aliases, normalize_team

NEWS_API_URL = "https://newsapi.org/v2/everything"

# Palavras/expressões-chave (PT-BR + EN) para sinais pré-jogo
KEYWORDS = {
    "injury_signal": [
        r"\bles(ão|oes|oes)\b", r"\blesionado", r"\bcontusão\b", r"\bdesfalque\b", r"\bfora do jogo\b",
        r"\binjury\b", r"\binjured\b", r"\bhurt\b", r"\bknock\b", r"\bhamstring\b", r"\bsprain\b",
    ],
    "suspension_signal": [
        r"\bsuspens(o|ão|ao)\b", r"\bcart(o|ão)\b", r"\bgancho\b", r"\bpena disciplinar\b",
        r"\bsuspended\b", r"\bban\b",
    ],
    "coach_change": [
        r"\bdemiss(a|ão)\b", r"\btreinador\b.*(sai|deixa|demitido)", r"\bnovo tecnico\b", r"\bnomeado\b",
        r"\bcoach\b.*(sacked|out|appointed)",
    ],
    "travel_fatigue": [
        r"\bviagem longa\b", r"\bdesgaste\b", r"\bjet lag\b", r"\bback-to-back\b", r"\bsequence away\b",
        r"\bmaratona\b", r"\btravel\b.*(long|far)",
    ],
}

def _score_article(text: str) -> dict[str, int]:
    scores = {k:0 for k in KEYWORDS}
    t = text.lower()
    for k, patterns in KEYWORDS.items():
        for pat in patterns:
            if re.search(pat, t):
                scores[k] += 1
    return scores

def _merge_scores(a: dict[str,int], b: dict[str,int]) -> dict[str,int]:
    out = dict(a)
    for k,v in b.items():
        out[k] = out.get(k,0) + int(v)
    return out

def news_query(team: str, from_dt: datetime, to_dt: datetime) -> list[dict]:
    key = os.environ.get("NEWSAPI_KEY", "")
    if not key:
        raise RuntimeError("[news] NEWSAPI_KEY ausente nos Secrets.")
    params = {
        "q": team,
        "from": from_dt.strftime("%Y-%m-%d"),
        "to": to_dt.strftime("%Y-%m-%d"),
        "language": "pt",
        "sortBy": "relevancy",
        "apiKey": key,
        "pageSize": 50,
    }
    r = requests.get(NEWS_API_URL, params=params, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"[news] HTTP {r.status_code}: {r.text[:200]}")
    data = r.json() or {}
    return data.get("articles", []) or []

def main():
    ap = argparse.ArgumentParser(description="Coleta sinais de notícias pré-jogo (lesões, suspensões, técnico, viagem)")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--days-window", type=int, default=5, help="Janela retroativa (dias) para buscar notícias")
    ap.add_argument("--cooldown", type=float, default=0.5, help="Delay entre requisições (seg)")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)

    mpath = base / "matches.csv"
    if not mpath.exists() or mpath.stat().st_size == 0:
        raise RuntimeError(f"[news] matches.csv ausente: {mpath}")
    matches = pd.read_csv(mpath).rename(columns=str.lower)

    alias_map = load_aliases()
    matches["home_n"] = matches["home"].astype(str).apply(lambda x: normalize_team(x, alias_map))
    matches["away_n"] = matches["away"].astype(str).apply(lambda x: normalize_team(x, alias_map))

    to_dt = datetime.utcnow()
    from_dt = to_dt - timedelta(days=int(args.days_window))

    raw_jsonl = base / "news_raw.jsonl"
    fout = open(raw_jsonl, "w", encoding="utf-8")

    rows = []
    for _, r in matches.iterrows():
        mid = int(r["match_id"])
        home = r["home_n"]
        away = r["away_n"]

        # consulta separada por time
        scores_home = {k:0 for k in KEYWORDS}
        scores_away = {k:0 for k in KEYWORDS}

        for team, tgt in [(home, "home"), (away, "away")]:
            try:
                arts = news_query(team, from_dt, to_dt)
            except Exception as e:
                print(f"[news] falha em {team}: {e}")
                arts = []
            time.sleep(args.cooldown)
            for art in arts:
                txt = " ".join([str(art.get(k,"")) for k in ("title","description","content")])
                score = _score_article(txt)
                if tgt == "home":
                    scores_home = _merge_scores(scores_home, score)
                else:
                    scores_away = _merge_scores(scores_away, score)
                fout.write(json.dumps({"match_id":mid,"team":team,"side":tgt,"score":score,"title":art.get("title","")}, ensure_ascii=False)+"\n")

        rows.append({
            "match_id": mid,
            "home": home,
            "away": away,
            "injury_signal_home": scores_home["injury_signal"],
            "suspension_signal_home": scores_home["suspension_signal"],
            "coach_change_home": scores_home["coach_change"],
            "travel_fatigue_home": scores_home["travel_fatigue"],
            "injury_signal_away": scores_away["injury_signal"],
            "suspension_signal_away": scores_away["suspension_signal"],
            "coach_change_away": scores_away["coach_change"],
            "travel_fatigue_away": scores_away["travel_fatigue"],
        })

    fout.close()
    out = pd.DataFrame(rows).sort_values("match_id")
    out_path = base / "news_signals.csv"
    out.to_csv(out_path, index=False)
    print(f"[news] OK -> {out_path}")

if __name__ == "__main__":
    main()
