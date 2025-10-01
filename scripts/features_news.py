# scripts/features_news.py
from __future__ import annotations
import argparse
import sys
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
from rapidfuzz import fuzz

def _safe_lower(x: Any) -> str:
    return str(x).lower() if pd.notna(x) else ""

INJURY_WORDS_PT = ["lesão","contusão","machucado","desfalque","fora da partida","fora do jogo","fora do clássico","suspenso","suspensão","dúvida","doubt","desconforto","cirurgia"]
INJURY_WORDS_EN = ["injury","injured","out of the match","out of game","sidelined","suspension","doubt","questionable","ruled out","hamstring","ankle","knee","groin"]
TRANSFER_WORDS = ["transfer","contratação","reforço","saída","empréstimo","loan","assinou","signed"]

KEYWORDS = [*INJURY_WORDS_PT, *INJURY_WORDS_EN, *TRANSFER_WORDS]

def _hit_keywords(txt: str) -> int:
    t = _safe_lower(txt)
    if not t:
        return 0
    c = 0
    for w in KEYWORDS:
        if w in t:
            c += 1
    return c

def _best_ratio(name: str, text: str) -> int:
    if not name or not text:
        return 0
    try:
        return int(fuzz.partial_ratio(name.lower(), text.lower()))
    except Exception:
        return 0

def main() -> None:
    ap = argparse.ArgumentParser(description="Gera features de notícias por partida.")
    ap.add_argument("--rodada", required=True, help="Ex.: 2025-09-27_1213")
    ap.add_argument("--match_threshold", type=int, default=80, help="score mínimo (0-100) para associar artigo a time (default=80)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rodada = args.rodada
    out_dir = Path("data/out") / rodada
    in_dir = Path("data/in") / rodada

    news_raw = out_dir / "news_raw.csv"
    news_flags = out_dir / "news_flags.csv"

    # Sempre escreveremos um CSV de saída
    out_cols = [
        "match_id","match_date","home","away",
        "news_count_total","news_count_home","news_count_away",
        "injury_hits_total","injury_hits_home","injury_hits_away",
        "transfer_hits_total","transfer_hits_home","transfer_hits_away"
    ]
    try:
        if not news_raw.exists():
            pd.DataFrame(columns=out_cols).to_csv(news_flags, index=False)
            print(f"[news-features] AVISO: {news_raw} ausente. OK -> {news_flags} (0 linhas)")
            return

        df = pd.read_csv(news_raw)
        if df.empty:
            pd.DataFrame(columns=out_cols).to_csv(news_flags, index=False)
            print(f"[news-features] AVISO: {news_raw} vazio. OK -> {news_flags} (0 linhas)")
            return

        # Garantir colunas básicas
        for c in ["match_id","match_date","home","away","title","description","content","lang"]:
            if c not in df.columns:
                df[c] = ""

        # Agregar por partida
        groups = df.groupby(["match_id","match_date","home","away"], dropna=False)

        rows: List[Dict[str,Any]] = []
        for (match_id, mdate, home, away), g in groups:
            home = str(home); away = str(away)

            total = len(g)
            home_cnt = 0
            away_cnt = 0
            inj_total = 0
            inj_home = 0
            inj_away = 0
            tr_total = 0
            tr_home = 0
            tr_away = 0

            for _, r in g.iterrows():
                title = str(r.get("title",""))
                desc  = str(r.get("description",""))
                cont  = str(r.get("content",""))
                blob = " ".join([title, desc, cont])

                # matching por fuzzy score
                h_ratio = max(_best_ratio(home, title), _best_ratio(home, desc), _best_ratio(home, cont))
                a_ratio = max(_best_ratio(away, title), _best_ratio(away, desc), _best_ratio(away, cont))

                if h_ratio >= args.match_threshold:
                    home_cnt += 1
                if a_ratio >= args.match_threshold:
                    away_cnt += 1

                hits = _hit_keywords(blob)
                if hits > 0:
                    inj_hits = sum(1 for w in (INJURY_WORDS_PT + INJURY_WORDS_EN) if w in blob.lower())
                    tr_hits  = sum(1 for w in TRANSFER_WORDS if w in blob.lower())
                    inj_total += inj_hits
                    tr_total  += tr_hits
                    if h_ratio >= args.match_threshold:
                        inj_home += inj_hits
                        tr_home  += tr_hits
                    if a_ratio >= args.match_threshold:
                        inj_away += inj_hits
                        tr_away  += tr_hits

            rows.append({
                "match_id": match_id,
                "match_date": mdate,
                "home": home,
                "away": away,
                "news_count_total": int(total),
                "news_count_home": int(home_cnt),
                "news_count_away": int(away_cnt),
                "injury_hits_total": int(inj_total),
                "injury_hits_home": int(inj_home),
                "injury_hits_away": int(inj_away),
                "transfer_hits_total": int(tr_total),
                "transfer_hits_home": int(tr_home),
                "transfer_hits_away": int(tr_away),
            })

        out = pd.DataFrame(rows, columns=out_cols)
        out.to_csv(news_flags, index=False)
        print(f"[news-features] OK -> {news_flags} ({len(out)} linhas)")
    except Exception as e:
        # Segurança: saída vazia em caso de erro
        pd.DataFrame(columns=out_cols).to_csv(news_flags, index=False)
        print(f"[news-features] ERRO não-fatal: {e}", file=sys.stderr)
        print(f"[news-features] OK -> {news_flags} (0 linhas)")

if __name__ == "__main__":
    main()
