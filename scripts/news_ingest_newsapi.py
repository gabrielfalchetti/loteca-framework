# scripts/news_ingest_newsapi.py
from __future__ import annotations
import os
import sys
import json
import time
import math
import csv
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional

import requests
import pandas as pd

UTC = timezone.utc

def _read_matches(matches_csv: Path) -> pd.DataFrame:
    """
    Lê o CSV de partidas (data/in/<RODADA>/matches_source.csv) e retorna DataFrame
    com colunas esperadas: match_id (ou índice), date (YYYY-MM-DD), home, away.
    Aceita nomes de coluna comuns e faz um 'normalize'.
    """
    df = pd.read_csv(matches_csv)
    # normalizar nomes
    cols = {c.lower().strip(): c for c in df.columns}
    # tentar mapear
    guess = {}
    for k in ["match_id", "id", "game_id"]:
        if k in cols:
            guess["match_id"] = cols[k]; break
    for k in ["date", "data", "match_date", "jogo_data"]:
        if k in cols:
            guess["date"] = cols[k]; break
    for k in ["home", "mandante", "home_team", "casa"]:
        if k in cols:
            guess["home"] = cols[k]; break
    for k in ["away", "visitante", "away_team", "fora"]:
        if k in cols:
            guess["away"] = cols[k]; break

    # se não tiver match_id, cria incremental
    if "match_id" not in guess:
        df["match_id"] = range(1, len(df) + 1)
        guess["match_id"] = "match_id"

    # coerção de data
    if "date" not in guess:
        # se não há data, usa hoje (seguro, mas menos informativo)
        df["date"] = datetime.now(UTC).date().isoformat()
        guess["date"] = "date"

    # renomear
    df = df.rename(columns={
        guess["match_id"]: "match_id",
        guess["date"]: "date",
        guess["home"]: "home" if "home" in guess else guess.get("home", "home"),
        guess["away"]: "away" if "away" in guess else guess.get("away", "away"),
    })
    # garantir colunas
    for c in ["home", "away"]:
        if c not in df.columns:
            df[c] = ""
    # str
    df["home"] = df["home"].fillna("").astype(str)
    df["away"] = df["away"].fillna("").astype(str)
    # date para str yyyy-mm-dd
    def _coerce_date(x: Any) -> str:
        try:
            return pd.to_datetime(x).date().isoformat()
        except Exception:
            return datetime.now(UTC).date().isoformat()
    df["date"] = df["date"].apply(_coerce_date)
    return df[["match_id", "date", "home", "away"]]

def _newsapi_fetch(q: str, from_date: str, to_date: str, api_key: str, page_size: int = 50, language: str = "pt") -> List[Dict[str, Any]]:
    """
    Busca na NewsAPI 'everything'. Respeita rate limit de maneira simples.
    Em erro (401/429/5xx), retorna lista vazia.
    """
    url = "https://newsapi.org/v2/everything"
    headers = {"X-Api-Key": api_key}
    params = {
        "q": q,
        "from": from_date,   # YYYY-MM-DD
        "to": to_date,       # YYYY-MM-DD
        "pageSize": page_size,
        "sortBy": "publishedAt",
        "language": language,   # pt (primário). Depois buscamos também 'en' se quiser.
    }
    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        if r.status_code == 429:
            # rate limit: esperar um pouco e seguir vazia
            time.sleep(2.0)
            return []
        r.raise_for_status()
        data = r.json()
        arts = data.get("articles", []) or []
        return arts
    except Exception:
        return []

def _unique(seq: List[str]) -> List[str]:
    seen = set()
    out = []
    for s in seq:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest de notícias via NewsAPI (seguro e aditivo).")
    ap.add_argument("--rodada", required=True, help="Ex.: 2025-09-27_1213")
    ap.add_argument("--window_days", type=int, default=5, help="Janela de dias ao redor da data do jogo (default=5)")
    ap.add_argument("--langs", default="pt,en", help="Línguas separadas por vírgula (default=pt,en)")
    ap.add_argument("--page_size", type=int, default=50, help="pageSize por chamado (default=50)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rodada = args.rodada
    out_dir = Path("data/out") / rodada
    in_dir = Path("data/in") / rodada
    out_dir.mkdir(parents=True, exist_ok=True)

    matches_csv = in_dir / "matches_source.csv"
    news_raw_csv = out_dir / "news_raw.csv"

    # Sempre escreveremos um CSV (mesmo vazio) para não quebrar nada.
    cols = [
        "match_id","match_date","home","away",
        "lang","query","source_name","author","title","description","content",
        "url","published_at"
    ]

    try:
        api_key = os.environ.get("NEWSAPI_KEY", "").strip()
        if not api_key:
            if args.debug:
                print("[news] NEWSAPI_KEY não configurado — gerando CSV vazio.")
            pd.DataFrame(columns=cols).to_csv(news_raw_csv, index=False)
            print(f"[news] OK -> {news_raw_csv} (0 linhas)")
            return

        dfm = _read_matches(matches_csv)
        langs = [x.strip() for x in args.langs.split(",") if x.strip()]
        queries: List[str] = _unique([*dfm["home"].tolist(), *dfm["away"].tolist()])
        rows: List[Dict[str, Any]] = []

        for _, row in dfm.iterrows():
            match_id = row["match_id"]
            md = row["date"]
            home = row["home"]; away = row["away"]
            # janela
            d0 = pd.to_datetime(md).date()
            from_d = (d0 - timedelta(days=args.window_days)).isoformat()
            to_d   = (d0 + timedelta(days=args.window_days)).isoformat()

            for lang in langs:
                for team in _unique([home, away]):
                    q = team.strip()
                    if not q:
                        continue
                    arts = _newsapi_fetch(q=q, from_date=from_d, to_date=to_d, api_key=api_key, page_size=args.page_size, language=lang)
                    if args.debug:
                        print(f"[news] {match_id} {md} [{lang}] '{q}' -> {len(arts)} artigos")
                    for a in arts:
                        rows.append({
                            "match_id": match_id,
                            "match_date": md,
                            "home": home,
                            "away": away,
                            "lang": lang,
                            "query": q,
                            "source_name": (a.get("source") or {}).get("name"),
                            "author": a.get("author"),
                            "title": a.get("title"),
                            "description": a.get("description"),
                            "content": a.get("content"),
                            "url": a.get("url"),
                            "published_at": a.get("publishedAt"),
                        })
                    # Throttle leve para não forçar rate limit
                    time.sleep(0.25)

        df = pd.DataFrame(rows, columns=cols)
        df.to_csv(news_raw_csv, index=False)
        print(f"[news] OK -> {news_raw_csv} ({len(df)} linhas)")
    except Exception as e:
        # Segurança: mesmo em erro, gerar CSV vazio
        pd.DataFrame(columns=cols).to_csv(news_raw_csv, index=False)
        print(f"[news] ERRO não-fatal: {e}", file=sys.stderr)
        print(f"[news] OK -> {news_raw_csv} (0 linhas)")

if __name__ == "__main__":
    main()
