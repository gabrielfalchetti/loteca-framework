#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_news.py
--------------
Agrega notícias por jogo e salva:
  data/out/{rodada}/news.csv  e  data/out/{rodada}/news.html

Compat:
- Aceita matches_source com colunas (home/away) OU (home_team/away_team).
- Mantém uso de aliases para normalização.
"""

import os
import re
import json
import argparse
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd

# --------------------------
# Utilitários
# --------------------------

def _load_aliases(path: str) -> Dict[str, str]:
    if path and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def _norm(name: str, aliases: Dict[str, str]) -> str:
    if not isinstance(name, str):
        return ""
    name = name.strip()
    return aliases.get(name, name)

def _read_matches(rodada: str, aliases: Dict[str, str]) -> pd.DataFrame:
    src = f"data/in/{rodada}/matches_source.csv"
    if not os.path.exists(src):
        raise FileNotFoundError(f"[news] arquivo não encontrado: {src}")

    df = pd.read_csv(src)

    # Aceita home/away ou home_team/away_team
    if "home" in df.columns and "away" in df.columns:
        df["home_team"] = df["home"].astype(str)
        df["away_team"] = df["away"].astype(str)
    elif "home_team" in df.columns and "away_team" in df.columns:
        pass
    else:
        raise RuntimeError("[news] matches_source precisa de colunas 'home/away' ou 'home_team/away_team'")

    if "match_id" not in df.columns:
        df["match_id"] = [f"m{i+1}" for i in range(len(df))]

    # Normaliza
    df["home_team"] = df["home_team"].apply(lambda x: _norm(x, aliases))
    df["away_team"] = df["away_team"].apply(lambda x: _norm(x, aliases))

    # (Opcional) data da partida se existir
    for cand in ["date", "match_date", "utc_date", "kickoff"]:
        if cand in df.columns:
            df[cand] = pd.to_datetime(df[cand], errors="coerce", utc=True)

    return df

# --------------------------
# Coletor "fake" (placeholder)
# Troque pelo seu provedor real de notícias quando quiser.
# --------------------------

def _dummy_news_for(match_row: pd.Series) -> List[Dict[str, str]]:
    home = match_row.get("home_team", "")
    away = match_row.get("away_team", "")
    pair = f"{home} x {away}".strip()
    # Placeholder mínimo: retorna vazio (não quebra pipeline)
    return []

# --------------------------
# HTML simples
# --------------------------

def _to_html_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "<p>Sem notícias coletadas.</p>"
    rows = []
    rows.append("<table border='1' cellspacing='0' cellpadding='6'>")
    rows.append("<thead><tr><th>match_id</th><th>home</th><th>away</th><th>title</th><th>source</th><th>url</th><th>ts</th></tr></thead>")
    rows.append("<tbody>")
    for _, r in df.iterrows():
        url = r.get("url","")
        title = r.get("title","")
        rows.append(
            f"<tr><td>{r.get('match_id','')}</td>"
            f"<td>{r.get('home','')}</td>"
            f"<td>{r.get('away','')}</td>"
            f"<td>{title}</td>"
            f"<td>{r.get('source','')}</td>"
            f"<td><a href='{url}' target='_blank'>{url}</a></td>"
            f"<td>{r.get('ts','')}</td></tr>"
        )
    rows.append("</tbody></table>")
    return "\n".join(rows)

# --------------------------
# Main
# --------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--provider", default="auto")
    ap.add_argument("--lang", default="pt")
    ap.add_argument("--regions", default="br")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--limit", type=int, default=6)
    ap.add_argument("--aliases", default="data/aliases_br.json")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    outdir = f"data/out/{args.rodada}"
    os.makedirs(outdir, exist_ok=True)

    aliases = _load_aliases(args.aliases)
    matches = _read_matches(args.rodada, aliases)

    records: List[Dict[str, str]] = []

    for _, row in matches.iterrows():
        news_items = _dummy_news_for(row)  # troque pelo conector real quando desejar
        # Normaliza saída
        for item in news_items[: max(args.limit, 0)]:
            records.append({
                "match_id": row.get("match_id"),
                "home": row.get("home_team"),
                "away": row.get("away_team"),
                "title": item.get("title",""),
                "source": item.get("source",""),
                "url": item.get("url",""),
                "ts": item.get("ts",""),
            })

    news_df = pd.DataFrame(records, columns=["match_id","home","away","title","source","url","ts"])
    news_csv = f"{outdir}/news.csv"
    news_df.to_csv(news_csv, index=False)
    print(f"[news] OK -> {news_csv} ({len(news_df)} linhas)")

    html = _to_html_table(news_df)
    with open(f"{outdir}/news.html","w",encoding="utf-8") as f:
        f.write(html)
    print(f"[news] OK -> {outdir}/news.html")


if __name__ == "__main__":
    main()
