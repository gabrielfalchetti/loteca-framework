#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coleta manchetes recentes (futebol) via NewsAPI e grava OUT_DIR/news.csv.
Falha se não houver chave/retorno.

Uso:
  python scripts/news_ingest_safe.py --out-dir data/out/<ID> --debug
Requisitos:
  NEWSAPI_KEY no env
"""

import argparse, csv, os, sys, time
from datetime import datetime, timedelta
try:
    import requests
except Exception as e:
    print(f"[news] ERRO: 'requests' indisponível: {e}", file=sys.stderr); sys.exit(7)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--debug", action="store_true")
    args=ap.parse_args()

    out_dir=args.out_dir
    os.makedirs(out_dir, exist_ok=True)
    out_csv=os.path.join(out_dir, "news.csv")

    key=os.getenv("NEWSAPI_KEY","").strip()
    if not key:
        print("::error::[news] NEWSAPI_KEY não definido.", file=sys.stderr); sys.exit(7)

    # Últimas 48h
    from_date=(datetime.utcnow()-timedelta(days=2)).date().isoformat()
    url="https://newsapi.org/v2/everything"
    params={"q":"(soccer OR futebol) AND (lesão OR injury OR lineup OR escalação)",
            "language":"pt", "from":from_date, "sortBy":"relevancy", "pageSize":50, "apiKey":key}

    try:
        r=requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        js=r.json()
    except requests.HTTPError as e:
        print(f"::error::[news] HTTPError: {e}", file=sys.stderr); sys.exit(7)
    except requests.RequestException as e:
        print(f"::error::[news] Erro de rede: {e}", file=sys.stderr); sys.exit(7)

    arts=js.get("articles",[])
    if not arts:
        print("::error::[news] Nenhuma notícia retornada.", file=sys.stderr); sys.exit(7)

    with open(out_csv,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=["publishedAt","source","title","url"])
        w.writeheader()
        for a in arts:
            w.writerow({
                "publishedAt": a.get("publishedAt",""),
                "source": (a.get("source") or {}).get("name",""),
                "title": a.get("title","")[:300],
                "url": a.get("url","")
            })

    if not os.path.isfile(out_csv) or os.path.getsize(out_csv)==0:
        print("::error::[news] news.csv não gerado.", file=sys.stderr); sys.exit(7)

    print(f"[news] OK -> {out_csv} linhas={len(arts)}")

if __name__=="__main__":
    main()