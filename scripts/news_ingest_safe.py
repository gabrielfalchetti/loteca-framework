#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ingest simples do NewsAPI (opcional, não bloqueia pipeline).
Salva news.csv com colunas: source,title,url,publishedAt
"""
import os
import csv
import argparse
import requests
from datetime import datetime, timedelta

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", required=True)
    args = p.parse_args()

    key = os.getenv("NEWSAPI_KEY","").strip()
    os.makedirs(args.out_dir, exist_ok=True)
    outp = os.path.join(args.out_dir, "news.csv")

    if not key:
        with open(outp,"w",newline="",encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["source","title","url","publishedAt"])
            w.writeheader()
        print("[news] NEWSAPI_KEY ausente — criando news.csv vazio.")
        return

    # busca últimas 24h por "soccer OR futebol"
    fr = (datetime.utcnow()-timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = "https://newsapi.org/v2/everything"
    params = {"q":"(soccer OR futebol) AND (lesões OR injuries OR escalação OR lineup)",
              "from":fr, "sortBy":"publishedAt", "language":"pt", "pageSize":20, "apiKey":key}
    r = requests.get(url, params=params, timeout=30)
    rows=[]
    if r.status_code==200:
        data=r.json()
        for a in data.get("articles",[]):
            rows.append({
                "source": a.get("source",{}).get("name"),
                "title": a.get("title"),
                "url": a.get("url"),
                "publishedAt": a.get("publishedAt")
            })

    with open(outp,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["source","title","url","publishedAt"])
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[news] OK -> {outp} ({len(rows)} linhas)")

if __name__ == "__main__":
    main()