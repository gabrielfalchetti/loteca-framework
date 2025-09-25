#!/usr/bin/env python3
import argparse, time, re, yaml, requests, feedparser, pandas as pd
from selectolax.parser import HTMLParser
from urllib.parse import urljoin
from pathlib import Path

# Palavras-chave (simples) para lineup/lesões/suspensões
KEYWORDS = r"(escala.c.o|line[- ]?up|desfalque|suspens.|les.o|prov.vel)"

def load_cfg():
    with open("config/config.yaml","r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def fetch_rss(url):
    d = feedparser.parse(url)
    items = []
    for e in d.entries:
        published = getattr(e, "published", "") or getattr(e, "updated", "") or ""
        items.append({
            "title": e.get("title",""),
            "summary": e.get("summary",""),
            "link": e.get("link",""),
            "published": published
        })
    return items

def fetch_html_list(url):
    # Respeite ToS/robots de cada site; aqui é um exemplo simples
    r = requests.get(url, timeout=20, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    html = HTMLParser(r.text)
    articles = []
    for a in html.css("a"):
        href = a.attributes.get("href","")
        if not href or href.startswith("#"): 
            continue
        full = urljoin(url, href)
        txt = (a.text() or "").strip()
        if re.search(KEYWORDS, txt, flags=re.I):
            articles.append({"title": txt, "link": full})
    return articles

def scrape_article(url):
    r = requests.get(url, timeout=20, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    h = HTMLParser(r.text)
    body = " ".join([p.text().strip() for p in h.css("article p, .content p, .post p")])
    return body[:4000]

def main(rodada):
    cfg = load_cfg()
    rows = []

    # RSS
    for url in cfg["news"]["rss_sources"]:
        for it in fetch_rss(url):
            text = (it["title"] + " " + it["summary"]).lower()
            if re.search(KEYWORDS, text, flags=re.I):
                rows.append({"source": url, **it, "kind": "rss"})

    # HTML (lista + artigo)
    for base in cfg["news"]["html_sources"]:
        for art in fetch_html_list(base):
            time.sleep(0.2)  # cortesia
            body = scrape_article(art["link"])
            if re.search(KEYWORDS, body, flags=re.I):
                rows.append({"source": base, **art, "summary": body[:400], "kind": "html"})
            time.sleep(0.2)

    df = pd.DataFrame(rows).drop_duplicates(subset=["link"])
    out_path = cfg["paths"]["news_out"].replace("${rodada}", rodada)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"[OK] lineups/news → {out_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--rodada", required=True)
    main(ap.parse_args().rodada)
