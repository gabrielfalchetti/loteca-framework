# scripts/news_fetch_all_news.py
from __future__ import annotations
import argparse, json, os, sys
from typing import Any, Dict, List, Tuple
import requests

# ---- Config dos provedores ----
RAPID_BASE = "https://google-news13.p.rapidapi.com/search"
RAPID_HOST = "google-news13.p.rapidapi.com"

def _rapid_key() -> str | None:
    key = os.environ.get("RAPIDAPI_KEY", "").strip()
    return key or None

def _rapid_headers() -> Dict[str, str]:
    key = _rapid_key()
    if not key:
        raise RuntimeError("RAPIDAPI_KEY ausente")
    return {"X-RapidAPI-Key": key, "X-RapidAPI-Host": RAPID_HOST}

def fetch_rapid(query: str, lang: str, limit: int) -> List[Dict[str, Any]]:
    if not _rapid_key():
        return []
    r = requests.get(
        RAPID_BASE,
        headers=_rapid_headers(),
        params={"keyword": query, "lr": lang},
        timeout=30,
    )
    r.raise_for_status()
    js = r.json()
    items: List[Dict[str, Any]] = []
    for it in js.get("items", [])[: limit]:
        url = it.get("newsUrl") or it.get("link")
        items.append({
            "title": it.get("title"),
            "description": it.get("snippet"),
            "url": url,
            "source": it.get("publisher"),
            "publishedAt": it.get("published"),
            "provider": "rapidapi-google-news",
        })
    return items

def safe_load_json(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        js = json.load(f)
    # garante provider
    out: List[Dict[str, Any]] = []
    for a in js:
        a2 = dict(a)
        a2.setdefault("provider", "newsapi")
        out.append(a2)
    return out

def dedup_by_url(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for it in items:
        url = (it.get("url") or "").strip()
        key = url.lower()
        if not url or key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out

def main() -> None:
    ap = argparse.ArgumentParser(description="Combina NewsAPI (pré-gerado) com RapidAPI Google News (online), deduplica e salva um JSON único.")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--query", required=True)
    ap.add_argument("--lang", default="pt-BR")
    ap.add_argument("--rapid_limit", type=int, default=30)
    args = ap.parse_args()

    out_dir = f"data/out/{args.rodada}/news_new"
    os.makedirs(out_dir, exist_ok=True)

    # 1) carrega se já tiver gerado com scripts/news_fetch_newsapi.py
    newsapi_json = os.path.join(out_dir, "news_newsapi.json")
    base_items = safe_load_json(newsapi_json)

    # 2) busca RapidAPI (opcional)
    rapid_items: List[Dict[str, Any]] = []
    try:
        rapid_items = fetch_rapid(args.query, args.lang, args.rapid_limit)
    except Exception as e:
        print(f"[news_all] AVISO RapidAPI: {e}")

    merged = dedup_by_url(base_items + rapid_items)
    out_json = os.path.join(out_dir, "news_all.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    print(f"[news_all] OK -> {out_json} ({len(merged)} itens)")

if __name__ == "__main__":
    main()
