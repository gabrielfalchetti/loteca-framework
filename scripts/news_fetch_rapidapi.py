# scripts/news_fetch_rapidapi.py
from __future__ import annotations
import argparse, json, os, sys, time
from typing import Any, Dict, List
import requests

BASE_URL = "https://google-news13.p.rapidapi.com"  # endpoint leve
HOST = "google-news13.p.rapidapi.com"

def headers() -> Dict[str, str]:
    key = os.environ.get("RAPIDAPI_KEY", "").strip()
    if not key:
        print("[news] ERRO: defina RAPIDAPI_KEY", file=sys.stderr); sys.exit(7)
    return {"X-RapidAPI-Key": key, "X-RapidAPI-Host": HOST}

def get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BASE_URL}/{path.lstrip('/')}"
    r = requests.get(url, headers=headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", required=True, help="ex.: Palmeiras OR Flamengo")
    ap.add_argument("--lang", default="pt-BR")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--limit", type=int, default=20)
    args = ap.parse_args()

    out_dir = f"data/out/{args.rodada}/news_new"
    os.makedirs(out_dir, exist_ok=True)
    data: List[Dict[str, Any]] = []

    # endpoint 'search' desta API (simplificado)
    js = get("search", {"keyword": args.query, "lr": args.lang})
    for item in js.get("items", [])[: args.limit]:
        data.append({
            "title": item.get("title"),
            "link": item.get("newsUrl") or item.get("link"),
            "source": item.get("publisher"),
            "published": item.get("published"),
        })
    with open(f"{out_dir}/news.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[news] OK -> {out_dir}/news.json ({len(data)} itens)")

if __name__ == "__main__":
    main()
