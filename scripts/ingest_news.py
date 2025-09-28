#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_news.py — coleta notícias por time/rodada (NewsAPI.org OU Google News RSS).
Saídas:
  data/out/<RODADA>/news.csv  (source,title,url,published_at,team_hit,match_id,home,away)
  data/out/<RODADA>/news.html (lista navegável)

Uso:
  python scripts/ingest_news.py --rodada 2025-09-27_1213 --provider auto --lang pt --regions br --days 7 --limit 6 --aliases data/aliases_br.json
"""

from __future__ import annotations
import argparse, os, sys, math, time, json, unicodedata
from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Any
import urllib.request, urllib.parse, urllib.error
import xml.etree.ElementTree as ET

import pandas as pd
import numpy as np

BR_TZ = timezone(timedelta(hours=-3))

def _norm(s: str) -> str:
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s = s.lower().strip()
    s = s.replace(".", " ")
    return " ".join(s.split())

def _apply_alias(name: str, aliases: Dict[str, List[str]]) -> str:
    n = _norm(name)
    if n in aliases:
        return n
    for canon, vars_ in aliases.items():
        if n == canon:
            return canon
        for v in vars_:
            if _norm(v) == n:
                return canon
    return n

def _load_aliases(path: str) -> Dict[str, List[str]]:
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        ali: Dict[str, List[str]] = {}
        for k, vals in raw.items():
            canon = _norm(k)
            vec = list({ _norm(v) for v in (vals or []) })
            ali[canon] = vec
        return ali
    except Exception as e:
        print(f"[news] AVISO: falha ao ler aliases '{path}': {e}", file=sys.stderr)
        return {}

def _http_get_json(url: str, headers: Dict[str,str] | None = None, retry=3, sleep=0.6) -> Dict[str,Any] | List[Any] | None:
    last_err = None
    for i in range(retry):
        try:
            req = urllib.request.Request(url, headers=headers or {}, method="GET")
            with urllib.request.urlopen(req, timeout=25) as resp:
                data = resp.read()
                try:
                    return json.loads(data.decode("utf-8", errors="ignore"))
                except Exception:
                    return None
        except Exception as e:
            last_err = str(e)
            time.sleep(sleep*(i+1))
    if last_err:
        print(f"[news] ERRO GET {url} -> {last_err}", file=sys.stderr)
    return None

def _http_get_text(url: str, headers: Dict[str,str] | None = None, retry=3, sleep=0.6) -> str | None:
    last_err = None
    for i in range(retry):
        try:
            req = urllib.request.Request(url, headers=headers or {}, method="GET")
            with urllib.request.urlopen(req, timeout=25) as resp:
                return resp.read().decode("utf-8", errors="ignore")
        except Exception as e:
            last_err = str(e)
            time.sleep(sleep*(i+1))
    if last_err:
        print(f"[news] ERRO GET {url} -> {last_err}", file=sys.stderr)
    return None

def _read_matches(rodada: str, aliases: Dict[str,List[str]]) -> pd.DataFrame:
    path = os.path.join("data","in",rodada,"matches_source.csv")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"[news] arquivo não encontrado: {path}")
    df = pd.read_csv(path)
    if "home" not in df.columns or "away" not in df.columns:
        raise RuntimeError("[news] matches_source precisa de colunas 'home' e 'away'")
    if "match_id" not in df.columns:
        df.insert(0, "match_id", range(1, len(df)+1))
    df["home_n"] = df["home"].apply(lambda x: _apply_alias(x, aliases))
    df["away_n"] = df["away"].apply(lambda x: _apply_alias(x, aliases))
    return df

def _newsapi_query(q: str, lang: str, days: int, key: str, page_size: int = 6) -> List[Dict[str,Any]]:
    base = "https://newsapi.org/v2/everything"
    from_param = (date.today() - timedelta(days=days)).isoformat()
    params = {
        "q": q,
        "language": lang,
        "from": from_param,
        "sortBy": "publishedAt",
        "pageSize": str(page_size)
    }
    url = f"{base}?{urllib.parse.urlencode(params)}"
    headers = {"X-Api-Key": key, "User-Agent": "loteca-news/1.0"}
    js = _http_get_json(url, headers=headers)
    arts: List[Dict[str,Any]] = []
    if isinstance(js, dict) and js.get("status") == "ok":
        for a in (js.get("articles") or []):
            arts.append({
                "source": (a.get("source") or {}).get("name") or "NewsAPI",
                "title": a.get("title") or "",
                "url": a.get("url") or "",
                "published_at": a.get("publishedAt") or "",
            })
    return arts

def _googlenews_rss(q: str, lang: str, regions: str, days: int, limit: int = 6) -> List[Dict[str,Any]]:
    # Ex.: q="palmeiras OR corinthians when:7d"
    # hl=pt-BR, gl=BR, ceid=BR:pt-419
    hl = "pt-BR" if lang.startswith("pt") else "en-US"
    gl = "BR" if "br" in regions.lower() else "US"
    ceid = "BR:pt-419" if gl == "BR" else "US:en"
    query = f"{q} when:{days}d"
    base = "https://news.google.com/rss/search"
    full = f"{base}?{urllib.parse.urlencode({'q': query, 'hl': hl, 'gl': gl, 'ceid': ceid})}"
    xml_txt = _http_get_text(full, headers={"User-Agent": "loteca-news/1.0"})
    arts: List[Dict[str,Any]] = []
    if not xml_txt:
        return arts
    try:
        root = ET.fromstring(xml_txt)
        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub = (item.findtext("{http://purl.org/dc/elements/1.1/}date") or item.findtext("pubDate") or "").strip()
            src_el = item.find(".//source")
            src = (src_el.text if src_el is not None else "GoogleNews").strip()
            arts.append({
                "source": src,
                "title": title,
                "url": link,
                "published_at": pub
            })
            if len(arts) >= limit:
                break
    except Exception as e:
        print(f"[news] AVISO: falha parse RSS: {e}", file=sys.stderr)
    return arts

def _dedup(rows: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    seen = set(); out = []
    for r in rows:
        key = (r.get("title","").strip(), r.get("url","").strip())
        if key in seen:
            continue
        seen.add(key); out.append(r)
    return out

def _build_html(rows: List[Dict[str,Any]], rodada: str, out_html: str) -> None:
    head = f"""<!doctype html>
<html lang="pt-br">
<meta charset="utf-8">
<title>Notícias — {rodada}</title>
<style>
body{{font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; margin:20px; color:#111}}
h1{{margin:0 0 8px 0}} .meta{{color:#666; font-size:12px; margin-bottom:16px}}
.card{{border:1px solid #eee; border-radius:10px; padding:12px; margin:10px 0}}
.card h3{{margin:0 0 4px 0; font-size:16px}}
.card .src{{color:#555; font-size:12px}}
.card .teams{{color:#222; font-size:13px; margin-top:6px}}
</style>
<h1>Notícias — {rodada}</h1>
<div class="meta">Gerado em {datetime.now(BR_TZ).isoformat(timespec='seconds')}</div>
"""
    parts = [head]
    if not rows:
        parts.append("<p><em>Nenhuma notícia encontrada.</em></p></html>")
    else:
        for r in rows:
            t = r.get("title","")
            u = r.get("url","")
            s = r.get("source","")
            teams = f"{r.get('home','')} x {r.get('away','')}" if r.get("home") and r.get("away") else (r.get("team_hit","") or "")
            parts.append(f'<div class="card"><h3><a href="{u}" target="_blank" rel="noopener noreferrer">{t}</a></h3><div class="src">{s} — {r.get("published_at","")}</div><div class="teams">{teams}</div></div>')
        parts.append("</html>")
    with open(out_html, "w", encoding="utf-8") as f:
        f.write("\n".join(parts))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--provider", default="auto", choices=["auto","newsapi","googlerss"])
    ap.add_argument("--lang", default="pt")
    ap.add_argument("--regions", default="br")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--limit", type=int, default=6, help="limite por time/consulta")
    ap.add_argument("--aliases", default="data/aliases_br.json")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rodada = args.rodada
    out_dir = os.path.join("data","out",rodada); os.makedirs(out_dir, exist_ok=True)
    out_csv  = os.path.join(out_dir, "news.csv")
    out_html = os.path.join(out_dir, "news.html")

    aliases = _load_aliases(args.aliases)
    matches = _read_matches(rodada, aliases)

    news_key = os.getenv("NEWSAPI_KEY","").strip()
    provider = args.provider
    if provider == "auto":
        provider = "newsapi" if news_key else "googlerss"

    rows: List[Dict[str,Any]] = []
    for _, m in matches.iterrows():
        h = m["home"]; a = m["away"]
        hq = _apply_alias(h, aliases); aq = _apply_alias(a, aliases)
        # consulta “time A OR time B” para pegar contexto do confronto
        query = f'"{hq}" OR "{aq}"'
        arts: List[Dict[str,Any]] = []
        if provider == "newsapi":
            q = f'{hq} OR {aq}'
            arts = _newsapi_query(q=q, lang=args.lang, days=args.days, key=news_key, page_size=args.limit)
        else:
            q = f'{hq} OR {aq}'
            arts = _googlenews_rss(q=q, lang=args.lang, regions=args.regions, days=args.days, limit=args.limit)

        for arow in arts:
            rows.append({
                "source": arow.get("source",""),
                "title": arow.get("title",""),
                "url": arow.get("url",""),
                "published_at": arow.get("published_at",""),
                "team_hit": f"{hq} | {aq}",
                "match_id": m.get("match_id",""),
                "home": h,
                "away": a
            })
        # pequena pausa para respeitar rate-limits (sobretudo NewsAPI)
        time.sleep(0.25)

    rows = _dedup(rows)
    df = pd.DataFrame(rows, columns=["source","title","url","published_at","team_hit","match_id","home","away"])
    df.to_csv(out_csv, index=False)
    _build_html(rows, rodada, out_html)
    print(f"[news] OK -> {out_csv} ({len(df)} linhas)")
    print(f"[news] OK -> {out_html}")

if __name__ == "__main__":
    main()
