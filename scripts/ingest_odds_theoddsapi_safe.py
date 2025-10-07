#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Coleta odds H2H no TheOddsAPI e escreve OUT_DIR/odds_theoddsapi.csv.
Modo endurecido: falha quando faltar qualquer requisito/retorno.

Uso:
  python scripts/ingest_odds_theoddsapi_safe.py \
    --rodada data/out/<ID> \
    --regions "uk,eu,us,au" \
    --sports "soccer_brazil_campeonato,soccer_argentina_primera_division" \
    --debug

Requisitos:
- THEODDS_API_KEY em env
- data/in/matches_source.csv com cabeçalhos: match_id,home,away (source opcional)
"""

import argparse, csv, os, sys, time, unicodedata
from datetime import datetime
from typing import Dict, List, Tuple
try:
    import requests
except Exception as e:
    print(f"[theoddsapi] ERRO: 'requests' indisponível: {e}", file=sys.stderr); sys.exit(2)

def norm(s:str)->str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()

def match_team_to_book(team: str, book_team: str)->bool:
    a, b = norm(team), norm(book_team)
    if a==b: return True
    a2 = a.replace(" fc","").replace(" sc","").replace(" ac","")
    b2 = b.replace(" fc","").replace(" sc","").replace(" ac","")
    return a2 in b2 or b2 in a2

def load_matches()->List[dict]:
    path = "data/in/matches_source.csv"
    if not os.path.isfile(path):
        print(f"::error::[theoddsapi] {path} ausente.", file=sys.stderr); sys.exit(2)
    rows=[]
    with open(path, "r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for col in ("match_id","home","away"):
            if col not in rd.fieldnames:
                print(f"::error::[theoddsapi] cabeçalho '{col}' ausente em {path}", file=sys.stderr); sys.exit(2)
        for r in rd:
            if r.get("home") and r.get("away"):
                rows.append({"match_id": r.get("match_id","").strip(),
                             "home": r["home"].strip(),
                             "away": r["away"].strip(),
                             "source": (r.get("source") or "").strip()})
    if not rows:
        print("::error::[theoddsapi] nenhuma linha válida em matches_source.csv", file=sys.stderr); sys.exit(2)
    return rows

def fetch_market(api_key: str, region: str, sport: str, debug=False)->list:
    url=f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
    params={"apiKey": api_key, "regions": region, "markets":"h2h"}
    if debug: print(f"[theoddsapi][DEBUG] GET {url} {params}")
    r=requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    time.sleep(0.35)  # respeito básico a rate-limit
    return r.json()

def pick_odds_for_match(event: dict, home: str, away: str)->Tuple[float,float,float]:
    # retorna (home, draw, away) quando possível; Caso o esporte não tenha empate, draw=None
    for book in event.get("bookmakers", []):
        for mk in book.get("markets", []):
            if mk.get("key")!="h2h": continue
            home_o = draw_o = away_o = None
            for outc in mk.get("outcomes", []):
                name=outc.get("name","")
                price=outc.get("price", None)
                if price is None: continue
                if match_team_to_book(home, name): home_o=float(price)
                elif match_team_to_book(away, name): away_o=float(price)
                elif norm(name) in ("draw","empate","tie"): draw_o=float(price)
            if home_o and away_o:
                return home_o, draw_o, away_o
    return (None, None, None)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório de saída (ex.: data/out/<ID>)")
    ap.add_argument("--regions", required=True, help="Regiões do TheOddsAPI (ex.: uk,eu,us,au)")
    ap.add_argument("--sports", required=False, default="soccer", help="Lista de esportes separados por vírgula")
    ap.add_argument("--debug", action="store_true")
    args=ap.parse_args()

    out_dir=args.rodada
    os.makedirs(out_dir, exist_ok=True)
    out_csv=os.path.join(out_dir, "odds_theoddsapi.csv")

    api_key=os.getenv("THEODDS_API_KEY","").strip()
    if not api_key:
        print("::error::[theoddsapi] THEODDS_API_KEY não definido.", file=sys.stderr); sys.exit(2)

    matches=load_matches()
    sports=[s.strip() for s in args.sports.split(",") if s.strip()]
    regions=[r.strip() for r in args.regions.split(",") if r.strip()]

    collected=[]
    hits=0
    try:
        for sport in sports:
            for region in regions:
                data=fetch_market(api_key, region, sport, args.debug)
                # index por teams para acelerar
                for ev in data:
                    title = ev.get("home_team","") + " vs " + ev.get("away_team","")
                # agora casamos por cada jogo declarado
                for m in matches:
                    # varremos eventos e tentamos coletar da primeira combinação que bate
                    found=False
                    for ev in data:
                        h, d, a = pick_odds_for_match(ev, m["home"], m["away"])
                        if h and a:
                            collected.append({
                                "match_id": m["match_id"],
                                "home": m["home"],
                                "away": m["away"],
                                "region": region,
                                "sport": sport,
                                "odds_home": h,
                                "odds_draw": d if d is not None else "",
                                "odds_away": a,
                                "last_update": ev.get("commence_time",""),
                                "source": "theoddsapi"
                            })
                            hits+=1
                            found=True
                            break
                    if not found:
                        # mantemos registro “no_match” para auditoria (NÃO inventa dados)
                        collected.append({
                            "match_id": m["match_id"], "home": m["home"], "away": m["away"],
                            "region": region, "sport": sport,
                            "odds_home":"", "odds_draw":"", "odds_away":"",
                            "last_update":"", "source":"theoddsapi:no_match"
                        })
    except requests.HTTPError as e:
        print(f"::error::[theoddsapi] HTTPError: {e}", file=sys.stderr); sys.exit(2)
    except requests.RequestException as e:
        print(f"::error::[theoddsapi] Erro de rede: {e}", file=sys.stderr); sys.exit(2)
    except Exception as e:
        print(f"::error::[theoddsapi] Falha inesperada: {e}", file=sys.stderr); sys.exit(2)

    # endurecido: pelo menos 1 hit com odds preenchidas
    if hits == 0:
        print("::error::[theoddsapi] Nenhuma odd válida encontrada (hits=0). Abortando.", file=sys.stderr)
        sys.exit(2)

    # escreve CSV final
    with open(out_csv,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=[
            "match_id","home","away","region","sport",
            "odds_home","odds_draw","odds_away","last_update","source"
        ])
        w.writeheader()
        for r in collected:
            # somente linhas com odds preenchidas
            if r["odds_home"] != "" and r["odds_away"] != "":
                w.writerow(r)

    # checagem final
    if not os.path.isfile(out_csv) or os.path.getsize(out_csv)==0:
        print("::error::[theoddsapi] odds_theoddsapi.csv não gerado ou vazio.", file=sys.stderr); sys.exit(2)

    print(f"[theoddsapi] OK -> {out_csv} (hits={hits})")

if __name__=="__main__":
    main()