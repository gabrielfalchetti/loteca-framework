# scripts/ingest_odds_theoddsapi.py
# busca odds na TheOddsAPI, faz matching contra matches_norm(_resolved).csv
# aprende aliases automaticamente e salva no cache
from __future__ import annotations
import os
import sys
import json
import time
import argparse
import requests
import pandas as pd
from typing import Dict, Any, Tuple, Optional
from _utils_norm import norm_name, best_match, token_key, load_json, dump_json

ODDS_URL = "https://api.the-odds-api.com/v4/sports/soccer/odds"

def fetch_theodds(api_key: str, regions: str, markets: str = "h2h") -> list[dict]:
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": markets,
        "oddsFormat": "decimal"
    }
    r = requests.get(ODDS_URL, params=params, timeout=40)
    if r.status_code == 401:
        print(f"[theoddsapi][ERROR] Unauthorized (401) — verifique a chave/limite")
        return []
    if r.status_code == 429:
        print(f"[theoddsapi][WARN] Rate limit — aguardando e repetindo...")
        time.sleep(2.0)
        r = requests.get(ODDS_URL, params=params, timeout=40)
    r.raise_for_status()
    try:
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []

def extract_h2h(event: dict) -> Optional[Tuple[float,float,float,str,str]]:
    """
    retorna (odds_home, odds_draw, odds_away, bookmaker, commence_time) se conseguir
    """
    commence = event.get("commence_time","")
    for bk in event.get("bookmakers", []) or []:
        key = bk.get("key","")
        for mk in bk.get("markets", []) or []:
            if mk.get("key") == "h2h":
                prices = mk.get("outcomes", []) or []
                # outcomes costumam vir com "name": home/away/draw
                home = next((o for o in prices if o.get("name","").lower() in ("home","home_team", event.get("home_team","").lower())), None)
                away = next((o for o in prices if o.get("name","").lower() in ("away","away_team", event.get("away_team","").lower())), None)
                draw = next((o for o in prices if o.get("name","").lower() in ("draw","empate")), None)
                # fallback: três outcomes sem nome padrão
                if not (home and draw and away) and len(prices) == 3:
                    # não temos ordem garantida; tenta deduzir pelo melhor/ pior preço é perigoso; mantém None
                    pass
                if home and draw and away:
                    try:
                        return (float(home["price"]), float(draw["price"]), float(away["price"]), key, commence)
                    except Exception:
                        continue
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="diretório OUT comum")
    ap.add_argument("--regions", required=True)
    ap.add_argument("--source_csv", required=True, help="matches_norm.csv ou matches_norm_resolved.csv")
    ap.add_argument("--catalog", default="data/ref/teams_catalog.parquet")
    ap.add_argument("--aliases_json", default="data/ref/aliases.json")
    args = ap.parse_args()

    os.makedirs(args.rodada, exist_ok=True)
    out_csv = os.path.join(args.rodada, "odds_theoddsapi.csv")

    api_key = os.getenv("THEODDS_API_KEY", "")
    if not api_key:
        print("::error::THEODDS_API_KEY não configurada")
        sys.exit(1)

    # leitura dos jogos-alvo
    df_src = pd.read_csv(args.source_csv)
    if "home" not in df_src.columns or "away" not in df_src.columns:
        print("::error::source_csv precisa de colunas home,away")
        sys.exit(2)

    # catálogos / aliases (para matching)
    cat = None
    if os.path.exists(args.catalog):
        cat = pd.read_parquet(args.catalog)[["team_id","name"]].copy()
        cat["canon"] = cat["name"].astype(str)
    aliases = load_json(args.aliases_json)

    # busca odds
    events = fetch_theodds(api_key, args.regions)
    if not events:
        # ainda salvamos CSV vazio caso o restante do pipeline espere o arquivo
        pd.DataFrame(columns=["team_home","team_away","odds_home","odds_draw","odds_away","bookmaker","commence_time"]).to_csv(out_csv, index=False)
        print(f"[theoddsapi]Eventos=0 | jogoselecionados={len(df_src)} | pareados=0 — salvo em {out_csv}")
        return

    # preparação de lista de jogos-alvo (preferindo IDs, se já resolvidos)
    want = []
    have_ids = set()
    use_ids = "home_team_id" in df_src.columns and "away_team_id" in df_src.columns
    for _, r in df_src.iterrows():
        item = {
            "home": str(r["home"]),
            "away": str(r["away"]),
            "home_id": int(r["home_team_id"]) if use_ids and pd.notna(r["home_team_id"]) else None,
            "away_id": int(r["away_team_id"]) if use_ids and pd.notna(r["away_team_id"]) else None,
        }
        want.append(item)
        if item["home_id"] and item["away_id"]:
            have_ids.add((item["home_id"], item["away_id"]))

    rows = []
    learned = {}

    # constrói pool de candidatos do catálogo para fuzzy (se necessário)
    candidates = []
    if cat is not None and len(cat):
        candidates = list(zip(cat["canon"].tolist(), cat["team_id"].tolist()))

    def match_side(observed: str) -> Optional[int]:
        # 1) cache de aliases
        key = norm_name(observed)
        aid = aliases.get(key, {}).get("team_id")
        if aid:
            return int(aid)
        # 2) catálogo fuzzy
        if candidates:
            tid, sc, canon = best_match(observed, candidates, min_score=0.90)
            if tid:
                learned[key] = {"team_id": int(tid), "source": "theoddsapi", "confidence": round(sc,3)}
                return int(tid)
        return None

    def is_target_pair(hname: str, aname: str) -> bool:
        # se temos IDs-alvo, tenta resolver nomes para IDs e comparar
        if have_ids:
            hid = match_side(hname)
            aid = match_side(aname)
            if hid and aid and (hid, aid) in have_ids:
                return True
            # sem IDs confiáveis, cai para comparação de nomes normalizados contra source_csv
        # fallback por nomes (agressivo)
        hk, ak = token_key(hname), token_key(aname)
        for w in want:
            if token_key(w["home"]) == hk and token_key(w["away"]) == ak:
                return True
        return False

    # varre eventos e guarda H2H
    for ev in events:
        hname = ev.get("home_team","") or ""
        aname = ev.get("away_team","") or ""
        if not hname or not aname:
            continue
        if not is_target_pair(hname, aname):
            continue
        h2h = extract_h2h(ev)
        if not h2h:
            continue
        oH, oD, oA, bk, t0 = h2h
        rows.append({
            "team_home": hname,
            "team_away": aname,
            "odds_home": oH,
            "odds_draw": oD,
            "odds_away": oA,
            "bookmaker": bk,
            "commence_time": t0,
        })

        # aprende aliases individuais (mesmo que par não esteja nos “want” por ID)
        for nm in (hname, aname):
            k = norm_name(nm)
            if k not in aliases and k not in learned:
                tid = match_side(nm)
                if tid:
                    learned[k] = {"team_id": int(tid), "source": "theoddsapi", "confidence": 0.95}

    # salva CSV (mesmo vazio)
    pd.DataFrame(rows).to_csv(out_csv, index=False)
    print(f"[theoddsapi]Eventos={len(events)} | jogoselecionados={len(df_src)} | pareados={len(rows)} — salvo em {out_csv}")

    # atualiza cache de aliases se houver aprendizados
    if learned:
        aliases.update(learned)
        dump_json(args.aliases_json, aliases)
        # relatório amigável
        aud = [{"observed": k, **v} for k, v in learned.items()]
        pd.DataFrame(aud).to_csv(os.path.join(args.rodada, "aliases_learned_theoddsapi.csv"), index=False)
        print(f"[theoddsapi] {len(learned)} aliases aprendidos → {args.aliases_json}")

if __name__ == "__main__":
    main()