#!/usr/bin/env python
from __future__ import annotations

# HOTFIX de import p/ end2end
import sys, json, datetime as dt
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import argparse, csv
from typing import Dict, Any, List, Tuple, Optional
from utils.oddsapi import resolve_brazil_soccer_sport_keys, fetch_odds_for_sport, OddsApiError
from utils.match_normalize import canonical, extend_aliases
from rapidfuzz import fuzz

def read_matches(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        print(f"[ERRO] Arquivo não encontrado: {path}")
        raise SystemExit(2)
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        needed = {"match_id","home","away"}
        if not needed.issubset(reader.fieldnames or []):
            print("Error: matches_source.csv precisa de colunas: match_id,home,away[,date].")
            raise SystemExit(2)
        return list(reader)

def parse_time(iso_s: str) -> Optional[dt.datetime]:
    try:
        return dt.datetime.fromisoformat(iso_s.replace("Z","+00:00")).astimezone(dt.timezone.utc)
    except Exception:
        return None

def score_pair(h1: str, h2: str, a1: str, a2: str) -> Tuple[int,int,int]:
    # retorna (score_min, score_home, score_away)
    sh = fuzz.token_set_ratio(h1, h2)
    sa = fuzz.token_set_ratio(a1, a2)
    return min(sh, sa), sh, sa

def flatten_odds(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flat = []
    for ev in events:
        home = ev.get("home_team") or ev.get("teams", {}).get("home")
        away = ev.get("away_team") or ev.get("teams", {}).get("away")
        ctime = ev.get("commence_time")
        if not home or not away:
            continue
        for bm in ev.get("bookmakers", []) or []:
            bname = bm.get("title")
            for m in bm.get("markets", []) or []:
                mkey = m.get("key")
                for o in m.get("outcomes", []) or []:
                    flat.append({
                        "prov_home": home, "prov_away": away,
                        "commence_time": ctime,
                        "bookmaker": bname, "market": mkey,
                        "selection": o.get("name"), "price": o.get("price")
                    })
    return flat

def match_provider_events(
    matches: List[Dict[str,str]],
    prov_rows: List[Dict[str, Any]],
    window_days: int,
    fuzzy_thr: int
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    out, unmatched = [], []

    # Index por dia UTC
    by_day: Dict[str, List[Dict[str,Any]]] = {}
    for r in prov_rows:
        t = parse_time(r.get("commence_time") or "")
        day = (t or dt.datetime(1970,1,1, tzinfo=dt.timezone.utc)).date().isoformat()
        by_day.setdefault(day, []).append(r)

    def rows_in_window(match_date: dt.date) -> List[Dict[str,Any]]:
        rows = []
        for d in range(-abs(window_days), abs(window_days)+1):
            rows.extend(by_day.get((match_date + dt.timedelta(days=d)).isoformat(), []))
        return rows

    for m in matches:
        mdate = None
        if m.get("date"):
            try:
                mdate = dt.date.fromisoformat(m["date"][:10])
            except Exception:
                mdate = None

        candidates = prov_rows if mdate is None else rows_in_window(mdate)
        if not candidates and mdate is not None:
            candidates = prov_rows  # fallback sem filtro de data

        mh_raw, ma_raw = m["home"], m["away"]
        mh, ma = canonical(mh_raw), canonical(ma_raw)

        best: Optional[Tuple[int,Dict[str,Any],str,int,int]] = None
        # best = (score_min, row, orientacao, score_home, score_away), orientacao ∈ {"dir","inv"}

        for r in candidates:
            ph_raw, pa_raw = r["prov_home"], r["prov_away"]
            ph, pa = canonical(ph_raw), canonical(pa_raw)

            s_dir, sh_dir, sa_dir = score_pair(mh, ph, ma, pa)   # direto
            s_inv, sh_inv, sa_inv = score_pair(mh, pa, ma, ph)   # invertido

            # escolhe a melhor orientação, mas exige que **ambos** os placares individuais >= fuzzy_thr
            cand = None
            if s_dir >= s_inv and sh_dir >= fuzzy_thr and sa_dir >= fuzzy_thr:
                cand = (s_dir, r, "dir", sh_dir, sa_dir)
            elif s_inv > s_dir and sh_inv >= fuzzy_thr and sa_inv >= fuzzy_thr:
                cand = (s_inv, r, "inv", sh_inv, sa_inv)

            if cand and (best is None or cand[0] > best[0]):
                best = cand

        if best:
            _, r, orient, sh, sa = best
            # dedup por (bookmaker, market, selection) — 1 linha por combinação
            seen = set()
            bkey = (r["bookmaker"] or "", r["market"] or "", r["selection"] or "")
            if bkey not in seen:
                seen.add(bkey)
                out.append({
                    "match_id": m["match_id"], "home": mh_raw, "away": ma_raw,
                    "bookmaker": r["bookmaker"], "market": r["market"],
                    "selection": r["selection"], "price": r["price"]
                })
        else:
            reason = "no_match_theoddsapi"
            if mdate is not None and not rows_in_window(mdate):
                reason = f"no_events_in_window_{window_days}d"
            unmatched.append({"match_id": m["match_id"], "home": mh_raw, "away": ma_raw, "motivo": reason})

    return out, unmatched

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--regions", default="uk,eu,us,au")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--window", type=int, default=3, help="Tolerância de datas (±N dias) p/ casar TheOddsAPI.")
    ap.add_argument("--fuzzy", type=int, default=93, help="Limiar fuzzy (0-100) token_set_ratio. Recomendado 90–95.")
    ap.add_argument("--aliases", type=str, default=None, help="JSON com aliases extras (opcional).")
    args = ap.parse_args()

    if args.aliases:
        p = Path(args.aliases)
        if p.exists():
            try:
                extra = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(extra, dict):
                    extend_aliases(extra)
                    print(f"[theoddsapi] Aliases extras carregados: {len(extra)} chaves")
            except Exception as e:
                print(f"[theoddsapi] AVISO: falha ao ler aliases {p}: {e}")

    base_in = Path("data/in") / args.rodada
    base_out = Path("data/out") / args.rodada
    base_dbg = base_out / "debug"
    base_out.mkdir(parents=True, exist_ok=True); base_dbg.mkdir(parents=True, exist_ok=True)

    rows = read_matches(base_in / "matches_source.csv")

    print("[theoddsapi] Resolvendo sport_keys BR…")
    try:
        keys = resolve_brazil_soccer_sport_keys()
    except OddsApiError as e:
        print(f"[theoddsapi] ERRO: {e}"); keys = []
    print(f"[theoddsapi] candidatos: {keys}")

    all_events = []
    per_key_counts = {}
    for k in keys:
        ev = fetch_odds_for_sport(k, regions=args.regions.split(","))
        per_key_counts[k] = len(ev)
        if ev: all_events.extend(ev)

    (base_dbg / "theoddsapi_counts.json").write_text(
        json.dumps({"counts": per_key_counts}, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (base_dbg / "theoddsapi_sample_events.json").write_text(
        json.dumps(all_events[:40], ensure_ascii=False, indent=2), encoding="utf-8"
    )

    flat = flatten_odds(all_events)
    matched, unmatched = match_provider_events(rows, flat, window_days=args.window, fuzzy_thr=args.fuzzy)

    out_csv = base_out / "odds_theoddsapi.csv"
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=["match_id","home","away","bookmaker","market","selection","price"])
        wr.writeheader(); wr.writerows(matched)

    if unmatched:
        um_csv = base_out / "unmatched_theoddsapi.csv"
        with um_csv.open("w", newline="", encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=["match_id","home","away","motivo"])
            wr.writeheader(); wr.writerows(unmatched)
        print(f"[theoddsapi] AVISO: {len(unmatched)} sem casamento → {um_csv}")

    print(f"[theoddsapi] OK -> {out_csv} ({len(matched)} linhas)")
    raise SystemExit(0)

if __name__ == "__main__":
    main()
