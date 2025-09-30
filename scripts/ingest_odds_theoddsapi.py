#!/usr/bin/env python
from __future__ import annotations

# HOTFIX de import p/ end2end
import sys, json, datetime as dt
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import argparse, csv
from typing import Dict, Any, List, Tuple
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

def parse_time(iso_s: str) -> dt.datetime | None:
    # TheOddsAPI → field "commence_time" em ISO UTC
    try:
        return dt.datetime.fromisoformat(iso_s.replace("Z","+00:00")).astimezone(dt.timezone.utc)
    except Exception:
        return None

def match_score(a: str, b: str) -> int:
    # robusto a ordem dos tokens e variações
    return fuzz.token_set_ratio(a, b)

def flatten_odds(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flat = []
    for ev in events:
        # compat: alguns retornam "home_team"/"away_team"; outros usam "teams": {"home","away"}
        home = ev.get("home_team") or ev.get("teams", {}).get("home")
        away = ev.get("away_team") or ev.get("teams", {}).get("away")
        ctime = ev.get("commence_time")
        if not home or not away:
            continue
        for bm in ev.get("bookmakers", []):
            bname = bm.get("title")
            for m in bm.get("markets", []):
                mkey = m.get("key")
                for o in m.get("outcomes", []):
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
    """
    Casa por (home,away) com fuzzy (rapidfuzz) e por data (±window_days).
    Também tenta casamento com home/away invertido se falhar.
    """
    out, unmatched = [], []

    # index por data (dia UTC) para reduzir busca
    by_day: Dict[str, List[Dict[str,Any]]] = {}
    for r in prov_rows:
        t = parse_time(r.get("commence_time") or "")
        day = (t or dt.datetime(1970,1,1, tzinfo=dt.timezone.utc)).date().isoformat()
        by_day.setdefault(day, []).append(r)

    def rows_in_window(match_date: dt.date) -> List[Dict[str,Any]]:
        rows = []
        for d in range(-abs(window_days), abs(window_days)+1):
            day = (match_date + dt.timedelta(days=d)).isoformat()
            rows.extend(by_day.get(day, []))
        return rows

    for m in matches:
        # data do match (se não tiver, usamos somente nomes)
        if m.get("date"):
            try:
                mdate = dt.date.fromisoformat(m["date"][:10])
            except Exception:
                mdate = None
        else:
            mdate = None

        candidates = prov_rows if mdate is None else rows_in_window(mdate)
        if not candidates and mdate is not None:
            candidates = prov_rows  # fallback sem filtro de data

        mh_raw, ma_raw = m["home"], m["away"]
        mh, ma = canonical(mh_raw), canonical(ma_raw)

        best_rows = []
        best_score = -1

        for r in candidates:
            ph_raw, pa_raw = r["prov_home"], r["prov_away"]
            ph, pa = canonical(ph_raw), canonical(pa_raw)

            # score direto
            s1 = min(match_score(mh, ph), match_score(ma, pa))
            # score invertido (caso o provedor tenha invertido mandante)
            s2 = min(match_score(mh, pa), match_score(ma, ph))
            s = max(s1, s2)

            if s >= fuzzy_thr and s >= best_score:
                if s > best_score:
                    best_rows = []
                best_score = s
                best_rows.append((s, r))

        if best_rows:
            for _, r in best_rows:
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
    ap.add_argument("--window", type=int, default=2, help="Tolerância de datas (±N dias) p/ casar TheOddsAPI.")
    ap.add_argument("--fuzzy", type=int, default=86, help="Limiar fuzzy (0-100) token_set_ratio.")
    ap.add_argument("--aliases", type=str, default=None, help="JSON com aliases extras (opcional).")
    args = ap.parse_args()

    # aliases extras (se fornecido)
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

    # salvar amostra de eventos p/ depurar
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
