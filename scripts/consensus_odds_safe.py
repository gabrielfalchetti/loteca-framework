# scripts/consensus_odds_safe.py
from __future__ import annotations

import argparse
import os
import sys
import csv
import math
from typing import Dict, Tuple, List

# -------- imports robustos do util --------
try:
    # se estiver dentro do pacote "scripts"
    from ._utils_norm import norm_name, load_json
except Exception:
    try:
        # se for chamado como "python -m scripts.xxx"
        from scripts._utils_norm import norm_name, load_json  # type: ignore
    except Exception:
        # último recurso: adiciona ./scripts ao PYTHONPATH em runtime
        sys.path.append(os.path.join(os.getcwd(), "scripts"))
        from _utils_norm import norm_name, load_json  # type: ignore


REQUIRED_COLS = ["team_home", "team_away", "odds_home", "odds_draw", "odds_away"]


def _read_csv(path: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return out
    with open(path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            out.append({k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()})
    return out


def _has_required_cols(rows: List[Dict[str, str]]) -> bool:
    if not rows:
        return False
    hdr = set(rows[0].keys())
    return all(c in hdr for c in REQUIRED_COLS)


def _to_float(x: str) -> float:
    try:
        v = float(x)
        if math.isnan(v) or v <= 0:
            return float("nan")
        return v
    except Exception:
        return float("nan")


def _key_match(h: str, a: str) -> Tuple[str, str]:
    return (norm_name(h), norm_name(a))


def _average(a: float, b: float) -> float:
    if math.isnan(a) and math.isnan(b):
        return float("nan")
    if math.isnan(a):
        return b
    if math.isnan(b):
        return a
    return (a + b) / 2.0


def _write_csv(path: str, rows: List[Dict[str, str]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=REQUIRED_COLS)
        wr.writeheader()
        for r in rows:
            wr.writerow({c: r.get(c, "") for c in REQUIRED_COLS})


def build_consensus(rodada_dir: str, strict: bool) -> str:
    out_path = os.path.join(rodada_dir, "odds_consensus.csv")

    # fontes
    p_theodds = os.path.join(rodada_dir, "odds_theoddsapi.csv")
    p_apifoot = os.path.join(rodada_dir, "odds_apifootball.csv")

    the_rows = _read_csv(p_theodds)
    api_rows = _read_csv(p_apifoot)

    if not the_rows and not api_rows:
        if strict:
            raise RuntimeError("Sem odds de nenhuma fonte")
        # cria arquivo vazio com header para não quebrar
        _write_csv(out_path, [])
        return out_path

    # se só houver TheOddsAPI, repassa
    if the_rows and not api_rows:
        if not _has_required_cols(the_rows):
            raise RuntimeError("TheOddsAPI não possui colunas necessárias")
        _write_csv(out_path, the_rows)
        return out_path

    # se só houver API-Football, repassa
    if api_rows and not the_rows:
        if not _has_required_cols(api_rows):
            raise RuntimeError("API-Football não possui colunas necessárias")
        _write_csv(out_path, api_rows)
        return out_path

    # ambas as fontes: mescla por (team_home, team_away) normalizados
    if not _has_required_cols(the_rows):
        raise RuntimeError("TheOddsAPI sem colunas necessárias")
    if not _has_required_cols(api_rows):
        raise RuntimeError("API-Football sem colunas necessárias")

    idx_api: Dict[Tuple[str, str], Dict[str, str]] = {
        _key_match(r["team_home"], r["team_away"]): r for r in api_rows
    }

    consensus: List[Dict[str, str]] = []
    for r in the_rows:
        k = _key_match(r["team_home"], r["team_away"])
        base = {
            "team_home": r["team_home"],
            "team_away": r["team_away"],
        }
        th = _to_float(r["odds_home"])
        td = _to_float(r["odds_draw"])
        ta = _to_float(r["odds_away"])

        if k in idx_api:
            rr = idx_api[k]
            ah = _to_float(rr["odds_home"])
            ad = _to_float(rr["odds_draw"])
            aa = _to_float(rr["odds_away"])
            out_row = {
                **base,
                "odds_home": f"{_average(th, ah):.6f}" if not math.isnan(_average(th, ah)) else "",
                "odds_draw": f"{_average(td, ad):.6f}" if not math.isnan(_average(td, ad)) else "",
                "odds_away": f"{_average(ta, aa):.6f}" if not math.isnan(_average(ta, aa)) else "",
            }
        else:
            out_row = {
                **base,
                "odds_home": r["odds_home"],
                "odds_draw": r["odds_draw"],
                "odds_away": r["odds_away"],
            }
        consensus.append(out_row)

    _write_csv(out_path, consensus)
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Pasta data/out/<RUN_ID> com os CSVs de odds")
    ap.add_argument("--strict", action="store_true", help="Falhar se não houver odds")
    args = ap.parse_args()

    out = build_consensus(args.rodada, args.strict)
    print(f"[consensus] OK — gerado {out}")

if __name__ == "__main__":
    main()