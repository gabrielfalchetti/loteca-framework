# scripts/harvest_aliases.py
from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import sys
from typing import Dict, Set, List

# ---- imports robustos do util ----
try:
    from ._utils_norm import norm_name, dump_json
except Exception:
    try:
        from scripts._utils_norm import norm_name, dump_json  # type: ignore
    except Exception:
        sys.path.append(os.path.join(os.getcwd(), "scripts"))
        from _utils_norm import norm_name, dump_json  # type: ignore


def _read_csv(path: str) -> List[dict]:
    if not path or not os.path.exists(path) or os.path.getsize(path) == 0:
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _parse_iso_when(s: str) -> dt.datetime | None:
    if not s:
        return None
    s = s.strip()
    try:
        # aceita “YYYY-MM-DD” ou ISO com Z/offset
        s = s.replace("Z", "+00:00")
        return dt.datetime.fromisoformat(s).replace(tzinfo=None)
    except Exception:
        return None


def harvest(source_csv: str, lookahead_hours: int) -> Dict[str, str]:
    rows = _read_csv(source_csv)
    if not rows:
        return {}

    now = dt.datetime.utcnow()
    horizon = now + dt.timedelta(hours=lookahead_hours)

    out: Dict[str, str] = {}
    seen: Set[str] = set()

    for r in rows:
        # tenta considerar janela temporal, se houver coluna de data
        when = (
            _parse_iso_when(r.get("date") or "")
            or _parse_iso_when(r.get("kickoff") or "")
            or _parse_iso_when(r.get("utc_datetime") or "")
        )
        if when is not None and not (now <= when <= horizon):
            continue

        for col in ("home", "away", "team_home", "team_away"):
            val = (r.get(col) or "").strip()
            if not val or val in seen:
                continue
            seen.add(val)
            key = norm_name(val)
            if key:
                # mapeia normalizado -> forma canônica encontrada
                out[key] = val

    return out


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Coleta aliases de times a partir do CSV normalizado da rodada."
    )
    # --- argumentos novos (usados no workflow atual) ---
    ap.add_argument("--source_csv", help="CSV normalizado (ex: data/out/<run>/matches_norm.csv)")
    ap.add_argument("--lookahead_hours", type=int, help="Janela em horas (ex: 72)")
    ap.add_argument("--out_json", help="Arquivo de saída (ex: data/aliases/auto_aliases.json)")

    # --- compatibilidade com versão legada ---
    ap.add_argument("--hours", type=int, help="(LEGADO) igual a --lookahead_hours")
    ap.add_argument("--regions", help="(LEGADO) ignorado; mantido por compatibilidade", default="")
    ap.add_argument("--out", help="(LEGADO) igual a --out_json")

    ap.add_argument("--default_csv", help="Fallback de CSV caso --source_csv não seja passado")

    args = ap.parse_args()

    # resolve parâmetros aceitando ambos formatos
    source_csv = args.source_csv or args.default_csv
    if not source_csv:
        # tenta um padrão conhecido do workflow
        source_csv = os.environ.get("SOURCE_CSV_NORM", "")
    lookahead = args.lookahead_hours if args.lookahead_hours is not None else args.hours
    out_json = args.out_json or args.out

    # sane defaults se algo faltar
    if not lookahead:
        lookahead = 72
    if not out_json:
        out_json = os.environ.get("AUTO_ALIASES_JSON", "data/aliases/auto_aliases.json")

    os.makedirs(os.path.dirname(out_json) or ".", exist_ok=True)

    aliases = harvest(source_csv, lookahead) if source_csv else {}

    # garante sempre um JSON válido
    dump_json(aliases or {}, out_json)
    print(f"[harvest_aliases] csv='{source_csv}' horas={lookahead} coletados={len(aliases)} → {out_json}")


if __name__ == "__main__":
    main()