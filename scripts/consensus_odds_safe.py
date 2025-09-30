# scripts/consensus_odds_safe.py
"""
Gera um CSV de consenso a partir dos provedores disponíveis (TheOddsAPI e API-Football).

- NUNCA falha o job (SAFE).
- Faz logs/prints compatíveis com o workflow existente.
- Se nenhum provedor tiver linhas, cria um CSV vazio com header mínimo.

Mensagens esperadas (mantidas):
  [consensus-safe] lido <path> -> N linhas
  [consensus-safe] AVISO: nenhum provedor retornou odds. CSV vazio gerado.
  [consensus-safe] OK -> <out_path> (X linhas)
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Any

from scripts.csv_utils import (
    ensure_dir,
    read_csv_rows,
    write_csv_rows,
    lower_all,
)

# -------------------------------------------------------------

def _parse_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


NUM_KEYS_SYNONYMS = [
    ("home", "h", "odd_home", "home_odds"),
    ("draw", "d", "odd_draw", "draw_odds", "empate"),
    ("away", "a", "odd_away", "away_odds", "visitante"),
]


def _extract_triplet(row: Dict[str, Any]) -> tuple[float | None, float | None, float | None]:
    """
    Tenta extrair (home, draw, away) de uma linha com chaves possivelmente diferentes.
    """
    values: Dict[str, float | None] = {"home": None, "draw": None, "away": None}
    # mapa direto
    for canonical, *aliases in NUM_KEYS_SYNONYMS:
        # canonical é 'home'/'draw'/'away'
        for k in (canonical, *aliases):
            if k in row:
                v = _parse_float(row[k])
                if v is not None:
                    values[canonical] = v
                    break  # achou o primeiro válido

    return values["home"], values["draw"], values["away"]


def _key_for_match(row: Dict[str, Any]) -> tuple:
    """
    Chave de união robusta:
    - usa match_id se existir; senão, usa (date?, home_team, away_team) em lowercase.
    """
    rlow = lower_all(row)
    if "match_id" in row and str(row["match_id"]).strip():
        return ("id", str(row["match_id"]).strip())
    # fallback comum
    return (
        "teams",
        rlow.get("date") or rlow.get("utc_date") or "",
        rlow.get("home_team") or rlow.get("home") or rlow.get("mandante") or "",
        rlow.get("away_team") or rlow.get("away") or rlow.get("visitante") or "",
    )


def _load_provider(path: Path, provider: str) -> Dict[tuple, Dict[str, Any]]:
    rows = read_csv_rows(path)
    print(f"[consensus-safe] lido {path} -> {len(rows)} linhas")
    out: Dict[tuple, Dict[str, Any]] = {}
    for r in rows:
        key = _key_for_match(r)
        h, d, a = _extract_triplet(r)
        base = out.get(key, {
            "match_key": key,
            "home_team": r.get("home_team") or r.get("home") or "",
            "away_team": r.get("away_team") or r.get("away") or "",
            "sources": set(),
            "home": [],
            "draw": [],
            "away": [],
        })
        base["sources"].add(provider)
        if h is not None:
            base["home"].append(h)
        if d is not None:
            base["draw"].append(d)
        if a is not None:
            base["away"].append(a)
        out[key] = base
    return out


def _mean(xs: List[float]) -> float | None:
    return sum(xs) / len(xs) if xs else None


def build_consensus(rodada: str) -> List[Dict[str, Any]]:
    out_dir = Path("data/out") / rodada
    theodds = out_dir / "odds_theoddsapi.csv"
    apifoot = out_dir / "odds_apifootball.csv"

    index: Dict[tuple, Dict[str, Any]] = {}

    if theodds.exists():
        d = _load_provider(theodds, "theoddsapi")
        index.update({k: v for k, v in d.items()})
    else:
        print(f"[consensus-safe] AVISO: arquivo não encontrado: {theodds}")

    if apifoot.exists():
        d = _load_provider(apifoot, "apifootball")
        for k, v in d.items():
            if k in index:
                index[k]["sources"].update(v["sources"])
                index[k]["home"] += v["home"]
                index[k]["draw"] += v["draw"]
                index[k]["away"] += v["away"]
                # preenche nomes se estiverem vazios
                if not index[k].get("home_team"):
                    index[k]["home_team"] = v.get("home_team", "")
                if not index[k].get("away_team"):
                    index[k]["away_team"] = v.get("away_team", "")
            else:
                index[k] = v
    else:
        print(f"[consensus-safe] AVISO: arquivo não encontrado: {apifoot}")

    # monta linhas finais
    rows: List[Dict[str, Any]] = []
    for v in index.values():
        rows.append({
            "match_key": str(v["match_key"]),
            "home_team": v.get("home_team", ""),
            "away_team": v.get("away_team", ""),
            "cons_home": _mean(v["home"]),
            "cons_draw": _mean(v["draw"]),
            "cons_away": _mean(v["away"]),
            "sources": ",".join(sorted(v["sources"])) if v.get("sources") else "",
        })

    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Gera odds de consenso (SAFE)")
    parser.add_argument("--rodada", required=True)
    args = parser.parse_args()

    out_dir = Path("data/out") / args.rodada
    out_path = out_dir / "odds_consensus.csv"
    ensure_dir(out_path)

    rows = build_consensus(args.rodada)

    if not rows:
        print("[consensus-safe] AVISO: nenhum provedor retornou odds. CSV vazio gerado.")
        written = write_csv_rows(
            out_path,
            [],
            fieldnames=["match_key", "home_team", "away_team", "cons_home", "cons_draw", "cons_away", "sources"],
        )
        # escrito 0
        print(f"[consensus-safe] OK -> {out_path} ({written} linhas)")
        return 0

    written = write_csv_rows(out_path, rows)
    # há dois prints em algumas execuções; manter compatibilidade:
    print(f"[consensus-safe] OK -> {out_path} ({written} linhas)")
    print(f"[consensus-safe] OK -> {out_path} ({written} linhas)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
