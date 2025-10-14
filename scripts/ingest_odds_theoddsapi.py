# scripts/ingest_odds_theoddsapi.py
from __future__ import annotations

import argparse
import csv
import os
import sys
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timezone

# Import tolerante: funciona se _utils_norm.py estiver na raiz OU dentro de scripts/
try:
    from ._utils_norm import norm_name, best_match, token_key, load_json, dump_json  # type: ignore
except Exception:  # pragma: no cover
    try:
        from _utils_norm import norm_name, best_match, token_key, load_json, dump_json  # type: ignore
    except Exception as e:  # pragma: no cover
        print(f"[theoddsapi][FATAL] não foi possível importar _utils_norm: {e}", file=sys.stderr)
        sys.exit(1)

import requests
import pandas as pd
import numpy as np


def _read_matches(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Aceita tanto colunas canônicas quanto variantes comuns
    # Esperado: match_id,home,away[,league,date]
    rename_map = {}
    cols_lower = {c.lower(): c for c in df.columns}

    # Normaliza home/away
    for want in ("home", "away"):
        if want not in df.columns:
            if want in cols_lower:
                rename_map[cols_lower[want]] = want
    if rename_map:
        df = df.rename(columns=rename_map)

    if not {"home", "away"}.issubset(df.columns):
        raise ValueError("Arquivo de origem precisa ter colunas 'home' e 'away'")

    # Garante match_id
    if "match_id" not in df.columns:
        df["match_id"] = np.arange(1, len(df) + 1)

    # Cria colunas normalizadas (canônicas) só para matching interno
    df["home_norm"] = df["home"].astype(str).map(norm_name)
    df["away_norm"] = df["away"].astype(str).map(norm_name)
    return df


def _fetch_theodds(api_key: str, regions: str) -> List[dict]:
    """
    Chama TheOddsAPI para 'upcoming' (todos os esportes) e filtra soccer no matching.
    (Observação: como os 'sport_keys' variam, usar 'upcoming' é o mais estável para um primeiro corte.)
    """
    url = "https://api.the-odds-api.com/v4/sports/upcoming/odds"
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": "h2h",
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    r = requests.get(url, params=params, timeout=25)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        return []
    return data


def _is_soccer_event(ev: dict) -> bool:
    # Muitos retornos trazem 'sport_key' começando com 'soccer_'.
    skey = ev.get("sport_key", "") or ""
    return skey.startswith("soccer_") or skey == "soccer"


def _extract_h2h_odds(ev: dict) -> Optional[Tuple[str, str, Dict[str, float]]]:
    """
    Lê odds H2H e retorna (home_team, away_team, {'home': x, 'draw': y, 'away': z}) com médias por mercado.
    """
    home = ev.get("home_team") or ""
    away = ev.get("away_team") or ""
    if not home or not away:
        return None

    # Agregar odds (média) por outcome
    agg = {"home": [], "draw": [], "away": []}
    for bm in ev.get("bookmakers", []) or []:
        for mk in bm.get("markets", []) or []:
            if (mk.get("key") or "").lower() != "h2h":
                continue
            for outcome in mk.get("outcomes", []) or []:
                name = (outcome.get("name") or "").strip().lower()
                price = outcome.get("price")
                if price is None:
                    continue
                if name in ("home", home.lower()):
                    agg["home"].append(float(price))
                elif name in ("away", away.lower()):
                    agg["away"].append(float(price))
                elif name in ("draw", "empate", "tie", "x"):
                    agg["draw"].append(float(price))

    if not any(agg.values()):
        return None

    def _mean(x: List[float]) -> Optional[float]:
        return float(np.mean(x)) if x else None

    odds = {"home": _mean(agg["home"]), "draw": _mean(agg["draw"]), "away": _mean(agg["away"])}
    # Pelo menos dois mercados ajudam a estabilidade, mas se vier 1 já salvamos
    if all(v is None for v in odds.values()):
        return None

    return home, away, odds


def _pair_key(a: str, b: str) -> Tuple[str, str]:
    """Chave de par ordenada usando nomes normalizados."""
    na, nb = norm_name(a), norm_name(b)
    return (na, nb) if na <= nb else (nb, na)


def _match_events_to_source(
    src_df: pd.DataFrame, events: List[Tuple[str, str, Dict[str, float]]], score_cutoff: int = 86
) -> List[Dict[str, object]]:
    """
    Faz matching melhor-esforço entre (home, away) do source e (home, away) dos eventos TheOdds.
    Usa best_match por lado, exige score mínimo, e garante direção (home vs away).
    """
    out_rows: List[Dict[str, object]] = []

    # Produz lista de pares do source
    src_pairs = []
    for _, row in src_df.iterrows():
        src_pairs.append(
            dict(
                match_id=int(row["match_id"]),
                home=row["home"],
                away=row["away"],
                home_norm=row["home_norm"],
                away_norm=row["away_norm"],
            )
        )

    for (eh, ea, odds) in events:
        # primeiro normalize os candidatos do evento
        eh_n, ea_n = norm_name(eh), norm_name(ea)

        # Tenta encontrar um par no source que case EH->home e EA->away
        best_row = None
        best_score = -1

        for src in src_pairs:
            # match lado a lado
            h_cand, h_score = best_match(eh, [src["home"]], score_cutoff=score_cutoff)
            a_cand, a_score = best_match(ea, [src["away"]], score_cutoff=score_cutoff)
            if h_cand and a_cand:
                score = min(h_score, a_score)
                if score > best_score:
                    best_score = score
                    best_row = src

            # também tentar o invertido (alguns feeds podem trocar lados)
            h_cand2, h_score2 = best_match(eh, [src["away"]], score_cutoff=score_cutoff)
            a_cand2, a_score2 = best_match(ea, [src["home"]], score_cutoff=score_cutoff)
            if h_cand2 and a_cand2:
                score2 = min(h_score2, a_score2)
                if score2 > best_score:
                    # Mas se casou invertido, vamos inverter odds depois
                    best_score = score2
                    best_row = dict(
                        match_id=src["match_id"],
                        home=src["away"],
                        away=src["home"],
                        home_norm=src["away_norm"],
                        away_norm=src["home_norm"],
                        _inverted=True,
                    )

        if best_row and best_score >= score_cutoff:
            inv = bool(best_row.get("_inverted", False))
            # Se o par foi invertido, precisamos inverter as odds home/away
            o_home = odds.get("away") if inv else odds.get("home")
            o_away = odds.get("home") if inv else odds.get("away")
            o_draw = odds.get("draw")

            out_rows.append(
                dict(
                    match_id=best_row["match_id"],
                    team_home=best_row["home"],
                    team_away=best_row["away"],
                    odds_home=o_home,
                    odds_draw=o_draw,
                    odds_away=o_away,
                    source="theoddsapi",
                    matched_score=best_score,
                )
            )

    return out_rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório de saída desta rodada (ex.: data/out/<run_id>)")
    ap.add_argument("--regions", required=True, help='Regiões theoddsapi (ex.: "uk,eu,us,au")')
    ap.add_argument("--source_csv", required=True, help="CSV normalizado com as partidas (matches_norm.csv)")
    args = ap.parse_args()

    api_key = os.getenv("THEODDS_API_KEY", "")
    if not api_key:
        print("[theoddsapi][FATAL] THEODDS_API_KEY não configurada no ambiente.", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.rodada, exist_ok=True)
    out_csv = os.path.join(args.rodada, "odds_theoddsapi.csv")

    # Carrega source
    try:
        src = _read_matches(args.source_csv)
    except Exception as e:
        print(f"[theoddsapi][ERROR] Falha lendo source_csv: {e}", file=sys.stderr)
        # ainda assim escrevemos um CSV vazio com header para não quebrar downstream
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away", "source", "matched_score"])
        print(f"[theoddsapi]Arquivo {out_csv} gerado vazio (falha no source).")
        return

    # Busca odds
    events = []
    try:
        raw = _fetch_theodds(api_key, args.regions)
        # Filtra futebol e extrai odds H2H
        for ev in raw:
            if not _is_soccer_event(ev):
                continue
            parsed = _extract_h2h_odds(ev)
            if parsed:
                events.append(parsed)
    except requests.HTTPError as e:
        print(f"[theoddsapi][ERROR] Falha HTTP: {e}", file=sys.stderr)
    except Exception as e:
        print(f"[theoddsapi][ERROR] Erro geral consultando TheOddsAPI: {e}", file=sys.stderr)

    # Matching
    rows = _match_events_to_source(src, events, score_cutoff=86)

    # Escreve CSV (sempre com header)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away", "source", "matched_score"])
        for r in rows:
            w.writerow([
                r.get("match_id"),
                r.get("team_home"),
                r.get("team_away"),
                r.get("odds_home"),
                r.get("odds_draw"),
                r.get("odds_away"),
                r.get("source", "theoddsapi"),
                r.get("matched_score", 0),
            ])

    print(f"[theoddsapi]Eventos={len(events)} | jogoselecionados={len(src)} | pareados={len(rows)} — salvo em {out_csv}")


if __name__ == "__main__":
    main()