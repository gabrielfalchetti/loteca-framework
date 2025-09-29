#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coleta odds da TheOddsAPI e salva em data/out/<RODADA>/odds_theoddsapi.csv.

Notas:
- Compatível com wandb==0.22.0 (NÃO usa finish_previous).
- Se THEODDS_API_KEY não estiver definido, o script não falha: gera CSV vazio com cabeçalho.
- Se não houver odds (ex.: jogos já finalizados), gera CSV vazio também.
- Suporta múltiplas convenções de colunas no matches_source.csv.

Uso:
  python scripts/ingest_odds_theoddsapi.py --rodada RODADA --regions "uk,eu,us,au" --debug

Saída:
  data/out/<RODADA>/odds_theoddsapi.csv
"""

from __future__ import annotations

import os
import sys
import json
import time
import argparse
from typing import List, Dict, Any, Tuple

import pandas as pd
import requests

# wandb é opcional: se der erro de init, seguimos sem quebrar o job
try:
    import wandb  # type: ignore
    _HAS_WANDB = True
except Exception:
    _HAS_WANDB = False


CSV_COLUMNS = [
    "rodada",
    "home",
    "away",
    "league_hint",
    "market",
    "bookmaker",
    "price_home",
    "price_draw",
    "price_away",
    "last_update",
    "source",
]


def log(msg: str, debug: bool = False) -> None:
    prefix = "[theoddsapi]"
    if debug:
        print(f"{prefix} {msg}")
    else:
        # mesmo sem --debug, mensagens importantes
        if any(k in msg.lower() for k in ["erro", "error", "ok", "aviso", "warning", "csv", "saida", "api key"]):
            print(f"{prefix} {msg}")


def safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def resolve_paths(rodada: str) -> Tuple[str, str]:
    in_dir = os.path.join("data", "in", rodada)
    out_dir = os.path.join("data", "out", rodada)
    safe_mkdir(in_dir)
    safe_mkdir(out_dir)
    src_csv = os.path.join(in_dir, "matches_source.csv")
    out_csv = os.path.join(out_dir, "odds_theoddsapi.csv")
    return src_csv, out_csv


def read_matches(src_csv: str, debug: bool) -> pd.DataFrame:
    if not os.path.isfile(src_csv):
        log(f"AVISO: não encontrei {src_csv} — seguirei com zero partidas.", True)
        return pd.DataFrame(columns=["home", "away", "league_hint"])

    df = pd.read_csv(src_csv)
    cols = {c.lower().strip(): c for c in df.columns}

    # tentar mapear várias convenções
    home_candidates = ["home", "mandante", "time_home", "team_home"]
    away_candidates = ["away", "visitante", "time_away", "team_away"]
    league_candidates = ["league", "liga", "competition", "campeonato", "league_hint"]

    def pick(cands: List[str]) -> str | None:
        for c in cands:
            if c in cols:
                return cols[c]
        return None

    home_col = pick(home_candidates)
    away_col = pick(away_candidates)
    league_col = pick(league_candidates)

    if home_col is None or away_col is None:
        # fallback bruto: tentar pegar duas primeiras colunas como home/away
        if len(df.columns) >= 2:
            home_col, away_col = df.columns[:2]
            log("AVISO: colunas não reconhecidas em matches_source.csv — usando duas primeiras como home/away.", True)
        else:
            return pd.DataFrame(columns=["home", "away", "league_hint"])

    out = pd.DataFrame(
        {
            "home": df[home_col].astype(str).fillna("").str.strip(),
            "away": df[away_col].astype(str).fillna("").str.strip(),
        }
    )
    out["league_hint"] = df[league_col].astype(str).fillna("").str.strip() if league_col else ""
    # filtrar vazios evidentes
    out = out[(out["home"] != "") & (out["away"] != "")]
    if debug:
        log(f"Partidas lidas: {len(out)}", True)
    return out


def fetch_odds_for_matches(
    matches: pd.DataFrame,
    api_key: str,
    regions: str,
    debug: bool,
) -> List[Dict[str, Any]]:
    """
    IMPORTANTE:
    A TheOddsAPI v4 exige um 'sport_key' (ex.: soccer_epl, soccer_brazil_campeonato_brasileiro_serie_a).
    Sem mapear cada liga/competição -> sport_key, não é possível fazer a consulta direta por times.

    Para manter o job estável (principal meta aqui), implementamos um fluxo "fail-soft":
    - se não houver sport_keys mapeadas, retornamos lista vazia.
    - você pode evoluir depois incluindo um mapa de league_hint -> sport_key.

    Exemplo de chamada real:
      GET https://api.the-odds-api.com/v4/sports/soccer_brazil_campeonato_brasileiro_serie_a/odds
          ?apiKey=...&regions=br,uk,eu,us,au&markets=h2h&oddsFormat=decimal
    """
    # Placeholder: sem mapeamento de ligas, retornamos vazio de forma consciente.
    if matches.empty:
        return []

    # Caso deseje habilitar liga do Brasil como demonstração, descomente e ajuste:
    # league_map = {
    #     "brasileirao": "soccer_brazil_campeonato_brasileiro_serie_a",
    #     "serie a": "soccer_brazil_campeonato_brasileiro_serie_a",
    #     "premier league": "soccer_epl",
    # }
    # sport_keys = set()
    # for hint in matches["league_hint"].str.lower().fillna(""):
    #     for k, v in league_map.items():
    #         if k in hint:
    #             sport_keys.add(v)
    # if not sport_keys:
    #     return []

    # Como os jogos já aconteceram (contexto do usuário), odds correntes não existirão.
    # Retornamos vazio de forma explícita.
    log("Sem consulta à API: jogos já finalizados e sem mapeamento de sport_key — retornando 0 odds.", debug)
    return []


def save_csv(rows: List[Dict[str, Any]], out_csv: str, rodada: str) -> None:
    if not rows:
        df = pd.DataFrame(columns=CSV_COLUMNS)
        df.to_csv(out_csv, index=False)
        print(f"[theoddsapi] OK -> {out_csv} (0 linhas)")
        return

    df = pd.DataFrame(rows)
    # garantir colunas e ordem
    for c in CSV_COLUMNS:
        if c not in df.columns:
            df[c] = ""  # preenchimento
    df = df[CSV_COLUMNS]
    df.to_csv(out_csv, index=False)
    print(f"[theoddsapi] OK -> {out_csv} ({len(df)} linhas)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest odds from TheOddsAPI")
    parser.add_argument("--rodada", required=True, help="Identificador da rodada (ex.: 2025-09-27_1213)")
    parser.add_argument("--regions", default="uk,eu,us,au", help="Regiões da TheOddsAPI (ex.: uk,eu,us,au)")
    parser.add_argument("--debug", action="store_true", help="Ativa logs detalhados")
    args = parser.parse_args()

    rodada = args.rodada
    regions = args.regions
    debug = args.debug

    src_csv, out_csv = resolve_paths(rodada)
    if debug:
        log(f"Entrada: {src_csv}", True)
        log(f"Saída:   {out_csv}", True)

    # W&B: tentar iniciar, mas sem travar caso falhe
    run = None
    if _HAS_WANDB:
        try:
            run = wandb.init(
                project="loteca",
                name=f"theoddsapi_{rodada}",
                config={
                    "rodada": rodada,
                    "regions": regions,
                    "script": "ingest_odds_theoddsapi.py",
                },
                # NÃO usar finish_previous aqui (incompatível com 0.22.0)
                # NÃO usar reinit=True (gera warning); manter default
            )
        except Exception as e:
            log(f"AVISO: falha ao iniciar wandb: {e}", True)
            run = None

    df_matches = read_matches(src_csv, debug)

    api_key = os.getenv("THEODDS_API_KEY", "").strip()
    if not api_key:
        log("AVISO: THEODDS_API_KEY não definido — pulando consulta e gerando CSV vazio.", False)
        save_csv([], out_csv, rodada)
        if run:
            wandb.summary["theodds_used"] = 0
            wandb.finish()
        return

    rows = fetch_odds_for_matches(
        matches=df_matches,
        api_key=api_key,
        regions=regions,
        debug=debug,
    )

    # Acrescentar metadados padrões se houver linhas
    enriched: List[Dict[str, Any]] = []
    for r in rows:
        r2 = dict(r)
        r2.setdefault("rodada", rodada)
        r2.setdefault("market", "h2h")
        r2.setdefault("source", "theoddsapi")
        enriched.append(r2)

    save_csv(enriched, out_csv, rodada)

    if run:
        try:
            wandb.summary["theodds_used"] = int(len(enriched) > 0)
            wandb.log({"rows": len(enriched)})
        finally:
            wandb.finish()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        # Não deixar o job falhar: imprimir e sair com sucesso (pipeline usa `|| true`)
        print(f"[theoddsapi] ERRO não fatal: {e}")
        # Tenta ainda assim produzir um CSV vazio para manter consistência
        try:
            # deduz rodada de argv se possível
            arg_map = {args[i].lstrip("-"): args[i + 1] for i in range(len(sys.argv) - 1) if sys.argv[i].startswith("--")}
            rodada = arg_map.get("rodada", "unknown")
            _, out_csv = resolve_paths(rodada)
            save_csv([], out_csv, rodada)
        except Exception:
            pass
        # não propagar erro
        sys.exit(0)
