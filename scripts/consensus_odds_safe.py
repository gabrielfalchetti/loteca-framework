#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/consensus_odds_safe.py

Gera odds de consenso a partir das fontes coletadas (TheOddsAPI e API-Football),
aplicando normalização de nomes via aliases e casando com a whitelist da rodada.

Saída: {OUT_DIR}/odds_consensus.csv com colunas:
match_id,team_home,team_away,odds_home,odds_draw,odds_away,source

Uso:
  python -m scripts.consensus_odds_safe --rodada <RODADA_ID|CAMINHO_OUT_DIR>

Comportamento:
- --rodada pode ser um ID numérico (ex.: 1759959606) ou o próprio caminho
  (ex.: data/out/1759959606). Se for ID, o script resolve para data/out/<ID>.
- Lê aliases de todos os CSVs em data/aliases/*.csv (se existirem) e aplica
  nos nomes de times (home/away) antes de casar com a whitelist.
- Faz merge entre whitelist e odds normalizadas (por nh, na) com sufixos
  explícitos para evitar conflitos: suffixes=("_wl", "_odds").
- Se não houver match, escreve cabeçalho vazio e falha com código 1,
  imprimindo diagnóstico.

Observação:
- Este script NÃO cria dados fictícios. Se as odds não existirem ou não
  casarem com os nomes dos times da whitelist, a saída ficará vazia e o
  processo encerrará com erro explícito.

"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import sys
from typing import Dict, List, Tuple

import pandas as pd


EXIT_CODE = 6  # manter compatível com o workflow (passo "consensus")


# ----------------------------
# Logging helpers
# ----------------------------
def _is_debug() -> bool:
    val = os.environ.get("DEBUG", "").strip().lower()
    return val in ("1", "true", "yes", "y")


def info(msg: str) -> None:
    print(msg, flush=True)


def debug(msg: str) -> None:
    if _is_debug():
        print(f"[consensus][DEBUG] {msg}", flush=True)


def warn(msg: str) -> None:
    print(f"Warning: {msg}", flush=True)


def err(msg: str) -> None:
    print(f"::error::{msg}", flush=True)


# ----------------------------
# Utilidades de normalização
# ----------------------------
_norm_space_re = re.compile(r"\s+")
_norm_punct_re = re.compile(r"[^\w\s]+")


def normalize_name(s: str) -> str:
    """
    Normaliza strings de nomes de times:
    - caixa baixa
    - remove acentuação simples (via NFKD + ascii)
    - remove pontuação
    - collapse de espaços
    """
    if not isinstance(s, str):
        return ""
    import unicodedata

    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = _norm_punct_re.sub(" ", s)
    s = _norm_space_re.sub(" ", s).strip()
    return s


def load_alias_maps(root_dir: str = "data/aliases") -> List[Tuple[Dict[str, str], bool]]:
    """
    Lê todos os CSVs de aliases em data/aliases/*.csv.

    Retorna uma lista de tuplas (alias_map, remove_when_empty_to)
      - alias_map: dict from->to
      - remove_when_empty_to: True se 'to' vazio significa remover o termo
        (ex.: global.csv com 'fc' -> '')
    """
    results: List[Tuple[Dict[str, str], bool]] = []

    if not os.path.isdir(root_dir):
        debug(f"aliases: diretório inexistente '{root_dir}' (pulando).")
        return results

    files = sorted(glob.glob(os.path.join(root_dir, "*.csv")))
    if not files:
        debug("aliases: nenhum arquivo *.csv encontrado (pulando).")
        return results

    for path in files:
        try:
            df = pd.read_csv(path)
            if not {"from", "to"}.issubset(df.columns.str.lower()):
                warn(f"aliases: arquivo sem colunas from/to: {path} (pulando).")
                continue

            # normalizar colunas from/to
            cols = {c.lower(): c for c in df.columns}
            df = df.rename(columns=cols)

            # construir dict
            amap: Dict[str, str] = {}
            remove_mode = False
            for _, r in df.iterrows():
                fr = str(r.get("from", "") or "").strip()
                to = str(r.get("to", "") or "").strip()
                if not fr:
                    continue
                fr_norm = normalize_name(fr)
                to_norm = normalize_name(to)
                amap[fr_norm] = to_norm
                if to_norm == "":
                    # se existir pelo menos uma linha com to vazio, ativa remove_mode
                    remove_mode = True

            results.append((amap, remove_mode))
            debug(f"aliases: carregado {path} (linhas={len(amap)}, remove_mode={remove_mode})")
        except Exception as e:
            warn(f"aliases: falha ao ler {path}: {e} (pulando).")

    return results


def apply_aliases(name: str, alias_sets: List[Tuple[Dict[str, str], bool]]) -> str:
    """
    Aplica múltiplos mapas de aliases sobre um nome (normalizado).
    - Se remove_when_empty_to=True e o 'to' for vazio, remove a ocorrência do 'from'.
    - Caso contrário, substitui exatamente o 'from' pelo 'to'.

    Estratégia simples e determinística.
    """
    n = normalize_name(name)

    # Heurística: aplicar substituições completas de token
    def _token_subst(text: str, frm: str, to: str) -> str:
        # substituição por palavra inteira; se 'to' vazio, remove
        pattern = r"\b" + re.escape(frm) + r"\b"
        return re.sub(pattern, to, text).strip()

    for amap, remove_mode in alias_sets:
        if not amap:
            continue
        # primeiro, substituições diretas (exatas)
        for frm, to in amap.items():
            if not frm:
                continue
            if remove_mode and to == "":
                # remover frm
                n = _token_subst(n, frm, "")
            else:
                n = _token_subst(n, frm, to)
        # collapse final
        n = _norm_space_re.sub(" ", n).strip()

    return n


# ----------------------------
# Carregamento de fontes
# ----------------------------
def resolve_out_dir(rodada_arg: str) -> str:
    """
    Se rodada_arg for diretório, usa direto.
    Se for ID (e.g., '1759959606'), resolve para data/out/<ID>.
    """
    if os.path.isdir(rodada_arg):
        return rodada_arg
    # tenta data/out/<ID>
    candidate = os.path.join("data", "out", str(rodada_arg))
    return candidate


def read_csv_safe(path: str, required: List[str] | None = None) -> pd.DataFrame:
    if not os.path.isfile(path):
        debug(f"read_csv_safe: arquivo ausente -> {path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        if required:
            missing = [c for c in required if c not in df.columns]
            if missing:
                warn(f"apifootball colunas faltantes em {path}; ignorando.")
                return pd.DataFrame()
        return df
    except Exception as e:
        warn(f"falha ao ler {path}: {e} (ignorando).")
        return pd.DataFrame()


def load_whitelist(out_dir: str) -> pd.DataFrame:
    path = os.path.join(out_dir, "matches_whitelist.csv")
    req = ["match_id", "team_home", "team_away", "source"]
    wl = read_csv_safe(path, required=req)
    if wl.empty:
        err(f"matches_whitelist.csv ausente/vazio em {path}")
        sys.exit(EXIT_CODE)
    return wl


def load_odds(out_dir: str) -> pd.DataFrame:
    # TheOddsAPI
    path_theodds = os.path.join(out_dir, "odds_theoddsapi.csv")
    # API-Football
    path_apifoot = os.path.join(out_dir, "odds_apifootball.csv")

    cols_odds = ["home", "away", "odds_home", "odds_draw", "odds_away"]
    cols_theodds_req = ["home", "away", "odds_home", "odds_draw", "odds_away"]
    cols_apifoot_req = ["home", "away", "odds_home", "odds_draw", "odds_away"]

    df_theodds = read_csv_safe(path_theodds, required=cols_theodds_req)
    if not df_theodds.empty:
        df_theodds["source"] = "theoddsapi"

    df_apifoot = read_csv_safe(path_apifoot, required=cols_apifoot_req)
    if not df_apifoot.empty:
        df_apifoot["source"] = "apifootball"

    if df_theodds.empty and df_apifoot.empty:
        err("Nenhuma fonte de odds carregada (theoddsapi/apifootball vazias).")
        return pd.DataFrame()

    frames = []
    if not df_theodds.empty:
        frames.append(df_theodds[cols_odds + ["source"]])
    if not df_apifoot.empty:
        frames.append(df_apifoot[cols_odds + ["source"]])

    all_odds = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return all_odds


def aggregate_consensus(all_odds: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega odds por par (nh,na) normalizado, tirando a média simples de cada odd.
    """
    if all_odds.empty:
        return pd.DataFrame()

    # normalizar nomes crus para chaves
    all_odds = all_odds.copy()
    all_odds["nh"] = all_odds["home"].map(normalize_name)
    all_odds["na"] = all_odds["away"].map(normalize_name)

    grp = (
        all_odds.groupby(["nh", "na"], dropna=False)[["odds_home", "odds_draw", "odds_away"]]
        .mean()
        .reset_index()
    )
    return grp


# ----------------------------
# Pipeline principal
# ----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="ID da rodada (ex.: 17599...) ou caminho data/out/<ID>")
    args = parser.parse_args()

    out_dir = resolve_out_dir(args.rodada)
    info("[consensus] ===================================================")
    info("[consensus] GERANDO ODDS CONSENSUS")
    info(f"[consensus] RODADA_DIR: {out_dir}")
    info("[consensus] ===================================================")

    # 1) carregar whitelist
    wl = load_whitelist(out_dir)
    debug(f"Aliases carregados: (serão listados individualmente abaixo)")

    # 2) carregar odds das fontes
    all_odds = load_odds(out_dir)
    if all_odds.empty:
        out = os.path.join(out_dir, "odds_consensus.csv")
        # escrever cabeçalho vazio e falhar
        with open(out, "w", newline="", encoding="utf-8") as f:
            f.write("match_id,team_home,team_away,odds_home,odds_draw,odds_away,source\n")
        err(f"odds_consensus.csv não gerado (fontes vazias).")
        sys.exit(EXIT_CODE)

    debug(f"Carregado odds from theoddsapi: {int((all_odds['source']=='theoddsapi').sum())} linhas")
    debug(f"Carregado odds from apifootball: {int((all_odds['source']=='apifootball').sum())} linhas")

    # 3) carregar aliases
    alias_sets = load_alias_maps("data/aliases")
    # (mensagem de diagnóstico de quantos arquivos)
    debug(f"aliases: total arquivos lidos = {len(alias_sets)}")

    # 4) normalizar nomes conforme aliases
    def norm_with_aliases(x: str) -> str:
        return apply_aliases(x, alias_sets)

    wl = wl.copy()
    wl["nh"] = wl["team_home"].map(norm_with_aliases)
    wl["na"] = wl["team_away"].map(norm_with_aliases)

    all_odds = all_odds.copy()
    all_odds["home_norm"] = all_odds["home"].map(norm_with_aliases)
    all_odds["away_norm"] = all_odds["away"].map(norm_with_aliases)

    # Para agregação, use as colunas normalizadas
    agg = (
        all_odds.assign(nh=lambda d: d["home_norm"], na=lambda d: d["away_norm"])
        .groupby(["nh", "na"], dropna=False)[["odds_home", "odds_draw", "odds_away"]]
        .mean()
        .reset_index()
    )

    if agg.empty:
        out = os.path.join(out_dir, "odds_consensus.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            f.write("match_id,team_home,team_away,odds_home,odds_draw,odds_away,source\n")
        err("Agregação vazia após normalização — verifique aliases.")
        sys.exit(EXIT_CODE)

    # 5) merge whitelist x agg (por nh,na) COM sufixos explícitos
    try:
        m = wl.merge(
            agg,
            on=["nh", "na"],
            how="left",
            suffixes=("_wl", "_odds"),  # evita conflito de colunas
        )
    except Exception as e:
        err(f"Falha no merge whitelist x odds agregadas: {e}")
        sys.exit(EXIT_CODE)

    # 6) filtrar só casados
    matched = m.dropna(subset=["odds_home", "odds_draw", "odds_away"]).copy()

    # 7) diagnóstico de não casados
    not_matched = wl.merge(
        matched[["match_id", "nh", "na"]],
        on=["match_id", "nh", "na"],
        how="left",
        indicator=True,
        suffixes=("_wl", "_mt")
    )
    not_matched = not_matched[not_matched["_merge"] == "left_only"]

    if not matched.empty:
        debug("Preview odds_consensus (até 10):")
        preview = matched[["match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away"]].head(10)
        for _, r in preview.iterrows():
            debug(
                f" match_id={r['match_id']}  {r['team_home']} x {r['team_away']}  "
                f"({r['odds_home']:.4f}, {r['odds_draw']:.4f}, {r['odds_away']:.4f})"
            )

    if not not_matched.empty:
        # imprime aviso com até 10 jogos não casados
        warn("[consensus] Alguns jogos da whitelist não casaram com odds. Mostrando até 10:")
        head10 = not_matched.head(10)
        # tenta manter a mesma formatação do log anterior
        print("match_id   team_home team_away              match_key")
        for _, r in head10.iterrows():
            mk = f"{normalize_name(str(r['team_home']))}__vs__{normalize_name(str(r['team_away']))}"
            print(f"{int(r['match_id']):8d} {r['team_home']}     {r['team_away']} {mk}")

    # 8) preparar saída
    out_cols = ["match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away"]
    out_df = matched[out_cols].copy()
    out_df["source"] = "consensus"

    out_path = os.path.join(out_dir, "odds_consensus.csv")
    out_df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)

    info(f"[consensus] OK -> {out_path}")


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        # preservar códigos de saída que o workflow espera
        raise
    except Exception as e:
        err(f"Falha inesperada: {e}")
        sys.exit(EXIT_CODE)