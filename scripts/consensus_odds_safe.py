#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/consensus_odds_safe.py

Gera odds de consenso a partir das fontes coletadas (TheOddsAPI e API-Football),
aplicando normalização/aliases de nomes e casando com a whitelist da rodada.

Saída: {OUT_DIR}/odds_consensus.csv com colunas:
match_id,team_home,team_away,odds_home,odds_draw,odds_away,source

Uso:
  python -m scripts.consensus_odds_safe --rodada <RODADA_ID|CAMINHO_OUT_DIR>

Regras importantes:
- NÃO cria dados fictícios. Se não houver odds ou casamento, o script falha (exit 6).
- Quando não casa nada, para o pipeline antes do predict, com diagnóstico claro.
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

EXIT_CODE = 6  # mantém compatível com o workflow

# ========== Log helpers ==========
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

# ========== Normalização ==========
_norm_space_re = re.compile(r"\s+")
_norm_punct_re = re.compile(r"[^\w\s]+")

def normalize_name(s: str) -> str:
    """lower, remove acentos, remove pontuação, compacta espaços."""
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
    Retorna lista de (map, remove_mode) — remove_mode=True se existem linhas com 'to' vazio.
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
            cols = {c.lower(): c for c in df.columns}
            if "from" not in cols or "to" not in cols:
                warn(f"aliases: arquivo sem colunas from/to: {path} (pulando).")
                continue
            df = df.rename(columns=cols)
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
                    remove_mode = True
            results.append((amap, remove_mode))
            debug(f"aliases: {os.path.basename(path)} (linhas={len(amap)}, remove_mode={remove_mode})")
        except Exception as e:
            warn(f"aliases: falha ao ler {path}: {e} (pulando).")
    return results

def apply_aliases(name: str, alias_sets: List[Tuple[Dict[str, str], bool]]) -> str:
    """Aplica múltiplos mapas de aliases por token (com remoção se 'to' vazio)."""
    n = normalize_name(name)
    def _token_subst(text: str, frm: str, to: str) -> str:
        pattern = r"\b" + re.escape(frm) + r"\b"
        return re.sub(pattern, to, text).strip()
    for amap, remove_mode in alias_sets:
        if not amap:
            continue
        for frm, to in amap.items():
            if not frm:
                continue
            if remove_mode and to == "":
                n = _token_subst(n, frm, "")
            else:
                n = _token_subst(n, frm, to)
        n = _norm_space_re.sub(" ", n).strip()
    return n

# ========== IO helpers ==========
def resolve_out_dir(rodada_arg: str) -> str:
    if os.path.isdir(rodada_arg):
        return rodada_arg
    return os.path.join("data", "out", str(rodada_arg))

def read_csv_safe(path: str, required: List[str] | None = None) -> pd.DataFrame:
    if not os.path.isfile(path):
        debug(f"read_csv_safe: arquivo ausente -> {path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        if required:
            missing = [c for c in required if c not in df.columns]
            if missing:
                warn(f"{os.path.basename(path)} colunas faltantes {missing}; ignorando.")
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
    path_theodds = os.path.join(out_dir, "odds_theoddsapi.csv")
    path_apifoot = os.path.join(out_dir, "odds_apifootball.csv")
    cols_req = ["home", "away", "odds_home", "odds_draw", "odds_away"]
    d1 = read_csv_safe(path_theodds, required=cols_req)
    if not d1.empty: d1["source"] = "theoddsapi"
    d2 = read_csv_safe(path_apifoot, required=cols_req)
    if not d2.empty: d2["source"] = "apifootball"
    if d1.empty and d2.empty:
        return pd.DataFrame()
    frames = []
    if not d1.empty: frames.append(d1[cols_req + ["source"]])
    if not d2.empty: frames.append(d2[cols_req + ["source"]])
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# ========== Principal ==========
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="ID numérico ou caminho data/out/<ID>")
    args = parser.parse_args()

    out_dir = resolve_out_dir(args.rodada)
    info("[consensus] ===================================================")
    info("[consensus] GERANDO ODDS CONSENSUS")
    info(f"[consensus] RODADA_DIR: {out_dir}")
    info("[consensus] ===================================================")

    wl = load_whitelist(out_dir)
    all_odds = load_odds(out_dir)

    if all_odds.empty:
        out = os.path.join(out_dir, "odds_consensus.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            f.write("match_id,team_home,team_away,odds_home,odds_draw,odds_away,source\n")
        err("Nenhuma fonte de odds carregada (theoddsapi/apifootball vazias).")
        sys.exit(EXIT_CODE)

    c_theodds = int((all_odds["source"] == "theoddsapi").sum())
    c_apifoot = int((all_odds["source"] == "apifootball").sum())
    debug(f"Carregado odds from theoddsapi: {c_theodds} linhas")
    debug(f"Carregado odds from apifootball: {c_apifoot} linhas")

    alias_sets = load_alias_maps("data/aliases")
    debug(f"aliases: total arquivos lidos = {len(alias_sets)}")

    # normalizar nomes com aliases
    wl = wl.copy()
    wl["nh"] = wl["team_home"].map(lambda x: apply_aliases(x, alias_sets))
    wl["na"] = wl["team_away"].map(lambda x: apply_aliases(x, alias_sets))

    all_odds = all_odds.copy()
    all_odds["nh"] = all_odds["home"].map(lambda x: apply_aliases(x, alias_sets))
    all_odds["na"] = all_odds["away"].map(lambda x: apply_aliases(x, alias_sets))

    # agregar por par normalizado
    agg = (
        all_odds.groupby(["nh", "na"], dropna=False)[["odds_home", "odds_draw", "odds_away"]]
        .mean()
        .reset_index()
    )

    if agg.empty:
        out = os.path.join(out_dir, "odds_consensus.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            f.write("match_id,team_home,team_away,odds_home,odds_draw,odds_away,source\n")
        err("Agregação vazia após normalização — verifique arquivos de aliases.")
        sys.exit(EXIT_CODE)

    # merge com sufixos explícitos
    try:
        merged = wl.merge(
            agg, on=["nh", "na"], how="left", suffixes=("_wl", "_odds")
        )
    except Exception as e:
        err(f"Falha no merge whitelist x odds agregadas: {e}")
        sys.exit(EXIT_CODE)

    matched = merged.dropna(subset=["odds_home", "odds_draw", "odds_away"]).copy()

    # diagnóstico de não-casados
    not_matched = wl.merge(
        matched[["match_id", "nh", "na"]],
        on=["match_id", "nh", "na"],
        how="left",
        indicator=True,
    )
    not_matched = not_matched[not_matched["_merge"] == "left_only"]

    total_wl = len(wl)
    total_ok = len(matched)
    info(f"[consensus] casados: {total_ok}/{total_wl}")

    if not not_matched.empty:
        warn("[consensus] Alguns jogos da whitelist não casaram com odds. Mostrando até 10:")
        print("match_id   team_home team_away              match_key")
        for _, r in not_matched.head(10).iterrows():
            mk = f"{normalize_name(str(r['team_home']))}__vs__{normalize_name(str(r['team_away']))}"
            print(f"{int(r['match_id']):8d} {r['team_home']}     {r['team_away']} {mk}")

    if matched.empty:
        # Não segue adiante com arquivo vazio — falha cedo e explícito
        out = os.path.join(out_dir, "odds_consensus.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            f.write("match_id,team_home,team_away,odds_home,odds_draw,odds_away,source\n")
        err("Nenhum jogo casou com odds após normalização/aliases (arquivo vazio).")
        sys.exit(EXIT_CODE)

    out_cols = ["match_id", "team_home", "team_away", "odds_home", "odds_draw", "odds_away"]
    out_df = matched[out_cols].copy()
    out_df["source"] = "consensus"

    out_path = os.path.join(out_dir, "odds_consensus.csv")
    out_df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    info(f"[consensus] OK -> {out_path}")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        err(f"Falha inesperada: {e}")
        sys.exit(EXIT_CODE)