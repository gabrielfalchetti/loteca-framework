#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera 3 cartões da Loteca a partir das probabilidades:
- Conservador: 14 secos (resultado mais provável)
- Intermediário: 2 triplos + 4 duplos (restante secos)
- Máximo: 14 triplos

Premissas:
- Mantemos a ordem dos 14 jogos conforme data/in/matches_source.csv
- Lemos as probabilidades de OUT_DIR/predictions_market.csv
  - Fallback: OUT_DIR/odds_consensus.csv (prob. implícitas normalizadas)
- Arquivo de saída vai para o mesmo OUT_DIR passado via --rodada

Uso:
  python scripts/build_cartao.py --rodada "data/out/XXXX" [--debug]
"""

import argparse
import json
import os
import sys
from typing import List, Tuple

import pandas as pd
import numpy as np


def log(msg: str, debug: bool):
    if debug:
        print(f"[cartao][DEBUG] {msg}")


def ensure_file(path: str, must_exist: bool = True):
    if must_exist and (not os.path.isfile(path) or os.path.getsize(path) == 0):
        raise FileNotFoundError(path)


def load_matches(matches_path: str, debug: bool) -> pd.DataFrame:
    """
    Espera colunas mínimas: home, away
    Opcional: match_key (se não existir, é gerado)
    """
    ensure_file(matches_path, True)
    df = pd.read_csv(matches_path)
    req_cols = {"home", "away"}
    missing = req_cols - set(df.columns.str.lower())
    if missing:
        raise ValueError(f"coluna(s) ausente(s) em {matches_path}: {', '.join(sorted(missing))}")

    # normalizar nomes de colunas
    colmap = {c: c.lower() for c in df.columns}
    df = df.rename(columns=colmap)

    # match_key (home__vs__away) em lower para casar com outputs
    if "match_key" not in df.columns:
        df["match_key"] = (df["home"].str.strip().str.lower() +
                           "__vs__" +
                           df["away"].str.strip().str.lower())

    # manter só 14 primeiros se tiver mais
    if len(df) > 14:
        df = df.iloc[:14].copy()

    log(f"matches lidos ({len(df)}):\n{df[['home','away','match_key']].to_string(index=False)}", debug)
    return df


def normalize_probs(row: pd.Series) -> pd.Series:
    p = np.array([row["prob_home"], row["prob_draw"], row["prob_away"]], dtype=float)
    p = np.clip(p, 1e-9, None)
    s = p.sum()
    if s <= 0:
        p = np.array([1/3, 1/3, 1/3])
    else:
        p = p / s
    return pd.Series({"prob_home": p[0], "prob_draw": p[1], "prob_away": p[2]})


def implied_from_odds(row: pd.Series) -> pd.Series:
    """
    Converte odds em probabilidades implícitas (sem overround).
    """
    oh, od, oa = row.get("odds_home", np.nan), row.get("odds_draw", np.nan), row.get("odds_away", np.nan)
    inv = []
    for x in (oh, od, oa):
        inv.append(0.0 if (pd.isna(x) or x <= 1.0) else 1.0 / float(x))
    inv = np.array(inv)
    s = inv.sum()
    if s <= 0:
        p = np.array([1/3, 1/3, 1/3])
    else:
        p = inv / s
    return pd.Series({"prob_home": p[0], "prob_draw": p[1], "prob_away": p[2]})


def load_predictions(out_dir: str, matches_df: pd.DataFrame, debug: bool) -> pd.DataFrame:
    """
    Tenta ler OUT_DIR/predictions_market.csv (preferência).
    Se não existir, cai para OUT_DIR/odds_consensus.csv e cria probs implícitas.
    Resultado final deve ter: match_key, home, away, prob_home, prob_draw, prob_away
    """
    pred_path = os.path.join(out_dir, "predictions_market.csv")
    odds_path = os.path.join(out_dir, "odds_consensus.csv")

    if os.path.isfile(pred_path) and os.path.getsize(pred_path) > 0:
        df = pd.read_csv(pred_path)
        log(f"predictions_market.csv encontrado ({len(df)} linhas)", debug)

        # normalização de colunas
        colmap = {c: c.lower() for c in df.columns}
        df = df.rename(columns=colmap)

        # garantir match_key
        if "match_key" not in df.columns:
            # tentar criar via team_home/team_away, se existir
            if {"team_home", "team_away"}.issubset(df.columns):
                df["match_key"] = (df["team_home"].str.strip().str.lower() +
                                   "__vs__" +
                                   df["team_away"].str.strip().str.lower())
            else:
                raise ValueError("predictions_market.csv sem match_key e sem team_home/team_away")

        # copiar nomes canônicos p/ home/away
        if "home" not in df.columns and "team_home" in df.columns:
            df["home"] = df["team_home"]
        if "away" not in df.columns and "team_away" in df.columns:
            df["away"] = df["team_away"]

        # garantir probabilidades
        has_probs = {"prob_home", "prob_draw", "prob_away"}.issubset(df.columns)
        if not has_probs and {"odds_home", "odds_draw", "odds_away"}.issubset(df.columns):
            df[["prob_home", "prob_draw", "prob_away"]] = df.apply(implied_from_odds, axis=1)
        elif not has_probs:
            raise ValueError("predictions_market.csv não contém probabilidades nem odds.")

        # normalizar probs
        df[["prob_home", "prob_draw", "prob_away"]] = df.apply(normalize_probs, axis=1)

    elif os.path.isfile(odds_path) and os.path.getsize(odds_path) > 0:
        df = pd.read_csv(odds_path)
        log(f"odds_consensus.csv encontrado ({len(df)} linhas) — gerando probs implícitas", debug)
        colmap = {c: c.lower() for c in df.columns}
        df = df.rename(columns=colmap)

        if "match_key" not in df.columns:
            if {"team_home", "team_away"}.issubset(df.columns):
                df["match_key"] = (df["team_home"].str.strip().str.lower() +
                                   "__vs__" +
                                   df["team_away"].str.strip().str.lower())
            else:
                raise ValueError("odds_consensus.csv sem match_key e sem team_home/team_away")

        if "home" not in df.columns and "team_home" in df.columns:
            df["home"] = df["team_home"]
        if "away" not in df.columns and "team_away" in df.columns:
            df["away"] = df["team_away"]

        # gerar probs de odds
        req_odds = {"odds_home", "odds_draw", "odds_away"}
        if not req_odds.issubset(df.columns):
            raise ValueError("odds_consensus.csv não possui odds_home/odds_draw/odds_away")
        df[["prob_home", "prob_draw", "prob_away"]] = df.apply(implied_from_odds, axis=1)

    else:
        raise FileNotFoundError("Nenhum arquivo de predição/odds encontrado em OUT_DIR (predictions_market.csv ou odds_consensus.csv).")

    # manter somente colunas necessárias
    keep = ["match_key", "home", "away", "prob_home", "prob_draw", "prob_away"]
    df = df[keep].copy()

    # juntar com matches para manter ORDEM EXATA dos 14 jogos
    merged = matches_df[["match_key", "home", "away"]].merge(
        df, on="match_key", how="left", suffixes=("", "_pred")
    )
    # se faltou prob, coloque neutral 1/3
    for c in ["prob_home", "prob_draw", "prob_away"]:
        if c not in merged.columns:
            merged[c] = np.nan
    merged[["prob_home", "prob_draw", "prob_away"]] = merged[["prob_home", "prob_draw", "prob_away"]].fillna(1/3)
    merged[["prob_home", "prob_draw", "prob_away"]] = merged.apply(normalize_probs, axis=1)

    # debug sample
    return merged


def entropia(row: pd.Series) -> float:
    p = np.array([row["prob_home"], row["prob_draw"], row["prob_away"]], dtype=float)
    p = np.clip(p, 1e-12, None)
    return float(-(p * np.log(p)).sum())


def escolha_seco(row: pd.Series) -> str:
    probs = [row["prob_home"], row["prob_draw"], row["prob_away"]]
    idx = int(np.argmax(probs))
    return ["1", "X", "2"][idx]


def escolha_duplo(row: pd.Series) -> str:
    # pega os 2 resultados mais prováveis
    probs = pd.Series(
        [row["prob_home"], row["prob_draw"], row["prob_away"]],
        index=["1", "X", "2"], dtype=float
    )
    top2 = probs.sort_values(ascending=False).head(2).index.tolist()
    return "".join(top2)


def escrever_cartao(linhas: List[str], path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(linhas) + "\n")


def montar_cartoes(preds: pd.DataFrame, out_dir: str, debug: bool):
    """
    preds: em ordem dos jogos (matches order) com prob_home/prob_draw/prob_away
    Gera 3 cartões e um preview.
    """
    df = preds.copy()
    df["entropia"] = df.apply(entropia, axis=1)

    # Índices ordenados por incerteza (maior entropia = mais incerto)
    # MAS preservando a ORDEM DO CARTÃO na hora de imprimir
    incertos_idx = df.sort_values("entropia", ascending=False).index.tolist()

    # Conservador: todos secos
    secos = [escolha_seco(df.loc[i]) for i in df.index]

    # Intermediário: 2 triplos + 4 duplos (restante secos)
    triplos_idx = set(incertos_idx[:2])
    duplos_idx = set(incertos_idx[2:6])

    inter = []
    for i in df.index:
        if i in triplos_idx:
            inter.append("1X2")
        elif i in duplos_idx:
            inter.append(escolha_duplo(df.loc[i]))
        else:
            inter.append(escolha_seco(df.loc[i]))

    # Máximo: todos triplos
    maximo = ["1X2"] * len(df)

    # Formatar linhas no padrão Loteca (1..14)
    def formatar_cartao(escolhas: List[str]) -> List[str]:
        linhas = []
        for j, i in enumerate(df.index, start=1):
            h = str(df.loc[i, "home"])
            a = str(df.loc[i, "away"])
            palpite = escolhas[j-1]
            linhas.append(f"{j}) {h} x {a} -> {palpite}")
        return linhas

    cartao_cons = formatar_cartao(secos)
    cartao_inter = formatar_cartao(inter)
    cartao_max = formatar_cartao(maximo)

    # Preview informativo (probabilidades)
    preview = []
    preview.append("== PREVIEW DE PROBABILIDADES ==")
    for j, i in enumerate(df.index, start=1):
        h = str(df.loc[i, "home"])
        a = str(df.loc[i, "away"])
        ph, pdw, pa = df.loc[i, ["prob_home", "prob_draw", "prob_away"]].tolist()
        preview.append(f"{j:02d}) {h} x {a}  |  1={ph:.3f}  X={pdw:.3f}  2={pa:.3f}  | entropia={df.loc[i,'entropia']:.4f}")
    preview.append("")
    preview.append("== ESCOLHAS ==")
    preview.append("[Conservador] " + " ".join([f"{i+1}:{s}" for i, s in enumerate(secos)]))
    preview.append("[Intermediário 2T+4D] " + " ".join([f"{i+1}:{s}" for i, s in enumerate(inter)]))
    preview.append("[Máximo] " + " ".join([f"{i+1}:{s}" for i, s in enumerate(maximo)]))

    # Salvar
    path_cons = os.path.join(out_dir, "loteca_cartao_conservador.txt")
    path_inter = os.path.join(out_dir, "loteca_cartao_intermediario.txt")
    path_max = os.path.join(out_dir, "loteca_cartao_maximo.txt")
    path_prev = os.path.join(out_dir, "loteca_cartao_preview.txt")

    escrever_cartao(cartao_cons, path_cons)
    escrever_cartao(cartao_inter, path_inter)
    escrever_cartao(cartao_max, path_max)
    escrever_cartao(preview, path_prev)

    print(f"[cartao] OK -> {path_cons}")
    print(f"[cartao] OK -> {path_inter}  (2 triplos + 4 duplos)")
    print(f"[cartao] OK -> {path_max}")
    print(f"[cartao] PREVIEW -> {path_prev}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True, help="Diretório OUT de trabalho (ex.: data/out/18245...)")
    parser.add_argument("--matches", default="data/in/matches_source.csv", help="Caminho do matches_source.csv")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    out_dir = args.rodada
    if out_dir.startswith("data/out/") is False:
        # não exigimos formato, apenas garantimos que exista
        pass

    if not os.path.isdir(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    # 1) matches (ordem dos 14 jogos)
    matches_path = args.matches
    matches_df = load_matches(matches_path, args.debug)

    # 2) predictions (ou odds) do OUT_DIR
    preds_df = load_predictions(out_dir, matches_df, args.debug)

    # 3) montar cartões
    montar_cartoes(preds_df, out_dir, args.debug)


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        print(f"[cartao] ERRO: arquivo ausente: {e}", file=sys.stderr)
        sys.exit(2)
    except ValueError as e:
        print(f"[cartao] ERRO: {e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"[cartao] ERRO inesperado: {e}", file=sys.stderr)
        sys.exit(1)