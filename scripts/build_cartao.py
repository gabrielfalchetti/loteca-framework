#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_cartao.py

Gera o cartão Loteca com decisões guiadas por valor (Kelly) e margem mínima.
- Simples (1/X/2) apenas se houver edge>0 e margem mínima.
- Caso contrário, duplo (1X/X2) ou triplo (1X2) conforme probabilidades.

Saídas:
- {OUT_DIR}/loteca_cartao.txt
"""

import argparse
import os
import sys
from typing import Optional, Tuple
import pandas as pd

# Parâmetros de decisão
MARGEM_MIN_SIMPLE = 0.20      # diferença entre top prob e segunda
MARGEM_MIN_DUPLO  = 0.08      # se entre top e segundo >= isso, permite duplo ao invés de triplo
P_DRAW_ALTO       = 0.30      # quando p_draw alto, prioriza cobrir X no duplo
STAKE_MIN_SIMPLE  = 0.5       # opcional: se stake (Kelly) < isso, evita cravar simples

def dbg(*msg):
    print("[cartao]", *msg, flush=True)


def read_csv_safe(path: str) -> Optional[pd.DataFrame]:
    if not os.path.isfile(path):
        return None
    try:
        df = pd.read_csv(path)
        if df.shape[0] == 0:
            return None
        return df
    except Exception:
        return None


def load_predictions(out_dir: str) -> pd.DataFrame:
    # prioridade: predictions_final.csv -> predictions_blend.csv
    p_final = read_csv_safe(os.path.join(out_dir, "predictions_final.csv"))
    if p_final is not None and {"match_key","home","away","p_home","p_draw","p_away"}.issubset(p_final.columns):
        return p_final.copy()
    p_blend = read_csv_safe(os.path.join(out_dir, "predictions_blend.csv"))
    if p_blend is not None and {"match_key","home","away","p_home","p_draw","p_away"}.issubset(p_blend.columns):
        return p_blend.copy()
    raise FileNotFoundError("Nenhum predictions_final.csv/blend.csv válido encontrado.")


def load_kelly(out_dir: str) -> Optional[pd.DataFrame]:
    k = read_csv_safe(os.path.join(out_dir, "kelly_stakes.csv"))
    if k is None:
        return None
    # normalizar picks para 1/X/2
    if "pick" in k.columns:
        k = k.rename(columns={"pick":"pick_raw"})
        def map_pick(x: str) -> str:
            x = str(x).upper()
            if x in ("HOME","1"): return "1"
            if x in ("DRAW","X"): return "X"
            if x in ("AWAY","2"): return "2"
            return "?"
        k["pick"] = k["pick_raw"].apply(map_pick)
    return k


def decide_row(ph: float, pd: float, pa: float,
               edge_1: float, edge_x: float, edge_2: float,
               stake_1: float, stake_x: float, stake_2: float) -> Tuple[str, str]:
    """
    Retorna (escolha, nota) onde escolha ∈ {"1","X","2","1X","X2","12","1X2"}
    Nota: string explicando motivo.
    """
    probs = {"1": ph, "X": pd, "2": pa}
    order = sorted(probs.items(), key=lambda kv: kv[1], reverse=True)
    top_key, top_p = order[0]
    second_key, second_p = order[1]
    margem = top_p - second_p

    # checar edge/stake da opção top
    edge_map  = {"1": edge_1, "X": edge_x, "2": edge_2}
    stake_map = {"1": stake_1, "X": stake_x, "2": stake_2}
    edge_top  = edge_map.get(top_key, 0.0) if edge_map is not None else 0.0
    stake_top = stake_map.get(top_key, 0.0) if stake_map is not None else 0.0

    # 1) SIMPLES apenas com valor + margem mínima
    if edge_top is not None and edge_top > 0 and margem >= MARGEM_MIN_SIMPLE and stake_top >= STAKE_MIN_SIMPLE:
        return top_key, f"simples por valor (edge>0) e margem {margem:.3f} (≥ {MARGEM_MIN_SIMPLE:.2f})"

    # 2) DUPLO quando margem moderada
    if margem >= MARGEM_MIN_DUPLO:
        # escolher entre 1X / X2 / 12 conforme top_key e p_draw
        if top_key == "1":
            # se p_draw alto, cobrir X; senão cobrir 2
            prefer = "1X" if pd >= P_DRAW_ALTO else "12"
            return prefer, f"duplo por margem {margem:.3f} (≥ {MARGEM_MIN_DUPLO:.2f}); draw alto={pd:.2f}"
        elif top_key == "2":
            prefer = "X2" if pd >= P_DRAW_ALTO else "12"
            return prefer, f"duplo por margem {margem:.3f} (≥ {MARGEM_MIN_DUPLO:.2f}); draw alto={pd:.2f}"
        else:  # top_key == "X"
            # empates muito prováveis -> cobrir lado mais forte
            if ph >= pa:
                return "1X", f"duplo por X alto; mandante levemente à frente (p_home={ph:.2f})"
            else:
                return "X2", f"duplo por X alto; visitante levemente à frente (p_away={pa:.2f})"

    # 3) TRIPLO quando muito equilibrado
    return "1X2", f"triplo por equilíbrio (margem {margem:.3f} < {MARGEM_MIN_DUPLO:.2f})"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex.: data/out/123456)")
    args = ap.parse_args()

    out_dir = args.rodada

    preds = load_predictions(out_dir)
    kelly = load_kelly(out_dir)

    # index auxiliar para buscar edges/stakes por match_key e opção
    edge_by_opt = {}
    stake_by_opt = {}
    if kelly is not None and {"match_key","pick","edge","stake"}.issubset(kelly.columns):
        for _, r in kelly.iterrows():
            mk = str(r.get("match_key", r.get("match_id", "")))
            pick = str(r["pick"]).upper()
            edge_by_opt[(mk, pick)] = float(r.get("edge", 0.0))
            stake_by_opt[(mk, pick)] = float(r.get("stake", 0.0))

    lines = ["==== CARTÃO LOTECA ===="]
    # tentar casar com whitelist para ordenar; caso contrário, seguir ordem natural
    wl = read_csv_safe(os.path.join(out_dir, "matches_whitelist.csv"))
    if wl is not None and {"match_id","match_key","team_home","team_away"}.issubset(wl.columns):
        wl = wl.rename(columns={"team_home":"home","team_away":"away"})
        order_keys = wl[["match_key","home","away"]].values.tolist()
        # construir dict por match_key
        pmap = {mk: row for mk, row in preds.set_index("match_key").iterrows()}
        numero = 1
        for mk, home, away in order_keys:
            row = pmap.get(mk)
            if row is None:
                lines.append(f"Jogo {numero:02d} - {home} x {away}: ? (sem probabilidades)")
                numero += 1
                continue
            ph, pd, pa = float(row["p_home"]), float(row["p_draw"]), float(row["p_away"])
            # edges/stakes por opção
            e1 = edge_by_opt.get((mk, "1"), 0.0); ex = edge_by_opt.get((mk, "X"), 0.0); e2 = edge_by_opt.get((mk, "2"), 0.0)
            s1 = stake_by_opt.get((mk, "1"), 0.0); sx = stake_by_opt.get((mk, "X"), 0.0); s2 = stake_by_opt.get((mk, "2"), 0.0)
            escolha, nota = decide_row(ph, pd, pa, e1, ex, e2, s1, sx, s2)
            # prob exibida do top (ou se triplo, mostrar p_draw)
            probs = {"1": ph, "X": pd, "2": pa}
            if escolha in ("1","X","2"):
                shown = probs[escolha]
            elif escolha == "1X2":
                shown = max(ph, pd, pa)
            elif escolha == "1X":
                shown = ph + pd
            elif escolha == "X2":
                shown = pd + pa
            else:  # "12"
                shown = ph + pa
            lines.append(f"Jogo {numero:02d} - {home} x {away}: {escolha} [prob~{shown*100:.1f}%]  // {nota}")
            numero += 1
    else:
        # fallback: sem whitelist
        numero = 1
        for _, row in preds.iterrows():
            home, away = row["home"], row["away"]
            mk = row["match_key"]
            ph, pd, pa = float(row["p_home"]), float(row["p_draw"]), float(row["p_away"])
            e1 = edge_by_opt.get((mk, "1"), 0.0); ex = edge_by_opt.get((mk, "X"), 0.0); e2 = edge_by_opt.get((mk, "2"), 0.0)
            s1 = stake_by_opt.get((mk, "1"), 0.0); sx = stake_by_opt.get((mk, "X"), 0.0); s2 = stake_by_opt.get((mk, "2"), 0.0)
            escolha, nota = decide_row(ph, pd, pa, e1, ex, e2, s1, sx, s2)
            probs = {"1": ph, "X": pd, "2": pa}
            if escolha in ("1","X","2"):
                shown = probs[escolha]
            elif escolha == "1X2":
                shown = max(ph, pd, pa)
            elif escolha == "1X":
                shown = ph + pd
            elif escolha == "X2":
                shown = pd + pa
            else:
                shown = ph + pa
            lines.append(f"Jogo {numero:02d} - {home} x {away}: {escolha} [prob~{shown*100:.1f}%]  // {nota}")
            numero += 1

    lines.append("=======================")
    out_path = os.path.join(out_dir, "loteca_cartao.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    dbg("OK ->", out_path)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[cartao][ERRO] {e}", file=sys.stderr)
        sys.exit(1)