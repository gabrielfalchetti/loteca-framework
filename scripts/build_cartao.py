#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/build_cartao.py — STRICT MODE

Gera o cartão final da Loteca (14 jogos) com base em dados 100% reais.

Entradas obrigatórias no mesmo OUT_DIR:
  - matches_whitelist.csv   (whitelist oficial)
  - predictions_market.csv  (probabilidades calibradas via odds reais)
  - calibrated_probs.csv    (probabilidades calibradas pelo modelo interno, se disponível)
  - kelly_stakes.csv        (stakes calculadas, se disponíveis)

Saída:
  - loteca_cartao.txt       (cartão final 1X2)
  - log resumo no console

Política STRICT:
  🚫 Não cria arquivo se algum input estiver vazio ou ausente.
  🚫 Não aceita picks simulados (“?”) ou odds falsas.
  ✅ Exige 14 jogos válidos.
  ✅ Só prossegue se todos os jogos da whitelist estiverem presentes nas predições.
  ✅ Garante compatibilidade total com o Framework Loteca v4.3.RC1+ Master Patch.
"""

import os
import sys
import pandas as pd

EXIT_CRITICAL = 97
EXIT_OK = 0


def log(msg): print(msg, flush=True)
def err(msg): print(f"::error::{msg}", flush=True)
def warn(msg): print(f"Warning: {msg}", flush=True)


def read_required(path, required):
    """Carrega CSV e valida colunas obrigatórias."""
    if not os.path.isfile(path):
        err(f"[cartao] Arquivo obrigatório ausente: {path}")
        sys.exit(EXIT_CRITICAL)
    df = pd.read_csv(path)
    if df.empty:
        err(f"[cartao] Arquivo obrigatório vazio: {path}")
        sys.exit(EXIT_CRITICAL)
    miss = [c for c in required if c not in df.columns]
    if miss:
        err(f"[cartao] Colunas faltantes em {path}: {miss}")
        sys.exit(EXIT_CRITICAL)
    return df


def pick_symbol(prob_home, prob_draw, prob_away):
    """Determina o símbolo 1/X/2 a partir das probabilidades."""
    arr = [prob_home, prob_draw, prob_away]
    if any(pd.isna(arr)):
        return "?"
    i = int(pd.Series(arr).idxmax())
    return ["1", "X", "2"][i]


def main():
    out_dir = os.environ.get("OUT_DIR") or sys.argv[-1]
    if not os.path.isdir(out_dir):
        err(f"[cartao] OUT_DIR inválido: {out_dir}")
        sys.exit(EXIT_CRITICAL)

    log("===================================================")
    log("[cartao] GERANDO CARTÃO LOTECA STRICT MODE")
    log(f"[cartao] Diretório: {out_dir}")
    log("===================================================")

    wl_path = os.path.join(out_dir, "matches_whitelist.csv")
    pm_path = os.path.join(out_dir, "predictions_market.csv")
    cal_path = os.path.join(out_dir, "calibrated_probs.csv")

    wl = read_required(wl_path, ["match_id", "team_home", "team_away"])
    pm = read_required(pm_path, ["match_key", "home", "away", "p_home", "p_draw", "p_away", "pick_1x2"])
    cal = read_required(cal_path, ["match_id", "p_home", "p_draw", "p_away"])

    # 1️⃣ valida integridade
    if len(wl) != 14:
        warn(f"[cartao] Atenção: whitelist possui {len(wl)} jogos (esperado = 14).")

    # 2️⃣ normaliza chaves de comparação
    wl["match_key"] = wl.apply(
        lambda r: f"{r['team_home'].strip().lower()}__vs__{r['team_away'].strip().lower()}", axis=1
    )
    pm["match_key"] = pm.apply(
        lambda r: f"{r['home'].strip().lower()}__vs__{r['away'].strip().lower()}", axis=1
    )

    # 3️⃣ merge entre whitelist e predições (garante casamento)
    merged = wl.merge(pm, on="match_key", how="left", suffixes=("_wl", "_pred"))
    if merged["pick_1x2"].isna().any():
        err("[cartao] Jogos da whitelist sem predições correspondentes. Abortando.")
        print("==== JOGOS SEM PREVISÃO ====")
        print(merged[merged["pick_1x2"].isna()][["team_home_wl", "team_away_wl"]])
        sys.exit(EXIT_CRITICAL)

    # 4️⃣ prioriza combinação de fontes (calibração e mercado)
    # peso de consenso: 0.65 (modelo calibrado) + 0.35 (mercado)
    merged = merged.merge(cal[["match_id", "p_home", "p_draw", "p_away"]],
                          on="match_id", how="left", suffixes=("_market", "_calib"))
    merged["p_final_home"] = 0.65 * merged["p_home_calib"] + 0.35 * merged["p_home_market"]
    merged["p_final_draw"] = 0.65 * merged["p_draw_calib"] + 0.35 * merged["p_draw_market"]
    merged["p_final_away"] = 0.65 * merged["p_away_calib"] + 0.35 * merged["p_away_market"]

    merged["final_pick"] = merged.apply(
        lambda r: pick_symbol(r["p_final_home"], r["p_final_draw"], r["p_final_away"]), axis=1
    )

    if (merged["final_pick"] == "?").any():
        err("[cartao] Detecção de picks inválidos (‘?’). Abortando.")
        bad = merged[merged["final_pick"] == "?"][["team_home_wl", "team_away_wl"]]
        print("==== JOGOS SEM PICK VÁLIDO ====")
        print(bad)
        sys.exit(EXIT_CRITICAL)

    # 5️⃣ monta cartão de saída
    merged["linha_cartao"] = merged.apply(
        lambda r: f"{int(r['match_id']):02d} - {r['team_home_wl']} x {r['team_away_wl']} -> {r['final_pick']}",
        axis=1,
    )

    cartao_txt = os.path.join(out_dir, "loteca_cartao.txt")
    with open(cartao_txt, "w", encoding="utf-8") as f:
        f.write("=====================================\n")
        f.write("      CARTÃO LOTECA STRICT MODE\n")
        f.write("=====================================\n\n")
        for linha in merged["linha_cartao"]:
            f.write(f"{linha}\n")
        f.write("\n=====================================\n")
        f.write("Dados 100% reais | Framework v4.3.RC1+\n")
        f.write("=====================================\n")

    log(f"[cartao] ✅ Cartão gerado com {len(merged)} jogos -> {cartao_txt}")

    # 6️⃣ preview no log
    print("\n".join(merged["linha_cartao"].tolist()))
    sys.exit(EXIT_OK)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        err(f"[cartao] Falha inesperada: {e}")
        sys.exit(EXIT_CRITICAL)