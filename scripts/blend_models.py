#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
blend_models.py

Gera predictions_final.csv a partir de:
- predictions_blend.csv (base)
- calibrated_probs.csv (calibração Bayes/Dirichlet)
- context_features.csv (clima + injuries + “entropy_x_gap” + xg_diff_proxy)

Parâmetros:
  --rodada OUT_DIR
  --w_calib float (peso da calibração - default 0.65)
  --w_market float (peso do mercado dentro do predictions_blend - default 0.35)
  --use-context (flag) ativa ajustes contextuais
  --context-strength float [0..1] (quanto o contexto pode deslocar p; default 0.15)

Saída:
  {OUT_DIR}/predictions_final.csv com colunas:
    match_key,home,away,p_home,p_draw,p_away,used_sources,notes
"""

import argparse
import csv
import os
import sys
from math import isfinite

def _read_csv(path):
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        return list(rd)

def _to_f(x, default=0.0):
    try:
        v = float(x)
        return v if isfinite(v) else default
    except:
        return default

def _renorm(p1, pX, p2):
    s = p1 + pX + p2
    if s <= 0:
        return (1/3, 1/3, 1/3)
    return (p1/s, pX/s, p2/s)

def load_base(rodada):
    # predictions_blend.csv contém p_ do mercado (ou só mercado quando calib ausente)
    base_path = os.path.join(rodada, "predictions_blend.csv")
    rows = _read_csv(base_path)
    out = {}
    for r in rows:
        key = r.get("match_id") or r.get("match_key")
        if not key:
            # tenta construir
            home = r.get("team_home") or r.get("home")
            away = r.get("team_away") or r.get("away")
            key = f"{(home or '').strip().lower()}__vs__{(away or '').strip().lower()}"
        out[str(key)] = {
            "match_key": key,
            "home": r.get("team_home") or r.get("home"),
            "away": r.get("team_away") or r.get("away"),
            "p_home": _to_f(r.get("p_home")),
            "p_draw": _to_f(r.get("p_draw")),
            "p_away": _to_f(r.get("p_away")),
            "used_sources": r.get("used_sources") or "market",
            "notes": []
        }
    return out

def load_calib(rodada):
    p = os.path.join(rodada, "calibrated_probs.csv")
    rows = _read_csv(p)
    out = {}
    for r in rows:
        mid = r.get("match_id") or ""
        out[mid.strip().lower().replace(" ", "_")] = (
            _to_f(r.get("calib_home")), _to_f(r.get("calib_draw")), _to_f(r.get("calib_away"))
        )
    return out

def load_context(rodada):
    p = os.path.join(rodada, "context_features.csv")
    rows = _read_csv(p)
    out = {}
    for r in rows:
        key = r.get("match_key") or r.get("match_id")
        if not key:
            home = r.get("home") or r.get("team_home")
            away = r.get("away") or r.get("team_away")
            key = f"{(home or '').strip().lower()}__vs__{(away or '').strip().lower()}"
        out[str(key)] = {
            "entropy_bits": _to_f(r.get("entropy_bits"), 0.0),
            "xg_diff_proxy": _to_f(r.get("xg_diff_proxy"), 0.0),
            "inj_h": _to_f(r.get("inj_home_weight"), 0.0),
            "inj_a": _to_f(r.get("inj_away_weight"), 0.0),
            "wind_kph": _to_f(r.get("wind_speed_kph"), 0.0),
            "precip_mm": _to_f(r.get("precip_mm"), 0.0)
        }
    return out

def apply_calibration(base, calib, w_calib):
    # Combina p_base com p_calib (se houver) por convex combination
    notes = []
    for key, row in base.items():
        # tenta casar calib por padrões comuns de chave
        variants = [
            key,
            key.replace("__vs__", "__").replace(" ", "_"),
            f"{(row['home'] or '').strip().replace(' ','_')}__{(row['away'] or '').strip().replace(' ','_')}".lower()
        ]
        cvec = None
        for v in variants:
            if v in calib:
                cvec = calib[v]
                break
        if cvec:
            b = (row["p_home"], row["p_draw"], row["p_away"])
            p = (
                (1 - w_calib) * b[0] + w_calib * cvec[0],
                (1 - w_calib) * b[1] + w_calib * cvec[1],
                (1 - w_calib) * b[2] + w_calib * cvec[2],
            )
            row["p_home"], row["p_draw"], row["p_away"] = _renorm(*p)
            row["used_sources"] = (row["used_sources"] + "+calib").strip("+")
            row["notes"].append(f"calib:{w_calib:.2f}")
        else:
            row["notes"].append("calib:absent")
    return base

def apply_context(base, ctx, strength):
    # Ajustes pequenos, limitados por 'strength' (0..1).
    # Heurísticas:
    # - Vento > 25 kph ou precip_mm > 0.2 => +p_draw (até strength*0.05)
    # - Entropia > 1.55 e xg_diff < 0.28 => +p_draw e -no favorito (até strength*0.07)
    # - Injuries: se inj_weight_sum_home >> away, penaliza home (e vice-versa) (até strength*0.06)
    for key, row in base.items():
        c = ctx.get(key)
        if not c:
            row["notes"].append("ctx:none")
            continue
        p1, pX, p2 = row["p_home"], row["p_draw"], row["p_away"]

        # clima
        bump_draw = 0.0
        if c["wind_kph"] > 25 or c["precip_mm"] > 0.2:
            bump_draw += 0.02 * strength

        # incerteza estrutural
        if c["entropy_bits"] > 1.55 and c["xg_diff_proxy"] < 0.28:
            bump_draw += 0.03 * strength
            # tira um pouco do favorito (lado com maior p)
            if p1 >= p2:
                p1 -= 0.02 * strength
            else:
                p2 -= 0.02 * strength

        # injuries
        diff_inj = c["inj_h"] - c["inj_a"]  # positivo: casa mais desfalcada
        if abs(diff_inj) > 0.9:  # só aplica quando diferença é relevante
            delta = min(0.04 * strength, 0.01 * abs(diff_inj))
            if diff_inj > 0:
                p1 -= delta
                pX += delta * 0.6
                p2 += delta * 0.4
            else:
                p2 -= delta
                pX += delta * 0.6
                p1 += delta * 0.4

        # aplica draw bump
        pX += bump_draw
        row["p_home"], row["p_draw"], row["p_away"] = _renorm(p1, pX, p2)
        row["notes"].append(f"ctx:{strength:.2f}")
    return base

def write_out(rodada, base):
    outp = os.path.join(rodada, "predictions_final.csv")
    cols = ["match_key","home","away","p_home","p_draw","p_away","used_sources","notes"]
    with open(outp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for _, row in base.items():
            w.writerow({
                "match_key": row["match_key"],
                "home": row["home"],
                "away": row["away"],
                "p_home": f"{row['p_home']:.6f}",
                "p_draw": f"{row['p_draw']:.6f}",
                "p_away": f"{row['p_away']:.6f}",
                "used_sources": row["used_sources"],
                "notes": ";".join(row["notes"])
            })
    print(f"[blend] OK -> {outp}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--w_calib", type=float, default=0.65)
    ap.add_argument("--w_market", type=float, default=0.35)  # mantido por compatibilidade de logs
    ap.add_argument("--use-context", dest="use_context", action="store_true")
    ap.add_argument("--context-strength", type=float, default=0.15)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rodada = args.rodada
    base = load_base(rodada)
    calib = load_calib(rodada)

    base = apply_calibration(base, calib, max(0.0, min(1.0, args.w_calib)))

    if args.use_context:
        ctx = load_context(rodada)
        base = apply_context(base, ctx, max(0.0, min(1.0, args.context_strength)))

    write_out(rodada, base)

if __name__ == "__main__":
    main()