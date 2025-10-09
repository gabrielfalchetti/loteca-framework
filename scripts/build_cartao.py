#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_cartao.py

Lê:
- matches_whitelist.csv
- predictions_final.csv (novo blend com calibração + contexto)
- kelly_stakes.csv (opcional; usamos apenas stake se existir)

Política de decisão (draw-aware):
- c = p_fav - p_segundo_maior
- Se c >= 0.18  => crava favorito (1 ou 2)
- Se 0.10 <= c < 0.18 e p_draw >= 0.30 => "X"
- Se 0.08 <= c < 0.18 e p_draw < 0.30 => "Dupla Chance" (1X ou X2)
- Se c < 0.08  => "X" (empate)
Esses thresholds reduzem erros como América-MG x Vila Nova.

Saída:
  {OUT_DIR}/loteca_cartao.txt
"""

import csv
import os
import sys
from math import isfinite

def _f(x, d=0.0):
    try:
        v = float(x);  return v if isfinite(v) else d
    except: return d

def _read(path):
    if not os.path.isfile(path): return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def load_wl(rodada):
    p = os.path.join(rodada, "matches_whitelist.csv")
    rows = _read(p)
    out = []
    for r in rows:
        out.append({
            "match_id": str(r.get("match_id") or "").strip(),
            "home": r.get("team_home") or r.get("home"),
            "away": r.get("team_away") or r.get("away")
        })
    return out

def load_pred(rodada):
    p = os.path.join(rodada, "predictions_final.csv")
    if not os.path.isfile(p):
        # fallback: predictions_blend.csv
        p = os.path.join(rodada, "predictions_blend.csv")
    rows = _read(p)
    idx = {}
    for r in rows:
        key = (r.get("match_key") or "").strip()
        idx[key] = {
            "home": r.get("home"),
            "away": r.get("away"),
            "p1": _f(r.get("p_home")),
            "pX": _f(r.get("p_draw")),
            "p2": _f(r.get("p_away")),
        }
    return idx

def find_pred(idx, home, away):
    k1 = f"{(home or '').strip().lower()}__vs__{(away or '').strip().lower()}"
    if k1 in idx: return idx[k1]
    # tolerante a variações
    for k, v in idx.items():
        if (v["home"] or "").lower() == (home or "").lower() and (v["away"] or "").lower() == (away or "").lower():
            return v
    return None

def decide_symbol(p1, pX, p2):
    # favorito e segundo
    fav = "1" if p1 >= p2 and p1 >= pX else ("X" if pX >= p2 else "2")
    trio = sorted([("1", p1), ("X", pX), ("2", p2)], key=lambda t: t[1], reverse=True)
    fav_sym, fav_p = trio[0]
    second_sym, second_p = trio[1]
    c = fav_p - second_p

    # Draw-aware rules
    if c >= 0.18:
        return fav_sym, "single"

    if 0.10 <= c < 0.18 and pX >= 0.30:
        return "X", "draw_swing"

    if 0.08 <= c < 0.18 and pX < 0.30:
        # dupla chance em favor do favorito
        return (f"{'1X' if fav_sym == '1' else 'X2'}"), "double_chance"

    if c < 0.08:
        return "X", "draw_low_margin"

    return fav_sym, "fallback"

def load_kelly(rodada):
    p = os.path.join(rodada, "kelly_stakes.csv")
    rows = _read(p)
    d = {}
    for r in rows:
        # no kelly, nosso match_key é o númerico (por join anterior)
        d[str(r.get("match_key") or "").strip()] = _f(r.get("stake"), 0.0)
    return d

def main():
    if len(sys.argv) < 3 or sys.argv[1] != "--rodada":
        print("Uso: build_cartao.py --rodada OUT_DIR", file=sys.stderr)
        sys.exit(2)
    rodada = sys.argv[2]

    wl = load_wl(rodada)
    preds = load_pred(rodada)
    kelly = load_kelly(rodada)

    lines = ["==== CARTÃO LOTECA ===="]
    for i, m in enumerate(wl, start=1):
        home, away = m["home"], m["away"]
        pr = find_pred(preds, home, away)
        if not pr:
            lines.append(f"Jogo {i:02d} - {home} x {away}: ? (stake=0.0) [nan%]")
            continue

        p1, pX, p2 = pr["p1"], pr["pX"], pr["p2"]
        symbol, reason = decide_symbol(p1, pX, p2)

        # stake se houver (por número do jogo ou tentativas de chave)
        st = _f(kelly.get(str(i)) or 0.0, 0.0)

        pick_pct = {"1": p1, "X": pX, "2": p2}.get(symbol, max(p1, pX, p2))
        lines.append(f"Jogo {i:02d} - {home} x {away}: {symbol} (stake={st:.1f}) [{pick_pct*100:.1f}%]")

    lines.append("=======================")

    outp = os.path.join(rodada, "loteca_cartao.txt")
    with open(outp, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("\n".join(lines))
    print(f"[cartao] OK -> {outp}")

if __name__ == "__main__":
    main()