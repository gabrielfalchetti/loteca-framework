# -*- coding: utf-8 -*-
"""
Gera o cartão Loteca (loteca_cartao.txt) APENAS com os jogos definidos
em data/in/matches_whitelist.csv, na ordem do match_id.

Regras:
- Se existir data/out/<RID>/kelly_stakes.csv, usa pick e stake dali.
- Senão, se existir predictions_blend.csv, usa pick pela maior prob.
- Senão, usa odds_consensus.csv e marca como "?".
- Sempre imprime 1 linha por jogo da whitelist, na ordem do match_id.

Coloque este arquivo em: scripts/build_cartao.py
"""

import csv
import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np

def fail(msg, code=26):
    print(f"::error::{msg}")
    sys.exit(code)

def pick_from_probs(ph, pd_, pa):
    arr = np.array([ph, pd_, pa], dtype=float)
    if not np.isfinite(arr).all():
        return "?"
    i = int(arr.argmax())
    return ["1", "X", "2"][i]

def main():
    out_dir = os.environ.get("OUT_DIR", "").strip()
    if not out_dir:
        rid = os.environ.get("RODADA_ID", "").strip()
        if not rid:
            fail("[cartao] OUT_DIR e RODADA_ID ausentes.")
        out_dir = f"data/out/{rid}"

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    wl_path = Path("data/in/matches_whitelist.csv")
    if not wl_path.exists():
        fail("[cartao] matches_whitelist.csv ausente. Rode scripts/match_whitelist.py antes.")

    wl = pd.read_csv(wl_path, dtype=str)
    wl["match_id_num"] = pd.to_numeric(wl["match_id"], errors="coerce")
    wl = wl.sort_values(["match_id_num", "match_id"], na_position="last")

    # tenta kelly
    p_kelly = out_dir / "kelly_stakes.csv"
    kelly = pd.read_csv(p_kelly) if p_kelly.exists() and p_kelly.stat().st_size > 0 else pd.DataFrame()

    # tenta predictions_blend
    p_blend = out_dir / "predictions_blend.csv"
    blend = pd.read_csv(p_blend) if p_blend.exists() and p_blend.stat().st_size > 0 else pd.DataFrame()

    # odds consensus (para fallback de nomes)
    p_cons = out_dir / "odds_consensus.csv"
    cons = pd.read_csv(p_cons) if p_cons.exists() and p_cons.stat().st_size > 0 else pd.DataFrame()

    lines = []
    for i, r in enumerate(wl.itertuples(index=False), start=1):
        mid = getattr(r, "match_id")
        home = getattr(r, "home")
        away = getattr(r, "away")

        # default
        pick = "?"
        stake = 0.0
        conf = np.nan

        # tenta kelly: precisa ligar por nomes
        if not kelly.empty and {"team_home", "team_away", "pick", "stake"}.issubset(kelly.columns):
            ksel = kelly[(kelly["team_home"] == home) & (kelly["team_away"] == away)]
            if not ksel.empty:
                pick = {
                    "HOME": "1",
                    "DRAW": "X",
                    "AWAY": "2"
                }.get(str(ksel.iloc[0]["pick"]).upper(), "?")
                stake = float(ksel.iloc[0].get("stake", 0.0))

        # se não veio pelo kelly, tenta blend
        if pick == "?" and not blend.empty and {"team_home", "team_away", "p_home", "p_draw", "p_away"}.issubset(blend.columns):
            b = blend[(blend["team_home"] == home) & (blend["team_away"] == away)]
            if not b.empty:
                ph = float(b.iloc[0]["p_home"])
                pd_ = float(b.iloc[0]["p_draw"])
                pa = float(b.iloc[0]["p_away"])
                pick = pick_from_probs(ph, pd_, pa)
                conf = 100.0 * max(ph, pd_, pa)

        # fallback: odds_consensus presente -> tenta inferir pick pela menor odd (maior favorito)
        if pick == "?" and not cons.empty and {"team_home", "team_away", "odds_home", "odds_draw", "odds_away"}.issubset(cons.columns):
            c = cons[(cons["team_home"] == home) & (cons["team_away"] == away)]
            if not c.empty:
                oh = float(c.iloc[0]["odds_home"])
                od = float(c.iloc[0]["odds_draw"])
                oa = float(c.iloc[0]["odds_away"])
                arr = np.array([oh, od, oa], dtype=float)
                if np.isfinite(arr).all():
                    i_min = int(arr.argmin())
                    pick = ["1", "X", "2"][i_min]

        line = f"Jogo {int(i):02d} - {home} x {away}: {pick} (stake={stake}) [{'' if np.isnan(conf) else f'{conf:.1f}%'}]"
        lines.append(line)

    txt = "==== CARTÃO LOTECA ====\n" + "\n".join(lines) + "\n=======================\n"

    out_file = out_dir / "loteca_cartao.txt"
    with out_file.open("w", encoding="utf-8") as f:
        f.write(txt)

    print("==== CARTÃO LOTECA ====")
    print(txt, end="")
    print("========================")
    print(f"[cartao] OK -> {out_file}")

if __name__ == "__main__":
    main()