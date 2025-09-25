import argparse, pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROC = ROOT/"data/processed"
RAW  = ROOT/"data/raw"
OUT  = ROOT/"outputs"
REP  = ROOT/"reports"

def main(rodada):
    feats = pd.read_parquet(PROC/"features.parquet")
    matches = pd.read_csv(RAW/"matches.csv")

    if "prob_home" not in feats.columns:
        feats["prob_home"] = 0.5

    lines = []
    for _, r in matches.sort_values("match_id").iterrows():
        mid = r["match_id"]
        ph = float(feats.loc[feats["match_id"]==mid, "prob_home"].values[0])
        pick = "1X" if ph >= 0.5 else "12"
        lines.append(f'{int(mid):02d} - {r["home"]} x {r["away"]}  ->  {pick}')

    OUT.mkdir(parents=True, exist_ok=True)
    REP.mkdir(parents=True, exist_ok=True)

    (OUT/"preds.csv").write_text("match_id,prob_home\n" + "\n".join(
        f"{int(r['match_id'])},{float(feats.loc[feats['match_id']==r['match_id'],'prob_home'].values[0])}"
        for _, r in matches.iterrows()
    ), encoding="utf-8")

    (OUT/"loteca_card.txt").write_text(
        "CARTÃO LOTECA (intermediário)\n" +
        "\n".join(lines) + f"\n\nRodada: {rodada}\n", encoding="utf-8"
    )

    (REP/"rodada.txt").write_text(
        f"Rodada: {rodada}\nJogos: {len(matches)}\n", encoding="utf-8"
    )

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    args = ap.parse_args()
    main(args.rodada)
