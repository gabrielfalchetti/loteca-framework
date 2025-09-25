#!/usr/bin/env python3
import argparse, pandas as pd, yaml
from pathlib import Path

def cfg(): return yaml.safe_load(open("config/config.yaml","r",encoding="utf-8"))

def main(rodada, contest_id):
    C = cfg()
    report = C["paths"]["context_score_out"].replace("${rodada}", rodada)
    df = pd.read_csv(report)
    mapping = pd.read_csv("data/raw/loteca_map.csv")
    chosen = mapping[mapping["loteca_contest_id"]==int(contest_id)]
    out = df.merge(chosen, on="match_id", how="inner")
    Path("reports").mkdir(exist_ok=True)
    out.to_csv(f"reports/loteca_{contest_id}_scores_{rodada}.csv", index=False)
    print(f"[OK] loteca recorte → reports/loteca_{contest_id}_scores_{rodada}.csv")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--contest", required=True, help="ID do concurso da Loteca (ex.: 1023)")
    args = ap.parse_args()
    main(args.rodada, args.contest)
