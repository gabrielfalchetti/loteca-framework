# -*- coding: utf-8 -*-
"""
Sanity / Reality Check
- Confere existência e coerência mínima dos artefatos da rodada.
- Em modo --strict, retorna exit>0 se faltar odds/predições/cartão.
Saídas auxiliares:
  data/out/<rodada>/reality_report.json
  data/out/<rodada>/reality_report.txt
"""
from __future__ import annotations
import argparse, json
from pathlib import Path

FILES_MUST = ["odds_theoddsapi.csv", "predictions_market.csv", "loteca_cartao.txt"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--strict", action="store_true")
    args = ap.parse_args()

    out_dir = Path(f"data/out/{args.rodada}")
    out_dir.mkdir(parents=True, exist_ok=True)

    report = {"rodada": args.rodada, "checks": {}, "ok": True}
    for f in FILES_MUST:
        p = out_dir/f
        ok = p.exists() and p.stat().st_size > 0
        report["checks"][f] = ok
        if not ok:
            report["ok"] = False

    (out_dir/"reality_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    summary = [f"{'OK ' if v else 'NOK'} {k}" for k,v in report["checks"].items()]
    (out_dir/"reality_report.txt").write_text("\n".join(summary) + "\n", encoding="utf-8")

    print(f"[reality] OK -> {out_dir/'reality_report.json'}")
    print(f"[reality] RESUMO -> {out_dir/'reality_report.txt'}")

    if args.strict and not report["ok"]:
        raise SystemExit(2)

if __name__ == "__main__":
    main()