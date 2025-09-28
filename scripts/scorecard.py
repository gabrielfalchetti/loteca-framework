#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scorecard.py — Framework Loteca v4.3
Gera um HTML de scorecard consolidando dados da rodada.

Uso:
  python scripts/scorecard.py --rodada 2025-09-27_1213 --out data/out/2025-09-27_1213/scorecard.html

Entradas (todas opcionais, o script tolera ausências):
  data/out/<RODADA>/matches.csv
  data/out/<RODADA>/odds.csv
  data/out/<RODADA>/features_base.csv
  data/out/<RODADA>/probabilities.csv
  data/out/<RODADA>/risk_report.csv

Saída:
  HTML com tabelas e KPIs mínimos.
"""

from __future__ import annotations
import argparse
import os
import sys
import html
from datetime import datetime, timezone, timedelta
from typing import Dict

import pandas as pd
import numpy as np

BR_TZ = timezone(timedelta(hours=-3))

def _read_csv_safe(path: str, required_cols=None) -> pd.DataFrame:
    if not os.path.isfile(path):
        print(f"[scorecard] AVISO: arquivo ausente: {path}", file=sys.stderr)
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"[scorecard] AVISO: falha ao ler {path} -> {e}", file=sys.stderr)
        return pd.DataFrame()
    if required_cols:
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            print(f"[scorecard] AVISO: {path} sem colunas {missing}", file=sys.stderr)
    return df

def _kpi_block(title: str, value: str, sub="") -> str:
    sub_html = f'<div class="kpi-sub">{html.escape(sub)}</div>' if sub else ""
    return f"""
    <div class="kpi">
      <div class="kpi-title">{html.escape(title)}</div>
      <div class="kpi-value">{html.escape(value)}</div>
      {sub_html}
    </div>
    """

def _table_html(df: pd.DataFrame, title: str, max_rows: int = 200) -> str:
    if df.empty:
        return f"<h2>{html.escape(title)}</h2><p><em>Sem dados</em></p>"
    df_show = df.copy()
    if len(df_show) > max_rows:
        df_show = df_show.head(max_rows)
    # escapa HTML perigoso
    df_show = df_show.replace({np.nan: ""})
    return f"<h2>{html.escape(title)}</h2>" + df_show.to_html(index=False, escape=True)

def build_scorecard(rodada: str, out_path: str) -> None:
    out_dir = os.path.dirname(out_path)
    os.makedirs(out_dir, exist_ok=True)

    base = os.path.join("data","out",rodada)
    paths: Dict[str,str] = {
        "matches":       os.path.join(base, "matches.csv"),
        "odds":          os.path.join(base, "odds.csv"),
        "features":      os.path.join(base, "features_base.csv"),
        "probs":         os.path.join(base, "probabilities.csv"),
        "risk":          os.path.join(base, "risk_report.csv"),
        "odds_theodds":  os.path.join(base, "odds_theoddsapi.csv"),
        "odds_apifoot":  os.path.join(base, "odds_apifootball.csv"),
    }

    matches   = _read_csv_safe(paths["matches"])
    odds      = _read_csv_safe(paths["odds"])
    features  = _read_csv_safe(paths["features"])
    probs     = _read_csv_safe(paths["probs"])
    risk      = _read_csv_safe(paths["risk"])
    odds_to   = _read_csv_safe(paths["odds_theodds"])
    odds_af   = _read_csv_safe(paths["odds_apifoot"])

    # KPIs simples
    n_matches = len(matches) if not matches.empty else 0
    n_odds = len(odds) if not odds.empty else 0
    n_probs = len(probs) if not probs.empty else 0
    n_risk = len(risk) if not risk.empty else 0

    # Overround médio (se odds 1X2 disponíveis)
    overround = np.nan
    if not odds.empty and all(c in odds.columns for c in ["k1","kx","k2"]):
        invsum = []
        for _, r in odds.iterrows():
            try:
                inv = sum(1.0/float(r[c]) for c in ["k1","kx","k2"] if pd.notna(r[c]) and float(r[c])>0)
                if inv>0:
                    invsum.append(inv)
            except Exception:
                pass
        if invsum:
            overround = float(np.mean(invsum))
    overround_pct = f"{(overround-1)*100:.2f}%" if not np.isnan(overround) and overround>1 else "n/d"

    # Edge médio (se risk_report tem colunas 'edge' ou similar)
    edge_mean = "n/d"
    if not risk.empty:
        cand_cols = [c for c in risk.columns if c.lower().startswith("edge")]
        if cand_cols:
            try:
                v = pd.to_numeric(risk[cand_cols[0]], errors="coerce").dropna()
                if len(v)>0:
                    edge_mean = f"{100*float(v.mean()):.2f}%"
            except Exception:
                pass

    # Tabelas resumidas
    # 1) Jogos + probs se existirem
    joined = pd.DataFrame()
    if not matches.empty:
        joined = matches[["match_id","home","away","date"]].copy()
        if not probs.empty:
            # tenta merge por chaves normalizadas se existirem
            for k in ["home_n","away_n"]:
                if k not in joined.columns and k in probs.columns:
                    joined[k] = ""
            left_on = []
            right_on = []
            if "home_n" in joined.columns and "away_n" in joined.columns and \
               "home_n" in probs.columns and "away_n" in probs.columns:
                left_on = ["home_n","away_n"]
                right_on = ["home_n","away_n"]
            else:
                left_on = ["home","away"]
                right_on = ["home","away"]
            try:
                joined = joined.merge(
                    probs,
                    left_on=left_on,
                    right_on=right_on,
                    how="left",
                    suffixes=("","_p")
                )
            except Exception:
                pass

    html_parts = []
    html_parts.append(f"""
<!doctype html>
<html lang="pt-br">
<head>
<meta charset="utf-8">
<title>Scorecard - {html.escape(rodada)}</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; margin: 20px; color: #111; }}
h1 {{ margin: 0 0 8px 0; }}
h2 {{ margin-top: 28px; }}
.meta {{ color: #666; font-size: 12px; margin-bottom: 16px; }}
.kpis {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin: 16px 0 8px; }}
.kpi {{ border: 1px solid #eee; border-radius: 10px; padding: 12px; }}
.kpi-title {{ font-size: 12px; color: #666; }}
.kpi-value {{ font-size: 22px; font-weight: 700; }}
.kpi-sub {{ font-size: 12px; color: #444; margin-top: 6px; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border: 1px solid #eee; padding: 6px 8px; font-size: 13px; }}
th {{ background: #fafafa; text-align: left; }}
em {{ color: #666; }}
.footer {{ margin-top: 36px; color: #777; font-size: 12px; }}
</style>
</head>
<body>
<h1>Scorecard — {html.escape(rodada)}</h1>
<div class="meta">Gerado em {datetime.now(BR_TZ).isoformat(timespec='seconds')}</div>
<div class="kpis">
  {_kpi_block("Jogos (matches.csv)", str(n_matches))}
  {_kpi_block("Odds (odds.csv)", str(n_odds), f"Overround médio: {overround_pct}")}
  {_kpi_block("Probabilidades (probabilities.csv)", str(n_probs))}
  {_kpi_block("Risco/Edge (risk_report.csv)", str(n_risk), f"Edge médio: {edge_mean}")}
</div>
""")

    html_parts.append(_table_html(joined, "Resumo da Rodada (matches + probabilities)", max_rows=200))
    html_parts.append(_table_html(odds, "Odds Consolidadas (odds.csv)", max_rows=200))
    html_parts.append(_table_html(features, "Features Base (features_base.csv)", max_rows=200))
    html_parts.append(_table_html(probs, "Probabilidades (probabilities.csv)", max_rows=200))
    html_parts.append(_table_html(risk, "Relatório de Risco (risk_report.csv)", max_rows=200))
    html_parts.append(_table_html(odds_to, "Odds TheOddsAPI (odds_theoddsapi.csv)", max_rows=200))
    html_parts.append(_table_html(odds_af, "Odds API-Football (odds_apifootball.csv)", max_rows=200))

    html_parts.append("""
<div class="footer">
  <div>Framework Loteca v4.3 — relatório automático.</div>
</div>
</body>
</html>
""")

    html_str = "\n".join(html_parts)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_str)
    print(f"[scorecard] OK -> {out_path}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Identificador da rodada (ex.: 2025-09-27_1213)")
    ap.add_argument("--out", default="", help="Caminho do HTML de saída (opcional)")
    args = ap.parse_args()

    rodada = (args.rodada or "").strip()
    if not rodada:
        raise RuntimeError("scorecard: --rodada vazio")

    out_path = (args.out or "").strip()
    if not out_path:
        out_path = os.path.join("data","out",rodada,"scorecard.html")

    build_scorecard(rodada, out_path)

if __name__ == "__main__":
    main()
