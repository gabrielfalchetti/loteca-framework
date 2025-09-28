#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scorecard.py — Framework Loteca v4.3
Gera um HTML de scorecard com KPIs, tabelas, Top Opportunities e seção de Notícias.
"""

from __future__ import annotations
import argparse, os, sys, html
from datetime import datetime, timezone, timedelta
from typing import Dict
import pandas as pd
import numpy as np

BR_TZ = timezone(timedelta(hours=-3))

def _read_csv_safe(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        print(f"[scorecard] AVISO: arquivo ausente: {path}", file=sys.stderr)
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"[scorecard] AVISO: falha ao ler {path} -> {e}", file=sys.stderr)
        return pd.DataFrame()

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
    df_show = df.replace({np.nan: ""}).copy()
    if len(df_show) > max_rows:
        df_show = df_show.head(max_rows)
    return f"<h2>{html.escape(title)}</h2>" + df_show.to_html(index=False, escape=True)

def _best_edges(risk: pd.DataFrame, topn: int = 10) -> pd.DataFrame:
    if risk.empty:
        return pd.DataFrame()
    rows = []
    for _, r in risk.iterrows():
        options = []
        for outc in ["1","X","2"]:
            edge = r.get(f"edge_{outc}")
            stake = r.get(f"stake_{outc}")
            k = r.get(f"k{outc.lower()}" if outc != "X" else "kx")
            if pd.notna(edge) and pd.notna(k):
                options.append((outc, float(edge), float(stake) if pd.notna(stake) else 0.0, float(k)))
        if not options:
            continue
        best = sorted(options, key=lambda t: (t[1], t[2]), reverse=True)[0]
        rows.append({
            "home": r.get("home",""),
            "away": r.get("away",""),
            "outcome": best[0],
            "edge": best[1],
            "stake": best[2],
            "odds": best[3]
        })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values(["stake","edge"], ascending=False)
    df["edge_%"] = (100.0*df["edge"]).map(lambda x: f"{x:.2f}%")
    df["stake_%"] = (100.0*df["stake"]).map(lambda x: f"{x:.2f}%")
    return df[["home","away","outcome","odds","edge_%","stake_%"]].head(topn)

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
        "xg":            os.path.join(base, "xg.csv"),
        "news":          os.path.join(base, "news.csv"),
        "news_html":     os.path.join(base, "news.html"),
    }

    matches   = _read_csv_safe(paths["matches"])
    odds      = _read_csv_safe(paths["odds"])
    features  = _read_csv_safe(paths["features"])
    probs     = _read_csv_safe(paths["probs"])
    risk      = _read_csv_safe(paths["risk"])
    odds_to   = _read_csv_safe(paths["odds_theodds"])
    odds_af   = _read_csv_safe(paths["odds_apifoot"])
    xg        = _read_csv_safe(paths["xg"])
    news_df   = _read_csv_safe(paths["news"])

    n_matches = len(matches) if not matches.empty else 0
    n_odds = len(odds) if not odds.empty else 0
    n_probs = len(probs) if not probs.empty else 0
    n_risk = len(risk) if not risk.empty else 0
    n_news = len(news_df) if not news_df.empty else 0

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

    top_edges = _best_edges(risk, topn=10)

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
.badge {{ display:inline-block; padding:2px 6px; border-radius:6px; background:#f5f5f5; font-size:12px; margin-left:6px; }}
.newsbox {{ border:1px solid #eee; border-radius:10px; padding:12px; }}
.newsbox a{{ text-decoration:none }}
</style>
</head>
<body>
<h1>Scorecard — {html.escape(rodada)} <span class="badge">v4.3</span></h1>
<div class="meta">Gerado em {datetime.now(BR_TZ).isoformat(timespec='seconds')}</div>
<div class="kpis">
  {_kpi_block("Jogos (matches.csv)", str(n_matches))}
  {_kpi_block("Odds (odds.csv)", str(n_odds), f"Overround médio: {overround_pct}")}
  {_kpi_block("Probabilidades (probabilities.csv)", str(n_probs))}
  {_kpi_block("Risco/Edge (risk_report.csv)", str(n_risk))}
  {_kpi_block("Notícias (news.csv)", str(n_news))}
</div>
""")

    # Notícias (resumo e link para news.html)
    if n_news > 0 and os.path.isfile(paths["news_html"]):
        html_parts.append(f"""
<h2>Notícias</h2>
<div class="newsbox">
  <p>Foram agregadas <strong>{n_news}</strong> notícias recentes sobre os confrontos desta rodada.</p>
  <p>Veja a lista com links: <a href="news.html" target="_blank" rel="noopener noreferrer">news.html</a></p>
</div>
""")
    else:
        html_parts.append("<h2>Notícias</h2><p><em>Nenhuma notícia encontrada nesta rodada.</em></p>")

    # Top opportunities
    html_parts.append(_table_html(top_edges, "Top Opportunities (edge/kelly)", max_rows=10))

    # Tabelas
    html_parts.append(_table_html(matches, "Matches (matches.csv)", max_rows=200))
    html_parts.append(_table_html(odds, "Odds Consolidadas (odds.csv)", max_rows=200))
    html_parts.append(_table_html(features, "Features Base (features_base.csv)", max_rows=200))
    html_parts.append(_table_html(probs, "Probabilidades (probabilities.csv)", max_rows=200))
    html_parts.append(_table_html(risk, "Relatório de Risco (risk_report.csv)", max_rows=200))
    html_parts.append(_table_html(xg, "xG (xg.csv)", max_rows=200))
    html_parts.append(_table_html(odds_to, "Odds TheOddsAPI (odds_theoddsapi.csv)", max_rows=200))
    html_parts.append(_table_html(odds_af, "Odds API-Football (odds_apifootball.csv)", max_rows=200))

    html_parts.append("""
<div class="footer">
  <div>Framework Loteca v4.3 — relatório automático.</div>
</div>
</body>
</html>
""")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(html_parts))
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
