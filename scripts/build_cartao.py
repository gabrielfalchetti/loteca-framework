# scripts/build_cartao.py
from __future__ import annotations
import csv
from pathlib import Path
import os

def _read_csv(path: Path) -> list[dict]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def main() -> None:
    rodada = (os.environ.get("RODADA") or "").strip()
    out_dir = Path(f"data/out/{rodada}")
    out_dir.mkdir(parents=True, exist_ok=True)

    odds = { (r.get("home","").strip(), r.get("away","").strip()): r
             for r in _read_csv(out_dir / "odds_consensus.csv") }

    news = { (r.get("home","").strip(), r.get("away","").strip()): r
             for r in _read_csv(out_dir / "news.csv") }

    inj =  { (r.get("home","").strip(), r.get("away","").strip()): r
             for r in _read_csv(out_dir / "injuries.csv") }

    # matches_source como fonte de ordem
    matches = _read_csv(Path(f"data/in/{rodada}/matches_source.csv"))

    lines: list[str] = []
    for m in matches:
        home = (m.get("home") or m.get("home_team") or "").strip()
        away = (m.get("away") or m.get("away_team") or "").strip()

        o = odds.get((home, away), {})
        pick = o.get("pick") or o.get("palpite") or ""
        pick_odd = o.get("pick_odd") or o.get("odd") or ""

        n = news.get((home, away), {})
        hits = n.get("news_hits") or "0"

        ij = inj.get((home, away), {})
        h_inj = ij.get("home_injuries") or "0"
        a_inj = ij.get("away_injuries") or "0"

        lines.append(f"{home} x {away} — Palpite: {pick or '-'} — odd do palpite: {pick_odd or '-'} — news: {hits} hits, injury {h_inj}/{a_inj}")

    out_txt = out_dir / "loteca_cartao.txt"
    out_txt.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[cartao] OK -> {out_txt}")

if __name__ == "__main__":
    main()
