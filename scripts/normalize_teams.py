# scripts/normalize_teams.py
import argparse, csv, os, sys, re
from unidecode import unidecode

PT_EN = {
    # Seleções comuns em PT → EN
    "eslováquia": "slovakia",
    "luxemburgo": "luxembourg",
    "eslovênia": "slovenia",
    "suica": "switzerland", "suíça": "switzerland",
    "irlanda do norte": "northern ireland",
    "alemanha": "germany",
    "islândia": "iceland", "islandia": "iceland",
    "frança": "france", "franca": "france",
    "país de gales": "wales", "pais de gales": "wales",
    "bélgica": "belgium", "belgica": "belgium",
    "suécia": "sweden", "suecia": "sweden",
    "macedônia do norte": "north macedonia", "macedonia do norte": "north macedonia",
    "cazaquistão": "kazakhstan", "cazaquistao": "kazakhstan",
    "ucrânia": "ukraine", "ucrania": "ukraine",
    "azerbaijão": "azerbaijan", "azerbaijao": "azerbaijan",
    # BR clubes mais comuns
    "athletico-pr": "athletico pr", "atletico-pr":"athletico pr",
    "atlético-go": "atletico go", "atletico-go":"atletico go",
    "crb": "crb", "ferroviária":"ferroviaria",
    "botafogo-sp":"botafogo sp", "chapecoense":"chapecoense",
    "paysandu":"paysandu", "remo":"remo", "avaí":"avai", "avai":"avai",
    "volta redonda":"volta redonda",
}

def canon(s: str) -> str:
    s = unidecode(s or "").lower().strip()
    s = re.sub(r"[^a-z0-9\s\-]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return PT_EN.get(s, s)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    assert os.path.exists(args.source), f"{args.source} not found"

    out_rows = []
    with open(args.source, encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for row in rd:
            out_rows.append({
                "match_id": row["match_id"],
                "home": row["home"],
                "away": row["away"],
                "home_norm": canon(row["home"]),
                "away_norm": canon(row["away"]),
            })
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=["match_id","home","away","home_norm","away_norm"])
        wr.writeheader()
        wr.writerows(out_rows)
    print(f"[normalize] OK — {len(out_rows)} linhas em {args.out}")

if __name__ == "__main__":
    main()