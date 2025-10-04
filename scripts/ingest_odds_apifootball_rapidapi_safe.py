#!/usr/bin/env python3
"""
Wrapper 'safe' para ingestão via API-Football. Ele:

1) Lê RODADA, SEASON e APIFOOT_LEAGUE_IDS (env ou CLI).
2) Tenta rodar o script legado 'scripts/ingest_odds_apifootball_rapidapi.py'
   passando os argumentos corretos.
3) Captura 403/erros e escreve arquivos vazios controlados, sem quebrar o job.
"""

import os, sys, argparse, subprocess, pathlib, json

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--season", required=True)
    ap.add_argument("--leagues", default=os.environ.get("APIFOOT_LEAGUE_IDS",""))
    ap.add_argument("--debug", action="store_true")
    return ap.parse_args()

def main():
    args = parse_args()
    xkey = os.environ.get("X_RAPIDAPI_KEY")
    if not xkey:
        print("[apifootball-safe] SKIP: X_RAPIDAPI_KEY ausente.")
        return 0

    out_dir = pathlib.Path(f"data/out/{args.rodada}")
    out_dir.mkdir(parents=True, exist_ok=True)
    odds_path = out_dir / "odds_apifootball.csv"
    unmatched_path = out_dir / "unmatched_apifootball.csv"

    # Se script legado existir, chamamos ele (já tem lógica completa e fuzzy/aliases)
    target = pathlib.Path("scripts/ingest_odds_apifootball_rapidapi.py")
    if not target.exists():
        print("[apifootball-safe] AVISO: scripts/ingest_odds_apifootball_rapidapi.py não encontrado — escrevendo vazios.")
        odds_path.write_text("")
        unmatched_path.write_text("")
        print(f"[apifootball-safe] linhas -> {{\"odds_apifootball.csv\": 0, \"unmatched_apifootball.csv\": 0}}")
        return 0

    cmd = [
        sys.executable, "-m", "scripts.ingest_odds_apifootball_rapidapi",
        "--rodada", args.rodada,
        "--season", args.season,
        "--window", "2",
        "--fuzzy", "0.90",
        "--aliases", "data/aliases_br.json"
    ]
    if args.debug:
        cmd.append("--debug")

    try:
        print(f"[apifootball-safe] Executando: {' '.join(cmd)}")
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        print(f"[apifootball-safe] ERRO ao executar script legado (exit {e.returncode}). "
              f"Escrevendo arquivos vazios para não quebrar o fluxo.")
        odds_path.write_text("")
        unmatched_path.write_text("")
        print(f"[apifootball-safe] linhas -> {{\"odds_apifootball.csv\": 0, \"unmatched_apifootball.csv\": 0}}")
        return 0

    # Se chegou aqui, o legado escreveu os arquivos. Só imprimimos um resumo simples.
    ok_odds = odds_path.exists() and odds_path.stat().st_size > 0
    ok_unm  = unmatched_path.exists() and unmatched_path.stat().st_size > 0
    print(f"[apifootball-safe] linhas -> {{\"odds_apifootball.csv\": {'?' if not ok_odds else '>'}, "
          f"\"unmatched_apifootball.csv\": {'?' if not ok_unm else '>'}}}")
    return 0

if __name__ == "__main__":
    sys.exit(main())