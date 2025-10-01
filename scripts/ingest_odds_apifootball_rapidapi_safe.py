# scripts/ingest_odds_apifootball_rapidapi_safe.py
from __future__ import annotations
import os, shlex, subprocess, json, sys
from pathlib import Path

from scripts.csv_utils import count_csv_rows

def _get_rapidapi_key() -> str:
    return (os.environ.get("X_RAPIDAPI_KEY")
            or os.environ.get("RAPIDAPI_KEY")
            or "").strip()

def main() -> None:
    rodada = (os.environ.get("RODADA") or "").strip()
    season = (os.environ.get("SEASON") or "2025").strip()
    debug = (os.environ.get("DEBUG") or "false").lower() == "true"

    odds_path = Path(f"data/out/{rodada}/odds_apifootball.csv")
    unmatched_path = Path(f"data/out/{rodada}/unmatched_apifootball.csv")

    counts = {
        "odds_apifootball.csv": count_csv_rows(odds_path),
        "unmatched_apifootball.csv": count_csv_rows(unmatched_path),
    }

    # se o módulo "oficial" existir, tentamos rodar
    try:
        import importlib.util
        spec = importlib.util.find_spec("scripts.ingest_odds_apifootball_rapidapi")
        if spec is not None:
            cmd = [
                sys.executable, "-m", "scripts.ingest_odds_apifootball_rapidapi",
                "--rodada", rodada,
                "--season", season,
                "--window", "2",
                "--fuzzy", "0.90",
                "--aliases", "data/aliases_br.json",
            ]
            if debug:
                cmd.append("--debug")
            # Só para garantir que a key correta está no ambiente
            env = os.environ.copy()
            if not env.get("RAPIDAPI_KEY") and _get_rapidapi_key():
                env["RAPIDAPI_KEY"] = _get_rapidapi_key()

            print(f"[apifootball-safe] Executando: {' '.join(shlex.quote(c) for c in cmd)}")
            proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=60)
            if debug:
                # não falha o job: apenas mostra stdout/stderr
                print(proc.stdout)
                print(proc.stderr, file=sys.stderr)

            # atualiza contagens após a execução
            counts["odds_apifootball.csv"] = count_csv_rows(odds_path)
            counts["unmatched_apifootball.csv"] = count_csv_rows(unmatched_path)

        else:
            if debug:
                print("[apifootball-safe] Módulo scripts.ingest_odds_apifootball_rapidapi não encontrado. SAFE: contagens atuais retornadas.")
    except subprocess.TimeoutExpired:
        print("[apifootball-safe] TIMEOUT após 60s — seguindo com contagens (SAFE).")
    except Exception as e:
        print(f"[apifootball-safe] ERRO ao executar módulo interno: {e}")

    print(f"[apifootball-safe] linhas -> {json.dumps(counts, ensure_ascii=False)}")

if __name__ == "__main__":
    main()
