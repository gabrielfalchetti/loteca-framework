import argparse, os, sys, yaml
from pathlib import Path
from subprocess import check_call, CalledProcessError

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"

def step(cmd):
    print(f"\n=== RUNNING: {cmd} ===", flush=True)
    check_call(cmd, shell=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=True)
    args = parser.parse_args()

    cfg = yaml.safe_load((ROOT/"config/features.yaml").read_text(encoding="utf-8"))
    use = cfg.get("use_features", {})

    try:
        if use.get("odds", True):    step(f"python {SCRIPTS/'ingest_odds.py'} --rodada {args.rodada}")
        if use.get("table", True):   step(f"python {SCRIPTS/'ingest_table.py'} --rodada {args.rodada}")
        if use.get("weather", True): step(f"python {SCRIPTS/'ingest_weather.py'} --rodada {args.rodada}")
        if use.get("news", True):    step(f"python {SCRIPTS/'ingest_news.py'} --rodada {args.rodada}")

        step(f"python {SCRIPTS/'merge_features.py'} --rodada {args.rodada}")
        step(f"python {SCRIPTS/'train_model.py'} --rodada {args.rodada}")
        step(f"python {SCRIPTS/'build_betcard.py'} --rodada {args.rodada}")

    except CalledProcessError as e:
        print(f"ERRO em uma etapa: {e}", file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
