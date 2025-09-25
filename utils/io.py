from pathlib import Path
import pandas as pd, yaml, sys

ROOT = Path(__file__).resolve().parents[1]

def load_schema():
    return yaml.safe_load((ROOT/"config/schema.yaml").read_text(encoding="utf-8"))

def ensure_cols(df: pd.DataFrame, req_cols, name):
    missing = [c for c in req_cols if c not in df.columns]
    if missing:
        print(f"ERRO: {name} com colunas faltando: {missing}", file=sys.stderr)
        sys.exit(1)
    if df.shape[0] == 0:
        print(f"ERRO: {name} está vazio (0 linhas).", file=sys.stderr)
        sys.exit(1)

def save_csv(df, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
