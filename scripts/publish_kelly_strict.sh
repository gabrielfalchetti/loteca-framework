#!/usr/bin/env bash
set -euo pipefail

# Uso:
#   scripts/publish_kelly_strict.sh --rodada 2025-09-27_1213 [--debug]
#
# Comportamento:
#   - Verifica se data/out/<RODADA>/odds_consensus.csv existe e contém odds reais
#     (pelo menos 2 entre odds_home/odds_draw/odds_away > 1.0 em alguma linha).
#   - Se não houver odds reais, sai com código 10 (fail-fast).
#   - Se houver, chama scripts/publish_kelly.py com os mesmos flags.

RODADA=""
DEBUG=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rodada)
      RODADA="${2:-}"
      shift 2
      ;;
    --debug)
      DEBUG=true
      shift
      ;;
    *)
      echo "[strict] ERRO: argumento desconhecido: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "${RODADA:-}" ]]; then
  echo "[strict] ERRO: --rodada é obrigatório" >&2
  exit 2
fi

OUT_DIR="data/out/${RODADA}"
CONS="${OUT_DIR}/odds_consensus.csv"

if [[ ! -f "$CONS" ]]; then
  echo "[strict] ERRO: arquivo não encontrado: ${CONS}" >&2
  exit 10
fi

# Verificação de odds reais via Python embutido
python - "$CONS" ${DEBUG:+--debug} << 'PYCODE'
import sys, pandas as pd, numpy as np

debug = ("--debug" in sys.argv)
path  = sys.argv[1]

try:
    df = pd.read_csv(path)
except Exception as e:
    print(f"[strict] ERRO: falha ao ler {path}: {e}", file=sys.stderr)
    sys.exit(10)

if debug:
    print(f"[strict] consensus lido: {len(df)} linhas")

# Normalização leve de nomes comuns
ren = {
    "home":"odds_home","1":"odds_home","home_win":"odds_home","price_home":"odds_home",
    "draw":"odds_draw","x":"odds_draw","tie":"odds_draw","price_draw":"odds_draw",
    "away":"odds_away","2":"odds_away","away_win":"odds_away","price_away":"odds_away",
}
cols = {c: ren.get(str(c).strip().lower(), c) for c in df.columns}
df = df.rename(columns=cols)

for c in ["odds_home","odds_draw","odds_away"]:
    if c not in df.columns:
        df[c] = np.nan
    df[c] = pd.to_numeric(df[c], errors="coerce")

def valid_row(r):
    vals = []
    for c in ("odds_home","odds_draw","odds_away"):
        v = r.get(c)
        if pd.notna(v) and np.isfinite(v) and v > 1.0:
            vals.append(v)
    # Exigimos pelo menos DUAS odds > 1.0
    return len(vals) >= 2

valid = int(df.apply(valid_row, axis=1).sum())
if debug:
    print(f"[strict] linhas com >=2 odds > 1.0: {valid}")

if valid <= 0:
    print("[strict] ERRO: nenhuma linha de odds válida (>=2 odds_* > 1.0).", file=sys.stderr)
    sys.exit(10)

print(f"[strict] OK: {valid} linhas com odds reais.")
sys.exit(0)
PYCODE

# Se chegamos aqui, há odds reais → roda Kelly
EXTRA=()
$DEBUG && EXTRA+=(--debug)

exec python scripts/publish_kelly.py --rodada "${RODADA}" "${EXTRA[@]}"