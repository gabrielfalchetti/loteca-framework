mkdir -p scripts
cat > scripts/build_team_aliases.py << 'EOF'
# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
from unidecode import unidecode
import json

def _log(msg: str) -> None:
    print(f"[build_team_aliases] {msg}", flush=True)

def build_team_aliases(matches_file, out_json):
    if not os.path.isfile(matches_file):
        _log(f"Arquivo {matches_file} não encontrado")
        sys.exit(4)

    try:
        matches = pd.read_csv(matches_file)
    except Exception as e:
        _log(f"Erro ao ler {matches_file}: {e}")
        sys.exit(4)

    home_col = 'team_home' if 'team_home' in matches.columns else 'home'
    away_col = 'team_away' if 'team_away' in matches.columns else 'away'
    teams = set(matches[home_col]).union(set(matches[away_col]))

    aliases = {}
    for team in teams:
        norm_team = unidecode(team).lower().strip()
        aliases[norm_team] = [team, norm_team, team.replace(' ', '-'), team + ' FC', team + ' SP', team + ' RJ', team + ' MG']

    os.makedirs(os.path.dirname(out_json), exist_ok=True)
    with open(out_json, 'w') as f:
        json.dump(aliases, f)
    _log(f"Aliases gerados em {out_json} para {len(aliases)} times")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--matches", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    build_team_aliases(args.matches, args.out)

if __name__ == "__main__":
    main()
EOF

cat > scripts/auto_aliases_harvest.py << 'EOF'
# -*- coding: utf-8 -*-
import argparse
import sys
import json
import os

def _log(msg: str) -> None:
    print(f"[auto_aliases_harvest] {msg}", flush=True)

def auto_aliases_harvest(aliases_file):
    if not os.path.isfile(aliases_file):
        _log(f"Arquivo {aliases_file} não encontrado")
        sys.exit(4)

    try:
        with open(aliases_file, 'r') as f:
            aliases = json.load(f)
    except Exception as e:
        _log(f"Erro ao ler {aliases_file}: {e}")
        sys.exit(4)

    # Simular harvest (adicionar aliases adicionais)
    for key in list(aliases.keys()):
        aliases[key].append(key.upper())
        aliases[key].append(key.lower())

    with open(aliases_file, 'w') as f:
        json.dump(aliases, f)
    _log(f"Aliases harvest concluído, atualizado {aliases_file}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--aliases", required=True)
    args = ap.parse_args()

    auto_aliases_harvest(args.aliases)

if __name__ == "__main__":
    main()
EOF

touch scripts/__init__.py