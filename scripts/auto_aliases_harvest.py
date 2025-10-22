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