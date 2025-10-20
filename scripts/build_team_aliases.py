# -*- coding: utf-8 -*-
import argparse
import sys
import requests
import json
from unidecode import unidecode
from datetime import datetime

def _log(msg: str) -> None:
    print(f"[build_aliases] {msg}", flush=True)

def generate_variations(name: str) -> list:
    variations = [name, unidecode(name), name.lower(), name.upper()]
    if "atletico" in name.lower():
        variations.append(name.replace("Atletico", "Atlético"))
    # Adicionar variações regionais comuns
    for suffix in ['/RJ', '/SP', '/MG', '/RS', '/CE', '/BA', '/PE']:
        variations.append(name + suffix)
    return list(set(variations))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api_key", default=os.getenv("API_FOOTBALL_KEY"))
    ap.add_argument("--out_json", required=True)
    args = ap.parse_args()

    if not args.api_key:
        _log("API_FOOTBALL_KEY não definida")
        sys.exit(3)

    headers = {"x-apisports-key": args.api_key}
    leagues = [
        71,  # Série A Brasil
        72,  # Série B Brasil
        203, # Copa do Brasil
        70,  # Carioca
        74,  # Mineiro
        77,  # Gaúcho
        39,  # Premier League
        140, # La Liga
        13,  # Libertadores
        2,   # Champions League
        112  # Outra liga se necessário
    ]
    year = datetime.now().year

    aliases = {}
    for league in leagues:
        url = "https://v3.football.api-sports.io/teams"
        params = {"league": league, "season": year}
        try:
            response = requests.get(url, headers=headers, params=params, timeout=25)
            response.raise_for_status()
            teams_data = response.json().get("response", [])
            _log(f"Times retornados para liga {league}: {len(teams_data)}")
            for team in teams_data:
                team_name = team["team"]["name"]
                code = team["team"].get("code", "")
                id = team["team"]["id"]
                normalized = normalize_team_name(team_name)
                aliases[normalized] = generate_variations(team_name) + [code] + [f"ID:{id}"]
        except Exception as e:
            _log(f"Erro ao buscar times da liga {league}: {e}")

    os.makedirs(os.path.dirname(args.out_json), exist_ok=True)
    with open(args.out_json, 'w') as f:
        json.dump(aliases, f, indent=4)
    _log(f"Arquivo {args.out_json} gerado com {len(aliases)} aliases")

if __name__ == "__main__":
    main()