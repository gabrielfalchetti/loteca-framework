# -*- coding: utf-8 -*-
import argparse
import sys
import requests
import json
from unidecode import unidecode
from datetime import datetime

def _log(msg: str) -> None:
    print(f"[build_aliases] {msg}", flush=True)

def normalize_team_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = unidecode(name).lower().strip()
    name = name.replace("/rj", "").replace("/sp", "").replace("/mg", "").replace("/rs", "").replace("/ce", "").replace("/ba", "").replace("/pe", "")
    name = name.replace("atletico", "atlético").replace("sao paulo", "são paulo").replace("inter de milao", "inter").replace("manchester united", "manchester utd")
    name = name.replace("sport recife", "sport").replace("atletico mineiro", "atlético").replace("bragantino-sp", "bragantino").replace("vasco da gama", "vasco")
    name = name.replace("fluminense", "fluminense").replace("santos", "santos").replace("vitoria", "vitória").replace("mirassol", "mirassol").replace("gremio", "grêmio")
    name = name.replace("juventude", "juventude").replace("roma", "roma").replace("getafe", "getafe").replace("real madrid", "real madrid").replace("liverpool", "liverpool")
    name = name.replace("atalanta bergamas", "atalanta").replace("fiorentina", "fiorentina").replace("osasuna", "osasuna")
    return name.capitalize()

def generate_variations(name: str) -> list:
    variations = [name, unidecode(name), name.lower(), name.upper()]
    if "atletico" in name.lower():
        variations.append(name.replace("Atletico", "Atlético"))
    for suffix in ['/RJ', '/SP', '/MG', '/RS', '/CE', '/BA', '/PE']:
        variations.append(name + suffix)
    return list(set(variations))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api_key", default=os.getenv("API_FOOTBALL_KEY"))
    ap.add_argument("--api_key_theodds", default=os.getenv("THEODDS_API_KEY"))
    ap.add_argument("--out_json", required=True)
    ap.add_argument("--regions", default="uk,eu,us,au")
    args = ap.parse_args()

    if not args.api_key or not args.api_key_theodds:
        _log("API_FOOTBALL_KEY ou THEODDS_API_KEY não definida")
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
    # Buscar times da API-Football
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
                code = team["team"].get("code", team_name)
                id = team["team"]["id"]
                normalized = normalize_team_name(team_name)
                aliases[normalized] = generate_variations(team_name) + [code] + [f"ID:{id}"]
        except Exception as e:
            _log(f"Erro ao buscar times da liga {league}: {e}")

    # Buscar nomes adicionais do TheOddsAPI
    sports = [
        "soccer_brazil_campeonato",
        "soccer_brazil_serie_b",
        "soccer_italy_serie_a",
        "soccer_epl",
        "soccer_spain_la_liga",
        "soccer_conmebol_copa_libertadores"
    ]
    for sport in sports:
        url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds?regions={args.regions}&markets=h2h&dateFormat=iso&oddsFormat=decimal&apiKey={args.api_key_theodds}"
        try:
            response = requests.get(url, timeout=25)
            response.raise_for_status()
            games = response.json()
            _log(f"TheOddsAPI retornou {len(games)} jogos para {sport}")
            for game in games:
                home_team = game["home_team"]
                away_team = game["away_team"]
                home_normalized = normalize_team_name(home_team)
                away_normalized = normalize_team_name(away_team)
                if home_normalized in aliases:
                    aliases[home_normalized].append(home_team)
                if away_normalized in aliases:
                    aliases[away_normalized].append(away_team)
        except Exception as e:
            _log(f"Erro ao buscar {sport}: {e}")

    # Remover duplicatas
    for key in aliases:
        aliases[key] = list(set(aliases[key]))

    os.makedirs(os.path.dirname(args.out_json), exist_ok=True)
    with open(args.out_json, 'w') as f:
        json.dump(aliases, f, indent=4)
    _log(f"Arquivo {args.out_json} gerado com {len(aliases)} aliases")

if __name__ == "__main__":
    main()