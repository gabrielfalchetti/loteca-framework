# -*- coding: utf-8 -*-
import argparse
import pandas as pd
import requests
import os
import json

def _log(msg: str) -> None:
    print(f"[ingest_odds_apifootball] {msg}", flush=True)

def ingest_odds_apifootball(rodada, source_csv, api_key, api_key_theodds, regions, aliases_file):
    try:
        matches = pd.read_csv(source_csv)
    except Exception as e:
        _log(f"Erro ao ler {source_csv}: {e}")
        return

    try:
        from unidecode import unidecode
    except ImportError:
        _log("Módulo unidecode não encontrado. Usando nomes sem normalização.")
        unidecode = lambda x: x.lower().strip()

    try:
        with open(aliases_file, 'r') as f:
            aliases = json.load(f)
    except Exception as e:
        _log(f"Erro ao ler {aliases_file}: {e}")
        aliases = {}

    odds_data = []
    for _, row in matches.iterrows():
        home_team = row['team_home'] if 'team_home' in row else row['home']
        away_team = row['team_away'] if 'team_away' in row else row['away']
        norm_home = unidecode(home_team).lower().strip()
        norm_away = unidecode(away_team).lower().strip()
        home_aliases = aliases.get(norm_home, [home_team])
        away_aliases = aliases.get(norm_away, [away_team])

        leagues = [
            {'league': 71, 'season': 2025},  # Serie A
            {'league': 72, 'season': 2025},  # Serie B
            {'league': 73, 'season': 2025}   # Copa do Brasil
        ]
        for league in leagues:
            url = f"https://v3.football.api-sports.io/odds?league={league['league']}&season={league['season']}&apiKey={api_key}"
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                for fixture in data.get('response', []):
                    teams = fixture.get('fixture', {}).get('teams', {})
                    if any(h.lower() in teams.get('home', {}).get('name', '').lower() for h in home_aliases) and \
                       any(a.lower() in teams.get('away', {}).get('name', '').lower() for a in away_aliases):
                        odds_data.append({
                            'team_home': home_team,
                            'team_away': away_team,
                            'odds_home': fixture.get('bookmakers', [{}])[0].get('bets', [{}])[0].get('values', [{}])[0].get('odd', 2.0),
                            'odds_draw': fixture.get('bookmakers', [{}])[0].get('bets', [{}])[0].get('values', [{}])[1].get('odd', 3.0),
                            'odds_away': fixture.get('bookmakers', [{}])[0].get('bets', [{}])[0].get('values', [{}])[2].get('odd', 2.5)
                        })
                        break
                else:
                    _log(f"Sem odds para {home_team} x {away_team} na liga {league['league']}")
                    odds_data.append({
                        'team_home': home_team,
                        'team_away': away_team,
                        'odds_home': 2.0,
                        'odds_draw': 3.0,
                        'odds_away': 2.5
                    })
                break
            except Exception as e:
                _log(f"Erro ao buscar odds para {home_team} x {away_team} na liga {league['league']}: {e}")
                # Fallback para endpoint /fixtures
                url_fallback = f"https://v3.football.api-sports.io/fixtures?league={league['league']}&season={league['season']}&apiKey={api_key}"
                try:
                    response = requests.get(url_fallback, timeout=10)
                    response.raise_for_status()
                    data = response.json()
                    for fixture in data.get('response', []):
                        teams = fixture.get('teams', {})
                        if any(h.lower() in teams.get('home', {}).get('name', '').lower() for h in home_aliases) and \
                           any(a.lower() in teams.get('away', {}).get('name', '').lower() for a in away_aliases):
                            odds_data.append({
                                'team_home': home_team,
                                'team_away': away_team,
                                'odds_home': 2.0,
                                'odds_draw': 3.0,
                                'odds_away': 2.5
                            })
                            break
                    else:
                        _log(f"Sem dados para {home_team} x {away_team} na liga {league['league']} (fallback)")
                        odds_data.append({
                            'team_home': home_team,
                            'team_away': away_team,
                            'odds_home': 2.0,
                            'odds_draw': 3.0,
                            'odds_away': 2.5
                        })
                    break
                except Exception as e:
                    _log(f"Erro no fallback para {home_team} x {away_team} na liga {league['league']}: {e}")
                    odds_data.append({
                        'team_home': home_team,
                        'team_away': away_team,
                        'odds_home': 2.0,
                        'odds_draw': 3.0,
                        'odds_away': 2.5
                    })

    output_file = os.path.join(rodada, 'odds_apifootball.csv')
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    pd.DataFrame(odds_data).to_csv(output_file, index=False)
    _log(f"Odds APIFootball salvos em {output_file}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--source_csv", required=True)
    ap.add_argument("--api_key", required=True)
    ap.add_argument("--api_key_theodds", required=True)
    ap.add_argument("--regions", required=True)
    ap.add_argument("--aliases_file", required=True)
    args = ap.parse_args()

    ingest_odds_apifootball(args.rodada, args.source_csv, args.api_key, args.api_key_theodds, args.regions, args.aliases_file)

if __name__ == "__main__":
    main()