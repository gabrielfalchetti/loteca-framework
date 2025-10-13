#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_odds_apifootball: Busca jogos e odds da API-Football.

CORREÇÃO FINAL: Ajusta o intervalo de datas para começar a busca
a partir do dia anterior (D-1) para compensar diferenças de fuso horário (UTC)
e garantir que os jogos do dia atual sejam sempre capturados.
"""

import os
import sys
import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

def log(level, msg):
    tag = "" if level == "INFO" else f"[{level}] "
    print(f"[apifootball]{tag}{msg}", flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--aliases", default="")
    args = ap.parse_args()

    API_KEY = os.environ.get("API_FOOTBALL_KEY")
    lookahead_days = int(os.environ.get("LOOKAHEAD_DAYS", 3))

    if not API_KEY:
        log("CRITICAL", "Variável de ambiente API_FOOTBALL_KEY não definida.")
        sys.exit(5)

    headers = {'x-apisports-key': API_KEY}
    url = "https://v3.football.api-sports.io/fixtures"
    
    # CORREÇÃO DE DATA: Começa a busca do dia anterior para criar uma janela segura de fuso horário.
    date_from = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    date_to = (datetime.utcnow() + timedelta(days=lookahead_days)).strftime('%Y-%m-%d')
    
    params = {
        "from": date_from,
        "to": date_to,
        "status": "NS" # Apenas jogos não iniciados (Not Started)
    }

    log("INFO", f"Buscando jogos de {date_from} até {date_to}...")

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        log("ERROR", f"Falha na requisição à API-Football: {e}")
        # Cria um arquivo vazio para não quebrar o pipeline em caso de falha da API
        pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away']).to_csv(os.path.join(args.rodada, "odds_apifootball.csv"), index=False)
        return 0

    fixtures = data.get('response', [])
    if not fixtures:
        log("WARN", "Nenhum jogo encontrado na API-Football para o período.")
        pd.DataFrame(columns=['match_id', 'home', 'away', 'odds_home', 'odds_draw', 'odds_away']).to_csv(os.path.join(args.rodada, "odds_apifootball.csv"), index=False)
        return 0

    log("INFO", f"{len(fixtures)} jogos encontrados. Filtrando e buscando odds...")

    rows = []
    odds_url = "https://v3.football.api-sports.io/odds"
    
    for fixture in fixtures:
        try:
            # Filtra apenas jogos que ainda não começaram, comparando timestamps
            fixture_timestamp = fixture['fixture']['timestamp']
            if fixture_timestamp < datetime
