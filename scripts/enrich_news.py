# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
from newsapi import NewsApiClient
from datetime import timedelta, datetime
from transformers import pipeline
import time

def _log(msg: str) -> None:
    print(f"[enrich_news] {msg}", flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features_in", required=True)
    ap.add_argument("--features_out", required=True)
    ap.add_argument("--newsapi_key", default=os.getenv("NEWSAPI_KEY"))
    ap.add_argument("--matches_csv", required=True)
    args = ap.parse_args()

    if not args.newsapi_key:
        _log("NEWSAPI_KEY não definida, pulando enriquecimento de notícias")
        return

    newsapi = NewsApiClient(api_key=args.newsapi_key)
    sentiment_pipeline = pipeline("sentiment-analysis", model="distilbert/distilbert-base-uncased-finetuned-sst-2-english", device=-1)  # CPU

    df = pd.read_parquet(args.features_in)
    if df.empty:
        _log("Arquivo de features vazio")
        sys.exit(2)

    _log(f"Colunas disponíveis no DataFrame: {list(df.columns)}")

    # Verificar coluna 'team'
    if 'team' not in df.columns:
        _log(f"Coluna 'team' não encontrada no DataFrame. Colunas disponíveis: {list(df.columns)}")
        sys.exit(2)

    # Carregar matches_norm.csv para filtrar times relevantes
    if not os.path.isfile(args.matches_csv):
        _log(f"Arquivo {args.matches_csv} não encontrado, abortando")
        sys.exit(2)

    try:
        matches = pd.read_csv(args.matches_csv)
    except Exception as e:
        _log(f"Erro ao ler {args.matches_csv}: {str(e)}")
        sys.exit(2)

    home_col = next((col for col in ['team_home', 'home'] if col in matches.columns), None)
    away_col = next((col for col in ['team_away', 'away'] if col in matches.columns), None)
    if not (home_col and away_col):
        _log("Colunas team_home/team_away ou home/away não encontradas em matches_norm.csv")
        sys.exit(2)

    teams = set(matches[home_col]).union(set(matches[away_col]))
    _log(f"Filtrando para {len(teams)} times dos jogos atuais: {teams}")

    # Adicionar colunas para sentiment e lesões
    df['sentiment'] = 0.0
    df['injuries'] = 0

    for team in teams:
        match_date = datetime.now()  # Default para hoje se 'date' não estiver disponível
        if 'date' in df.columns:
            team_row = df[df['team'] == team]
            if not team_row.empty and pd.notnull(team_row['date'].iloc[0]):
                match_date = datetime.strptime(team_row['date'].iloc[0], '%Y-%m-%d')

        # Buscar notícias para o time
        from_date = (match_date - timedelta(days=7)).strftime('%Y-%m-%d')
        to_date = match_date.strftime('%Y-%m-%d')
        try:
            news = newsapi.get_everything(q=team, from_param=from_date, to=to_date, language='pt', sort_by='relevancy')
            if news['status'] != 'ok':
                _log(f"Erro na API para {team}: {news.get('message', 'Unknown error')}")
                continue
            articles = news.get('articles', [])
            sentiment = 0.0
            injuries = 0
            for article in articles[:5]:  # Limitar a 5 artigos para eficiência
                title = article.get('title', '')
                if not title:
                    continue
                sent = sentiment_pipeline(title)[0]
                sentiment += sent['score'] if sent['label'] == 'POSITIVE' else -sent['score']
                if 'injury' in title.lower() or 'lesão' in title.lower():
                    injuries += 1
            sentiment = sentiment / max(len(articles), 1)
            df.loc[df['team'] == team, 'sentiment'] = sentiment
            df.loc[df['team'] == team, 'injuries'] = injuries
            _log(f"Sucesso para {team}: sentiment={sentiment:.3f}, injuries={injuries}")
            time.sleep(0.5)  # Delay para evitar rate limit
        except Exception as e:
            _log(f"Erro ao buscar notícias para {team}: {str(e)}")
            if 'rateLimited' in str(e):
                _log("Rate limit atingido. Considere plano pago da NewsAPI ou esperar 12 horas.")
            time.sleep(1)  # Delay maior após erro

    df.to_parquet(args.features_out, index=False)
    _log(f"Features enriquecidas com notícias salvas em {args.features_out}")

if __name__ == "__main__":
    main()