# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
import os
from newsapi import NewsApiClient
from datetime import timedelta, datetime
from transformers import pipeline

def _log(msg: str) -> None:
    print(f"[enrich_news] {msg}", flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features_in", required=True)
    ap.add_argument("--features_out", required=True)
    ap.add_argument("--newsapi_key", default=os.getenv("NEWSAPI_KEY"))
    args = ap.parse_args()

    if not args.newsapi_key:
        _log("NEWSAPI_KEY não definida, pulando enriquecimento de notícias")
        return

    newsapi = NewsApiClient(api_key=args.newsapi_key)
    sentiment_pipeline = pipeline("sentiment-analysis")

    df = pd.read_parquet(args.features_in)
    if df.empty:
        _log("Arquivo de features vazio")
        sys.exit(2)

    # Logar colunas disponíveis
    _log(f"Colunas disponíveis no DataFrame: {list(df.columns)}")

    # Verificar colunas disponíveis
    team_col = 'team_home' if 'team_home' in df.columns else 'team' if 'team' in df.columns else None
    opponent_col = 'team_away' if 'team_away' in df.columns else None
    if team_col is None:
        _log(f"Coluna 'team_home' ou 'team' não encontrada no DataFrame. Colunas disponíveis: {list(df.columns)}")
        sys.exit(2)

    # Adicionar colunas para sentiment e lesões
    df['sentiment'] = 0.0
    df['injuries'] = 0

    for idx, row in df.iterrows():
        team = row[team_col]
        match_date = datetime.strptime(row['date'], '%Y-%m-%d') if 'date' in df.columns else datetime.now()

        # Buscar notícias para o time
        from_date = (match_date - timedelta(days=7)).strftime('%Y-%m-%d')
        to_date = match_date.strftime('%Y-%m-%d')
        try:
            news = newsapi.get_everything(q=team, from_param=from_date, to=to_date, language='en', sort_by='relevancy')
            articles = news['articles']
            sentiment = 0.0
            injuries = 0
            for article in articles[:5]:  # Limitar a 5 artigos para eficiência
                title = article['title']
                sent = sentiment_pipeline(title)[0]
                sentiment += sent['score'] if sent['label'] == 'POSITIVE' else -sent['score']
                if 'injury' in title.lower() or 'lesão' in title.lower():
                    injuries += 1
            df.at[idx, 'sentiment'] = sentiment / max(len(articles), 1)
            df.at[idx, 'injuries'] = injuries
        except Exception as e:
            _log(f"Erro ao buscar notícias para {team}: {e}")

    df.to_parquet(args.features_out, index=False)
    _log(f"Features enriquecidas com notícias salvas em {args.features_out}")

if __name__ == "__main__":
    main()