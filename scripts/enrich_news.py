# -*- coding: utf-8 -*-
import argparse
import sys
import pandas as pd
from newsapi import NewsApiClient
from datetime import timedelta, datetime
from transformers import pipeline  # Para sentiment analysis

def _log(msg: str) -> None:
    print(f"[enrich_news] {msg}", flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features_in", required=True)
    ap.add_argument("--features_out", required=True)
    ap.add_argument("--newsapi_key", default=os.getenv("NEWSAPI_KEY"))
    args = ap.parse_args()

    if not args.newsapi_key:
        _log("NEWSAPI_KEY não definida")
        sys.exit(2)

    newsapi = NewsApiClient(api_key=args.newsapi_key)
    sentiment_pipeline = pipeline("sentiment-analysis")

    df = pd.read_parquet(args.features_in)
    if df.empty:
        _log("Arquivo de features vazio")
        sys.exit(2)

    # Adicionar columns para sentiment e lesões
    df['home_sentiment'] = 0.0
    df['away_sentiment'] = 0.0
    df['home_injuries'] = 0
    df['away_injuries'] = 0

    for idx, row in df.iterrows():
        home_team = row['home']
        away_team = row['away']
        match_date = row['date'] if 'date' in df.columns else datetime.now()

        # Buscar notícias para home team
        from_date = (match_date - timedelta(days=7)).strftime('%Y-%m-%d')
        to_date = match_date.strftime('%Y-%m-%d')
        home_news = newsapi.get_everything(q=home_team, from_param=from_date, to=to_date, language='en', sort_by='relevancy')
        home_articles = home_news['articles']
        home_sentiment = 0.0
        home_injuries = 0
        for article in home_articles[:5]:  # Limitar a 5 artigos para eficiência
            title = article['title']
            sentiment = sentiment_pipeline(title)[0]
            home_sentiment += sentiment['score'] if sentiment['label'] == 'POSITIVE' else -sentiment['score']
            if 'injury' in title.lower() or 'lesão' in title.lower():
                home_injuries += 1

        df.at[idx, 'home_sentiment'] = home_sentiment / max(len(home_articles), 1)
        df.at[idx, 'home_injuries'] = home_injuries

        # Buscar notícias para away team
        away_news = newsapi.get_everything(q=away_team, from_param=from_date, to=to_date, language='en', sort_by='relevancy')
        away_articles = away_news['articles']
        away_sentiment = 0.0
        away_injuries = 0
        for article in away_articles[:5]:
            title = article['title']
            sentiment = sentiment_pipeline(title)[0]
            away_sentiment += sentiment['score'] if sentiment['label'] == 'POSITIVE' else -sentiment['score']
            if 'injury' in title.lower() or 'lesão' in title.lower():
                away_injuries += 1

        df.at[idx, 'away_sentiment'] = away_sentiment / max(len(away_articles), 1)
        df.at[idx, 'away_injuries'] = away_injuries

    df.to_parquet(args.features_out, index=False)
    _log(f"Features enriquecidas com news salvas em {args.features_out}")

if __name__ == "__main__":
    main()