import pandas as pd
import unidecode
import os
import argparse

def normalize_team_name(name):
    """Normaliza nome do time, removendo estado e acentos."""
    if not name:
        return ""
    if '/' in name:
        name = name.split('/')[0].strip()
    name = unidecode.unidecode(name.lower())
    return name.capitalize()

def main():
    parser = argparse.ArgumentParser(description='Normaliza arquivo de matches.')
    parser.add_argument('--source_csv', required=True, help='Caminho para matches_source.csv')
    parser.add_argument('--out_dir', required=True, help='Diretório de saída')
    parser.add_argument('--season', required=True, help='Temporada (ex.: 2025)')
    
    args = parser.parse_args()
    
    # Ler o CSV fonte
    if not os.path.exists(args.source_csv):
        raise FileNotFoundError(f"Arquivo {args.source_csv} não encontrado")
    df = pd.read_csv(args.source_csv)
    
    # Assumindo colunas no source_csv: ex. 'id', 'home_team', 'away_team', 'match_date'
    # Renomear e normalizar
    if 'id' in df.columns:
        df = df.rename(columns={'id': 'match_id'})
    else:
        # Se não tiver match_id, gerar um simples
        df['match_id'] = range(1, len(df) + 1)
    
    if 'home_team' in df.columns:
        df = df.rename(columns={'home_team': 'home'})
    if 'away_team' in df.columns:
        df = df.rename(columns={'away_team': 'away'})
    if 'match_date' in df.columns:
        df = df.rename(columns={'match_date': 'date'})
    
    # Normalizar nomes de times
    df['home'] = df['home'].apply(normalize_team_name)
    df['away'] = df['away'].apply(normalize_team_name)
    
    # Selecionar apenas colunas necessárias
    df = df[['match_id', 'home', 'away', 'date']]
    
    # Salvar no out_dir
    output_path = os.path.join(args.out_dir, 'matches_norm.csv')
    os.makedirs(args.out_dir, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"[normalize_matches] Arquivo normalizado salvo em {output_path}")

if __name__ == "__main__":
    main()