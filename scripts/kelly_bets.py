import pandas as pd
import sys
import os

def kelly_bets(probs_path, odds_source_path, bankroll, fraction, cap, top_n, round_to, out_path):
    # Carregar DataFrames com verificações
    if not os.path.exists(probs_path):
        raise FileNotFoundError(f"Arquivo {probs_path} não encontrado. Verifique etapas upstream.")
    probs_df = pd.read_csv(probs_path)
    print(f"Colunas em {probs_path}: {probs_df.columns.tolist()}")  # Debug: mostre isso no log do workflow
    
    if not os.path.exists(odds_source_path):
        raise FileNotFoundError(f"Arquivo {odds_source_path} não encontrado. Verifique geração de odds_consensus.csv.")
    odds_df = pd.read_csv(odds_source_path)
    if odds_df.empty:
        raise ValueError(f"{odds_source_path} está vazio. Sem dados de odds para processar.")
    print(f"Colunas em {odds_source_path}: {odds_df.columns.tolist()}")  # Debug: identifique colunas reais
    print(f"Primeiras linhas de odds:\n{odds_df.head()}")  # Debug: estrutura dos dados
    
    # Identificar coluna de ID comum (flexível para variações)
    possible_id_cols = ['match_id', 'id', 'fixture_id', 'event_id', 'game_id']
    probs_id_col = next((col for col in possible_id_cols if col in probs_df.columns), None)
    odds_id_col = next((col for col in possible_id_cols if col in odds_df.columns), None)
    
    if not probs_id_col:
        raise ValueError(f"Nenhuma coluna de ID encontrada em {probs_path}. Colunas disponíveis: {probs_df.columns.tolist()}")
    if not odds_id_col:
        raise ValueError(f"Nenhuma coluna de ID encontrada em {odds_source_path}. Colunas disponíveis: {odds_df.columns.tolist()}")
    
    if probs_id_col != odds_id_col:
        print(f"Aviso: Usando '{probs_id_col}' em probs e '{odds_id_col}' em odds. Renomeando para padronizar.")
        odds_df = odds_df.rename(columns={odds_id_col: 'match_id'})
        odds_id_col = 'match_id'
        probs_df = probs_df.rename(columns={probs_id_col: 'match_id'})
        probs_id_col = 'match_id'
    
    # Merge automático em vez de loop frágil (mais eficiente e seguro)
    merged_df = pd.merge(probs_df, odds_df, on='match_id', how='inner')
    if merged_df.empty:
        raise ValueError("Nenhuma correspondência entre probs e odds. Verifique IDs ou fontes de dados (ex.: Sportmonks fixtures).")
    
    print(f"Merge realizado: {len(merged_df)} partidas correspondentes.")  # Debug
    
    # Agora, processe o merged_df em vez de loop manual
    # Exemplo: Calcule Kelly para cada linha (ajuste conforme o resto do seu código)
    bets = []
    for _, row in merged_df.iterrows():
        match_id = row['match_id']
        # Assuma colunas como 'home_prob', 'away_prob', 'draw_prob' em probs; 'home_odds', etc. em odds
        # Calcule edge, kelly_fraction, etc. (implemente sua lógica aqui)
        edge = (row['home_prob'] * (row['home_odds'] - 1) - (1 - row['home_prob']))  # Exemplo simplificado
        kelly_frac = fraction * (edge / (row['home_odds'] - 1)) if edge > 0 else 0
        kelly_frac = min(kelly_frac, cap)  # Cap
        stake = round(bankroll * kelly_frac, round_to)
        if stake > 0:
            bets.append({
                'match_id': match_id,
                'stake': stake,
                # Adicione outras colunas...
            })
    
    # Salve top_n
    top_bets = sorted(bets, key=lambda x: x['stake'], reverse=True)[:top_n]
    pd.DataFrame(top_bets).to_csv(out_path, index=False)
    print(f"Bets salvas em {out_path}: {len(top_bets)} apostas.")