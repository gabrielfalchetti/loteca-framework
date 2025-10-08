# Fix Loteca – Normalização de nomes e reconstrução do consenso de odds

## O que isso resolve
- Corrige **matching de clubes** que vinham com grafias diferentes entre as fontes (ex.: "Atletico Mineiro" vs "Atlético-MG"; "Sport Recife" vs "Sport").
- Reconstrói o `odds_consensus.csv` a partir de `odds_theoddsapi.csv` e (opcional) `odds_apifootball.csv`.
- Resultado: o confronto **Atlético-MG x Sport** passa a constar no consenso; **América-MG x Vila Nova** seguirá ausente enquanto não houver odds na(s) fonte(s).

## Como usar no workflow (exemplo)
```bash
python scripts/normalize_odds_and_merge.py \
  --theodds data/out/odds_theoddsapi.csv \
  --apifoot data/out/odds_apifootball.csv \
  --out data/out/odds_consensus.csv
