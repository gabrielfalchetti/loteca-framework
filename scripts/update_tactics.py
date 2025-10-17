#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extrai táticas de relatórios textuais de partidas usando LLMs, gerando um JSON.
Integra dados do histórico (results.csv) para contextos.

Uso:
  python -m scripts.update_tactics --history data/history/results.csv --out data/history/tactics.json
"""

from __future__ import annotations

import argparse
import json
import os
import pandas as pd
from transformers import pipeline
from typing import Dict, Optional

def _log(msg: str) -> None:
    print(f"[update_tactics] {msg}", flush=True)

def update_tactics(history: str, out: str) -> None:
    """Extrai táticas usando LLM com base no histórico."""
    if not os.path.isfile(history):
        _log(f"{history} não encontrado")
        return

    try:
        df = pd.read_csv(history)
        nlp = pipeline("question-answering", model="distilbert-base-cased", device=0 if torch.cuda.is_available() else -1)
        tactics: Dict[str, Dict] = {}

        for _, row in df.iterrows():
            context = f"Match: {row['home']} vs {row['away']} on {row['date']}. Tactics used: [Insert report here]"
            # Placeholder: Substitua [Insert report here] por texto real (ex.: scraping ou input manual)
            answer = nlp(question="Qual a tática usada?", context=context)
            tactic_score = 0.5 if answer and answer["score"] > 0.5 else 0.0  # Score preliminar
            tactics[row["date"]] = {
                "team": row["home"],
                "tactic": answer["answer"] if answer else "unknown",
                "tactic_score": tactic_score
            }

        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(tactics, f, ensure_ascii=False, indent=2)
        _log(f"OK — gerado {out} com {len(tactics)} táticas")

    except Exception as e:
        _log(f"[CRITICAL] Erro: {e}")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump({}, f)  # Stub vazio

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", type=str, required=True, help="Caminho do CSV de histórico")
    parser.add_argument("--out", type=str, required=True, help="Caminho do JSON de táticas")
    args = parser.parse_args()
    update_tactics(args.history, args.out)

if __name__ == "__main__":
    import torch  # Necessário para device
    main()