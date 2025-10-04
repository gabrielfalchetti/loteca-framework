# scripts/text_normalizer.py
"""
Módulo utilitário para normalizar nomes de times e gerar match_key.
Usado pelos ingesters e pelo consenso.
"""

import unicodedata
import re
import json
from unidecode import unidecode

def normalize_text(s: str) -> str:
    """Normaliza string: minúsculo, sem acentos, sem caracteres especiais."""
    if not isinstance(s, str):
        return ""
    s = s.lower().strip()
    s = unidecode(s)  # remove acentos
    s = re.sub(r"[^a-z0-9 ]", "", s)  # mantém letras/números/espaço
    s = re.sub(r"\s+", " ", s)  # normaliza espaços
    return s.strip()

def make_match_key(home: str, away: str) -> str:
    """Cria chave única para o jogo: home__vs__away (normalizados)."""
    return f"{normalize_text(home)}__vs__{normalize_text(away)}"

def equals_team(a: str, b: str) -> bool:
    """Compara nomes de times de forma normalizada."""
    return normalize_text(a) == normalize_text(b)

def load_aliases(path: str) -> dict:
    """Carrega dicionário de aliases de times a partir de um JSON."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}