# -*- coding: utf-8 -*-
"""
Funções comuns de normalização de nomes e match_key.
"""

import re
import unicodedata

def _strip_accents(s: str) -> str:
    if s is None:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

def slugify_team(name: str) -> str:
    """Normaliza nome de time -> slug minúsculo (sem acentos, sem pontuação)."""
    s = _strip_accents(str(name).strip().lower())
    s = s.replace("&", "and")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s

def match_key_from_teams(home: str, away: str) -> str:
    """Gera chave canônica home__vs__away."""
    return f"{slugify_team(home)}__vs__{slugify_team(away)}"