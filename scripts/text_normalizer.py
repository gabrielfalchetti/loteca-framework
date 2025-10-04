# -*- coding: utf-8 -*-
"""
text_normalizer.py
------------------
Normalizador e reconciliador de nomes de times para o framework da Loteca.

Objetivos:
- Remover acentos, padronizar caixa, espaços e pontuação;
- Aplicar aliases canônicos a partir de data/aliases_br.json;
- Gerar match_key consistente "time_home__vs__time_away";
- Comparar nomes com tolerância (fuzzy) usando rapidfuzz.

Dependências:
- rapidfuzz (já está no requirements.txt)
- pandas (apenas se você optar por helpers que tocam DataFrames, ver fim do arquivo)

Uso típico:
from scripts.text_normalizer import (
    load_aliases, canonicalize_team, make_match_key, equals_team
)

aliases = load_aliases("data/aliases_br.json")
home = canonicalize_team("Botafogo Ribeirão Preto", aliases)
away = canonicalize_team("Coritiba FC", aliases)
key  = make_match_key(home, away)

print(home, away, key)
# -> "Botafogo-SP" "Coritiba" "botafogo-sp__vs__coritiba"
"""

from __future__ import annotations
import json
import re
import unicodedata
from pathlib import Path
from typing import Dict, Optional, Tuple

try:
    # rapidfuzz é leve e já está no requirements
    from rapidfuzz import fuzz
except Exception:  # pragma: no cover
    fuzz = None  # fallback: desabilita equals_team se não disponível


# -----------------------------
# Normalização de strings
# -----------------------------

_PUNCT_PATTERN = re.compile(r"[^\w\s-]", flags=re.UNICODE)
_MULTI_SPACE = re.compile(r"\s+")


def strip_accents(text: str) -> str:
    """
    Remove acentos/diacríticos, preservando letras básicas ASCII.
    """
    if text is None:
        return ""
    nfkd = unicodedata.normalize("NFKD", str(text))
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))


def normalize_str(text: str) -> str:
    """
    Normaliza texto para comparação:
    - lowercase
    - remove acentos
    - remove pontuação, preserva hífen
    - colapsa espaços
    - strip
    """
    if text is None:
        return ""
    t = strip_accents(text).lower()
    t = _PUNCT_PATTERN.sub(" ", t)   # remove pontuação
    t = _MULTI_SPACE.sub(" ", t)     # colapsa espaços
    t = t.strip()
    return t


def slugify_team(team: str) -> str:
    """
    Slug simples para compor match_key (lowercase, sem acento, espaços->espaço, mantem hífen).
    Ex.: "São Paulo" -> "sao paulo"; "Botafogo-SP" -> "botafogo-sp"
    """
    t = normalize_str(team)
    # mantemos hífens; só transformamos espaços múltiplos em espaço simples
    return t


# -----------------------------
# Aliases (canônicos)
# -----------------------------

def load_aliases(path: str | Path) -> Dict[str, str]:
    """
    Carrega aliases do JSON. As chaves do dicionário serão normalizadas.
    O valor (nome canônico) é mantido como está (case/pontuação/acento),
    pois deve bater com o que você usa no matches_source.csv.
    """
    path = Path(path)
    if not path.exists():
        # Sem aliases? retornamos dict vazio, o restante continua funcional.
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    out = {}
    for k, v in data.items():
        out[normalize_str(k)] = v
    return out


# Mapa leve de tokens descartáveis comuns em nomes de clubes
_STOP_TOKENS = {
    "ec", "fc", "sc", "ac", "afc", "clube", "esporte", "regatas", "do", "da", "de", "futebol", "associacao",
    "associação", "sociedade", "athletico", "atletico", "clube", "sport", "clube", "esportivo"
}


def _soft_simplify(team_norm: str) -> str:
    """
    Remove alguns tokens 'fracos' para melhorar o match por alias.
    Ex.: "clube de regatas brasil" -> "brasil" (mas aliases já mapeiam CRB).
    """
    tokens = [tok for tok in team_norm.split() if tok not in _STOP_TOKENS]
    return " ".join(tokens).strip() or team_norm


def canonicalize_team(team: str, aliases: Optional[Dict[str, str]] = None) -> str:
    """
    Retorna o nome canônico do time.
    Estratégia:
      1) Normaliza chave e tenta lookup direto em aliases;
      2) Se não encontrar, faz uma versão 'soft' e tenta de novo;
      3) Se ainda não encontrar, retorna o original (com trimming).
    """
    team = (team or "").strip()
    if not team:
        return ""

    if not aliases:
        return team  # sem aliases, devolve o que veio

    key = normalize_str(team)
    if key in aliases:
        return aliases[key]

    key_soft = _soft_simplify(key)
    if key_soft in aliases:
        return aliases[key_soft]

    # fallback: sem mapeamento; devolve original
    return team


# -----------------------------
# match_key
# -----------------------------

def make_match_key(home_team: str, away_team: str, aliases: Optional[Dict[str, str]] = None) -> str:
    """
    Gera uma match_key estável a partir dos nomes (preferencialmente já canônicos).
    - Aplica canonicalize_team antes, se aliases forem fornecidos.
    - Usa slugify_team() e concatena com "__vs__"
    """
    if aliases:
        home_team = canonicalize_team(home_team, aliases)
        away_team = canonicalize_team(away_team, aliases)

    home_slug = slugify_team(home_team)
    away_slug = slugify_team(away_team)
    return f"{home_slug}__vs__{away_slug}"


# -----------------------------
# Comparação tolerante (fuzzy)
# -----------------------------

def equals_team(a: str, b: str, aliases: Optional[Dict[str, str]] = None, threshold: int = 90) -> bool:
    """
    Compara dois nomes de time aceitando variações usando:
      - alias exato (se disponível)
      - slug normalizado
      - fuzzy ratio da rapidfuzz com threshold (default 90)

    Retorna True se considerar o par equivalente.
    """
    if not a and not b:
        return True
    if not a or not b:
        return False

    if aliases:
        ca = canonicalize_team(a, aliases)
        cb = canonicalize_team(b, aliases)
        if ca == cb:
            return True
        # também compara slugs dos canônicos
        if slugify_team(ca) == slugify_team(cb):
            return True

    # sem aliases: tenta normalização direta
    na = slugify_team(a)
    nb = slugify_team(b)
    if na == nb:
        return True

    if fuzz is None:
        return False

    score = fuzz.ratio(na, nb)
    return score >= threshold


# -----------------------------
# Helpers opcionais (DataFrame)
# -----------------------------

def df_apply_canon(df, home_col: str = "team_home", away_col: str = "team_away",
                   out_home_col: Optional[str] = None, out_away_col: Optional[str] = None,
                   aliases: Optional[Dict[str, str]] = None):
    """
    Se você quiser padronizar colunas de um DataFrame rapidamente.
    (Evita import circular: import feito dentro.)
    """
    import pandas as pd  # import local, só quando necessário
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df_apply_canon: df precisa ser um pandas.DataFrame")

    out_home_col = out_home_col or home_col
    out_away_col = out_away_col or away_col

    df[out_home_col] = df[home_col].astype(str).map(lambda x: canonicalize_team(x, aliases))
    df[out_away_col] = df[away_col].astype(str).map(lambda x: canonicalize_team(x, aliases))
    return df


def df_make_match_key(df, home_col: str = "team_home", away_col: str = "team_away",
                      out_col: str = "match_key", aliases: Optional[Dict[str, str]] = None):
    """
    Cria/atualiza a coluna match_key em um DataFrame.
    """
    import pandas as pd
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df_make_match_key: df precisa ser um pandas.DataFrame")

    df[out_col] = [
        make_match_key(h, a, aliases=aliases)
        for h, a in zip(df[home_col].astype(str), df[away_col].astype(str))
    ]
    return df


# -----------------------------
# CLI simples (para teste manual)
# -----------------------------

if __name__ == "__main__":  # pragma: no cover
    import argparse

    p = argparse.ArgumentParser(description="Teste rápido do normalizador")
    p.add_argument("--aliases", default="data/aliases_br.json", help="Caminho para o JSON de aliases")
    p.add_argument("--home", required=True, help="Nome do mandante (qualquer variação)")
    p.add_argument("--away", required=True, help="Nome do visitante (qualquer variação)")
    args = p.parse_args()

    aliases = load_aliases(args.aliases)
    home_c = canonicalize_team(args.home, aliases)
    away_c = canonicalize_team(args.away, aliases)
    key = make_match_key(home_c, away_c)  # aqui já são canônicos

    print(f"home_in : {args.home}")
    print(f"away_in : {args.away}")
    print(f"home_can: {home_c}")
    print(f"away_can: {away_c}")
    print(f"match_key: {key}")