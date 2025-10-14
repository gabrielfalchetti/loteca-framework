# scripts/_utils_norm.py
# utilitários de normalização e matching de nomes de times (PT/EN)

from __future__ import annotations
import re
import json
import unicodedata
from typing import Iterable, Tuple, Dict, Any, Optional
from unidecode import unidecode
from difflib import SequenceMatcher

# tokens “ruído” que não ajudam no match
STOP_TOKENS = {
    "fc","acf","aec","ac","sc","ec","afc","club","clube","athletic","athletico",
    "de","da","do","dos","as","esporte","sport","futebol","associacao","associação",
    "football","f.c.","a.f.c.","s.c.","e.c.","cf","c.f."
}

# mapeia sufixos/UFs e anotações comuns para remover
UF_PAT = re.compile(r"\b\(?[a-z]{2}\)?\b", re.IGNORECASE)  # (SP), -PR etc (depois de higienizar)
PARENS_PAT = re.compile(r"\([^)]*\)")
PUNCT_PAT = re.compile(r"[^\w\s]+")

REPLACERS = [
    (r"atl[. ]*goianiense", "atletico goianiense"),
    (r"atl[. ]*mineiro", "atletico mineiro"),
    (r"athletico", "atletico"),
    (r"botafogo[- ]*sp|botafogo ribeirao", "botafogo sp"),
    (r"rem o|remo \(pa\)", "remo"),
    (r"paysandu/remo|paysandu vs remo", "paysandu remo"),
    (r"chapecoense.*", "chapecoense"),
]

def basic_clean(s: str) -> str:
    if not s:
        return ""
    s = unidecode(s).lower().strip()
    s = PARENS_PAT.sub(" ", s)
    s = s.replace("-", " ").replace("/", " ")
    s = PUNCT_PAT.sub(" ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def strip_uf(s: str) -> str:
    # remove UF solto: “sp”, “pr”, etc, quando sozinho como token final
    toks = s.split()
    if toks and len(toks[-1]) == 2 and toks[-1].isalpha():
        toks = toks[:-1]
    return " ".join(toks)

def apply_replacers(s: str) -> str:
    out = s
    for pat, rep in REPLACERS:
        out = re.sub(pat, rep, out)
    return out

def norm_name(s: str) -> str:
    """
    Normalização agressiva: remove acentos, pontuação, UF, conectores irrelevantes.
    """
    s = basic_clean(s)
    s = apply_replacers(s)
    s = strip_uf(s)
    # remove tokens de pouco valor
    toks = [t for t in s.split() if t not in STOP_TOKENS]
    return " ".join(toks).strip()

def token_key(s: str) -> str:
    """chave de comparação: tokens únicos ordenados"""
    toks = sorted(set(norm_name(s).split()))
    return " ".join(toks)

def token_jaccard(a: str, b: str) -> float:
    ta, tb = set(norm_name(a).split()), set(norm_name(b).split())
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    union = len(ta | tb)
    return inter / union if union else 0.0

def seq_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, token_key(a), token_key(b)).ratio()

def score_names(a: str, b: str) -> float:
    """
    score final [0..1] combinando jaccard + sequence ratio.
    Dá mais peso a Jaccard (tokens corrigem inversões).
    """
    j = token_jaccard(a, b)
    r = seq_ratio(a, b)
    return 0.65 * j + 0.35 * r

def best_match(observed: str, candidates: Iterable[Tuple[str,int]], min_score: float = 0.88) -> Tuple[Optional[int], float, Optional[str]]:
    """
    candidates: iterável de (nome_canon, team_id)
    retorna (team_id, score, nome_canon) com melhor score acima do limiar
    """
    best = (None, 0.0, None)
    for canon, tid in candidates:
        sc = score_names(observed, canon)
        if sc > best[1]:
            best = (tid, sc, canon)
    if best[1] >= min_score:
        return best
    return (None, best[1], best[2])

def load_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def dump_json(path: str, obj: Dict[str, Any]) -> None:
    import os
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)