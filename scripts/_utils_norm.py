# -*- coding: utf-8 -*-
from unidecode import unidecode
import re

_BR_ALIASES = {
    # times brasileiros com variações usuais
    "athletico-pr": ["athletico pr","athletico paranaense","atletico-pr","atletico paranaense"],
    "avai": ["avaí","avai fc","avai-sc","avai sc"],
    "botafogo-sp": ["botafogo sp","botafogo ribeirao","botafogo ribeirão","botafogo de ribeirao preto","botafogo de ribeirão preto"],
    "chapecoense": ["chapecoense-sc","chapecoense sc","chapeco"],
    "crb": ["crb-al","crb al"],
    "ferroviaria": ["ferroviária","a. ferroviaria","a ferroviaria","ferroviaria-sp","ferroviaria sp"],
    "paysandu": ["paysandu sc","paysandu (pa)","paysandu-pa","paysandu pa"],
    "remo": ["remo (pa)","remo-pa","remo pa","clube do remo"],
    "volta redonda": ["volta redonda-rj","volta redonda rj","volta redonda fc"],
    "atletico-go": ["atlético-go","atletico goianiense","atlético goianiense","atletico-goianiense"],
}

def _canon_br(token: str) -> str:
    t = token
    for canon, variants in _BR_ALIASES.items():
        if t == canon or t in variants:
            return canon
    return token

def norm_name(s: str) -> str:
    """
    Normaliza nomes PT/EN -> forma canônica (ascii, minusc, sem pontuação)
    e aplica aliases BR conhecidos.
    """
    if s is None:
        return ""
    t = unidecode(s).lower().strip()
    t = re.sub(r"[\.\(\)\[\]\{\}]", " ", t)
    t = re.sub(r"[\/\-–—_]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    t = _canon_br(t)
    return t

def soft_eq(a: str, b: str) -> bool:
    """Igualdade fraca após normalização completa."""
    return norm_name(a) == norm_name(b)