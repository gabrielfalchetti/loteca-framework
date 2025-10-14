# scripts/_utils_norm.py
from __future__ import annotations
import json
import os
import re
from typing import Dict, Iterable, List, Optional, Tuple
from unidecode import unidecode
from difflib import get_close_matches

_ALIASES_PATHS = [
    "data/aliases/team_aliases.json",     # preferido
    "team_aliases.json",                  # fallback simples
]

# tokens comuns para normalizar variações BR/EN e sufixos
_REPLACERS: Dict[str, str] = {
    # pontuação e conectores comuns
    "&": " and ",
    "/": " ",
    "-": " ",
    " fc": " ",
    " afc": " ",
    " sc": " ",
    " ac": " ",
    " ec": " ",
    " ca ": " ",
    " cf ": " ",
    " de ": " ",
    " do ": " ",
    " da ": " ",
    " (pa)": " ",
    " (sp)": " ",
    " (rj)": " ",
    " (mg)": " ",
    " (pr)": " ",
    " (rs)": " ",
    # específicos Brasil
    " athletico pr": " athletico paranaense",
    " atletico pr": " athletico paranaense",
    " atletico go": " atletico goianiense",
    " botafogo sp": " botafogo sp",
    " botafogo-rp": " botafogo sp",
    " ferroviaria": " ferroviaria",
    " paysandu": " paysandu",
    " remo pa": " remo",
    " america mg": " america mineiro",
    " avai": " avai",
    " chapecoense": " chapecoense",
}

_WORD_RX = re.compile(r"[a-z0-9]+")

def _apply_replacers(s: str) -> str:
    out = s
    for k, v in _REPLACERS.items():
        out = out.replace(k, v)
    return out

def norm_name(name: str) -> str:
    """Normaliza nome de time: minúsculo, sem acentos, tokens reduzidos."""
    if not isinstance(name, str):
        return ""
    s = unidecode(name).lower().strip()
    s = _apply_replacers(s)
    tokens = _WORD_RX.findall(s)
    return " ".join(tokens)

def token_key(name: str) -> str:
    """Chave ainda mais agressiva (sem espaços) para facilitar matching."""
    return norm_name(name).replace(" ", "")

def load_json(path: str, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def dump_json(obj, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _load_alias_map() -> Dict[str, str]:
    """Carrega alias map (chave normalizada -> canônico)."""
    for p in _ALIASES_PATHS:
        if os.path.isfile(p):
            data = load_json(p, default={}) or {}
            # normaliza chaves e valores
            out = {}
            for k, v in data.items():
                out[token_key(k)] = norm_name(v)
            return out
    return {}

_ALIAS_MAP = _load_alias_map()

def apply_alias(name: str) -> str:
    key = token_key(name)
    return _ALIAS_MAP.get(key, norm_name(name))

def best_match(target: str, candidates: Iterable[str], cutoff: float = 0.84) -> Optional[str]:
    """
    Fuzzy match com stdlib (difflib). Retorna melhor candidato >= cutoff (0..1).
    """
    target_n = apply_alias(target)
    cand_norm = {c: apply_alias(c) for c in candidates}
    # criamos uma lista de strings normalizadas únicas
    norm_to_orig = {}
    norm_list: List[str] = []
    for orig, nn in cand_norm.items():
        if nn not in norm_to_orig:
            norm_to_orig[nn] = orig
            norm_list.append(nn)
    # get_close_matches usa ratio semelhante ao SequenceMatcher; threshold ~0..1
    matches = get_close_matches(target_n, norm_list, n=1, cutoff=cutoff)
    if not matches:
        return None
    return norm_to_orig[matches[0]]