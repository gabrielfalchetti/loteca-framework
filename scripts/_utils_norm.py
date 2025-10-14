# scripts/_utils_norm.py
from __future__ import annotations
import json
import os
import re
from typing import Dict, Iterable, List, Optional
from unidecode import unidecode
from difflib import get_close_matches

# Procuraremos aliases em múltiplas fontes (manual e gerado automaticamente)
_ALIAS_FILES = [
    "data/aliases/auto_aliases.json",     # gerado pelo harvester (automático)
    "data/aliases/team_aliases.json",     # curadoria manual (opcional)
    "team_aliases.json",                  # fallback simples
]

# substituições leves para normalizar
_REPLACERS: Dict[str, str] = {
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
}

_WORD_RX = re.compile(r"[a-z0-9]+")

def _apply_replacers(s: str) -> str:
    out = s
    for k, v in _REPLACERS.items():
        out = out.replace(k, v)
    return out

def norm_name(name: str) -> str:
    """Normaliza nome: sem acento, minúsculo, somente [a-z0-9] separados por espaço."""
    if not isinstance(name, str):
        return ""
    s = unidecode(name).lower().strip()
    s = _apply_replacers(s)
    tokens = _WORD_RX.findall(s)
    return " ".join(tokens)

def token_key(name: str) -> str:
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

def _merge_alias_sources() -> Dict[str, str]:
    """
    Lê todos os arquivos de alias e mescla (o primeiro da lista tem prioridade).
    Formato esperado: {"grafia_da_fonte": "Nome Canonico"}
    No dicionário final, a CHAVE é sempre token_key(grafia_da_fonte),
    e o VALOR é norm_name(Nome Canonico).
    """
    merged: Dict[str, str] = {}
    for p in _ALIAS_FILES:
        data = load_json(p, default=None)
        if not isinstance(data, dict):
            continue
        # entradas anteriores NÃO são sobrepostas pelos posteriores (prioridade por ordem em _ALIAS_FILES)
        for raw, canon in data.items():
            k = token_key(str(raw))
            v = norm_name(str(canon))
            if k not in merged:
                merged[k] = v
    return merged

_ALIAS_MAP = _merge_alias_sources()

def apply_alias(name: str) -> str:
    """Aplica mapeamento se existir; senão, retorna nome normalizado."""
    key = token_key(name)
    return _ALIAS_MAP.get(key, norm_name(name))

def best_match(target: str, candidates: Iterable[str], cutoff: float = 0.84) -> Optional[str]:
    """
    Fuzzy match entre strings (padrão: difflib) com nomes já normalizados/aliased.
    Retorna o candidato ORIGINAL (não-normalizado) mais próximo.
    """
    target_n = apply_alias(target)
    cand_norm = {c: apply_alias(c) for c in candidates}
    # lista única de normalizados
    norm_to_orig = {}
    norm_list: List[str] = []
    for orig, nn in cand_norm.items():
        if nn not in norm_to_orig:
            norm_to_orig[nn] = orig
            norm_list.append(nn)
    matches = get_close_matches(target_n, norm_list, n=1, cutoff=cutoff)
    if not matches:
        return None
    return norm_to_orig[matches[0]]