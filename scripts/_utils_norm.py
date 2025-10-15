# scripts/_utils_norm.py
from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from unidecode import unidecode
except Exception:  # pragma: no cover
    def unidecode(x: str) -> str:
        return x

# RapidFuzz é preferível; se não existir, caímos em difflib
try:
    from rapidfuzz import fuzz, process  # type: ignore
    _HAVE_RAPIDFUZZ = True
except Exception:
    import difflib
    _HAVE_RAPIDFUZZ = False


# ------------------------
# Utilidades de arquivo
# ------------------------
def load_json(path: str, default: Any = None) -> Any:
    """Carrega JSON seguro; retorna default se não existir/estiver vazio."""
    if default is None:
        default = {}
    try:
        if not os.path.exists(path) or os.path.isdir(path) or os.path.getsize(path) == 0:
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def dump_json(obj: Any, path: str) -> None:
    """Salva JSON (cria diretório se necessário)."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


# ------------------------
# Normalização e alias
# ------------------------
_BR_SUFFIXES = [
    r"\bFC\b", r"\bEC\b", r"\bAC\b", r"\bSC\b", r"\bAA\b",
    r"\bFutebol Clube\b", r"\bEsporte Clube\b", r"\bAtlético Clube\b",
    r"\bSaf\b", r"\bS\.A\.F\.\b",
]

_PARENS = re.compile(r"\s*\([^)]*\)")
_MULTI_WS = re.compile(r"\s+")

def _cleanup(text: str) -> str:
    # remove parênteses, pontuação e espaços extras
    text = _PARENS.sub("", text)
    text = re.sub(r"[.,;:!?'’]", " ", text)
    text = _MULTI_WS.sub(" ", text).strip()
    return text


def norm_name(name: str) -> str:
    """
    Normaliza nomes de times para comparação/matching.
    Ex.: "Botafogo-SP" -> "botafogo sp" ; "Paysandu/Remo (PA)" -> "paysandu remo pa"
    """
    if not isinstance(name, str):
        return ""
    s = name.strip()

    # uniformiza separadores: hífen/"/"
    s = s.replace("/", " ").replace("-", " ")

    # remove sufixos comuns do BR (FC, EC, etc.)
    for pat in _BR_SUFFIXES:
        s = re.sub(pat, " ", s, flags=re.IGNORECASE)

    s = _cleanup(s)

    # normaliza acentos e caixa
    s = unidecode(s).lower()

    # normalizações específicas úteis
    # ex.: “atletico pr” ~ “athletico pr”
    s = s.replace("athletico", "atletico")
    s = s.replace("botafogo sp", "botafogo sp")  # idempotente, mas deixa explícito
    s = s.replace("remo pa", "remo")             # alguns feeds trazem UF

    # garante tokens simples
    s = _MULTI_WS.sub(" ", s).strip()
    return s


def token_key(name: str) -> str:
    """Chave de tokens ordenados para ajudar matching determinístico."""
    norm = norm_name(name)
    if not norm:
        return ""
    toks = norm.split()
    toks.sort()
    return " ".join(toks)


def load_alias_maps() -> Dict[str, str]:
    """
    Carrega e mescla aliases:
      - data/aliases/team_aliases.json
      - data/aliases/auto_aliases.json
    Formato esperado: { "alias_normalizado": "canonical_name" }
    """
    manual = load_json("data/aliases/team_aliases.json", default={})
    auto = load_json("data/aliases/auto_aliases.json", default={})

    # normaliza chaves para garantir consistência
    out: Dict[str, str] = {}
    for src in (manual, auto):
        if isinstance(src, dict):
            for k, v in src.items():
                nk = norm_name(k)
                if nk:
                    out[nk] = v
    return out


def apply_alias(name: str, alias_map: Optional[Dict[str, str]] = None) -> str:
    """Aplica alias se houver, respeitando normalização da chave."""
    if alias_map is None:
        alias_map = load_alias_maps()
    nk = norm_name(name)
    return alias_map.get(nk, name)


# ------------------------
# Matching com score_cutoff
# ------------------------
def best_match(
    query: str,
    choices: Iterable[str],
    *,
    score_cutoff: Optional[int] = None,
    scorer: Optional[Any] = None
) -> Tuple[Optional[str], float]:
    """
    Retorna (melhor_candidato, score) para `query` em `choices`.
    Aceita `score_cutoff`: se score < cutoff -> (None, 0.0)

    - Usa RapidFuzz se disponível (padrão scorer: fuzz.WRatio).
    - Fallback: difflib SequenceMatcher (score 0–100).
    """
    qn = norm_name(query)
    norm_choices: List[str] = [norm_name(c) for c in choices]

    if not qn or not norm_choices:
        return (None, 0.0)

    if _HAVE_RAPIDFUZZ:
        _scorer = scorer or fuzz.WRatio
        result = process.extractOne(
            qn,
            norm_choices,
            scorer=_scorer,
            score_cutoff=score_cutoff if score_cutoff is not None else 0,
        )
        if not result:
            return (None, 0.0)
        # result = (match, score, index)
        match_str, score, idx = result
        # devolve o original correspondente ao índice para manter integridade
        original = list(choices)[idx]
        if score_cutoff is not None and score < score_cutoff:
            return (None, float(score))
        return (original, float(score))

    # --- fallback difflib ---
    best_s = -1.0
    best_c: Optional[str] = None
    for orig, nc in zip(choices, norm_choices):
        s = _ratio_difflib(qn, nc)
        if s > best_s:
            best_s = s
            best_c = orig
    if score_cutoff is not None and best_s < score_cutoff:
        return (None, float(best_s))
    return (best_c, float(best_s))


def _ratio_difflib(a: str, b: str) -> float:
    import difflib
    return difflib.SequenceMatcher(None, a, b).ratio() * 100.0