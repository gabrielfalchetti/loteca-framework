# _utils_norm.py
# Utilidades compartilhadas para normalização de nomes de times / matching de aliases

from __future__ import annotations
import json
import re
from typing import Iterable, Tuple, Optional, Any, Dict

from unidecode import unidecode
try:
    # rapidfuzz é rápido e robusto; se não estiver disponível, caímos para uma no-op simples
    from rapidfuzz import fuzz, process
    _HAS_RF = True
except Exception:  # pragma: no cover
    _HAS_RF = False


# Palavras pouco informativas que podem ser descartadas ao gerar a "chave" de tokens
_STOPWORDS = {
    "fc", "sc", "ec", "ac", "afc", "cf", "clube", "club", "futebol", "football",
    "u23", "sub23", "u20", "b"
}

_ws = re.compile(r"\s+")

def _clean_ascii(s: str) -> str:
    s = unidecode(str(s)).lower()
    # troca qualquer coisa não alfanumérica por espaço
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = _ws.sub(" ", s).strip()
    return s

def token_key(name: str) -> str:
    """
    Reduz o nome a uma chave canônica baseada em tokens (ascii, minúsculo, sem stopwords).
    Ex.: "Atlético-GO" -> "atletico go"; "Botafogo-SP" -> "botafogo sp"
    """
    base = _clean_ascii(name)
    tokens = [t for t in base.split() if t and t not in _STOPWORDS]
    return " ".join(tokens)


# Mapas de aliases -> canônico (já em forma token_key)
# Atenção: a chave e o valor são *sem acento* e *minúsculos*
_ALIAS_CANON: Dict[str, str] = {
    # BR times/state hints
    "athletico pr": "athletico-pr",
    "athletico paranaense": "athletico-pr",
    "atletico pr": "athletico-pr",
    "atletico paranaense": "athletico-pr",
    "atletico pr paranaense": "athletico-pr",
    "atletico-pr": "athletico-pr",

    "botafogo sp": "botafogo-sp",
    "botafogo sao paulo": "botafogo-sp",
    "botafogo-sp": "botafogo-sp",

    "ferroviaria": "ferroviaria",
    "ferroviaria sp": "ferroviaria",

    "paysandu": "paysandu",
    "paysandu pa": "paysandu",

    "remo": "remo",
    "remo pa": "remo",

    "avai": "avai",
    "avai sc": "avai",

    "chapecoense": "chapecoense",
    "chapecoense sc": "chapecoense",

    "crb": "crb",
    "crb al": "crb",

    "volta redonda": "volta redonda",
    "volta redonda rj": "volta redonda",

    "atletico go": "atletico-go",
    "atletico goianiense": "atletico-go",
    "atletico goiania": "atletico-go",
    "atletico-go": "atletico-go",

    # Formas comuns sem hífen que queremos padronizar com hífen
    "botafogo sp fc": "botafogo-sp",
    "athletico pr fc": "athletico-pr",
}

# Para facilitar, também aceitamos as chaves com hífen transformadas em token_key
for v in list(_ALIAS_CANON.values()):
    _ALIAS_CANON.setdefault(token_key(v), v)


def _postformat(canon: str) -> str:
    """
    Aplica formatação final amigável (hífens onde esperamos).
    Entradas e saídas são SEM acentos.
    """
    # já armazenamos os canônicos no formato desejado
    return canon


def norm_name(name: str) -> str:
    """
    Normaliza o nome do time para um canônico estável.
    - remove acentos, caixa, pontuação
    - aplica mapa de aliases
    - devolve no formato final (ex.: 'athletico-pr', 'botafogo-sp', 'atletico-go')
    """
    if not name:
        return ""
    key = token_key(name)
    canon = _ALIAS_CANON.get(key, key)
    return _postformat(canon)


def best_match(query: str, population: Iterable[str], score_cutoff: int = 86) -> Tuple[Optional[str], int]:
    """
    Faz o melhor matching de 'query' contra uma lista de candidatos usando os nomes NORMALIZADOS.
    Retorna (candidato_original, score). Se nada atingir score_cutoff, retorna (None, 0).
    """
    qn = norm_name(query)
    pop = list(population)  # manter referência aos originais
    if not pop:
        return None, 0

    # Primeiro, tente igualdades exatas após normalização
    canon_map = {}
    for p in pop:
        pn = norm_name(p)
        canon_map.setdefault(pn, []).append(p)

    if qn in canon_map:
        # empate? devolve o mais curto/limpo
        choices = sorted(canon_map[qn], key=lambda s: (len(s), s))
        return choices[0], 100

    # Fallback: fuzzy
    if _HAS_RF:
        # Avaliamos sobre as *formas normalizadas*, mas preservamos o original
        norm2orig = {norm_name(p): p for p in pop}
        choices_norm = list(norm2orig.keys())
        # token_sort_ratio é estável para nomes com ordem similar
        res = process.extractOne(
            qn, choices_norm,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=score_cutoff
        )
        if res:
            best_norm, score, _ = res
            return norm2orig[best_norm], int(score)
    else:  # fallback super simples
        for p in pop:
            if norm_name(p) == qn:
                return p, 100

    return None, 0


# Pequenos helpers para JSON
def load_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {} if default is None else default


def dump_json(obj: Any, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)


# Debug rápido
if __name__ == "__main__":  # pragma: no cover
    samples = [
        "Atlético-GO", "Atletico Goianiense", "Atletico-GO",
        "Athletico-PR", "Atlético Paranaense", "Atletico PR",
        "Botafogo-SP", "Botafogo SP FC",
        "Ferroviária", "Ferroviaria SP",
        "Paysandu (PA)", "Remo (PA)",
        "Volta Redonda-RJ", "CRB-AL", "Avaí-SC", "Chapecoense-SC",
    ]
    for s in samples:
        print(f"{s:25s} -> {norm_name(s)}  | key={token_key(s)}")