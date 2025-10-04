import json
import re
import unicodedata
from typing import Dict, Tuple, Optional


# =========================
# Helpers de normalização
# =========================

_SPACES_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s-]", flags=re.UNICODE)
_MULTI_DASH_RE = re.compile(r"-+")

def _strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))

def normalize_string(s: str) -> str:
    """
    Normaliza string para comparação resiliente:
      - lower()
      - remove acentos
      - remove pontuação
      - colapsa espaços
      - trim
    """
    if s is None:
        return ""
    s = _strip_accents(s.lower())
    s = _PUNCT_RE.sub(" ", s)
    s = _SPACES_RE.sub(" ", s).strip()
    return s

def slugify(s: str) -> str:
    """
    Gera slug simples (ex.: "Atlético Goianiense" -> "atletico-goianiense")
    """
    s = normalize_string(s)
    s = s.replace(" ", "-")
    s = _MULTI_DASH_RE.sub("-", s)
    return s


# =========================
# Aliases de times
# =========================

def load_aliases(path: Optional[str]) -> Dict[str, str]:
    """
    Lê mapa de aliases: { "alias normalizado": "nome canônico" }.
    Se o arquivo não existir/for None, retorna {}.
    """
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # normaliza chaves; valores ficam como canônicos originais
        norm = {}
        for k, v in data.items():
            norm[normalize_string(k)] = v
        return norm
    except FileNotFoundError:
        return {}
    except Exception:
        # Em caso de JSON malformado, prefira não travar o pipeline
        return {}


# =========================
# Canonicalização e chaves
# =========================

_STOP_TOKENS = {
    "fc", "ec", "sc", "ac", "afc", "saf", "club", "clube", "futebol", "futebol clube",
    "atletico", "atlético", "associacao", "associação", "de", "do", "da", "esporte", "sport",
    "grêmio", "gremio", "paranaense", "paulista", "mineiro", "goianiense", "goias", "goiás",
}

def _light_canonical_tokens(name: str) -> str:
    """
    Canon reduzida para ajudar matching: remove tokens genéricos e mantêm núcleo do nome.
    """
    tokens = [t for t in normalize_string(name).split(" ") if t and t not in _STOP_TOKENS]
    return " ".join(tokens) if tokens else normalize_string(name)

def canonicalize_team(name: str, aliases: Dict[str, str]) -> str:
    """
    Retorna nome canônico de time. Regra:
      1) se o normalizado estiver no dicionário de aliases -> retorna o valor canônico
      2) senão, retorna o próprio 'name' com whitespace aparado
    """
    if not name:
        return ""
    key = normalize_string(name)
    if key in aliases:
        return aliases[key]
    # fallback: retorna como veio (mantendo caixa/acentos originais)
    return name.strip()

def make_match_key(team_home: str, team_away: str) -> str:
    """
    Gera a chave de jogo utilizada no framework:
        "<home>__vs__<away>"
    A chave é feita a partir da versão normalizada (sem acentos/pontuação, minúscula).
    """
    h = normalize_string(team_home)
    a = normalize_string(team_away)
    return f"{h}__vs__{a}"


# =========================
# Comparações
# =========================

def equals_team(a: str, b: str, aliases: Optional[Dict[str, str]] = None) -> bool:
    """
    Compara dois nomes de time de forma robusta:
      - aplica aliases (se fornecidos)
      - normaliza
      - reduz tokens genéricos (_light_canonical_tokens)
    """
    if aliases is None:
        aliases = {}
    # aplica aliases para canônico
    ca = canonicalize_team(a, aliases)
    cb = canonicalize_team(b, aliases)
    # compara versões light
    na = _light_canonical_tokens(ca)
    nb = _light_canonical_tokens(cb)
    return na == nb


# =========================
# Mapeadores/Atalhos públicos
# =========================

__all__ = [
    "load_aliases",
    "canonicalize_team",
    "make_match_key",
    "equals_team",
    "normalize_string",
    "slugify",
]