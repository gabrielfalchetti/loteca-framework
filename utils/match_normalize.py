# utils/match_normalize.py
from __future__ import annotations
import unicodedata, re, difflib
from typing import List, Dict

ALIASES: Dict[str, List[str]] = {
    "america mineiro": ["america mg","america-mg","america futebol clube mg","america"],
    "chapecoense": ["chapecoense sc","associacao chapecoense de futebol","chapeco"],
    "avai": ["avai fc","avai sc","avai futebol clube","avai-sc"],
    "volta redonda": ["volta redonda rj","volta redonda fc","volta redonda futebol clube"],
    "sao paulo": ["sao paulo fc","sao paulo futebol clube","spfc"],
    "ceara": ["ceara sc","ceara sporting club"],
    "amazonas": ["amazonas fc","amazonas futebol clube"],
    "atletico mineiro": ["atletico mg","clube atletico mineiro","cam","atletico-mg"],
    "atletico paranaense": ["athletico pr","athletico paranaense","cap","athletico-pr","atletico-pr"],
}

def extend_aliases(extra: Dict[str, List[str]]) -> None:
    # mescla/normaliza chaves para lowercase sem acento
    for k, vals in (extra or {}).items():
        can = canonical(k)
        ALIASES.setdefault(can, [])
        for v in vals or []:
            vv = canonical(v)
            if vv != can and vv not in ALIASES[can]:
                ALIASES[can].append(vv)

def canonical(name: str) -> str:
    n = unicodedata.normalize("NFKD", name).encode("ascii","ignore").decode("ascii").lower()
    n = re.sub(r"[^a-z0-9 ]+", " ", n)
    n = re.sub(r"\b(ec|fc|afc|sc|ac|esporte clube|futebol clube)\b","", n)
    n = " ".join(n.split())
    for can, alts in ALIASES.items():
        if n == can or n in alts:
            return can
    return n

def fuzzy_match(target: str, candidates: List[str], threshold: float = 0.92) -> str | None:
    t = canonical(target)
    cands = list({canonical(c) for c in candidates})
    best, score = None, -1.0
    for c in cands:
        r = difflib.SequenceMatcher(a=t, b=c).ratio()
        if r > score:
            best, score = c, r
    return best if score >= threshold else None
