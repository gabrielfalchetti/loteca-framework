# scripts/aliases_auto_builder.py
# -*- coding: utf-8 -*-
"""
Gera/atualiza automaticamente data/aliases.json usando:
 - whitelist da rodada: {OUT_DIR}/matches_whitelist.csv
 - API-Football (/teams?search=) via RapidAPI
 - mescla com o arquivo existente (se houver)

Saída: data/aliases.json no formato:
{
  "teams": {
    "Ponte Preta": ["Ponte Preta/SP", "ponte preta", ...],
    "Guarani": ["Guarani/SP", ...],
    ...
  },
  "meta": {
    "updated_at": "2025-10-11T12:00:00Z",
    "source": "aliases_auto_builder"
  }
}

Requisitos:
  - Variáveis: OUT_DIR, X_RAPIDAPI_KEY
  - Arquivo: {OUT_DIR}/matches_whitelist.csv com colunas match_id,home,away
"""

from __future__ import annotations
import os, sys, csv, json, re, unicodedata, time
from typing import Dict, List, Optional
from datetime import datetime, timezone
import urllib.request, urllib.parse

API_BASE = "https://api-football-v1.p.rapidapi.com/v3"

def log(s: str) -> None:
    print(s, flush=True)

def deacc(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

def norm(s: str) -> str:
    if s is None: return ""
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"/[A-Za-z]{2}($|[^A-Za-z])", " ", s)  # remove "/SP"
    s = deacc(s).lower().strip()
    return s

def rq(endpoint: str, params: Dict[str, str]) -> dict:
    key = os.environ.get("X_RAPIDAPI_KEY", "")
    if not key:
        raise RuntimeError("X_RAPIDAPI_KEY vazio")
    url = f"{API_BASE}/{endpoint}?" + urllib.parse.urlencode(params)
    headers = {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com",
        "Accept": "application/json",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=30) as resp:
        if resp.status >= 400:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        return json.loads(resp.read().decode("utf-8"))

def teams_search_best(name: str) -> Optional[str]:
    """Retorna o nome oficial mais provável (string) para o 'name' informado."""
    try:
        js = rq("teams", {"search": name})
    except Exception as e:
        log(f"[aliases][WARN] teams search '{name}' falhou: {e}")
        return None
    resp = (js or {}).get("response") or []
    if not resp:
        return None

    target = norm(name)
    best = None
    for item in resp:
        tm = (item or {}).get("team") or {}
        off = (tm.get("name") or "").strip()
        if not off: 
            continue
        if norm(off) == target:
            return off
        if best is None:
            best = off
    return best

def read_whitelist(out_dir: str) -> List[str]:
    wl = os.path.join(out_dir, "matches_whitelist.csv")
    if not os.path.exists(wl):
        raise FileNotFoundError(f"Whitelist não encontrada: {wl}")
    names: List[str] = []
    with open(wl, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        need = ["match_id", "home", "away"]
        low = [c.lower() for c in (r.fieldnames or [])]
        for c in need:
            if c not in low:
                raise RuntimeError(f"Coluna ausente em whitelist: {c}")
        idx = {c.lower(): c for c in r.fieldnames}
        for row in r:
            home = (row[idx["home"]] or "").strip()
            away = (row[idx["away"]] or "").strip()
            if home: names.append(home)
            if away: names.append(away)
    return names

def read_aliases(path: str) -> Dict[str, List[str]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        teams = data.get("teams") or {}
        fixed = {}
        for k, v in teams.items():
            if isinstance(v, list):
                fixed[k] = v
            elif isinstance(v, str):
                fixed[k] = [v]
        return fixed
    except FileNotFoundError:
        return {}
    except Exception as e:
        log(f"[aliases][WARN] falha lendo {path}: {e}")
        return {}

def write_aliases(path: str, teams: Dict[str, List[str]]) -> None:
    data = {
        "teams": {k: sorted(set(v)) for k, v in teams.items()},
        "meta": {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "source": "aliases_auto_builder",
        },
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log(f"[aliases] atualizado: {path} (times={len(teams)})")

def main() -> int:
    out_dir = os.environ.get("OUT_DIR", "").strip()
    if not out_dir:
        log("::error::OUT_DIR vazio")
        return 5

    # nomes vindos da whitelist
    names = read_whitelist(out_dir)
    names_unique = sorted(set(names), key=lambda s: norm(s))
    log(f"[aliases] coletados {len(names_unique)} nomes únicos da whitelist")

    # carrega aliases existentes
    alias_path = "data/aliases.json"
    current = read_aliases(alias_path)  # {canon: [aliases...]}

    # índice reverso para identificação rápida de canônicos já conhecidos
    rev = {}
    for canon, alist in current.items():
        rev[norm(canon)] = canon
        for a in alist:
            rev[norm(a)] = canon

    # aprende novos
    added = 0
    for name in names_unique:
        n = norm(name)
        if not n:
            continue
        if n in rev:
            # já mapeado
            canon = rev[n]
            # registra nome original como alias se não existir
            if name != canon and name not in current.get(canon, []):
                current.setdefault(canon, []).append(name)
            continue

        # ainda não mapeado: consulta API-Football
        official = teams_search_best(name)
        if not official:
            # fallback: usa o próprio nome capitalizado
            official = name.strip()

        canon = official
        current.setdefault(canon, [])
        if name != canon:
            current[canon].append(name)
        added += 1

        # rate limiting simples para não esgotar quota
        time.sleep(0.2)

    write_aliases(alias_path, current)
    log(f"[aliases] novos aprendidos: {added}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        log(f"::error::Falha inesperada aliases_auto_builder: {e}")
        sys.exit(5)