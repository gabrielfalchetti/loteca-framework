# services/team_resolver_api.py
# -*- coding: utf-8 -*-
"""
FastAPI para resolução de nomes de times:
 - /health
 - /resolve?name=...
 - /bulk_resolve (POST json {"names": [...]})

Ordem de resolução:
 1) data/aliases.json (canônico + aliases)
 2) API-Football /teams?search= (se houver X_RAPIDAPI_KEY)
 3) devolve o próprio nome "limpo" como canônico

Pode ser usado pelos scripts em tempo real. Mantém um cache em memória
e pode opcionalmente persistir novos aprendizados em data/aliases.json.

Executar:
  uvicorn services.team_resolver_api:app --host 0.0.0.0 --port 8088
"""

from __future__ import annotations
import os, json, re, unicodedata, threading, time
from typing import Dict, List, Optional
from fastapi import FastAPI, Query
from pydantic import BaseModel
import urllib.request, urllib.parse

API_BASE = "https://api-football-v1.p.rapidapi.com/v3"

def deacc(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

def norm(s: str) -> str:
    if s is None: return ""
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"/[A-Za-z]{2}($|[^A-Za-z])", " ", s)
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

class BulkRequest(BaseModel):
    names: List[str]

class Resolver:
    def __init__(self, aliases_path: str = "data/aliases.json"):
        self.aliases_path = aliases_path
        self.lock = threading.Lock()
        self.teams: Dict[str, List[str]] = {}
        self.rev: Dict[str, str] = {}
        self.load()

    def load(self):
        try:
            with open(self.aliases_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.teams = {k: (v if isinstance(v, list) else [v]) for k, v in (data.get("teams") or {}).items()}
        except FileNotFoundError:
            self.teams = {}
        self._rebuild_rev()

    def _rebuild_rev(self):
        self.rev = {}
        for canon, alist in self.teams.items():
            self.rev[norm(canon)] = canon
            for a in alist:
                self.rev[norm(a)] = canon

    def save(self):
        os.makedirs(os.path.dirname(self.aliases_path), exist_ok=True)
        with open(self.aliases_path, "w", encoding="utf-8") as f:
            json.dump({"teams": self.teams}, f, ensure_ascii=False, indent=2)

    def resolve_local(self, name: str) -> Optional[str]:
        n = norm(name)
        return self.rev.get(n)

    def resolve_remote(self, name: str) -> Optional[str]:
        try:
            js = rq("teams", {"search": name})
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
        except Exception:
            return None

    def learn(self, original: str, canonical: str):
        with self.lock:
            self.teams.setdefault(canonical, [])
            if original != canonical and original not in self.teams[canonical]:
                self.teams[canonical].append(original)
            self._rebuild_rev()

resolver = Resolver()
app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True, "aliases": len(resolver.teams)}

@app.get("/resolve")
def resolve(name: str = Query(..., min_length=1)):
    # 1) local
    local = resolver.resolve_local(name)
    if local:
        return {"input": name, "canonical": local, "source": "aliases"}

    # 2) remote
    remote = resolver.resolve_remote(name)
    if remote:
        resolver.learn(name,