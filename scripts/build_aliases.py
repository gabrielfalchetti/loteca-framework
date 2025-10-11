# scripts/build_aliases.py
import argparse
import json
import os
import re
import sys
from collections import defaultdict

import pandas as pd

def _deacc(s: str) -> str:
    # remoção manual de acentos (rápida, sem depender de unidecode)
    trans = str.maketrans(
        "ÁÀÃÂÄáàãâäÉÊÈËéêèëÍÎÌÏíîìïÓÔÒÕÖóôòõöÚÛÙÜúûùüÇçÑñ",
        "AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn",
    )
    return s.translate(trans)

def _slugify(s: str) -> str:
    s = _deacc(s)
    s = s.lower().strip()
    s = re.sub(r"[^\w\s\-]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--whitelist", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--debug", action="store_true")
    args, unknown = p.parse_known_args()
    if "--debug" in unknown:
        args.debug = True

    if not os.path.isfile(args.whitelist):
        print(f"::error::whitelist not found: {args.whitelist}")
        sys.exit(2)

    # carrega whitelist
    df = pd.read_csv(args.whitelist)
    need_cols = {"match_id", "home", "away"}
    if not need_cols.issubset(set(c.lower() for c in df.columns)):
        print("::error::whitelist missing required columns match_id,home,away")
        sys.exit(2)

    # normaliza headers
    cols = {c.lower(): c for c in df.columns}
    df = df.rename(columns={cols.get("match_id"): "match_id",
                            cols.get("home"): "home",
                            cols.get("away"): "away"})

    # carrega aliases existente (se houver)
    aliases_path = args.out
    existing = {}
    if os.path.isfile(aliases_path):
        try:
            with open(aliases_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception as e:
            print(f"::warning::failed to read existing aliases: {e}")
            existing = {}

    # estrutura base
    # aliases["teams"][canonical] = list_of_aliases
    # aliases["countries"][canonical] = list_of_aliases
    aliases = {"teams": {}, "countries": {}}
    if isinstance(existing, dict):
        aliases["teams"] = dict(existing.get("teams", {}))
        aliases["countries"] = dict(existing.get("countries", {}))

    # heurística: qualquer nome com espaço => time; sem espaço => país (bem simples).
    def add_alias(bucket: str, canonical: str, name: str):
        arr = aliases[bucket].setdefault(canonical, [])
        if name not in arr:
            arr.append(name)

    # gera a partir da whitelist
    for _, row in df.iterrows():
        for col in ("home", "away"):
            raw = str(row[col]).strip()
            if not raw:
                continue
            canon = raw  # já normalizado anteriormente no pipeline
            low = raw.lower()
            slug = _slugify(raw)
            deac = _deacc(raw)

            # decide bucket
            if " " in low or "/" in low or "-" in low:
                bucket = "teams"
            else:
                bucket = "countries"

            # insere variações úteis
            for variant in {raw, low, deac, slug}:
                add_alias(bucket, canon, variant)

            # remova UF do final (e.g., "Ponte Preta/SP" -> "Ponte Preta")
            uf_stripped = re.sub(r"/[A-Za-z]{2}($|[^A-Za-z])", r"\1", raw).strip()
            if uf_stripped and uf_stripped != raw:
                for variant in {uf_stripped, _slugify(uf_stripped), _deacc(uf_stripped),
                                uf_stripped.lower()}:
                    add_alias(bucket, canon, variant)

    # ordena e salva
    for k in ("teams", "countries"):
        aliases[k] = {canon: sorted(set(vs)) for canon, vs in sorted(aliases[k].items())}

    os.makedirs(os.path.dirname(aliases_path), exist_ok=True)
    with open(aliases_path, "w", encoding="utf-8") as f:
        json.dump(aliases, f, ensure_ascii=False, indent=2)

    if args.debug:
        print(f"[aliases] wrote {aliases_path}")
        print(json.dumps(aliases, ensure_ascii=False, indent=2)[:1200])

if __name__ == "__main__":
    main()