#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera odds de consenso a partir de:
  - <rodada>/odds_theoddsapi.csv
  - <rodada>/odds_apifootball.csv

Saída:
  - <rodada>/odds_consensus.csv

Colunas:
  team_home, team_away, odds_home, odds_draw, odds_away

Novidades:
 - --ignore-match-ids "1,7,14" -> ignora esses jogos da whitelist
 - Junta por nomes normalizados (tolerante a acentos, FC/AA etc.)
 - Em --strict, falha se algum jogo (após ignorados) ficar sem odds
"""

import os
import re
import sys
import argparse
from unicodedata import normalize as _ucnorm
import pandas as pd

REQ_COLS = ["team_home", "team_away", "odds_home", "odds_draw", "odds_away"]

STOPWORD_TOKENS = {
    "aa","ec","ac","sc","fc","afc","cf","ca","cd","ud",
    "sp","pr","rj","rs","mg","go","mt","ms","pa","pe","pb","rn","ce","ba","al","se","pi","ma","df","es","sc",
}

def log(level, msg):
    tag = "" if level == "INFO" else f"[{level}] "
    print(f"[consensus]{tag}{msg}", flush=True)

def _deaccent(s: str) -> str:
    return _ucnorm("NFKD", str(s or "")).encode("ascii", "ignore").decode("ascii")

def norm_key(name: str) -> str:
    s = _deaccent(name).lower()
    s = s.replace("&", " e ")
    s = re.sub(r"[/()\-_.]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_key_tokens(name: str) -> str:
    toks = [t for t in re.split(r"\s+", norm_key(name)) if t and t not in STOPWORD_TOKENS]
    return " ".join(toks)

def read_csv_safe(path: str):
    if not os.path.isfile(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        log("WARN", f"Falha lendo {path}: {e}")
        return pd.DataFrame()

def secure_float(x):
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None

def median_ignore_none(vals):
    f = [secure_float(v) for v in vals if secure_float(v) is not None]
    if not f:
        return None
    f.sort()
    n = len(f)
    m = n // 2
    return f[m] if n % 2 else (f[m-1] + f[m]) / 2.0

def build_index(df: pd.DataFrame):
    """
    Recebe DF com colunas: [match_id?, home|team_home, away|team_away, odds_*]
    Retorna index: "homeTok|awayTok" -> [(oh,od,oa), ...]
    """
    idx = {}
    if df.empty:
        return idx
    # normaliza nomes de colunas
    cols = {c.lower(): c for c in df.columns}
    home_col = cols.get("home", cols.get("team_home"))
    away_col = cols.get("away", cols.get("team_away"))
    if not home_col or not away_col:
        return idx

    for _, r in df.iterrows():
        h = str(r.get(home_col, "") or "")
        a = str(r.get(away_col, "") or "")
        if not h or not a:
            continue
        k = f"{norm_key_tokens(h)}|{norm_key_tokens(a)}"
        oh = secure_float(r.get("odds_home"))
        od = secure_float(r.get("odds_draw"))
        oa = secure_float(r.get("odds_away"))
        if any(v is None for v in (oh, od, oa)):
            continue
        idx.setdefault(k, []).append((oh, od, oa))
    return idx

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--strict", action="store_true", help="Falha se algum jogo (não ignorado) ficar sem odds")
    ap.add_argument("--ignore-match-ids", default="", help='Lista separada por vírgula. Ex: "1,7,14"')
    return ap.parse_args()

def load_whitelist(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        log("CRITICAL", f"Whitelist não encontrada: {path}")
        sys.exit(6)
    df = pd.read_csv(path)
    # renomeia para colunas padrão de saída
    df = df.rename(columns={"home": "team_home", "away": "team_away"})
    return df[["match_id", "team_home", "team_away"]].copy()

def parse_ignore_ids(s: str):
    ids = set()
    for part in re.split(r"[,\s]+", s.strip()):
        if part:
            ids.add(part.strip())
    return ids

def main():
    args = parse_args()
    rodada = args.rodada
    wl_path = os.path.join(rodada, "matches_whitelist.csv")

    log("INFO", "==================================================")
    log("INFO", f"{'STRICT MODE — ' if args.strict else ''}RODADA: {rodada}")
    log("INFO", "==================================================")

    wl = load_whitelist(wl_path)

    ignore_ids = parse_ignore_ids(args.ignore_match_ids)
    if ignore_ids:
        before = len(wl)
        wl = wl[~wl["match_id"].astype(str).isin(ignore_ids)].copy()
        skipped = before - len(wl)
        log("INFO", f"Ignorando {skipped} jogo(s) por --ignore-match-ids: {sorted(ignore_ids)}")

    log("INFO", f"whitelist: {wl_path}  linhas={len(wl)}  mapping={{'match_id':'match_id','home':'home','away':'away'}}")

    # fontes
    p_theodds = os.path.join(rodada, "odds_theoddsapi.csv")
    p_apifoot = os.path.join(rodada, "odds_apifootball.csv")
    df_the = read_csv_safe(p_theodds)
    df_api = read_csv_safe(p_apifoot)
    n_the = 0 if df_the is None else len(df_the)
    n_api = 0 if df_api is None else len(df_api)
    log("INFO", f"fontes: theodds={n_the}  apifoot={n_api}")

    idx_the = build_index(df_the)
    idx_api = build_index(df_api)

    out_rows = []
    missing = []

    for _, r in wl.iterrows():
        th = str(r["team_home"])
        ta = str(r["team_away"])
        key = f"{norm_key_tokens(th)}|{norm_key_tokens(ta)}"

        cands = []
        if key in idx_the:
            cands.extend(idx_the[key])
        if key in idx_api:
            cands.extend(idx_api[key])

        if not cands:
            missing.append((r["match_id"], th, ta))
            continue

        oh = median_ignore_none([x[0] for x in cands])
        od = median_ignore_none([x[1] for x in cands])
        oa = median_ignore_none([x[2] for x in cands])

        if oh is None or od is None or oa is None:
            missing.append((r["match_id"], th, ta))
            continue

        out_rows.append({
            "team_home": th,
            "team_away": ta,
            "odds_home": oh,
            "odds_draw": od,
            "odds_away": oa,
        })

    out_path = os.path.join(rodada, "odds_consensus.csv")
    pd.DataFrame(out_rows, columns=REQ_COLS).to_csv(out_path, index=False)

    if args.strict and missing:
        print("##[error][CRITICAL] Jogos sem odds após consenso (modo estrito ligado).")
        print("match_id   team_home team_away")
        for mid, th, ta in missing:
            print(f"{str(mid):>7} {th}   {ta}")
        sys.exit(6)

    if not out_rows:
        print("##[error][CRITICAL] Consenso vazio (nenhuma linha gerada).")
        sys.exit(6)

    log("INFO", f"consenso gerado: {out_path}  linhas={len(out_rows)}  missing={len(missing)}")
    if missing:
        log("WARN", f"Jogos sem odds (não ignorados): {len(missing)} -> {[m[0] for m in missing]}")
    return 0

if __name__ == "__main__":
    sys.exit(main())