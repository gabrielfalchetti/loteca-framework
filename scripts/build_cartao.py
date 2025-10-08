#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera o cartão Loteca a partir dos artefatos da rodada.

Prioridade de entradas:
1) ${OUT_DIR}/matches_whitelist.csv (se existir);
2) data/in/matches_source.csv (fallback automático).

Dados opcionais para enriquecer o cartão:
- ${OUT_DIR}/kelly_stakes.csv  -> pick/stake por jogo (preferencial)
- ${OUT_DIR}/predictions_blend.csv -> probabilidades calibradas (fallback p/ confiança)
- ${OUT_DIR}/predictions_market.csv -> probabilidades de mercado (fallback p/ confiança)

Saída:
- ${OUT_DIR}/loteca_cartao.txt

Formato de cada linha:
Jogo NN - <mandante> x <visitante>: <pick> (stake=<valor>) [<conf>%]
"""

import argparse
import csv
import os
import sys
import unicodedata
import re
from typing import Dict, Tuple, List

def log(msg: str) -> None:
    print(msg, flush=True)

def norm_str(s: str) -> str:
    s = str(s or "").strip().lower()
    # remove acentos
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    # separadores em hífen
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s

def match_key_from_teams(home: str, away: str) -> str:
    return f"{norm_str(home)}__vs__{norm_str(away)}"

def read_csv_safe(path: str) -> List[Dict[str, str]]:
    if not os.path.isfile(path):
        return []
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        sniffer = csv.Sniffer()
        sample = f.read(4096)
        f.seek(0)
        dialect = sniffer.sniff(sample) if sample else csv.excel
        reader = csv.DictReader(f, dialect=dialect)
        for r in reader:
            rows.append({k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in r.items()})
    return rows

def load_whitelist(out_dir: str) -> List[Dict[str, str]]:
    # 1) Tenta whitelist da rodada
    wl_path = os.path.join(out_dir, "matches_whitelist.csv")
    rows = read_csv_safe(wl_path)
    if rows:
        # normaliza colunas esperadas
        fixed = []
        for r in rows:
            # aceita tanto 'team_home'/'team_away' quanto 'home'/'away'
            home = r.get("team_home") or r.get("home") or ""
            away = r.get("team_away") or r.get("away") or ""
            mk = r.get("match_key") or match_key_from_teams(home, away)
            mid = r.get("match_id") or r.get("id") or ""
            fixed.append({"match_id": str(mid or ""), "team_home": home, "team_away": away, "match_key": mk})
        log(f"[cartao] whitelist: usando {wl_path} ({len(fixed)} jogos)")
        return fixed

    # 2) Fallback: matches_source.csv (repositório)
    src_path = os.path.join("data", "in", "matches_source.csv")
    rows = read_csv_safe(src_path)
    if not rows:
        raise FileNotFoundError(
            f"[cartao] Nenhuma fonte de jogos encontrada. "
            f"Esperado {wl_path} OU {src_path}."
        )

    fixed = []
    for r in rows:
        home = r.get("home") or r.get("team_home") or ""
        away = r.get("away") or r.get("team_away") or ""
        mid  = r.get("match_id") or r.get("id") or ""
        mk   = match_key_from_teams(home, away)
        fixed.append({"match_id": str(mid or ""), "team_home": home, "team_away": away, "match_key": mk})
    log(f"[cartao] whitelist: fallback {src_path} ({len(fixed)} jogos)")
    return fixed

def load_kelly(out_dir: str) -> Dict[str, Dict[str, float]]:
    """Retorna por match_key: {pick, stake, prob?, edge?} quando disponível."""
    path = os.path.join(out_dir, "kelly_stakes.csv")
    rows = read_csv_safe(path)
    if not rows:
        log("[cartao] kelly_stakes.csv ausente — picks/stakes serão '?' e 0.0")
        return {}
    out: Dict[str, Dict[str, float]] = {}
    for r in rows:
        mk = r.get("match_key") or match_key_from_teams(r.get("team_home",""), r.get("team_away",""))
        pick = r.get("pick") or "?"
        try:
            stake = float(r.get("stake", 0.0) or 0.0)
        except Exception:
            stake = 0.0
        # campos opcionais para confiança
        try:
            prob = float(r.get("prob", "") or "nan")
        except Exception:
            prob = float("nan")
        try:
            edge = float(r.get("edge", "") or "nan")
        except Exception:
            edge = float("nan")

        out[mk] = {"pick": pick, "stake": stake, "prob": prob, "edge": edge}
    log(f"[cartao] kelly: carregado {len(out)} linhas de {path}")
    return out

def load_probs(out_dir: str) -> Dict[str, Tuple[float, float, float]]:
    """
    Probabilidades para confiança [%]:
    prioridade: predictions_blend.csv -> predictions_market.csv
    Retorna dict por match_key: (p_home, p_draw, p_away)
    """
    blend = os.path.join(out_dir, "predictions_blend.csv")
    market = os.path.join(out_dir, "predictions_market.csv")
    result: Dict[str, Tuple[float, float, float]] = {}

    rows = read_csv_safe(blend)
    if rows and {"match_id","p_home","p_draw","p_away"}.issubset(rows[0].keys()):
        for r in rows:
            mk = match_key_from_teams(r.get("team_home",""), r.get("team_away",""))
            try:
                result[mk] = (float(r["p_home"]), float(r["p_draw"]), float(r["p_away"]))
            except Exception:
                pass
        if result:
            log(f"[cartao] probs: usando predictions_blend.csv ({len(result)})")
            return result

    rows = read_csv_safe(market)
    # em predictions_market, as colunas são match_key, p_home, p_draw, p_away
    if rows:
        for r in rows:
            mk = r.get("match_key") or match_key_from_teams(r.get("home",""), r.get("away",""))
            try:
                p_home = float(r.get("p_home","nan"))
                p_draw = float(r.get("p_draw","nan"))
                p_away = float(r.get("p_away","nan"))
                if not (p_home != p_home or p_draw != p_draw or p_away != p_away):  # checa NaN
                    result[mk] = (p_home, p_draw, p_away)
            except Exception:
                pass
    if result:
        log(f"[cartao] probs: usando predictions_market.csv ({len(result)})")
    else:
        log("[cartao] probs: nenhum arquivo de probabilidades disponível")
    return result

def pick_to_symbol(pick: str) -> str:
    p = (pick or "").strip().upper()
    # convenção: HOME=1, DRAW=X, AWAY=2
    if p in ("HOME", "1", "H"):
        return "1"
    if p in ("DRAW", "X", "D"):
        return "X"
    if p in ("AWAY", "2", "A"):
        return "2"
    return "?"

def best_confidence_for_pick(pick: str, probs: Tuple[float,float,float]) -> float:
    if not probs:
        return float("nan")
    p_home, p_draw, p_away = probs
    sym = pick_to_symbol(pick)
    if sym == "1":
        return p_home * 100.0
    if sym == "X":
        return p_draw * 100.0
    if sym == "2":
        return p_away * 100.0
    # se não tem pick, retorna a maior probabilidade como sinal de confiança bruta
    return max(p_home, p_draw, p_away) * 100.0

def main():
    parser = argparse.ArgumentParser(description="Gera o cartão Loteca.")
    parser.add_argument("--rodada", required=True, help="Diretório da rodada (OUT_DIR)")
    args = parser.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)

    # Carrega jogos (whitelist OU matches_source)
    try:
        wl = load_whitelist(out_dir)
    except Exception as e:
        log(f"##[error][cartao] {e}")
        sys.exit(26)

    if not wl:
        log("##[error][cartao] Nenhum jogo encontrado para o cartão.")
        sys.exit(26)

    # Artefatos opcionais
    kelly = load_kelly(out_dir)
    probs = load_probs(out_dir)

    # Monta o cartão
    linhas = []
    for idx, r in enumerate(wl, start=1):
        home = r["team_home"]
        away = r["team_away"]
        mk   = r["match_key"]

        pick = "?"
        stake = 0.0
        conf_pct = float("nan")

        if mk in kelly:
            pick = kelly[mk].get("pick", "?")
            stake = float(kelly[mk].get("stake", 0.0) or 0.0)
            # confiança baseada nas probabilidades se existirem
            if mk in probs:
                conf_pct = best_confidence_for_pick(pick, probs[mk])

        elif mk in probs:
            # sem Kelly, infere pick como o maior prob
            ph, pd, pa = probs[mk]
            if ph >= pd and ph >= pa:
                pick = "1"
                conf_pct = ph * 100.0
            elif pd >= ph and pd >= pa:
                pick = "X"
                conf_pct = pd * 100.0
            else:
                pick = "2"
                conf_pct = pa * 100.0

        conf_str = f"{conf_pct:.1f}%" if conf_pct == conf_pct else "nan%"

        linhas.append(f"Jogo {idx:02d} - {home} x {away}: {pick} (stake={stake}) [{conf_str}]")

    # Salva cartão
    out_card = os.path.join(out_dir, "loteca_cartao.txt")
    with open(out_card, "w", encoding="utf-8") as f:
        f.write("==== CARTÃO LOTECA ====\n")
        for line in linhas:
            f.write(line + "\n")
        f.write("=======================\n")

    # Echo para o log do workflow
    print("==== CARTÃO LOTECA ====")
    for line in linhas:
        print(line)
    print("=======================")
    log(f"[cartao] OK -> {out_card}")

if __name__ == "__main__":
    main()