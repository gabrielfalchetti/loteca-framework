#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/publish_kelly.py

Gera apostas por Kelly para a rodada:
 - Lê odds do consenso (se existir) e/ou faz fallback para odds_theoddsapi.csv
 - Extrai odds mesmo se vierem aninhadas (JSON em colunas como bookmakers/markets/prices)
 - Reconstrói probabilidades implícitas das odds (corrigindo overround)
 - Aplica Kelly com fração, cap, arredondamento e top_n
 - Publica data/out/<RODADA>/kelly_stakes.csv
 - Logs e diagnósticos em data/out/<RODADA>/debug/

Requer: pandas, numpy
"""

from __future__ import annotations

import os
import sys
import json
import math
import argparse
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict

import pandas as pd
import numpy as np


# ========= Util / Normalização de Colunas =========

HOME_KEYS = ["team_home", "home", "home_team", "mandante"]
AWAY_KEYS = ["team_away", "away", "away_team", "visitante"]
MATCH_KEYS = ["match_key", "match_id", "id_jogo", "partida", "fixture_id"]

ODDS_HOME_KEYS = ["odds_home", "home_odds", "home_price", "price_home", "odd_home", "decimal_home"]
ODDS_DRAW_KEYS = ["odds_draw", "draw_odds", "draw_price", "price_draw", "odd_draw", "decimal_draw"]
ODDS_AWAY_KEYS = ["odds_away", "away_odds", "away_price", "price_away", "odd_away", "decimal_away"]

PROB_HOME_KEYS = ["prob_home", "home_prob", "p_home", "prob1"]
PROB_DRAW_KEYS = ["prob_draw", "draw_prob", "p_draw", "probx"]
PROB_AWAY_KEYS = ["prob_away", "away_prob", "p_away", "prob2"]

def first_col(df: pd.DataFrame, keys: List[str]) -> Optional[str]:
    for k in keys:
        if k in df.columns:
            return k
    # tenta por lowercase match solto
    low = {c.lower(): c for c in df.columns}
    for k in keys:
        if k in low:
            return low[k]
    return None

def normalize_basic_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Traz colunas padrão: team_home, team_away, match_key (quando existirem)."""
    out = df.copy()
    h = first_col(out, HOME_KEYS)
    a = first_col(out, AWAY_KEYS)
    m = first_col(out, MATCH_KEYS)
    if h and h != "team_home": out = out.rename(columns={h: "team_home"})
    if a and a != "team_away": out = out.rename(columns={a: "team_away"})
    if m and m != "match_key": out = out.rename(columns={m: "match_key"})

    # garante strings e um __join_key robusto
    for c in ["team_home", "team_away"]:
        if c in out.columns:
            out[c] = out[c].astype(str).str.strip()
        else:
            out[c] = None

    if "match_key" not in out.columns or out["match_key"].isna().all():
        out["match_key"] = out.apply(
            lambda r: f"{(r.get('team_home') or '')}__vs__{(r.get('team_away') or '')}", axis=1
        )
    out["__join_key"] = out.apply(
        lambda r: f"{(r.get('team_home') or '').lower()}__vs__{(r.get('team_away') or '').lower()}",
        axis=1
    )
    return out

def _try_map_price_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """Cria odds_home/draw/away a partir de aliases comuns."""
    out = df.copy()
    def _pick(keys):
        for k in keys:
            if k in out.columns:
                return k
        return None
    m = _pick(ODDS_HOME_KEYS)
    if m and "odds_home" not in out.columns: out = out.rename(columns={m: "odds_home"})
    m = _pick(ODDS_DRAW_KEYS)
    if m and "odds_draw" not in out.columns: out = out.rename(columns={m: "odds_draw"})
    m = _pick(ODDS_AWAY_KEYS)
    if m and "odds_away" not in out.columns: out = out.rename(columns={m: "odds_away"})
    return out

def _try_map_prob_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """Cria prob_home/draw/away a partir de aliases comuns."""
    out = df.copy()
    def _pick(keys):
        for k in keys:
            if k in out.columns:
                return k
        return None
    m = _pick(PROB_HOME_KEYS)
    if m and "prob_home" not in out.columns: out = out.rename(columns={m: "prob_home"})
    m = _pick(PROB_DRAW_KEYS)
    if m and "prob_draw" not in out.columns: out = out.rename(columns={m: "prob_draw"})
    m = _pick(PROB_AWAY_KEYS)
    if m and "prob_away" not in out.columns: out = out.rename(columns={m: "prob_away"})
    return out

def try_pivot_if_needed(df_longish: pd.DataFrame) -> pd.DataFrame:
    """
    Se vier num formato longo (ex.: outcome in ['home','draw','away'] + price),
    pivoteia para odds_home/draw/away.
    Tenta detectar colunas típicas: outcome/outcome_name/side e price/odd/decimal.
    """
    df = df_longish.copy()
    # já está no formato certo?
    already = [c for c in ["odds_home","odds_draw","odds_away"] if c in df.columns]
    if len(already) >= 2:
        return df

    # detecta possíveis colunas
    outcome_col = None
    for k in ["outcome", "outcome_name", "side", "pick", "selection", "name"]:
        if k in df.columns:
            outcome_col = k
            break
    price_col = None
    for k in ["price", "odd", "odds", "decimal", "value"]:
        if k in df.columns:
            price_col = k
            break
    if not outcome_col or not price_col:
        return df

    # normaliza outcome -> home/draw/away
    def norm_outcome(x: str) -> Optional[str]:
        if not isinstance(x, str): 
            return None
        s = x.strip().lower()
        if s in ("home", "1", "h", "mandante"): return "odds_home"
        if s in ("draw", "x", "empate"): return "odds_draw"
        if s in ("away", "2", "a", "visitante"): return "odds_away"
        return None

    df["_oc_norm"] = df[outcome_col].map(norm_outcome)
    if df["_oc_norm"].isna().all():
        return df

    base = normalize_basic_cols(df)
    subset = base[["match_key","__join_key","team_home","team_away", "_oc_norm", price_col]].dropna(subset=["_oc_norm"])
    pivot = (
        subset
        .pivot_table(index=["match_key","__join_key","team_home","team_away"],
                     columns="_oc_norm", values=price_col, aggfunc="max")
        .reset_index()
    )
    pivot.columns.name = None
    return pivot


# ========= Kelly =========

@dataclass
class KellyConfig:
    bankroll: float = 1000.0
    kelly_fraction: float = 0.5
    kelly_cap: float = 0.10     # fração máxima do bankroll por aposta (ex: 0.10 = 10%)
    min_stake: float = 0.0
    max_stake: float = 0.0      # 0 = sem teto absoluto (além do cap)
    round_to: float = 1.0       # arredonda stake (ex: 1 real)
    top_n: int = 14             # publicar até N melhores

def stake_from_kelly(p: float, o: float, cfg: KellyConfig) -> Tuple[float, float, float]:
    """
    Retorna (stake, kelly_full, edge).
      - p: prob. do evento
      - o: odd decimal (> 1.0)
      - kelly_full: fração Kelly original (sem fraction nem cap), em fração do bankroll
      - stake: valor em moeda (após fraction, cap e arredondamento)
      - edge: p*o - 1
    """
    # validações
    if p is None or o is None:
        return 0.0, 0.0, 0.0
    try:
        p = float(p)
        o = float(o)
    except Exception:
        return 0.0, 0.0, 0.0
    if p <= 0 or p >= 1 or o <= 1.0:
        return 0.0, 0.0, 0.0

    b = o - 1.0
    edge = p * o - 1.0
    kelly_full = (b * p - (1 - p)) / b  # fórmula clássica
    if kelly_full <= 0:
        return 0.0, kelly_full, edge

    # aplica fraction
    f = kelly_full * cfg.kelly_fraction
    # aplica cap relativo ao bankroll
    f = min(f, cfg.kelly_cap)

    stake = f * cfg.bankroll
    # aplica teto absoluto, se houver (max_stake > 0)
    if cfg.max_stake and cfg.max_stake > 0:
        stake = min(stake, cfg.max_stake)
    # piso
    stake = max(stake, cfg.min_stake)
    # arredonda
    if cfg.round_to and cfg.round_to > 0:
        stake = math.floor(stake / cfg.round_to + 1e-9) * cfg.round_to

    return float(stake), float(kelly_full), float(edge)


# ========= Leitura de Odds =========

def _maybe_json(x):
    if isinstance(x, (dict, list)):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        s2 = s.replace("'", '"')
        try:
            return json.loads(s2)
        except Exception:
            try:
                return json.loads(s)
            except Exception:
                return None
    return None

def _extract_h2h_best_prices(row_dict: dict) -> dict:
    """
    Estruturas típicas do TheOddsAPI:
    {
      "bookmakers":[
        {"markets":[{"key":"h2h","outcomes":[{"name":"Home","price":1.9}, ...]}]}
      ]
    }
    Retorna o melhor preço encontrado por outcome.
    """
    best = {"odds_home": None, "odds_draw": None, "odds_away": None}
    if not isinstance(row_dict, dict):
        return best

    pools = []

    bks = row_dict.get("bookmakers")
    if isinstance(bks, list):
        for bk in bks:
            if isinstance(bk, dict) and isinstance(bk.get("markets"), list):
                pools.extend(bk["markets"])

    # às vezes vem markets direto na raiz
    if isinstance(row_dict.get("markets"), list):
        pools.extend(row_dict["markets"])

    def try_market(m):
        key = (m.get("key") or "").lower()
        if key not in ("h2h", "1x2", "match_odds", "full_time_result"):
            return
        outcomes = m.get("outcomes") or m.get("prices") or m.get("lines") or []
        if not isinstance(outcomes, list):
            return
        for o in outcomes:
            name = (o.get("name") or o.get("outcome") or "").strip().lower()
            price = o.get("price") or o.get("decimal") or o.get("odd") or o.get("odds")
            try:
                price = float(price)
            except Exception:
                price = None
            target = None
            if name in ("home", "1", "h", "mandante"):
                target = "odds_home"
            elif name in ("draw", "x", "empate"):
                target = "odds_draw"
            elif name in ("away", "2", "a", "visitante"):
                target = "odds_away"
            if target and price and price > 1:
                prev = best.get(target)
                if prev is None or price > prev:
                    best[target] = price

    for m in pools:
        if isinstance(m, dict):
            try_market(m)

    return best

def load_odds_from_provider(out_dir: str, debug: bool = False) -> Optional[pd.DataFrame]:
    """
    Fallback para odds de provedores individuais (hoje, TheOddsAPI).
    Retorna DataFrame com colunas: match_key, team_home, team_away, odds_home, odds_draw, odds_away
    ou None se nada utilizável for encontrado.
    """
    candidates = ["odds_theoddsapi.csv"]
    debug_dir = os.path.join(out_dir, "debug")
    os.makedirs(debug_dir, exist_ok=True)

    for fn in candidates:
        p = os.path.join(out_dir, fn)
        if not os.path.exists(p):
            continue
        try:
            df = pd.read_csv(p)
        except Exception as e:
            if debug:
                print(f"[kelly] falha ao ler {fn}: {e}")
            continue

        # dump colunas para diagnóstico
        try:
            with open(os.path.join(debug_dir, "kelly_provider_cols.txt"), "w", encoding="utf-8") as f:
                f.write(", ".join(map(str, df.columns.tolist())) + "\n")
        except Exception:
            pass

        # (1) caminho plano: tenta pivot + aliases
        plain = try_pivot_if_needed(df.copy())
        plain = normalize_basic_cols(plain)
        plain = _try_map_price_aliases(plain)
        available = [c for c in ["odds_home", "odds_draw", "odds_away"] if c in plain.columns]
        if available:
            keep = [c for c in ["match_key","team_home","team_away","__join_key", *available] if c in plain.columns]
            out = plain[keep].drop_duplicates()
            if debug:
                print(f"[kelly] odds fallback de: {fn} (plano) {len(out)} linhas cols={keep}")
            if not out.empty:
                return out

        # (2) caminho JSON-aware: procurar colunas com payload de odds
        json_cols = [c for c in df.columns if c.lower() in ("bookmakers","markets","prices","odds","payload","data")]
        json_hit = None
        for c in json_cols:
            sample = df[c].dropna().head(1)
            if not sample.empty and _maybe_json(sample.iloc[0]) is not None:
                json_hit = c
                break

        if json_hit:
            base = normalize_basic_cols(df.copy())
            rows = []
            for _, r in base.iterrows():
                payload = _maybe_json(r.get(json_hit))
                prices = _extract_h2h_best_prices(payload) if payload else {"odds_home":None,"odds_draw":None,"odds_away":None}
                rows.append({
                    "match_key": r.get("match_key"),
                    "__join_key": r.get("__join_key"),
                    "team_home": r.get("team_home"),
                    "team_away": r.get("team_away"),
                    **prices,
                })
            out = pd.DataFrame(rows)
            mask = out[["odds_home","odds_draw","odds_away"]].notna().any(axis=1)
            out = out[mask].drop_duplicates()
            if debug:
                print(f"[kelly] odds fallback de: {fn} (json) {len(out)} linhas")
            if not out.empty:
                return out

        if debug:
            print(f"[kelly] provider {fn} sem odds utilizáveis (plano/json).")

    return None


# ========= Probabilidades a partir de Odds =========

def probs_from_odds_row(oh: float, od: float, oa: float) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Converte odds decimais em probabilidades normalizadas (corrige overround).
    Retorna (ph, pd, pa) ou (None, None, None) se inválido.
    """
    try:
        vals = [float(oh), float(od), float(oa)]
    except Exception:
        return (None, None, None)
    if any(v is None or v <= 1.0 for v in vals):
        return (None, None, None)

    inv = [1.0/v for v in vals]
    s = sum(inv)
    if s <= 0:
        return (None, None, None)
    return tuple(i/s for i in inv)  # type: ignore


# ========= Núcleo de Cálculo =========

def compute_kelly_rows(df: pd.DataFrame, cfg: KellyConfig, debug: bool=False) -> pd.DataFrame:
    """
    Recebe DF com colunas:
      team_home, team_away, match_key, (prob_home/prob_draw/prob_away)*, (odds_home/odds_draw/odds_away)*
    Calcula a melhor seleção por Kelly para cada jogo e retorna tabela final.
    """
    base = df.copy()

    # garantias
    base = normalize_basic_cols(base)
    base = _try_map_price_aliases(base)
    base = _try_map_prob_aliases(base)

    have_probs = all(c in base.columns for c in ["prob_home","prob_draw","prob_away"])
    have_odds  = all(c in base.columns for c in ["odds_home","odds_draw","odds_away"])

    # se não houver probs mas houver odds, cria probs implícitas
    if not have_probs and have_odds:
        ph, pd, pa = [], [], []
        for _, r in base.iterrows():
            pH, pD, pA = probs_from_odds_row(r.get("odds_home"), r.get("odds_draw"), r.get("odds_away"))
            ph.append(pH); pd.append(pD); pa.append(pA)
        base["prob_home"] = ph
        base["prob_draw"] = pd
        base["prob_away"] = pa
        have_probs = True

    # se ainda faltarem odds → não há como calcular stake
    if not have_odds:
        if debug:
            print("[kelly] AVISO: odds ausentes no consenso — gerando arquivo vazio.")
        return pd.DataFrame(columns=[
            "match_key","team_home","team_away","pick","prob_pick","odds_pick","kelly_full","stake","edge"
        ])

    # loop por jogo: decide pick e stake
    out_rows = []
    for _, r in base.iterrows():
        mh = r.get("team_home")
        ma = r.get("team_away")
        mk = r.get("match_key")

        probs = {
            "1": r.get("prob_home"),
            "X": r.get("prob_draw"),
            "2": r.get("prob_away"),
        }
        odds = {
            "1": r.get("odds_home"),
            "X": r.get("odds_draw"),
            "2": r.get("odds_away"),
        }

        # candidata por Kelly (maior stake com edge > 0)
        best = None
        for sigla in ["1","X","2"]:
            p = probs.get(sigla)
            o = odds.get(sigla)
            stake, kfull, edge = stake_from_kelly(p or 0.0, o or 0.0, cfg)
            cand = {
                "match_key": mk,
                "team_home": mh,
                "team_away": ma,
                "pick": sigla,
                "prob_pick": p,
                "odds_pick": o,
                "kelly_full": kfull,
                "stake": stake,
                "edge": edge,
            }
            if best is None:
                best = cand
            else:
                # prioriza stake > 0 e maior stake; em empate, maior edge
                if (cand["stake"] > (best["stake"] or 0)) or (
                    cand["stake"] == (best["stake"] or 0) and (cand["edge"] or -9) > (best["edge"] or -9)
                ):
                    best = cand

        # se nada com stake > 0, escolhe outcome mais provável só para compor o cartão
        if best and (best["stake"] or 0) <= 0:
            # seleciona maior probabilidade
            spick = max(probs.items(), key=lambda kv: (kv[1] if kv[1] is not None else -1))[0] if any(v is not None for v in probs.values()) else "1"
            best["pick"] = spick
            best["prob_pick"] = probs.get(spick)
            best["odds_pick"] = odds.get(spick)
            # stake permanece 0
        if best:
            out_rows.append(best)

    out = pd.DataFrame(out_rows)

    # ordena por stake desc, depois edge desc
    if not out.empty:
        out = out.sort_values(by=["stake","edge"], ascending=[False, False]).reset_index(drop=True)

        # aplica top_n se fizer sentido (mantém as demais com stake=0 para o cartão completo)
        if cfg.top_n and cfg.top_n > 0:
            # mantém todas as partidas, mas zera stake após top_n, preservando o cartão
            stakes_idx = out.index[out["stake"] > 0].tolist()
            if len(stakes_idx) > cfg.top_n:
                # zera stakes excedentes mantendo ordem
                to_zero = stakes_idx[cfg.top_n:]
                out.loc[to_zero, "stake"] = 0.0

    return out


# ========= Leitura do Consenso e Merge =========

def read_consensus(out_dir: str, debug: bool=False) -> Optional[pd.DataFrame]:
    p = os.path.join(out_dir, "odds_consensus.csv")
    if not os.path.exists(p):
        return None
    try:
        df = pd.read_csv(p)
    except Exception as e:
        if debug:
            print(f"[kelly] erro lendo odds_consensus.csv: {e}")
        return None
    df = normalize_basic_cols(df)
    df = _try_map_price_aliases(df)
    df = _try_map_prob_aliases(df)
    return df


# ========= Main =========

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rodada", required=False, default=os.environ.get("RODADA", "").strip())
    parser.add_argument("--debug", action="store_true", default=(os.environ.get("DEBUG","").lower() == "true"))
    parser.add_argument("--bankroll", type=float, default=float(os.environ.get("BANKROLL", 1000)))
    parser.add_argument("--kelly_fraction", type=float, default=float(os.environ.get("KELLY_FRACTION", 0.5)))
    parser.add_argument("--kelly_cap", type=float, default=float(os.environ.get("KELLY_CAP", 0.10)))
    parser.add_argument("--min_stake", type=float, default=float(os.environ.get("MIN_STAKE", 0)))
    parser.add_argument("--max_stake", type=float, default=float(os.environ.get("MAX_STAKE", 0)))
    parser.add_argument("--round_to", type=float, default=float(os.environ.get("ROUND_TO", 1)))
    parser.add_argument("--top_n", type=int, default=int(os.environ.get("KELLY_TOP_N", 14)))
    args = parser.parse_args()

    if not args.rodada:
        print("ERRO: --rodada obrigatório (ou RODADA no ambiente)", file=sys.stderr)
        sys.exit(2)

    out_dir = os.path.join("data","out", args.rodada)
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(os.path.join(out_dir, "debug"), exist_ok=True)

    cfg = KellyConfig(
        bankroll=args.bankroll,
        kelly_fraction=args.kelly_fraction,
        kelly_cap=args.kelly_cap,
        min_stake=args.min_stake,
        max_stake=args.max_stake,
        round_to=args.round_to,
        top_n=args.top_n,
    )
    print(f"[kelly] config: {json.dumps(cfg.__dict__, ensure_ascii=False)}")
    print(f"[kelly] out_dir: {out_dir}")

    # 1) tenta consenso
    consensus = read_consensus(out_dir, debug=args.debug)
    mapping_final = {"team_home":"team_home","team_away":"team_away","match_key":"match_key",
                     "prob_home":None,"prob_draw":None,"prob_away":None,"odds_home":None,"odds_draw":None,"odds_away":None}
    if consensus is not None:
        # checa quais colunas vieram
        for k in ["prob_home","prob_draw","prob_away","odds_home","odds_draw","odds_away"]:
            mapping_final[k] = k if k in consensus.columns else None
    print(f"[kelly] mapeamento de colunas final: {json.dumps(mapping_final, ensure_ascii=False)}")

    if consensus is not None:
        # echo do que foi detectado
        det_lines = []
        for k in ["team_home","team_away","match_key","prob_home","prob_draw","prob_away","odds_home","odds_draw","odds_away"]:
            det_lines.append(f"   -  {k}: {k if k in consensus.columns else 'None'}")
        print("[kelly] mapeamento de colunas detectado:\n" + "\n".join(det_lines))
    else:
        print("[kelly] AVISO: odds_consensus.csv inexistente.")

    # 2) se odds faltarem no consenso → tenta fallback provider
    df_work: Optional[pd.DataFrame] = consensus
    have_odds = df_work is not None and all(c in df_work.columns for c in ["odds_home","odds_draw","odds_away"])
    if not have_odds:
        prov = load_odds_from_provider(out_dir, debug=args.debug)
        if prov is not None and not prov.empty:
            if df_work is None:
                df_work = prov
            else:
                # left join por __join_key
                c_j = normalize_basic_cols(df_work.copy())
                p_j = normalize_basic_cols(prov.copy())
                merged = pd.merge(
                    c_j,
                    p_j[["__join_key","odds_home","odds_draw","odds_away"]],
                    on="__join_key",
                    how="left",
                    suffixes=("", "_prov")
                )
                # se colunas odds_* originais não existem, usa _prov
                for c in ["odds_home","odds_draw","odds_away"]:
                    if c not in merged.columns or merged[c].isna().all():
                        alt = f"{c}_prov"
                        if alt in merged.columns:
                            merged[c] = merged[c].fillna(merged[alt])
                df_work = merged
                if args.debug:
                    print("[kelly] odds fallback aplicado (merge por __join_key).")
        else:
            print("[kelly] provider odds_theoddsapi.csv sem colunas odds_* após normalização; ignorando.")

    # 3) Ainda sem odds? Não há o que fazer.
    if df_work is None or not any(c in df_work.columns for c in ["odds_home","odds_draw","odds_away"]):
        print("[kelly] AVISO: odds ausentes após fallback — gerando arquivo vazio.")
        out_empty = os.path.join(out_dir, "kelly_stakes.csv")
        pd.DataFrame(columns=[
            "match_key","team_home","team_away","pick","prob_pick","odds_pick","kelly_full","stake","edge"
        ]).to_csv(out_empty, index=False)
        return

    # 4) Computa Kelly
    picks = compute_kelly_rows(df_work, cfg, debug=args.debug)

    # 5) Salva
    out_path = os.path.join(out_dir, "kelly_stakes.csv")
    picks.to_csv(out_path, index=False)
    if args.debug:
        print(f"[kelly] OK -> {out_path} ({len(picks)} linhas)")

if __name__ == "__main__":
    main()
