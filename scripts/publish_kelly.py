from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import asdict
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ----------------------------------------------------------------------
# Import robusto do módulo kelly:
# 1) tenta via pacote (se scripts/ é pacote)
# 2) fallback via importlib com registro em sys.modules (evita bug no @dataclass)
# ----------------------------------------------------------------------
try:
    from scripts.kelly import KellyConfig, stake_from_kelly  # type: ignore
except ModuleNotFoundError:
    import importlib.util

    ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    kelly_path = os.path.join(ROOT, "scripts", "kelly.py")
    spec = importlib.util.spec_from_file_location("scripts.kelly", kelly_path)
    if spec is None or spec.loader is None:
        raise
    kelly = importlib.util.module_from_spec(spec)
    # >>> Registro necessário para @dataclass funcionar bem
    kelly.__package__ = "scripts"
    sys.modules["scripts.kelly"] = kelly
    # -----------------------------------------------------
    spec.loader.exec_module(kelly)  # type: ignore
    KellyConfig = kelly.KellyConfig
    stake_from_kelly = kelly.stake_from_kelly


def env_float(name: str, default: float) -> float:
    v = os.environ.get(name, "")
    try:
        return float(v) if str(v).strip() != "" else default
    except Exception:
        return default


def env_int(name: str, default: int) -> int:
    v = os.environ.get(name, "")
    try:
        return int(v) if str(v).strip() != "" else default
    except Exception:
        return default


def build_config_from_env() -> KellyConfig:
    return KellyConfig(
        bankroll=env_float("BANKROLL", 1000.0),
        kelly_fraction=env_float("KELLY_FRACTION", 0.5),
        kelly_cap=env_float("KELLY_CAP", 0.10),
        min_stake=env_float("MIN_STAKE", 0.0),
        max_stake=env_float("MAX_STAKE", 0.0),
        round_to=env_float("ROUND_TO", 1.0),
        top_n=env_int("KELLY_TOP_N", 14),
    )


def read_input_df(out_dir: str, debug: bool = False) -> pd.DataFrame:
    """
    Lê o melhor arquivo disponível:
      1) odds_consensus.csv
      2) odds_theoddsapi.csv (fallback)
    Retorna DataFrame (pode ser vazio).
    """
    candidates = [
        os.path.join(out_dir, "odds_consensus.csv"),
        os.path.join(out_dir, "odds_theoddsapi.csv"),
    ]
    for p in candidates:
        if os.path.isfile(p):
            if debug:
                print(f"[kelly] lendo: {p}")
            try:
                return pd.read_csv(p)
            except Exception as e:
                print(f"[kelly] ERRO lendo {p}: {e}")
    if debug:
        print("[kelly] nenhum CSV de odds encontrado; retornando DF vazio.")
    return pd.DataFrame()


def _find_columns(df: pd.DataFrame, debug: bool = False) -> Dict[str, Optional[str]]:
    """
    Descobre colunas para 1X2 (probabilidades e odds) em formatos comuns.
    Tenta várias convenções para minimizar acoplamento.
    Retorna dict com chaves:
      team_home, team_away, match_key,
      prob_home, prob_draw, prob_away,
      odds_home, odds_draw, odds_away
    Qualquer chave pode ser None (será tratada depois).
    """
    cols = {c.lower(): c for c in df.columns}

    def pick(*options: str) -> Optional[str]:
        for o in options:
            if o in cols:
                return cols[o]
        return None

    mapping = {
        "team_home": pick("home", "time_casa", "team_home", "home_team", "mandante"),
        "team_away": pick("away", "time_fora", "team_away", "away_team", "visitante"),
        "match_key": pick("match_id", "id_partida", "fixture_id", "game_id", "partida", "match"),
        "prob_home": pick("prob_home", "p_home", "ph", "prob_1", "prob_casa"),
        "prob_draw": pick("prob_draw", "p_draw", "pd", "prob_x", "prob_empate"),
        "prob_away": pick("prob_away", "p_away", "pa", "prob_2", "prob_fora"),
        "odds_home": pick("odds_home", "o_home", "oh", "odds_1", "odds_casa", "price_home", "decimal_home"),
        "odds_draw": pick("odds_draw", "o_draw", "od", "odds_x", "odds_empate", "price_draw", "decimal_draw"),
        "odds_away": pick("odds_away", "o_away", "oa", "odds_2", "odds_fora", "price_away", "decimal_away"),
    }

    # Heurística extra: muitas planilhas usam "prob_H/D/A" e "odds_H/D/A".
    if mapping["prob_home"] is None:
        mapping["prob_home"] = pick("prob_h")
    if mapping["prob_draw"] is None:
        mapping["prob_draw"] = pick("prob_d", "prob_x")
    if mapping["prob_away"] is None:
        mapping["prob_away"] = pick("prob_a")

    if mapping["odds_home"] is None:
        mapping["odds_home"] = pick("odds_h", "price_h", "decimal_h")
    if mapping["odds_draw"] is None:
        mapping["odds_draw"] = pick("odds_d", "odds_x", "price_d", "decimal_d")
    if mapping["odds_away"] is None:
        mapping["odds_away"] = pick("odds_a", "price_a", "decimal_a")

    if debug:
        print("[kelly] mapeamento de colunas detectado:")
        for k, v in mapping.items():
            print(f"   - {k:>10}: {v}")

    return mapping


def _safe_float(x) -> Optional[float]:
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def compute_kelly_rows(df: pd.DataFrame, cfg: KellyConfig, debug: bool = False) -> pd.DataFrame:
    """
    Para cada jogo, avalia H/D/A, escolhe o melhor edge e calcula stake.
    Retorna tabela com uma linha por jogo (melhor seleção).
    """
    if df.empty:
        return pd.DataFrame(columns=[
            "match_key", "home", "away",
            "pick", "prob", "odds", "edge", "kelly_full", "stake",
            "implied_odds", "obs"
        ])

    mapping = _find_columns(df, debug=debug)
    home_col = mapping["team_home"] or "home"
    away_col = mapping["team_away"] or "away"

    out_rows: List[Dict] = []

    # Prepara nomes seguros
    if home_col not in df.columns:
        df[home_col] = ""
    if away_col not in df.columns:
        df[away_col] = ""
    match_key_col = mapping["match_key"]
    if match_key_col is None:
        # cria uma chave sintética
        match_key_col = "__match_key__"
        df[match_key_col] = [f"m{i+1}" for i in range(len(df))]

    # Probabilidades e odds
    ph_col, pd_col, pa_col = mapping["prob_home"], mapping["prob_draw"], mapping["prob_away"]
    oh_col, od_col, oa_col = mapping["odds_home"], mapping["odds_draw"], mapping["odds_away"]

    # Se faltar odds, não tem como apostar — marca observação e segue
    for _, row in df.iterrows():
        home = str(row.get(home_col, "") or "")
        away = str(row.get(away_col, "") or "")
        mkey = str(row.get(match_key_col, "") or "")

        # Coleta probabilidades
        ph = _safe_float(row.get(ph_col)) if ph_col else None
        pd_ = _safe_float(row.get(pd_col)) if pd_col else None
        pa = _safe_float(row.get(pa_col)) if pa_col else None

        # Coleta odds
        oh = _safe_float(row.get(oh_col)) if oh_col else None
        od = _safe_float(row.get(od_col)) if od_col else None
        oa = _safe_float(row.get(oa_col)) if oa_col else None

        candidates: List[Tuple[str, Optional[float], Optional[float]]] = [
            ("H", ph, oh),
            ("D", pd_, od),
            ("A", pa, oa),
        ]

        best = None  # (pick, prob, odds, stake, kfull, edge)
        for pick, p, o in candidates:
            stake, kfull, edge = stake_from_kelly(p or 0.0, o or 0.0, cfg)
            if best is None or (edge is not None and (best[5] if best else -1e9) < edge):
                best = (pick, p or 0.0, o or 0.0, stake, kfull, edge)

        pick, p, o, stake, kfull, edge = best
        implied = (1.0 / o) if (o and o > 0) else None

        obs = ""
        if (oh is None and od is None and oa is None) or (o is None or o <= 1.0):
            obs = "odds ausentes/invalidas"
        elif p is None or p <= 0 or p >= 1:
            obs = "probabilidade ausente/invalida"
        elif stake <= 0:
            obs = "kelly <= 0 ou cap/rounding zerou"

        out_rows.append({
            "match_key": mkey,
            "home": home,
            "away": away,
            "pick": pick,               # H / D / A
            "prob": round(p, 6) if p is not None else None,
            "odds": round(o, 6) if o is not None else None,
            "edge": round(edge, 6) if edge is not None else None,
            "kelly_full": round(kfull, 6) if kfull is not None else None,
            "stake": float(stake),
            "implied_odds": round(implied, 6) if implied else None,
            "obs": obs
        })

    out = pd.DataFrame(out_rows)

    # Ordena por stake desc (ou edge, se stake for tudo zero)
    if (out["stake"] > 0).any():
        out = out.sort_values(["stake", "edge"], ascending=[False, False])
    else:
        out = out.sort_values(["edge", "prob"], ascending=[False, False])

    return out.reset_index(drop=True)


def save_artifacts(out_dir: str, rodada: str, picks_df: pd.DataFrame, cfg: KellyConfig, debug: bool = False) -> Dict:
    os.makedirs(out_dir, exist_ok=True)

    stakes_path = os.path.join(out_dir, "stakes_kelly.csv")
    final_path = os.path.join(out_dir, "picks_final_kelly.csv")
    report_path = os.path.join(out_dir, "kelly_report.json")

    # 1) todas as linhas (uma por jogo)
    picks_df.to_csv(stakes_path, index=False, quoting=csv.QUOTE_MINIMAL)

    # 2) top N (por stake)
    if "stake" in picks_df.columns and (picks_df["stake"] > 0).any():
        top = picks_df.sort_values(["stake", "edge"], ascending=[False, False]).head(max(cfg.top_n, 1))
    else:
        top = picks_df.sort_values(["edge", "prob"], ascending=[False, False]).head(max(cfg.top_n, 1))
    top.to_csv(final_path, index=False, quoting=csv.QUOTE_MINIMAL)

    # 3) relatório
    total_stake = float(picks_df["stake"].sum()) if "stake" in picks_df.columns else 0.0
    report = {
        "rodada": rodada,
        "outputs": {
            "stakes_kelly_csv": os.path.relpath(stakes_path),
            "picks_final_kelly_csv": os.path.relpath(final_path),
            "kelly_report_json": os.path.relpath(report_path),
        },
        "config": asdict(cfg),
        "summary": {
            "n_matches": int(len(picks_df)),
            "n_positive_stakes": int((picks_df["stake"] > 0).sum() if "stake" in picks_df.columns else 0),
            "total_stake": total_stake,
        },
    }

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    if debug:
        print(f"[kelly] salvo: {stakes_path}")
        print(f"[kelly] salvo: {final_path}")
        print(f"[kelly] salvo: {report_path}")
        print(f"[kelly] total_stake: {total_stake:.2f}")

    return report


def main():
    parser = argparse.ArgumentParser(description="Publica stakes baseadas em Kelly a partir de odds/probabilidades 1X2.")
    parser.add_argument("--rodada", required=True, help="Identificador da rodada (ex.: 2025-09-27_1213)")
    parser.add_argument("--debug", action="store_true", help="Log detalhado")
    args = parser.parse_args()

    cfg = build_config_from_env()
    out_dir = os.path.join("data", "out", args.rodada)

    if args.debug:
        print("[kelly] config:", json.dumps(asdict(cfg), ensure_ascii=False))
        print(f"[kelly] out_dir: {out_dir}")

    df = read_input_df(out_dir, debug=args.debug)
    picks = compute_kelly_rows(df, cfg, debug=args.debug)
    save_artifacts(out_dir, args.rodada, picks, cfg, debug=args.debug)


if __name__ == "__main__":
    main()