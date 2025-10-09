#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
feature_join_context.py

Objetivo:
- Unir sinais de contexto (weather, injuries, news, features uni/bivariado, xg etc.)
  em um único arquivo: <OUT_DIR>/context_features.csv
- Ser resiliente: NUNCA quebrar a pipeline se um insumo estiver ausente ou vazio.
- Padronizar tipos e chaves (match_id string) para evitar erros de merge.

Entrada mínima:
  - <OUT_DIR>/matches_whitelist.csv  (obrigatório; base dos jogos)
    colunas aceitas: [match_id, match_key, team_home|home, team_away|away]

Entradas opcionais (todas tolerantes a ausência/vazio):
  - <OUT_DIR>/features_univariado.csv
  - <OUT_DIR>/features_bivariado.csv
  - <OUT_DIR>/features_xg.csv
  - <OUT_DIR>/weather.csv
  - <OUT_DIR>/injuries.csv      (se existir, contabiliza contagens por time)
  - <OUT_DIR>/news.csv          (se existir, contagem simples por time)
  - <OUT_DIR>/odds_consensus.csv (para apoio e consistência de nomes)

Saída:
  - <OUT_DIR>/context_features.csv com, no mínimo:
      match_id,home,away,context_score
    e colunas auxiliares úteis ao debug.

Uso:
  python scripts/feature_join_context.py --rodada data/out/<RID> [--debug]

"""

import argparse
import os
import sys
import math
from typing import Optional, List

import pandas as pd


# -------------------- Utilidades -------------------- #

def dbg(enabled: bool, *args):
    if enabled:
        print("[context]", *args, flush=True)

def exists_nonempty(path: str) -> bool:
    return os.path.isfile(path) and os.path.getsize(path) > 0

def read_csv_safe(path: str, debug: bool) -> Optional[pd.DataFrame]:
    try:
        if exists_nonempty(path):
            df = pd.read_csv(path)
            if df is not None and not df.empty:
                return df
            dbg(debug, f"[AVISO] Arquivo vazio: {path}")
            return None
        else:
            dbg(debug, f"[AVISO] Arquivo ausente/vazio: {path}")
            return None
    except Exception as e:
        dbg(debug, f"[ERRO] Falha ao ler {path}: {e}")
        return None

def get_first_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def as_str_series(s):
    return s.astype(str) if s is not None else None

def safe_minmax(x: pd.Series) -> pd.Series:
    """Escalonamento simples [0,1]; se constante ou NaN, vira 0."""
    try:
        x = x.astype(float)
        vmin = x.min()
        vmax = x.max()
        if not math.isfinite(vmin) or not math.isfinite(vmax) or vmax <= vmin:
            return pd.Series(0.0, index=x.index)
        return (x - vmin) / (vmax - vmin)
    except Exception:
        return pd.Series(0.0, index=x.index)

def to_float(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def ensure_match_id_str(df: pd.DataFrame, col: str = "match_id") -> pd.DataFrame:
    if col in df.columns:
        df[col] = df[col].astype(str)
    return df


# -------------------- Carga Base -------------------- #

def load_base(out_dir: str, debug: bool) -> pd.DataFrame:
    base_path = os.path.join(out_dir, "matches_whitelist.csv")
    dfb = read_csv_safe(base_path, debug)
    if dfb is None:
        print(f"##[error]Arquivo obrigatório não encontrado: {base_path}", file=sys.stderr)
        sys.exit(28)

    # normaliza nomes
    ch = get_first_col(dfb, ["team_home", "home"])
    ca = get_first_col(dfb, ["team_away", "away"])
    if ch is None or ca is None:
        # tenta odds_consensus para mapear nomes se necessário
        oc = read_csv_safe(os.path.join(out_dir, "odds_consensus.csv"), debug)
        if oc is not None:
            oc_h = get_first_col(oc, ["team_home", "home"])
            oc_a = get_first_col(oc, ["team_away", "away"])
            if oc_h and oc_a:
                # fallback: deriver nomes pelo match_id se coincidir
                pass

    # garante colunas padrão home/away
    if "home" not in dfb.columns:
        if "team_home" in dfb.columns:
            dfb = dfb.rename(columns={"team_home": "home"})
    if "away" not in dfb.columns:
        if "team_away" in dfb.columns:
            dfb = dfb.rename(columns={"team_away": "away"})

    # garante match_id
    if "match_id" not in dfb.columns:
        # derive de match_key se existir
        if "match_key" in dfb.columns:
            dfb["match_id"] = dfb["match_key"].astype(str)
        else:
            # cria usando home__away
            dfb["match_id"] = (dfb["home"].astype(str) + "__" + dfb["away"].astype(str))

    dfb = ensure_match_id_str(dfb)
    keep = ["match_id", "home", "away"]
    dfb = dfb[keep].copy()
    dbg(debug, f"Base (whitelist) linhas={len(dfb)}")
    return dfb


# -------------------- Cargas Opcionais -------------------- #

def load_weather(out_dir: str, debug: bool) -> Optional[pd.DataFrame]:
    w = read_csv_safe(os.path.join(out_dir, "weather.csv"), debug)
    if w is None:
        return None
    w = ensure_match_id_str(w)
    needed = [
        "match_id","lat","lon","temp_c","apparent_temp_c","wind_speed_kph",
        "wind_gust_kph","wind_dir_deg","precip_mm","precip_prob",
        "relative_humidity","cloud_cover","pressure_hpa","weather_source","fetched_at_utc"
    ]
    for c in needed:
        if c not in w.columns:
            w[c] = pd.NA
    return w[needed].copy()

def load_uni(out_dir: str, debug: bool) -> Optional[pd.DataFrame]:
    f = read_csv_safe(os.path.join(out_dir, "features_univariado.csv"), debug)
    if f is None:
        return None
    # chave: match_key normalmente é "home__vs__away"; preferimos derivar por home/away
    ch = get_first_col(f, ["home","team_home"])
    ca = get_first_col(f, ["away","team_away"])
    if ch and ca and "match_id" not in f.columns:
        f["match_id"] = f[ch].astype(str) + "__" + f[ca].astype(str)
    f = ensure_match_id_str(f)
    # seleciona algumas colunas úteis
    cols = ["match_id","fair_p_home","fair_p_draw","fair_p_away","overround","entropy_bits","gap_top_second","gap_home_away"]
    for c in cols:
        if c not in f.columns:
            f[c] = pd.NA
    return to_float(f, cols)

def load_bi(out_dir: str, debug: bool) -> Optional[pd.DataFrame]:
    f = read_csv_safe(os.path.join(out_dir, "features_bivariado.csv"), debug)
    if f is None:
        return None
    ch = get_first_col(f, ["home","team_home"])
    ca = get_first_col(f, ["away","team_away"])
    if ch and ca and "match_id" not in f.columns:
        f["match_id"] = f[ch].astype(str) + "__" + f[ca].astype(str)
    f = ensure_match_id_str(f)
    cols = ["match_id","diff_ph_pa","ratio_ph_pa","entropy_x_gap","overround_x_entropy"]
    for c in cols:
        if c not in f.columns:
            f[c] = pd.NA
    return to_float(f, cols)

def load_xg(out_dir: str, debug: bool) -> Optional[pd.DataFrame]:
    f = read_csv_safe(os.path.join(out_dir, "features_xg.csv"), debug)
    if f is None:
        return None
    ch = get_first_col(f, ["home","team_home"])
    ca = get_first_col(f, ["away","team_away"])
    if ch and ca and "match_id" not in f.columns:
        f["match_id"] = f[ch].astype(str) + "__" + f[ca].astype(str)
    f = ensure_match_id_str(f)
    cols = ["match_id","xg_home_proxy","xg_away_proxy","xg_diff_proxy"]
    for c in cols:
        if c not in f.columns:
            f[c] = pd.NA
    return to_float(f, cols)

def load_injuries(out_dir: str, debug: bool) -> Optional[pd.DataFrame]:
    inj = read_csv_safe(os.path.join(out_dir, "injuries.csv"), debug)
    if inj is None:
        return None
    # Injuries pode não ter home/away, então mapeamos por nome do time com contagem bruta:
    # Procuramos colunas 'team' ou 'team_name'
    tc = get_first_col(inj, ["team","team_name","team_full","team_id_name"])
    if tc is None:
        return None
    # Contagem de registros por time:
    cnt = inj.groupby(inj[tc].astype(str), dropna=False).size().rename("inj_count").reset_index()
    cnt = cnt.rename(columns={tc: "team"})
    return cnt

def load_news(out_dir: str, debug: bool) -> Optional[pd.DataFrame]:
    news = read_csv_safe(os.path.join(out_dir, "news.csv"), debug)
    if news is None:
        return None
    # Contagem simples por ocorrência do nome do time no título/descrição
    text_col = get_first_col(news, ["title","description","content"])
    if text_col is None:
        return None
    news[text_col] = news[text_col].astype(str).str.lower()
    return news[[text_col]].copy()


# -------------------- Score de Contexto -------------------- #

def build_context_score(df: pd.DataFrame, debug: bool) -> pd.DataFrame:
    """
    Cria 'context_score' a partir do que existir:
      + xg_diff_proxy (quanto maior, melhor para o mandante)
      + diff_ph_pa / ratio_ph_pa / gap_top_second / entropy_x_gap (sinais de mercado)
      - inj_count_away (mais lesões no visitante = favorece mandante)
      + inj_count_home (opcionalmente negativo; aqui mantemos neutro se não houver mapeamento)
      - weather_vento(gust) muito alto e precip_mm alta (ruído → puxa score para 0.5 neutro)
    Score final em [0,1], onde >0.5 favorece HOME, <0.5 favorece AWAY.
    """
    # base neutra
    df["context_score"] = 0.5

    # Sinais (normalizados)
    signals = []

    if "xg_diff_proxy" in df.columns:
        s = safe_minmax(df["xg_diff_proxy"]).fillna(0.0)
        signals.append(("xg_diff_proxy", s, +1.0))

    for c in ["diff_ph_pa", "ratio_ph_pa", "gap_top_second", "entropy_x_gap"]:
        if c in df.columns:
            s = safe_minmax(df[c]).fillna(0.0)
            # gap_top_second e diff_ph_pa positivos favorecem HOME
            # ratio_ph_pa >1 também sugere vantagem HOME
            signals.append((c, s, +0.6 if c != "entropy_x_gap" else +0.3))

    # Injuries: se tivermos contagens, mapeadas para home/away
    if ("inj_count_home" in df.columns) and ("inj_count_away" in df.columns):
        s_home = safe_minmax(df["inj_count_home"].fillna(0.0))
        s_away = safe_minmax(df["inj_count_away"].fillna(0.0))
        # mais lesões no visitante => favorece HOME
        signals.append(("injuries_home", 1.0 - s_home, +0.2))  # menos lesões em casa é bom
        signals.append(("injuries_away", s_away, +0.2))         # mais lesões fora é bom para casa

    # Clima: rajadas e precip puxam o contexto para o meio (reduz confiança no favorito)
    # Aqui aplicamos um "penalty" que aproxima score de 0.5
    if "wind_gust_kph" in df.columns:
        wg = df["wind_gust_kph"].fillna(0.0).astype(float)
        wg_n = safe_minmax(wg)  # 0..1
        # penalty magnitude
        penalty_wind = 0.1 * wg_n
    else:
        penalty_wind = pd.Series(0.0, index=df.index)

    if "precip_mm" in df.columns:
        pr = df["precip_mm"].fillna(0.0).astype(float)
        pr_n = safe_minmax(pr)
        penalty_rain = 0.1 * pr_n
    else:
        penalty_rain = pd.Series(0.0, index=df.index)

    # Agregação de sinais (média ponderada)
    if signals:
        num = None
        den = 0.0
        for name, sig, w in signals:
            if num is None:
                num = w * sig
            else:
                num = num + w * sig
            den += abs(w)
        agg = num / den if den > 0 else pd.Series(0.0, index=df.index)
        # mapeia [0,1] para favorecer HOME (>0.5)
        df["context_score"] = 0.5 + 0.5 * (agg - 0.5) * 2  # identidade (mantém 0..1)
    else:
        df["context_score"] = 0.5

    # Aplica penalidades climáticas aproximando do neutro (0.5)
    total_penalty = (penalty_wind + penalty_rain).clip(0.0, 0.3)  # max 0.3
    df["context_score"] = 0.5 + (df["context_score"] - 0.5) * (1.0 - total_penalty)

    return df


# -------------------- Main -------------------- #

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rodada", required=True, help="Diretório da rodada (ex.: data/out/123456)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_dir = args.rodada
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "context_features.csv")

    # 1) Base de jogos
    base = load_base(out_dir, args.debug)

    # 2) Carregar opcionais
    w  = load_weather(out_dir, args.debug)
    uni = load_uni(out_dir, args.debug)
    bi  = load_bi(out_dir, args.debug)
    xg  = load_xg(out_dir, args.debug)

    # 3) Merge progressivo (sempre por match_id como string)
    df = base.copy()
    for extra, name in [(w, "weather"), (uni, "univariado"), (bi, "bivariado"), (xg, "xg")]:
        if extra is not None:
            df = df.merge(extra, on="match_id", how="left")
            dbg(args.debug, f"merge {name}: df -> {len(df)} linhas")

    # 4) Injuries: agregamos por nome do time e juntamos aos lados home/away
    inj = load_injuries(out_dir, args.debug)
    if inj is not None:
        # mapeia contagem pela coluna 'team' em inj ao nome em df.home/df.away
        inj_home = inj.rename(columns={"team": "home", "inj_count": "inj_count_home"})
        inj_away = inj.rename(columns={"team": "away", "inj_count": "inj_count_away"})
        df = df.merge(inj_home[["home", "inj_count_home"]], on="home", how="left")
        df = df.merge(inj_away[["away", "inj_count_away"]], on="away", how="left")
        dbg(args.debug, f"merge injuries: df -> {len(df)} linhas")

    # 5) News (opcional): contagem ingênua por substring do time no texto
    news = load_news(out_dir, args.debug)
    if news is not None:
        news_col = get_first_col(news, news.columns.tolist())
        text = news[news_col].astype(str)
        # Conte quantas vezes home/away aparecem (bem simples e robusto)
        df["news_hits_home"] = df["home"].astype(str).str.lower().apply(lambda t: text.str.contains(t, na=False).sum() if t and t != "nan" else 0)
        df["news_hits_away"] = df["away"].astype(str).str.lower().apply(lambda t: text.str.contains(t, na=False).sum() if t and t != "nan" else 0)
        dbg(args.debug, "news hits calculados")

    # 6) Construir context_score
    df = build_context_score(df, args.debug)

    # 7) Ordena colunas-chave para facilitar debug
    order_front = ["match_id", "home", "away", "context_score"]
    other_cols = [c for c in df.columns if c not in order_front]
    df = df[order_front + other_cols]

    # 8) Escrita segura
    df.to_csv(out_path, index=False)
    dbg(args.debug, f"OK -> {out_path} (linhas={len(df)})")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        # Nunca quebrar: escreve um CSV mínimo com base (se possível)
        out_dir = os.environ.get("OUT_DIR") or "."
        try:
            base = load_base(out_dir, debug=False)
            base["context_score"] = 0.5
            base.to_csv(os.path.join(out_dir, "context_features.csv"), index=False)
            print(f"##[warning]Context gerado no modo mínimo (erro: {e})", file=sys.stderr)
            sys.exit(0)
        except Exception as e2:
            print(f"##[error]Falha fatal no context: {e} / fallback={e2}", file=sys.stderr)
            sys.exit(28)