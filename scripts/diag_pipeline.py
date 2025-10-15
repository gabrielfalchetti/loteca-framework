# scripts/diag_pipeline.py
from __future__ import annotations

import os, sys, json, re, math, time, textwrap, unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Any, Optional

# Dependências padrão do projeto
try:
    import pandas as pd
except Exception as e:
    print(f"[diag] ERRO: pandas não disponível: {e}", file=sys.stderr)
    sys.exit(2)

try:
    import requests
except Exception as e:
    print(f"[diag] ERRO: requests não disponível: {e}", file=sys.stderr)
    sys.exit(2)

# ---------------------------- util de texto/nomes ----------------------------

_STOP = {"fc","ec","ac","sc","u20","u23","futebol","clube","club","regatas","associacao",
         "athletico","associação","esporte","sport","de","da","do","dos","das"}

def _strip_accents(s: str) -> str:
    if not isinstance(s, str):
        s = "" if s is None else str(s)
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))

def canonical_name(s: str) -> str:
    """normaliza de forma agressiva para comparação robusta."""
    s = _strip_accents(s).lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[-_/.,:;()]+", " ", s)
    s = s.replace("athletico", "atletico")  # PR, etc.
    s = s.replace("porto alegre", "")       # gremio
    s = s.replace("red bull ", "")          # bragantino
    s = s.replace("sport recife", "sport")  # comum na Odds
    s = re.sub(r"\b(atletico)\s+mineiro\b", r"\1 mg", s)
    s = re.sub(r"\b(atletico)\s+goianiense\b", r"\1 go", s)
    s = re.sub(r"\b(sao)\s*paulo\b", r"\1 paulo", s)
    s = re.sub(r"\s+", " ", s).strip()
    # token key
    toks = [t for t in s.split() if t not in _STOP]
    if not toks:
        toks = s.split()
    return " ".join(toks)

def ratio(a: str, b: str) -> float:
    """similaridade simples (token set + char ratio). 0..1"""
    if not a or not b:
        return 0.0
    ta = canonical_name(a).split()
    tb = canonical_name(b).split()
    if not ta or not tb:
        return 0.0
    set_a, set_b = set(ta), set(tb)
    inter = len(set_a & set_b)
    uni = len(set_a | set_b)
    token_score = inter / max(1, uni)
    # char level (barato)
    sa, sb = "".join(ta), "".join(tb)
    # overlap de subsequência simples
    from difflib import SequenceMatcher
    char_score = SequenceMatcher(None, sa, sb).ratio()
    return 0.6*token_score + 0.4*char_score

# -------------------------------- modelos -----------------------------------

@dataclass
class Cfg:
    regions: str
    lookahead_days: int
    source_csv: str
    source_csv_norm: str
    features_parquet: str
    auto_aliases_json: str
    out_dir: str
    theodds_api_key: str

# ------------------------------- helpers time --------------------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def to_utc(dt: Any) -> Optional[datetime]:
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    try:
        d = pd.to_datetime(dt, utc=True)
        if pd.isna(d):
            return None
        if isinstance(d, pd.Timestamp):
            return d.to_pydatetime()
        return d
    except Exception:
        return None

# ----------------------------- leitura de dados ------------------------------

def read_source_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    # colunas mínimas
    cols = {c.lower(): c for c in df.columns}
    for need in ("home","away","date"):
        if need not in {k.lower() for k in df.columns}:
            raise ValueError(f"CSV '{path}' sem coluna obrigatória: {need}")
    # normaliza nome das colunas para home/away/date
    rename = {}
    for c in df.columns:
        cl = c.lower()
        if cl in ("home_team","mandante","home"): rename[c]="home"
        if cl in ("away_team","visitante","away"): rename[c]="away"
        if cl in ("date","datetime","kickoff","start_time"): rename[c]="date"
    if rename:
        df = df.rename(columns=rename)
    # datas -> UTC
    df["date_utc"] = df["date"].apply(to_utc)
    return df

def read_aliases(path: str) -> Dict[str, str]:
    if not path or not os.path.exists(path) or os.path.getsize(path)==0:
        return {}
    try:
        return json.loads(open(path, "r", encoding="utf-8").read())
    except Exception:
        return {}

# -------------------------- TheOddsAPI comunicação ---------------------------

BASE = "https://api.the-odds-api.com/v4"

def _req(url: str, params: Dict[str, Any]) -> Tuple[Optional[Any], Dict[str,str], Optional[str]]:
    try:
        r = requests.get(url, params=params, timeout=20)
        hdr = {k.lower(): v for k,v in r.headers.items()}
        if r.status_code != 200:
            return None, hdr, f"HTTP {r.status_code}: {r.text[:200]}"
        return r.json(), hdr, None
    except Exception as e:
        return None, {}, str(e)

def get_sports(api_key: str) -> Tuple[List[Dict[str,Any]], Dict[str,str], Optional[str]]:
    return _req(f"{BASE}/sports", {"apiKey": api_key})

def get_events(api_key: str, sport_key: str, regions: str) -> Tuple[List[Dict[str,Any]], Dict[str,str], Optional[str]]:
    params = {
        "apiKey": api_key,
        "regions": regions,               # ex: "uk,eu,us,au"
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    return _req(f"{BASE}/sports/{sport_key}/events", params)

# ------------------------------ matching core --------------------------------

def match_row_to_events(row: pd.Series, evs: List[Dict[str,Any]],
                        cutoff: float = 0.82) -> Tuple[Optional[Dict[str,Any]], float, float]:
    h, a = str(row["home"]), str(row["away"])
    best = None
    best_score = -1.0
    best_pair = (0.0, 0.0)
    for ev in evs:
        eh, ea = ev.get("home_team",""), ev.get("away_team","")
        sh = ratio(h, eh)
        sa = ratio(a, ea)
        # também tente invertido (se a API por acaso invertesse)
        sh2 = ratio(h, ea)
        sa2 = ratio(a, eh)
        scores = [(sh, sa, False), (sh2, sa2, True)]
        for xh, xa, flipped in scores:
            s = min(xh, xa)
            if s > best_score:
                best_score = s
                best = dict(ev) | {"__flipped": flipped}
                best_pair = (xh, xa)
    if best and best_score >= cutoff:
        return best, best_pair[0], best_pair[1]
    return None, best_pair[0], best_pair[1]

# --------------------------------- runner ------------------------------------

def main():
    # coleta cfg de ENV (com defaults seguros)
    cfg = Cfg(
        regions=os.getenv("REGIONS","uk,eu,us,au"),
        lookahead_days=int(os.getenv("LOOKAHEAD_DAYS","3")),
        source_csv=os.getenv("SOURCE_CSV","data/in/matches_source.csv"),
        source_csv_norm=os.getenv("SOURCE_CSV_NORM","data/out/matches_norm.csv"),
        features_parquet=os.getenv("FEATURES_PARQUET","data/history/features.parquet"),
        auto_aliases_json=os.getenv("AUTO_ALIASES_JSON","data/aliases/auto_aliases.json"),
        out_dir=os.getenv("OUT_DIR", f"data/out/diag_{int(time.time())}"),
        theodds_api_key=os.getenv("THEODDS_API_KEY",""),
    )
    os.makedirs(cfg.out_dir, exist_ok=True)

    print(f"[diag] OUT_DIR={cfg.out_dir}")
    print(f"[diag] REGIONS={cfg.regions} | LOOKAHEAD_DAYS={cfg.lookahead_days}")
    print(f"[diag] SOURCE_CSV={cfg.source_csv}")
    print(f"[diag] SOURCE_CSV_NORM={cfg.source_csv_norm}")
    print(f"[diag] AUTO_ALIASES_JSON={cfg.auto_aliases_json}")

    # ler CSVs
    issues: List[str] = []
    src_df = pd.DataFrame()
    norm_df = pd.DataFrame()

    try:
        src_df = read_source_csv(cfg.source_csv)
        print(f"[diag] source_csv linhas={len(src_df)}  período≈ {src_df['date_utc'].min()} .. {src_df['date_utc'].max()}")
    except Exception as e:
        issues.append(f"Falha lendo SOURCE_CSV: {e}")

    if os.path.exists(cfg.source_csv_norm):
        try:
            norm_df = read_source_csv(cfg.source_csv_norm)
            print(f"[diag] source_csv_norm linhas={len(norm_df)} período≈ {norm_df['date_utc'].min()} .. {norm_df['date_utc'].max()}")
        except Exception as e:
            issues.append(f"Falha lendo SOURCE_CSV_NORM: {e}")
    else:
        print(f"[diag] AVISO: SOURCE_CSV_NORM não existe ({cfg.source_csv_norm})")

    aliases = read_aliases(cfg.auto_aliases_json)
    print(f"[diag] aliases carregados: {len(aliases)}")

    # janela de tempo
    now = utcnow()
    horizon = now + timedelta(days=cfg.lookahead_days)
    def in_window(d: Optional[datetime]) -> bool:
        return d is not None and now <= d <= horizon

    cand = norm_df if not norm_df.empty else src_df
    cand = cand.copy()
    if "date_utc" not in cand.columns:
        cand["date_utc"] = cand["date"].apply(to_utc)
    cand = cand[cand["date_utc"].apply(in_window)]
    print(f"[diag] jogos na janela ({cfg.lookahead_days}d): {len(cand)}")

    # ---------------------- TheOddsAPI: esportes e eventos --------------------
    sports_keys: List[str] = []
    sports_meta, sports_hdr, err = get_sports(cfg.theodds_api_key) if cfg.theodds_api_key else ([],{}, "THEODDS_API_KEY vazio")
    if err:
        issues.append(f"TheOdds /sports: {err}")
    else:
        for sp in sports_meta:
            key = sp.get("key","")
            group = (sp.get("group") or "").lower()
            title = (sp.get("title") or "").lower()
            if "brazil" in key or "brazil" in group or "brasil" in title:
                sports_keys.append(key)
        # fallback seguro caso filtro acima não encontre
        if not sports_keys:
            for k in ("soccer_brazil_serie_a","soccer_brazil_serie_b","soccer_brazil_campeonato"):
                if any(k in (sp.get("key","")) for sp in sports_meta) or k.endswith(("a","b")):
                    sports_keys.append(k)
        print(f"[diag] sports detectados p/ Brasil: {sports_keys}")
        if "x-requests-remaining" in sports_hdr:
            print(f"[diag] quota: {sports_hdr.get('x-requests-remaining')} restantes")

    all_events: List[Dict[str,Any]] = []
    ev_by_key: Dict[str, List[Dict[str,Any]]] = {}
    if cfg.theodds_api_key and sports_keys:
        for sk in sports_keys:
            evs, hdr, err2 = get_events(cfg.theodds_api_key, sk, cfg.regions)
            if err2:
                issues.append(f"/events {sk}: {err2}")
                continue
            # filtra pela janela
            kept = []
            for ev in evs or []:
                try:
                    t = to_utc(ev.get("commence_time"))
                    if in_window(t):
                        kept.append(ev)
                except Exception:
                    pass
            ev_by_key[sk] = kept
            all_events.extend(kept)
            if "x-requests-remaining" in hdr:
                print(f"[diag] quota após {sk}: {hdr.get('x-requests-remaining')}")
        print(f"[diag] eventos coletados na janela: {len(all_events)}")
    else:
        print("[diag] PULANDO coleta de eventos (sem chave ou sem sports)")

    # salva eventos crus
    with open(os.path.join(cfg.out_dir, "diag_events.json"), "w", encoding="utf-8") as f:
        json.dump(all_events, f, ensure_ascii=False, indent=2)

    # ------------------------------ matching ---------------------------------
    rows = []
    unmatched_names = set()
    for _, r in cand.iterrows():
        ali_home = aliases.get(str(r["home"]), r["home"])
        ali_away = aliases.get(str(r["away"]), r["away"])
        row = r.copy()
        row["home_eff"] = ali_home
        row["away_eff"] = ali_away
        ev, sh, sa = match_row_to_events(row, all_events, cutoff=0.82)
        if ev:
            rows.append({
                "home": r["home"], "away": r["away"],
                "home_eff": row["home_eff"], "away_eff": row["away_eff"],
                "date_utc": r["date_utc"],
                "ev_home": ev.get("home_team",""),
                "ev_away": ev.get("away_team",""),
                "score_home": round(sh,3),
                "score_away": round(sa,3),
                "ev_commence": ev.get("commence_time",""),
                "sport_key": ev.get("sport_key") or ev.get("sport_key",""),
            })
        else:
            rows.append({
                "home": r["home"], "away": r["away"],
                "home_eff": row["home_eff"], "away_eff": row["away_eff"],
                "date_utc": r["date_utc"],
                "ev_home": "", "ev_away": "",
                "score_home": round(sh,3), "score_away": round(sa,3),
                "ev_commence": "", "sport_key": "",
            })
            unmatched_names.add(canonical_name(str(row["home_eff"])))
            unmatched_names.add(canonical_name(str(row["away_eff"])))

    diag_df = pd.DataFrame(rows)
    diag_path = os.path.join(cfg.out_dir, "diag_matches.csv")
    diag_df.to_csv(diag_path, index=False, encoding="utf-8")
    matched = int((diag_df["ev_home"]!="").sum())
    total = len(diag_df)

    # sugestões de aliases
    suggestions: Dict[str,str] = {}
    for _, rr in diag_df.iterrows():
        if rr["ev_home"]:
            k1 = canonical_name(rr["ev_home"])
            v1 = rr["home_eff"]
            if k1 and v1 and k1 != canonical_name(v1):
                suggestions[rr["ev_home"]] = v1
        if rr["ev_away"]:
            k2 = canonical_name(rr["ev_away"])
            v2 = rr["away_eff"]
            if k2 and v2 and k2 != canonical_name(v2):
                suggestions[rr["ev_away"]] = v2

    sug_path = os.path.join("data","aliases","auto_aliases_suggestions.json")
    os.makedirs(os.path.dirname(sug_path), exist_ok=True)
    with open(sug_path, "w", encoding="utf-8") as f:
        json.dump(suggestions, f, ensure_ascii=False, indent=2)

    # sumário
    summary = {
        "now_utc": now.isoformat(),
        "lookahead_days": cfg.lookahead_days,
        "regions": cfg.regions,
        "source_rows": int(len(src_df)),
        "norm_rows": int(len(norm_df)) if not norm_df.empty else None,
        "in_window_rows": total,
        "events_in_window": int(len(all_events)),
        "matched_rows": matched,
        "match_rate": round(matched / max(1,total), 3),
        "sports_keys": sports_keys,
        "issues": issues,
    }
    with open(os.path.join(cfg.out_dir, "diag_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # relatório humano
    md = []
    md.append("# Diagnóstico do pipeline de odds\n")
    md.append(f"- Data (UTC): **{now:%Y-%m-%d %H:%M}**")
    md.append(f"- LOOKAHEAD_DAYS: **{cfg.lookahead_days}** | REGIONS: **{cfg.regions}**")
    md.append(f"- Jogos na janela: **{total}** | Eventos (OddsAPI) na janela: **{len(all_events)}**")
    md.append(f"- Matched: **{matched}/{total}** (taxa {summary['match_rate']*100:.1f}%)")
    md.append(f"- Sports keys BR usadas: `{', '.join(sports_keys) or 'N/D'}`")
    if issues:
        md.append("\n## Issues encontradas")
        for it in issues:
            md.append(f"- {it}")
    if unmatched_names:
        md.append("\n## Times não casados (normalizados)")
        uns = sorted(t for t in unmatched_names if t)
        md.append("```\n" + "\n".join(uns) + "\n```")
    md.append("\n## Arquivos gerados")
    md.append(f"- `{diag_path}` — tabela de match por jogo")
    md.append(f"- `{sug_path}` — sugestões de aliases automáticos")
    md.append(f"- `{os.path.join(cfg.out_dir,'diag_events.json')}` — eventos crus")
    md.append(f"- `{os.path.join(cfg.out_dir,'diag_summary.json')}` — resumo programático\n")
    with open(os.path.join(cfg.out_dir, "diag_report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(md))

    print(f"[diag] OK — matched {matched}/{total}. Relatório: {os.path.join(cfg.out_dir,'diag_report.md')}")
    # não falha o job; exit 0 sempre
    sys.exit(0)

if __name__ == "__main__":
    main()