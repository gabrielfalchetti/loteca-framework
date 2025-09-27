# scripts/ingest_odds_apifootball_rapidapi.py
# Coleta odds da API-Football (RapidAPI) com retry/backoff e tolerância a falhas.
# - Lê data/out/<rodada>/matches.csv (match_id, home, away[,date])
# - Busca fixture_id por data (ou janela de dias) e depois odds (h2h)
# - Salva em data/out/<rodada>/odds_apifootball.csv
# Robustez:
#   * Retries com backoff exponencial para erros 5xx/429/timeout
#   * --allow-partial permite seguir mesmo com alguns jogos sem odds
#   * Se não houver 'home/away' no retorno de odds, salvamos ao menos match_id/odds

from __future__ import annotations
import argparse
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
from rapidfuzz import fuzz, process as rf_process

API_HOST = "api-football-v1.p.rapidapi.com"
BASE_URL = f"https://{API_HOST}/v3"  # v3 endpoints

# ------------------------- HTTP com retry -------------------------
def api_get(path: str,
            params: Dict[str, Any],
            *,
            retries: int = 5,
            timeout: int = 20,
            backoff: float = 1.7) -> Dict[str, Any]:
    """
    GET com retries e backoff para 5xx/429/timeouts. Retorna dict já com 'response' (ou {}).
    Levanta RuntimeError só se esgotar tentativas E for erro não tolerável.
    """
    key = os.environ.get("RAPIDAPI_KEY", "").strip()
    if not key:
        raise RuntimeError("[apifootball] RAPIDAPI_KEY ausente no ambiente.")
    url = BASE_URL + path
    headers = {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": API_HOST
    }
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            status = r.status_code
            if status == 200:
                try:
                    return r.json()
                except Exception as e:
                    last_err = RuntimeError(f"[apifootball] JSON inválido em {path}: {e}")
            elif status in (429, 500, 502, 503, 504):
                # Respeita Retry-After se presente
                retry_after = r.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except Exception:
                        sleep_s = backoff ** attempt
                else:
                    sleep_s = backoff ** attempt
                print(f"[apifootball] HTTP {status} em {path} (tentativa {attempt}/{retries}); "
                      f"pausando {sleep_s:.1f}s e re-tentando…")
                time.sleep(sleep_s)
            else:
                # Erro não recuperável
                txt = r.text[:300].replace("\n", " ")
                last_err = RuntimeError(f"[apifootball] GET {path} HTTP {status}: {txt}")
                break
        except requests.exceptions.Timeout:
            sleep_s = backoff ** attempt
            print(f"[apifootball] Timeout em {path} (tentativa {attempt}/{retries}); "
                  f"pausando {sleep_s:.1f}s e re-tentando…")
            time.sleep(sleep_s)
        except requests.exceptions.RequestException as e:
            # Outras falhas de rede – retry
            sleep_s = backoff ** attempt
            print(f"[apifootball] Erro de rede em {path}: {e} (tentativa {attempt}/{retries}); "
                  f"pausando {sleep_s:.1f}s e re-tentando…")
            time.sleep(sleep_s)
    if last_err:
        raise last_err
    return {"response": []}

# ------------------------- Util / Normalização -------------------------
def norm_name(s: str) -> str:
    return (s or "").strip().lower()

def best_team_match(candidates: List[str], target: str) -> Tuple[str, int]:
    if not candidates:
        return "", 0
    res = rf_process.extractOne(target, candidates, scorer=fuzz.WRatio)
    # res: (match_string, score, index)
    if res is None:
        return "", 0
    return res[0], int(res[1])

# ------------------------- Endpoints -------------------------
def fixtures_by_date(date_iso: str, league: Optional[int]=None, season: Optional[int]=None) -> List[Dict[str,Any]]:
    params = {"date": date_iso}
    if league: params["league"] = league
    if season: params["season"] = season
    js = api_get("/fixtures", params)
    return js.get("response", [])

def odds_by_fixture(fid: int) -> List[Dict[str,Any]]:
    js = api_get("/odds", {"fixture": fid})
    return js.get("response", [])

# ------------------------- Matching fixture -------------------------
def find_fixture_id(date_iso: str,
                    home: str,
                    away: str,
                    season_year: Optional[int],
                    league_hint: Optional[int]=None) -> Optional[int]:
    """
    Busca fixture por data (mesmo dia) e faz matching fuzzy com nomes de times.
    Retorna fixture_id ou None.
    """
    resp = fixtures_by_date(date_iso, league=league_hint, season=season_year)
    if not resp:
        return None

    # lista de candidatos (home-away) com ids
    cands = []
    for item in resp:
        try:
            fid = int(item["fixture"]["id"])
            th = item["teams"]["home"]["name"]
            ta = item["teams"]["away"]["name"]
            if not th or not ta:
                continue
            cands.append((fid, th, ta))
        except Exception:
            continue

    if not cands:
        return None

    target_home = norm_name(home)
    target_away = norm_name(away)
    # lista de nomes canônicos para fuzzy
    list_home = [norm_name(x[1]) for x in cands]
    list_away = [norm_name(x[2]) for x in cands]

    best_h, score_h = best_team_match(list_home, target_home)
    best_a, score_a = best_team_match(list_away, target_away)

    # Escolhe o candidato que melhor bate simultaneamente
    best_idx = None
    best_sum = -1
    for idx, (fid, th, ta) in enumerate(cands):
        if norm_name(th) == best_h and norm_name(ta) == best_a:
            sc = score_h + score_a
            if sc > best_sum:
                best_sum = sc
                best_idx = idx

    if best_idx is None:
        # fallback: pega o par com maior soma de similaridade
        for idx, (fid, th, ta) in enumerate(cands):
            sc = fuzz.WRatio(norm_name(th), target_home) + fuzz.WRatio(norm_name(ta), target_away)
            if sc > best_sum:
                best_sum = sc
                best_idx = idx

    if best_idx is None:
        return None

    fid = int(cands[best_idx][0])
    return fid

# ------------------------- odds -> linha H2H -------------------------
def extract_h2h_row(odds_resp: List[Dict[str,Any]],
                    match_id: int,
                    home: str,
                    away: str) -> Optional[Dict[str,Any]]:
    """
    Varre odds_resp para mercado 'Match Winner'/'1x2' e retorna melhor linha agregada (média) do mercado H2H.
    """
    rows = []
    for item in odds_resp:
        try:
            bookmaker = item["bookmaker"]["name"]
            for m in item.get("bets", []) or item.get("markets", []):
                # API-Football costuma usar keys diferentes conforme plano.
                name = m.get("name") or m.get("key") or ""
                name = (name or "").lower()
                if "match winner" in name or "1x2" in name or name == "h2h":
                    for v in m.get("values", []) or m.get("outcomes", []):
                        val_name = (v.get("value") or v.get("name") or "").strip().lower()
                        odd = v.get("odd") or v.get("price") or v.get("decimal") or None
                        if odd is None:
                            continue
                        try:
                            odd = float(odd)
                        except Exception:
                            continue
                        if odd <= 1.0:
                            continue
                        # mapear rotulos
                        if val_name in ("home", "1", "1 (home)"):
                            rows.append((bookmaker, odd, None, None))
                        elif val_name in ("draw", "x", "draw (x)"):
                            rows.append((bookmaker, None, odd, None))
                        elif val_name in ("away", "2", "2 (away)"):
                            rows.append((bookmaker, None, None, odd))
        except Exception:
            continue

    if not rows:
        return None

    # Agrega por bookmaker: completa trincas (Home,Draw,Away)
    by_bm: Dict[str, Dict[str, float]] = {}
    for bm, oh, od, oa in rows:
        d = by_bm.setdefault(bm, {})
        if oh is not None: d["odd_home"] = oh
        if od is not None: d["odd_draw"] = od
        if oa is not None: d["odd_away"] = oa

    # Constrói linhas válidas
    lines = []
    for bm, d in by_bm.items():
        if all(k in d for k in ("odd_home","odd_draw","odd_away")):
            lines.append({"bookmaker": bm, **d})

    if not lines:
        return None

    df = pd.DataFrame(lines)
    # Média simples por coluna (sem devig aqui; consenso fará o devig Shin depois)
    oh = float(df["odd_home"].mean())
    od = float(df["odd_draw"].mean())
    oa = float(df["odd_away"].mean())
    return {
        "match_id": match_id,
        "home": home,
        "away": away,
        "bookmaker": "apifootball_avg",
        "odd_home": oh,
        "odd_draw": od,
        "odd_away": oa
    }

# ------------------------- Principal -------------------------
def main():
    ap = argparse.ArgumentParser(description="Ingestão de odds via API-Football (RapidAPI) com retries")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--country-hint", default=None, help="Ex.: 'Brazil' (apenas para diagnóstico/log)")
    ap.add_argument("--days-window", type=int, default=2, help="Janela +/- dias na busca por fixtures (por data ISO)")
    ap.add_argument("--min-match", type=int, default=85, help="limiar de similaridade de nomes 0-100 (fuzzy)")
    ap.add_argument("--season-year", type=int, default=None, help="Ex.: 2025")
    ap.add_argument("--retries", type=int, default=5, help="total de tentativas por requisição")
    ap.add_argument("--timeout", type=int, default=20, help="timeout por requisição (segundos)")
    ap.add_argument("--allow-partial", action="store_true", help="não aborta se algum jogo ficar sem odds")
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    base.mkdir(parents=True, exist_ok=True)

    matches_path = base / "matches.csv"
    if not matches_path.exists() or matches_path.stat().st_size == 0:
        raise RuntimeError(f"[apifootball] matches.csv ausente/vazio: {matches_path}")

    dfm = pd.read_csv(matches_path).rename(columns=str.lower)
    need = {"match_id","home","away"}
    if not need.issubset(dfm.columns):
        raise RuntimeError(f"[apifootball] matches.csv precisa conter colunas: {sorted(list(need))}")

    # data opcional
    has_date = "date" in dfm.columns
    if not has_date:
        print("[apifootball] aviso: coluna 'date' não está presente; buscando apenas por data da rodada, se fornecida externamente.")

    rows_out = []
    missing = []

    # Funções locais com retries customizados
    def safe_fixtures_by_date(dstr: str) -> List[Dict[str,Any]]:
        return api_get("/fixtures", {"date": dstr}, retries=args.retries, timeout=args.timeout).get("response", [])

    def safe_odds_by_fixture(fid: int) -> List[Dict[str,Any]]:
        return api_get("/odds", {"fixture": fid}, retries=args.retries, timeout=args.timeout).get("response", [])

    # Busca por cada jogo
    for _, r in dfm.iterrows():
        mid = int(r["match_id"])
        home = str(r["home"])
        away = str(r["away"])
        date_iso = str(r["date"]) if has_date and pd.notna(r["date"]) else None

        fid = None
        tried_dates = []

        # 1) tenta a própria data (se existir)
        if date_iso and len(date_iso) >= 10:
            d0 = date_iso[:10]
            tried_dates.append(d0)
            try:
                fid = find_fixture_id(d0, home, away, args.season_year, None)
            except Exception as e:
                print(f"[apifootball] falha ao buscar fixture {mid} em {d0}: {e}")

        # 2) tenta datas na janela +/- days_window
        if fid is None and date_iso and len(date_iso) >= 10 and args.days_window > 0:
            from datetime import datetime, timedelta
            d0 = datetime.fromisoformat(date_iso[:10])
            for delta in range(1, args.days_window+1):
                for sign in (-1, +1):
                    dtry = (d0 + timedelta(days=sign*delta)).strftime("%Y-%m-%d")
                    if dtry in tried_dates:  # evitar repetição
                        continue
                    tried_dates.append(dtry)
                    try:
                        fid = find_fixture_id(dtry, home, away, args.season_year, None)
                        if fid is not None:
                            break
                    except Exception as e:
                        print(f"[apifootball] falha ao buscar fixture {mid} em {dtry}: {e}")
                if fid is not None:
                    break

        if fid is None:
            msg = f"[apifootball] fixture não encontrado para match_id={mid} ({home} vs {away}); datas tentadas={tried_dates or ['(nenhuma)']}"
            print(msg)
            missing.append(mid)
            continue

        # Coleta odds do fixture
        try:
            odds_resp = safe_odds_by_fixture(fid)
        except RuntimeError as e:
            # se a API der 504/5xx mesmo após retries, trata conforme allow-partial
            print(f"[apifootball] erro ao buscar odds do fixture {fid} (match_id={mid}): {e}")
            if args.allow_partial:
                missing.append(mid)
                continue
            else:
                raise

        row = extract_h2h_row(odds_resp, match_id=mid, home=home, away=away)
        if row is None:
            print(f"[apifootball] sem H2H para match_id={mid} (fixture {fid})")
            missing.append(mid)
            continue

        rows_out.append(row)

    # saída
    out_path = base / "odds_apifootball.csv"
    if not rows_out:
        msg = "[apifootball] Nenhuma odd coletada após tentativas."
        if args.allow_partial:
            # grava arquivo vazio com cabeçalho para não quebrar
            pd.DataFrame(columns=["match_id","home","away","bookmaker","odd_home","odd_draw","odd_away"]).to_csv(out_path, index=False)
            print(msg)
            return
        else:
            raise RuntimeError(msg)

    pd.DataFrame(rows_out).to_csv(out_path, index=False)
    print(f"[apifootball] OK -> {out_path} (n={len(rows_out)})")
    if missing:
        print(f"[apifootball] Aviso: sem odds para match_id: {sorted(set(missing))}")

if __name__ == "__main__":
    main()
