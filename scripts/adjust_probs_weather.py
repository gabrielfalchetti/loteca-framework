# scripts/adjust_probs_weather.py
# Ajuste de probabilidades por CLIMA usando:
# - API-Football (RapidAPI) para achar o fixture e pegar venue (lat/lon/elevation)
# - Open-Meteo (sem chave) para previsão no dia do jogo
# Saída: data/out/<rodada>/joined_weather.csv com p_* ajustadas e odds recalculadas
from __future__ import annotations
import argparse, os, math
from pathlib import Path
from typing import Optional, Tuple
from datetime import date, timedelta, datetime

import requests
import pandas as pd
import numpy as np
from rapidfuzz import fuzz

# ---------- Config ----------
API_HOST = "api-football-v1.p.rapidapi.com"
API_BASE = f"https://{API_HOST}/v3"
OM_FORECAST = "https://api.open-meteo.com/v1/forecast"

# Limiares & intensidades (pode ajustar depois)
BETA_RAIN_MAX = 0.05   # até +5 pp no X com chuva forte (>=10 mm/d)
BETA_WIND_MAX = 0.04   # até +4 pp no X com vento forte (>=10 m/s ~ 36 km/h)
BETA_TEMP_MAX = 0.03   # até +3 pp no X com calor (>=32C) ou frio (<=5C)
GAMMA_ALT_HOME = 0.03  # +3% no p_home em altitude >= 1500m (se info existir)
DAYS_WINDOW_DEFAULT = 2
MIN_MATCH_DEFAULT = 85

# ---------- HTTP ----------
def _headers():
    key = os.getenv("RAPIDAPI_KEY", "").strip()
    if not key:
        raise RuntimeError("[weather] RAPIDAPI_KEY não definido (usado para fixtures/venue).")
    return {"X-RapidAPI-Key": key, "X-RapidAPI-Host": API_HOST}

def api_get(path: str, params: dict) -> dict:
    r = requests.get(f"{API_BASE}{path}", headers=_headers(), params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    if isinstance(j, dict) and j.get("errors"):
        raise RuntimeError(f"[weather] API-Football error: {j.get('errors')}")
    return j

def openmeteo_get(params: dict) -> dict:
    r = requests.get(OM_FORECAST, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

# ---------- Utils ----------
def _norm(s: str) -> str:
    s = (s or "").lower().strip()
    rep = [(" futebol clube",""),(" futebol",""),(" clube",""),(" club",""),
           (" fc",""),(" afc",""),(" sc",""),(" ac",""),(" de futebol",""),
           ("-sp",""),("-rj",""),("-mg",""),("-rs",""),("-pr",""),
           ("/sp",""),("/rj",""),("/mg",""),("/rs",""),("/pr",""),
           ("  "," ")]
    for a,b in rep: s = s.replace(a,b)
    return s

def find_team_id(name: str, country_hint: Optional[str]=None) -> Tuple[int,str,str]:
    data = api_get("/teams", {"search": name})
    res = data.get("response", [])
    if not res:
        raise RuntimeError(f"[weather] time não encontrado: {name}")
    best=(0,None,"","")
    for it in res:
        tname = it["team"]["name"]
        country = (it.get("team",{}) or {}).get("country") or ""
        score = fuzz.token_set_ratio(_norm(name), _norm(tname))
        if country_hint and country and country_hint.lower() in country.lower():
            score += 3
        if score > best[0]:
            best=(score, it["team"]["id"], tname, country)
    return int(best[1]), str(best[2]), str(best[3])

def fixtures_by_date(d: str) -> list:
    return api_get("/fixtures", {"date": d}).get("response", [])

def find_fixture_id(date_iso: str, home: str, away: str, season_year: int,
                    country_hint: Optional[str], days_window: int, min_match: int) -> Optional[int]:
    # 1) tenta H2H por IDs
    try:
        hid,_,_ = find_team_id(home, country_hint)
        aid,_,_ = find_team_id(away, country_hint)
        h2h = f"{hid}-{aid}"
        resp = api_get("/fixtures", {"h2h": h2h, "season": season_year}).get("response", [])
        def score_date(d: str) -> int:
            try:
                d0 = date.fromisoformat(date_iso); d1 = date.fromisoformat(d[:10])
                diff = abs((d1-d0).days); return 10 - diff
            except: return -999
        best=None; sbest=-999
        for it in resp:
            s = score_date((it["fixture"].get("date") or "")[:10])
            if s > sbest: best=it; sbest=s
        if best and sbest >= 8:
            return int(best["fixture"]["id"])
    except Exception:
        pass
    # 2) varredura ±window por nome
    d0 = date.fromisoformat(date_iso)
    best_id=None; best_score=-1
    for off in range(-days_window, days_window+1):
        dstr = (d0 + timedelta(days=off)).isoformat()
        resp = fixtures_by_date(dstr)
        for it in resp:
            hn = (it.get("teams",{}).get("home",{}) or {}).get("name","")
            an = (it.get("teams",{}).get("away",{}) or {}).get("name","")
            s1 = fuzz.token_set_ratio(_norm(home), _norm(hn))
            s2 = fuzz.token_set_ratio(_norm(away), _norm(an))
            sc = (s1+s2)//2
            if sc > best_score:
                best_score = sc; best_id = int(it["fixture"]["id"])
    if best_score >= min_match:
        return best_id
    return None

def probs_from_odds(oh, od, oa) -> np.ndarray:
    arr = np.array([oh,od,oa], dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        inv = 1.0/arr
    inv[~np.isfinite(inv)] = 0.0
    s = inv.sum()
    if s <= 0: return np.array([1/3,1/3,1/3], dtype=float)
    return inv/s

def renorm(ph, pd, pa) -> np.ndarray:
    ph = max(float(ph), 1e-9); pd = max(float(pd), 1e-9); pa = max(float(pa), 1e-9)
    s = ph+pd+pa
    return np.array([ph/s, pd/s, pa/s], dtype=float)

def draw_boost(p: np.ndarray, boost: float) -> np.ndarray:
    """Aumenta pd em 'boost' (em pontos percentuais) e tira dos lados proporcionalmente."""
    boost = max(0.0, min(float(boost), 0.2))  # capa em 20 pp por segurança
    ph,pd,pa = p.tolist()
    # não deixa pd > 0.85 por sanidade
    max_room = max(0.0, 0.85 - pd)
    inc = min(boost, max_room)
    if inc <= 1e-12: return p
    # retira dos lados proporcionalmente ao tamanho
    tot_side = ph+pa
    if tot_side <= 1e-12:
        return p
    ph_new = ph - inc*(ph/tot_side)
    pa_new = pa - inc*(pa/tot_side)
    return renorm(ph_new, pd+inc, pa_new)

def home_alt_boost(p: np.ndarray, gamma: float) -> np.ndarray:
    """Multiplica p_home por (1+gamma) e renormaliza."""
    ph,pd,pa = p.tolist()
    ph2 = ph*(1.0+max(0.0, float(gamma)))
    return renorm(ph2, pd, pa)

def pick_day_params(lat: float, lon: float, date_iso: str):
    # Pedimos dados diários para a data do jogo
    return {
        "latitude": lat,
        "longitude": lon,
        "daily": "precipitation_sum,temperature_2m_max,temperature_2m_min,windspeed_10m_max",
        "timezone": "auto",
        "start_date": date_iso,
        "end_date": date_iso
    }

def extract_daily(weather_json: dict) -> Tuple[Optional[float],Optional[float],Optional[float],Optional[float],Optional[float]]:
    daily = weather_json.get("daily") or {}
    # Alguns metadados úteis (elevação pode vir no topo)
    elevation = weather_json.get("elevation", None)
    try:
        pr = float((daily.get("precipitation_sum") or [None])[0]) if daily.get("precipitation_sum") else None
        tmax = float((daily.get("temperature_2m_max") or [None])[0]) if daily.get("temperature_2m_max") else None
        tmin = float((daily.get("temperature_2m_min") or [None])[0]) if daily.get("temperature_2m_min") else None
        wmax = float((daily.get("windspeed_10m_max") or [None])[0]) if daily.get("windspeed_10m_max") else None
    except Exception:
        pr=tmax=tmin=wmax=None
    return pr, tmax, tmin, wmax, elevation

def climate_boosts(pr_mm: Optional[float], tmax: Optional[float], tmin: Optional[float], wmax: Optional[float]) -> float:
    """Converte clima do dia em incremento no empate (pontos percentuais)."""
    boost = 0.0
    # Chuva: cap 10mm/d para boost cheio
    if pr_mm is not None and pr_mm >= 1.0:
        rain_factor = min(pr_mm/10.0, 1.0)
        boost += BETA_RAIN_MAX * rain_factor
    # Vento: cap 10 m/s para boost cheio (~36 km/h)
    if wmax is not None and wmax >= 6.0:
        wind_factor = min(wmax/10.0, 1.0)
        boost += BETA_WIND_MAX * wind_factor
    # Temperatura: calor extremo (>=32C) OU frio extremo (<=5C)
    if tmax is not None and tmax >= 32.0:
        # quanto maior acima de 32, mais sobe até cap ~38C
        hot_factor = min(max((tmax-32.0)/6.0, 0.0), 1.0)
        boost += BETA_TEMP_MAX * hot_factor
    if tmin is not None and tmin <= 5.0:
        cold_factor = min(max((5.0 - tmin)/10.0, 0.0), 1.0)
        boost += BETA_TEMP_MAX * cold_factor
    # cap geral de aumento do X por clima
    return min(boost, BETA_RAIN_MAX + BETA_WIND_MAX + BETA_TEMP_MAX)

def main():
    ap = argparse.ArgumentParser(description="Ajuste de probabilidades por clima (Open-Meteo + API-Football)")
    ap.add_argument("--rodada", required=True)
    ap.add_argument("--country-hint", default="Brazil")
    ap.add_argument("--days-window", type=int, default=DAYS_WINDOW_DEFAULT)
    ap.add_argument("--min-match", type=int, default=MIN_MATCH_DEFAULT)
    ap.add_argument("--altitude-threshold", type=float, default=1500.0)
    args = ap.parse_args()

    base = Path(f"data/out/{args.rodada}")
    joined_path = base/"joined.csv"
    out_path = base/"joined_weather.csv"
    if not joined_path.exists() or joined_path.stat().st_size == 0:
        raise RuntimeError(f"[weather] joined.csv ausente/vazio: {joined_path}")

    df = pd.read_csv(joined_path)
    need = ["match_id","home","away","odd_home","odd_draw","odd_away"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise RuntimeError(f"[weather] joined.csv faltando colunas: {miss}")

    date_iso = args.rodada.split("_",1)[0]
    season_year = int(date_iso.split("-")[0])

    rows=[]
    for _, r in df.iterrows():
        home = str(r["home"]); away = str(r["away"])
        oh, od, oa = r["odd_home"], r["odd_draw"], r["odd_away"]

        # Se não tem odds, apenas repassa NaNs nas p_*
        if pd.isna(oh) or pd.isna(od) or pd.isna(oa):
            rows.append({**r,
                         "p_home": np.nan, "p_draw": np.nan, "p_away": np.nan,
                         "pr_mm": np.nan, "tmax": np.nan, "tmin": np.nan, "wmax": np.nan,
                         "elevation": np.nan, "source_weather": "none"})
            continue

        p = probs_from_odds(float(oh), float(od), float(oa))

        # 1) fixture -> venue
        try:
            fid = find_fixture_id(date_iso, home, away, season_year,
                                  country_hint=args.country_hint,
                                  days_window=args.days_window, min_match=args.min_match)
        except Exception:
            fid = None

        lat = lon = elev = None
        if fid:
            try:
                fx = api_get("/fixtures", {"id": fid}).get("response", [])
                if fx:
                    venue = fx[0].get("fixture",{}).get("venue",{}) or {}
                    lat = venue.get("latitude")
                    lon = venue.get("longitude")
                    elev = venue.get("elevation")
            except Exception:
                pass

        # 2) Open-Meteo para o dia do jogo (se tivermos lat/lon)
        pr_mm=tmax=tmin=wmax=None
        if lat is not None and lon is not None:
            try:
                meteo = openmeteo_get(pick_day_params(lat, lon, date_iso))
                pr_mm, tmax, tmin, wmax, om_elev = extract_daily(meteo)
                if elev is None and om_elev is not None:
                    elev = om_elev
            except Exception:
                pass

        # 3) aplica ajustes
        p_adj = p.copy()
        b_draw = climate_boosts(pr_mm, tmax, tmin, wmax)
        if b_draw > 0:
            p_adj = draw_boost(p_adj, b_draw)
        if elev is not None:
            try:
                if float(elev) >= float(args.altitude_threshold):
                    p_adj = home_alt_boost(p_adj, GAMMA_ALT_HOME)
            except Exception:
                pass

        rows.append({
            **r,
            "p_home": p_adj[0], "p_draw": p_adj[1], "p_away": p_adj[2],
            "pr_mm": pr_mm if pr_mm is not None else np.nan,
            "tmax": tmax if tmax is not None else np.nan,
            "tmin": tmin if tmin is not None else np.nan,
            "wmax": wmax if wmax is not None else np.nan,
            "elevation": float(elev) if elev is not None else np.nan,
            "source_weather": "open-meteo" if (lat is not None and lon is not None) else "none"
        })

    out = pd.DataFrame(rows)

    # garantir numérico
    for col in ["p_home","p_draw","p_away","pr_mm","tmax","tmin","wmax","elevation"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    # Recalcular odds coerentes quando p_* válidas
    def inv(arr: np.ndarray) -> np.ndarray:
        with np.errstate(divide="ignore", invalid="ignore"):
            res = 1.0 / np.where(arr > 1e-9, arr, np.nan)
        return res

    pnum = out[["p_home","p_draw","p_away"]].apply(pd.to_numeric, errors="coerce")
    mask = pnum.notna().all(axis=1)
    out.loc[mask, ["odd_home","odd_draw","odd_away"]] = inv(pnum[mask].values)

    out.to_csv(out_path, index=False)
    print(f"[weather] joined_weather.csv salvo em {out_path}")

if __name__ == "__main__":
    main()
