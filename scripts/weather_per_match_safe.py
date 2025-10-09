#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
weather_per_match_safe.py

Lê data/in/matches_source.csv (ou arquivo passado em --in) e consulta o
Open-Meteo por coordenada de cada jogo, gerando:

  <OUT_DIR>/weather.csv

Robustez:
- Requisições com retries exponenciais e timeout configurável.
- Em caso de falha por jogo, grava linha com métricas vazias e
  weather_source='open-meteo-error' (pipeline não quebra).
- Nunca levanta exceção fatal; sempre escreve um CSV com cabeçalho
  e 1 linha por jogo de entrada.

Saída (colunas):
  match_id,lat,lon,temp_c,apparent_temp_c,wind_speed_kph,wind_gust_kph,
  wind_dir_deg,precip_mm,precip_prob,relative_humidity,cloud_cover,
  pressure_hpa,weather_source,fetched_at_utc
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests


COLUMNS = [
    "match_id","lat","lon","temp_c","apparent_temp_c","wind_speed_kph",
    "wind_gust_kph","wind_dir_deg","precip_mm","precip_prob",
    "relative_humidity","cloud_cover","pressure_hpa",
    "weather_source","fetched_at_utc"
]


def debug_print(enabled: bool, *args):
    if enabled:
        print(*args, flush=True)


def read_matches(path: str, debug: bool) -> pd.DataFrame:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {path}")

    df = pd.read_csv(path)
    # tenta mapear nomes comuns
    # match_id pode vir como 'match_id' ou 'id'
    if "match_id" not in df.columns:
        if "id" in df.columns:
            df = df.rename(columns={"id": "match_id"})
        else:
            # gera um sequencial para não quebrar
            df = df.reset_index().rename(columns={"index": "match_id"})
            df["match_id"] = df["match_id"].astype(str)

    # lat/lon podem vir como 'lat/lon' ou 'latitude/longitude'
    if "lat" not in df.columns and "latitude" in df.columns:
        df = df.rename(columns={"latitude": "lat"})
    if "lon" not in df.columns and "longitude" in df.columns:
        df = df.rename(columns={"longitude": "lon"})

    if "lat" not in df.columns or "lon" not in df.columns:
        # cria lat/lon vazios; escreveremos linhas com erro
        df["lat"] = float("nan")
        df["lon"] = float("nan")

    # garante tipos de saída como string para match_id
    df["match_id"] = df["match_id"].astype(str)

    if debug:
        debug_print(True, f"[weather] entradas: {len(df)} linhas")
    return df


def fetch_open_meteo(lat: float, lon: float, timeout: int, debug: bool):
    """
    Consulta Open-Meteo 'current' com variáveis compatíveis com seu CSV.
    Retorna dict com campos já nos nomes esperados ou None em falha.
    """
    base = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": (
            "temperature_2m,apparent_temperature,"
            "wind_speed_10m,wind_gusts_10m,wind_direction_10m,"
            "precipitation,relative_humidity_2m,cloud_cover,pressure_msl"
        ),
        # unidades padrão: C, km/h, hPa
        "timezone": "UTC",
    }
    try:
        resp = requests.get(base, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json() or {}
        cur = data.get("current") or {}
        # mapeia para seus nomes
        out = {
            "temp_c": cur.get("temperature_2m"),
            "apparent_temp_c": cur.get("apparent_temperature"),
            "wind_speed_kph": cur.get("wind_speed_10m"),
            "wind_gust_kph": cur.get("wind_gusts_10m"),
            "wind_dir_deg": cur.get("wind_direction_10m"),
            "precip_mm": cur.get("precipitation"),
            # a API "current" não traz probabilidade diretamente:
            "precip_prob": None,
            "relative_humidity": cur.get("relative_humidity_2m"),
            "cloud_cover": cur.get("cloud_cover"),
            "pressure_hpa": cur.get("pressure_msl"),
            "weather_source": "open-meteo",
        }
        return out
    except Exception as e:
        debug_print(debug, f"[weather] exception Open-Meteo: {e}")
        return None


def fetch_with_retries(lat: float, lon: float, retries: int, backoff: float, timeout: int, debug: bool):
    attempt = 0
    while attempt <= retries:
        res = fetch_open_meteo(lat, lon, timeout, debug)
        if res is not None:
            return res
        sleep = backoff * (2 ** attempt)
        time.sleep(sleep)
        attempt += 1
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", required=True, help="CSV de entrada com jogos (ex.: data/in/matches_source.csv)")
    ap.add_argument("--out-dir", required=True, help="Diretório da rodada (ex.: data/out/123456)")
    ap.add_argument("--timeout", type=int, default=20, help="Timeout da requisição (segundos)")
    ap.add_argument("--retries", type=int, default=3, help="Número de tentativas extras em caso de erro")
    ap.add_argument("--backoff", type=float, default=1.0, help="Backoff base (segundos) para retries exponenciais")
    ap.add_argument("--sleep-between", type=float, default=0.2, help="Pequeno delay entre jogos para evitar burst")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    out_path = os.path.join(args.out_dir, "weather.csv")

    try:
        df = read_matches(args.infile, args.debug)
    except Exception as e:
        # Não quebra: escreve CSV vazio com cabeçalho (sem linhas)
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(COLUMNS)
        print(f"[weather] FALHA ao ler entrada: {e}", file=sys.stderr)
        # Ainda assim retornamos 0 — o passo seguinte usa apenas a existência do arquivo
        return 0

    rows = []
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
    for _, r in df.iterrows():
        match_id = str(r.get("match_id"))
        lat = r.get("lat")
        lon = r.get("lon")

        # valida lat/lon
        try:
            latf = float(lat)
            lonf = float(lon)
        except Exception:
            latf = float("nan")
            lonf = float("nan")

        if not (isinstance(latf, float) and isinstance(lonf, float)) or (pd.isna(latf) or pd.isna(lonf)):
            # Sem coordenadas — grava linha de erro amigável
            rows.append([
                match_id, lat, lon,
                None, None, None, None, None, None, None, None, None,
                "open-meteo-missing-coords", now_utc
            ])
            continue

        info = fetch_with_retries(latf, lonf, args.retries, args.backoff, args.timeout, args.debug)
        if info is None:
            print(f"[weather] ERRO em match_id={match_id}: timeout/falha após retries", file=sys.stderr)
            rows.append([
                match_id, latf, lonf,
                None, None, None, None, None, None, None, None, None,
                "open-meteo-error", now_utc
            ])
        else:
            rows.append([
                match_id, latf, lonf,
                info.get("temp_c"), info.get("apparent_temp_c"), info.get("wind_speed_kph"),
                info.get("wind_gust_kph"), info.get("wind_dir_deg"), info.get("precip_mm"),
                info.get("precip_prob"), info.get("relative_humidity"), info.get("cloud_cover"),
                info.get("pressure_hpa"), info.get("weather_source"), now_utc
            ])

        # evita bursts muito rápidos
        time.sleep(args.sleep_between)

    # Sempre escreve arquivo com cabeçalho + N linhas (1 por match)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(COLUMNS)
        wr.writerows(rows)

    # Pequeno resumo no debug
    ok = sum(1 for row in rows if row[13] == "open-meteo")
    err = len(rows) - ok
    debug_print(args.debug, f"[weather] escrito {out_path} | ok={ok} erros={err}")

    # Nunca quebrar o workflow
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        # fallback final: ainda assim escrevemos apenas o cabeçalho para não quebrar
        out_dir = os.environ.get("OUT_DIR", ".")
        try:
            os.makedirs(out_dir, exist_ok=True)
            with open(os.path.join(out_dir, "weather.csv"), "w", encoding="utf-8", newline="") as f:
                csv.writer(f).writerow(COLUMNS)
        except Exception:
            pass
        print(f"[weather] FALHA: {e}", file=sys.stderr)
        sys.exit(0)