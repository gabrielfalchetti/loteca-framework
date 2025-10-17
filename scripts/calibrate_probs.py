if not os.path.isfile(args.inp):
    _log(f"{args.inp} não encontrado")
    sys.exit(9)
if not os.path.isfile(args.cal):
    _log(f"{args.cal} não encontrado")
    sys.exit(9)

try:
    # Verificar se pandas está importado corretamente
    try:
        pd.DataFrame()  # Teste simples de importação
    except NameError:
        _log("Erro crítico: módulo pandas não importado corretamente.")
        sys.exit(9)

    df = pd.read_csv(args.inp)
    # Validação de entrada
    if not all(col in df.columns for col in ["match_id", "team_home", "team_away", "p_home", "p_draw", "p_away"]):
        raise ValueError("CSV de entrada sem colunas esperadas")
    probs = df[["p_home", "p_draw", "p_away"]].values
    if not np.all((probs >= 0) & (probs <= 1)):
        raise ValueError("Probs inválidas (fora de [0,1])")
    if not np.allclose(probs.sum(axis=1), 1, atol=0.01):
        _log("Soma de probs != 1, normalizando...")
        probs = probs / probs.sum(axis=1, keepdims=True)

    # Carregar calibrador
    calibrators = None
    try:
        with open(args.cal, "rb") as f:
            calibrators = pickle.load(f)
    except Exception as e:
        _log(f"Erro ao carregar calibrador: {e}. Usando probs originais.")
        calibrators = {"home": None, "draw": None, "away": None}
    if not isinstance(calibrators, dict) or not all(k in calibrators for k in ["home", "draw", "away"]):
        _log("Calibrador inválido, usando probs originais.")
        calibrators = {"home": None, "draw": None, "away": None}

    # Aplicar calibração
    cal_probs = np.zeros_like(probs)
    for i, (ph, pd, pa) in enumerate(probs):
        cal_probs[i, 0] = _apply_calibration(np.array([ph]), calibrators["home"], args.method)
        cal_probs[i, 1] = _apply_calibration(np.array([pd]), calibrators["draw"], args.method)
        cal_probs[i, 2] = _apply_calibration(np.array([pa]), calibrators["away"], args.method)
    s = cal_probs.sum(axis=1, keepdims=True)
    cal_probs = cal_probs / s if s.any() > 0 else probs  # Normaliza se soma > 0

    # Calcular Brier Score (placeholder, requer verdadeiros)
    # brier = _calculate_brier_score(np.ones_like(cal_probs) * 0.33, cal_probs)  # Exemplo fictício
    # _log(f"Brier Score: {brier:.4f}")

    # Salvar resultados
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out_rows = [[r["match_id"], r["team_home"], r["team_away"], cal_p[0], cal_p[1], cal_p[2]] 
                for r, cal_p in zip(df.to_dict("records"), cal_probs)]
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["match_id", "team_home", "team_away", "p_home_cal", "p_draw_cal", "p_away_cal"])
        w.writerows(out_rows)
    _log(f"OK -> {args.out} (linhas={len(out_rows)})")
except Exception as e:
    _log(f"[CRITICAL] Erro: {e}")
    sys.exit(9)