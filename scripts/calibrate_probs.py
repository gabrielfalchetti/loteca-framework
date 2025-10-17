# -*- coding: utf-8 -*-
import os
import sys
import pandas as pd  # Importação direta
import numpy as np
from sklearn.isotonic import IsotonicRegression
from sklearn.calibration import CalibratedClassifierCV
import csv
from typing import Dict, List

# Verificação inicial da importação
try:
    _log(f"Versão do pandas: {pd.__version__}")
except NameError:
    print("[calibrate] Erro crítico: módulo pandas não importado corretamente.", file=sys.stderr)
    sys.exit(9)

"""
Calibra probabilidades de previsão de resultados de futebol usando Regressão Isotônica ou Dirichlet.
Aplica modelo pré-treinado salvo em pickle, ajustando probs brutas para valores calibrados.

Saída: CSV com cabeçalho: match_id,team_home,team_away,p_home_cal,p_draw_cal,p_away_cal

Uso:
  python -m scripts.calibrate_probs --in predictions.csv