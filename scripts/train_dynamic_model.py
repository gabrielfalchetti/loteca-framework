#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
train_dynamic_model: Script que gera os parâmetros preditivos dinâmicos.

CORREÇÃO: Este script agora lê os times presentes na rodada (de odds_consensus.csv)
e gera um arquivo de parâmetros que inclui TODOS eles, evitando o erro de
"Parâmetros dinâmicos ausentes".
"""

import os
import sys
import argparse
import json
import pandas as pd
import numpy as np
import re
from unicodedata import normalize as _ucnorm

def log(level, msg):
    tag = "" if level == "INFO" else f"[{level}] "
    print(f"[train_dyn]{tag}{msg}", flush=True)

# Funções de normalização de nome de time (devem ser idênticas às do xg_bivariate)
STOPWORD_TOKENS = {
    "aa","ec","ac","sc","fc","afc","cf","ca","cd","ud",
    "sp","pr","rj","rs","mg","go","mt","ms","pa","pe","pb","rn","ce","ba","al","se","pi","ma","df","es","sc",
}

def _deaccent(s: str) -> str:
    return _ucnorm("NFKD", str(s or "")).encode("ascii", "ignore").decode("ascii")

def norm_key(name: str) -> str:
    s = _deaccent(name).lower()
    s = s.replace("&", " e ")
    s = re.sub(r"[/()\-_.]", " ", s)
    s = re.sub(r"\
