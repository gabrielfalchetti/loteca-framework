#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import logging
import os
import sys
from typing import Optional

class _Color:
    RESET = "\033[0m"
    DIM = "\033[2m"
    BOLD = "\033[1m"

def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Logger padrão:
      - nível controlado por ENV DEBUG (true/false)
      - formato enxuto e legível em Actions
    """
    level = logging.DEBUG if str(os.getenv("DEBUG", "false")).lower() == "true" else logging.INFO
    logger = logging.getLogger(name if name else "loteca")
    if logger.handlers:
        logger.setLevel(level)
        return logger

    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    handler.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
    logger.addHandler(handler)
    logger.propagate = False
    return logger
