#!/usr/bin/env python3
import argparse, sys
p = argparse.ArgumentParser()
p.add_argument("--rodada", required=True)
args = p.parse_args()
print(f"[stub] scorecard rodando para rodada={args.rodada}")
sys.exit(0)
