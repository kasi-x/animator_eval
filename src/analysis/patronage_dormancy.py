"""Compatibility shim — actual code in src/analysis/scoring/patronage_dormancy.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.scoring.patronage_dormancy")
