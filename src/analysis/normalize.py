"""Compatibility shim — actual code in src/analysis/scoring/normalize.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.scoring.normalize")
