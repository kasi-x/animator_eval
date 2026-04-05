"""Compatibility shim — actual code in src/analysis/scoring/integrated_value.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.scoring.integrated_value")
