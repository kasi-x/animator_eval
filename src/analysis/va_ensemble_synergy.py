"""Compatibility shim — actual code in src/analysis/va/ensemble_synergy.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.va.ensemble_synergy")
