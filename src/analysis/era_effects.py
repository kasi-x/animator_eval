"""Compatibility shim — actual code in src/analysis/causal/era_effects.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.causal.era_effects")
