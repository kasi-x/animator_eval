"""Compatibility shim — actual code in src/analysis/causal/studio_identification.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.causal.studio_identification")
