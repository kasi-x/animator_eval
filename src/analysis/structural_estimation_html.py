"""Compatibility shim — actual code in src/analysis/causal/structural_estimation_html.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.causal.structural_estimation_html")
