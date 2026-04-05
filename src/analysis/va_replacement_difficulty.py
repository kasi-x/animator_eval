"""Compatibility shim — actual code in src/analysis/va/replacement_difficulty.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.va.replacement_difficulty")
