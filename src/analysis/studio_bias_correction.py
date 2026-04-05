"""Compatibility shim — actual code in src/analysis/studio/bias_correction.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.studio.bias_correction")
