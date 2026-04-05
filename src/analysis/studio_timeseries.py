"""Compatibility shim — actual code in src/analysis/studio/timeseries.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.studio.timeseries")
