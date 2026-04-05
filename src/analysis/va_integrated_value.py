"""Compatibility shim — actual code in src/analysis/va/integrated_value.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.va.integrated_value")
