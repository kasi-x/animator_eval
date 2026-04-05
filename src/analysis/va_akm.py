"""Compatibility shim — actual code in src/analysis/va/akm.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.va.akm")
