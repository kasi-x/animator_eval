"""Compatibility shim — actual code in src/analysis/network/trust.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.network.trust")
