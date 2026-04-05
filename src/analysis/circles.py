"""Compatibility shim — actual code in src/analysis/network/circles.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.network.circles")
