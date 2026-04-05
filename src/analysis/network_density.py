"""Compatibility shim — actual code in src/analysis/network/network_density.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.network.network_density")
