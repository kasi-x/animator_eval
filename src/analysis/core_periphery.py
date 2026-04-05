"""Compatibility shim — actual code in src/analysis/network/core_periphery.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.network.core_periphery")
