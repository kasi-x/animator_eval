"""Compatibility shim — actual code in src/analysis/network/structural_holes.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.network.structural_holes")
