"""Compatibility shim — actual code in src/analysis/genre/network.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.genre.network")
