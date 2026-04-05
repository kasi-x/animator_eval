"""Compatibility shim — actual code in src/analysis/genre/specialization.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.genre.specialization")
