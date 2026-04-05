"""Compatibility shim — actual code in src/analysis/va/character_diversity.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.va.character_diversity")
