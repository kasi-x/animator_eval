"""Compatibility shim — actual code in src/analysis/scoring/expected_ability.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.scoring.expected_ability")
