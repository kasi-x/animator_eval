"""Compatibility shim — actual code in src/analysis/scoring/individual_contribution.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.scoring.individual_contribution")
