"""Compatibility shim — actual code in src/analysis/studio/clustering.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.studio.clustering")
