"""Compatibility shim — actual code in src/analysis/studio/profile.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.studio.profile")
