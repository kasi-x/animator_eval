"""Compatibility shim — actual code in src/analysis/causal/event_study_viz.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.causal.event_study_viz")
