"""Compatibility shim — actual code in src/analysis/network/temporal_pagerank.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.network.temporal_pagerank")
