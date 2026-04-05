"""Compatibility shim — actual code in src/analysis/network/community_detection.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.network.community_detection")
