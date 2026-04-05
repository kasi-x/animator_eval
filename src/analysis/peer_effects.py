"""Compatibility shim — actual code in src/analysis/network/peer_effects.py"""
import importlib, sys
sys.modules[__name__] = importlib.import_module("src.analysis.network.peer_effects")
