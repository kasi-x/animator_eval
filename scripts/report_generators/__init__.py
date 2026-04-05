"""Report generators package for Animetor Eval.

Contains modular utilities for generating HTML analysis reports:
- html_templates: HTML template functions and CSS styling
- helpers: Visualization utilities, feature extraction, and data I/O
"""

from . import helpers, html_templates

__all__ = ["helpers", "html_templates"]
