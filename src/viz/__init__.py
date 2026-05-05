"""Visualization system v3.

See docs/VIZ_SYSTEM_v3.md for the design rationale and primitive catalog.

Public API
----------
- ``apply_theme(fig, theme)``  unified plotly_dark / light / print theme
- ``embed(fig, div_id, height)``  HTML embed (back-compat shim)
- ``palettes``  CB-safe Okabe-Ito + fixed category mappings
- ``ci``  CI band / whisker drawers
- ``null_overlay``  null model envelope drawers
- ``shrinkage_badge``  shrinkage application annotation
- ``primitives``  CIScatter (P1) and other 8 primitives
"""

from __future__ import annotations

from . import ci, null_overlay, palettes, primitives, shrinkage_badge, theme
from .embed import embed
from .theme import apply_theme

__all__ = [
    "apply_theme",
    "embed",
    "ci",
    "null_overlay",
    "palettes",
    "primitives",
    "shrinkage_badge",
    "theme",
]
