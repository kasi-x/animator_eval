"""Typography for v3 charts.

A single font stack across themes; only sizes / colors differ.
"""

from __future__ import annotations

from dataclasses import dataclass

FONT_STACK: str = (
    "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, "
    "'Noto Sans CJK JP', 'Hiragino Kaku Gothic ProN', sans-serif"
)


@dataclass(frozen=True)
class Typography:
    family: str = FONT_STACK
    title_size: int = 16
    axis_title_size: int = 13
    tick_size: int = 11
    legend_size: int = 11
    annotation_size: int = 10
    color: str = "#c0c0d0"
