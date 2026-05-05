"""Unified theme application for plotly figures.

Three themes are supported:
- ``dark``: glass-morphism (current web default)
- ``light``: high-contrast white background (embeds, print-friendly web)
- ``print``: monochrome + pattern fill for PDF / SVG export

Reports MUST NOT call ``fig.update_layout`` for cosmetic styling.
Call ``apply_theme(fig, theme="dark")`` once at render time and
override only data-bearing fields (axis titles, tick formats, etc.).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import plotly.graph_objects as go

from .palettes import OKABE_ITO, OKABE_ITO_DARK
from .typography import Typography

ThemeName = Literal["dark", "light", "print"]


@dataclass(frozen=True)
class GridStyle:
    color: str
    width: float = 1.0
    zeroline_color: str = "#5a5a8a"


@dataclass(frozen=True)
class CIBandStyle:
    opacity: float = 0.18
    line_width: float = 0.0


@dataclass(frozen=True)
class NullOverlayStyle:
    color: str = "#a0a0a0"
    opacity: float = 0.10
    line_dash: str = "dot"


@dataclass(frozen=True)
class AnnotationStyle:
    bgcolor: str = "rgba(0,0,0,0)"
    bordercolor: str = "rgba(0,0,0,0)"
    font_color: str = "#a0a0c0"
    font_size: int = 10


@dataclass(frozen=True)
class Theme:
    name: ThemeName
    paper_bgcolor: str
    plot_bgcolor: str
    palette: tuple[str, ...]
    typography: Typography
    grid: GridStyle
    annotations: AnnotationStyle
    ci_band: CIBandStyle = field(default_factory=CIBandStyle)
    null_overlay: NullOverlayStyle = field(default_factory=NullOverlayStyle)


_DARK = Theme(
    name="dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0.2)",
    palette=OKABE_ITO_DARK,
    typography=Typography(color="#c0c0d0"),
    grid=GridStyle(color="rgba(255,255,255,0.08)", zeroline_color="#5a5a8a"),
    annotations=AnnotationStyle(font_color="#a0a0c0"),
)

_LIGHT = Theme(
    name="light",
    paper_bgcolor="#ffffff",
    plot_bgcolor="#fafafa",
    palette=OKABE_ITO,
    typography=Typography(color="#1a1a2e"),
    grid=GridStyle(color="rgba(0,0,0,0.08)", zeroline_color="#888888"),
    annotations=AnnotationStyle(font_color="#444"),
)

_PRINT = Theme(
    name="print",
    paper_bgcolor="#ffffff",
    plot_bgcolor="#ffffff",
    palette=("#000000", "#444444", "#808080", "#a0a0a0", "#c0c0c0", "#e0e0e0"),
    typography=Typography(color="#000000"),
    grid=GridStyle(color="rgba(0,0,0,0.15)", zeroline_color="#000000"),
    annotations=AnnotationStyle(font_color="#000"),
    null_overlay=NullOverlayStyle(color="#888", opacity=0.18, line_dash="dot"),
)


_THEMES: dict[str, Theme] = {"dark": _DARK, "light": _LIGHT, "print": _PRINT}


def get_theme(name: ThemeName = "dark") -> Theme:
    return _THEMES[name]


def apply_theme(
    fig: go.Figure,
    *,
    theme: ThemeName = "dark",
    height: int | None = None,
    margin: dict | None = None,
) -> go.Figure:
    """Apply v3 theme to a plotly figure.

    Idempotent: safe to call multiple times. Returns ``fig`` for chaining.
    """
    t = get_theme(theme)
    plotly_template = "plotly_dark" if theme == "dark" else "plotly_white"

    layout: dict = {
        "template": plotly_template,
        "paper_bgcolor": t.paper_bgcolor,
        "plot_bgcolor": t.plot_bgcolor,
        "font": {
            "family": t.typography.family,
            "color": t.typography.color,
            "size": t.typography.tick_size,
        },
        "title_font": {"size": t.typography.title_size},
        "legend": {"font": {"size": t.typography.legend_size}},
        "margin": margin or {"l": 64, "r": 28, "t": 56, "b": 52},
        "xaxis": {
            "gridcolor": t.grid.color,
            "zerolinecolor": t.grid.zeroline_color,
            "title_font": {"size": t.typography.axis_title_size},
        },
        "yaxis": {
            "gridcolor": t.grid.color,
            "zerolinecolor": t.grid.zeroline_color,
            "title_font": {"size": t.typography.axis_title_size},
        },
    }
    if height is not None:
        layout["height"] = height
    fig.update_layout(**layout)
    return fig


def palette_for(theme: ThemeName = "dark") -> tuple[str, ...]:
    return get_theme(theme).palette
