"""Null model overlay drawers (envelope band + reference line).

Reports declaring a ``null_model`` in their ``ReportSpec`` MUST
overlay either an envelope band or a reference line so the reader
sees how far observed values lie from the null distribution.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

import plotly.graph_objects as go

from .palettes import hex_to_rgba
from .theme import get_theme


def add_null_envelope(
    fig: go.Figure,
    x: Sequence,
    null_lo: Sequence,
    null_hi: Sequence,
    *,
    name: str = "null 95%",
    theme: str = "dark",
    color: str | None = None,
    opacity: float | None = None,
) -> None:
    """Null-model P2.5–P97.5 envelope as a background band.

    Always added before observed traces so it sits behind. Hover is
    disabled to avoid noise.
    """
    if not (len(x) == len(null_lo) == len(null_hi)):
        raise ValueError("x/null_lo/null_hi length mismatch")
    style = get_theme(theme).null_overlay
    c = color if color is not None else style.color
    op = opacity if opacity is not None else style.opacity
    x_poly = list(x) + list(reversed(list(x)))
    y_poly = list(null_hi) + list(reversed(list(null_lo)))
    fig.add_trace(
        go.Scatter(
            x=x_poly,
            y=y_poly,
            fill="toself",
            fillcolor=hex_to_rgba(c, op),
            line=dict(width=0),
            mode="lines",
            hoverinfo="skip",
            name=name,
            showlegend=True,
        )
    )


def add_null_reference_line(
    fig: go.Figure,
    value: float,
    *,
    label: str = "null",
    direction: Literal["h", "v"] = "h",
    theme: str = "dark",
    annotation_position: str = "top right",
) -> None:
    """Single null reference line (HR=1, HHI=0.001, etc.)."""
    style = get_theme(theme).null_overlay
    if direction == "h":
        fig.add_hline(
            y=value,
            line_dash=style.line_dash,
            line_color=style.color,
            annotation_text=label,
            annotation_position=annotation_position,
        )
    else:
        fig.add_vline(
            x=value,
            line_dash=style.line_dash,
            line_color=style.color,
            annotation_text=label,
            annotation_position=annotation_position,
        )
