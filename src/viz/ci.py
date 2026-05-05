"""CI band / whisker drawers.

Reports MUST use these helpers when CI data is present. Raw
``go.Scatter`` / ``error_x`` / ``error_y`` calls for CI rendering
in report code is discouraged (see ``docs/VIZ_SYSTEM_v3.md`` §10).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

import plotly.graph_objects as go

from .palettes import hex_to_rgba
from .theme import get_theme


def add_ci_band(
    fig: go.Figure,
    x: Sequence,
    lo: Sequence,
    hi: Sequence,
    *,
    color: str,
    opacity: float | None = None,
    name: str | None = None,
    legendgroup: str | None = None,
    theme: str = "dark",
) -> None:
    """Continuous CI band via ``fill='toself'``.

    ``x``, ``lo``, ``hi`` must have equal length. ``color`` is the
    base hex; alpha is taken from theme defaults if not given.
    """
    if not (len(x) == len(lo) == len(hi)):
        raise ValueError(
            f"x/lo/hi length mismatch: {len(x)}/{len(lo)}/{len(hi)}"
        )
    op = opacity if opacity is not None else get_theme(theme).ci_band.opacity
    x_poly = list(x) + list(reversed(list(x)))
    y_poly = list(hi) + list(reversed(list(lo)))
    fig.add_trace(
        go.Scatter(
            x=x_poly,
            y=y_poly,
            fill="toself",
            fillcolor=hex_to_rgba(color, op),
            line=dict(width=0),
            mode="lines",
            hoverinfo="skip",
            showlegend=name is not None,
            name=name or "",
            legendgroup=legendgroup,
        )
    )


def add_ci_whisker(
    fig: go.Figure,
    *,
    x: Sequence[float],
    y: Sequence,
    lo: Sequence[float],
    hi: Sequence[float],
    color: str,
    direction: Literal["x", "y"] = "x",
    name: str | None = None,
    marker_symbol: str = "square",
    marker_size: int = 9,
    line_width: int = 2,
) -> None:
    """Discrete CI whiskers (forest plot style)."""
    n = len(x)
    if not (n == len(y) == len(lo) == len(hi)):
        raise ValueError("x/y/lo/hi length mismatch")
    if direction == "x":
        err = dict(
            type="data",
            symmetric=False,
            array=[hi[i] - x[i] for i in range(n)],
            arrayminus=[x[i] - lo[i] for i in range(n)],
            color=color,
            thickness=line_width,
            width=6,
        )
        trace = go.Scatter(
            x=list(x),
            y=list(y),
            mode="markers",
            marker=dict(color=color, size=marker_size, symbol=marker_symbol),
            error_x=err,
            name=name or "",
            showlegend=name is not None,
        )
    else:
        err = dict(
            type="data",
            symmetric=False,
            array=[hi[i] - y[i] for i in range(n) if isinstance(y[i], (int, float))],
            arrayminus=[y[i] - lo[i] for i in range(n) if isinstance(y[i], (int, float))],
            color=color,
            thickness=line_width,
            width=6,
        )
        trace = go.Scatter(
            x=list(x),
            y=list(y),
            mode="markers",
            marker=dict(color=color, size=marker_size, symbol=marker_symbol),
            error_y=err,
            name=name or "",
            showlegend=name is not None,
        )
    fig.add_trace(trace)
