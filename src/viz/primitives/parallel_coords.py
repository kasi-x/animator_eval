"""P10: ParallelCoords — multi-axis benchmark visualization.

Wraps go.Parcoords with v3 theme defaults (viridis colorscale,
percentile-rank axes, sticky brush state). Used for studio benchmark
cards (5+ axes), person profile (multi-metric), etc.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import plotly.graph_objects as go

from ..theme import apply_theme


@dataclass(frozen=True)
class ParallelAxis:
    label: str
    values: Sequence[float]
    range_min: float | None = None
    range_max: float | None = None
    tickformat: str = ""


@dataclass(frozen=True)
class ParallelCoordsSpec:
    axes: list[ParallelAxis]
    color_values: Sequence[float]    # gradient color per row (composite score)
    color_label: str = "score"
    colorscale: str = "Viridis"
    title: str = ""
    height: int = 520
    show_colorbar: bool = True


def render_parallel_coords(
    spec: ParallelCoordsSpec, *, theme: str = "dark"
) -> go.Figure:
    if not spec.axes or not spec.color_values:
        fig = go.Figure()
        fig.update_layout(title=spec.title or "(no data)")
        return apply_theme(fig, theme=theme, height=spec.height)

    dims = []
    for ax in spec.axes:
        vals = list(ax.values)
        dim_kwargs = dict(
            label=ax.label,
            values=vals,
            tickformat=ax.tickformat or None,
        )
        if ax.range_min is not None and ax.range_max is not None:
            dim_kwargs["range"] = [ax.range_min, ax.range_max]
        dims.append(dim_kwargs)

    fig = go.Figure(
        go.Parcoords(
            line=dict(
                color=list(spec.color_values),
                colorscale=spec.colorscale,
                showscale=spec.show_colorbar,
                colorbar=dict(title=spec.color_label),
            ),
            dimensions=dims,
        )
    )
    fig.update_layout(title=spec.title)
    return apply_theme(fig, theme=theme, height=spec.height)
