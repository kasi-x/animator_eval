"""P4: SmallMultiples — facet grid wrapper around any sub-primitive.

A facet cell is a (row_label, col_label, sub_figure) triple. The
sub-figures are typically built by other primitives (CIScatter / KMCurve
/ RidgePlot / BoxStripCI). SmallMultiples handles the subplot grid,
shared axes / null reference, and consistent labeling.
"""

from __future__ import annotations

from dataclasses import dataclass

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ..palettes import hex_to_rgba
from ..theme import apply_theme


@dataclass(frozen=True)
class FacetCell:
    row_label: str
    col_label: str
    sub_figure: go.Figure  # produced by another primitive


@dataclass(frozen=True)
class SmallMultiplesSpec:
    facets: list[FacetCell]
    title: str = ""
    n_cols: int = 4
    shared_x: bool = True
    shared_y: bool = True
    shared_null_band: tuple[float, float] | None = None  # (lo, hi) on x axis
    null_band_color: str = "#a0a0a0"
    null_band_opacity: float = 0.10
    height_per_row: int = 280
    horizontal_spacing: float = 0.06
    vertical_spacing: float = 0.10
    show_facet_titles: bool = True


def _facet_grid(facets: list[FacetCell], n_cols: int) -> tuple[int, int]:
    n = len(facets)
    cols = min(n_cols, n)
    rows = (n + cols - 1) // cols
    return rows, cols


def render_small_multiples(spec: SmallMultiplesSpec, *, theme: str = "dark") -> go.Figure:
    if not spec.facets:
        fig = go.Figure()
        fig.update_layout(title=spec.title or "(no facets)")
        return apply_theme(fig, theme=theme, height=spec.height_per_row)

    rows, cols = _facet_grid(spec.facets, spec.n_cols)

    if spec.show_facet_titles:
        subplot_titles = [
            f"{f.row_label} × {f.col_label}".strip(" ×")
            for f in spec.facets
        ]
    else:
        subplot_titles = None

    fig = make_subplots(
        rows=rows,
        cols=cols,
        shared_xaxes=spec.shared_x,
        shared_yaxes=spec.shared_y,
        subplot_titles=subplot_titles,
        horizontal_spacing=spec.horizontal_spacing,
        vertical_spacing=spec.vertical_spacing,
    )

    for idx, facet in enumerate(spec.facets):
        r = idx // cols + 1
        c = idx % cols + 1
        for trace in facet.sub_figure.data:
            fig.add_trace(trace, row=r, col=c)

    if spec.shared_null_band is not None:
        lo, hi = spec.shared_null_band
        for idx in range(len(spec.facets)):
            r = idx // cols + 1
            c = idx % cols + 1
            fig.add_vrect(
                x0=lo,
                x1=hi,
                fillcolor=hex_to_rgba(spec.null_band_color, spec.null_band_opacity),
                line_width=0,
                layer="below",
                row=r,
                col=c,
            )

    fig.update_layout(
        title=spec.title,
        showlegend=False,  # facets typically share legend in a sibling panel
    )
    height = max(spec.height_per_row, rows * spec.height_per_row + 80)
    return apply_theme(fig, theme=theme, height=height)
