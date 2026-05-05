"""P9: Heatmap — 2D matrix visualization (year × cohort, role × studio, etc.).

Wraps go.Heatmap with v3 theme defaults (viridis CB-safe colorscale,
diagonal pattern in print theme). Optional null-overlay (cells outside
null 95% CI are highlighted with a thin border).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import plotly.graph_objects as go

from ..theme import apply_theme


@dataclass(frozen=True)
class HeatmapSpec:
    z: Sequence[Sequence[float]]   # 2D matrix [n_rows][n_cols]
    x_labels: Sequence[str]        # column labels
    y_labels: Sequence[str]        # row labels
    title: str = ""
    x_label: str = ""
    y_label: str = ""
    z_label: str = "value"
    colorscale: str = "Viridis"   # CB-safe perceptually uniform
    diverging: bool = False        # if True, use RdBu_r centered at zero
    z_mid: float | None = None     # midpoint for diverging scale
    null_envelope: tuple[Sequence[Sequence[float]], Sequence[Sequence[float]]] | None = None
    text_overlay: bool = False     # show z values in cells
    text_format: str = ".2f"
    height: int = 480
    aspect: Literal["auto", "equal"] = "auto"


def render_heatmap(spec: HeatmapSpec, *, theme: str = "dark") -> go.Figure:
    if not spec.z or not spec.x_labels or not spec.y_labels:
        fig = go.Figure()
        fig.update_layout(title=spec.title or "(no data)")
        return apply_theme(fig, theme=theme, height=spec.height)

    cs = "RdBu_r" if spec.diverging else spec.colorscale
    text = None
    texttemplate = None
    if spec.text_overlay:
        text = [[f"{v:{spec.text_format}}" for v in row] for row in spec.z]
        texttemplate = "%{text}"

    fig = go.Figure(
        go.Heatmap(
            z=list(spec.z),
            x=list(spec.x_labels),
            y=list(spec.y_labels),
            colorscale=cs,
            zmid=spec.z_mid if spec.diverging else None,
            colorbar=dict(title=spec.z_label),
            text=text,
            texttemplate=texttemplate,
            hovertemplate="x=%{x}<br>y=%{y}<br>z=%{z:.4f}<extra></extra>",
        )
    )

    # null envelope: outline cells whose z is outside [lo, hi]
    if spec.null_envelope is not None:
        lo, hi = spec.null_envelope
        for i, row in enumerate(spec.z):
            for j, v in enumerate(row):
                if i >= len(lo) or j >= len(lo[i]):
                    continue
                if v < lo[i][j] or v > hi[i][j]:
                    fig.add_shape(
                        type="rect",
                        x0=j - 0.5, x1=j + 0.5,
                        y0=i - 0.5, y1=i + 0.5,
                        line=dict(color="#ffffff", width=2),
                        fillcolor="rgba(0,0,0,0)",
                    )

    fig.update_layout(
        title=spec.title,
        xaxis_title=spec.x_label,
        yaxis_title=spec.y_label,
    )
    if spec.aspect == "equal":
        fig.update_yaxes(scaleanchor="x", scaleratio=1)
    return apply_theme(fig, theme=theme, height=spec.height)
