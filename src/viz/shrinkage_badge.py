"""Shrinkage badge annotation.

When a report presents shrunk estimates (James-Stein, Empirical Bayes,
etc.), it MUST display a badge so the reader is not misled into
treating the displayed values as raw counts.
"""

from __future__ import annotations

import plotly.graph_objects as go


def add_shrinkage_badge(
    fig: go.Figure,
    *,
    method: str,
    n_threshold: int | None = None,
    xref: str = "paper",
    yref: str = "paper",
    x: float = 1.0,
    y: float = 1.05,
) -> None:
    """Top-right badge: e.g. 「縮約適用: James-Stein (n<30 で適用)」."""
    label = f"縮約適用: {method}"
    if n_threshold is not None:
        label += f" (n<{n_threshold} で適用)"
    fig.add_annotation(
        text=label,
        xref=xref,
        yref=yref,
        x=x,
        y=y,
        xanchor="right",
        yanchor="bottom",
        showarrow=False,
        font=dict(size=10, color="#a0a0c0"),
        bgcolor="rgba(0,0,0,0.25)",
        bordercolor="rgba(255,255,255,0.15)",
        borderwidth=1,
        borderpad=4,
    )
