"""P5: RidgePlot — joyplot-style overlapping density curves.

Renders KDE for each row in a stacked layout. The IQR (or any
configured quantile pair) is highlighted with darker shading. An
optional shared null distribution is drawn beneath every row in gray.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import numpy as np
import plotly.graph_objects as go

from ..palettes import OKABE_ITO_DARK, hex_to_rgba
from ..theme import apply_theme


@dataclass(frozen=True)
class RidgeRow:
    label: str
    samples: Sequence[float]
    color: str | None = None
    n: int | None = None


@dataclass(frozen=True)
class RidgePlotSpec:
    distributions: list[RidgeRow]
    title: str = ""
    x_label: str = ""
    overlap: float = 0.6
    quantile_band: tuple[float, float] = (0.25, 0.75)
    null_distribution: Sequence[float] | None = None
    null_label: str = "null distribution"
    height: int = 540
    bandwidth: float | None = None  # None → scott rule
    n_grid: int = 200
    default_palette: tuple[str, ...] = field(default_factory=lambda: OKABE_ITO_DARK)


def _kde(samples: np.ndarray, grid: np.ndarray, bw: float | None) -> np.ndarray:
    """Gaussian KDE without scipy (lightweight, sufficient for ridge plots)."""
    n = len(samples)
    if n == 0:
        return np.zeros_like(grid)
    if bw is None:
        # Scott's rule
        bw = 1.06 * float(np.std(samples)) * (n ** (-1 / 5))
        if bw <= 0:
            bw = 1e-3
    diff = (grid[:, None] - samples[None, :]) / bw
    weights = np.exp(-0.5 * diff * diff) / np.sqrt(2 * np.pi)
    return weights.sum(axis=1) / (n * bw)


def render_ridge_plot(spec: RidgePlotSpec, *, theme: str = "dark") -> go.Figure:
    if not spec.distributions:
        fig = go.Figure()
        fig.update_layout(title=spec.title or "(no data)")
        return apply_theme(fig, theme=theme, height=spec.height)

    fig = go.Figure()

    # 1. domain (shared x grid)
    all_samples = np.concatenate(
        [np.asarray(r.samples, dtype=float) for r in spec.distributions
         if len(r.samples) > 0]
    )
    if len(all_samples) == 0:
        fig.update_layout(title=spec.title or "(empty samples)")
        return apply_theme(fig, theme=theme, height=spec.height)

    x_min, x_max = float(all_samples.min()), float(all_samples.max())
    pad = 0.05 * (x_max - x_min if x_max > x_min else 1.0)
    grid = np.linspace(x_min - pad, x_max + pad, spec.n_grid)

    # 2. precompute densities to find global ymax for ridge spacing
    densities: list[np.ndarray] = []
    for row in spec.distributions:
        s = np.asarray(row.samples, dtype=float)
        densities.append(_kde(s, grid, spec.bandwidth))
    ymax_global = max((float(d.max()) for d in densities if d.size), default=1.0)

    # 3. shared null distribution (drawn under every row)
    null_density = None
    if spec.null_distribution is not None and len(spec.null_distribution) > 0:
        null_density = _kde(np.asarray(spec.null_distribution, dtype=float),
                            grid, spec.bandwidth)
        null_density = null_density / max(null_density.max(), 1e-9) * ymax_global

    # 4. each row, stacked top-to-bottom
    for idx, (row, dens) in enumerate(zip(spec.distributions, densities)):
        color = row.color or spec.default_palette[idx % len(spec.default_palette)]
        offset = (len(spec.distributions) - idx - 1) * ymax_global * (1 - spec.overlap)
        y_top = dens + offset

        # null layer beneath this row
        if null_density is not None:
            fig.add_trace(
                go.Scatter(
                    x=grid,
                    y=null_density + offset,
                    mode="lines",
                    line=dict(color="#a0a0a0", width=0.8),
                    fill="tonexty",
                    fillcolor=hex_to_rgba("#a0a0a0", 0.08),
                    hoverinfo="skip",
                    showlegend=(idx == 0),
                    name=spec.null_label,
                )
            )

        # IQR shading
        s = np.asarray(row.samples, dtype=float)
        if s.size:
            q_lo = float(np.quantile(s, spec.quantile_band[0]))
            q_hi = float(np.quantile(s, spec.quantile_band[1]))
            mask = (grid >= q_lo) & (grid <= q_hi)
            if mask.any():
                fig.add_trace(
                    go.Scatter(
                        x=grid[mask],
                        y=(dens + offset)[mask],
                        mode="lines",
                        line=dict(width=0),
                        fill="tonexty",
                        fillcolor=hex_to_rgba(color, 0.45),
                        hoverinfo="skip",
                        showlegend=False,
                    )
                )

        # main density curve (filled)
        n_str = f" (n={row.n:,})" if row.n is not None else ""
        fig.add_trace(
            go.Scatter(
                x=grid,
                y=y_top,
                mode="lines",
                line=dict(color=color, width=1.5),
                fill="tonexty",
                fillcolor=hex_to_rgba(color, 0.20),
                name=f"{row.label}{n_str}",
                hovertemplate=f"{row.label}: x=%{{x:.3f}}<extra></extra>",
            )
        )

    # 5. y-axis: hide ticks (offsets are not meaningful)
    fig.update_yaxes(showticklabels=False, showgrid=False, zeroline=False)
    fig.update_layout(
        title=spec.title,
        xaxis_title=spec.x_label,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    return apply_theme(fig, theme=theme, height=spec.height)
