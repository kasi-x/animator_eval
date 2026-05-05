"""P1: CIScatter — point estimate + interval (forest plot included).

A spec-driven wrapper around plotly that:
- Always draws CI whiskers when ``ci_lo`` / ``ci_hi`` are present.
- Optionally overlays a null reference line (e.g. HR=1) and/or a
  null band (e.g. permutation P2.5–P97.5 around the reference).
- Encodes statistical significance via marker fill (filled vs hollow).
- Supports log-x axis (forest plot convention for hazard / odds ratios).
- Sorts rows by x / label / p value.
- Adds a shrinkage badge when ``shrinkage`` is set.

Use this primitive in any report producing a forest-plot-like chart.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import plotly.graph_objects as go

from ..null_overlay import add_null_reference_line
from ..palettes import OKABE_ITO_DARK, hex_to_rgba
from ..shrinkage_badge import add_shrinkage_badge
from ..theme import apply_theme


@dataclass(frozen=True)
class CIPoint:
    label: str
    x: float
    ci_lo: float
    ci_hi: float
    p_value: float | None = None
    color: str | None = None
    n: int | None = None


@dataclass(frozen=True)
class ShrinkageInfo:
    method: str
    n_threshold: int | None = None


@dataclass(frozen=True)
class CIScatterSpec:
    points: list[CIPoint]
    x_label: str
    y_label: str = ""
    title: str = ""
    log_x: bool = False
    reference: float | None = None
    reference_label: str = "null"
    null_band: tuple[float, float] | None = None
    null_band_label: str = "null 95%"
    shrinkage: ShrinkageInfo | None = None
    sort_by: Literal["x", "label", "p", "input"] = "x"
    significance_threshold: float = 0.05
    default_color: str = OKABE_ITO_DARK[2]  # sky blue on dark
    marker_size: int = 10
    line_width: int = 2
    height_per_row: int = 32
    height_min: int = 360
    extra_annotations: list[dict] = field(default_factory=list)


def _sort_points(spec: CIScatterSpec) -> list[CIPoint]:
    pts = list(spec.points)
    if spec.sort_by == "x":
        return sorted(pts, key=lambda p: p.x)
    if spec.sort_by == "label":
        return sorted(pts, key=lambda p: p.label)
    if spec.sort_by == "p":
        return sorted(pts, key=lambda p: (p.p_value if p.p_value is not None else 1.0))
    return pts


def _is_significant(p: CIPoint, threshold: float) -> bool:
    return p.p_value is not None and p.p_value < threshold


def _row_height(n_rows: int, spec: CIScatterSpec) -> int:
    return max(spec.height_min, n_rows * spec.height_per_row + 120)


def render_ci_scatter(spec: CIScatterSpec, *, theme: str = "dark") -> go.Figure:
    """Build a forest-plot-style figure from a ``CIScatterSpec``.

    The returned ``go.Figure`` already has the theme applied; callers
    typically pass it to ``viz.embed(fig, div_id)``.
    """
    pts = _sort_points(spec)
    if not pts:
        fig = go.Figure()
        fig.update_layout(
            title=spec.title or "(no data)",
            annotations=[
                dict(
                    text="データなし",
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                    font=dict(size=14, color="#a0a0c0"),
                )
            ],
        )
        return apply_theme(fig, theme=theme, height=spec.height_min)

    labels = [p.label for p in pts]

    # 1. null band (background, drawn first so observed sits on top)
    fig = go.Figure()
    if spec.null_band is not None:
        # vertical band spanning all rows of the y-axis (categorical)
        lo, hi = spec.null_band
        x_poly = [lo, hi, hi, lo]
        y_poly = [-0.5, -0.5, len(labels) - 0.5, len(labels) - 0.5]
        fig.add_trace(
            go.Scatter(
                x=x_poly,
                y=y_poly,
                fill="toself",
                fillcolor=hex_to_rgba("#a0a0a0", 0.10),
                line=dict(width=0),
                mode="lines",
                hoverinfo="skip",
                name=spec.null_band_label,
                showlegend=True,
            )
        )

    # 2. observed points + CI whiskers (one trace per significance group
    #    so legend can split filled vs hollow)
    sig_pts = [p for p in pts if _is_significant(p, spec.significance_threshold)]
    nonsig_pts = [p for p in pts if not _is_significant(p, spec.significance_threshold)]

    def _add_group(group: list[CIPoint], *, sig: bool) -> None:
        if not group:
            return
        xs = [p.x for p in group]
        ys = [p.label for p in group]
        lo = [p.ci_lo for p in group]
        hi = [p.ci_hi for p in group]
        colors = [
            (p.color if p.color is not None else spec.default_color) for p in group
        ]
        # error bars must be lists of arrays per plotly; use single color when all match
        single_color = colors[0] if len(set(colors)) == 1 else None
        marker = dict(
            size=spec.marker_size,
            symbol="square" if sig else "square-open",
            color=colors if single_color is None else single_color,
            line=dict(
                width=2,
                color=colors if single_color is None else single_color,
            ),
        )
        hover = []
        for p in group:
            parts = [f"<b>{p.label}</b>", f"x={p.x:.3f}", f"CI=[{p.ci_lo:.3f}, {p.ci_hi:.3f}]"]
            if p.p_value is not None:
                parts.append(f"p={p.p_value:.3g}")
            if p.n is not None:
                parts.append(f"n={p.n:,}")
            hover.append("<br>".join(parts))
        fig.add_trace(
            go.Scatter(
                x=xs,
                y=ys,
                mode="markers",
                marker=marker,
                error_x=dict(
                    type="data",
                    symmetric=False,
                    array=[hi[i] - xs[i] for i in range(len(group))],
                    arrayminus=[xs[i] - lo[i] for i in range(len(group))],
                    color=single_color or "rgba(255,255,255,0.5)",
                    thickness=spec.line_width,
                    width=6,
                ),
                name=("有意 (p<{:g})".format(spec.significance_threshold)) if sig
                else "非有意",
                hovertext=hover,
                hoverinfo="text",
                showlegend=True,
            )
        )

    _add_group(nonsig_pts, sig=False)
    _add_group(sig_pts, sig=True)

    # 3. reference line (HR=1, etc.)
    if spec.reference is not None:
        add_null_reference_line(
            fig,
            spec.reference,
            label=f"{spec.reference_label} = {spec.reference:g}",
            direction="v",
            theme=theme,
            annotation_position="top right",
        )

    # 4. layout
    fig.update_layout(
        title=spec.title,
        xaxis_title=spec.x_label,
        yaxis_title=spec.y_label,
        margin=dict(l=200, r=40, t=64, b=56),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    if spec.log_x:
        fig.update_xaxes(type="log")
    fig.update_yaxes(autorange="reversed", categoryorder="array", categoryarray=labels)

    # 5. shrinkage badge
    if spec.shrinkage is not None:
        add_shrinkage_badge(
            fig,
            method=spec.shrinkage.method,
            n_threshold=spec.shrinkage.n_threshold,
        )

    # 6. extra annotations passthrough
    for ann in spec.extra_annotations:
        fig.add_annotation(**ann)

    return apply_theme(fig, theme=theme, height=_row_height(len(pts), spec))
