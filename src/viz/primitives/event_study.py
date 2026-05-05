"""P3: EventStudyPanel — dynamic effect at lead/lag periods around t=0.

Plots point estimates with bootstrap CI bands. Renders a vertical
line at the treatment period, normalizes a chosen pre-period to zero,
and overlays placebo runs as faint gray lines if provided.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import plotly.graph_objects as go

from ..ci import add_ci_band
from ..palettes import OKABE_ITO_DARK
from ..theme import apply_theme


@dataclass(frozen=True)
class EventStudySpec:
    leads_lags: Sequence[int]          # e.g. [-5, -4, ..., 5]
    estimates: Sequence[float]
    ci_lo: Sequence[float]
    ci_hi: Sequence[float]
    placebo_runs: list[Sequence[float]] | None = None
    title: str = ""
    x_label: str = "lead / lag (period)"
    y_label: str = "estimated effect"
    treatment_label: str = "t = 0 (treatment)"
    pre_period_normalization: int | None = -1
    height: int = 480
    color: str = OKABE_ITO_DARK[2]
    placebo_color: str = "#a0a0a0"
    placebo_opacity: float = 0.18


def _validate_lengths(spec: EventStudySpec) -> None:
    n = len(spec.leads_lags)
    if not (n == len(spec.estimates) == len(spec.ci_lo) == len(spec.ci_hi)):
        raise ValueError("leads_lags / estimates / ci_lo / ci_hi length mismatch")


def render_event_study(spec: EventStudySpec, *, theme: str = "dark") -> go.Figure:
    if not spec.leads_lags:
        fig = go.Figure()
        fig.update_layout(title=spec.title or "(no data)")
        return apply_theme(fig, theme=theme, height=spec.height)

    _validate_lengths(spec)

    fig = go.Figure()

    # 1. placebo runs (background, before observed)
    if spec.placebo_runs:
        for placebo in spec.placebo_runs:
            if len(placebo) != len(spec.leads_lags):
                continue
            fig.add_trace(
                go.Scatter(
                    x=list(spec.leads_lags),
                    y=list(placebo),
                    mode="lines",
                    line=dict(color=spec.placebo_color, width=1),
                    opacity=spec.placebo_opacity,
                    hoverinfo="skip",
                    showlegend=False,
                )
            )

    # 2. CI band
    add_ci_band(
        fig,
        x=list(spec.leads_lags),
        lo=list(spec.ci_lo),
        hi=list(spec.ci_hi),
        color=spec.color,
        name="95% CI",
        theme=theme,
    )

    # 3. point estimate line + markers
    fig.add_trace(
        go.Scatter(
            x=list(spec.leads_lags),
            y=list(spec.estimates),
            mode="lines+markers",
            line=dict(color=spec.color, width=2),
            marker=dict(color=spec.color, size=8),
            name="観測値",
            hovertemplate="lead/lag=%{x}<br>effect=%{y:.4f}<extra></extra>",
        )
    )

    # 4. treatment vline + zero hline
    fig.add_vline(
        x=0,
        line_dash="solid",
        line_color="rgba(255,255,255,0.4)",
        annotation_text=spec.treatment_label,
        annotation_position="top right",
    )
    fig.add_hline(y=0, line_dash="dot", line_color="rgba(255,255,255,0.3)")

    # 5. pre-period normalization marker
    if spec.pre_period_normalization is not None and \
            spec.pre_period_normalization in spec.leads_lags:
        fig.add_annotation(
            x=spec.pre_period_normalization,
            y=0,
            text=f"pre-period (t={spec.pre_period_normalization}) を 0 化",
            showarrow=True,
            arrowhead=2,
            ax=0,
            ay=-32,
            font=dict(size=9, color="#a0a0c0"),
            bgcolor="rgba(0,0,0,0.4)",
            bordercolor="rgba(255,255,255,0.15)",
            borderwidth=1,
            borderpad=3,
        )

    fig.update_layout(
        title=spec.title,
        xaxis_title=spec.x_label,
        yaxis_title=spec.y_label,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    return apply_theme(fig, theme=theme, height=spec.height)
