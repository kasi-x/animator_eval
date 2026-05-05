"""P2: KMCurve — Kaplan–Meier survival curves with Greenwood CI bands.

Strata are overlaid on a single panel using step plots. A risk table
(at-risk counts at each evaluation time) is rendered as a subplot
beneath the survival panel. Optional permutation null envelope.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ..palettes import OKABE_ITO_DARK, hex_to_rgba
from ..theme import apply_theme


@dataclass(frozen=True)
class KMStratum:
    label: str
    timeline: Sequence[float]
    survival: Sequence[float]
    ci_lo: Sequence[float] | None = None
    ci_hi: Sequence[float] | None = None
    n_at_risk: Sequence[int] | None = None
    median_survival: float | None = None
    color: str | None = None
    n: int | None = None


@dataclass(frozen=True)
class NullSeries:
    timeline: Sequence[float]
    lo: Sequence[float]
    hi: Sequence[float]
    label: str = "permutation null 95%"


@dataclass(frozen=True)
class KMCurveSpec:
    strata: list[KMStratum]
    title: str = ""
    x_label: str = "経過時間 t"
    y_label: str = "S(t)"
    null_envelope: NullSeries | None = None
    risk_table: bool = True
    risk_table_times: Sequence[float] | None = None  # when None, auto 5 ticks
    median_marker: bool = True
    height: int = 540
    risk_table_height_frac: float = 0.22
    default_palette: tuple[str, ...] = field(default_factory=lambda: OKABE_ITO_DARK)


def _at_risk_at(stratum: KMStratum, t: float) -> int | None:
    if stratum.n_at_risk is None or not stratum.timeline:
        return None
    # nearest index with timeline value <= t
    last_n = None
    for ti, ni in zip(stratum.timeline, stratum.n_at_risk):
        if ti <= t:
            last_n = ni
        else:
            break
    return last_n


def _auto_risk_times(strata: list[KMStratum], n_ticks: int = 5) -> list[float]:
    all_t = [t for s in strata for t in s.timeline]
    if not all_t:
        return []
    t_min, t_max = min(all_t), max(all_t)
    step = (t_max - t_min) / (n_ticks - 1) if n_ticks > 1 else 0
    return [round(t_min + i * step, 2) for i in range(n_ticks)]


def render_km_curve(spec: KMCurveSpec, *, theme: str = "dark") -> go.Figure:
    """Build a KM survival panel (and optional risk-table subplot)."""
    if not spec.strata:
        fig = go.Figure()
        fig.update_layout(title=spec.title or "(no data)")
        return apply_theme(fig, theme=theme, height=spec.height)

    if spec.risk_table:
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            row_heights=[1 - spec.risk_table_height_frac, spec.risk_table_height_frac],
            vertical_spacing=0.05,
            subplot_titles=("", "at-risk"),
        )
        main_kw = dict(row=1, col=1)
    else:
        fig = go.Figure()
        main_kw = {}

    # 1. null envelope (background)
    if spec.null_envelope is not None:
        env = spec.null_envelope
        # Use add_ci_band logic but through plotly directly because
        # we need precise subplot routing.
        x_poly = list(env.timeline) + list(reversed(list(env.timeline)))
        y_poly = list(env.hi) + list(reversed(list(env.lo)))
        fig.add_trace(
            go.Scatter(
                x=x_poly,
                y=y_poly,
                fill="toself",
                fillcolor=hex_to_rgba("#a0a0a0", 0.10),
                line=dict(width=0),
                mode="lines",
                hoverinfo="skip",
                name=env.label,
                showlegend=True,
            ),
            **main_kw,
        )

    # 2. stratum step + CI band
    for idx, stratum in enumerate(spec.strata):
        color = stratum.color or spec.default_palette[idx % len(spec.default_palette)]
        if stratum.ci_lo is not None and stratum.ci_hi is not None:
            x_poly = list(stratum.timeline) + list(reversed(list(stratum.timeline)))
            y_poly = list(stratum.ci_hi) + list(reversed(list(stratum.ci_lo)))
            fig.add_trace(
                go.Scatter(
                    x=x_poly,
                    y=y_poly,
                    fill="toself",
                    fillcolor=hex_to_rgba(color, 0.18),
                    line=dict(width=0),
                    mode="lines",
                    hoverinfo="skip",
                    showlegend=False,
                    legendgroup=stratum.label,
                ),
                **main_kw,
            )

        n_str = f" (n={stratum.n:,})" if stratum.n is not None else ""
        fig.add_trace(
            go.Scatter(
                x=list(stratum.timeline),
                y=list(stratum.survival),
                mode="lines",
                line=dict(color=color, width=2, shape="hv"),
                name=f"{stratum.label}{n_str}",
                legendgroup=stratum.label,
                hovertemplate=f"<b>{stratum.label}</b><br>t=%{{x}}<br>S(t)=%{{y:.3f}}<extra></extra>",
            ),
            **main_kw,
        )

        # Median survival drop marker
        if spec.median_marker and stratum.median_survival is not None:
            fig.add_trace(
                go.Scatter(
                    x=[stratum.median_survival, stratum.median_survival],
                    y=[0, 0.5],
                    mode="lines",
                    line=dict(color=color, width=1, dash="dot"),
                    hovertemplate=f"{stratum.label} median = %{{x:.2f}}<extra></extra>",
                    showlegend=False,
                    legendgroup=stratum.label,
                ),
                **main_kw,
            )

    # 3. risk table subplot
    if spec.risk_table:
        risk_times = list(spec.risk_table_times) if spec.risk_table_times \
            else _auto_risk_times(spec.strata)
        for idx, stratum in enumerate(spec.strata):
            ys = [stratum.label] * len(risk_times)
            counts = [_at_risk_at(stratum, t) for t in risk_times]
            color = stratum.color or spec.default_palette[idx % len(spec.default_palette)]
            text = [str(c) if c is not None else "" for c in counts]
            fig.add_trace(
                go.Scatter(
                    x=risk_times,
                    y=ys,
                    mode="text",
                    text=text,
                    textfont=dict(color=color, size=11),
                    hoverinfo="skip",
                    showlegend=False,
                ),
                row=2,
                col=1,
            )
        fig.update_yaxes(autorange="reversed", row=2, col=1, showgrid=False)
        fig.update_xaxes(showticklabels=False, row=2, col=1)

    # 4. layout
    fig.update_layout(
        title=spec.title,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    if spec.risk_table:
        fig.update_yaxes(title_text=spec.y_label, range=[0, 1.02], row=1, col=1)
        fig.update_xaxes(title_text=spec.x_label, row=2, col=1)
    else:
        fig.update_yaxes(title_text=spec.y_label, range=[0, 1.02])
        fig.update_xaxes(title_text=spec.x_label)

    return apply_theme(fig, theme=theme, height=spec.height)
