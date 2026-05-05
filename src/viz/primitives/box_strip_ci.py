"""P6: BoxStripCI — box + raw-point strip + 95% CI mark per group.

Combines distribution summary (box), raw observations (jittered strip),
and analytical CI (right-side marker) so the reader sees the
distribution shape AND the inferential interval in one chart.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import numpy as np
import plotly.graph_objects as go

from ..palettes import OKABE_ITO_DARK, hex_to_rgba
from ..theme import apply_theme


@dataclass(frozen=True)
class BoxGroup:
    label: str
    samples: Sequence[float]
    ci_lo: float | None = None
    ci_hi: float | None = None
    n: int | None = None
    color: str | None = None


@dataclass(frozen=True)
class BoxStripCISpec:
    groups: list[BoxGroup]
    title: str = ""
    x_label: str = ""
    y_label: str = ""
    show_strip: bool = True
    strip_max_n: int = 200
    strip_jitter: float = 0.30
    null_median: float | None = None
    null_label: str = "null median"
    height: int = 460
    box_width: float = 0.50
    default_palette: tuple[str, ...] = field(default_factory=lambda: OKABE_ITO_DARK)
    rng_seed: int = 42


def render_box_strip_ci(spec: BoxStripCISpec, *, theme: str = "dark") -> go.Figure:
    if not spec.groups:
        fig = go.Figure()
        fig.update_layout(title=spec.title or "(no data)")
        return apply_theme(fig, theme=theme, height=spec.height)

    rng = np.random.default_rng(spec.rng_seed)
    fig = go.Figure()

    for idx, grp in enumerate(spec.groups):
        color = grp.color or spec.default_palette[idx % len(spec.default_palette)]
        s = np.asarray(grp.samples, dtype=float)

        # 1. box
        fig.add_trace(
            go.Box(
                y=s,
                x=[grp.label] * len(s),
                name=grp.label,
                marker_color=color,
                line_color=color,
                fillcolor=hex_to_rgba(color, 0.18),
                width=spec.box_width,
                boxpoints=False,  # use strip below instead
                showlegend=False,
                hovertemplate=f"<b>{grp.label}</b><br>"
                              "Q1=%{q1:.3f} Q3=%{q3:.3f}<br>"
                              "median=%{median:.3f}<extra></extra>",
            )
        )

        # 2. strip (subsampled raw points; plotly applies categorical jitter
        #    automatically when x is a category list, so we only subsample)
        if spec.show_strip and s.size:
            sample = s if s.size <= spec.strip_max_n \
                else rng.choice(s, size=spec.strip_max_n, replace=False)
            xs = [grp.label] * sample.size
            fig.add_trace(
                go.Scatter(
                    x=xs,
                    y=sample,
                    mode="markers",
                    marker=dict(
                        color=hex_to_rgba(color, 0.45),
                        size=4,
                        line=dict(width=0),
                    ),
                    hoverinfo="skip",
                    showlegend=False,
                )
            )

        # 3. 95% CI marker on the right side of the box
        if grp.ci_lo is not None and grp.ci_hi is not None:
            mid = float(np.mean(s)) if s.size else (grp.ci_lo + grp.ci_hi) / 2
            fig.add_trace(
                go.Scatter(
                    x=[grp.label],
                    y=[mid],
                    mode="markers",
                    marker=dict(
                        color="#ffffff",
                        size=10,
                        symbol="diamond-open",
                        line=dict(width=2, color="#ffffff"),
                    ),
                    error_y=dict(
                        type="data",
                        symmetric=False,
                        array=[grp.ci_hi - mid],
                        arrayminus=[mid - grp.ci_lo],
                        color="#ffffff",
                        thickness=2,
                        width=8,
                    ),
                    hovertemplate=f"<b>{grp.label}</b> 95% CI<br>"
                                  f"[{grp.ci_lo:.3f}, {grp.ci_hi:.3f}]<extra></extra>",
                    showlegend=False,
                )
            )

        # 4. n annotation under the box
        if grp.n is not None:
            fig.add_annotation(
                x=grp.label,
                yref="paper",
                y=-0.06,
                text=f"n={grp.n:,}",
                showarrow=False,
                font=dict(size=10, color="#a0a0c0"),
            )

    # 5. null median line
    if spec.null_median is not None:
        fig.add_hline(
            y=spec.null_median,
            line_dash="dash",
            line_color="#a0a0a0",
            annotation_text=spec.null_label,
            annotation_position="top right",
        )

    fig.update_layout(
        title=spec.title,
        xaxis_title=spec.x_label,
        yaxis_title=spec.y_label,
        boxmode="group",
    )
    return apply_theme(fig, theme=theme, height=spec.height)
