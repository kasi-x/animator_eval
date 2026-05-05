"""P8: RadialNetwork — ego-network local view.

Places an ego node at the center and arranges its neighbors clockwise
on a circle, sorted by edge weight. Edge thickness reflects weight,
edge opacity is inversely proportional to CI width (wider CI → fainter
edge). Optional null density shading as concentric rings.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Literal

import plotly.graph_objects as go

from ..palettes import OKABE_ITO_DARK, hex_to_rgba
from ..theme import apply_theme


@dataclass(frozen=True)
class Neighbor:
    label: str
    edge_weight: float
    ci_lo: float | None = None
    ci_hi: float | None = None
    color: str | None = None
    metadata: dict | None = None


@dataclass(frozen=True)
class RadialNetworkSpec:
    ego_label: str
    neighbors: list[Neighbor]
    title: str = ""
    sort_by: Literal["weight", "label"] = "weight"
    max_neighbors: int = 30
    null_density: float | None = None  # null-expected weight envelope (radius)
    height: int = 600
    radius: float = 1.0
    default_palette: tuple[str, ...] = field(default_factory=lambda: OKABE_ITO_DARK)


def _sorted_neighbors(spec: RadialNetworkSpec) -> list[Neighbor]:
    if spec.sort_by == "weight":
        items = sorted(spec.neighbors, key=lambda n: n.edge_weight, reverse=True)
    else:
        items = sorted(spec.neighbors, key=lambda n: n.label)
    return items[: spec.max_neighbors]


def _ci_opacity(neighbor: Neighbor, weight_range: float) -> float:
    if neighbor.ci_lo is None or neighbor.ci_hi is None or weight_range <= 0:
        return 0.7
    width = neighbor.ci_hi - neighbor.ci_lo
    rel = width / weight_range
    # narrow CI → bold edge; wide CI → faint
    return max(0.20, min(0.90, 0.85 - rel))


def render_radial_network(spec: RadialNetworkSpec, *, theme: str = "dark") -> go.Figure:
    if not spec.neighbors:
        fig = go.Figure()
        fig.update_layout(title=spec.title or "(no neighbors)")
        return apply_theme(fig, theme=theme, height=spec.height)

    neighbors = _sorted_neighbors(spec)
    n = len(neighbors)
    weights = [nb.edge_weight for nb in neighbors]
    w_max = max(weights) if weights else 1.0
    w_min = min(weights) if weights else 0.0
    w_range = w_max - w_min if w_max > w_min else 1.0

    fig = go.Figure()

    # 1. null density rings (background)
    if spec.null_density is not None:
        # draw 3 concentric rings at 0.5, 1.0, 1.5 × null_density radius (proxy)
        for k, frac in enumerate((0.5, 1.0, 1.5)):
            r = spec.radius * frac
            theta = [2 * math.pi * i / 64 for i in range(65)]
            xs = [r * math.cos(t) for t in theta]
            ys = [r * math.sin(t) for t in theta]
            fig.add_trace(
                go.Scatter(
                    x=xs,
                    y=ys,
                    mode="lines",
                    line=dict(color="#a0a0a0", width=0.6, dash="dot"),
                    opacity=0.30,
                    hoverinfo="skip",
                    showlegend=(k == 1),
                    name="null density rings" if k == 1 else "",
                )
            )

    # 2. edges (ego → neighbor)
    for idx, nb in enumerate(neighbors):
        angle = -2 * math.pi * idx / n + math.pi / 2  # start at top, clockwise
        x = spec.radius * math.cos(angle)
        y = spec.radius * math.sin(angle)
        color = nb.color or spec.default_palette[idx % len(spec.default_palette)]
        opacity = _ci_opacity(nb, w_range)
        # edge thickness scales 2..8 px with weight
        thickness = 2 + 6 * (nb.edge_weight - w_min) / w_range

        fig.add_trace(
            go.Scatter(
                x=[0, x],
                y=[0, y],
                mode="lines",
                line=dict(color=hex_to_rgba(color, opacity), width=thickness),
                hoverinfo="skip",
                showlegend=False,
            )
        )
        # neighbor node
        ci_text = ""
        if nb.ci_lo is not None and nb.ci_hi is not None:
            ci_text = f"<br>CI=[{nb.ci_lo:.3f}, {nb.ci_hi:.3f}]"
        fig.add_trace(
            go.Scatter(
                x=[x],
                y=[y],
                mode="markers+text",
                marker=dict(
                    color=color,
                    size=10 + 14 * (nb.edge_weight - w_min) / w_range,
                    line=dict(color="#ffffff", width=1),
                ),
                text=[nb.label],
                textposition="top center" if y >= 0 else "bottom center",
                textfont=dict(size=10),
                hovertemplate=f"<b>{nb.label}</b><br>"
                              f"weight={nb.edge_weight:.3f}{ci_text}<extra></extra>",
                showlegend=False,
            )
        )

    # 3. ego at center
    fig.add_trace(
        go.Scatter(
            x=[0],
            y=[0],
            mode="markers+text",
            marker=dict(
                color="#ffffff",
                size=22,
                line=dict(color="#000000", width=2),
            ),
            text=[f"<b>{spec.ego_label}</b>"],
            textposition="middle center",
            textfont=dict(size=11, color="#000000"),
            hoverinfo="text",
            hovertext=spec.ego_label,
            showlegend=False,
        )
    )

    # 4. layout: equal-aspect, hide axes
    pad = spec.radius * 1.5
    fig.update_xaxes(visible=False, range=[-pad, pad], scaleanchor="y", scaleratio=1)
    fig.update_yaxes(visible=False, range=[-pad, pad])
    fig.update_layout(
        title=spec.title,
        showlegend=False,
        plot_bgcolor="rgba(0,0,0,0.05)",
    )
    return apply_theme(fig, theme=theme, height=spec.height)
