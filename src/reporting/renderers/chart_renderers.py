"""Map each concrete ChartSpec to a ``plotly.graph_objects.Figure``.

The renderer dispatches on ``isinstance``.  Each function takes the spec and a
data dict (keyed by the spec's ``data_key``) and returns a ``go.Figure``.
The caller (the HTML assembler) then passes the figure through
``plotly_div_safe`` to produce an embeddable ``<div>``.

Colour palette and layout defaults are intentionally aligned with the dark
theme already used by the legacy ``generate_all_reports.py`` reports.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import plotly.graph_objects as go

from src.reporting.specs.chart import (
    BarSpec,
    BoxSpec,
    ChartSpec,
    ForestSpec,
    HeatmapSpec,
    HistogramSpec,
    LineSpec,
    RidgeSpec,
    SankeySpec,
    ScatterSpec,
    ViolinSpec,
)

# Shared palette — same values used in helpers.py
_PALETTE = [
    "#667eea", "#f093fb", "#06D6A0", "#FFD166",
    "#EF476F", "#a0d2db", "#26547C", "#F72585",
    "#4CC9F0", "#7209B7",
]


# ---------------------------------------------------------------------------
# Individual renderers
# ---------------------------------------------------------------------------

def _render_scatter(spec: ScatterSpec, rows: list[dict[str, Any]]) -> go.Figure:
    x = [r[spec.x_field] for r in rows]
    y = [r[spec.y_field] for r in rows]

    marker: dict[str, Any] = {"color": "#667eea", "size": 6, "opacity": 0.7}
    if spec.color_field:
        marker["color"] = [r.get(spec.color_field, 0) for r in rows]
        marker["colorscale"] = "Viridis"
        marker["showscale"] = True
    if spec.size_field:
        raw_sizes = [r.get(spec.size_field, 5) for r in rows]
        marker["size"] = [max(3, min(s, 30)) for s in raw_sizes]

    text = None
    if spec.label_field and spec.label_top_n:
        vals = [(r.get(spec.y_field, 0), r.get(spec.label_field, "")) for r in rows]
        vals.sort(key=lambda t: -t[0])
        top_labels = {v[1] for v in vals[: spec.label_top_n]}
        text = [r.get(spec.label_field, "") if r.get(spec.label_field, "") in top_labels else "" for r in rows]

    fig = go.Figure(
        go.Scatter(x=x, y=y, mode="markers", marker=marker, text=text, textposition="top center")
    )
    fig.update_layout(
        title=spec.title,
        xaxis_title=spec.xlabel or spec.x_field,
        yaxis_title=spec.ylabel or spec.y_field,
        height=spec.height,
    )
    return fig


def _render_bar(spec: BarSpec, rows: list[dict[str, Any]]) -> go.Figure:
    cats = [r[spec.category_field] for r in rows]
    vals = [r[spec.value_field] for r in rows]

    error: dict[str, Any] | None = None
    if spec.error_field:
        errs = [r.get(spec.error_field, 0) for r in rows]
        if spec.orientation == "h":
            error = {"type": "data", "array": errs, "visible": True}
        else:
            error = {"type": "data", "array": errs, "visible": True}

    if spec.orientation == "h":
        fig = go.Figure(go.Bar(y=cats, x=vals, orientation="h", marker_color="#667eea", error_x=error))
    else:
        fig = go.Figure(go.Bar(x=cats, y=vals, marker_color="#667eea", error_y=error))

    fig.update_layout(
        title=spec.title,
        xaxis_title=spec.xlabel,
        yaxis_title=spec.ylabel,
        height=spec.height,
    )
    return fig


def _render_forest(spec: ForestSpec, estimates: list[dict[str, Any]]) -> go.Figure:
    names = [e["name"] for e in estimates]
    ests = [e["estimate"] for e in estimates]
    ci_lo = [e["estimate"] - e["ci_lower"] for e in estimates]
    ci_hi = [e["ci_upper"] - e["estimate"] for e in estimates]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=ests,
            y=names,
            mode="markers",
            marker={"size": 10, "color": "#667eea"},
            error_x={
                "type": "data",
                "symmetric": False,
                "array": ci_hi,
                "arrayminus": ci_lo,
                "color": "rgba(102,126,234,0.5)",
                "thickness": 2,
                "width": 6,
            },
            showlegend=False,
        )
    )
    fig.add_vline(x=0, line_dash="dash", line_color="rgba(255,255,255,0.4)")
    fig.update_layout(
        title=spec.title,
        xaxis_title=spec.xlabel,
        height=spec.height or max(300, len(estimates) * 40 + 100),
        yaxis={"categoryorder": "array", "categoryarray": list(reversed(names))},
    )
    return fig


def _render_violin(spec: ViolinSpec, rows: list[dict[str, Any]]) -> go.Figure:
    groups = [r[spec.group_field] for r in rows]
    vals = [r[spec.value_field] for r in rows]

    fig = go.Figure(
        go.Violin(
            x=groups,
            y=vals,
            line_color="#667eea",
            fillcolor="rgba(102,126,234,0.3)",
            meanline_visible=True,
            scalemode="width",
        )
    )
    fig.update_layout(
        title=spec.title,
        xaxis_title=spec.xlabel,
        yaxis_title=spec.ylabel or spec.value_field,
        height=spec.height,
    )
    return fig


def _render_ridge(spec: RidgeSpec, rows: list[dict[str, Any]]) -> go.Figure:
    # Group data by group_field
    grouped: dict[str, list[float]] = {}
    for r in rows:
        g = r[spec.group_field]
        grouped.setdefault(g, []).append(r[spec.value_field])

    fig = go.Figure()
    group_names = list(grouped.keys())
    for i, (name, vals) in enumerate(grouped.items()):
        arr = np.array([v for v in vals if v is not None and np.isfinite(v)])
        if len(arr) < 2:
            continue
        fig.add_trace(
            go.Violin(
                x=arr.tolist(),
                y0=name,
                name=name,
                side="positive",
                meanline_visible=True,
                line_color=_PALETTE[i % len(_PALETTE)],
                fillcolor=_PALETTE[i % len(_PALETTE)],
                opacity=0.65,
                spanmode="soft",
                scalemode="width",
                width=0.8,
            )
        )

    fig.update_layout(
        title=spec.title,
        xaxis_title=spec.xlabel,
        showlegend=False,
        height=spec.height,
        violingap=0.05,
        violinmode="overlay",
        yaxis={"categoryorder": "array", "categoryarray": list(reversed(group_names))},
    )
    return fig


def _render_heatmap(spec: HeatmapSpec, rows: list[dict[str, Any]]) -> go.Figure:
    x = [r[spec.x_field] for r in rows]
    y = [r[spec.y_field] for r in rows]
    z = [r[spec.z_field] for r in rows]

    # Build unique sorted axes
    x_labels = sorted(set(x), key=str)
    y_labels = sorted(set(y), key=str)
    x_idx = {v: i for i, v in enumerate(x_labels)}
    y_idx = {v: i for i, v in enumerate(y_labels)}

    matrix = [[None] * len(x_labels) for _ in range(len(y_labels))]
    for xi, yi, zi in zip(x, y, z):
        matrix[y_idx[yi]][x_idx[xi]] = zi

    fig = go.Figure(
        go.Heatmap(
            z=matrix,
            x=[str(v) for v in x_labels],
            y=[str(v) for v in y_labels],
            colorscale=spec.colorscale,
        )
    )
    fig.update_layout(
        title=spec.title,
        xaxis_title=spec.xlabel,
        yaxis_title=spec.ylabel,
        height=spec.height,
    )
    return fig


def _render_line(spec: LineSpec, rows: list[dict[str, Any]]) -> go.Figure:
    fig = go.Figure()

    if spec.series_field:
        series: dict[str, tuple[list, list]] = {}
        for r in rows:
            s = r[spec.series_field]
            xs, ys = series.setdefault(s, ([], []))
            xs.append(r[spec.x_field])
            ys.append(r[spec.y_field])
        for i, (name, (xs, ys)) in enumerate(series.items()):
            fig.add_trace(
                go.Scatter(x=xs, y=ys, mode="lines+markers", name=str(name), line_color=_PALETTE[i % len(_PALETTE)])
            )
    else:
        x = [r[spec.x_field] for r in rows]
        y = [r[spec.y_field] for r in rows]
        fig.add_trace(go.Scatter(x=x, y=y, mode="lines+markers", line_color="#667eea"))

    fig.update_layout(
        title=spec.title,
        xaxis_title=spec.xlabel or spec.x_field,
        yaxis_title=spec.ylabel or spec.y_field,
        height=spec.height,
    )
    return fig


def _render_sankey(spec: SankeySpec, data: dict[str, Any]) -> go.Figure:
    nodes = data.get("nodes", [])
    links = data.get("links", [])

    fig = go.Figure(
        go.Sankey(
            node={
                "label": [n.get("name", n.get("label", "")) for n in nodes],
                "color": [n.get("color", "#667eea") for n in nodes],
            },
            link={
                "source": [lnk["source"] for lnk in links],
                "target": [lnk["target"] for lnk in links],
                "value": [lnk["value"] for lnk in links],
            },
        )
    )
    fig.update_layout(title=spec.title, height=spec.height)
    return fig


def _render_box(spec: BoxSpec, rows: list[dict[str, Any]]) -> go.Figure:
    groups = [r[spec.group_field] for r in rows]
    vals = [r[spec.value_field] for r in rows]

    fig = go.Figure(
        go.Box(x=groups, y=vals, marker_color="#667eea", boxpoints="outliers")
    )
    fig.update_layout(
        title=spec.title,
        xaxis_title=spec.xlabel,
        yaxis_title=spec.ylabel or spec.value_field,
        height=spec.height,
    )
    return fig


def _render_histogram(spec: HistogramSpec, rows: list[dict[str, Any]]) -> go.Figure:
    vals = [r[spec.value_field] for r in rows]

    kwargs: dict[str, Any] = {"x": vals, "marker_color": "#667eea"}
    if spec.nbins is not None:
        kwargs["nbinsx"] = spec.nbins

    fig = go.Figure(go.Histogram(**kwargs))
    fig.update_layout(
        title=spec.title,
        xaxis_title=spec.xlabel or spec.value_field,
        yaxis_title=spec.ylabel or "度数",
        height=spec.height,
    )
    return fig


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

_RENDERERS = {
    ScatterSpec: _render_scatter,
    BarSpec: _render_bar,
    ForestSpec: _render_forest,
    ViolinSpec: _render_violin,
    RidgeSpec: _render_ridge,
    HeatmapSpec: _render_heatmap,
    LineSpec: _render_line,
    SankeySpec: _render_sankey,
    BoxSpec: _render_box,
    HistogramSpec: _render_histogram,
}


def render_chart(spec: ChartSpec, data: dict[str, Any]) -> go.Figure:
    """Render a ChartSpec into a Plotly Figure.

    ``data`` is the full provider dict — the chart's ``data_key`` selects the
    relevant subset.
    """
    renderer = _RENDERERS.get(type(spec))
    if renderer is None:
        raise TypeError(f"No renderer for chart type {type(spec).__name__}")
    chart_data = data.get(spec.data_key, [])
    return renderer(spec, chart_data)
