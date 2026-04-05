"""PlotlyRenderer — ChartSpec → go.Figure → HTML div.

パイロットスコープ: Scatter, Bar, Violin, Histogram, Line, StackedArea の6タイプ。
"""

from __future__ import annotations

import base64
import math
from typing import Any

import numpy as np
import plotly.graph_objects as go

from src.viz.chart_spec import (
    BarSpec,
    ChartSpec,
    HeatmapSpec,
    HistogramSpec,
    LineSpec,
    ScatterSpec,
    ViolinSpec,
)
from src.viz.helpers.statistics import correlation_annotation, pearson_r
from src.viz.helpers.subsample import raincloud_mode, subsample_arrays


class PlotlyRenderer:
    """ChartSpecをPlotly go.Figureに変換しHTML divとして返す."""

    def __init__(self) -> None:
        self._dispatch: dict[type, Any] = {
            ScatterSpec: self._render_scatter,
            BarSpec: self._render_bar,
            ViolinSpec: self._render_violin,
            HistogramSpec: self._render_histogram,
            LineSpec: self._render_line,
            HeatmapSpec: self._render_heatmap,
        }

    # ── public API ──

    def render(self, spec: ChartSpec) -> go.Figure:
        """ChartSpec → go.Figure."""
        renderer_fn = self._dispatch.get(type(spec))
        if renderer_fn is None:
            raise NotImplementedError(
                f"PlotlyRenderer does not support {type(spec).__name__} yet"
            )
        fig = renderer_fn(spec)
        self._apply_common_layout(fig, spec)
        self._apply_annotations(fig, spec)
        return fig

    def render_to_html_div(self, spec: ChartSpec) -> str:
        """ChartSpec → HTMLフラグメント（base64 + lazy load）."""
        fig = self.render(spec)
        return self._fig_to_safe_div(fig, spec.chart_id, spec.height)

    # ── chart type renderers ──

    def _render_scatter(self, spec: ScatterSpec) -> go.Figure:
        x_t = spec.x
        y_t = spec.y
        labels_t = spec.labels
        cats_t = spec.categories

        # Subsample if too large
        if len(x_t) > spec.max_points:
            arrays = [x_t, y_t]
            if labels_t:
                arrays.append(labels_t)
            if cats_t:
                arrays.append(cats_t)
            result = subsample_arrays(*arrays, max_n=spec.max_points)
            x_t = result[0]
            y_t = result[1]
            idx = 2
            if labels_t:
                labels_t = result[idx]
                idx += 1
            if cats_t:
                cats_t = result[idx]

        x = list(x_t)
        y = list(y_t)
        labels = list(labels_t) if labels_t else None

        fig = go.Figure()

        if cats_t:
            # Grouped scatter
            cats = list(cats_t)
            unique_cats = sorted(set(cats))
            palette = spec.colors.palette
            for ci, cat in enumerate(unique_cats):
                mask = [i for i, c in enumerate(cats) if c == cat]
                fig.add_trace(go.Scattergl(
                    x=[x[i] for i in mask],
                    y=[y[i] for i in mask],
                    mode=spec.mode,
                    name=cat,
                    marker=dict(
                        size=4,
                        opacity=spec.colors.opacity,
                        color=palette[ci % len(palette)],
                    ),
                    text=[labels[i] for i in mask] if labels else None,
                    hovertemplate="<b>%{text}</b><br>x=%{x:.3f}<br>y=%{y:.3f}<extra></extra>" if labels else None,
                ))
        else:
            fig.add_trace(go.Scattergl(
                x=x, y=y,
                mode=spec.mode,
                marker=dict(
                    size=4,
                    opacity=spec.colors.opacity,
                    color=spec.colors.palette[0],
                ),
                text=labels,
                hovertemplate="<b>%{text}</b><br>x=%{x:.3f}<br>y=%{y:.3f}<extra></extra>" if labels else None,
            ))

        # Regression line + correlation annotation
        if spec.show_regression and len(x) >= 3:
            r, p = pearson_r(x, y)
            ann_text = correlation_annotation(x, y)
            # OLS fit
            clean = [(xi, yi) for xi, yi in zip(x, y)
                     if math.isfinite(xi) and math.isfinite(yi)]
            if len(clean) >= 3:
                cx, cy = zip(*clean)
                z = np.polyfit(cx, cy, 1)
                x_line = [min(cx), max(cx)]
                y_line = [z[0] * xi + z[1] for xi in x_line]
                fig.add_trace(go.Scatter(
                    x=x_line, y=y_line,
                    mode="lines",
                    line=dict(color="#FFD166", width=2, dash="dash"),
                    name="OLS",
                    showlegend=False,
                ))
                fig.add_annotation(
                    x=0.02, y=0.98, xref="paper", yref="paper",
                    text=ann_text, showarrow=False,
                    font=dict(size=11, color="#FFD166"),
                    bgcolor="rgba(0,0,0,0.5)", bordercolor="#FFD166",
                    borderwidth=1, borderpad=4,
                )

        # Top-N labels
        if spec.label_top_n > 0 and labels:
            top_idx = sorted(
                range(len(y)),
                key=lambda i: y[i] if math.isfinite(y[i]) else float("-inf"),
                reverse=True,
            )[:spec.label_top_n]
            fig.add_trace(go.Scatter(
                x=[x[i] for i in top_idx],
                y=[y[i] for i in top_idx],
                mode="markers+text",
                text=[labels[i] for i in top_idx],
                textposition="top center",
                textfont=dict(size=9, color="#f093fb"),
                marker=dict(size=8, color="#f093fb", symbol="circle-open"),
                showlegend=False,
            ))

        return fig

    def _render_bar(self, spec: BarSpec) -> go.Figure:
        fig = go.Figure()

        if spec.stacked_series:
            palette = spec.colors.palette
            for i, (name, vals) in enumerate(spec.stacked_series.items()):
                fig.add_trace(go.Bar(
                    x=list(spec.categories) if spec.orientation == "v" else list(vals),
                    y=list(vals) if spec.orientation == "v" else list(spec.categories),
                    name=name,
                    orientation=spec.orientation,
                    marker_color=palette[i % len(palette)],
                ))
            fig.update_layout(barmode=spec.bar_mode)
        else:
            fig.add_trace(go.Bar(
                x=list(spec.categories) if spec.orientation == "v" else list(spec.values),
                y=list(spec.values) if spec.orientation == "v" else list(spec.categories),
                orientation=spec.orientation,
                marker_color=spec.colors.palette[0],
            ))

        return fig

    def _render_violin(self, spec: ViolinSpec) -> go.Figure:
        fig = go.Figure()
        palette = spec.colors.palette

        for i, (name, vals) in enumerate(spec.groups.items()):
            data = list(vals)
            color = palette[i % len(palette)]

            if spec.raincloud:
                mode = raincloud_mode(len(data))
                if mode == "box":
                    fig.add_trace(go.Box(
                        y=data, name=name,
                        marker_color=color,
                        boxpoints="all" if len(data) <= 10 else "outliers",
                    ))
                elif mode == "box_jitter":
                    fig.add_trace(go.Box(
                        y=data, name=name,
                        marker_color=color,
                        boxpoints="all", jitter=0.35, pointpos=-1.5,
                    ))
                else:  # violin
                    fig.add_trace(go.Violin(
                        y=data, name=name,
                        side="positive",
                        line_color=color,
                        fillcolor=color.replace(")", ",0.3)").replace("rgb", "rgba")
                        if color.startswith("rgb") else color,
                        points="all", jitter=0.05,
                        spanmode="hard",
                        box_visible=True,
                        meanline_visible=True,
                    ))
            else:
                fig.add_trace(go.Violin(
                    y=data, name=name,
                    side=spec.side if spec.side != "both" else None,
                    line_color=color,
                    box_visible=spec.show_box,
                    points=spec.show_points if spec.show_points != "none" else False,
                ))

        return fig

    def _render_histogram(self, spec: HistogramSpec) -> go.Figure:
        fig = go.Figure()

        if spec.multi_series:
            palette = spec.colors.palette
            for i, (name, vals) in enumerate(spec.multi_series.items()):
                fig.add_trace(go.Histogram(
                    x=list(vals), name=name,
                    nbinsx=spec.nbins,
                    marker_color=palette[i % len(palette)],
                    opacity=0.6,
                ))
            fig.update_layout(barmode="overlay")
        else:
            fig.add_trace(go.Histogram(
                x=list(spec.values),
                nbinsx=spec.nbins,
                marker_color=spec.colors.palette[0],
            ))

        return fig

    def _render_line(self, spec: LineSpec) -> go.Figure:
        fig = go.Figure()
        palette = spec.colors.palette

        # Stacked area mode
        if spec.stacked and spec.stacked_series:
            x = list(spec.x)
            for i, (name, vals) in enumerate(spec.stacked_series.items()):
                color = palette[i % len(palette)]
                fig.add_trace(go.Scatter(
                    x=x, y=list(vals),
                    name=name,
                    mode="lines",
                    stackgroup="one",
                    line=dict(color=color),
                    hovertemplate="%{x}: " + name + " %{y}<extra></extra>",
                ))
            return fig

        # Standard line mode
        for i, (name, points) in enumerate(spec.series.items()):
            xs = [p[0] for p in points]
            ys = [p[1] for p in points]
            color = palette[i % len(palette)]
            fig.add_trace(go.Scatter(
                x=xs, y=ys,
                mode="lines+markers",
                name=name,
                line=dict(color=color),
                marker=dict(size=4),
            ))

            # CI bands
            if spec.ci_bands and name in spec.ci_bands:
                band = spec.ci_bands[name]
                bx = [b[0] for b in band]
                b_upper = [b[2] for b in band]
                b_lower = [b[1] for b in band]
                fig.add_trace(go.Scatter(
                    x=bx, y=b_upper,
                    mode="lines", line=dict(width=0),
                    showlegend=False,
                ))
                fig.add_trace(go.Scatter(
                    x=bx, y=b_lower,
                    mode="lines", line=dict(width=0),
                    fill="tonexty",
                    fillcolor=f"rgba({_hex_to_rgb(color)},0.15)",
                    showlegend=False,
                ))

        return fig

    def _render_heatmap(self, spec: HeatmapSpec) -> go.Figure:
        fig = go.Figure()
        z = [list(row) for row in spec.z]
        fig.add_trace(go.Heatmap(
            z=z,
            x=list(spec.x_labels),
            y=list(spec.y_labels),
            colorscale=spec.colors.colorscale,
            text=[[f"{v:.2f}" for v in row] for row in z] if spec.show_text else None,
            texttemplate="%{text}" if spec.show_text else None,
            hovertemplate="x=%{x}<br>y=%{y}<br>z=%{z:.3f}<extra></extra>",
            zmid=0,
        ))
        return fig

    # ── common layout & rendering ──

    def _apply_common_layout(self, fig: go.Figure, spec: ChartSpec) -> None:
        layout_kwargs: dict[str, Any] = {
            "height": spec.height,
            "margin": dict(l=60, r=30, t=50, b=50),
            "title": spec.title,
            "xaxis_title": spec.x_axis.label,
            "yaxis_title": spec.y_axis.label,
            "template": "plotly_dark",
            "paper_bgcolor": "rgba(0,0,0,0)",
            "plot_bgcolor": "rgba(0,0,0,0.2)",
            "font": dict(color="#c0c0d0"),
        }
        if spec.x_axis.log_scale:
            layout_kwargs["xaxis_type"] = "log"
        if spec.y_axis.log_scale:
            layout_kwargs["yaxis_type"] = "log"
        if spec.x_axis.range:
            layout_kwargs["xaxis_range"] = list(spec.x_axis.range)
        if spec.y_axis.range:
            layout_kwargs["yaxis_range"] = list(spec.y_axis.range)
        fig.update_layout(**layout_kwargs)

    def _apply_annotations(self, fig: go.Figure, spec: ChartSpec) -> None:
        for ann in spec.annotations:
            fig.add_annotation(
                x=ann.x, y=ann.y,
                xref=ann.x_ref, yref=ann.y_ref,
                text=ann.text,
                showarrow=ann.show_arrow,
                font=dict(size=ann.font_size, color=ann.color),
                bgcolor="rgba(0,0,0,0.5)",
            )

    def _fig_to_safe_div(self, fig: go.Figure, div_id: str, height: int) -> str:
        """go.Figure → base64エンコード + IntersectionObserver lazy load HTML."""
        # Box/Violin traces: subsample >200 points
        for trace in fig.data:
            trace_type = getattr(trace, "type", "")
            if trace_type in ("box", "violin"):
                y_vals = trace.y
                if y_vals is not None and hasattr(y_vals, "__len__") and len(y_vals) > 200:
                    n = len(y_vals)
                    rng = np.random.default_rng(42)
                    sel = rng.choice(n, size=min(200, n), replace=False)
                    arr_y = np.asarray(y_vals)
                    trace.y = arr_y[sel].tolist()
                    x_vals = trace.x
                    if x_vals is not None and hasattr(x_vals, "__len__") and len(x_vals) == n:
                        trace.x = np.asarray(x_vals)[sel].tolist()

        chart_json = fig.to_json()
        encoded = base64.b64encode(chart_json.encode()).decode()

        return (
            f'<div class="chart-container">'
            f'<div id="{div_id}" data-b64="{encoded}" style="min-height:{height}px;"></div>'
            "<script>"
            "(function() {"
            f'var el = document.getElementById("{div_id}");'
            "var done = false;"
            "function doRender() {"
            'var b64 = el.getAttribute("data-b64");'
            'el.removeAttribute("data-b64");'
            "var d = JSON.parse(atob(b64));"
            f'return Plotly.newPlot("{div_id}", d.data, d.layout,'
            " {responsive: true, displayModeBar: true});"
            "}"
            'if (typeof IntersectionObserver !== "undefined") {'
            "var obs = new IntersectionObserver(function(entries) {"
            "if (done || !entries[0].isIntersecting) return;"
            "done = true; obs.disconnect();"
            'if (typeof queuePlot === "function") { queuePlot(doRender); }'
            "else { doRender(); }"
            '}, {rootMargin: "200px"});'
            "obs.observe(el);"
            "} else { doRender(); }"
            "})();"
            "</script>"
            "</div>"
        )


def _hex_to_rgb(hex_color: str) -> str:
    """#RRGGBB → 'R,G,B'."""
    h = hex_color.lstrip("#")
    if len(h) == 6:
        return f"{int(h[0:2], 16)},{int(h[2:4], 16)},{int(h[4:6], 16)}"
    return "128,128,128"
