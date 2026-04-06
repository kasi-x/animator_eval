"""Tests for chart_renderers: every chart type produces a valid go.Figure."""

from __future__ import annotations

import plotly.graph_objects as go
import pytest

from src.reporting.renderers.chart_renderers import render_chart
from src.reporting.specs import ExplanationMeta
from src.reporting.specs.chart import (
    BarSpec,
    BoxSpec,
    ForestSpec,
    HeatmapSpec,
    HistogramSpec,
    LineSpec,
    RidgeSpec,
    SankeySpec,
    ScatterSpec,
    ViolinSpec,
)


def _exp() -> ExplanationMeta:
    return ExplanationMeta(question="Q?", reading_guide="G.")


# ---------------------------------------------------------------------------
# Scatter
# ---------------------------------------------------------------------------

def test_scatter_basic() -> None:
    spec = ScatterSpec(slug="s1", title="T", data_key="d", explanation=_exp(), x_field="x", y_field="y")
    data = {"d": [{"x": 1, "y": 2}, {"x": 3, "y": 4}]}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 1


def test_scatter_with_color_and_size() -> None:
    spec = ScatterSpec(
        slug="s2", title="T", data_key="d", explanation=_exp(),
        x_field="x", y_field="y", color_field="c", size_field="s",
    )
    data = {"d": [{"x": 1, "y": 2, "c": 0.5, "s": 10}]}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)


def test_scatter_with_labels() -> None:
    spec = ScatterSpec(
        slug="s3", title="T", data_key="d", explanation=_exp(),
        x_field="x", y_field="y", label_field="name", label_top_n=1,
    )
    data = {"d": [{"x": 1, "y": 10, "name": "A"}, {"x": 2, "y": 5, "name": "B"}]}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)


def test_scatter_empty_data() -> None:
    spec = ScatterSpec(slug="s4", title="T", data_key="d", explanation=_exp(), x_field="x", y_field="y")
    fig = render_chart(spec, {"d": []})
    assert isinstance(fig, go.Figure)


# ---------------------------------------------------------------------------
# Bar
# ---------------------------------------------------------------------------

def test_bar_vertical() -> None:
    spec = BarSpec(slug="b1", title="T", data_key="d", explanation=_exp(), category_field="cat", value_field="v")
    data = {"d": [{"cat": "A", "v": 10}, {"cat": "B", "v": 20}]}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)


def test_bar_horizontal_with_error() -> None:
    spec = BarSpec(
        slug="b2", title="T", data_key="d", explanation=_exp(),
        category_field="cat", value_field="v", orientation="h", error_field="err",
    )
    data = {"d": [{"cat": "A", "v": 10, "err": 1.5}]}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)


# ---------------------------------------------------------------------------
# Forest
# ---------------------------------------------------------------------------

def test_forest_basic() -> None:
    spec = ForestSpec(slug="f1", title="T", data_key="d", explanation=_exp())
    data = {"d": [
        {"name": "A", "estimate": 0.5, "ci_lower": 0.3, "ci_upper": 0.7},
        {"name": "B", "estimate": -0.1, "ci_lower": -0.4, "ci_upper": 0.2},
    ]}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)


def test_forest_auto_height() -> None:
    """height=None should not crash — renderer computes it."""
    spec = ForestSpec(slug="f2", title="T", data_key="d", explanation=_exp(), height=None)
    data = {"d": [{"name": "X", "estimate": 1, "ci_lower": 0, "ci_upper": 2}]}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)
    assert fig.layout.height >= 300


# ---------------------------------------------------------------------------
# Violin
# ---------------------------------------------------------------------------

def test_violin_basic() -> None:
    spec = ViolinSpec(slug="v1", title="T", data_key="d", explanation=_exp(), group_field="g", value_field="v")
    data = {"d": [{"g": "A", "v": i} for i in range(20)]}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)


# ---------------------------------------------------------------------------
# Ridge
# ---------------------------------------------------------------------------

def test_ridge_basic() -> None:
    spec = RidgeSpec(slug="r1", title="T", data_key="d", explanation=_exp(), group_field="g", value_field="v")
    data = {"d": [{"g": "A", "v": float(i)} for i in range(10)] + [{"g": "B", "v": float(i)} for i in range(10)]}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)


def test_ridge_small_group_skipped() -> None:
    """Groups with < 2 values should be silently skipped."""
    spec = RidgeSpec(slug="r2", title="T", data_key="d", explanation=_exp(), group_field="g", value_field="v")
    data = {"d": [{"g": "single", "v": 1.0}]}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 0


# ---------------------------------------------------------------------------
# Heatmap
# ---------------------------------------------------------------------------

def test_heatmap_basic() -> None:
    spec = HeatmapSpec(slug="h1", title="T", data_key="d", explanation=_exp())
    data = {"d": [
        {"x": "A", "y": "1", "z": 10},
        {"x": "B", "y": "1", "z": 20},
        {"x": "A", "y": "2", "z": 30},
        {"x": "B", "y": "2", "z": 40},
    ]}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)


# ---------------------------------------------------------------------------
# Line
# ---------------------------------------------------------------------------

def test_line_single_series() -> None:
    spec = LineSpec(slug="l1", title="T", data_key="d", explanation=_exp(), x_field="t", y_field="v")
    data = {"d": [{"t": 2020, "v": 1}, {"t": 2021, "v": 2}]}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)


def test_line_multi_series() -> None:
    spec = LineSpec(
        slug="l2", title="T", data_key="d", explanation=_exp(),
        x_field="t", y_field="v", series_field="s",
    )
    data = {"d": [
        {"t": 2020, "v": 1, "s": "A"},
        {"t": 2021, "v": 2, "s": "A"},
        {"t": 2020, "v": 3, "s": "B"},
    ]}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 2


# ---------------------------------------------------------------------------
# Sankey
# ---------------------------------------------------------------------------

def test_sankey_basic() -> None:
    spec = SankeySpec(slug="snk", title="T", data_key="d", explanation=_exp())
    data = {"d": {
        "nodes": [{"name": "A"}, {"name": "B"}, {"name": "C"}],
        "links": [{"source": 0, "target": 1, "value": 10}, {"source": 1, "target": 2, "value": 5}],
    }}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)


# ---------------------------------------------------------------------------
# Box
# ---------------------------------------------------------------------------

def test_box_basic() -> None:
    spec = BoxSpec(slug="bx1", title="T", data_key="d", explanation=_exp(), group_field="g", value_field="v")
    data = {"d": [{"g": "A", "v": i} for i in range(10)]}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)


# ---------------------------------------------------------------------------
# Histogram
# ---------------------------------------------------------------------------

def test_histogram_basic() -> None:
    spec = HistogramSpec(slug="hi1", title="T", data_key="d", explanation=_exp(), value_field="v")
    data = {"d": [{"v": i * 0.1} for i in range(50)]}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)


def test_histogram_with_nbins() -> None:
    spec = HistogramSpec(slug="hi2", title="T", data_key="d", explanation=_exp(), value_field="v", nbins=10)
    data = {"d": [{"v": i} for i in range(100)]}
    fig = render_chart(spec, data)
    assert isinstance(fig, go.Figure)


# ---------------------------------------------------------------------------
# Dispatcher errors
# ---------------------------------------------------------------------------

def test_unknown_spec_type_raises() -> None:
    """A non-ChartSpec type should raise TypeError."""
    with pytest.raises(TypeError, match="No renderer"):
        render_chart("not a spec", {})  # type: ignore[arg-type]


def test_missing_data_key_uses_empty() -> None:
    """If data_key is missing from the dict, render with empty data."""
    spec = BarSpec(slug="b3", title="T", data_key="missing", explanation=_exp(), category_field="c", value_field="v")
    fig = render_chart(spec, {})
    assert isinstance(fig, go.Figure)
