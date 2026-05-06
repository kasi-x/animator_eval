"""Smoke tests for primitives P9 (Heatmap) / P10 (ParallelCoords) / P11 (ChoroplethJP).

Export tests → tests/unit/test_viz_export.py
Interactivity tests → tests/unit/test_viz_interactivity.py
"""

from __future__ import annotations

import numpy as np

from src.viz.primitives import (
    ChoroplethJPSpec,
    HeatmapSpec,
    ParallelAxis,
    ParallelCoordsSpec,
    render_choropleth_jp,
    render_heatmap,
    render_parallel_coords,
)


# ---- P9 Heatmap ---------------------------------------------------------


def test_heatmap_basic_render():
    rng = np.random.default_rng(42)
    z = rng.normal(0, 1, (4, 6)).tolist()
    spec = HeatmapSpec(
        z=z,
        x_labels=[f"y{i}" for i in range(6)],
        y_labels=[f"r{i}" for i in range(4)],
        title="demo",
    )
    fig = render_heatmap(spec)
    assert len(fig.data) == 1
    assert fig.data[0].type == "heatmap"


def test_heatmap_diverging_uses_rdbu():
    spec = HeatmapSpec(
        z=[[1, 2], [3, 4]], x_labels=["a", "b"], y_labels=["r1", "r2"],
        diverging=True, z_mid=2.5,
    )
    fig = render_heatmap(spec)
    assert fig.data[0].colorscale[0][1].lower().startswith("rgb") or \
        "RdBu" in str(fig.data[0].colorscale)


def test_heatmap_null_envelope_outlines_outliers():
    # row=col=2, null = [-0.5, 0.5]; values 5.0 and 1.0 are outside.
    z = [[0.0, 5.0], [1.0, 0.0]]
    lo = [[-0.5, -0.5], [-0.5, -0.5]]
    hi = [[0.5, 0.5], [0.5, 0.5]]
    spec = HeatmapSpec(
        z=z, x_labels=["c0", "c1"], y_labels=["r0", "r1"],
        null_envelope=(lo, hi),
    )
    fig = render_heatmap(spec)
    rects = [s for s in fig.layout.shapes if s.type == "rect"]
    assert len(rects) == 2  # both outlier cells outlined


def test_heatmap_empty_data():
    fig = render_heatmap(HeatmapSpec(z=[], x_labels=[], y_labels=[]))
    assert "(no data)" in fig.layout.title.text


# ---- P10 ParallelCoords -------------------------------------------------


def test_parallel_coords_basic():
    spec = ParallelCoordsSpec(
        axes=[
            ParallelAxis("R5", [0.4, 0.6, 0.8]),
            ParallelAxis("VA", [0.2, 0.5, 0.7]),
            ParallelAxis("Tier", [3, 4, 5], range_min=1, range_max=5),
        ],
        color_values=[0.5, 0.7, 0.9],
    )
    fig = render_parallel_coords(spec)
    assert len(fig.data) == 1
    assert fig.data[0].type == "parcoords"
    assert len(fig.data[0].dimensions) == 3


def test_parallel_coords_empty():
    fig = render_parallel_coords(
        ParallelCoordsSpec(axes=[], color_values=[])
    )
    assert "(no data)" in fig.layout.title.text


# ---- P11 ChoroplethJP (bar fallback) ------------------------------------


def test_choropleth_bar_fallback_render():
    spec = ChoroplethJPSpec(
        values={"東京都": 1500, "大阪府": 800, "京都府": 350},
        title="prefecture credits",
    )
    fig = render_choropleth_jp(spec)
    # bar fallback emits 1 trace
    assert len(fig.data) == 1
    assert fig.data[0].type == "bar"


def test_choropleth_empty():
    fig = render_choropleth_jp(ChoroplethJPSpec(values={}))
    assert "(no data)" in fig.layout.title.text


