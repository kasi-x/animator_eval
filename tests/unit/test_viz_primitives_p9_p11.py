"""Smoke tests for primitives P9 (Heatmap) / P10 (ParallelCoords) / P11 (ChoroplethJP)
plus interactivity + static export.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from src.viz import (
    CrossFilterFacet,
    cross_filter_panel,
    export,
    link_brushing,
)
from src.viz.primitives import (
    ChoroplethJPSpec,
    CIPoint,
    CIScatterSpec,
    HeatmapSpec,
    ParallelAxis,
    ParallelCoordsSpec,
    render_choropleth_jp,
    render_ci_scatter,
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


# ---- interactivity ------------------------------------------------------


def test_link_brushing_emits_attached_handlers():
    js = link_brushing(["chart_a", "chart_b"], key="person_id")
    assert "chart_a" in js
    assert "chart_b" in js
    assert "plotly_hover" in js
    assert "person_id" in js


def test_cross_filter_panel_renders_select_per_facet():
    panel = cross_filter_panel(
        [
            CrossFilterFacet(field="cohort_decade", label="デビュー年代",
                             options=["1990s", "2000s", "2010s"]),
            CrossFilterFacet(field="role_group", label="役職グループ",
                             options=["animator", "director"]),
        ],
        target_div_ids=["chart_a", "chart_b"],
    )
    assert "cohort_decade" in panel
    assert "role_group" in panel
    assert "chart_a" in panel and "chart_b" in panel
    assert "Plotly.restyle" in panel


# ---- static export ------------------------------------------------------


def _sample_fig():
    spec = CIScatterSpec(
        points=[CIPoint("a", 1.2, 1.0, 1.4, p_value=0.01)],
        x_label="HR", reference=1.0,
    )
    return render_ci_scatter(spec, theme="dark")


def test_export_svg(tmp_path: Path):
    out = export.to_svg(_sample_fig(), tmp_path / "x.svg")
    assert out.exists() and out.stat().st_size > 0


def test_export_png(tmp_path: Path):
    out = export.to_png(_sample_fig(), tmp_path / "x.png", scale=1.0)
    assert out.exists() and out.stat().st_size > 0


def test_export_pdf_single(tmp_path: Path):
    out = export.to_pdf(_sample_fig(), tmp_path / "x.pdf")
    assert out.exists() and out.stat().st_size > 0


def test_export_format_routing(tmp_path: Path):
    out = export.export(_sample_fig(), tmp_path / "x.svg", format="svg")
    assert out.exists()


def test_export_unknown_format_raises(tmp_path: Path):
    with pytest.raises(ValueError):
        export.export(_sample_fig(), tmp_path / "x.bmp", format="bmp")  # type: ignore[arg-type]
