"""Unit tests for src.viz primitives (Phase 0).

See docs/VIZ_SYSTEM_v3.md §10 for the Phase 0 acceptance criteria.
"""

from __future__ import annotations

import plotly.graph_objects as go
import pytest

from src.viz import apply_theme, embed, palettes
from src.viz.primitives import CIPoint, CIScatterSpec, render_ci_scatter


# --- palette --------------------------------------------------------------


def test_okabe_ito_has_8_colors():
    assert len(palettes.OKABE_ITO) == 8
    for c in palettes.OKABE_ITO:
        assert c.startswith("#") and len(c) == 7


def test_okabe_ito_dark_swaps_black():
    assert palettes.OKABE_ITO[0] == "#000000"
    assert palettes.OKABE_ITO_DARK[0] != "#000000"
    assert len(palettes.OKABE_ITO_DARK) == 8


def test_career_stage_mapping_complete():
    for stage in ("初級ランク", "中級ランク", "上級ランク"):
        assert stage in palettes.CAREER_STAGE


def test_role_group_covers_seven_buckets():
    for role in ("animator", "director", "designer", "production",
                 "writing", "technical", "other"):
        assert role in palettes.ROLE_GROUP


def test_hex_to_rgba():
    assert palettes.hex_to_rgba("#000000", 0.5) == "rgba(0,0,0,0.500)"
    assert palettes.hex_to_rgba("#FFFFFF", 1.0) == "rgba(255,255,255,1.000)"


def test_hex_to_rgba_rejects_bad_input():
    with pytest.raises(ValueError):
        palettes.hex_to_rgba("not-hex")


# --- theme ----------------------------------------------------------------


def test_apply_theme_idempotent():
    fig = go.Figure()
    apply_theme(fig, theme="dark")
    apply_theme(fig, theme="dark")  # 2 回呼んでも壊れない
    assert fig.layout.paper_bgcolor == "rgba(0,0,0,0)"


def test_apply_theme_light():
    fig = go.Figure()
    apply_theme(fig, theme="light")
    assert fig.layout.paper_bgcolor == "#ffffff"


def test_apply_theme_print():
    fig = go.Figure()
    apply_theme(fig, theme="print")
    assert fig.layout.paper_bgcolor == "#ffffff"


# --- embed ----------------------------------------------------------------


def test_embed_produces_div_with_data():
    fig = go.Figure(data=[go.Scatter(x=[1, 2], y=[3, 4])])
    html = embed(fig, "test_div", height=300)
    assert '<div id="test_div"' in html
    assert "data-b64=" in html
    assert "Plotly.newPlot" in html


# --- CIScatter ------------------------------------------------------------


def _sample_points() -> list[CIPoint]:
    return [
        CIPoint(label="cov_a", x=1.18, ci_lo=1.05, ci_hi=1.32, p_value=0.005),
        CIPoint(label="cov_b", x=0.92, ci_lo=0.81, ci_hi=1.04, p_value=0.18),
        CIPoint(label="cov_c", x=0.74, ci_lo=0.62, ci_hi=0.88, p_value=0.001),
    ]


def test_ci_scatter_basic_render():
    spec = CIScatterSpec(
        points=_sample_points(),
        x_label="HR",
        log_x=True,
        reference=1.0,
    )
    fig = render_ci_scatter(spec, theme="dark")
    # nonsig + sig groups → 2 traces (no null band)
    assert len(fig.data) == 2
    # log scale applied
    assert fig.layout.xaxis.type == "log"


def test_ci_scatter_with_null_band_adds_trace():
    spec = CIScatterSpec(
        points=_sample_points(),
        x_label="HR",
        reference=1.0,
        null_band=(0.95, 1.05),
    )
    fig = render_ci_scatter(spec, theme="dark")
    # null band + nonsig + sig → 3 traces
    assert len(fig.data) == 3


def test_ci_scatter_empty_points_yields_placeholder():
    spec = CIScatterSpec(points=[], x_label="HR")
    fig = render_ci_scatter(spec)
    # placeholder annotation
    assert fig.layout.annotations
    assert "データなし" in fig.layout.annotations[0].text


def test_ci_scatter_significance_split():
    """Filled (significant) and hollow (non-significant) markers go in
    separate traces so the legend can distinguish them."""
    pts = [
        CIPoint(label="sig", x=1.5, ci_lo=1.2, ci_hi=1.8, p_value=0.001),
        CIPoint(label="nonsig", x=1.0, ci_lo=0.8, ci_hi=1.2, p_value=0.5),
    ]
    spec = CIScatterSpec(points=pts, x_label="HR", reference=1.0)
    fig = render_ci_scatter(spec)
    names = [t.name for t in fig.data]
    assert any("有意" in n for n in names)
    assert any("非有意" in n for n in names)


def test_ci_scatter_sort_by_x():
    pts = [
        CIPoint(label="hi", x=2.0, ci_lo=1.5, ci_hi=2.5, p_value=0.01),
        CIPoint(label="lo", x=0.5, ci_lo=0.3, ci_hi=0.7, p_value=0.01),
    ]
    spec = CIScatterSpec(points=pts, x_label="HR", sort_by="x")
    fig = render_ci_scatter(spec)
    # categoryarray reflects sort order: lo (0.5) first, hi (2.0) second
    arr = fig.layout.yaxis.categoryarray
    assert list(arr) == ["lo", "hi"]


def test_ci_scatter_shrinkage_badge():
    from src.viz.primitives.ci_scatter import ShrinkageInfo

    spec = CIScatterSpec(
        points=_sample_points(),
        x_label="HR",
        shrinkage=ShrinkageInfo(method="James-Stein", n_threshold=30),
    )
    fig = render_ci_scatter(spec)
    texts = [a.text for a in fig.layout.annotations]
    assert any("James-Stein" in t for t in texts)
    assert any("n<30" in t for t in texts)
