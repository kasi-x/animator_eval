"""Graceful fallback tests for all 11 viz primitives (P1–P11).

Each primitive must:
- Accept empty / minimal spec without raising.
- Produce a "(no data)" (or equivalent) placeholder title / annotation.
- Handle optional fields (ci_lo/ci_hi, null_overlay) being None / absent.

Existing smoke/behavior tests remain in:
  tests/unit/test_viz_primitives.py        (P1 CIScatter)
  tests/unit/test_viz_primitives_p2_p8.py  (P2–P8)
  tests/unit/test_viz_primitives_p9_p11.py (P9–P11)
"""

from __future__ import annotations

from src.viz.primitives import (
    BoxGroup,
    BoxStripCISpec,
    CIPoint,
    CIScatterSpec,
    ChoroplethJPSpec,
    EventStudySpec,
    HeatmapSpec,
    KMCurveSpec,
    KMStratum,
    Neighbor,
    ParallelCoordsSpec,
    RadialNetworkSpec,
    RidgePlotSpec,
    RidgeRow,
    SankeyFlowSpec,
    SankeyNode,
    SmallMultiplesSpec,
    render_box_strip_ci,
    render_choropleth_jp,
    render_ci_scatter,
    render_event_study,
    render_heatmap,
    render_km_curve,
    render_parallel_coords,
    render_radial_network,
    render_ridge_plot,
    render_sankey_flow,
    render_small_multiples,
)


# ── P1: CIScatter ─────────────────────────────────────────────────────────


def test_p1_ci_scatter_empty_no_raise():
    """Empty points list must not raise; placeholder annotation shown."""
    fig = render_ci_scatter(CIScatterSpec(points=[], x_label="HR"))
    annotations = fig.layout.annotations or []
    assert any("データなし" in (a.text or "") for a in annotations)


def test_p1_ci_scatter_ci_none_fields():
    """CIPoint with ci_lo/ci_hi present but p_value=None must render."""
    pts = [CIPoint(label="x", x=1.0, ci_lo=0.8, ci_hi=1.2)]
    fig = render_ci_scatter(CIScatterSpec(points=pts, x_label="HR"))
    assert len(fig.data) >= 1


def test_p1_ci_scatter_no_null_band():
    """Spec with null_band=None (default) must work without error."""
    pts = [CIPoint(label="a", x=1.1, ci_lo=0.9, ci_hi=1.3, p_value=0.01)]
    fig = render_ci_scatter(CIScatterSpec(points=pts, x_label="HR", null_band=None))
    assert len(fig.data) >= 1


# ── P2: KMCurve ───────────────────────────────────────────────────────────


def test_p2_km_curve_empty_strata_no_raise():
    """Empty strata list must produce (no data) placeholder."""
    fig = render_km_curve(KMCurveSpec(strata=[]))
    assert "(no data)" in (fig.layout.title.text or "")


def test_p2_km_curve_ci_none():
    """Stratum with ci_lo=None / ci_hi=None must render without raise."""
    stratum = KMStratum(
        label="group",
        timeline=[0, 5, 10],
        survival=[1.0, 0.7, 0.4],
        ci_lo=None,
        ci_hi=None,
    )
    fig = render_km_curve(KMCurveSpec(strata=[stratum], risk_table=False))
    assert len(fig.data) >= 1


def test_p2_km_curve_no_null_envelope():
    """Spec without null_envelope must render cleanly."""
    stratum = KMStratum("g", timeline=[0, 5], survival=[1.0, 0.6])
    fig = render_km_curve(KMCurveSpec(strata=[stratum], null_envelope=None, risk_table=False))
    assert len(fig.data) >= 1


# ── P3: EventStudyPanel ───────────────────────────────────────────────────


def test_p3_event_study_empty_no_raise():
    """Empty leads_lags must produce a placeholder without raising."""
    fig = render_event_study(
        EventStudySpec(leads_lags=[], estimates=[], ci_lo=[], ci_hi=[])
    )
    assert fig.layout.title.text == "(no data)"


def test_p3_event_study_no_placebo():
    """Spec with placebo_runs=None must render the main estimate."""
    fig = render_event_study(
        EventStudySpec(
            leads_lags=[-1, 0, 1],
            estimates=[0.0, 0.1, 0.2],
            ci_lo=[-0.05, 0.0, 0.1],
            ci_hi=[0.05, 0.2, 0.3],
            placebo_runs=None,
        )
    )
    # CI band + estimate line = 2 traces
    assert len(fig.data) == 2


# ── P4: SmallMultiples ────────────────────────────────────────────────────


def test_p4_small_multiples_empty_no_raise():
    """Empty facets list must produce (no facets) placeholder."""
    fig = render_small_multiples(SmallMultiplesSpec(facets=[]))
    assert "no facets" in (fig.layout.title.text or "")


# ── P5: RidgePlot ─────────────────────────────────────────────────────────


def test_p5_ridge_plot_empty_no_raise():
    """Empty distributions list must produce (no data) placeholder."""
    fig = render_ridge_plot(RidgePlotSpec(distributions=[]))
    assert "(no data)" in (fig.layout.title.text or "")


def test_p5_ridge_plot_no_null_distribution():
    """Spec with null_distribution=None (default) must render without error."""
    fig = render_ridge_plot(
        RidgePlotSpec(
            distributions=[RidgeRow("r1", samples=[0.1, 0.5, 0.9])],
            null_distribution=None,
        )
    )
    assert len(fig.data) >= 1


# ── P6: BoxStripCI ────────────────────────────────────────────────────────


def test_p6_box_strip_ci_empty_no_raise():
    """Empty groups list must produce (no data) placeholder."""
    fig = render_box_strip_ci(BoxStripCISpec(groups=[]))
    assert "(no data)" in (fig.layout.title.text or "")


def test_p6_box_strip_ci_ci_none():
    """Group with ci_lo=None / ci_hi=None must render without raise."""
    grp = BoxGroup(label="A", samples=[0.1, 0.5, 0.9], ci_lo=None, ci_hi=None)
    fig = render_box_strip_ci(BoxStripCISpec(groups=[grp]))
    assert len(fig.data) >= 1


# ── P7: SankeyFlow ────────────────────────────────────────────────────────


def test_p7_sankey_empty_nodes_no_raise():
    """Empty nodes and links must produce (no data) placeholder."""
    fig = render_sankey_flow(
        SankeyFlowSpec(nodes=[], links=[], layer_labels=[])
    )
    assert "(no data)" in (fig.layout.title.text or "")


def test_p7_sankey_empty_links_no_raise():
    """Nodes with no links must produce (no data) placeholder."""
    nodes = [SankeyNode("a", "src", 0), SankeyNode("b", "tgt", 1)]
    fig = render_sankey_flow(SankeyFlowSpec(nodes=nodes, links=[], layer_labels=["L0", "L1"]))
    assert "(no data)" in (fig.layout.title.text or "")


# ── P8: RadialNetwork ─────────────────────────────────────────────────────


def test_p8_radial_network_empty_neighbors_no_raise():
    """Empty neighbors list must produce (no neighbors) placeholder."""
    fig = render_radial_network(RadialNetworkSpec(ego_label="ego", neighbors=[]))
    assert "(no neighbors)" in (fig.layout.title.text or "")


def test_p8_radial_network_neighbor_ci_none():
    """Neighbor with ci_lo=None / ci_hi=None must render without raise."""
    neighbor = Neighbor(label="n1", edge_weight=0.8, ci_lo=None, ci_hi=None)
    fig = render_radial_network(
        RadialNetworkSpec(ego_label="ego", neighbors=[neighbor])
    )
    assert len(fig.data) >= 1


# ── P9: Heatmap ───────────────────────────────────────────────────────────


def test_p9_heatmap_empty_no_raise():
    """Empty z matrix must produce (no data) placeholder."""
    fig = render_heatmap(HeatmapSpec(z=[], x_labels=[], y_labels=[]))
    assert "(no data)" in (fig.layout.title.text or "")


def test_p9_heatmap_no_null_envelope():
    """Spec with null_envelope=None (default) must render cleanly."""
    fig = render_heatmap(
        HeatmapSpec(z=[[1.0, 2.0], [3.0, 4.0]], x_labels=["c0", "c1"], y_labels=["r0", "r1"])
    )
    assert len(fig.data) == 1


# ── P10: ParallelCoords ───────────────────────────────────────────────────


def test_p10_parallel_coords_empty_no_raise():
    """Empty axes list must produce (no data) placeholder."""
    fig = render_parallel_coords(ParallelCoordsSpec(axes=[], color_values=[]))
    assert "(no data)" in (fig.layout.title.text or "")


# ── P11: ChoroplethJP ─────────────────────────────────────────────────────


def test_p11_choropleth_empty_no_raise():
    """Empty values dict must produce (no data) placeholder."""
    fig = render_choropleth_jp(ChoroplethJPSpec(values={}))
    assert "(no data)" in (fig.layout.title.text or "")
