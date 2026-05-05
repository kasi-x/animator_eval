"""Smoke + behavior tests for primitives P2 through P8.

Phase 1 acceptance: each primitive should
- accept its spec dataclass
- emit at least one trace for non-empty data
- gracefully render a "(no data)" placeholder for empty input
- never raise for the documented happy path
"""

from __future__ import annotations

import numpy as np
import pytest

from src.viz.primitives import (
    BoxGroup,
    BoxStripCISpec,
    EventStudySpec,
    FacetCell,
    KMCurveSpec,
    KMStratum,
    Neighbor,
    NullSeries,
    RadialNetworkSpec,
    RidgePlotSpec,
    RidgeRow,
    SankeyFlowSpec,
    SankeyLink,
    SankeyNode,
    SmallMultiplesSpec,
    render_box_strip_ci,
    render_event_study,
    render_km_curve,
    render_radial_network,
    render_ridge_plot,
    render_sankey_flow,
    render_small_multiples,
)


# ---- P2: KMCurve --------------------------------------------------------


def test_km_curve_basic_overlay():
    spec = KMCurveSpec(
        strata=[
            KMStratum(
                "1990s", timeline=[0, 2, 5, 10],
                survival=[1.0, 0.85, 0.62, 0.40],
                ci_lo=[1.0, 0.78, 0.55, 0.32],
                ci_hi=[1.0, 0.91, 0.69, 0.48],
                n_at_risk=[100, 75, 50, 28],
                median_survival=7.2, n=100,
            ),
            KMStratum(
                "2010s", timeline=[0, 2, 5, 10],
                survival=[1.0, 0.92, 0.78, 0.62],
                ci_lo=[1.0, 0.86, 0.71, 0.55],
                ci_hi=[1.0, 0.97, 0.84, 0.69],
                n_at_risk=[200, 180, 150, 120],
                median_survival=12.0, n=200,
            ),
        ],
    )
    fig = render_km_curve(spec, theme="dark")
    # 2 strata × (CI band + step + median marker) + 2 risk-table rows = 8
    assert len(fig.data) >= 6


def test_km_curve_with_null_envelope():
    spec = KMCurveSpec(
        strata=[
            KMStratum("only", timeline=[0, 5], survival=[1.0, 0.5]),
        ],
        null_envelope=NullSeries(timeline=[0, 5], lo=[1.0, 0.4], hi=[1.0, 0.6]),
        risk_table=False,
    )
    fig = render_km_curve(spec)
    names = [t.name for t in fig.data if t.name]
    assert any("null" in (n or "") for n in names)


def test_km_curve_empty_strata_placeholder():
    fig = render_km_curve(KMCurveSpec(strata=[]))
    assert "(no data)" in fig.layout.title.text


# ---- P3: EventStudyPanel ------------------------------------------------


def test_event_study_with_placebo():
    spec = EventStudySpec(
        leads_lags=[-3, -2, -1, 0, 1, 2, 3],
        estimates=[0, 0, 0, 0, 0.05, 0.08, 0.10],
        ci_lo=[-0.02, -0.02, -0.01, 0, 0.01, 0.02, 0.03],
        ci_hi=[0.02, 0.02, 0.01, 0, 0.09, 0.14, 0.17],
        placebo_runs=[[0] * 7, [0.01] * 7],
    )
    fig = render_event_study(spec)
    # CI band + estimate + 2 placebo = 4 traces
    assert len(fig.data) == 4


def test_event_study_length_validation():
    with pytest.raises(ValueError):
        render_event_study(
            EventStudySpec(
                leads_lags=[-1, 0, 1],
                estimates=[0, 0.1],
                ci_lo=[-0.05, 0],
                ci_hi=[0.05, 0.2],
            )
        )


# ---- P4: SmallMultiples -------------------------------------------------


def test_small_multiples_renders_each_facet():
    sub1 = render_event_study(EventStudySpec(
        leads_lags=[-1, 0, 1], estimates=[0, 0.1, 0.2],
        ci_lo=[-0.05, 0, 0.1], ci_hi=[0.05, 0.2, 0.3],
    ))
    sub2 = render_event_study(EventStudySpec(
        leads_lags=[-1, 0, 1], estimates=[0, 0.05, 0.15],
        ci_lo=[-0.05, -0.05, 0.05], ci_hi=[0.05, 0.15, 0.25],
    ))
    fig = render_small_multiples(SmallMultiplesSpec(
        facets=[
            FacetCell("cohort=1990", "role=animator", sub1),
            FacetCell("cohort=2010", "role=animator", sub2),
        ],
        n_cols=2,
    ))
    # each facet contributes 2 traces (CI band + line)
    assert len(fig.data) == 4


def test_small_multiples_empty():
    fig = render_small_multiples(SmallMultiplesSpec(facets=[]))
    assert "no facets" in fig.layout.title.text


# ---- P5: RidgePlot ------------------------------------------------------


def test_ridge_plot_basic():
    rng = np.random.default_rng(42)
    spec = RidgePlotSpec(
        distributions=[
            RidgeRow("1990s", samples=rng.normal(0, 1, 200).tolist(), n=200),
            RidgeRow("2010s", samples=rng.normal(1, 1, 300).tolist(), n=300),
        ],
    )
    fig = render_ridge_plot(spec)
    # each row: IQR shading + curve = 2 traces (no null overlay here)
    assert len(fig.data) >= 4


def test_ridge_plot_with_null():
    rng = np.random.default_rng(42)
    spec = RidgePlotSpec(
        distributions=[RidgeRow("a", samples=rng.normal(0, 1, 100).tolist())],
        null_distribution=rng.normal(0, 2, 200).tolist(),
    )
    fig = render_ridge_plot(spec)
    names = [t.name for t in fig.data if t.name]
    assert any("null" in (n or "") for n in names)


# ---- P6: BoxStripCI -----------------------------------------------------


def test_box_strip_ci_basic():
    rng = np.random.default_rng(42)
    spec = BoxStripCISpec(
        groups=[
            BoxGroup("A", samples=rng.normal(0, 1, 250).tolist(),
                     ci_lo=-0.1, ci_hi=0.1, n=250),
            BoxGroup("B", samples=rng.normal(0.5, 0.9, 180).tolist(),
                     ci_lo=0.3, ci_hi=0.7, n=180),
        ],
        null_median=0.0,
    )
    fig = render_box_strip_ci(spec)
    # box + strip + CI marker per group ≥ 6 traces
    assert len(fig.data) >= 6


def test_box_strip_ci_subsamples_large_groups():
    rng = np.random.default_rng(42)
    spec = BoxStripCISpec(
        groups=[BoxGroup("big", samples=rng.normal(0, 1, 5000).tolist(), n=5000)],
        strip_max_n=100,
    )
    fig = render_box_strip_ci(spec)
    # find the strip trace (mode="markers"); subsample size should be ≤100
    strip_traces = [t for t in fig.data if getattr(t, "mode", "") == "markers"]
    assert any(len(t.y) <= 100 for t in strip_traces)


# ---- P7: SankeyFlow -----------------------------------------------------


def test_sankey_flow_basic():
    spec = SankeyFlowSpec(
        nodes=[
            SankeyNode("a", "初級", 0),
            SankeyNode("b", "中級", 1),
            SankeyNode("c", "上級", 2),
        ],
        links=[
            SankeyLink("a", "b", 100, null_baseline=80),
            SankeyLink("b", "c", 60, null_baseline=40),
        ],
        layer_labels=["初級", "中級", "上級"],
    )
    fig = render_sankey_flow(spec)
    assert len(fig.data) == 1
    assert fig.data[0].type == "sankey"


def test_sankey_aggregates_small_links():
    """Links below ``min_link_value`` should be merged into an Other node."""
    spec = SankeyFlowSpec(
        nodes=[
            SankeyNode("a", "src", 0),
            SankeyNode("b", "tgt1", 1),
            SankeyNode("c", "tgt2", 1),
        ],
        links=[
            SankeyLink("a", "b", 50),  # kept
            SankeyLink("a", "c", 2),   # aggregated
        ],
        layer_labels=["L0", "L1"],
        min_link_value=5,
    )
    fig = render_sankey_flow(spec)
    labels = list(fig.data[0].node.label)
    assert any("Other" in label_ for label_ in labels)


# ---- P8: RadialNetwork --------------------------------------------------


def test_radial_network_basic():
    spec = RadialNetworkSpec(
        ego_label="ego",
        neighbors=[
            Neighbor("n1", edge_weight=0.9, ci_lo=0.85, ci_hi=0.95),
            Neighbor("n2", edge_weight=0.7, ci_lo=0.60, ci_hi=0.80),
            Neighbor("n3", edge_weight=0.5, ci_lo=0.30, ci_hi=0.70),
        ],
    )
    fig = render_radial_network(spec)
    # 3 neighbors × (edge + node) + ego = 7 traces
    assert len(fig.data) >= 7


def test_radial_network_empty():
    fig = render_radial_network(RadialNetworkSpec(ego_label="x", neighbors=[]))
    assert "(no neighbors)" in fig.layout.title.text


def test_radial_network_max_neighbors_caps_display():
    spec = RadialNetworkSpec(
        ego_label="ego",
        neighbors=[Neighbor(f"n{i}", edge_weight=float(50 - i)) for i in range(50)],
        max_neighbors=10,
    )
    fig = render_radial_network(spec)
    # 10 edges + 10 nodes + 1 ego = 21 traces
    assert len(fig.data) == 21
