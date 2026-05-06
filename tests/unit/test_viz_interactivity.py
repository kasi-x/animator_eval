"""Interactivity tests for src.viz.interactivity.

Migrated from test_viz_primitives_p9_p11.py (interactivity section) and
extended with 3-chart relay, default="all" option, and empty-facets guard.
"""

from __future__ import annotations

from src.viz import (
    CrossFilterFacet,
    cross_filter_panel,
    link_brushing,
)


# ---------------------------------------------------------------------------
# Migrated from test_viz_primitives_p9_p11.py
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# New: 3-chart relay
# ---------------------------------------------------------------------------


def test_link_brushing_with_3_charts():
    """All three div IDs appear inside the generated JS block."""
    js = link_brushing(["div_alpha", "div_beta", "div_gamma"], key="anime_id")
    assert "div_alpha" in js
    assert "div_beta" in js
    assert "div_gamma" in js
    # The script must wire up plotly_hover on each div.
    assert js.count("plotly_hover") >= 1


# ---------------------------------------------------------------------------
# New: default="all" produces "すべて" option in the filter UI
# ---------------------------------------------------------------------------


def test_cross_filter_default_value_all():
    """When default='all' (default), the select element has a 'すべて' option."""
    panel = cross_filter_panel(
        [
            CrossFilterFacet(
                field="studio_tier", label="スタジオ規模",
                options=["大手", "中堅", "独立"],
                default="all",
            ),
        ],
        target_div_ids=["chart_x"],
    )
    assert "すべて" in panel
    assert 'value="all"' in panel


# ---------------------------------------------------------------------------
# New: empty facets list renders without exception
# ---------------------------------------------------------------------------


def test_cross_filter_no_facets_renders_empty():
    """cross_filter_panel with facets=[] must not raise and returns a string."""
    panel = cross_filter_panel([], target_div_ids=["chart_z"])
    assert isinstance(panel, str)
    # Even with no facets the panel div wrapper should be present.
    assert "cross-filter-panel" in panel
