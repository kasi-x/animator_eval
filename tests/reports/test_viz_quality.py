"""Tests for scripts/report_generators/viz_quality."""

from __future__ import annotations

import pytest

from scripts.report_generators.viz_quality import (
    COLOR_BG_DARK,
    PALETTE_OKABE_ITO,
    PALETTE_VIRIDIS,
    add_ci_band,
    add_point_with_ci,
    apply_quality_layout,
    contrast_ratio,
    forest_plot,
    heatmap_quality,
    palette_color,
    palette_wcag_audit,
    relative_luminance,
    violin_compare,
    _hex_to_rgba,
)


class TestPaletteWCAG:
    def test_relative_luminance_white_is_one(self):
        assert relative_luminance("#ffffff") > 0.99

    def test_relative_luminance_black_is_zero(self):
        assert relative_luminance("#000000") < 0.01

    def test_contrast_ratio_white_black(self):
        cr = contrast_ratio("#ffffff", "#000000")
        # max WCAG contrast = 21:1
        assert 20.5 <= cr <= 21.5

    def test_contrast_ratio_symmetric(self):
        # contrast is symmetric (max/min)
        cr_ab = contrast_ratio("#56B4E9", COLOR_BG_DARK)
        cr_ba = contrast_ratio(COLOR_BG_DARK, "#56B4E9")
        assert abs(cr_ab - cr_ba) < 1e-9

    def test_okabe_ito_palette_meets_aa(self):
        audit = palette_wcag_audit(PALETTE_OKABE_ITO, COLOR_BG_DARK)
        # Most colors should meet AA (≥ 4.5); a couple may be borderline
        passes = sum(1 for _, _, p in audit if p)
        assert passes >= 6, f"Okabe-Ito vs dark bg: {audit}"

    def test_palette_color_modulo(self):
        # Index past palette length wraps around
        c = palette_color(100)
        assert c in PALETTE_OKABE_ITO

    def test_invalid_hex_returns_zero(self):
        assert relative_luminance("not-a-color") == 0.0


class TestHexToRgba:
    def test_valid_hex(self):
        rgba = _hex_to_rgba("#56B4E9", 0.5)
        assert "rgba(86, 180, 233, 0.5)" == rgba

    def test_invalid_hex_fallback(self):
        rgba = _hex_to_rgba("xxx", 0.3)
        assert "rgba(128, 128, 128, 0.3)" == rgba


class TestLayoutApply:
    def test_returns_figure(self):
        import plotly.graph_objects as go
        fig = go.Figure()
        out = apply_quality_layout(fig, title="t", xaxis_title="x", yaxis_title="y")
        assert out is fig
        assert fig.layout.title.text == "t"

    def test_log_y_applied(self):
        import plotly.graph_objects as go
        fig = go.Figure()
        apply_quality_layout(fig, log_y=True)
        assert fig.layout.yaxis.type == "log"


class TestCIBand:
    def test_add_ci_band_adds_trace(self):
        import plotly.graph_objects as go
        fig = go.Figure()
        add_ci_band(
            fig, [1, 2, 3], [0.1, 0.2, 0.3], [0.4, 0.5, 0.6],
            label="test", color="#56B4E9",
        )
        assert len(fig.data) == 1

    def test_add_point_with_ci_adds_two_traces(self):
        import plotly.graph_objects as go
        fig = go.Figure()
        add_point_with_ci(
            fig, [1, 2, 3], [0.25, 0.35, 0.45],
            [0.1, 0.2, 0.3], [0.4, 0.5, 0.6],
            label="trend", color=PALETTE_VIRIDIS[2],
        )
        # CI band + point trace
        assert len(fig.data) == 2


class TestForestPlot:
    def test_basic_forest(self):
        fig = forest_plot(
            labels=["A", "B", "C"],
            estimates=[0.3, -0.1, 0.5],
            ci_lows=[0.1, -0.3, 0.2],
            ci_highs=[0.5, 0.1, 0.8],
        )
        # 3 CI lines + 1 marker scatter + vline = 4 traces (vline is a shape)
        # CI lines are 3, marker is 1
        assert len(fig.data) == 4

    def test_input_length_mismatch_raises(self):
        with pytest.raises(ValueError):
            forest_plot(
                labels=["A", "B"], estimates=[0.3],
                ci_lows=[0.1], ci_highs=[0.5],
            )

    def test_log_x_applied(self):
        fig = forest_plot(
            labels=["A"], estimates=[2.0],
            ci_lows=[1.5], ci_highs=[3.0],
            log_x=True,
        )
        assert fig.layout.xaxis.type == "log"


class TestViolinCompare:
    def test_basic_violin(self):
        groups = {"A": [1.0, 2.0, 3.0], "B": [2.0, 3.0, 4.0]}
        fig = violin_compare(groups, title="t", yaxis_title="y")
        assert len(fig.data) == 2

    def test_empty_groups(self):
        fig = violin_compare({}, title="t", yaxis_title="y")
        assert len(fig.data) == 0


class TestHeatmap:
    def test_basic_heatmap(self):
        fig = heatmap_quality(
            z=[[1, 2], [3, 4]],
            x_labels=["a", "b"],
            y_labels=["c", "d"],
            title="t",
        )
        assert len(fig.data) == 1

    def test_divergent_uses_zmid(self):
        fig = heatmap_quality(
            z=[[-1, 0, 1]],
            x_labels=["a", "b", "c"],
            y_labels=["row"],
            title="t",
            zmid=0,
        )
        # zmid causes RdBu_r colorscale
        assert fig.data[0].zmid == 0
