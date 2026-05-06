"""Static export tests for src.viz.export.

Migrated from test_viz_primitives_p9_p11.py (export section) and extended
with multi-page PDF, print-theme, and height-inference tests.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.viz import export
from src.viz.primitives import (
    CIPoint,
    CIScatterSpec,
    render_ci_scatter,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _sample_fig():
    spec = CIScatterSpec(
        points=[CIPoint("a", 1.2, 1.0, 1.4, p_value=0.01)],
        x_label="HR", reference=1.0,
    )
    return render_ci_scatter(spec, theme="dark")


# ---------------------------------------------------------------------------
# Migrated from test_viz_primitives_p9_p11.py
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# New: multi-page PDF
# ---------------------------------------------------------------------------


def test_export_pdf_multi_page(tmp_path: Path):
    """to_pdf([fig1, fig2, fig3], path) merges pages into one PDF via pypdf."""
    figs = [_sample_fig(), _sample_fig(), _sample_fig()]
    out = export.to_pdf(figs, tmp_path / "multi.pdf")
    assert out.exists() and out.stat().st_size > 0

    from pypdf import PdfReader
    reader = PdfReader(str(out))
    assert len(reader.pages) == 3


# ---------------------------------------------------------------------------
# New: print theme applied when rethemed=True
# ---------------------------------------------------------------------------


def test_export_print_theme_applied(tmp_path: Path):
    """to_svg(fig, path, rethemed=True) applies the print theme (white bg)."""
    fig = _sample_fig()
    # Verify the source figure is NOT using a white paper_bgcolor.
    # Dark theme uses transparent bg, so it should not equal #ffffff.
    assert fig.layout.paper_bgcolor != "#ffffff"

    out = export.to_svg(fig, tmp_path / "themed.svg", rethemed=True)
    assert out.exists() and out.stat().st_size > 0

    # The original figure must remain untouched.
    assert fig.layout.paper_bgcolor != "#ffffff"

    # The SVG content should contain white from the print theme.
    # Plotly kaleido renders #ffffff as "rgb(255, 255, 255)" in SVG output.
    svg_text = out.read_text(encoding="utf-8", errors="ignore")
    assert "rgb(255, 255, 255)" in svg_text or "ffffff" in svg_text.lower()


# ---------------------------------------------------------------------------
# New: fig.layout.height takes priority over default height argument
# ---------------------------------------------------------------------------


def test_export_height_inferred_from_layout(tmp_path: Path):
    """If fig.layout.height is set, it is used even when height kwarg is None."""
    import plotly.graph_objects as go
    from src.viz import export as viz_export

    fig = _sample_fig()
    fig.update_layout(height=320)

    # to_svg reads out.layout.height (320) instead of the default 480.
    # We verify by inspecting the SVG viewport attribute.
    out = viz_export.to_svg(fig, tmp_path / "sized.svg", rethemed=False)
    assert out.exists()

    svg_text = out.read_text(encoding="utf-8", errors="ignore")
    # Plotly writes height as an attribute on the outer <svg> element.
    assert "320" in svg_text
