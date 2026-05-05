"""Static export: SVG / PDF / PNG via plotly + kaleido.

The export switches the figure to the ``print`` theme (monochrome +
pattern fill, no glass-morphism) before writing the file. Original
``fig`` is untouched.

Usage:
    from src.viz import export
    export.to_svg(fig, "out/chart.svg")
    export.to_pdf(fig, "out/brief.pdf")  # multi-figure PDF supported
    export.to_png(fig, "out/thumb.png", scale=2)
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Literal

import plotly.graph_objects as go

from .theme import apply_theme

ExportFormat = Literal["svg", "pdf", "png"]


def _prepare_for_print(fig: go.Figure) -> go.Figure:
    """Return a clone of ``fig`` re-themed for static export."""
    new = go.Figure(fig)
    return apply_theme(new, theme="print")


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def to_svg(
    fig: go.Figure,
    path: str | Path,
    *,
    width: int = 900,
    height: int | None = None,
    rethemed: bool = True,
) -> Path:
    """Write ``fig`` as SVG. Returns the written path."""
    p = Path(path)
    _ensure_parent(p)
    out = _prepare_for_print(fig) if rethemed else fig
    h = height if height is not None else (out.layout.height or 480)
    out.write_image(str(p), format="svg", width=width, height=h)
    return p


def to_png(
    fig: go.Figure,
    path: str | Path,
    *,
    width: int = 900,
    height: int | None = None,
    scale: float = 2.0,
    rethemed: bool = False,
) -> Path:
    """Write ``fig`` as PNG (raster, retina by default)."""
    p = Path(path)
    _ensure_parent(p)
    out = _prepare_for_print(fig) if rethemed else fig
    h = height if height is not None else (out.layout.height or 480)
    out.write_image(str(p), format="png", width=width, height=h, scale=scale)
    return p


def to_pdf(
    figs: go.Figure | Iterable[go.Figure],
    path: str | Path,
    *,
    width: int = 900,
    height: int | None = None,
    rethemed: bool = True,
) -> Path:
    """Write one or more ``fig`` objects as a single PDF.

    For a single ``fig``: writes a one-page PDF.
    For an iterable: concatenates pages in order using PyPDF2 if
    available, otherwise raises ``RuntimeError``.
    """
    p = Path(path)
    _ensure_parent(p)
    if isinstance(figs, go.Figure):
        out = _prepare_for_print(figs) if rethemed else figs
        h = height if height is not None else (out.layout.height or 480)
        out.write_image(str(p), format="pdf", width=width, height=h)
        return p

    fig_list = list(figs)
    if not fig_list:
        raise ValueError("to_pdf: empty figure iterable")

    try:
        from pypdf import PdfWriter
    except ImportError as e:
        raise RuntimeError(
            "Multi-figure PDF requires the `pypdf` package. "
            "Add it to pixi.toml [feature.dev] dependencies."
        ) from e

    writer = PdfWriter()
    for i, fig in enumerate(fig_list):
        out = _prepare_for_print(fig) if rethemed else fig
        h = height if height is not None else (out.layout.height or 480)
        tmp = p.parent / f".{p.stem}__page{i}.pdf"
        out.write_image(str(tmp), format="pdf", width=width, height=h)
        writer.append(str(tmp))
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
    with open(p, "wb") as f:
        writer.write(f)
    return p


def export(
    fig: go.Figure,
    path: str | Path,
    *,
    format: ExportFormat = "svg",
    **kwargs,
) -> Path:
    """Format-routing convenience wrapper."""
    if format == "svg":
        return to_svg(fig, path, **kwargs)
    if format == "png":
        return to_png(fig, path, **kwargs)
    if format == "pdf":
        return to_pdf(fig, path, **kwargs)
    raise ValueError(f"unknown format: {format}")
