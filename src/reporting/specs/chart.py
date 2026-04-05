"""ChartSpec hierarchy.

Ten concrete chart specifications cover the common patterns in existing
reports. New chart types are added here on demand — do not speculate.

Every ChartSpec is a pure description of **how to render** a chart. Data is
supplied separately by a ``provider`` and looked up by ``data_key``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Union

from src.reporting.specs.explanation import ExplanationMeta


@dataclass(frozen=True)
class ScatterSpec:
    """2D scatter plot. Optionally colour- and size-encoded."""

    slug: str
    title: str
    data_key: str
    explanation: ExplanationMeta
    x_field: str
    y_field: str
    xlabel: str = ""
    ylabel: str = ""
    height: int = 500
    color_field: str | None = None
    size_field: str | None = None
    label_field: str | None = None
    label_top_n: int = 0
    use_density: bool = False


@dataclass(frozen=True)
class BarSpec:
    """Bar chart, vertical or horizontal, optional error bars."""

    slug: str
    title: str
    data_key: str
    explanation: ExplanationMeta
    category_field: str
    value_field: str
    orientation: str = "v"              # "v" or "h"
    color_field: str | None = None
    error_field: str | None = None      # CI / SE half-width
    xlabel: str = ""
    ylabel: str = ""
    height: int = 450


@dataclass(frozen=True)
class ForestSpec:
    """Forest plot (dot + CI).

    Used wherever a Finding's interval must be displayed side-by-side with
    other estimates — e.g. subsample grids for compensation fairness.
    ``data_key`` resolves to a list of dicts with keys ``name``, ``estimate``,
    ``ci_lower``, ``ci_upper``.
    """

    slug: str
    title: str
    data_key: str
    explanation: ExplanationMeta
    xlabel: str = "effect"
    height: int | None = None


@dataclass(frozen=True)
class ViolinSpec:
    """Violin plot; optionally split by a secondary category."""

    slug: str
    title: str
    data_key: str
    explanation: ExplanationMeta
    group_field: str
    value_field: str
    split_field: str | None = None
    xlabel: str = ""
    ylabel: str = ""
    height: int = 500


@dataclass(frozen=True)
class RidgeSpec:
    """Ridgeline / joyplot — stacked density curves."""

    slug: str
    title: str
    data_key: str
    explanation: ExplanationMeta
    group_field: str
    value_field: str
    xlabel: str = ""
    height: int = 600


@dataclass(frozen=True)
class HeatmapSpec:
    """2D heatmap. ``data_key`` resolves to a list of ``(x, y, z)`` tuples
    or to a nested list with accompanying axis labels."""

    slug: str
    title: str
    data_key: str
    explanation: ExplanationMeta
    x_field: str = "x"
    y_field: str = "y"
    z_field: str = "z"
    colorscale: str = "Viridis"
    xlabel: str = ""
    ylabel: str = ""
    height: int = 500


@dataclass(frozen=True)
class LineSpec:
    """Line chart, single- or multi-series."""

    slug: str
    title: str
    data_key: str
    explanation: ExplanationMeta
    x_field: str
    y_field: str
    series_field: str | None = None
    xlabel: str = ""
    ylabel: str = ""
    height: int = 450


@dataclass(frozen=True)
class SankeySpec:
    """Sankey diagram. Expects ``data_key`` to resolve to a dict with
    ``nodes`` and ``links`` arrays (Plotly Sankey format)."""

    slug: str
    title: str
    data_key: str
    explanation: ExplanationMeta
    height: int = 600


@dataclass(frozen=True)
class BoxSpec:
    """Box plot over categorical groups."""

    slug: str
    title: str
    data_key: str
    explanation: ExplanationMeta
    group_field: str
    value_field: str
    xlabel: str = ""
    ylabel: str = ""
    height: int = 450


@dataclass(frozen=True)
class HistogramSpec:
    """Single-field histogram with fixed or auto bins."""

    slug: str
    title: str
    data_key: str
    explanation: ExplanationMeta
    value_field: str
    nbins: int | None = None
    xlabel: str = ""
    ylabel: str = ""
    height: int = 400


#: Union of all known concrete chart specs. Renderers dispatch on
#: ``isinstance`` — new chart types must be added both here and in
#: ``src/reporting/renderers/chart_renderers.py``.
ChartSpec = Union[
    ScatterSpec,
    BarSpec,
    ForestSpec,
    ViolinSpec,
    RidgeSpec,
    HeatmapSpec,
    LineSpec,
    SankeySpec,
    BoxSpec,
    HistogramSpec,
]
