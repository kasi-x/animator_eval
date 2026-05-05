"""Chart primitives for v3.

11 primitive set (P1-P8 from docs/VIZ_SYSTEM_v3.md §6 + 3 extension):

- P1 CIScatter        — point + CI (forest plot)
- P2 KMCurve          — Kaplan–Meier survival curves with Greenwood CI
- P3 EventStudyPanel  — lead/lag dynamic effect
- P4 SmallMultiples   — facet grid wrapper
- P5 RidgePlot        — joyplot-style overlapping densities
- P6 BoxStripCI       — box + raw strip + 95% CI marker
- P7 SankeyFlow       — staged transitions
- P8 RadialNetwork    — ego-network local view
- P9 Heatmap          — 2D matrix (year × cohort, role × studio, ...)
- P10 ParallelCoords  — multi-axis benchmark
- P11 ChoroplethJP    — Japan prefecture-level choropleth (bar fallback)
"""

from __future__ import annotations

from .box_strip_ci import BoxGroup, BoxStripCISpec, render_box_strip_ci
from .choropleth_jp import ChoroplethJPSpec, render_choropleth_jp
from .ci_scatter import CIPoint, CIScatterSpec, ShrinkageInfo, render_ci_scatter
from .event_study import EventStudySpec, render_event_study
from .heatmap import HeatmapSpec, render_heatmap
from .km_curve import KMCurveSpec, KMStratum, NullSeries, render_km_curve
from .parallel_coords import (
    ParallelAxis, ParallelCoordsSpec, render_parallel_coords,
)
from .radial_network import Neighbor, RadialNetworkSpec, render_radial_network
from .ridge import RidgePlotSpec, RidgeRow, render_ridge_plot
from .sankey import SankeyFlowSpec, SankeyLink, SankeyNode, render_sankey_flow
from .small_multiples import FacetCell, SmallMultiplesSpec, render_small_multiples

__all__ = [
    # P1
    "CIPoint", "CIScatterSpec", "ShrinkageInfo", "render_ci_scatter",
    # P2
    "KMCurveSpec", "KMStratum", "NullSeries", "render_km_curve",
    # P3
    "EventStudySpec", "render_event_study",
    # P4
    "FacetCell", "SmallMultiplesSpec", "render_small_multiples",
    # P5
    "RidgePlotSpec", "RidgeRow", "render_ridge_plot",
    # P6
    "BoxGroup", "BoxStripCISpec", "render_box_strip_ci",
    # P7
    "SankeyFlowSpec", "SankeyLink", "SankeyNode", "render_sankey_flow",
    # P8
    "Neighbor", "RadialNetworkSpec", "render_radial_network",
    # P9
    "HeatmapSpec", "render_heatmap",
    # P10
    "ParallelAxis", "ParallelCoordsSpec", "render_parallel_coords",
    # P11
    "ChoroplethJPSpec", "render_choropleth_jp",
]
