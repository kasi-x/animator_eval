"""P11: ChoroplethJP — Japan prefecture-level choropleth.

For visualizing geographic distribution of credits / studios / talent
across Japan's 47 prefectures. Uses Plotly's go.Choropleth with the
``data/geo/japan_prefectures.geojson`` (dataofjapan/land, MIT) base map.

The base GeoJSON is loaded lazily; if not present (the file is fetched
via ``scripts/maintenance/fetch_jp_geojson.py``), the primitive falls
back to a sortable bar chart.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import plotly.graph_objects as go

from ..theme import apply_theme

# Geo data path — populated by scripts/maintenance/fetch_jp_geojson.py.
_GEO_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "geo" / "japan_prefectures.geojson"
)


# Cached GeoJSON; loaded once per process when first needed.
_geojson_cache: dict | None = None


def _load_geojson() -> dict | None:
    """Return parsed GeoJSON dict, or None when the file is unavailable."""
    global _geojson_cache
    if _geojson_cache is not None:
        return _geojson_cache
    if not _GEO_PATH.exists():
        return None
    try:
        with _GEO_PATH.open(encoding="utf-8") as f:
            _geojson_cache = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    return _geojson_cache


@dataclass(frozen=True)
class ChoroplethJPSpec:
    values: Mapping[str, float]   # prefecture name (JA, e.g. "東京都") → value
    title: str = ""
    z_label: str = "count"
    colorscale: str = "Viridis"
    height: int = 540
    fallback_to_bar: bool = False  # if True, force the bar fallback even when
                                   # GeoJSON is available (used for printing)


def _render_bar_fallback(
    spec: ChoroplethJPSpec, theme: str
) -> go.Figure:
    """Sortable bar chart when prefecture GeoJSON is unavailable."""
    items = sorted(spec.values.items(), key=lambda kv: kv[1], reverse=True)
    labels = [k for k, _ in items]
    vals = [v for _, v in items]
    fig = go.Figure(
        go.Bar(
            x=vals,
            y=labels,
            orientation="h",
            marker=dict(color=vals, colorscale=spec.colorscale, showscale=True,
                        colorbar=dict(title=spec.z_label)),
            hovertemplate="%{y}: %{x:,}<extra></extra>",
        )
    )
    fig.update_layout(
        title=f"{spec.title} (bar fallback)",
        xaxis_title=spec.z_label,
        margin=dict(l=120),
    )
    fig.update_yaxes(autorange="reversed")
    return apply_theme(fig, theme=theme, height=spec.height)


def render_choropleth_jp(
    spec: ChoroplethJPSpec, *, theme: str = "dark"
) -> go.Figure:
    """Render an interactive Japan-prefecture choropleth.

    When ``data/geo/japan_prefectures.geojson`` is present and
    ``spec.fallback_to_bar`` is False, the function emits a real
    ``go.Choropleth`` map keyed by ``properties.nam_ja``. Otherwise it
    falls back to a sortable horizontal bar chart so the report still
    renders something useful.
    """
    if not spec.values:
        fig = go.Figure()
        fig.update_layout(title=spec.title or "(no data)")
        return apply_theme(fig, theme=theme, height=spec.height)

    geo = None if spec.fallback_to_bar else _load_geojson()
    if geo is None:
        return _render_bar_fallback(spec, theme)

    # Filter values to those that match a known prefecture name in the
    # GeoJSON (defensive against typos / partial keys).
    known = {f["properties"]["nam_ja"] for f in geo["features"]}
    locations = [k for k in spec.values if k in known]
    z_values = [spec.values[k] for k in locations]
    if not locations:
        return _render_bar_fallback(spec, theme)

    fig = go.Figure(
        go.Choropleth(
            geojson=geo,
            featureidkey="properties.nam_ja",
            locations=locations,
            z=z_values,
            colorscale=spec.colorscale,
            colorbar=dict(title=spec.z_label),
            hovertemplate="%{location}: %{z:,}<extra></extra>",
            marker_line_color="rgba(255,255,255,0.4)",
            marker_line_width=0.5,
        )
    )
    fig.update_geos(
        fitbounds="locations",
        visible=False,
        bgcolor="rgba(0,0,0,0)",
    )
    fig.update_layout(
        title=spec.title,
        margin=dict(l=10, r=10, t=60, b=10),
    )
    return apply_theme(fig, theme=theme, height=spec.height)
