"""Tests for P11 ChoroplethJP — real GeoJSON + bar fallback paths."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.viz.primitives import ChoroplethJPSpec, render_choropleth_jp
from src.viz.primitives import choropleth_jp as _choropleth_module


_GEO_PATH = Path(__file__).resolve().parents[2] / "data" / "geo" / "japan_prefectures.geojson"


@pytest.fixture(autouse=True)
def _clear_geojson_cache():
    """Reset the module-level geojson cache between tests so file-presence
    toggles take effect."""
    _choropleth_module._geojson_cache = None
    yield
    _choropleth_module._geojson_cache = None


def test_geojson_file_exists_and_has_47_prefectures():
    """The bundled GeoJSON file is the v3 minimum; CI relies on it."""
    if not _GEO_PATH.exists():
        pytest.skip("GeoJSON file not present (run scripts/maintenance/fetch_jp_geojson.py)")
    import json
    with _GEO_PATH.open(encoding="utf-8") as f:
        geo = json.load(f)
    assert geo["type"] == "FeatureCollection"
    assert len(geo["features"]) == 47
    # Each feature carries the JA name we key against.
    sample = geo["features"][0]["properties"]
    assert "nam_ja" in sample


def test_choropleth_uses_real_map_when_geojson_present():
    if not _GEO_PATH.exists():
        pytest.skip("GeoJSON file not present")
    spec = ChoroplethJPSpec(
        values={"東京都": 1500, "大阪府": 800, "京都府": 350},
        title="credits by prefecture",
    )
    fig = render_choropleth_jp(spec)
    assert len(fig.data) == 1
    assert fig.data[0].type == "choropleth"


def test_force_bar_fallback_even_when_geojson_present():
    spec = ChoroplethJPSpec(
        values={"東京都": 100, "大阪府": 50},
        fallback_to_bar=True,
    )
    fig = render_choropleth_jp(spec)
    assert fig.data[0].type == "bar"


def test_unknown_prefecture_falls_back_to_bar():
    """Names not present in the GeoJSON should not raise — bar fallback."""
    if not _GEO_PATH.exists():
        pytest.skip("GeoJSON file not present")
    spec = ChoroplethJPSpec(
        values={"unknown_prefecture": 100, "another_unknown": 50},
    )
    fig = render_choropleth_jp(spec)
    assert fig.data[0].type == "bar"


def test_empty_values_renders_placeholder():
    fig = render_choropleth_jp(ChoroplethJPSpec(values={}))
    assert "(no data)" in fig.layout.title.text


def test_missing_geojson_falls_back_to_bar(monkeypatch):
    """Even when the GeoJSON file is absent the chart still renders."""
    monkeypatch.setattr(_choropleth_module, "_GEO_PATH", Path("/non/existent/path.geojson"))
    _choropleth_module._geojson_cache = None
    spec = ChoroplethJPSpec(values={"東京都": 100})
    fig = render_choropleth_jp(spec)
    assert fig.data[0].type == "bar"
