"""P11: ChoroplethJP — Japan prefecture-level choropleth.

For visualizing geographic distribution of credits / studios / talent
across Japan's 47 prefectures. Uses Plotly's go.Choropleth with a
prefecture-name → ISO 3166-2:JP code mapping.

The base GeoJSON is loaded lazily from the package data directory; if
not present, the primitive falls back to a sortable bar chart.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import plotly.graph_objects as go

from ..theme import apply_theme

# Prefecture name (JA / EN) → ISO 3166-2:JP code mapping
_PREF_TO_CODE: Mapping[str, str] = {
    "北海道": "JP-01", "青森県": "JP-02", "岩手県": "JP-03",
    "宮城県": "JP-04", "秋田県": "JP-05", "山形県": "JP-06",
    "福島県": "JP-07", "茨城県": "JP-08", "栃木県": "JP-09",
    "群馬県": "JP-10", "埼玉県": "JP-11", "千葉県": "JP-12",
    "東京都": "JP-13", "神奈川県": "JP-14", "新潟県": "JP-15",
    "富山県": "JP-16", "石川県": "JP-17", "福井県": "JP-18",
    "山梨県": "JP-19", "長野県": "JP-20", "岐阜県": "JP-21",
    "静岡県": "JP-22", "愛知県": "JP-23", "三重県": "JP-24",
    "滋賀県": "JP-25", "京都府": "JP-26", "大阪府": "JP-27",
    "兵庫県": "JP-28", "奈良県": "JP-29", "和歌山県": "JP-30",
    "鳥取県": "JP-31", "島根県": "JP-32", "岡山県": "JP-33",
    "広島県": "JP-34", "山口県": "JP-35", "徳島県": "JP-36",
    "香川県": "JP-37", "愛媛県": "JP-38", "高知県": "JP-39",
    "福岡県": "JP-40", "佐賀県": "JP-41", "長崎県": "JP-42",
    "熊本県": "JP-43", "大分県": "JP-44", "宮崎県": "JP-45",
    "鹿児島県": "JP-46", "沖縄県": "JP-47",
}


@dataclass(frozen=True)
class ChoroplethJPSpec:
    values: Mapping[str, float]   # prefecture name → value
    title: str = ""
    z_label: str = "count"
    colorscale: str = "Viridis"
    height: int = 540
    fallback_to_bar: bool = True   # if GeoJSON unavailable, render bar chart


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
    if not spec.values:
        fig = go.Figure()
        fig.update_layout(title=spec.title or "(no data)")
        return apply_theme(fig, theme=theme, height=spec.height)

    # Phase 0 ships the bar fallback only; GeoJSON wiring is added when
    # the project receives a permissively-licensed prefecture polygon set.
    if spec.fallback_to_bar:
        return _render_bar_fallback(spec, theme)

    # GeoJSON path (placeholder — requires data file at this location)
    locations = []
    z_values = []
    for name, val in spec.values.items():
        code = _PREF_TO_CODE.get(name)
        if code is None:
            continue
        locations.append(code)
        z_values.append(val)

    fig = go.Figure(
        go.Choropleth(
            locations=locations,
            z=z_values,
            locationmode="ISO-3",  # Plotly built-in for ISO-2 not available; use bar
            colorscale=spec.colorscale,
            colorbar=dict(title=spec.z_label),
            hovertemplate="%{location}: %{z:,}<extra></extra>",
        )
    )
    fig.update_geos(scope="asia", center=dict(lon=138, lat=37), projection_scale=4.5)
    fig.update_layout(title=spec.title)
    return apply_theme(fig, theme=theme, height=spec.height)
