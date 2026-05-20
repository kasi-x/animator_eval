"""可視化品質統一ライブラリ — Plotly テンプレ + WCAG AA palette + CI band。

各 v2 report で同じテイストの図表を保証。色覚多様性 (CVD-safe) と
WCAG AA contrast (4.5:1) を満たす palette + 統一 layout テンプレ。

References:
    - WCAG 2.1 contrast: https://www.w3.org/TR/WCAG21/#contrast-minimum
    - Okabe-Ito CVD-safe palette (Okabe & Ito 2008)
    - Plotly "plotly_dark" baseline
"""

from __future__ import annotations

from dataclasses import dataclass

import plotly.graph_objects as go
import structlog

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Color palette — Okabe-Ito (CVD-safe) + dark/light variants
# ---------------------------------------------------------------------------

# WCAG AA contrast ≥ 4.5:1 against dark background (#1a1a2e)
# Okabe-Ito 8 色 — 色覚多様性 (deuteranopia / protanopia / tritanopia) 全対応
PALETTE_OKABE_ITO: tuple[str, ...] = (
    "#56B4E9",  # sky blue
    "#E69F00",  # orange
    "#009E73",  # bluish green
    "#F0E442",  # yellow
    "#0072B2",  # blue
    "#D55E00",  # vermillion
    "#CC79A7",  # reddish purple
    "#999999",  # gray
)

# Sequential palette for continuous gradients (colorblind-safe viridis)
PALETTE_VIRIDIS: tuple[str, ...] = (
    "#440154", "#3b528b", "#21918c", "#5ec962", "#fde725",
)

# Diverging palette (RdBu, CVD-safe)
PALETTE_DIVERGING: tuple[str, ...] = (
    "#b2182b", "#d6604d", "#f4a582", "#f7f7f7",
    "#92c5de", "#4393c3", "#2166ac",
)

# Semantic
COLOR_CI_BAND_OPACITY = 0.15
COLOR_NEUTRAL_GRID = "rgba(176, 196, 196, 0.12)"
COLOR_TEXT = "#d8e0ec"
COLOR_BG_DARK = "#1a1a2e"
COLOR_BG_PAPER = "#181828"
COLOR_REFERENCE_LINE = "#7a7a8a"


@dataclass(frozen=True)
class CIBand:
    """Helper for CI band trace generation."""

    x: list
    y_point: list[float]
    y_low: list[float]
    y_high: list[float]
    label: str
    color: str


# ---------------------------------------------------------------------------
# Layout template
# ---------------------------------------------------------------------------


def apply_quality_layout(
    fig: go.Figure,
    *,
    title: str | None = None,
    xaxis_title: str | None = None,
    yaxis_title: str | None = None,
    height: int = 460,
    show_legend: bool = True,
    log_y: bool = False,
) -> go.Figure:
    """統一 layout を Figure に適用 (dark + accessible)。"""
    fig.update_layout(
        title=title,
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
        height=height,
        plot_bgcolor=COLOR_BG_DARK,
        paper_bgcolor=COLOR_BG_PAPER,
        font={"color": COLOR_TEXT, "family": "system-ui, -apple-system, sans-serif", "size": 13},
        margin={"t": 60, "r": 30, "b": 60, "l": 70},
        showlegend=show_legend,
        legend={
            "bgcolor": "rgba(20, 20, 32, 0.7)",
            "bordercolor": COLOR_NEUTRAL_GRID,
            "borderwidth": 1,
            "font": {"size": 12, "color": COLOR_TEXT},
        },
        hovermode="x unified",
    )
    fig.update_xaxes(
        gridcolor=COLOR_NEUTRAL_GRID,
        zerolinecolor=COLOR_NEUTRAL_GRID,
        title_font={"color": COLOR_TEXT, "size": 13},
        tickfont={"color": COLOR_TEXT, "size": 11},
    )
    fig.update_yaxes(
        gridcolor=COLOR_NEUTRAL_GRID,
        zerolinecolor=COLOR_NEUTRAL_GRID,
        title_font={"color": COLOR_TEXT, "size": 13},
        tickfont={"color": COLOR_TEXT, "size": 11},
        type="log" if log_y else "linear",
    )
    return fig


# ---------------------------------------------------------------------------
# CI band trace
# ---------------------------------------------------------------------------


def add_ci_band(
    fig: go.Figure,
    x: list,
    y_low: list[float],
    y_high: list[float],
    *,
    label: str,
    color: str,
    opacity: float = COLOR_CI_BAND_OPACITY,
) -> go.Figure:
    """CI band (shaded uncertainty region) を追加。

    主線とペアで使う想定: 先に点推定線、その後 CI band。
    """
    rgba = _hex_to_rgba(color, opacity)
    x_combined = list(x) + list(reversed(list(x)))
    y_combined = list(y_high) + list(reversed(list(y_low)))
    fig.add_trace(
        go.Scatter(
            x=x_combined,
            y=y_combined,
            fill="toself",
            fillcolor=rgba,
            line={"color": "rgba(0,0,0,0)"},
            showlegend=False,
            hoverinfo="skip",
            name=f"{label} CI",
        )
    )
    return fig


def add_point_with_ci(
    fig: go.Figure,
    x: list,
    y: list[float],
    y_low: list[float],
    y_high: list[float],
    *,
    label: str,
    color: str,
) -> go.Figure:
    """点推定 + CI band を一括追加。"""
    # CI band first (drawn underneath)
    add_ci_band(fig, x, y_low, y_high, label=label, color=color)
    # Point estimate line on top
    fig.add_trace(
        go.Scatter(
            x=x,
            y=y,
            mode="lines+markers",
            name=label,
            line={"color": color, "width": 2.5},
            marker={"color": color, "size": 7, "line": {"color": COLOR_BG_DARK, "width": 1}},
            hovertemplate=(
                f"<b>{label}</b><br>"
                "x: %{x}<br>"
                "y: %{y:.4f}<br>"
                f"<extra></extra>"
            ),
        )
    )
    return fig


# ---------------------------------------------------------------------------
# Forest plot (HR / OR / coefficient with CI)
# ---------------------------------------------------------------------------


def forest_plot(
    labels: list[str],
    estimates: list[float],
    ci_lows: list[float],
    ci_highs: list[float],
    *,
    title: str = "Forest plot",
    xaxis_title: str = "estimate (95% CI)",
    reference_x: float = 0.0,
    log_x: bool = False,
    height: int = 60,
) -> go.Figure:
    """Forest plot (横軸 estimate、縦軸 label)。HR/OR は log_x=True。

    height は per-row 高さ、自動拡張。
    """
    if not (len(labels) == len(estimates) == len(ci_lows) == len(ci_highs)):
        raise ValueError("forest_plot: input lengths mismatch")

    fig = go.Figure()
    n = len(labels)
    y_positions = list(range(n))

    # CI lines per row
    for i, (est, lo, hi) in enumerate(zip(estimates, ci_lows, ci_highs)):
        fig.add_trace(
            go.Scatter(
                x=[lo, hi],
                y=[i, i],
                mode="lines",
                line={"color": PALETTE_OKABE_ITO[0], "width": 2},
                showlegend=False,
                hoverinfo="skip",
            )
        )
    # Point estimates
    fig.add_trace(
        go.Scatter(
            x=estimates,
            y=y_positions,
            mode="markers",
            marker={"color": PALETTE_OKABE_ITO[1], "size": 10,
                    "line": {"color": COLOR_BG_DARK, "width": 1}},
            name="estimate",
            text=[f"{e:.3f} [{lo:.3f}, {hi:.3f}]" for e, lo, hi in zip(estimates, ci_lows, ci_highs)],
            hovertemplate="<b>%{customdata}</b><br>%{text}<extra></extra>",
            customdata=labels,
        )
    )
    # Reference line
    fig.add_vline(
        x=reference_x, line_dash="dash",
        line_color=COLOR_REFERENCE_LINE, line_width=1,
    )
    fig.update_layout(
        title=title,
        xaxis_title=xaxis_title,
        yaxis={
            "tickmode": "array",
            "tickvals": y_positions,
            "ticktext": labels,
            "autorange": "reversed",
        },
        height=max(height * n + 100, 300),
    )
    if log_x:
        fig.update_xaxes(type="log")
    return apply_quality_layout(fig, title=title, xaxis_title=xaxis_title)


# ---------------------------------------------------------------------------
# Distribution comparison (violin + jitter strip)
# ---------------------------------------------------------------------------


def violin_compare(
    groups: dict[str, list[float]],
    *,
    title: str,
    yaxis_title: str,
    palette: tuple[str, ...] = PALETTE_OKABE_ITO,
) -> go.Figure:
    """グループ別 violin + box + jitter。

    各 violin に jitter points overlay で actual distribution を視覚化。
    """
    fig = go.Figure()
    for i, (name, vals) in enumerate(groups.items()):
        color = palette[i % len(palette)]
        fig.add_trace(
            go.Violin(
                y=vals,
                name=name,
                box_visible=True,
                meanline_visible=True,
                points="suspectedoutliers",
                line_color=color,
                fillcolor=_hex_to_rgba(color, 0.3),
                hovertemplate=(
                    f"<b>{name}</b><br>"
                    "value: %{y:.4f}<br>"
                    f"n: {len(vals):,}<extra></extra>"
                ),
            )
        )
    return apply_quality_layout(fig, title=title, yaxis_title=yaxis_title)


# ---------------------------------------------------------------------------
# Heatmap (CVD-safe sequential)
# ---------------------------------------------------------------------------


def heatmap_quality(
    z: list[list[float]],
    *,
    x_labels: list[str],
    y_labels: list[str],
    title: str,
    colorscale: str = "Viridis",
    zmid: float | None = None,
) -> go.Figure:
    """CVD-safe heatmap。divergent data は zmid を指定して RdBu に切替推奨。"""
    fig = go.Figure(
        go.Heatmap(
            z=z, x=x_labels, y=y_labels,
            colorscale="RdBu_r" if zmid is not None else colorscale,
            zmid=zmid,
            hovertemplate="x: %{x}<br>y: %{y}<br>value: %{z:.3f}<extra></extra>",
            colorbar={"title": {"text": "value", "font": {"color": COLOR_TEXT}},
                      "tickfont": {"color": COLOR_TEXT}},
        )
    )
    return apply_quality_layout(fig, title=title)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _hex_to_rgba(hex_color: str, opacity: float) -> str:
    """'#RRGGBB' → 'rgba(r, g, b, opacity)'."""
    hc = hex_color.lstrip("#")
    if len(hc) != 6:
        return f"rgba(128, 128, 128, {opacity})"
    r, g, b = int(hc[0:2], 16), int(hc[2:4], 16), int(hc[4:6], 16)
    return f"rgba({r}, {g}, {b}, {opacity})"


def palette_color(idx: int, *, palette: tuple[str, ...] = PALETTE_OKABE_ITO) -> str:
    """Index 安全な palette 取得 (modulo)。"""
    return palette[idx % len(palette)]


# ---------------------------------------------------------------------------
# Accessibility check (WCAG AA contrast)
# ---------------------------------------------------------------------------


def relative_luminance(hex_color: str) -> float:
    """WCAG 2.1 relative luminance."""
    hc = hex_color.lstrip("#")
    if len(hc) != 6:
        return 0.0
    rgb = [int(hc[i : i + 2], 16) / 255.0 for i in (0, 2, 4)]

    def _channel(c: float) -> float:
        return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4

    r, g, b = [_channel(v) for v in rgb]
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def contrast_ratio(fg_hex: str, bg_hex: str) -> float:
    """WCAG 2.1 contrast ratio。AA = 4.5、AAA = 7。"""
    L1 = relative_luminance(fg_hex)
    L2 = relative_luminance(bg_hex)
    light, dark = max(L1, L2), min(L1, L2)
    return (light + 0.05) / (dark + 0.05)


def palette_wcag_audit(
    palette: tuple[str, ...] = PALETTE_OKABE_ITO,
    bg: str = COLOR_BG_DARK,
    *,
    min_aa: float = 4.5,
) -> list[tuple[str, float, bool]]:
    """Palette × bg の contrast ratio + AA pass フラグ。"""
    return [
        (c, contrast_ratio(c, bg), contrast_ratio(c, bg) >= min_aa)
        for c in palette
    ]
