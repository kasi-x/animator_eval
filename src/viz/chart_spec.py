"""バックエンド非依存のチャート仕様 (ChartSpec) 階層.

PlotlyにもCairoMakieにも依存しない純粋なデータ構造として、
全チャートを定義する。Rendererがtype dispatchで描画を行う。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


# ──────────────────────────────────────────────
# 補助型
# ──────────────────────────────────────────────

@dataclass(frozen=True)
class Annotation:
    """チャート上のテキストアノテーション."""

    text: str
    x: float | str = 0.02
    y: float | str = 0.98
    x_ref: Literal["data", "paper"] = "paper"
    y_ref: Literal["data", "paper"] = "paper"
    font_size: int = 11
    color: str = "#FFD166"
    show_arrow: bool = False


@dataclass(frozen=True)
class AxisSpec:
    """軸の設定."""

    label: str = ""
    log_scale: bool = False
    range: tuple[float, float] | None = None
    category_order: list[str] | None = None
    tick_format: str | None = None
    reversed: bool = False


# 10色のデフォルトパレット（プロジェクト共通）
_DEFAULT_PALETTE = (
    "#f093fb", "#667eea", "#06D6A0", "#EF476F", "#FFD166",
    "#a0d2db", "#fda085", "#9B59B6", "#2ECC71", "#E74C3C",
)


@dataclass(frozen=True)
class ColorMapping:
    """色設定（パレット・カラースケール・透明度）."""

    palette: tuple[str, ...] = _DEFAULT_PALETTE
    colorscale: str = "Viridis"
    opacity: float = 0.7


# ──────────────────────────────────────────────
# 説明メタデータ — 理想的説明プロセスの構造
# ──────────────────────────────────────────────

@dataclass(frozen=True)
class ExplanationMeta:
    """全チャートに付与する構造化された説明.

    question と reading_guide は必須。残りはオプションだが推奨。
    key_findings はデータ駆動で算出すること（ハードコード禁止）。
    """

    # 必須フィールド
    question: str       # このチャートが答える問い
    reading_guide: str  # 軸・色・パターンの読み方

    # オプション（推奨）
    key_findings: tuple[str, ...] = ()
    caveats: tuple[str, ...] = ()
    competing_interpretations: tuple[tuple[str, tuple[str, ...]], ...] = ()
    context: str = ""
    significance: str = ""
    utilization: tuple[dict[str, str], ...] = ()
    glossary: dict[str, str] = field(default_factory=dict)


# ──────────────────────────────────────────────
# 基底 ChartSpec
# ──────────────────────────────────────────────

@dataclass(frozen=True)
class ChartSpec:
    """全チャート型の基底クラス."""

    chart_id: str
    title: str
    explanation: ExplanationMeta
    height: int = 500
    x_axis: AxisSpec = field(default_factory=AxisSpec)
    y_axis: AxisSpec = field(default_factory=AxisSpec)
    colors: ColorMapping = field(default_factory=ColorMapping)
    annotations: tuple[Annotation, ...] = ()
    renderer_hints: dict[str, Any] = field(default_factory=dict)


# ──────────────────────────────────────────────
# 具象 ChartSpec 型（14タイプ）
# ──────────────────────────────────────────────

@dataclass(frozen=True)
class ScatterSpec(ChartSpec):
    """散布図 / ScatterGL."""

    x: tuple[float, ...] = ()
    y: tuple[float, ...] = ()
    labels: tuple[str, ...] | None = None
    sizes: tuple[float, ...] | None = None
    categories: tuple[str, ...] | None = None
    mode: Literal["markers", "lines", "markers+lines", "markers+text"] = "markers"
    show_regression: bool = False
    max_points: int = 5000
    label_top_n: int = 0


@dataclass(frozen=True)
class DensityScatterSpec(ChartSpec):
    """2D密度等高線 + 上位ラベル."""

    x: tuple[float, ...] = ()
    y: tuple[float, ...] = ()
    label_names: tuple[str, ...] | None = None
    label_top: int = 15
    n_contours: int = 20


@dataclass(frozen=True)
class BarSpec(ChartSpec):
    """棒グラフ（垂直/水平/積み上げ）."""

    categories: tuple[str, ...] = ()
    values: tuple[float, ...] = ()
    orientation: Literal["v", "h"] = "v"
    stacked_series: dict[str, tuple[float, ...]] | None = None
    error_bars: tuple[float, ...] | None = None
    bar_mode: Literal["group", "stack", "relative"] = "group"


@dataclass(frozen=True)
class HeatmapSpec(ChartSpec):
    """ヒートマップ."""

    z: tuple[tuple[float, ...], ...] = ()
    x_labels: tuple[str, ...] = ()
    y_labels: tuple[str, ...] = ()
    show_text: bool = True


@dataclass(frozen=True)
class ViolinSpec(ChartSpec):
    """バイオリン / レインクラウドプロット."""

    groups: dict[str, tuple[float, ...]] = field(default_factory=dict)
    show_box: bool = True
    show_points: Literal["all", "outliers", "none"] = "outliers"
    side: Literal["both", "positive", "negative"] = "both"
    raincloud: bool = False


@dataclass(frozen=True)
class BoxSpec(ChartSpec):
    """箱ひげ図."""

    groups: dict[str, tuple[float, ...]] = field(default_factory=dict)
    show_points: Literal["all", "outliers", "none"] = "outliers"


@dataclass(frozen=True)
class HistogramSpec(ChartSpec):
    """ヒストグラム."""

    values: tuple[float, ...] = ()
    nbins: int | None = None
    multi_series: dict[str, tuple[float, ...]] | None = None


@dataclass(frozen=True)
class LineSpec(ChartSpec):
    """折れ線 / 時系列 / 積み上げ面グラフ.

    通常の折れ線: series = {"name": ((x1, y1), (x2, y2), ...)}
    積み上げ面: stacked=True, x=(...), stacked_series = {"name": (y1, y2, ...)}
    """

    series: dict[str, tuple[tuple[float, float], ...]] = field(default_factory=dict)
    ci_bands: dict[str, tuple[tuple[float, float, float], ...]] | None = None
    fill_area: bool = False
    stacked: bool = False
    x: tuple[float | int | str, ...] = ()
    stacked_series: dict[str, tuple[float, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class SankeySpec(ChartSpec):
    """サンキーダイアグラム."""

    nodes: tuple[str, ...] = ()
    node_colors: tuple[str, ...] | None = None
    links: tuple[tuple[int, int, float], ...] = ()
    link_colors: tuple[str, ...] | None = None


@dataclass(frozen=True)
class ForestPlotSpec(ChartSpec):
    """フォレストプロット（水平CI + 点推定）."""

    estimates: tuple[dict[str, Any], ...] = ()
    reference_line: float = 0.0


@dataclass(frozen=True)
class RidgePlotSpec(ChartSpec):
    """リッジプロット / KDE分布比較."""

    groups: dict[str, tuple[float, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class PieSpec(ChartSpec):
    """円グラフ / ドーナツ."""

    labels: tuple[str, ...] = ()
    values: tuple[float, ...] = ()
    hole: float = 0.0


@dataclass(frozen=True)
class NetworkGraphSpec(ChartSpec):
    """ネットワークグラフ."""

    nodes: tuple[dict[str, Any], ...] = ()
    edges: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class SubplotSpec(ChartSpec):
    """複合チャート（グリッドレイアウト）."""

    specs: tuple[ChartSpec, ...] = ()
    rows: int = 1
    cols: int = 1
    subplot_titles: tuple[str, ...] = ()


# すべてのChartSpec型のレジストリ
ALL_CHART_TYPES: tuple[type[ChartSpec], ...] = (
    ScatterSpec, DensityScatterSpec, BarSpec, HeatmapSpec,
    ViolinSpec, BoxSpec, HistogramSpec, LineSpec,
    SankeySpec, ForestPlotSpec, RidgePlotSpec, PieSpec,
    NetworkGraphSpec, SubplotSpec,
)
