"""MakieRenderer — ChartSpec → CairoMakie SVG.

juliacall 経由で Julia/CairoMakie を呼び出し、
ChartSpec を論文品質の SVG ファイルとして出力する。

Usage:
    renderer = MakieRenderer()
    renderer.render_svg(spec, Path("output/chart.svg"))
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import structlog

from src.viz.chart_spec import ChartSpec

logger = structlog.get_logger()

# Julia プロジェクトパス
_JULIA_VIZ_DIR = Path(__file__).resolve().parent.parent.parent.parent / "julia_viz"


class MakieRenderer:
    """ChartSpec → CairoMakie SVG レンダラー.

    初回インスタンス化時に juliacall で Julia を起動し、
    JuliaViz モジュールをロードする。以降は同一セッションで
    繰り返し render_svg() を呼べる（起動コスト回避）。
    """

    def __init__(self) -> None:
        self._jl: Any = None
        self._initialized = False

    def _ensure_initialized(self) -> None:
        """遅延初期化: Julia セッション起動 + JuliaViz ロード."""
        if self._initialized:
            return

        try:
            from juliacall import Main as jl  # type: ignore[import-untyped]
        except ImportError as e:
            raise RuntimeError(
                "juliacall is not installed. "
                "Install via: pixi install -e viz  or  pip install juliacall"
            ) from e

        logger.info("makie_renderer.initializing", julia_viz_dir=str(_JULIA_VIZ_DIR))

        # Julia プロジェクトをアクティベート + JuliaViz ロード
        # juliacall の Main.seval は Julia コードを実行する標準 API
        _activate_julia_project(jl, _JULIA_VIZ_DIR)

        self._jl = jl
        self._initialized = True
        logger.info("makie_renderer.initialized")

    def render_svg(self, spec: ChartSpec, output_path: Path) -> Path:
        """ChartSpec → SVG ファイル.

        Args:
            spec: レンダリングするチャート仕様
            output_path: 出力 SVG ファイルパス

        Returns:
            出力ファイルの Path
        """
        self._ensure_initialized()

        output_path.parent.mkdir(parents=True, exist_ok=True)

        # ChartSpec → JSON (chart_type フィールドを追加)
        spec_dict = _spec_to_dict(spec)
        spec_json = json.dumps(spec_dict, ensure_ascii=False, default=str)

        # Julia 側でレンダリング + 保存
        self._jl.JuliaViz.render_and_save(spec_json, str(output_path))

        logger.info(
            "makie_renderer.saved",
            chart_id=spec.chart_id,
            path=str(output_path),
        )
        return output_path

    @property
    def is_available(self) -> bool:
        """juliacall が利用可能かチェック."""
        try:
            import juliacall  # type: ignore[import-untyped]  # noqa: F401

            return True
        except ImportError:
            return False


def _activate_julia_project(jl: Any, project_dir: Path) -> None:
    """Julia プロジェクトをアクティベートし JuliaViz をロードする.

    juliacall の Main.seval() は Julia 式を実行する公式 API。
    ここではパッケージ管理と module ロードのみに使用。
    """
    # Pkg.activate で julia_viz プロジェクトを有効化
    activate_expr = f'using Pkg; Pkg.activate("{project_dir}")'
    jl.seval(activate_expr)  # noqa: S307 — juliacall API, not Python eval

    # LOAD_PATH に追加して using JuliaViz を可能にする
    load_path_expr = f'push!(LOAD_PATH, "{project_dir / "src"}")'
    jl.seval(load_path_expr)  # noqa: S307

    # JuliaViz モジュールをロード
    jl.seval("using JuliaViz")  # noqa: S307


def _spec_to_dict(spec: ChartSpec) -> dict[str, Any]:
    """ChartSpec → dict (chart_type フィールド付き)."""
    from src.viz.chart_spec import LineSpec

    d = asdict(spec)

    # LineSpec(stacked=True) → Julia 側の StackedAreaSpec にマッピング
    if isinstance(spec, LineSpec) and spec.stacked:
        d["chart_type"] = "StackedAreaSpec"
        d["x"] = d.get("x", [])
        d["series"] = d.get("stacked_series", {})
    else:
        d["chart_type"] = type(spec).__name__

    # tuple → list 変換 (JSON互換)
    return _convert_tuples(d)


def _convert_tuples(obj: Any) -> Any:
    """再帰的に tuple を list に変換."""
    if isinstance(obj, tuple):
        return [_convert_tuples(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _convert_tuples(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_tuples(item) for item in obj]
    return obj
