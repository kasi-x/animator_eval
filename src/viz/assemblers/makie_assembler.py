"""MakieAssembler — ReportSpec → CairoMakie SVG ファイル群.

ReportSpec 内の全チャートを CairoMakie で SVG に変換し、
レポートごとのディレクトリに保存する。

出力構造:
    result/makie/{report_name}/
    ├── {chart_id}.svg
    ├── {chart_id}.svg
    └── ...
"""

from __future__ import annotations

from pathlib import Path

import structlog

from src.viz.renderers.makie_renderer import MakieRenderer
from src.viz.report_spec import ReportSpec, iter_charts

logger = structlog.get_logger()


class MakieAssembler:
    """ReportSpec → SVG ファイル群."""

    def __init__(self, renderer: MakieRenderer | None = None) -> None:
        self._renderer = renderer or MakieRenderer()

    def assemble(self, spec: ReportSpec, output_dir: Path) -> list[Path]:
        """ReportSpec 内の全チャートを SVG として output_dir に保存."""
        output_dir.mkdir(parents=True, exist_ok=True)
        saved: list[Path] = []

        for section in spec.sections:
            for chart in iter_charts(section):
                svg_path = output_dir / f"{chart.chart_id}.svg"
                try:
                    self._renderer.render_svg(chart, svg_path)
                    saved.append(svg_path)
                except Exception:
                    logger.exception(
                        "makie_assembler.chart_failed",
                        chart_id=chart.chart_id,
                        report=spec.title,
                    )

        logger.info(
            "makie_assembler.complete",
            report=spec.title,
            charts_saved=len(saved),
            output_dir=str(output_dir),
        )
        return saved
