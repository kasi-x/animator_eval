"""レポートレジストリ — 全レポートビルダーの登録."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from src.viz.report_spec import ReportSpec

# (name, builder_function) のリスト
# builder は json_dir: Path を受け取り ReportSpec | None を返す
ReportBuilder = Callable[[Path], ReportSpec | None]

ALL_REPORTS: list[tuple[str, ReportBuilder]] = []


def _register() -> None:
    """遅延インポートでレポートビルダーを登録."""
    from src.viz.reports.bridge_report import build_bridge_report
    from src.viz.reports.growth_score_report import build_growth_score_report
    from src.viz.reports.network_evolution_report import build_network_evolution_report

    ALL_REPORTS.clear()
    ALL_REPORTS.extend([
        ("bridge_analysis", build_bridge_report),
        ("growth_score", build_growth_score_report),
        ("network_evolution", build_network_evolution_report),
    ])


def get_all_reports() -> list[tuple[str, ReportBuilder]]:
    """登録済み全レポートを返す."""
    if not ALL_REPORTS:
        _register()
    return ALL_REPORTS
