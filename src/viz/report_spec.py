"""レポート構造の仕様 (ReportSpec).

チャート・テーブル・統計カード・セクションを組み合わせて
1つのレポートドキュメントを定義する。
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass, field

from src.viz.chart_spec import ChartSpec


@dataclass(frozen=True)
class StatCardSpec:
    """統計カード（stats-grid内の1アイテム）."""

    label: str
    value: str
    badge_class: str = ""  # "badge-high", "badge-mid", "badge-low", or ""


@dataclass(frozen=True)
class TableSpec:
    """HTMLテーブル."""

    headers: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]
    sortable: bool = True
    caption: str = ""


@dataclass(frozen=True)
class SectionSpec:
    """レポート内の1セクション."""

    title: str
    description: str = ""
    charts: tuple[ChartSpec, ...] = ()
    tables: tuple[TableSpec, ...] = ()
    stats: tuple[StatCardSpec, ...] = ()
    subsections: tuple[SectionSpec, ...] = ()


@dataclass(frozen=True)
class ReportSpec:
    """完全なレポート定義."""

    title: str
    subtitle: str = ""
    audience: str = ""
    description: str = ""
    sections: tuple[SectionSpec, ...] = ()
    glossary: dict[str, str] = field(default_factory=dict)


# ── ユーティリティ ──


def iter_charts(section: SectionSpec) -> Iterator[ChartSpec]:
    """セクション + サブセクション内の全チャートをイテレート."""
    yield from section.charts
    for sub in section.subsections:
        yield from sub.charts


def slugify(text: str) -> str:
    """セクションタイトル → HTML id 用スラグ."""
    slug = re.sub(r"[^\w\s-]", "", text)
    slug = re.sub(r"[\s_]+", "-", slug).strip("-").lower()
    return slug or "section"
