"""Stat cards and tables — non-chart display primitives."""

from __future__ import annotations

from dataclasses import dataclass

from src.reporting.specs.explanation import ExplanationMeta


@dataclass(frozen=True)
class StatCardSpec:
    """Single big-number card.

    ``value_field`` is a key into the data dict returned by the provider.
    """

    label: str
    value_field: str
    value_format: str = "{:,.0f}"
    badge: str = ""      # "high" | "mid" | "low" | ""
    sublabel: str = ""


@dataclass(frozen=True)
class TableSpec:
    """Structured table.

    ``data_key`` resolves to a list of dicts. ``columns`` is a tuple of
    ``(field_name, display_name)`` pairs.
    """

    slug: str
    title: str
    data_key: str
    columns: tuple[tuple[str, str], ...]
    sortable: bool = True
    searchable: bool = False         # e.g. N1 browser
    max_rows: int | None = 100
    explanation: ExplanationMeta | None = None
