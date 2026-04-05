"""Structured info dataclasses for DATA_SCOPE, METHODS, REPRODUCIBILITY sections.

These enforce the "academic" contract that each of these sections contains
specific fields — sample flow, identification assumptions, code references —
instead of freeform text.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DataScopeInfo:
    """Structured content for a DATA_SCOPE section.

    ``filter_steps`` is an ordered list of ``(description, remaining_n)``
    tuples that describes the sample flow from ``original_n`` to ``final_n``.
    """

    original_n: int
    final_n: int
    filter_steps: tuple[tuple[str, int], ...]

    source_json_files: tuple[str, ...]
    source_db_tables: tuple[str, ...] = ()

    exclusion_criteria: tuple[str, ...] = ()

    # Quality flags — ``anime_score_used`` must be False for every report
    # (see validation rule R-5; CLAUDE.md hard constraint).
    anime_score_used: bool = False
    known_biases: tuple[str, ...] = ()

    time_range: tuple[int, int] | None = None


@dataclass(frozen=True)
class MethodsInfo:
    """Structured content for a METHODS section.

    ``equations`` is a tuple of ``(label, latex_or_text)`` pairs. Labels are
    human-facing, e.g. "Eq. 1: AKM".

    ``rejected_alternatives`` documents *why* obvious alternatives were not
    chosen — this is part of what makes a method justifiable.

    ``code_references`` is required and enforced by R-6: the implementation
    is the single source of truth for *how* a method was applied.
    """

    equations: tuple[tuple[str, str], ...] = ()
    estimator_description: str = ""
    identification_assumptions: tuple[str, ...] = ()
    rejected_alternatives: tuple[tuple[str, str], ...] = ()
    code_references: tuple[str, ...] = ()


@dataclass(frozen=True)
class ReproducibilityInfo:
    """Structured content for a REPRODUCIBILITY section.

    Lists the exact inputs, SQL queries, code entry points, seeds, and
    parameters required to re-derive the report. ``inputs`` is required and
    enforced by R-7.
    """

    inputs: tuple[str, ...]
    db_queries: tuple[tuple[str, str], ...] = ()
    entry_points: tuple[str, ...] = ()
    seeds: tuple[tuple[str, int], ...] = ()
    parameters: tuple[tuple[str, str], ...] = ()
    pipeline_run_id: str = ""
    data_snapshot_date: str = ""
