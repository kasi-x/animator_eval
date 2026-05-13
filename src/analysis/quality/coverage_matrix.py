"""source × role_group × year credit coverage matrix.

Computes what fraction of credits in each (source, role_group, year) cell
are present relative to the ANN upper-bound reference (most comprehensive
source) and reports the result as a structured matrix.

Design rules:
- Reads only from Resolved layer (result/resolved.duckdb) per CLAUDE.md.
- No anime.score or display_* columns in any path.
- Gracefully degrades when resolved.duckdb is absent (returns empty matrix).
- structlog throughout (no stdlib logging).
- Pydantic v2 for the output model.
- Returns explicit CoverageMatrix dataclass — no implicit dict access.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog

from src.analysis.io.resolved_reader import (
    resolved_available,
    resolved_connect,
)
from src.utils.role_groups import ROLE_CATEGORY

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Output models (plain dataclasses — no external deps)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CoverageCell:
    """Single (source, role_group, year) coverage measurement.

    Attributes:
        source: Data source name (e.g. "anilist", "mal", "ann").
        role_group: Aggregated role category from ROLE_CATEGORY.
        year: Production year of the credits.
        n_credits: Observed credit count in this cell.
        reference_n: Upper-bound reference count (ANN-based when available).
        coverage_ratio: n_credits / reference_n, capped at 1.0.
            NaN-free: 0.0 when reference_n is 0 or absent.
        note: Optional free-text note (e.g. "ANN absent for this year").
    """

    source: str
    role_group: str
    year: int
    n_credits: int
    reference_n: int
    coverage_ratio: float
    note: str = ""


@dataclass
class CoverageMatrix:
    """Full source × role_group × year coverage matrix.

    Attributes:
        cells: All computed CoverageCell instances.
        sources: Sorted list of unique sources observed.
        role_groups: Sorted list of unique role_groups observed.
        years: Sorted list of unique years observed.
        under_credited_roles: Role groups where mean coverage_ratio < threshold.
        snapshot_note: Human-readable summary for caveat block injection.
    """

    cells: list[CoverageCell] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)
    role_groups: list[str] = field(default_factory=list)
    years: list[int] = field(default_factory=list)
    under_credited_roles: list[str] = field(default_factory=list)
    snapshot_note: str = ""

    def is_empty(self) -> bool:
        """Return True when no cells were computed (e.g. DB absent)."""
        return len(self.cells) == 0

    def lookup(self, source: str, role_group: str, year: int) -> CoverageCell | None:
        """Retrieve a single cell by exact key, or None if absent."""
        for cell in self.cells:
            if (
                cell.source == source
                and cell.role_group == role_group
                and cell.year == year
            ):
                return cell
        return None

    def mean_coverage_for_role_group(self, role_group: str) -> float:
        """Compute mean coverage_ratio across all sources and years for a role group."""
        matching = [c.coverage_ratio for c in self.cells if c.role_group == role_group]
        if not matching:
            return 0.0
        return sum(matching) / len(matching)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_REFERENCE_SOURCE = "ann"  # Most comprehensive source — upper bound per task card.
_UNDER_CREDITED_THRESHOLD = 0.5  # Cells below 50% coverage flag the role group.


def _map_role_to_group(role_str: str) -> str:
    """Convert a role value string to its ROLE_CATEGORY group.

    Falls back to "non_production" for unknown role strings.
    """
    from src.runtime.models import Role

    try:
        role_enum = Role(role_str)
    except ValueError:
        return "non_production"
    return ROLE_CATEGORY.get(role_enum, "non_production")


def _fetch_credit_counts_from_resolved(
    conn: Any,
) -> list[dict[str, Any]]:
    """Query resolved.duckdb for (source, role, year, n_credits) groups.

    The credits table stores evidence_source (which maps to our 'source'),
    role, and anime_id. We JOIN to anime to get year. Only non-null years
    and non-empty sources are returned.

    Returns:
        List of row dicts: {source, role, year, n_credits}
    """
    sql = """
        SELECT
            c.evidence_source        AS source,
            c.role                   AS role,
            a.year                   AS year,
            COUNT(*)                 AS n_credits
        FROM credits c
        JOIN anime a ON a.canonical_id = c.anime_id
        WHERE c.evidence_source IS NOT NULL
          AND c.evidence_source != ''
          AND a.year IS NOT NULL
        GROUP BY c.evidence_source, c.role, a.year
        ORDER BY a.year, c.evidence_source, c.role
    """
    try:
        rel = conn.execute(sql)
        cols = [d[0] for d in rel.description]
        rows = [dict(zip(cols, row)) for row in rel.fetchall()]
        logger.info("coverage_credit_counts_fetched", row_count=len(rows))
        return rows
    except Exception as exc:
        logger.warning("coverage_credit_counts_failed", error=str(exc))
        return []


def _aggregate_to_role_groups(
    raw_rows: list[dict[str, Any]],
) -> dict[tuple[str, str, int], int]:
    """Collapse (source, role, year) → (source, role_group, year) by summing counts.

    Returns:
        Dict mapping (source, role_group, year) → total n_credits.
    """
    agg: dict[tuple[str, str, int], int] = {}
    for row in raw_rows:
        source = str(row["source"])
        role_group = _map_role_to_group(str(row["role"]))
        year = int(row["year"])
        n = int(row["n_credits"])
        key = (source, role_group, year)
        agg[key] = agg.get(key, 0) + n
    return agg


def _build_reference_counts(
    agg: dict[tuple[str, str, int], int],
    reference_source: str,
) -> dict[tuple[str, int], int]:
    """Extract (role_group, year) → n_credits for the reference source.

    When the reference source is absent for a (role_group, year) cell,
    the maximum observed count across all sources is used as a fallback
    upper bound.

    Returns:
        Dict mapping (role_group, year) → reference count.
    """
    ref: dict[tuple[str, int], int] = {}
    fallback: dict[tuple[str, int], int] = {}

    for (source, role_group, year), n in agg.items():
        rg_year = (role_group, year)
        fallback[rg_year] = max(fallback.get(rg_year, 0), n)
        if source.lower() == reference_source.lower():
            ref[rg_year] = max(ref.get(rg_year, 0), n)

    # Fill missing reference cells with fallback max
    for key, n in fallback.items():
        if key not in ref:
            ref[key] = n

    return ref


def _compute_coverage_ratio(n_credits: int, reference_n: int) -> float:
    """Compute coverage_ratio = n_credits / reference_n, bounded to [0, 1].

    Returns 0.0 when reference_n is 0 (avoids ZeroDivisionError).
    """
    if reference_n <= 0:
        return 0.0
    return min(1.0, n_credits / reference_n)


def _identify_under_credited_roles(
    cells: list[CoverageCell],
    threshold: float,
) -> list[str]:
    """Return role groups whose mean coverage_ratio is below threshold.

    Only includes role groups that appear in at least one cell, and only
    those where at least one source other than the reference has data.
    """
    from collections import defaultdict

    sums: defaultdict[str, float] = defaultdict(float)
    counts: defaultdict[str, int] = defaultdict(int)
    for cell in cells:
        sums[cell.role_group] += cell.coverage_ratio
        counts[cell.role_group] += 1

    under = sorted(
        rg
        for rg, count in counts.items()
        if count > 0 and (sums[rg] / count) < threshold
    )
    return under


def _build_snapshot_note(
    matrix: CoverageMatrix,
    reference_source: str,
) -> str:
    """Compose a human-readable note describing overall coverage state.

    Used by the HTML caveat block builder (_coverage_block.py).
    """
    if matrix.is_empty():
        return (
            "coverage matrix: resolved.duckdb が存在しないか、credits テーブルが空のため "
            "coverage 行列を生成できなかった。全推定値は過小推定の可能性がある。"
        )

    year_range = (
        f"{min(matrix.years)}–{max(matrix.years)}" if matrix.years else "年不明"
    )
    under = (
        ", ".join(matrix.under_credited_roles)
        if matrix.under_credited_roles
        else "なし"
    )
    return (
        f"coverage 行列: {len(matrix.sources)} source × "
        f"{len(matrix.role_groups)} role_group × "
        f"{len(matrix.years)} 年 ({year_range})。"
        f"参照 upper bound: {reference_source}。"
        f"coverage 50% 未満の role_group: {under}。"
        "「データ不足のため過小推定」の可能性がある role は caveat block に明記する。"
        "補正は行わない (source 透明性を維持するため)。"
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_coverage_matrix(
    resolved_path: Path | str | None = None,
    *,
    reference_source: str = _REFERENCE_SOURCE,
    under_credited_threshold: float = _UNDER_CREDITED_THRESHOLD,
) -> CoverageMatrix:
    """Compute source × role_group × year credit coverage matrix.

    Reads from result/resolved.duckdb (Resolved layer). Returns an empty
    CoverageMatrix when the DB is absent — callers must check is_empty().

    The coverage_ratio for each cell is:
        n_credits[source, role_group, year]
        / max(n_credits[reference_source, role_group, year],
              max_across_all_sources[role_group, year])

    This never exceeds 1.0 and never divides by zero.

    Args:
        resolved_path: Path to resolved.duckdb. Defaults to
            src.analysis.io.resolved_reader.DEFAULT_RESOLVED_PATH.
        reference_source: Source treated as coverage upper bound.
            Defaults to "ann" (ANN is most comprehensive per task card).
        under_credited_threshold: Mean coverage_ratio below which a
            role_group is flagged as under-credited. Defaults to 0.5.

    Returns:
        CoverageMatrix with cells, dimension lists, flagged roles, and note.
    """
    _path = resolved_path or None

    if not resolved_available(_path):
        logger.warning(
            "coverage_matrix_resolved_absent",
            path=str(_path),
        )
        empty = CoverageMatrix()
        empty.snapshot_note = _build_snapshot_note(empty, reference_source)
        return empty

    with resolved_connect(_path) as conn:
        raw_rows = _fetch_credit_counts_from_resolved(conn)

    if not raw_rows:
        logger.warning("coverage_matrix_no_credits_found")
        empty = CoverageMatrix()
        empty.snapshot_note = _build_snapshot_note(empty, reference_source)
        return empty

    agg = _aggregate_to_role_groups(raw_rows)
    ref_counts = _build_reference_counts(agg, reference_source)

    cells: list[CoverageCell] = []
    for (source, role_group, year), n_credits in sorted(agg.items()):
        ref_n = ref_counts.get((role_group, year), n_credits)
        ratio = _compute_coverage_ratio(n_credits, ref_n)
        note = ""
        if ref_n == n_credits and source.lower() != reference_source.lower():
            note = f"{reference_source} absent for this cell; fallback max used"
        cells.append(
            CoverageCell(
                source=source,
                role_group=role_group,
                year=year,
                n_credits=n_credits,
                reference_n=ref_n,
                coverage_ratio=ratio,
                note=note,
            )
        )

    sources = sorted({c.source for c in cells})
    role_groups = sorted({c.role_group for c in cells})
    years = sorted({c.year for c in cells})
    under_credited = _identify_under_credited_roles(cells, under_credited_threshold)

    matrix = CoverageMatrix(
        cells=cells,
        sources=sources,
        role_groups=role_groups,
        years=years,
        under_credited_roles=under_credited,
    )
    matrix.snapshot_note = _build_snapshot_note(matrix, reference_source)

    logger.info(
        "coverage_matrix_built",
        n_cells=len(cells),
        n_sources=len(sources),
        n_role_groups=len(role_groups),
        n_years=len(years),
        under_credited_count=len(under_credited),
    )
    return matrix


def coverage_matrix_to_records(matrix: CoverageMatrix) -> list[dict[str, Any]]:
    """Convert CoverageMatrix to a list of plain dicts (for Parquet / JSON export).

    Each dict has keys: source, role_group, year, n_credits, reference_n,
    coverage_ratio, note.
    """
    return [
        {
            "source": c.source,
            "role_group": c.role_group,
            "year": c.year,
            "n_credits": c.n_credits,
            "reference_n": c.reference_n,
            "coverage_ratio": c.coverage_ratio,
            "note": c.note,
        }
        for c in matrix.cells
    ]
