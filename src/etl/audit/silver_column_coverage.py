"""SILVER column coverage audit tool.

Measures the NULL/empty rate of every nullable column in the primary SILVER
tables (anime, persons, characters, studios) and cross-references BRONZE
parquet to identify *silent ingestion failures* — cases where BRONZE has a
value but SILVER does not.

Usage (CLI):
    python -m src.etl.audit.silver_column_coverage \
        --bronze-root result/bronze \
        --silver-db   result/silver.duckdb \
        --out         result/audit/silver_column_coverage.md

Public API:
    measure_column_coverage(conn, table, col) -> ColumnCoverageRow
    find_bronze_source_with_value(bronze_conn, bronze_root, source, table, col) -> int
    gap_analysis(silver_conn, bronze_conn, bronze_root) -> list[GapRow]
    generate_report(gap_rows, out_path)
"""
from __future__ import annotations

import datetime
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import duckdb
import structlog

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# COLUMN_BRONZE_MAP
#
# Maps (silver_table, silver_col) → dict of source_name → (bronze_table, bronze_col)
#
# Only columns that are nullable in SILVER *and* have a known BRONZE counterpart
# are listed here.  Columns not in this map are reported as "unmapped".
#
# bronze_col: the actual column name in the BRONZE parquet.
# ---------------------------------------------------------------------------

COLUMN_BRONZE_MAP: dict[
    tuple[str, str],
    dict[str, tuple[str, str]],
] = {
    # ── persons ────────────────────────────────────────────────────────────────
    ("persons", "gender"): {
        "anilist":  ("persons", "gender"),
        "bangumi":  ("persons", "gender"),
        "keyframe": ("persons", "gender"),
    },
    ("persons", "birth_date"): {
        "anilist":  ("persons", "date_of_birth"),
        "bangumi":  ("persons", "birth_year"),   # partial: year only
    },
    ("persons", "death_date"): {
        # No known BRONZE source carries death_date; reported as unmapped
    },
    ("persons", "image_large"): {
        "anilist":  ("persons", "image_large"),
        "keyframe": ("persons", "image_large"),
    },
    ("persons", "image_medium"): {
        "anilist":  ("persons", "image_medium"),
        "keyframe": ("persons", "image_medium"),
    },
    ("persons", "hometown"): {
        "anilist":  ("persons", "hometown"),
        "keyframe": ("persons", "hometown"),
    },
    ("persons", "nationality"): {
        "anilist":  ("persons", "nationality"),
        "keyframe": ("persons", "nationality"),
    },
    ("persons", "description"): {
        "anilist":  ("persons", "description"),
        "bangumi":  ("persons", "summary"),
    },
    ("persons", "blood_type"): {
        "anilist":  ("persons", "blood_type"),
        "bangumi":  ("persons", "blood_type"),
    },
    ("persons", "website_url"): {
        "anilist":  ("persons", "site_url"),
    },
    # ── anime ──────────────────────────────────────────────────────────────────
    ("anime", "country_of_origin"): {
        "anilist":  ("anime", "country_of_origin"),
    },
    ("anime", "year"): {
        "anilist":  ("anime", "year"),
        "mal":      ("anime", "year"),
    },
    ("anime", "episodes"): {
        "anilist":  ("anime", "episodes"),
        "mal":      ("anime", "episodes"),
    },
    ("anime", "format"): {
        "anilist":  ("anime", "format"),
        "mal":      ("anime", "type"),           # mal uses "type" for format
    },
    ("anime", "synonyms"): {
        "anilist":  ("anime", "synonyms"),
    },
    ("anime", "trailer_url"): {
        "anilist":  ("anime", "trailer_url"),
    },
    ("anime", "external_links_json"): {
        "anilist":  ("anime", "external_links_json"),
    },
    ("anime", "airing_schedule_json"): {
        "anilist":  ("anime", "airing_schedule_json"),
    },
    ("anime", "description"): {
        "anilist":  ("anime", "description"),
        "mal":      ("anime", "synopsis"),
    },
    ("anime", "start_date"): {
        "anilist":  ("anime", "start_date"),
        "mal":      ("anime", "aired_from"),
    },
    ("anime", "status"): {
        "anilist":  ("anime", "status"),
        "mal":      ("anime", "status"),
    },
    # ── characters ─────────────────────────────────────────────────────────────
    ("characters", "gender"): {
        "anilist":  ("characters", "gender"),
    },
    ("characters", "description"): {
        "anilist":  ("characters", "description"),
    },
    ("characters", "date_of_birth"): {
        "anilist":  ("characters", "date_of_birth"),
    },
    ("characters", "image_large"): {
        "anilist":  ("characters", "image_large"),
    },
    ("characters", "image_medium"): {
        "anilist":  ("characters", "image_medium"),
    },
    # ── studios ────────────────────────────────────────────────────────────────
    ("studios", "country_of_origin"): {
        "anilist":  ("studios", "country_of_origin"),
    },
    ("studios", "is_animation_studio"): {
        "anilist":  ("studios", "is_animation_studio"),
    },
    ("studios", "site_url"): {
        "anilist":  ("studios", "site_url"),
    },
}

# ---------------------------------------------------------------------------
# SILVER tables and columns to audit
# ---------------------------------------------------------------------------

# (table, [nullable_col, ...]) — only nullable columns are interesting for gap analysis
SILVER_AUDIT_TARGETS: list[tuple[str, list[str]]] = [
    ("persons", [
        "gender",
        "birth_date",
        "death_date",
        "image_large",
        "image_medium",
        "hometown",
        "nationality",
        "description",
        "blood_type",
        "website_url",
        "aliases",
        "primary_occupations",
        "years_active",
    ]),
    ("anime", [
        "country_of_origin",
        "year",
        "episodes",
        "format",
        "season",
        "synonyms",
        "trailer_url",
        "external_links_json",
        "airing_schedule_json",
        "description",
        "start_date",
        "end_date",
        "status",
        "source_mat",
        "work_type",
        "scale_class",
        "is_adult",
    ]),
    ("characters", [
        "gender",
        "description",
        "date_of_birth",
        "image_large",
        "image_medium",
        "blood_type",
        "age",
    ]),
    ("studios", [
        "country_of_origin",
        "is_animation_studio",
        "site_url",
        "anilist_id",
        "favourites",
    ]),
]


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class ColumnCoverageRow:
    """NULL/empty-rate measurement for one (silver_table, silver_col) pair."""

    silver_table: str
    silver_col: str
    total: int
    non_null: int
    non_empty: int        # non_null AND non-empty-string (for VARCHAR cols)
    error: Optional[str] = field(default=None)

    @property
    def null_rate(self) -> float:
        """Fraction of rows where the column is NULL."""
        if self.total == 0:
            return 0.0
        return 1.0 - self.non_null / self.total

    @property
    def empty_rate(self) -> float:
        """Fraction of rows that are NULL or empty string."""
        if self.total == 0:
            return 0.0
        return 1.0 - self.non_empty / self.total


@dataclass
class BronzeSourceStat:
    """Count of BRONZE rows that have a non-null, non-empty value for one column."""

    source: str
    bronze_table: str
    bronze_col: str
    rows_with_value: int
    error: Optional[str] = field(default=None)


@dataclass
class GapRow:
    """Gap analysis result for one (silver_table, silver_col) combination.

    A "gap" exists when BRONZE has data but SILVER does not carry it.
    """

    silver_table: str
    silver_col: str
    total_silver: int
    non_null_silver: int
    null_rate: float
    bronze_sources: list[BronzeSourceStat]
    severity: str           # CRITICAL / HIGH / MEDIUM / LOW / OK
    mapped: bool            # False = no BRONZE mapping declared


# ---------------------------------------------------------------------------
# Measurement helpers
# ---------------------------------------------------------------------------


def _silver_column_exists(conn: duckdb.DuckDBPyConnection, table: str, col: str) -> bool:
    """Return True if col exists in table (SILVER)."""
    try:
        rows = conn.execute(f"DESCRIBE {table}").fetchall()
        return any(r[0] == col for r in rows)
    except Exception:
        return False


def measure_column_coverage(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    col: str,
) -> ColumnCoverageRow:
    """Measure NULL / empty rate for one SILVER column.

    Args:
        conn:  Read-only DuckDB connection to SILVER.
        table: SILVER table name.
        col:   Column name within table.

    Returns:
        ColumnCoverageRow with total, non_null, non_empty counts.
    """
    if not _silver_column_exists(conn, table, col):
        return ColumnCoverageRow(
            silver_table=table,
            silver_col=col,
            total=0,
            non_null=0,
            non_empty=0,
            error=f"column does not exist in {table}",
        )
    try:
        # Use a single pass for efficiency
        result = conn.execute(f"""
            SELECT
                COUNT(*)                                                   AS total,
                COUNT({col})                                               AS non_null,
                COUNT(CASE WHEN {col} IS NOT NULL
                                AND CAST({col} AS VARCHAR) <> ''
                           THEN 1 END)                                     AS non_empty
            FROM {table}
        """).fetchone()
        total, non_null, non_empty = result
        return ColumnCoverageRow(
            silver_table=table,
            silver_col=col,
            total=total,
            non_null=non_null,
            non_empty=non_empty,
        )
    except Exception as exc:
        return ColumnCoverageRow(
            silver_table=table,
            silver_col=col,
            total=0,
            non_null=0,
            non_empty=0,
            error=str(exc),
        )


def find_bronze_source_with_value(
    bronze_conn: duckdb.DuckDBPyConnection,
    bronze_root: Path,
    source: str,
    table: str,
    col: str,
) -> BronzeSourceStat:
    """Count distinct primary-key rows in BRONZE where col has a non-null, non-empty value.

    Uses the latest date partition (most-recent snapshot) to avoid double-counting.

    Args:
        bronze_conn: In-memory DuckDB connection (for parquet scanning).
        bronze_root: Root of hive-partitioned BRONZE parquet.
        source:      BRONZE source name (e.g. "anilist").
        table:       BRONZE table name (e.g. "persons").
        col:         Column name to check.

    Returns:
        BronzeSourceStat with rows_with_value count.
    """
    glob = str(
        bronze_root / f"source={source}" / f"table={table}" / "date=*" / "*.parquet"
    )
    try:
        # Determine primary key for dedup (id is standard; fallback to COUNT(*))
        schema_rows = bronze_conn.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true) LIMIT 0",
            [glob],
        ).fetchall()
        schema_cols = {r[0] for r in schema_rows}

        if col not in schema_cols:
            return BronzeSourceStat(
                source=source,
                bronze_table=table,
                bronze_col=col,
                rows_with_value=0,
                error=f"column '{col}' not in BRONZE {source}/{table}",
            )

        # Use id for dedup if available, else count all
        id_col = "id" if "id" in schema_cols else None
        if id_col:
            # Dedup to most-recent date, then count rows with value
            count = bronze_conn.execute(f"""
                SELECT COUNT(DISTINCT CASE
                    WHEN {col} IS NOT NULL AND CAST({col} AS VARCHAR) <> ''
                    THEN {id_col}
                    END)
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY {id_col} ORDER BY date DESC) AS _rn
                    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
                    WHERE {id_col} IS NOT NULL
                )
                WHERE _rn = 1
            """, [glob]).fetchone()[0]
        else:
            count = bronze_conn.execute(f"""
                SELECT COUNT(*)
                FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
                WHERE {col} IS NOT NULL AND CAST({col} AS VARCHAR) <> ''
            """, [glob]).fetchone()[0]

        return BronzeSourceStat(
            source=source,
            bronze_table=table,
            bronze_col=col,
            rows_with_value=count,
        )
    except Exception as exc:
        return BronzeSourceStat(
            source=source,
            bronze_table=table,
            bronze_col=col,
            rows_with_value=0,
            error=str(exc),
        )


def _classify_severity(null_rate: float, total_bronze_with_value: int) -> str:
    """Classify gap severity.

    Severity:
    - CRITICAL: null_rate > 0.80 AND BRONZE has > 10,000 rows with value
    - HIGH:     null_rate > 0.50 AND BRONZE has > 1,000 rows with value
    - MEDIUM:   null_rate > 0.30
    - LOW:      null_rate > 0.10
    - OK:       otherwise
    """
    if null_rate > 0.80 and total_bronze_with_value > 10_000:
        return "CRITICAL"
    if null_rate > 0.50 and total_bronze_with_value > 1_000:
        return "HIGH"
    if null_rate > 0.30:
        return "MEDIUM"
    if null_rate > 0.10:
        return "LOW"
    return "OK"


# ---------------------------------------------------------------------------
# Main gap analysis
# ---------------------------------------------------------------------------


def gap_analysis(
    silver_conn: duckdb.DuckDBPyConnection,
    bronze_conn: duckdb.DuckDBPyConnection,
    bronze_root: Path,
) -> list[GapRow]:
    """Run column coverage audit for all SILVER_AUDIT_TARGETS.

    For each (silver_table, silver_col):
    1. Measure SILVER NULL rate.
    2. For each declared BRONZE source, count rows with a value.
    3. Classify severity.

    Args:
        silver_conn: Read-only SILVER DuckDB connection.
        bronze_conn: In-memory DuckDB connection for parquet scanning.
        bronze_root: BRONZE hive-partition root.

    Returns:
        List of GapRow, one per (silver_table, silver_col) combination.
    """
    rows: list[GapRow] = []

    for silver_table, cols in SILVER_AUDIT_TARGETS:
        for col in cols:
            cov = measure_column_coverage(silver_conn, silver_table, col)
            if cov.error:
                logger.warning(
                    "silver_column_coverage.gap_analysis: measurement error",
                    table=silver_table,
                    col=col,
                    error=cov.error,
                )

            null_rate = cov.null_rate

            # Lookup BRONZE mapping
            bronze_mapping = COLUMN_BRONZE_MAP.get((silver_table, col))
            mapped = bronze_mapping is not None and len(bronze_mapping) > 0

            bronze_sources: list[BronzeSourceStat] = []
            total_bronze_with_value = 0

            if mapped and bronze_root.exists():
                for source, (bronze_table, bronze_col) in bronze_mapping.items():
                    stat = find_bronze_source_with_value(
                        bronze_conn, bronze_root, source, bronze_table, bronze_col
                    )
                    bronze_sources.append(stat)
                    if not stat.error:
                        total_bronze_with_value += stat.rows_with_value

            severity = _classify_severity(null_rate, total_bronze_with_value)

            rows.append(GapRow(
                silver_table=silver_table,
                silver_col=col,
                total_silver=cov.total,
                non_null_silver=cov.non_null,
                null_rate=null_rate,
                bronze_sources=bronze_sources,
                severity=severity,
                mapped=mapped,
            ))

            logger.debug(
                "silver_column_coverage.gap_analysis",
                table=silver_table,
                col=col,
                null_rate=f"{null_rate * 100:.1f}%",
                severity=severity,
                bronze_with_value=total_bronze_with_value,
            )

    return rows


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


def _severity_sort_key(severity: str) -> int:
    """Sort order for severity labels (lower = more severe)."""
    return {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3, "OK": 4}.get(severity, 5)


def _fmt_bronze_sources(sources: list[BronzeSourceStat]) -> str:
    """Format BRONZE source stats as 'source:count' comma-separated string."""
    if not sources:
        return "—"
    parts = []
    for s in sources:
        if s.error:
            parts.append(f"{s.source}:err")
        else:
            parts.append(f"{s.source}:{s.rows_with_value:,}")
    return ", ".join(parts)


def generate_report(
    gap_rows: list[GapRow],
    out_path: Path,
) -> None:
    """Write a Markdown column coverage audit report.

    Sections:
    1. Summary counts by severity
    2. Per-table NULL rate overview (top 10 highest-null columns per table)
    3. Critical gaps table
    4. Full gap analysis table (sorted by severity)
    5. Disclaimers

    Args:
        gap_rows: Output of gap_analysis().
        out_path: Destination .md path.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Separate by severity
    critical = [r for r in gap_rows if r.severity == "CRITICAL"]
    high = [r for r in gap_rows if r.severity == "HIGH"]
    medium = [r for r in gap_rows if r.severity == "MEDIUM"]
    low = [r for r in gap_rows if r.severity == "LOW"]
    ok = [r for r in gap_rows if r.severity == "OK"]

    sorted_rows = sorted(gap_rows, key=lambda r: (_severity_sort_key(r.severity), r.null_rate * -1))

    sections: list[str] = []

    # ── Header ───────────────────────────────────────────────────────────────
    sections.append(f"# SILVER Column Coverage Audit\n\nGenerated: {now}\n")

    sections.append(textwrap.dedent(f"""\
        ## Summary

        | Metric | Count |
        |--------|-------|
        | Total columns audited | {len(gap_rows)} |
        | CRITICAL (null>80%, BRONZE>10K) | {len(critical)} |
        | HIGH (null>50%, BRONZE>1K) | {len(high)} |
        | MEDIUM (null>30%) | {len(medium)} |
        | LOW (null>10%) | {len(low)} |
        | OK | {len(ok)} |
    """))

    # ── Critical gaps ────────────────────────────────────────────────────────
    if critical:
        sections.append("## CRITICAL Gaps\n")
        sections.append(
            "| silver_table | silver_col | null_rate | "
            "BRONZE sources (rows with value) | notes |\n"
            "|---|---|---|---|---|\n"
        )
        for r in sorted(critical, key=lambda x: x.null_rate, reverse=True):
            bronze_str = _fmt_bronze_sources(r.bronze_sources)
            note = "unmapped" if not r.mapped else ""
            sections.append(
                f"| {r.silver_table} | {r.silver_col} "
                f"| {r.null_rate * 100:.1f}% "
                f"| {bronze_str} | {note} |"
            )
        sections.append("")

    # ── Per-table NULL rate overview ─────────────────────────────────────────
    sections.append("## Per-Table NULL Rate Overview\n")

    tables = [t for t, _ in SILVER_AUDIT_TARGETS]
    for tbl in tables:
        tbl_rows = [r for r in gap_rows if r.silver_table == tbl]
        if not tbl_rows:
            continue
        # Top 10 by null_rate descending
        top10 = sorted(tbl_rows, key=lambda r: r.null_rate, reverse=True)[:10]
        sections.append(f"### {tbl}\n")
        sections.append(
            "| column | total_silver | non_null | null_rate | severity |\n"
            "|--------|-------------|---------|-----------|----------|\n"
        )
        for r in top10:
            sections.append(
                f"| {r.silver_col} | {r.total_silver:,} | {r.non_null_silver:,} "
                f"| {r.null_rate * 100:.1f}% | {r.severity} |"
            )
        sections.append("")

    # ── Full gap analysis table ──────────────────────────────────────────────
    sections.append("## Full Gap Analysis\n")
    sections.append(
        "| silver_table | silver_col | null_rate | "
        "BRONZE with_value | severity | mapped |\n"
        "|---|---|---|---|---|---|\n"
    )
    for r in sorted_rows:
        bronze_str = _fmt_bronze_sources(r.bronze_sources)
        mapped_str = "yes" if r.mapped else "no"
        sections.append(
            f"| {r.silver_table} | {r.silver_col} "
            f"| {r.null_rate * 100:.1f}% "
            f"| {bronze_str} "
            f"| {r.severity} | {mapped_str} |"
        )
    sections.append("")

    # ── Disclaimer ───────────────────────────────────────────────────────────
    sections.append(textwrap.dedent("""\
        ---

        **Disclaimer (JA)**: 本レポートは公開クレジットデータの列単位取込率を示す構造的監査であり、
        個人の評価や能力判断には一切使用しない。数値はネットワーク位置と取込密度の指標。

        **Disclaimer (EN)**: This report presents structural column-coverage metrics
        derived solely from public credit records. The figures reflect ingestion
        completeness, not any subjective assessment of individuals.
    """))

    report = "\n".join(sections)
    out_path.write_text(report, encoding="utf-8")
    logger.info("silver_column_coverage: report written", path=str(out_path))


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main(
    bronze_root: Path = Path("result/bronze"),
    silver_db: str = "result/silver.duckdb",
    out: Path = Path("result/audit/silver_column_coverage.md"),
) -> list[GapRow]:
    """CLI entry point.

    Args:
        bronze_root: Root of hive-partitioned BRONZE parquet.
        silver_db:   Path to the SILVER DuckDB file.
        out:         Output path for the Markdown report.

    Returns:
        List of GapRow for programmatic use.
    """
    bronze_root = Path(bronze_root)
    silver_db = str(silver_db)

    silver_conn = duckdb.connect(silver_db, read_only=True)
    bronze_conn = duckdb.connect(":memory:")

    try:
        rows = gap_analysis(silver_conn, bronze_conn, bronze_root)
    finally:
        silver_conn.close()
        bronze_conn.close()

    generate_report(rows, out)
    return rows


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SILVER column coverage audit")
    parser.add_argument("--bronze-root", type=Path, default=Path("result/bronze"))
    parser.add_argument("--silver-db", type=str, default="result/silver.duckdb")
    parser.add_argument("--out", type=Path, default=Path("result/audit/silver_column_coverage.md"))
    args = parser.parse_args()
    main(bronze_root=args.bronze_root, silver_db=args.silver_db, out=args.out)
