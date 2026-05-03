"""Post-hoc SILVER lineage collector.

Walks all 28 SILVER tables, resolves each table's BRONZE source(s) from the
static mapping below, counts partition rows from the BRONZE parquet tree, and
writes one ``meta_lineage`` row per (silver_table, bronze_source, bronze_table)
into silver.duckdb.

Entry point::

    pixi run python -m src.etl.lineage.collect

Does NOT touch ``src/etl/conformed_loaders/*`` — purely post-hoc aggregation.
"""

from __future__ import annotations

import datetime as _dt
import os
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import structlog

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DEFAULT_SILVER_PATH: Path = Path(
    os.environ.get("ANIMETOR_SILVER_PATH", "result/silver.duckdb")
)
DEFAULT_BRONZE_ROOT: Path = Path(
    os.environ.get("ANIMETOR_BRONZE_ROOT", "result/bronze")
)

# ---------------------------------------------------------------------------
# DDL for meta_lineage inside silver.duckdb
# ---------------------------------------------------------------------------

_META_LINEAGE_DDL = """
CREATE TABLE IF NOT EXISTS meta_lineage (
    silver_table    VARCHAR NOT NULL,
    bronze_source   VARCHAR NOT NULL,
    bronze_table    VARCHAR NOT NULL,
    partition_date  VARCHAR,
    silver_row_count  BIGINT DEFAULT 0,
    bronze_row_count  BIGINT DEFAULT 0,
    collected_at    TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (silver_table, bronze_source, bronze_table)
);
"""

# ---------------------------------------------------------------------------
# Static SILVER → BRONZE mapping
# Authoritative list of which BRONZE sources feed each SILVER table.
# ---------------------------------------------------------------------------

# Each entry: silver_table -> list of (bronze_source, bronze_table) tuples.
# If bronze_table is the same across sources, use None to indicate "same name".
_SILVER_TO_BRONZE: dict[str, list[tuple[str, str]]] = {
    "anime": [
        ("anilist", "anime"),
        ("ann", "anime"),
        ("mal", "anime"),
        ("bangumi", "subjects"),
        ("mediaarts", "anime"),
        ("keyframe", "anime"),
        ("seesaawiki", "anime"),
    ],
    "persons": [
        ("anilist", "persons"),
        ("ann", "persons"),
        ("mal", "staff_credits"),
        ("bangumi", "persons"),
        ("keyframe", "persons"),
        ("seesaawiki", "persons"),
    ],
    "credits": [
        ("anilist", "credits"),
        ("ann", "credits"),
        ("mal", "staff_credits"),
        ("bangumi", "subject_persons"),
        ("seesaawiki", "credits"),
        ("keyframe", "credits"),
        ("mediaarts", "credits"),
        ("sakuga_atwiki", "credits"),
    ],
    "characters": [
        ("anilist", "characters"),
        ("ann", "cast"),
        ("mal", "anime_characters"),
        ("bangumi", "characters"),
    ],
    "character_voice_actors": [
        ("anilist", "character_voice_actors"),
        ("ann", "cast"),
        ("mal", "anime_characters"),
        ("bangumi", "person_characters"),
    ],
    "studios": [
        ("anilist", "studios"),
        ("ann", "company"),
        ("mal", "anime_studios"),
        ("mediaarts", "anime"),
        ("seesaawiki", "anime"),
        ("keyframe", "studios_master"),
    ],
    "anime_studios": [
        ("anilist", "anime_studios"),
        ("ann", "company"),
        ("mal", "anime_studios"),
        ("mediaarts", "anime"),
        ("seesaawiki", "anime"),
        ("keyframe", "anime_studios"),
    ],
    "anime_genres": [
        ("anilist", "anime"),
        ("mal", "anime_genres"),
    ],
    "anime_episodes": [
        ("ann", "episodes"),
    ],
    "anime_companies": [
        ("ann", "company"),
    ],
    "anime_releases": [
        ("ann", "releases"),
    ],
    "anime_news": [
        ("ann", "news"),
    ],
    "anime_relations": [
        ("anilist", "relations"),
        ("mal", "anime_relations"),
    ],
    "anime_recommendations": [
        ("mal", "anime_recommendations"),
    ],
    "anime_broadcasters": [
        ("mediaarts", "broadcasters"),
    ],
    "anime_broadcast_schedule": [
        ("mediaarts", "broadcast_schedule"),
    ],
    "anime_video_releases": [
        ("mediaarts", "video_releases"),
    ],
    "anime_production_companies": [
        ("mediaarts", "production_companies"),
    ],
    "anime_production_committee": [
        ("mediaarts", "production_committee"),
        ("seesaawiki", "production_committee"),
    ],
    "anime_original_work_links": [
        ("mediaarts", "original_work_links"),
    ],
    "anime_theme_songs": [
        ("seesaawiki", "theme_songs"),
    ],
    "anime_episode_titles": [
        ("seesaawiki", "episode_titles"),
    ],
    "anime_gross_studios": [
        ("seesaawiki", "gross_studios"),
    ],
    "anime_original_work_info": [
        ("seesaawiki", "original_work_info"),
    ],
    "person_jobs": [
        ("keyframe", "person_jobs"),
    ],
    "person_studio_affiliations": [
        ("keyframe", "person_studios"),
    ],
    "anime_settings_categories": [
        ("keyframe", "settings_categories"),
    ],
    "sakuga_work_title_resolution": [
        ("sakuga_atwiki", "pages"),
    ],
}

# ---------------------------------------------------------------------------
# Dataclass for a lineage row
# ---------------------------------------------------------------------------


@dataclass
class LineageRow:
    """One row in the meta_lineage table."""

    silver_table: str
    bronze_source: str
    bronze_table: str
    partition_date: str | None
    silver_row_count: int
    bronze_row_count: int
    collected_at: _dt.datetime = field(default_factory=_dt.datetime.utcnow)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _latest_partition_date(bronze_root: Path, source: str, table: str) -> str | None:
    """Return the latest date= partition string for a bronze source/table."""
    table_dir = bronze_root / f"source={source}" / f"table={table}"
    if not table_dir.exists():
        return None
    dates = sorted(d.name.split("=", 1)[1] for d in table_dir.glob("date=*") if d.is_dir())
    return dates[-1] if dates else None


def _count_bronze_rows(
    conn: duckdb.DuckDBPyConnection,
    bronze_root: Path,
    source: str,
    table: str,
) -> int:
    """Count total rows across ALL date partitions for a bronze source/table.

    Uses DuckDB's parquet scanning with glob. Returns 0 if no parquet found.
    """
    glob = str(bronze_root / f"source={source}" / f"table={table}" / "date=*" / "*.parquet")
    try:
        result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{glob}')").fetchone()
        return int(result[0]) if result else 0
    except duckdb.IOException:
        return 0
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "bronze_count_error",
            source=source,
            table=table,
            error=str(exc),
        )
        return 0


def _count_silver_rows(
    conn: duckdb.DuckDBPyConnection,
    silver_table: str,
) -> int:
    """Count rows in a SILVER table. Returns 0 if table does not exist."""
    try:
        result = conn.execute(f"SELECT COUNT(*) FROM {silver_table}").fetchone()
        return int(result[0]) if result else 0
    except Exception as exc:  # noqa: BLE001
        log.warning("silver_count_error", table=silver_table, error=str(exc))
        return 0


# ---------------------------------------------------------------------------
# Core collector
# ---------------------------------------------------------------------------


def collect(
    conn: duckdb.DuckDBPyConnection,
    bronze_root: Path | str | None = None,
    *,
    mapping: dict[str, list[tuple[str, str]]] | None = None,
) -> int:
    """Collect post-hoc lineage for all 28 SILVER tables.

    For each (silver_table, bronze_source, bronze_table) triple:
    - counts rows in the SILVER table
    - counts rows in the BRONZE parquet (all date partitions)
    - writes / upserts into meta_lineage

    Args:
        conn: Open DuckDB connection to silver.duckdb (read-write).
        bronze_root: Root directory of BRONZE parquet tree. Defaults to
            ``DEFAULT_BRONZE_ROOT``.
        mapping: Override the default ``_SILVER_TO_BRONZE`` mapping.
            Useful for testing with a subset.

    Returns:
        Number of meta_lineage rows written (inserted or replaced).
    """
    bronze_root = Path(bronze_root or DEFAULT_BRONZE_ROOT)
    mapping = mapping if mapping is not None else _SILVER_TO_BRONZE

    conn.execute(_META_LINEAGE_DDL)

    rows_written = 0
    collected_at = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)

    for silver_table, sources in mapping.items():
        silver_count = _count_silver_rows(conn, silver_table)

        for bronze_source, bronze_table in sources:
            bronze_count = _count_bronze_rows(conn, bronze_root, bronze_source, bronze_table)
            latest_date = _latest_partition_date(bronze_root, bronze_source, bronze_table)

            log.debug(
                "lineage_row",
                silver_table=silver_table,
                bronze_source=bronze_source,
                bronze_table=bronze_table,
                silver_rows=silver_count,
                bronze_rows=bronze_count,
                latest_date=latest_date,
            )

            conn.execute(
                """
                INSERT INTO meta_lineage
                    (silver_table, bronze_source, bronze_table,
                     partition_date, silver_row_count, bronze_row_count, collected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (silver_table, bronze_source, bronze_table)
                DO UPDATE SET
                    partition_date   = excluded.partition_date,
                    silver_row_count = excluded.silver_row_count,
                    bronze_row_count = excluded.bronze_row_count,
                    collected_at     = excluded.collected_at
                """,
                [
                    silver_table,
                    bronze_source,
                    bronze_table,
                    latest_date,
                    silver_count,
                    bronze_count,
                    collected_at,
                ],
            )
            rows_written += 1

    log.info("lineage_collect_done", rows_written=rows_written)
    return rows_written


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _parse_args() -> tuple[Path, Path]:
    import argparse

    parser = argparse.ArgumentParser(
        description="Collect post-hoc SILVER lineage into meta_lineage table."
    )
    parser.add_argument(
        "--silver",
        default=str(DEFAULT_SILVER_PATH),
        help=f"Path to silver.duckdb (default: {DEFAULT_SILVER_PATH})",
    )
    parser.add_argument(
        "--bronze-root",
        default=str(DEFAULT_BRONZE_ROOT),
        help=f"Root of BRONZE parquet tree (default: {DEFAULT_BRONZE_ROOT})",
    )
    args = parser.parse_args()
    return Path(args.silver), Path(args.bronze_root)


def main() -> None:
    """CLI entry: collect lineage and print summary."""
    silver_path, bronze_root = _parse_args()

    log.info("lineage_collect_start", silver=str(silver_path), bronze_root=str(bronze_root))

    conn = duckdb.connect(str(silver_path))
    try:
        n = collect(conn, bronze_root)
        print(f"meta_lineage: {n} rows written to {silver_path}")
        summary = conn.execute(
            "SELECT silver_table, bronze_source, silver_row_count, bronze_row_count "
            "FROM meta_lineage ORDER BY silver_table, bronze_source"
        ).fetchdf()
        print(summary.to_string(index=False))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
