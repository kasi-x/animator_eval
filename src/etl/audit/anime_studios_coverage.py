"""BRONZE → SILVER anime_studios coverage audit tool.

Measures per-source how many BRONZE anime have studio information and
how many are represented in SILVER anime_studios.

Usage (CLI):
    python -m src.etl.audit.anime_studios_coverage \
        --bronze-root result/bronze \
        --silver-db   result/silver.duckdb \
        --out         result/audit/anime_studios_coverage.md

Public API:
    measure(silver_db, bronze_root) -> list[CoverageRow]
    generate_report(rows, out_path)
"""
from __future__ import annotations

import datetime
import textwrap
from dataclasses import dataclass
from pathlib import Path

import duckdb
import structlog

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class CoverageRow:
    """Per-source anime_studios coverage measurement."""

    source: str
    bronze_anime: int
    bronze_with_studio: int
    silver_in_anime_studios: int
    notes: str = ""

    @property
    def bronze_studio_pct(self) -> float:
        """Fraction of BRONZE anime that have studio information."""
        if self.bronze_anime == 0:
            return 0.0
        return self.bronze_with_studio / self.bronze_anime

    @property
    def capture_rate(self) -> float:
        """Fraction of BRONZE-with-studio anime captured in SILVER anime_studios."""
        if self.bronze_with_studio == 0:
            return 0.0
        return min(self.silver_in_anime_studios / self.bronze_with_studio, 1.0)


# ---------------------------------------------------------------------------
# Source definitions
# ---------------------------------------------------------------------------


def _glob(bronze_root: Path, source: str, table: str) -> str:
    return str(bronze_root / f"source={source}" / f"table={table}" / "date=*" / "*.parquet")


def _has_parquet(con: duckdb.DuckDBPyConnection, glob: str) -> bool:
    try:
        con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}', union_by_name=true) LIMIT 0")
        return True
    except Exception:
        return False


def _count_distinct(
    con: duckdb.DuckDBPyConnection,
    glob: str,
    id_col: str,
    where: str = "",
) -> int:
    where_clause = f"AND ({where})" if where else ""
    try:
        return con.execute(
            f"SELECT COUNT(DISTINCT {id_col}) FROM read_parquet('{glob}', union_by_name=true)"
            f" WHERE {id_col} IS NOT NULL {where_clause}"
        ).fetchone()[0]
    except Exception:
        return 0


def _count_silver(
    silver_con: duckdb.DuckDBPyConnection,
    source: str,
) -> int:
    try:
        return silver_con.execute(
            "SELECT COUNT(DISTINCT anime_id) FROM anime_studios WHERE source = ?",
            [source],
        ).fetchone()[0]
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Per-source measurement functions
# ---------------------------------------------------------------------------


def _measure_anilist(
    con: duckdb.DuckDBPyConnection,
    silver_con: duckdb.DuckDBPyConnection,
    bronze_root: Path,
) -> CoverageRow:
    """AniList: studios info comes from anime.studios[] array AND anime_studios table."""
    anime_glob = _glob(bronze_root, "anilist", "anime")
    as_glob = _glob(bronze_root, "anilist", "anime_studios")

    bronze_anime = _count_distinct(con, anime_glob, "id")

    # anime with studios[] array (non-empty)
    bronze_with_studio_array = 0
    if _has_parquet(con, anime_glob):
        try:
            bronze_with_studio_array = con.execute(
                f"SELECT COUNT(DISTINCT id) FROM read_parquet('{anime_glob}', union_by_name=true)"
                " WHERE id IS NOT NULL AND studios IS NOT NULL AND len(studios) > 0"
            ).fetchone()[0]
        except Exception:
            pass

    # anime_studios BRONZE table
    bronze_with_studio_table = _count_distinct(con, as_glob, "anime_id") if _has_parquet(con, as_glob) else 0

    # Union of both sources
    bronze_with_studio = max(bronze_with_studio_array, bronze_with_studio_table)

    silver = _count_silver(silver_con, "anilist")
    return CoverageRow(
        source="anilist",
        bronze_anime=bronze_anime,
        bronze_with_studio=bronze_with_studio,
        silver_in_anime_studios=silver,
        notes=(
            f"anime.studios[] array: {bronze_with_studio_array}; "
            f"anime_studios table: {bronze_with_studio_table}"
        ),
    )


def _measure_mal(
    con: duckdb.DuckDBPyConnection,
    silver_con: duckdb.DuckDBPyConnection,
    bronze_root: Path,
) -> CoverageRow:
    """MAL: studios in anime_studios BRONZE (kind='studio')."""
    anime_glob = _glob(bronze_root, "mal", "anime")
    as_glob = _glob(bronze_root, "mal", "anime_studios")

    bronze_anime = _count_distinct(con, anime_glob, "mal_id")
    bronze_with_studio = (
        _count_distinct(con, as_glob, "mal_id", "kind = 'studio'")
        if _has_parquet(con, as_glob)
        else 0
    )
    silver = _count_silver(silver_con, "mal")
    return CoverageRow(
        source="mal",
        bronze_anime=bronze_anime,
        bronze_with_studio=bronze_with_studio,
        silver_in_anime_studios=silver,
        notes="kind='studio' filter applied (excl. producer/licensor)",
    )


def _measure_ann(
    con: duckdb.DuckDBPyConnection,
    silver_con: duckdb.DuckDBPyConnection,
    bronze_root: Path,
) -> CoverageRow:
    """ANN: animation studios in company BRONZE (task='Animation Production')."""
    anime_glob = _glob(bronze_root, "ann", "anime")
    company_glob = _glob(bronze_root, "ann", "company")

    bronze_anime = _count_distinct(con, anime_glob, "ann_id")
    bronze_with_studio = (
        _count_distinct(con, company_glob, "ann_anime_id", "task = 'Animation Production'")
        if _has_parquet(con, company_glob)
        else 0
    )
    silver = _count_silver(silver_con, "ann")
    return CoverageRow(
        source="ann",
        bronze_anime=bronze_anime,
        bronze_with_studio=bronze_with_studio,
        silver_in_anime_studios=silver,
        notes="task='Animation Production' from company table",
    )


def _measure_mediaarts(
    con: duckdb.DuckDBPyConnection,
    silver_con: duckdb.DuckDBPyConnection,
    bronze_root: Path,
) -> CoverageRow:
    """Mediaarts: studios in anime.studios[] array (C-prefix) + production_companies."""
    anime_glob = _glob(bronze_root, "mediaarts", "anime")
    pc_glob = _glob(bronze_root, "mediaarts", "production_companies")

    bronze_anime = _count_distinct(con, anime_glob, "madb_id") if _has_parquet(con, anime_glob) else 0

    # C-prefix anime have a studios array
    bronze_with_studio_array = 0
    if _has_parquet(con, anime_glob):
        try:
            bronze_with_studio_array = con.execute(
                f"SELECT COUNT(DISTINCT madb_id) FROM read_parquet('{anime_glob}', union_by_name=true)"
                " WHERE madb_id LIKE 'C%' AND studios IS NOT NULL AND len(studios) > 0"
            ).fetchone()[0]
        except Exception:
            pass

    # production_companies with アニメーション制作 role
    bronze_with_pc = (
        _count_distinct(
            con, pc_glob, "madb_id",
            "role_label = 'アニメーション制作' OR is_main = true"
        )
        if _has_parquet(con, pc_glob)
        else 0
    )

    bronze_with_studio = max(bronze_with_studio_array, bronze_with_pc)
    silver = _count_silver(silver_con, "mediaarts")
    return CoverageRow(
        source="mediaarts",
        bronze_anime=bronze_anime,
        bronze_with_studio=bronze_with_studio,
        silver_in_anime_studios=silver,
        notes=(
            f"anime.studios[] (C-prefix): {bronze_with_studio_array}; "
            f"production_companies: {bronze_with_pc}"
        ),
    )


def _measure_seesaawiki(
    con: duckdb.DuckDBPyConnection,
    silver_con: duckdb.DuckDBPyConnection,
    bronze_root: Path,
) -> CoverageRow:
    """SeesaaWiki: anime_studios BRONZE table."""
    anime_glob = _glob(bronze_root, "seesaawiki", "anime")
    as_glob = _glob(bronze_root, "seesaawiki", "anime_studios")

    bronze_anime = _count_distinct(con, anime_glob, "id")
    bronze_with_studio = (
        _count_distinct(con, as_glob, "anime_id") if _has_parquet(con, as_glob) else 0
    )
    silver = _count_silver(silver_con, "seesaawiki")
    return CoverageRow(
        source="seesaawiki",
        bronze_anime=bronze_anime,
        bronze_with_studio=bronze_with_studio,
        silver_in_anime_studios=silver,
    )


def _measure_keyframe(
    con: duckdb.DuckDBPyConnection,
    silver_con: duckdb.DuckDBPyConnection,
    bronze_root: Path,
) -> CoverageRow:
    """Keyframe: anime_studios BRONZE table."""
    anime_glob = _glob(bronze_root, "keyframe", "anime")
    as_glob = _glob(bronze_root, "keyframe", "anime_studios")

    bronze_anime = _count_distinct(con, anime_glob, "id") if _has_parquet(con, anime_glob) else 0
    bronze_with_studio = (
        _count_distinct(con, as_glob, "anime_id") if _has_parquet(con, as_glob) else 0
    )
    silver = _count_silver(silver_con, "keyframe")
    return CoverageRow(
        source="keyframe",
        bronze_anime=bronze_anime,
        bronze_with_studio=bronze_with_studio,
        silver_in_anime_studios=silver,
    )


def _measure_bangumi(
    con: duckdb.DuckDBPyConnection,
    silver_con: duckdb.DuckDBPyConnection,
    bronze_root: Path,
) -> CoverageRow:
    """Bangumi: no dedicated studio table in BRONZE — N/A."""
    subjects_glob = _glob(bronze_root, "bangumi", "subjects")
    bronze_anime = (
        _count_distinct(con, subjects_glob, "id", "type = 2")
        if _has_parquet(con, subjects_glob)
        else 0
    )
    silver = _count_silver(silver_con, "bangumi")
    return CoverageRow(
        source="bangumi",
        bronze_anime=bronze_anime,
        bronze_with_studio=0,
        silver_in_anime_studios=silver,
        notes="No studio table in BRONZE; studio info not available",
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def measure(
    silver_db: Path | str,
    bronze_root: Path | str,
) -> list[CoverageRow]:
    """Return per-source coverage rows.

    Args:
        silver_db: Path to the SILVER DuckDB file.
        bronze_root: Root directory of BRONZE parquet partitions.

    Returns:
        List of CoverageRow, one per source.
    """
    silver_db = Path(silver_db)
    bronze_root = Path(bronze_root)

    con = duckdb.connect(":memory:")
    silver_con = duckdb.connect(str(silver_db), read_only=True)

    try:
        rows = [
            _measure_anilist(con, silver_con, bronze_root),
            _measure_mal(con, silver_con, bronze_root),
            _measure_ann(con, silver_con, bronze_root),
            _measure_mediaarts(con, silver_con, bronze_root),
            _measure_seesaawiki(con, silver_con, bronze_root),
            _measure_keyframe(con, silver_con, bronze_root),
            _measure_bangumi(con, silver_con, bronze_root),
        ]
    finally:
        con.close()
        silver_con.close()

    return rows


def generate_report(
    rows: list[CoverageRow],
    out_path: Path | str,
    silver_total_anime: int | None = None,
) -> None:
    """Write Markdown coverage report.

    Args:
        rows: Output of measure().
        out_path: Path to write the .md file.
        silver_total_anime: Total anime rows in SILVER (for overall coverage pct).
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    total_silver_as = sum(r.silver_in_anime_studios for r in rows)
    overall_note = ""
    if silver_total_anime:
        pct = total_silver_as / silver_total_anime * 100
        overall_note = f"\n**Overall**: {total_silver_as:,} / {silver_total_anime:,} anime with studio linkage ({pct:.1f}%).\n"

    table_rows = []
    for r in rows:
        studio_pct = f"{r.bronze_studio_pct * 100:.0f}%" if r.bronze_anime else "—"
        capture = f"{r.capture_rate * 100:.0f}%" if r.bronze_with_studio else "—"
        table_rows.append(
            f"| {r.source} | {r.bronze_anime:,} | {r.bronze_with_studio:,} ({studio_pct})"
            f" | {r.silver_in_anime_studios:,} | {capture} | {r.notes} |"
        )

    report = textwrap.dedent(f"""\
        # anime_studios Coverage Audit

        Generated: {datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M UTC")}
        {overall_note}
        ## Per-Source Coverage

        | Source | BRONZE anime | BRONZE w/ studio | SILVER anime_studios | Capture rate | Notes |
        |--------|-------------|-----------------|---------------------|-------------|-------|
        {chr(10).join(table_rows)}

        ## Definitions

        - **BRONZE anime**: distinct anime rows in most-recent BRONZE parquet snapshot.
        - **BRONZE w/ studio**: BRONZE anime that have at least one studio entry.
        - **SILVER anime_studios**: distinct `anime_id` in SILVER `anime_studios` for this source.
        - **Capture rate**: `SILVER anime_studios / BRONZE w/ studio`. Values > 100% indicate
          SILVER has anime from multiple scrape dates that are not all in the latest BRONZE.

        ## Disclaimers

        本レポートはネットワーク構造指標の計算品質を測定するものであり、個人の能力評価を
        目的としていません。スタジオ帰属は公開クレジット情報から取得した構造的事実です。

        This report measures structural data coverage for network analysis. Studio attribution
        is based solely on publicly available credit records and does not constitute a judgment
        of any individual's performance.
    """)

    out_path.write_text(report, encoding="utf-8")
    logger.info("anime_studios_coverage_report_written", path=str(out_path))


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Measure anime_studios BRONZE→SILVER coverage")
    parser.add_argument("--bronze-root", default="result/bronze", help="BRONZE parquet root")
    parser.add_argument("--silver-db", default="result/silver.duckdb", help="SILVER DuckDB path")
    parser.add_argument("--out", default="result/audit/anime_studios_coverage.md", help="Output path")
    args = parser.parse_args()

    silver_con = duckdb.connect(args.silver_db, read_only=True)
    total_anime = silver_con.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
    silver_con.close()

    rows = measure(args.silver_db, args.bronze_root)
    generate_report(rows, args.out, silver_total_anime=total_anime)

    print(f"Report written to {args.out}")
    for r in rows:
        print(
            f"  {r.source:15s}  bronze_w_studio={r.bronze_with_studio:6d}  "
            f"silver={r.silver_in_anime_studios:6d}  capture={r.capture_rate:.0%}"
        )
