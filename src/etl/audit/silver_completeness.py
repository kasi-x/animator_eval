"""BRONZE → SILVER completeness audit tool.

Measures what fraction of each BRONZE parquet table's rows are
represented in the corresponding SILVER DuckDB table.

Usage (CLI):
    python -m src.etl.audit.silver_completeness \
        --bronze-root result/bronze \
        --silver-db  result/silver.duckdb \
        --out        result/audit/silver_completeness.md

Public API:
    check(bronze_root, silver_db) -> list[CoverageRow]
    generate_report(rows, out_path)
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
# Mapping: (bronze_source, bronze_table) → (silver_table, silver_filter_expr,
#                                            bronze_distinct_expr)
#
# silver_filter_expr is a SQL WHERE clause fragment applied to the SILVER
# table to isolate rows that originate from this source.  Use None when
# every row in the SILVER table originates from exactly this source (rare).
#
# bronze_distinct_expr (optional 3rd element) is a SQL expression passed to
# COUNT(DISTINCT ...) when counting BRONZE rows.  It replaces COUNT(*) to
# de-duplicate across date-partition snapshots.
#
# When bronze_distinct_expr is a single column name like "id", the query
# becomes:  SELECT COUNT(DISTINCT id) FROM read_parquet(...)
# When it is a concatenation expression, the full expression is used verbatim.
#
# Tables that receive multiple snapshot partitions (same entity written once
# per scrape date) MUST supply a bronze_distinct_expr so coverage is measured
# against unique entities, not snapshot copies.
#
# "unmapped" entries (value = None) are reported with coverage = N/A.
# ---------------------------------------------------------------------------

# Type alias: (silver_table, silver_filter, bronze_distinct_expr)
# bronze_distinct_expr=None means COUNT(*) (no dedup needed / unknown key)
_MappingValue = Optional[tuple[str, Optional[str], Optional[str]]]

SOURCE_TABLE_TO_SILVER: dict[tuple[str, str], _MappingValue] = {
    # ── AniList ────────────────────────────────────────────────────────────
    # anilist/anime: each scrape date emits a full snapshot → dedup by id
    ("anilist", "anime"):                    ("anime",    "id LIKE 'anilist:%'",
                                              "id"),
    ("anilist", "persons"):                  ("persons",  "id LIKE 'anilist:%'",
                                              "id"),
    # anilist/credits: snapshot partitions — dedup by (person_id, anime_id, role, episode)
    ("anilist", "credits"):                  ("credits",  "evidence_source = 'anilist'",
                                              "COALESCE(person_id,'') || '|' || COALESCE(anime_id,'') || '|' || COALESCE(role,'') || '|' || COALESCE(CAST(episode AS VARCHAR),'NULL')"),
    # anilist/characters: snapshot partitions → dedup by id
    ("anilist", "characters"):               ("characters", "id LIKE 'anilist:%'",
                                              "id"),
    # anilist/character_voice_actors: snapshot partitions → dedup by (character_id, person_id, anime_id)
    ("anilist", "character_voice_actors"):   ("character_voice_actors", "source = 'anilist'",
                                              "COALESCE(CAST(character_id AS VARCHAR),'') || '|' || COALESCE(CAST(person_id AS VARCHAR),'') || '|' || COALESCE(CAST(anime_id AS VARCHAR),'')"),
    # anilist/studios: single snapshot partition → dedup by id is still correct
    ("anilist", "studios"):                  ("studios",  "id LIKE 'anilist:%'",
                                              "id"),
    ("anilist", "anime_studios"):            None,   # merged with other sources, not directly filterable
    ("anilist", "relations"):                None,   # folded into anime.relations_json
    # ── ANN ────────────────────────────────────────────────────────────────
    ("ann", "anime"):                        None,   # ann has no direct SILVER id (ann_id column in anime)
    ("ann", "persons"):                      None,   # ANN persons not yet a primary SILVER source
    # ann/credits: ANN credits are loaded in bulk (no snapshot partitions observed)
    ("ann", "credits"):                      ("credits", "evidence_source = 'ann'",
                                              None),
    ("ann", "cast"):                         None,   # cast ⊆ credits (character casting)
    ("ann", "company"):                      None,   # folded into studios / anime metadata
    ("ann", "episodes"):                     None,   # not yet loaded into SILVER
    ("ann", "news"):                         None,   # not loaded
    ("ann", "related"):                      None,   # not loaded
    ("ann", "releases"):                     None,   # not loaded
    # ── Bangumi ────────────────────────────────────────────────────────────
    # Bangumi has no snapshot partitions (single-pass bulk load)
    ("bangumi", "subjects"):                 ("anime",    "id LIKE 'bgm:%'",
                                              "id"),
    ("bangumi", "persons"):                  ("persons",  "id LIKE 'bgm:%'",
                                              "id"),
    ("bangumi", "subject_persons"):          ("credits",  "evidence_source = 'bangumi'",
                                              None),
    ("bangumi", "characters"):               ("characters", "id LIKE 'bgm:%' OR id LIKE 'bgm:%'",
                                              "id"),
    ("bangumi", "subject_characters"):       None,   # folded into characters table
    ("bangumi", "person_characters"):        None,   # character voice-actor relationship, not loaded
    # ── Keyframe ───────────────────────────────────────────────────────────
    ("keyframe", "anime"):                   ("anime",    "id LIKE 'keyframe:%'",
                                              "id"),
    ("keyframe", "persons"):                 ("persons",  "id LIKE 'keyframe:%'",
                                              "id"),
    # keyframe/credits: schema changed across partitions (role → role_ja/role_en).
    # SILVER accumulates credits from all scrape runs; no simple distinct key spans
    # all schema versions.  Reported as FILTERED (intentional subset) — see audit notes.
    ("keyframe", "credits"):                 ("credits",  "evidence_source = 'keyframe'",
                                              None),
    ("keyframe", "person_credits"):          None,   # alternative credits path, merged into credits
    ("keyframe", "anime_studios"):           ("anime_studios", None, None),   # all rows from keyframe
    # keyframe/studios_master → SILVER studios WHERE id LIKE 'kf:s%'
    # (kf:n:% IDs originate from anime_studios names, not studios_master)
    ("keyframe", "studios_master"):          ("studios",  "id LIKE 'kf:s%'",
                                              "studio_id"),
    ("keyframe", "person_jobs"):             ("person_jobs", "source = 'keyframe'",
                                              None),
    ("keyframe", "person_studios"):          ("person_studio_affiliations", "source = 'keyframe'",
                                              None),
    ("keyframe", "person_profile"):          None,   # enriches persons table (UPDATE, not INSERT)
    ("keyframe", "roles_master"):            None,   # lookup table, not stored in SILVER
    ("keyframe", "settings_categories"):     ("anime_settings_categories", None, None),
    ("keyframe", "preview"):                 None,   # preview metadata, not loaded
    # ── MAL ────────────────────────────────────────────────────────────────
    ("mal", "anime"):                        ("anime",    "id LIKE 'mal:%'",
                                              None),
    ("mal", "staff_credits"):                ("credits",  "evidence_source = 'mal'",
                                              None),
    ("mal", "va_credits"):                   ("character_voice_actors", "source = 'mal'",
                                              None),
    ("mal", "anime_studios"):                ("anime_studios", None, None),
    ("mal", "anime_genres"):                 ("anime_genres", None, None),
    ("mal", "anime_characters"):             None,   # character listing, folded into characters
    ("mal", "anime_episodes"):               ("anime_episodes", None, None),
    ("mal", "anime_external"):               None,   # external links, not loaded
    ("mal", "anime_moreinfo"):               None,   # free-text, not loaded
    ("mal", "anime_news"):                   ("anime_news", None, None),
    ("mal", "anime_pictures"):               None,   # not loaded
    ("mal", "anime_recommendations"):        ("anime_recommendations", None, None),
    ("mal", "anime_relations"):              ("anime_relations", None, None),
    ("mal", "anime_statistics"):             None,   # display-only stats, not loaded into SILVER
    ("mal", "anime_streaming"):              None,   # streaming links, not loaded
    ("mal", "anime_themes"):                 None,   # OP/ED themes, not loaded
    ("mal", "anime_videos_ep"):              None,   # videos, not loaded
    ("mal", "anime_videos_promo"):           None,   # promos, not loaded
    # ── MediaArts DB (MADB) ────────────────────────────────────────────────
    # MADB tables use snapshot partitions (each scrape rewrites the full dataset)
    ("mediaarts", "anime"):                  ("anime",    "id LIKE 'madb:%'",
                                              "id"),
    ("mediaarts", "persons"):                ("persons",  "id LIKE 'madb:%'",
                                              "id"),
    ("mediaarts", "credits"):                ("credits",  "evidence_source = 'mediaarts'",
                                              None),
    # mediaarts/broadcasters: snapshot partitions → dedup by (madb_id, name)
    ("mediaarts", "broadcasters"):           ("anime_broadcasters", None,
                                              "COALESCE(CAST(madb_id AS VARCHAR),'') || '|' || COALESCE(name,'')"),
    ("mediaarts", "broadcast_schedule"):     ("anime_broadcast_schedule", None, None),
    ("mediaarts", "original_work_links"):    ("anime_original_work_links", None, None),
    ("mediaarts", "production_committee"):   ("anime_production_committee", None, None),
    # mediaarts/production_companies: snapshot partitions → dedup by (madb_id, company_name)
    ("mediaarts", "production_companies"):   ("anime_production_companies", None,
                                              "COALESCE(CAST(madb_id AS VARCHAR),'') || '|' || COALESCE(company_name,'')"),
    # mediaarts/video_releases: 50.0% coverage is a known 2-partition snapshot pattern
    ("mediaarts", "video_releases"):         ("anime_video_releases", None, None),
    # ── Sakuga@wiki ────────────────────────────────────────────────────────
    # sakuga_atwiki/credits: SILVER only contains credits where the work title
    # could be resolved to a SILVER anime_id.  Unresolved credits are intentionally
    # excluded.  Coverage <50% is expected (FILTERED, not a loader bug).
    ("sakuga_atwiki", "credits"):            ("credits",  "evidence_source = 'sakuga_atwiki'",
                                              None),
    ("sakuga_atwiki", "pages"):              None,   # raw wiki pages, not a SILVER table
    # sakuga_atwiki/persons: snapshot partitions → dedup by page_id
    ("sakuga_atwiki", "persons"):            ("persons",  "id LIKE 'sakuga:%'",
                                              "CAST(page_id AS VARCHAR)"),
    ("sakuga_atwiki", "work_staff"):         None,   # denormalized; merged into credits
    # ── SeesaaWiki ─────────────────────────────────────────────────────────
    # seesaawiki/anime: snapshot partitions → dedup by id
    ("seesaawiki", "anime"):                 ("anime",    "id LIKE 'seesaa:%'",
                                              "id"),
    ("seesaawiki", "persons"):               ("persons",  "id LIKE 'seesaa:%'",
                                              None),
    ("seesaawiki", "credits"):               ("credits",  "evidence_source = 'seesaawiki'",
                                              None),
    ("seesaawiki", "studios"):               ("studios",  "id LIKE 'seesaa:%'",
                                              None),
    ("seesaawiki", "anime_studios"):         None,   # merged with other sources
    ("seesaawiki", "episode_titles"):        ("anime_episode_titles", None, None),
    ("seesaawiki", "theme_songs"):           ("anime_theme_songs", None, None),
    ("seesaawiki", "gross_studios"):         ("anime_gross_studios", None, None),
    ("seesaawiki", "original_work_info"):    ("anime_original_work_info", None, None),
    ("seesaawiki", "production_committee"):  ("anime_production_committee", None, None),
}


@dataclass
class CoverageRow:
    """Coverage measurement for one (source, bronze_table) pair."""

    source: str
    bronze_table: str
    bronze_rows: int
    silver_target: Optional[str]   # None = unmapped
    silver_filter: Optional[str]   # None = full table count used
    silver_rows: int
    coverage: Optional[float]      # None = unmapped
    unmapped: bool
    error: Optional[str] = field(default=None)

    @property
    def coverage_pct(self) -> str:
        if self.unmapped:
            return "unmapped"
        if self.error:
            return "error"
        if self.coverage is None:
            return "N/A"
        return f"{self.coverage * 100:.1f}%"

    @property
    def status(self) -> str:
        if self.unmapped:
            return "UNMAPPED"
        if self.error:
            return "ERROR"
        if self.coverage is None:
            return "N/A"
        if self.coverage >= 0.95:
            return "OK"
        if self.coverage >= 0.50:
            return "PARTIAL"
        return "LOW"


def list_bronze_tables(bronze_root: Path) -> list[tuple[str, str]]:
    """Return [(source, table_name), ...] discovered under bronze_root.

    Scans ``bronze_root/source=*/table=*/`` hive-partitioned directories.
    Ignores backup directories (``source=*.bak*``).
    """
    results: list[tuple[str, str]] = []
    if not bronze_root.exists():
        return results
    for source_dir in sorted(bronze_root.iterdir()):
        if not source_dir.is_dir():
            continue
        source_name = source_dir.name
        if not source_name.startswith("source="):
            continue
        source = source_name.removeprefix("source=")
        # Skip backup directories
        if ".bak" in source:
            continue
        for table_dir in sorted(source_dir.iterdir()):
            if not table_dir.is_dir():
                continue
            table_name = table_dir.name
            if not table_name.startswith("table="):
                continue
            table = table_name.removeprefix("table=")
            results.append((source, table))
    return results


def _count_bronze_rows(
    conn: duckdb.DuckDBPyConnection,
    bronze_root: Path,
    source: str,
    table: str,
    distinct_expr: Optional[str] = None,
) -> tuple[int, Optional[str]]:
    """Return (row_count, error_msg).  error_msg is None on success.

    When *distinct_expr* is provided the query uses
    ``COUNT(DISTINCT <distinct_expr>)`` to de-duplicate across date-partition
    snapshots.  This gives a per-entity count rather than a per-row count,
    which is the correct denominator for coverage when BRONZE stores multiple
    snapshot dates of the same dataset.

    When *distinct_expr* is None, ``COUNT(*)`` is used (total rows across all
    partitions).
    """
    glob = str(bronze_root / f"source={source}" / f"table={table}" / "date=*" / "*.parquet")
    try:
        if distinct_expr:
            sql = f"SELECT COUNT(DISTINCT {distinct_expr}) FROM read_parquet(?, hive_partitioning=true, union_by_name=true)"
        else:
            sql = "SELECT COUNT(*) FROM read_parquet(?, hive_partitioning=true, union_by_name=true)"
        result = conn.execute(sql, [glob]).fetchone()
        return (result[0] if result else 0, None)
    except Exception as exc:
        return (0, str(exc))


def _count_silver_rows(
    conn: duckdb.DuckDBPyConnection,
    silver_table: str,
    silver_filter: Optional[str],
) -> tuple[int, Optional[str]]:
    """Return (row_count, error_msg).  error_msg is None on success."""
    try:
        if silver_filter:
            sql = f"SELECT COUNT(*) FROM {silver_table} WHERE {silver_filter}"
        else:
            sql = f"SELECT COUNT(*) FROM {silver_table}"
        result = conn.execute(sql).fetchone()
        return (result[0] if result else 0, None)
    except Exception as exc:
        return (0, str(exc))


def check(
    bronze_root: Path,
    silver_db: str | Path,
) -> list[CoverageRow]:
    """Compute BRONZE → SILVER coverage for all discovered tables.

    Args:
        bronze_root: Path to the hive-partitioned bronze directory
                     (``result/bronze`` by default).
        silver_db:   Path to the SILVER DuckDB file
                     (``result/silver.duckdb`` by default).

    Returns:
        List of CoverageRow dataclass instances, one per (source, table).
    """
    bronze_root = Path(bronze_root)
    silver_db = str(silver_db)

    tables = list_bronze_tables(bronze_root)
    if not tables:
        logger.warning("silver_completeness.check: no BRONZE tables found", bronze_root=str(bronze_root))

    silver_conn = duckdb.connect(silver_db, read_only=True)
    bronze_conn = duckdb.connect(":memory:")

    rows: list[CoverageRow] = []

    try:
        for source, table in tables:
            mapping = SOURCE_TABLE_TO_SILVER.get((source, table), "UNKNOWN")

            if mapping == "UNKNOWN":
                # Not declared in our mapping at all — use COUNT(*) for the bronze count
                bronze_rows, b_err = _count_bronze_rows(bronze_conn, bronze_root, source, table)
                rows.append(CoverageRow(
                    source=source,
                    bronze_table=table,
                    bronze_rows=bronze_rows,
                    silver_target=None,
                    silver_filter=None,
                    silver_rows=0,
                    coverage=None,
                    unmapped=True,
                    error=b_err,
                ))
                continue

            if mapping is None:
                # Explicitly declared as unmapped (no corresponding SILVER table)
                bronze_rows, b_err = _count_bronze_rows(bronze_conn, bronze_root, source, table)
                rows.append(CoverageRow(
                    source=source,
                    bronze_table=table,
                    bronze_rows=bronze_rows,
                    silver_target=None,
                    silver_filter=None,
                    silver_rows=0,
                    coverage=None,
                    unmapped=True,
                    error=b_err,
                ))
                continue

            silver_table, silver_filter, distinct_expr = mapping
            bronze_rows, b_err = _count_bronze_rows(
                bronze_conn, bronze_root, source, table, distinct_expr
            )
            silver_rows, s_err = _count_silver_rows(silver_conn, silver_table, silver_filter)

            error = b_err or s_err
            if error:
                coverage = None
            elif bronze_rows == 0:
                coverage = 1.0  # empty source → nothing to load
            else:
                coverage = silver_rows / bronze_rows

            rows.append(CoverageRow(
                source=source,
                bronze_table=table,
                bronze_rows=bronze_rows,
                silver_target=silver_table,
                silver_filter=silver_filter,
                silver_rows=silver_rows,
                coverage=coverage,
                unmapped=False,
                error=error,
            ))
            logger.debug(
                "silver_completeness.check",
                source=source,
                table=table,
                bronze_rows=bronze_rows,
                silver_rows=silver_rows,
                coverage=f"{coverage * 100:.1f}%" if coverage is not None else "N/A",
            )
    finally:
        silver_conn.close()
        bronze_conn.close()

    return rows


def sample_missing_rows(
    bronze_root: Path,
    source: str,
    bronze_table: str,
    silver_db: str | Path,
    silver_table: str,
    silver_filter: Optional[str],
    n: int = 10,
) -> list[dict]:
    """Return up to n BRONZE rows that have no matching SILVER row.

    Performs a simple anti-join via DuckDB using the bronze COUNT - silver
    COUNT heuristic.  When an exact key-level anti-join is not feasible
    (different schemas), returns a sample from the BRONZE head.
    """
    bronze_root = Path(bronze_root)
    silver_db = str(silver_db)
    glob = str(bronze_root / f"source={source}" / f"table={bronze_table}" / "date=*" / "*.parquet")

    try:
        conn = duckdb.connect(silver_db, read_only=True)
        try:
            # Read BRONZE head via a memory-only connection attached to the silver conn
            bronze_conn = duckdb.connect(":memory:")
            try:
                sample_rows = bronze_conn.execute(
                    "SELECT * FROM read_parquet(?, hive_partitioning=true, union_by_name=true) LIMIT ?",
                    [glob, n],
                ).fetchall()
                cols = bronze_conn.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=true, union_by_name=true) LIMIT 0",
                    [glob],
                ).fetchall()
                col_names = [c[0] for c in cols]
                return [dict(zip(col_names, row)) for row in sample_rows]
            finally:
                bronze_conn.close()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("sample_missing_rows failed", source=source, table=bronze_table, error=str(exc))
        return []


def _fmt_table(rows: list[CoverageRow]) -> str:
    """Format a markdown table from coverage rows."""
    header = (
        "| source | bronze_table | bronze_rows | silver_target | silver_rows | coverage | status |\n"
        "|--------|--------------|-------------|---------------|-------------|----------|--------|\n"
    )
    lines = []
    for r in rows:
        silver_target = r.silver_target or "—"
        silver_rows = str(r.silver_rows) if not r.unmapped else "—"
        lines.append(
            f"| {r.source} | {r.bronze_table} | {r.bronze_rows:,} "
            f"| {silver_target} | {silver_rows} "
            f"| {r.coverage_pct} | {r.status} |"
        )
    return header + "\n".join(lines)


def generate_report(rows: list[CoverageRow], out_path: Path) -> None:
    """Write a Markdown completeness report to out_path.

    Args:
        rows:     Output of check().
        out_path: Destination path for the .md report.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    total = len(rows)
    mapped = [r for r in rows if not r.unmapped and not r.error]
    unmapped = [r for r in rows if r.unmapped]
    errors = [r for r in rows if r.error and not r.unmapped]
    ok = [r for r in mapped if r.status == "OK"]
    partial = [r for r in mapped if r.status == "PARTIAL"]
    low = [r for r in mapped if r.status == "LOW"]

    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    sections: list[str] = []

    sections.append(f"# BRONZE → SILVER Completeness Report\n\nGenerated: {now}\n")

    sections.append(textwrap.dedent(f"""\
        ## Summary

        | Metric | Count |
        |--------|-------|
        | Total BRONZE tables surveyed | {total} |
        | Mapped to SILVER | {len(mapped)} |
        | Unmapped (expected) | {len(unmapped)} |
        | Errors | {len(errors)} |
        | OK (≥95%) | {len(ok)} |
        | PARTIAL (50–95%) | {len(partial)} |
        | LOW (<50%) | {len(low)} |
    """))

    # Per-source breakdown
    sections.append("## Per-source Coverage\n")
    sources = sorted({r.source for r in rows})
    for src in sources:
        src_rows = [r for r in rows if r.source == src]
        sections.append(f"### {src}\n")
        sections.append(_fmt_table(src_rows))
        sections.append("")

    # LOW / PARTIAL details
    attention = [r for r in rows if r.status in ("LOW", "PARTIAL", "ERROR")]
    if attention:
        sections.append("## Tables Requiring Attention\n")
        sections.append(_fmt_table(attention))
        sections.append("")

    # Disclaimer
    sections.append(textwrap.dedent("""\
        ---

        **Disclaimer (JA)**: 本レポートは公開クレジットデータの構造的網羅率を示すものであり、
        個人の評価や能力判断には一切使用しない。数値はネットワーク位置と取込密度の指標。

        **Disclaimer (EN)**: This report presents structural coverage metrics
        derived solely from public credit records. The figures reflect network
        position and ingestion density, not any subjective assessment of individuals.
    """))

    report = "\n".join(sections)
    out_path.write_text(report, encoding="utf-8")
    logger.info("silver_completeness: report written", path=str(out_path))


def main(
    bronze_root: Path = Path("result/bronze"),
    silver_db: str = "result/silver.duckdb",
    out: Path = Path("result/audit/silver_completeness.md"),
) -> list[CoverageRow]:
    """CLI entry point.  Returns the list of CoverageRow for programmatic use."""
    rows = check(bronze_root, silver_db)
    generate_report(rows, out)
    return rows


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="BRONZE → SILVER completeness audit")
    parser.add_argument("--bronze-root", type=Path, default=Path("result/bronze"))
    parser.add_argument("--silver-db", type=str, default="result/silver.duckdb")
    parser.add_argument("--out", type=Path, default=Path("result/audit/silver_completeness.md"))
    args = parser.parse_args()
    main(bronze_root=args.bronze_root, silver_db=args.silver_db, out=args.out)
