"""anime 拡張列 cross-source copy: anilist rows → non-anilist rows.

Propagates country_of_origin / synonyms / description / external_links_json
and related display columns from anilist-sourced SILVER rows to other SILVER
rows that represent the same real-world anime.

Matching strategy (H3 compliant — no entity-resolution logic, only existing
ID-based mapping):
    1. For anilist rows (id LIKE 'anilist:%'):
       anilist_id_int = TRY_CAST(REPLACE(id, 'anilist:', '') AS INTEGER)
    2. For keyframe rows (id LIKE 'keyframe:%'):
       anilist_id_int from BRONZE keyframe/table=anime anilist_id column.
    3. For MAL rows (id LIKE 'mal:%'):
       anilist_id_int via BRONZE anilist/table=anime mal_id cross-reference.

Column copy rules:
    - country_of_origin: overwrite (anilist is authoritative; COALESCE dst first)
    - synonyms:          overwrite (anilist is authoritative; COALESCE dst first)
    - description:       COALESCE (keep existing dst value when non-NULL)
    - external_links_json: overwrite (anilist is authoritative; COALESCE dst first)

All copies are COALESCE-guarded on the destination side so existing non-NULL
values are never replaced by NULL.

IMPORTANT — Phase 2 migration note:
    This module implements Resolved-layer functionality inside the Conformed
    (SILVER) layer as a temporary measure.  When the 5-tier architecture's
    Resolved layer is implemented (Phase 2), this cross-source copy logic
    must be removed and callers updated to read from the Resolved layer.
    See: docs/ARCHITECTURE_5_TIER_PROPOSAL.md
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import structlog

logger = structlog.get_logger()

# ─── DDL ─────────────────────────────────────────────────────────────────────

# anilist_id_int is the integer AniList media ID stored on every SILVER anime
# row that can be linked to an AniList record.  Used exclusively as a join key
# for cross-source copy — never as a scoring input.
_DDL_ANILIST_ID_INT = (
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS anilist_id_int INTEGER"
)

# ─── SQL: populate anilist_id_int ────────────────────────────────────────────

# Populate anilist_id_int for anilist rows: extract the numeric suffix.
_POPULATE_ANILIST_ROWS_SQL = """
UPDATE anime
SET anilist_id_int = TRY_CAST(REPLACE(id, 'anilist:', '') AS INTEGER)
WHERE id LIKE 'anilist:%'
  AND anilist_id_int IS NULL
"""

# Populate anilist_id_int for keyframe rows using BRONZE keyframe anime data.
_POPULATE_KEYFRAME_ROWS_SQL = """
UPDATE anime
SET anilist_id_int = bronze.anilist_id
FROM (
    SELECT id, TRY_CAST(anilist_id AS INTEGER) AS anilist_id
    FROM (
        SELECT id, anilist_id,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
        FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
        WHERE  id IS NOT NULL
          AND  anilist_id IS NOT NULL
    )
    WHERE _rn = 1
) AS bronze
WHERE anime.id = bronze.id
  AND anime.anilist_id_int IS NULL
  AND bronze.anilist_id IS NOT NULL
"""

# Populate anilist_id_int for MAL rows using the anilist BRONZE mal_id
# cross-reference: find anilist rows whose BRONZE entry lists mal_id matching
# the MAL row's mal_id_int.
_POPULATE_MAL_ROWS_SQL = """
UPDATE anime
SET anilist_id_int = anilist_map.anilist_int
FROM (
    SELECT
        TRY_CAST(mal_id AS INTEGER)             AS mal_int,
        TRY_CAST(REPLACE(id, 'anilist:', '') AS INTEGER) AS anilist_int
    FROM (
        SELECT id, mal_id,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
        FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
        WHERE  id IS NOT NULL
          AND  mal_id IS NOT NULL
          AND  id LIKE 'anilist:%'
    )
    WHERE _rn = 1
      AND TRY_CAST(mal_id AS INTEGER) IS NOT NULL
) AS anilist_map
WHERE anime.mal_id_int = anilist_map.mal_int
  AND anime.anilist_id_int IS NULL
  AND anime.id LIKE 'mal:%'
"""

# ─── SQL: COALESCE copy ──────────────────────────────────────────────────────

# Copy anilist extra columns to non-anilist rows sharing the same anilist_id_int.
# COALESCE rules:
#   - country_of_origin: use src value (anilist authoritative), COALESCE preserves dst when src NULL
#   - synonyms:          same as country_of_origin
#   - description:       COALESCE(dst, src) — preserve any existing description
#   - external_links_json: use src value (anilist authoritative), COALESCE preserves dst when src NULL
_COPY_FROM_ANILIST_SQL = """
UPDATE anime AS dst
SET
    country_of_origin    = COALESCE(src.country_of_origin, dst.country_of_origin),
    synonyms             = COALESCE(src.synonyms, dst.synonyms),
    description          = COALESCE(dst.description, src.description),
    external_links_json  = COALESCE(src.external_links_json, dst.external_links_json)
FROM anime AS src
WHERE dst.anilist_id_int IS NOT NULL
  AND src.anilist_id_int = dst.anilist_id_int
  AND src.id LIKE 'anilist:%'
  AND dst.id NOT LIKE 'anilist:%'
  AND (
      dst.country_of_origin   IS NULL
   OR dst.synonyms            IS NULL
   OR dst.description         IS NULL
   OR dst.external_links_json IS NULL
  )
"""


# ─── public API ──────────────────────────────────────────────────────────────


def _apply_ddl(conn: duckdb.DuckDBPyConnection) -> None:
    """Add anilist_id_int column to the anime table (idempotent)."""
    conn.execute(_DDL_ANILIST_ID_INT)


def _populate_anilist_id_int(
    conn: duckdb.DuckDBPyConnection,
    bronze_root: Path | None,
) -> dict[str, int]:
    """Populate anime.anilist_id_int for all linkable rows.

    Args:
        conn: Open DuckDB connection pointing at the SILVER database.
        bronze_root: Root directory of BRONZE parquet partitions.
                     When None, only anilist-row extraction is performed
                     (keyframe and MAL cross-reference require BRONZE access).

    Returns:
        Dict with counts of rows updated per strategy:
        ``anilist_self``, ``keyframe_via_bronze``, ``mal_via_bronze``.
    """
    counts: dict[str, int] = {}

    # Strategy 1: anilist rows — extract numeric suffix from id
    conn.execute(_POPULATE_ANILIST_ROWS_SQL)
    counts["anilist_self"] = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE id LIKE 'anilist:%' AND anilist_id_int IS NOT NULL"
    ).fetchone()[0]

    if bronze_root is None:
        counts["keyframe_via_bronze"] = 0
        counts["mal_via_bronze"] = 0
        return counts

    bronze_root = Path(bronze_root)

    # Strategy 2: keyframe rows — from BRONZE keyframe anime anilist_id
    kf_glob = str(
        bronze_root / "source=keyframe" / "table=anime" / "date=*" / "*.parquet"
    )
    kf_files = list((bronze_root / "source=keyframe" / "table=anime").glob("date=*/*.parquet"))
    if kf_files:
        try:
            conn.execute(_POPULATE_KEYFRAME_ROWS_SQL, [kf_glob])
        except Exception as exc:
            logger.warning("cross_source_kf_populate_skip", error=str(exc))
    counts["keyframe_via_bronze"] = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE id LIKE 'keyframe:%' AND anilist_id_int IS NOT NULL"
    ).fetchone()[0]

    # Strategy 3: MAL rows — via anilist BRONZE mal_id cross-reference
    al_glob = str(
        bronze_root / "source=anilist" / "table=anime" / "date=*" / "*.parquet"
    )
    al_files = list((bronze_root / "source=anilist" / "table=anime").glob("date=*/*.parquet"))
    if al_files:
        try:
            conn.execute(_POPULATE_MAL_ROWS_SQL, [al_glob])
        except Exception as exc:
            logger.warning("cross_source_mal_populate_skip", error=str(exc))
    counts["mal_via_bronze"] = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE id LIKE 'mal:%' AND anilist_id_int IS NOT NULL"
    ).fetchone()[0]

    return counts


def copy_from_anilist(
    conn: duckdb.DuckDBPyConnection,
    bronze_root: Path | str | None = None,
) -> dict[str, int]:
    """Copy anilist extra columns to non-anilist rows sharing the same anime.

    Performs three steps:
        1. Add anilist_id_int column (DDL, idempotent).
        2. Populate anilist_id_int for all rows that can be linked.
        3. COALESCE-copy extra columns from anilist rows to linked non-anilist rows.

    Extra columns copied:
        - country_of_origin    (anilist authoritative; COALESCE preserves dst)
        - synonyms             (anilist authoritative; COALESCE preserves dst)
        - description          (COALESCE dst first — preserve existing descriptions)
        - external_links_json  (anilist authoritative; COALESCE preserves dst)

    H1 compliance: no score / popularity columns are written.
    H3 compliance: no entity-resolution logic; only existing ID-based mapping.

    IMPORTANT — Phase 2 migration note:
        When the Resolved layer (5-tier architecture) is complete, this function
        must be removed.  See src/etl/cross_source_copy/__init__.py for details.

    Args:
        conn: Open DuckDB connection pointing at the SILVER database.
              Must already contain the anime table with extension columns
              (applied by silver_loaders.anilist.integrate).
        bronze_root: Root directory of BRONZE parquet partitions.
                     Required for keyframe and MAL cross-reference population.
                     When None, only anilist-row self-population is performed.

    Returns:
        Dict with counts of changed rows:
            ``anilist_id_int_anilist``   – anilist rows with anilist_id_int set
            ``anilist_id_int_keyframe``  – keyframe rows with anilist_id_int set
            ``anilist_id_int_mal``       – MAL rows with anilist_id_int set
            ``country_copied``           – rows updated with country_of_origin
            ``synonyms_copied``          – rows updated with synonyms
            ``description_copied``       – rows updated with description
            ``external_links_copied``    – rows updated with external_links_json
    """
    if bronze_root is not None:
        bronze_root = Path(bronze_root)

    _apply_ddl(conn)

    populate_counts = _populate_anilist_id_int(conn, bronze_root)
    logger.info(
        "cross_source_copy_populate",
        anilist_self=populate_counts.get("anilist_self", 0),
        keyframe_via_bronze=populate_counts.get("keyframe_via_bronze", 0),
        mal_via_bronze=populate_counts.get("mal_via_bronze", 0),
    )

    # Capture pre-copy NULL counts to compute delta
    pre = conn.execute("""
        SELECT
            COUNT(*) FILTER (WHERE country_of_origin   IS NOT NULL AND id NOT LIKE 'anilist:%') AS pre_country,
            COUNT(*) FILTER (WHERE synonyms            IS NOT NULL AND id NOT LIKE 'anilist:%') AS pre_synonyms,
            COUNT(*) FILTER (WHERE description         IS NOT NULL AND id NOT LIKE 'anilist:%') AS pre_desc,
            COUNT(*) FILTER (WHERE external_links_json IS NOT NULL AND id NOT LIKE 'anilist:%') AS pre_ext
        FROM anime
    """).fetchone()

    conn.execute(_COPY_FROM_ANILIST_SQL)

    post = conn.execute("""
        SELECT
            COUNT(*) FILTER (WHERE country_of_origin   IS NOT NULL AND id NOT LIKE 'anilist:%') AS post_country,
            COUNT(*) FILTER (WHERE synonyms            IS NOT NULL AND id NOT LIKE 'anilist:%') AS post_synonyms,
            COUNT(*) FILTER (WHERE description         IS NOT NULL AND id NOT LIKE 'anilist:%') AS post_desc,
            COUNT(*) FILTER (WHERE external_links_json IS NOT NULL AND id NOT LIKE 'anilist:%') AS post_ext
        FROM anime
    """).fetchone()

    result = {
        "anilist_id_int_anilist": populate_counts.get("anilist_self", 0),
        "anilist_id_int_keyframe": populate_counts.get("keyframe_via_bronze", 0),
        "anilist_id_int_mal": populate_counts.get("mal_via_bronze", 0),
        "country_copied": max(0, post[0] - pre[0]),
        "synonyms_copied": max(0, post[1] - pre[1]),
        "description_copied": max(0, post[2] - pre[2]),
        "external_links_copied": max(0, post[3] - pre[3]),
    }

    logger.info(
        "cross_source_copy_done",
        country_copied=result["country_copied"],
        synonyms_copied=result["synonyms_copied"],
        description_copied=result["description_copied"],
        external_links_copied=result["external_links_copied"],
    )

    return result
