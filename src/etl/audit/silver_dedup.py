"""SILVER cross-source duplicate candidate detection.

Detects duplicate candidates across persons / anime / studios / credits
in the SILVER DuckDB layer.  This module is **detection only** — no rows
are merged (H3: entity_resolution logic unchanged).

H1 invariant: anime.score / display_* columns are never read here.
"""

from __future__ import annotations

import datetime
from pathlib import Path

import duckdb
import structlog

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Normalisation helpers (applied in SQL via DuckDB string functions)
# ---------------------------------------------------------------------------

# SQL expression that normalises a title/name string:
#   1. NFKC-equivalent via LOWER (DuckDB handles unicode normalisation via
#      regexp_replace of wide chars)
#   2. strip CJK punctuation and whitespace
#   3. lowercase
_NORM_TITLE_SQL = (
    "LOWER(REGEXP_REPLACE(REGEXP_REPLACE({col},"
    " '[！？。、・＊～「」【】（）『』♪☆★…【】〔〕〈〉《》]', '', 'g'),"
    " '[\\s]+', '', 'g'))"
)

_NORM_NAME_SQL = (
    "LOWER(REGEXP_REPLACE({col}, '[\\s\\t.,，。、・／/·]+', '', 'g'))"
)

# ---------------------------------------------------------------------------
# Detection: persons
# ---------------------------------------------------------------------------

_PERSON_DUP_SQL = """
WITH base AS (
    SELECT
        id,
        SPLIT_PART(id, ':', 1)                              AS src,
        COALESCE(name_ja, '')                               AS name_ja,
        COALESCE(name_en, '')                               AS name_en,
        birth_date
    FROM persons
    WHERE (name_ja IS NOT NULL AND name_ja != '')
      OR  (name_en IS NOT NULL AND name_en != '')
),
grouped AS (
    SELECT
        COALESCE(NULLIF(name_ja, ''), name_en)              AS match_name,
        birth_date,
        COUNT(*)                                            AS row_cnt,
        COUNT(DISTINCT src)                                 AS src_cnt,
        STRING_AGG(DISTINCT src, ',')                       AS sources,
        STRING_AGG(id, ',')                                 AS candidate_ids
    FROM base
    WHERE (name_ja != '' OR name_en != '')
      AND birth_date IS NOT NULL
    GROUP BY COALESCE(NULLIF(name_ja, ''), name_en), birth_date
    HAVING COUNT(DISTINCT src) > 1
)
SELECT
    SPLIT_PART(candidate_ids, ',', 1)   AS candidate_id_a,
    SPLIT_PART(candidate_ids, ',', 2)   AS candidate_id_b,
    sources,
    match_name                          AS evidence_name,
    birth_date                          AS evidence_birth_date,
    row_cnt,
    src_cnt,
    1.0                                 AS similarity
FROM grouped
ORDER BY src_cnt DESC, row_cnt DESC
"""


def find_person_dup_candidates(
    conn: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Return persons that share exact name_ja + birth_date across sources.

    Returns:
        List of dicts with keys:
            candidate_id_a, candidate_id_b, sources, evidence_name,
            evidence_birth_date, row_cnt, src_cnt, similarity
    """
    rows = conn.execute(_PERSON_DUP_SQL).fetchall()
    cols = [
        "candidate_id_a",
        "candidate_id_b",
        "sources",
        "evidence_name",
        "evidence_birth_date",
        "row_cnt",
        "src_cnt",
        "similarity",
    ]
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# Detection: anime
# ---------------------------------------------------------------------------

_ANIME_DUP_SQL = """
WITH normalized AS (
    SELECT
        id,
        SPLIT_PART(id, ':', 1)  AS src,
        year,
        format,
        LOWER(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    COALESCE(title_ja, ''),
                    '[！？。、・＊～「」【】（）『』♪☆★…〔〕〈〉《》]', '', 'g'
                ),
                '[\\s]+', '', 'g'
            )
        )                        AS norm_title
    FROM anime
    WHERE title_ja IS NOT NULL
      AND title_ja != ''
      AND year     IS NOT NULL
      AND format   IS NOT NULL
),
grouped AS (
    SELECT
        norm_title,
        year,
        format,
        COUNT(*)            AS row_cnt,
        COUNT(DISTINCT src) AS src_cnt,
        STRING_AGG(DISTINCT src, ',')   AS sources,
        STRING_AGG(id, ',')             AS candidate_ids
    FROM normalized
    GROUP BY norm_title, year, format
    HAVING COUNT(DISTINCT src) > 1
)
SELECT
    SPLIT_PART(candidate_ids, ',', 1)   AS candidate_id_a,
    SPLIT_PART(candidate_ids, ',', 2)   AS candidate_id_b,
    sources,
    norm_title                          AS evidence_title,
    year                                AS evidence_year,
    format                              AS evidence_format,
    row_cnt,
    src_cnt,
    1.0                                 AS similarity
FROM grouped
ORDER BY src_cnt DESC, row_cnt DESC
"""


def find_anime_dup_candidates(
    conn: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Return anime rows that share normalised title_ja + year + format across sources.

    H1: anime.score / display_* columns are NOT referenced.

    Returns:
        List of dicts with keys:
            candidate_id_a, candidate_id_b, sources, evidence_title,
            evidence_year, evidence_format, row_cnt, src_cnt, similarity
    """
    rows = conn.execute(_ANIME_DUP_SQL).fetchall()
    cols = [
        "candidate_id_a",
        "candidate_id_b",
        "sources",
        "evidence_title",
        "evidence_year",
        "evidence_format",
        "row_cnt",
        "src_cnt",
        "similarity",
    ]
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# Detection: studios
# ---------------------------------------------------------------------------

_STUDIO_DUP_SQL = """
WITH normalized AS (
    SELECT
        id,
        SPLIT_PART(id, ':', 1)  AS src,
        country_of_origin,
        LOWER(
            REGEXP_REPLACE(name, '[\\s\\t.,，。、・／/·]+', '', 'g')
        )                        AS norm_name
    FROM studios
    WHERE name IS NOT NULL
      AND name != ''
),
grouped AS (
    SELECT
        norm_name,
        country_of_origin,
        COUNT(*)            AS row_cnt,
        COUNT(DISTINCT src) AS src_cnt,
        STRING_AGG(DISTINCT src, ',')   AS sources,
        STRING_AGG(id, ',')             AS candidate_ids
    FROM normalized
    GROUP BY norm_name, country_of_origin
    HAVING COUNT(DISTINCT src) > 1
)
SELECT
    SPLIT_PART(candidate_ids, ',', 1)   AS candidate_id_a,
    SPLIT_PART(candidate_ids, ',', 2)   AS candidate_id_b,
    sources,
    norm_name                           AS evidence_name,
    country_of_origin,
    row_cnt,
    src_cnt,
    1.0                                 AS similarity
FROM grouped
ORDER BY src_cnt DESC, row_cnt DESC
"""


def find_studio_dup_candidates(
    conn: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Return studios sharing the same normalised name + country across sources.

    Returns:
        List of dicts with keys:
            candidate_id_a, candidate_id_b, sources, evidence_name,
            country_of_origin, row_cnt, src_cnt, similarity
    """
    rows = conn.execute(_STUDIO_DUP_SQL).fetchall()
    cols = [
        "candidate_id_a",
        "candidate_id_b",
        "sources",
        "evidence_name",
        "country_of_origin",
        "row_cnt",
        "src_cnt",
        "similarity",
    ]
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# Detection: credits (within-source only)
# ---------------------------------------------------------------------------

_CREDIT_WITHIN_SOURCE_DUP_SQL = """
SELECT
    COALESCE(person_id, '<NULL>')   AS person_id,
    anime_id,
    role,
    evidence_source,
    episode,
    COUNT(*)                        AS dup_count
FROM credits
GROUP BY person_id, anime_id, role, evidence_source, episode
HAVING COUNT(*) > 1
ORDER BY dup_count DESC
"""


def find_credit_within_source_dup(
    conn: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Return credit rows that are duplicated within the same evidence_source.

    Cross-source duplication (same person+anime+role, different source) is
    expected and intentional — this function surfaces only within-source
    duplicates which indicate loading or ETL bugs.

    Returns:
        List of dicts with keys:
            person_id, anime_id, role, evidence_source, episode, dup_count
    """
    rows = conn.execute(_CREDIT_WITHIN_SOURCE_DUP_SQL).fetchall()
    cols = [
        "person_id",
        "anime_id",
        "role",
        "evidence_source",
        "episode",
        "dup_count",
    ]
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# CSV writer helper
# ---------------------------------------------------------------------------


def _write_csv(rows: list[dict], path: Path) -> None:
    """Write a list of dicts to a CSV file."""
    if not rows:
        path.write_text("# no candidates found\n", encoding="utf-8")
        return
    import csv

    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def audit(
    conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
) -> dict[str, int]:
    """Generate SILVER cross-source dedup audit CSVs and summary.

    Detects duplicate candidates for persons / anime / studios / credits and
    writes one CSV per table plus a Markdown summary into *output_dir*.

    Args:
        conn: Open DuckDB connection to the SILVER database (read-only is fine).
        output_dir: Directory to write result files (created if absent).

    Returns:
        Dict mapping table name to candidate count:
        ``{"persons": N, "anime": N, "studios": N, "credits_within_src": N}``
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("silver_dedup.audit: starting", output_dir=str(output_dir))

    persons_rows = find_person_dup_candidates(conn)
    log.info("persons candidates", count=len(persons_rows))

    anime_rows = find_anime_dup_candidates(conn)
    log.info("anime candidates", count=len(anime_rows))

    studio_rows = find_studio_dup_candidates(conn)
    log.info("studios candidates", count=len(studio_rows))

    credit_rows = find_credit_within_source_dup(conn)
    log.info("credit within-source dups", count=len(credit_rows))

    # Write CSVs
    _write_csv(persons_rows, output_dir / "silver_dedup_persons.csv")
    _write_csv(anime_rows, output_dir / "silver_dedup_anime.csv")
    _write_csv(studio_rows, output_dir / "silver_dedup_studios.csv")
    _write_csv(credit_rows, output_dir / "silver_dedup_credits.csv")

    counts = {
        "persons": len(persons_rows),
        "anime": len(anime_rows),
        "studios": len(studio_rows),
        "credits_within_src": len(credit_rows),
    }

    _write_summary(conn, counts, persons_rows, anime_rows, studio_rows, credit_rows, output_dir)
    log.info("silver_dedup.audit: done", **counts)
    return counts


# ---------------------------------------------------------------------------
# Markdown summary
# ---------------------------------------------------------------------------


def _top_rows(rows: list[dict], n: int = 20) -> list[dict]:
    return rows[:n]


def _write_summary(
    conn: duckdb.DuckDBPyConnection,
    counts: dict[str, int],
    persons_rows: list[dict],
    anime_rows: list[dict],
    studio_rows: list[dict],
    credit_rows: list[dict],
    output_dir: Path,
) -> None:
    """Write silver_dedup_summary.md to *output_dir*."""

    # Fetch total row counts for stop-if rate check
    totals: dict[str, int] = {}
    for tbl in ("anime", "persons", "credits", "studios"):
        totals[tbl] = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]  # type: ignore[index]

    ts = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    lines: list[str] = [
        "# SILVER Cross-Source Dedup Audit",
        "",
        f"**Generated**: {ts}  ",
        "**Detection**: exact match only — no merge performed (H3)  ",
        "**H1**: anime.score / display_* columns not used  ",
        "",
        "## Summary",
        "",
        "| Table | Total rows | Dup candidates | Rate |",
        "|-------|-----------|----------------|------|",
    ]

    def _rate(cands: int, total: int) -> str:
        if total == 0:
            return "n/a"
        return f"{cands / total * 100:.2f}%"

    lines.append(
        f"| persons | {totals['persons']:,} | {counts['persons']:,}"
        f" | {_rate(counts['persons'], totals['persons'])} |"
    )
    lines.append(
        f"| anime | {totals['anime']:,} | {counts['anime']:,}"
        f" | {_rate(counts['anime'], totals['anime'])} |"
    )
    lines.append(
        f"| studios | {totals['studios']:,} | {counts['studios']:,}"
        f" | {_rate(counts['studios'], totals['studios'])} |"
    )
    lines.append(
        f"| credits (within-src) | {totals['credits']:,} | {counts['credits_within_src']:,}"
        f" | {_rate(counts['credits_within_src'], totals['credits'])} |"
    )

    # Stop-if check: warn if any rate > 50%
    for tbl, cands, total in [
        ("persons", counts["persons"], totals["persons"]),
        ("anime", counts["anime"], totals["anime"]),
        ("studios", counts["studios"], totals["studios"]),
        ("credits (within-src)", counts["credits_within_src"], totals["credits"]),
    ]:
        if total > 0 and cands / total > 0.5:
            lines.append("")
            lines.append(
                f"> **STOP-IF WARNING**: {tbl} candidate rate"
                f" {cands / total * 100:.1f}% > 50% — tighten detection criteria"
            )

    def _table_section(
        title: str, rows: list[dict], key_fields: list[str]
    ) -> list[str]:
        out = ["", f"## {title}", ""]
        top = _top_rows(rows)
        if not top:
            out.append("_No candidates found._")
            return out
        out.append(f"Top {len(top)} of {len(rows)} total candidates:")
        out.append("")
        header = "| " + " | ".join(key_fields) + " |"
        sep = "| " + " | ".join(["---"] * len(key_fields)) + " |"
        out.append(header)
        out.append(sep)
        for r in top:
            cells = [str(r.get(f, "")) for f in key_fields]
            out.append("| " + " | ".join(cells) + " |")
        return out

    lines.extend(
        _table_section(
            "Persons Candidates (name_ja + birth_date, cross-source)",
            persons_rows,
            ["candidate_id_a", "candidate_id_b", "sources", "evidence_name", "evidence_birth_date", "similarity"],
        )
    )

    lines.extend(
        _table_section(
            "Anime Candidates (norm title_ja + year + format, cross-source)",
            anime_rows,
            ["candidate_id_a", "candidate_id_b", "sources", "evidence_title", "evidence_year", "evidence_format"],
        )
    )

    lines.extend(
        _table_section(
            "Studios Candidates (norm name + country, cross-source)",
            studio_rows,
            ["candidate_id_a", "candidate_id_b", "sources", "evidence_name", "country_of_origin"],
        )
    )

    lines.extend(
        _table_section(
            "Credits Within-Source Duplicates",
            credit_rows,
            ["person_id", "anime_id", "role", "evidence_source", "episode", "dup_count"],
        )
    )

    lines.extend([
        "",
        "## Notes",
        "",
        "- **Persons**: matched on `name_ja` (exact) + `birth_date` (exact). Cross-source only.",
        "- **Anime**: matched on NFKC-lowercased `title_ja` (punctuation stripped) + `year` + `format`. Cross-source only.",
        "- **Studios**: matched on lowercased `name` (whitespace/punctuation stripped) + `country_of_origin`. Cross-source only.",
        "- **Credits within-source**: `person_id × anime_id × role × evidence_source × episode` exact group. Null person_id rows included.",
        "- All CSVs in `result/audit/silver_dedup_*.csv`.",
        "- No merges performed. Use `src/analysis/entity_resolution.py` for actual resolution.",
    ])

    summary_path = output_dir / "silver_dedup_summary.md"
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log.info("summary written", path=str(summary_path))
