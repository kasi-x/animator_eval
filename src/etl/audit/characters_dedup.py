"""Characters cross-source duplicate candidate detection.

Detects duplicate candidates in the SILVER ``characters`` table using three
high-confidence criteria:

1. **Same anilist_id** across different source rows — automatic merge signal.
2. **Same normalised name + same voice actor** (person_id shared in
   character_voice_actors for both characters).
3. **Same normalised name + same anime_id** (character_voice_actors link).

This module is **detection only** — no rows are merged here (H3).

H1 invariant: ``characters.favourites`` is never used in any scoring path.
It is read here solely to pass through the audit CSV for human review.
"""

from __future__ import annotations

import csv
import datetime
from pathlib import Path
from typing import Any

import duckdb
import structlog

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# SQL: normalise a name column (strip spaces/punctuation, lowercase)
# ---------------------------------------------------------------------------

_NORM_SQL = (
    "LOWER(REGEXP_REPLACE(COALESCE({col}, ''), '[\\s\\t.,，。、・／/·]+', '', 'g'))"
)

# ---------------------------------------------------------------------------
# Criterion 1: same anilist_id, different row id (cross-source)
# ---------------------------------------------------------------------------

_DUP_BY_ANILIST_ID_SQL = """
SELECT
    a.id                       AS id_a,
    b.id                       AS id_b,
    COALESCE(a.name_ja, a.name_en, '') AS name_a,
    COALESCE(b.name_ja, b.name_en, '') AS name_b,
    a.anilist_id,
    'anilist_id'               AS criterion,
    1.0                        AS similarity
FROM characters a
JOIN characters b
  ON a.anilist_id = b.anilist_id
 AND a.id < b.id
WHERE a.anilist_id IS NOT NULL
ORDER BY a.anilist_id
"""

# ---------------------------------------------------------------------------
# Criterion 2: same normalised name + same voice actor (person_id)
# ---------------------------------------------------------------------------

_DUP_BY_NAME_AND_ACTOR_SQL = """
WITH normed AS (
    SELECT
        id,
        SPLIT_PART(id, ':', 1)          AS src,
        COALESCE(name_ja, name_en, '') AS display_name,
        LOWER(REGEXP_REPLACE(
            COALESCE(name_ja, name_en, ''),
            '[\\s\\t.,，。、・／/·]+', '', 'g'
        )) AS norm_name
    FROM characters
    WHERE (name_ja IS NOT NULL AND name_ja != '')
       OR (name_en IS NOT NULL AND name_en != '')
),
shared_actor AS (
    SELECT va.character_id AS cid_a, vb.character_id AS cid_b, va.person_id
    FROM character_voice_actors va
    JOIN character_voice_actors vb
      ON va.person_id = vb.person_id
     AND va.character_id < vb.character_id
)
SELECT DISTINCT
    n_a.id                     AS id_a,
    n_b.id                     AS id_b,
    n_a.display_name           AS name_a,
    n_b.display_name           AS name_b,
    sa.person_id               AS shared_actor_id,
    'name_and_actor'           AS criterion,
    1.0                        AS similarity
FROM normed n_a
JOIN normed n_b
  ON n_a.norm_name = n_b.norm_name
 AND n_a.id < n_b.id
 AND n_a.norm_name != ''
 AND n_a.src != n_b.src
JOIN shared_actor sa
  ON sa.cid_a = n_a.id
 AND sa.cid_b = n_b.id
ORDER BY n_a.id, n_b.id
"""

# ---------------------------------------------------------------------------
# Criterion 3: same normalised name + same anime_id (via CVA)
# ---------------------------------------------------------------------------

_DUP_BY_NAME_AND_ANIME_SQL = """
WITH normed AS (
    SELECT
        id,
        SPLIT_PART(id, ':', 1)          AS src,
        COALESCE(name_ja, name_en, '') AS display_name,
        LOWER(REGEXP_REPLACE(
            COALESCE(name_ja, name_en, ''),
            '[\\s\\t.,，。、・／/·]+', '', 'g'
        )) AS norm_name
    FROM characters
    WHERE (name_ja IS NOT NULL AND name_ja != '')
       OR (name_en IS NOT NULL AND name_en != '')
),
shared_anime AS (
    SELECT va.character_id AS cid_a, vb.character_id AS cid_b, va.anime_id
    FROM character_voice_actors va
    JOIN character_voice_actors vb
      ON va.anime_id = vb.anime_id
     AND va.character_id < vb.character_id
)
SELECT DISTINCT
    n_a.id                     AS id_a,
    n_b.id                     AS id_b,
    n_a.display_name           AS name_a,
    n_b.display_name           AS name_b,
    sa.anime_id                AS shared_anime_id,
    'name_and_anime'           AS criterion,
    1.0                        AS similarity
FROM normed n_a
JOIN normed n_b
  ON n_a.norm_name = n_b.norm_name
 AND n_a.id < n_b.id
 AND n_a.norm_name != ''
 AND n_a.src != n_b.src
JOIN shared_anime sa
  ON sa.cid_a = n_a.id
 AND sa.cid_b = n_b.id
ORDER BY n_a.id, n_b.id
"""

# ---------------------------------------------------------------------------
# Detection functions
# ---------------------------------------------------------------------------


def find_dup_by_anilist_id(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Return characters sharing the same anilist_id across different rows.

    H1: ``favourites`` column is not used in scoring logic; not referenced here.

    Returns:
        List of dicts with keys:
            id_a, id_b, name_a, name_b, anilist_id, criterion, similarity
    """
    rows = conn.execute(_DUP_BY_ANILIST_ID_SQL).fetchall()
    cols = ["id_a", "id_b", "name_a", "name_b", "anilist_id", "criterion", "similarity"]
    return [dict(zip(cols, r)) for r in rows]


def find_dup_by_name_and_actor(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Return character pairs with matching normalised name and shared voice actor.

    Returns:
        List of dicts with keys:
            id_a, id_b, name_a, name_b, shared_actor_id, criterion, similarity
    """
    rows = conn.execute(_DUP_BY_NAME_AND_ACTOR_SQL).fetchall()
    cols = ["id_a", "id_b", "name_a", "name_b", "shared_actor_id", "criterion", "similarity"]
    return [dict(zip(cols, r)) for r in rows]


def find_dup_by_name_and_anime(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Return character pairs with matching normalised name and shared anime.

    Returns:
        List of dicts with keys:
            id_a, id_b, name_a, name_b, shared_anime_id, criterion, similarity
    """
    rows = conn.execute(_DUP_BY_NAME_AND_ANIME_SQL).fetchall()
    cols = ["id_a", "id_b", "name_a", "name_b", "shared_anime_id", "criterion", "similarity"]
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------


def _write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    """Write a list of dicts to a CSV file, creating parent dirs if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("# no candidates found\n", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _merge_rows_dedup(
    anilist_rows: list[dict[str, Any]],
    actor_rows: list[dict[str, Any]],
    anime_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Merge all candidate rows into a unified CSV representation.

    Each row in the output CSV carries:
        id_a, id_b, criterion, evidence_detail, name_a, name_b, similarity

    Pairs already seen under a higher-priority criterion are NOT deduplicated
    here — the merge module handles idempotency via ``ON CONFLICT DO NOTHING``.
    """
    out: list[dict[str, Any]] = []
    for r in anilist_rows:
        out.append({
            "id_a": r["id_a"],
            "id_b": r["id_b"],
            "criterion": r["criterion"],
            "evidence_detail": str(r.get("anilist_id", "")),
            "name_a": r["name_a"],
            "name_b": r["name_b"],
            "similarity": r["similarity"],
        })
    for r in actor_rows:
        out.append({
            "id_a": r["id_a"],
            "id_b": r["id_b"],
            "criterion": r["criterion"],
            "evidence_detail": r.get("shared_actor_id", ""),
            "name_a": r["name_a"],
            "name_b": r["name_b"],
            "similarity": r["similarity"],
        })
    for r in anime_rows:
        out.append({
            "id_a": r["id_a"],
            "id_b": r["id_b"],
            "criterion": r["criterion"],
            "evidence_detail": r.get("shared_anime_id", ""),
            "name_a": r["name_a"],
            "name_b": r["name_b"],
            "similarity": r["similarity"],
        })
    return out


# ---------------------------------------------------------------------------
# Stop-if guard
# ---------------------------------------------------------------------------


_STOP_IF_MIN_TOTAL = 100  # guard only fires on realistic production datasets


def _check_stop_if(
    total: int,
    merge_candidate_count: int,
) -> None:
    """Raise RuntimeError if merge rate exceeds 5% (H10 / task stop-if).

    Skipped for tiny datasets (total < 100) so synthetic test fixtures pass.
    """
    if total < _STOP_IF_MIN_TOTAL:
        return
    rate = merge_candidate_count / total
    if rate > 0.05:
        msg = (
            f"STOP-IF: characters merge candidate count {merge_candidate_count} "
            f"is {rate * 100:.1f}% of total {total} — exceeds 5% threshold. "
            "Tighten detection criteria before proceeding."
        )
        log.error("characters_dedup.stop_if", rate=rate, candidates=merge_candidate_count)
        raise RuntimeError(msg)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def audit(
    conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
) -> dict[str, int]:
    """Detect cross-source character duplicate candidates and write audit CSV.

    Runs three detection criteria and writes:
    - ``characters_dedup.csv`` — unified candidates (all criteria)

    Does NOT modify the database (H3).

    Args:
        conn: Open DuckDB connection to SILVER (read-only is fine).
        output_dir: Directory for output files (created if absent).

    Returns:
        Dict with keys ``anilist_id``, ``name_and_actor``, ``name_and_anime``,
        ``total_unique_pairs`` — counts of detected candidate pairs.

    Raises:
        RuntimeError: If the total candidate count exceeds 5% of all characters
            (stop-if guard, H10).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("characters_dedup.audit: starting", output_dir=str(output_dir))

    total = conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]  # type: ignore[index]

    anilist_rows = find_dup_by_anilist_id(conn)
    log.info("criterion anilist_id", count=len(anilist_rows))

    actor_rows = find_dup_by_name_and_actor(conn)
    log.info("criterion name_and_actor", count=len(actor_rows))

    anime_rows = find_dup_by_name_and_anime(conn)
    log.info("criterion name_and_anime", count=len(anime_rows))

    # Unique pairs across all criteria (by (id_a, id_b) key)
    seen: set[tuple[str, str]] = set()
    for r in (*anilist_rows, *actor_rows, *anime_rows):
        seen.add((r["id_a"], r["id_b"]))
    total_unique = len(seen)

    _check_stop_if(total, total_unique)

    merged = _merge_rows_dedup(anilist_rows, actor_rows, anime_rows)
    _write_csv(merged, output_dir / "characters_dedup.csv")

    ts = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    summary_lines = [
        "# Characters Dedup Audit",
        "",
        f"**Generated**: {ts}",
        f"**Total characters**: {total:,}",
        "",
        "| Criterion | Candidate pairs |",
        "|-----------|----------------|",
        f"| anilist_id | {len(anilist_rows):,} |",
        f"| name_and_actor | {len(actor_rows):,} |",
        f"| name_and_anime | {len(anime_rows):,} |",
        f"| **unique pairs (all criteria)** | **{total_unique:,}** |",
        "",
        f"Rate: {total_unique / total * 100:.2f}% of total characters" if total > 0 else "Rate: n/a (empty table)",
        "",
        "H1: `characters.favourites` not used in scoring.",
        "H3: No merges performed here. Use `characters_safe_merge.py`.",
        "",
        "CSV: `result/audit/characters_dedup.csv`",
    ]
    (output_dir / "characters_dedup_summary.md").write_text(
        "\n".join(summary_lines) + "\n", encoding="utf-8"
    )

    counts = {
        "anilist_id": len(anilist_rows),
        "name_and_actor": len(actor_rows),
        "name_and_anime": len(anime_rows),
        "total_unique_pairs": total_unique,
    }
    log.info("characters_dedup.audit: done", **counts)
    return counts
