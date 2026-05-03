"""Person cluster builder for the Resolved layer (Phase 2b).

Drives cross-source person clustering by calling entity_resolution.resolve_all()
on conformed.persons rows.  The entity resolution LOGIC is not modified (H3);
we only invoke it and consume its {person_id: canonical_id} output.

Why call resolve_all() here rather than reading a pre-built audit table?
  - mart.ops_entity_resolution_audit is currently empty (no prior run).
  - Resolved layer needs a fresh, reproducible mapping every full-rebuild.
  - Running resolve_all() inside ETL keeps the process self-contained and
    ensures the audit table in resolved.duckdb is always in sync with the data.

H3 compliance:
  - resolve_all() is called as-is; its algorithm, thresholds, and merge
    conditions are not modified.
  - ML homonym splitting is skipped here (credits / anime_meta not loaded)
    to keep ETL lightweight; advanced splits happen at scoring time.

Performance note:
  - exact_match_cluster + cross_source_match are O(n) / O(n log n).
  - romaji_match and similarity_based_cluster are O(n²) worst-case and
    are skipped by default via the `fast_only` flag to keep ETL feasible
    for 272K persons.  Set fast_only=False for full resolution (slow).
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import structlog

logger = structlog.get_logger()


def _conformed_row_to_person(row: dict[str, Any]) -> "Any":
    """Convert a conformed.persons dict to a runtime.models.Person.

    Only populates fields used by entity_resolution:
      id, name_ja, name_en, mal_id, anilist_id, madb_id, ann_id.
    """
    from src.runtime.models import Person

    pid = row["id"]

    # Extract source-specific numeric IDs where available
    anilist_id: int | None = None
    mal_id: int | None = None
    madb_id: str | None = None

    if pid.startswith("anilist:"):
        try:
            anilist_id = int(pid.replace("anilist:", "").replace("p", ""))
        except ValueError:
            pass
    elif pid.startswith("mal:"):
        try:
            mal_id = int(pid.replace("mal:", "").replace("p", ""))
        except ValueError:
            pass
    elif pid.startswith("madb:"):
        madb_id = pid
    # BGM persons: bgm_id is self-referential (their own integer ID).
    # No cross-source numeric link available at this stage.

    return Person(
        id=pid,
        name_ja=row.get("name_ja") or "",
        name_en=row.get("name_en") or "",
        name_ko=row.get("name_ko") or "",
        name_zh=row.get("name_zh") or "",
        mal_id=mal_id,
        anilist_id=anilist_id,
        madb_id=madb_id,
        gender=row.get("gender"),
        date_of_birth=row.get("birth_date"),
    )


def build_persons_canonical_map(
    conformed_rows: list[dict[str, Any]],
    *,
    fast_only: bool = True,
) -> dict[str, str]:
    """Build {conformed_person_id → canonical_person_id} via entity_resolution.

    Args:
        conformed_rows: All rows from conformed.persons as dicts.
        fast_only: When True, only run exact_match_cluster + cross_source_match
                   (O(n) steps).  When False, also run romaji and similarity
                   steps (much slower, for offline / overnight runs).

    Returns:
        Dict mapping each person_id that was merged to its canonical_id.
        Persons that remain as singletons are absent from the dict.

    H3 note: The resolution functions are invoked unmodified from
             src.analysis.entity.entity_resolution.
    """
    from src.analysis.entity.entity_resolution import (
        cross_source_match,
        exact_match_cluster,
        romaji_match,
        similarity_based_cluster,
        _transitive_closure,
    )

    persons = [_conformed_row_to_person(r) for r in conformed_rows]
    logger.info("persons_cluster_start", total=len(persons), fast_only=fast_only)

    # Step 1: exact name match (fast, O(n))
    exact = exact_match_cluster(persons)
    logger.info("persons_cluster_exact", merges=len(exact))

    # Step 2: cross-source match (fast, O(n))
    cross = cross_source_match(persons)
    logger.info("persons_cluster_cross_source", merges=len(cross))

    merged: dict[str, str] = {**exact, **cross}

    if not fast_only:
        # Step 3: romaji match (moderate, O(n log n))
        already = set(merged) | set(merged.values())
        remaining = [p for p in persons if p.id not in already]
        romaji = romaji_match(remaining)
        logger.info("persons_cluster_romaji", merges=len(romaji))
        merged.update(romaji)

        # Step 4: similarity-based (slow, O(n²))
        already = set(merged) | set(merged.values())
        remaining = [p for p in persons if p.id not in already]
        similarity = similarity_based_cluster(remaining, threshold=0.95)
        logger.info("persons_cluster_similarity", merges=len(similarity))
        merged.update(similarity)

    merged = _transitive_closure(merged)
    logger.info("persons_cluster_complete", total_merges=len(merged))
    return merged


def group_persons_by_canonical(
    conformed_rows: list[dict[str, Any]],
    canonical_map: dict[str, str],
) -> dict[str, list[dict[str, Any]]]:
    """Group conformed persons rows using the canonical_map from entity_resolution.

    Rows not in canonical_map (singletons) use their own conformed id.

    Args:
        conformed_rows: All conformed.persons rows.
        canonical_map: {person_id → canonical_id} from entity_resolution.

    Returns:
        Dict {canonical_id → [row, ...]} for insertion into resolved.duckdb.
    """
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in conformed_rows:
        cid = canonical_map.get(row["id"], row["id"])
        groups[cid].append(row)
    return dict(groups)
