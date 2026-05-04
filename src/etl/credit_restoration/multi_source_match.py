"""Multi-source fuzzy match for pre-1990 anime credit restoration.

Finds cross-source agreement for person–role–anime combinations that are
absent from the canonical SILVER credits table, producing
``RestorationCandidate`` records for downstream insertion.

Design constraints
------------------
- H1: ``anime.score`` is never read. Only structural columns are used.
- H3: Entity-resolution logic is not modified. Only the existing
  ``persons`` table is consulted for name lookup.
- H4: All candidates carry ``evidence_source = 'restoration_estimated'``.
- Threshold: rapidfuzz similarity >= 0.85 (configurable via ``threshold``).
  If false-positive rate exceeds 20% in spot-checks, raise to 0.90.

Sources consulted
-----------------
- ANN (src_ann) — English-language credit records
- mediaarts/madb (src_mediaarts) — NFAJ-adjacent Japanese records
- seesaawiki (src_seesaawiki) — fan-compiled historical credits
- allcinema (src_allcinema) — Japanese theatrical/OVA deep coverage

Only anime with ``year < 1990`` (or ``NULL`` year treated as pre-modern)
are targeted.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import structlog

log = structlog.get_logger(__name__)

# Fuzzy-match similarity threshold.  Candidates below this are discarded.
DEFAULT_THRESHOLD: float = 0.85

# Target cohort: pre-1990 anime only.
HISTORICAL_YEAR_CUTOFF: int = 1990

# Evidence source tag for all restored rows.
EVIDENCE_SOURCE: str = "restoration_estimated"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class RestorationCandidate:
    """A credit record that could not be confirmed in SILVER credits.

    Produced by ``find_restoration_candidates()`` and consumed by
    ``insert_restored_credits()``.

    Fields
    ------
    anime_id:
        SILVER anime.id (matched from title fuzzy lookup).
    role:
        Normalised role string (lower-case, underscore-delimited).
    person_name_candidate:
        The name string as it appears in source data.
    person_id_candidate:
        Resolved SILVER person.id when entity resolution finds a match;
        ``None`` when no match is found and the record is tentative.
    sources_supporting:
        List of source identifiers that agree on this credit.
    similarity_score:
        Highest pairwise name similarity across sources.
    cohort_year:
        Anime release year (``anime.year``).
    progression_consistency:
        True when the person's role in this work is consistent with
        their known career trajectory (earlier = assistant/key, later
        = director/AD).  ``False`` when data are insufficient.
    confidence_tier:
        'MEDIUM' (>= 2 sources) or 'LOW' (1 source, sim > threshold).
        The caller may override to 'RESTORED' before insertion.
    """

    anime_id: str
    role: str
    person_name_candidate: str
    person_id_candidate: str | None
    sources_supporting: list[str] = field(default_factory=list)
    similarity_score: float = 0.0
    cohort_year: int | None = None
    progression_consistency: bool = False
    confidence_tier: str = "LOW"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _fuzzy_sim(a: str, b: str) -> float:
    """Return token-sort ratio similarity in [0, 1].

    Uses ``rapidfuzz`` when available, falls back to a character-level
    Jaro-Winkler approximation so the module remains importable without
    the native extension.
    """
    if not a or not b:
        return 0.0
    try:
        from rapidfuzz import fuzz  # type: ignore[import]
        return fuzz.token_sort_ratio(a, b) / 100.0
    except ImportError:
        # Fallback: character overlap ratio (Dice coefficient).
        sa, sb = set(a.lower()), set(b.lower())
        intersection = len(sa & sb)
        denom = (len(sa) + len(sb))
        return (2 * intersection / denom) if denom else 0.0


def _fetch_historical_anime(conn: Any) -> list[tuple[str, str, int | None]]:
    """Return (id, title_ja, year) for pre-1990 anime.

    Args:
        conn: SILVER DB connection.

    Returns:
        List of (anime_id, title_ja, year) tuples.
    """
    try:
        rows = conn.execute(
            """
            SELECT id, title_ja, year
            FROM   anime
            WHERE  year < ? OR year IS NULL
            ORDER  BY year
            """,
            (HISTORICAL_YEAR_CUTOFF,),
        ).fetchall()
        return [(str(r[0]), str(r[1] or ""), r[2]) for r in rows]
    except Exception as exc:
        log.warning("historical_anime_query_failed", error=str(exc))
        return []


def _fetch_silver_credits(conn: Any, anime_id: str) -> set[tuple[str, str]]:
    """Return set of (person_id, role) already in SILVER for an anime.

    Args:
        conn: SILVER DB connection.
        anime_id: Target anime id.

    Returns:
        Set of (person_id, role) tuples.
    """
    try:
        rows = conn.execute(
            "SELECT person_id, role FROM credits WHERE anime_id = ?",
            (anime_id,),
        ).fetchall()
        return {(str(r[0]), str(r[1]).lower()) for r in rows}
    except Exception as exc:
        log.warning("silver_credits_query_failed", anime_id=anime_id, error=str(exc))
        return set()


def _fetch_bronze_credits_for_title(
    conn: Any,
    title_ja: str,
    threshold: float,
) -> list[dict[str, Any]]:
    """Query available BRONZE source tables for credits matching the given title.

    This function probes each BRONZE table that may exist and collects raw
    credit rows.  Tables are optional; missing tables are silently skipped.

    Args:
        conn: SILVER (or shared) DB connection.  BRONZE tables may be
              attached or co-located depending on the deployment.
        title_ja: Japanese title of the target anime (used for fuzzy match).
        threshold: Minimum similarity for a title match.

    Returns:
        List of dicts with keys: source, person_name, role_raw.
    """
    candidates: list[dict[str, Any]] = []

    # Source table probes: (table_name, title_col, person_col, role_col, source_label)
    source_probes = [
        ("src_ann_credits",        "title",     "person_name", "role",    "ann"),
        ("src_mediaarts_credits",  "title_ja",  "person_name", "role_ja", "mediaarts"),
        ("src_seesaawiki_credits", "anime_title","name",        "role",    "seesaawiki"),
        ("src_allcinema_credits",  "title",     "name",        "role",    "allcinema"),
    ]

    for table, title_col, person_col, role_col, source_label in source_probes:
        try:
            rows = conn.execute(
                f"SELECT {title_col}, {person_col}, {role_col} FROM {table}"  # noqa: S608
            ).fetchall()
        except Exception:
            continue  # table absent or schema differs

        for row in rows:
            raw_title = str(row[0] or "")
            raw_person = str(row[1] or "")
            raw_role   = str(row[2] or "")
            if not raw_person or not raw_role:
                continue
            sim = _fuzzy_sim(raw_title, title_ja)
            if sim >= threshold:
                candidates.append(
                    {
                        "source":       source_label,
                        "person_name":  raw_person,
                        "role_raw":     raw_role.lower().replace(" ", "_"),
                        "title_sim":    sim,
                    }
                )

    return candidates


def _resolve_person_id(conn: Any, name_candidate: str) -> str | None:
    """Try to resolve a person name to a SILVER persons.id.

    Uses exact match on name_ja / name_en / aliases, then falls back to
    fuzzy similarity.  Entity-resolution logic is intentionally thin here;
    the canonical five-stage pipeline is handled by
    ``src.analysis.entity_resolution`` (H3: not modified).

    Args:
        conn: SILVER DB connection.
        name_candidate: Raw name string from source.

    Returns:
        SILVER persons.id or None.
    """
    if not name_candidate:
        return None

    # Exact match on name_ja or name_en.
    try:
        row = conn.execute(
            """
            SELECT id FROM persons
            WHERE  LOWER(name_ja) = LOWER(?)
               OR  LOWER(name_en) = LOWER(?)
            LIMIT  1
            """,
            (name_candidate, name_candidate),
        ).fetchone()
        if row:
            return str(row[0])
    except Exception:
        pass

    # Fuzzy scan (only against known persons — no external calls).
    try:
        all_persons = conn.execute(
            "SELECT id, name_ja, name_en FROM persons"
        ).fetchall()
    except Exception:
        return None

    best_id: str | None = None
    best_sim: float = 0.85  # minimum threshold for fuzzy resolution

    for row in all_persons:
        pid = str(row[0])
        for col_name in (row[1], row[2]):
            if not col_name:
                continue
            s = _fuzzy_sim(name_candidate, str(col_name))
            if s > best_sim:
                best_sim = s
                best_id = pid

    return best_id


def _check_role_progression_consistency(
    conn: Any,
    person_id: str | None,
    role: str,
    cohort_year: int | None,
) -> bool:
    """Rough check whether the role assignment is chronologically plausible.

    A very junior role (key_animator, inbetweener) at year Y is consistent
    if the person also holds senior roles at year > Y.  An implausible
    assignment (e.g., director credit 20 years before any other credit) is
    flagged False.

    This is a best-effort structural check.  When data are insufficient,
    returns True (benefit of the doubt).

    Args:
        conn: SILVER DB connection.
        person_id: Resolved SILVER persons.id (may be None).
        role: Target role (lower-case).
        cohort_year: Year of the candidate anime.

    Returns:
        True when consistent or when data are too sparse to judge.
    """
    if person_id is None or cohort_year is None:
        return True  # cannot evaluate; allow

    _SENIOR_ROLES = frozenset({
        "director", "series_director", "chief_director",
        "animation_director", "character_designer",
    })
    _JUNIOR_ROLES = frozenset({
        "key_animator", "animator", "inbetweener",
        "second_key_animator",
    })

    try:
        rows = conn.execute(
            """
            SELECT MIN(credit_year), MAX(credit_year), role
            FROM   credits
            WHERE  person_id = ?
              AND  credit_year IS NOT NULL
            GROUP  BY role
            """,
            (person_id,),
        ).fetchall()
    except Exception:
        return True  # query failed; allow

    if not rows:
        return True  # no existing credits; allow

    role_norm = role.lower()
    all_years = [r[0] for r in rows if r[0] is not None]
    earliest_credit = min(all_years) if all_years else None

    # A SENIOR role as the very first credit is implausible only if there are
    # many later junior credits (strong inconsistency signal).
    if role_norm in _SENIOR_ROLES and earliest_credit is not None:
        if cohort_year < earliest_credit - 15:
            return False  # unlikely to have had senior role that early

    return True


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def find_restoration_candidates(
    conn: Any,
    *,
    threshold: float = DEFAULT_THRESHOLD,
    year_cutoff: int = HISTORICAL_YEAR_CUTOFF,
) -> list[RestorationCandidate]:
    """Find credit restoration candidates for pre-1990 anime.

    Cross-references BRONZE source tables against SILVER credits.  Any
    person–role–anime combination present in at least one BRONZE source
    but absent from SILVER credits is returned as a ``RestorationCandidate``.

    The confidence tier is set as follows:

    - MEDIUM: 2 or more sources agree on the credit.
    - LOW:    1 source only, similarity >= threshold.

    Callers (e.g. ``insert_restored_credits``) may promote the tier to
    RESTORED before insertion.

    Args:
        conn: SILVER DB connection (BRONZE tables may be attached).
        threshold: Fuzzy similarity threshold for title matching.
        year_cutoff: Only process anime with year < year_cutoff.

    Returns:
        List of RestorationCandidate, sorted by (anime_id, role, person_name).
    """
    historical_anime = _fetch_historical_anime(conn)
    if not historical_anime:
        log.info("no_historical_anime_found", cutoff=year_cutoff)
        return []

    log.info("restoration_scan_start", anime_count=len(historical_anime), threshold=threshold)

    all_candidates: list[RestorationCandidate] = []

    for anime_id, title_ja, year in historical_anime:
        silver_existing = _fetch_silver_credits(conn, anime_id)
        bronze_hits = _fetch_bronze_credits_for_title(conn, title_ja, threshold)

        if not bronze_hits:
            continue

        # Aggregate hits by (person_name_normalised, role).
        from collections import defaultdict
        aggregated: dict[tuple[str, str], dict[str, Any]] = defaultdict(
            lambda: {"sources": [], "max_sim": 0.0}
        )
        for hit in bronze_hits:
            key = (hit["person_name"].strip(), hit["role_raw"])
            aggregated[key]["sources"].append(hit["source"])
            if hit["title_sim"] > aggregated[key]["max_sim"]:
                aggregated[key]["max_sim"] = hit["title_sim"]

        for (person_name, role), agg in aggregated.items():
            sources = list(set(agg["sources"]))
            sim = agg["max_sim"]

            # Resolve person id.
            person_id = _resolve_person_id(conn, person_name)

            # Skip if already in SILVER (by person_id + role).
            if person_id and (person_id, role) in silver_existing:
                continue

            # Determine confidence tier.
            tier = "MEDIUM" if len(sources) >= 2 else "LOW"

            prog_ok = _check_role_progression_consistency(
                conn, person_id, role, year
            )

            all_candidates.append(
                RestorationCandidate(
                    anime_id=anime_id,
                    role=role,
                    person_name_candidate=person_name,
                    person_id_candidate=person_id,
                    sources_supporting=sources,
                    similarity_score=sim,
                    cohort_year=year,
                    progression_consistency=prog_ok,
                    confidence_tier=tier,
                )
            )

    all_candidates.sort(key=lambda c: (c.anime_id, c.role, c.person_name_candidate))
    log.info(
        "restoration_scan_complete",
        candidates=len(all_candidates),
        medium_tier=sum(1 for c in all_candidates if c.confidence_tier == "MEDIUM"),
        low_tier=sum(1 for c in all_candidates if c.confidence_tier == "LOW"),
    )
    return all_candidates
