"""Nationality resolver for O4 foreign national position analysis.

Priority order:
  1. persons.country_of_origin (high confidence — AniList source)
  2. name_zh present → infer CN/TW (medium confidence)
  3. name_ko present → infer KR (medium confidence)
  4. fallback → "unknown" (low confidence)

Country codes are ISO 3166-1 alpha-2 where possible.
SE-Asia umbrella covers TH, PH, VN, ID, MY, SG.

Note: name_zh / name_ko inference carries false-positive risk (CJK diaspora,
second-generation residents, transliteration). Confidence field is exposed so
callers can apply appropriate thresholds.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field

import structlog

log = structlog.get_logger(__name__)

# Country codes treated as "domestic" (Japan)
_DOMESTIC_CODES: frozenset[str] = frozenset({"JP", "JPN", "Japan", "japan"})

# SE-Asia country codes (ISO 3166-1 alpha-2)
_SE_ASIA_CODES: frozenset[str] = frozenset(
    {"TH", "PH", "VN", "ID", "MY", "SG", "MM", "KH", "LA", "BN"}
)

# CN cluster (mainland + TW + HK)
_CN_CLUSTER_CODES: frozenset[str] = frozenset({"CN", "TW", "HK"})

# Canonical group labels
GROUP_DOMESTIC = "JP"
GROUP_CN = "CN"
GROUP_KR = "KR"
GROUP_SE_ASIA = "SE_ASIA"
GROUP_OTHER = "OTHER"
GROUP_UNKNOWN = "UNKNOWN"

# Confidence levels
CONF_HIGH = "high"
CONF_MEDIUM = "medium"
CONF_LOW = "low"


@dataclass
class NationalityRecord:
    """Resolved nationality for a single person."""

    person_id: str
    country_code: str
    group: str
    confidence: str


def _normalize_country_code(raw: str | None) -> str | None:
    """Return upper-cased stripped code, or None if empty."""
    if not raw:
        return None
    return raw.strip().upper()


def _map_to_group(code: str) -> str:
    """Map a normalized country code to analysis group."""
    if code in _DOMESTIC_CODES or code == "JP":
        return GROUP_DOMESTIC
    if code in _CN_CLUSTER_CODES:
        return GROUP_CN
    if code == "KR":
        return GROUP_KR
    if code in _SE_ASIA_CODES:
        return GROUP_SE_ASIA
    return GROUP_OTHER


def resolve_nationality(
    person_id: str,
    country_of_origin: str | None,
    name_zh: str | None,
    name_ko: str | None,
) -> NationalityRecord:
    """Resolve nationality for a single person row.

    Args:
        person_id: SILVER persons.id
        country_of_origin: persons.country_of_origin (may be None)
        name_zh: persons.name_zh (may be None or empty)
        name_ko: persons.name_ko (may be None or empty)

    Returns:
        NationalityRecord with group and confidence level.
    """
    raw_code = _normalize_country_code(country_of_origin)

    if raw_code:
        group = _map_to_group(raw_code)
        return NationalityRecord(
            person_id=person_id,
            country_code=raw_code,
            group=group,
            confidence=CONF_HIGH,
        )

    # Fallback: name_zh → CN cluster estimate
    if name_zh and name_zh.strip():
        return NationalityRecord(
            person_id=person_id,
            country_code="CN_EST",
            group=GROUP_CN,
            confidence=CONF_MEDIUM,
        )

    # Fallback: name_ko → KR estimate
    if name_ko and name_ko.strip():
        return NationalityRecord(
            person_id=person_id,
            country_code="KR_EST",
            group=GROUP_KR,
            confidence=CONF_MEDIUM,
        )

    return NationalityRecord(
        person_id=person_id,
        country_code="UNK",
        group=GROUP_UNKNOWN,
        confidence=CONF_LOW,
    )


def load_nationality_records(conn: sqlite3.Connection) -> list[NationalityRecord]:
    """Load and resolve nationality for all persons in SILVER.

    Queries persons table for country_of_origin, name_zh, name_ko.
    Gracefully handles missing columns (older schema).

    Args:
        conn: SILVER SQLite connection.

    Returns:
        List of NationalityRecord, one per person.
    """
    # Detect available columns
    try:
        col_info = conn.execute("PRAGMA table_info(persons)").fetchall()
        cols = {r[1] for r in col_info}
    except Exception:
        cols = set()

    has_country = "country_of_origin" in cols
    has_zh = "name_zh" in cols
    has_ko = "name_ko" in cols

    select_parts = ["id"]
    if has_country:
        select_parts.append("country_of_origin")
    else:
        select_parts.append("NULL AS country_of_origin")
    if has_zh:
        select_parts.append("name_zh")
    else:
        select_parts.append("NULL AS name_zh")
    if has_ko:
        select_parts.append("name_ko")
    else:
        select_parts.append("NULL AS name_ko")

    sql = f"SELECT {', '.join(select_parts)} FROM persons"

    try:
        rows = conn.execute(sql).fetchall()
    except Exception as exc:
        log.warning("nationality_resolver_query_failed", error=str(exc))
        return []

    records: list[NationalityRecord] = []
    for row in rows:
        pid, country, zh, ko = row[0], row[1], row[2], row[3]
        records.append(resolve_nationality(pid, country, zh, ko))

    return records


@dataclass
class NationalitySummary:
    """Aggregate nationality coverage statistics."""

    total_persons: int
    n_high_confidence: int
    n_medium_confidence: int
    n_low_confidence: int
    group_counts: dict[str, int] = field(default_factory=dict)
    coverage_pct: float = 0.0

    @classmethod
    def from_records(cls, records: list[NationalityRecord]) -> "NationalitySummary":
        """Compute summary from resolved nationality records."""
        total = len(records)
        n_high = sum(1 for r in records if r.confidence == CONF_HIGH)
        n_med = sum(1 for r in records if r.confidence == CONF_MEDIUM)
        n_low = sum(1 for r in records if r.confidence == CONF_LOW)

        group_counts: dict[str, int] = {}
        for r in records:
            group_counts[r.group] = group_counts.get(r.group, 0) + 1

        known = n_high + n_med
        coverage = 100.0 * known / total if total > 0 else 0.0

        return cls(
            total_persons=total,
            n_high_confidence=n_high,
            n_medium_confidence=n_med,
            n_low_confidence=n_low,
            group_counts=group_counts,
            coverage_pct=coverage,
        )


def person_fe_by_nationality(
    conn: sqlite3.Connection,
    nationality_records: list[NationalityRecord],
) -> dict[str, list[float]]:
    """Fetch person FE values grouped by nationality group.

    Joins nationality_records with feat_person_scores.
    Groups with fewer than 5 persons are excluded.

    Args:
        conn: SILVER/GOLD SQLite connection.
        nationality_records: Pre-resolved nationality list.

    Returns:
        Dict mapping group label → list of person_fe floats.
    """
    # Build a person_id → group map for the high/medium confidence records
    id_to_group: dict[str, str] = {
        r.person_id: r.group
        for r in nationality_records
        if r.confidence in (CONF_HIGH, CONF_MEDIUM)
    }

    if not id_to_group:
        return {}

    try:
        rows = conn.execute(
            "SELECT person_id, person_fe FROM feat_person_scores "
            "WHERE person_fe IS NOT NULL"
        ).fetchall()
    except Exception as exc:
        log.warning("person_fe_query_failed", error=str(exc))
        return {}

    groups: dict[str, list[float]] = {}
    for pid, fe in rows:
        group = id_to_group.get(pid)
        if group is None:
            continue
        groups.setdefault(group, []).append(float(fe))

    # Remove groups with < 5 persons
    return {g: vals for g, vals in groups.items() if len(vals) >= 5}


def studio_foreign_share(
    conn: sqlite3.Connection,
    nationality_records: list[NationalityRecord],
    min_credits: int = 10,
) -> list[dict]:
    """Compute foreign-person credit share per studio.

    Args:
        conn: SILVER SQLite connection.
        nationality_records: Pre-resolved nationality records.
        min_credits: Minimum total credits for a studio to be included.

    Returns:
        List of dicts with studio_id, total_credits, foreign_credits,
        foreign_share, and studio_fe (if available).
        Sorted by foreign_share descending.
    """
    foreign_ids: frozenset[str] = frozenset(
        r.person_id
        for r in nationality_records
        if r.group not in (GROUP_DOMESTIC, GROUP_UNKNOWN)
        and r.confidence in (CONF_HIGH, CONF_MEDIUM)
    )

    if not foreign_ids:
        return []

    try:
        rows = conn.execute("""
            SELECT a.studio_id, c.person_id, COUNT(*) AS n_credits
            FROM credits c
            JOIN anime a ON c.anime_id = a.id
            WHERE a.studio_id IS NOT NULL
            GROUP BY a.studio_id, c.person_id
        """).fetchall()
    except Exception as exc:
        log.warning("studio_foreign_share_query_failed", error=str(exc))
        return []

    # Aggregate per studio
    studio_total: dict[str, int] = {}
    studio_foreign: dict[str, int] = {}

    for studio_id, person_id, n_credits in rows:
        studio_total[studio_id] = studio_total.get(studio_id, 0) + n_credits
        if person_id in foreign_ids:
            studio_foreign[studio_id] = studio_foreign.get(studio_id, 0) + n_credits

    # Fetch studio FEs if available
    studio_fe: dict[str, float] = {}
    try:
        fe_rows = conn.execute(
            "SELECT studio_id, studio_fe FROM feat_studio_affiliation "
            "WHERE studio_fe IS NOT NULL GROUP BY studio_id HAVING COUNT(*) >= 1"
        ).fetchall()
        studio_fe = {r[0]: float(r[1]) for r in fe_rows}
    except Exception:
        pass

    results = []
    for studio_id, total in studio_total.items():
        if total < min_credits:
            continue
        foreign = studio_foreign.get(studio_id, 0)
        share = foreign / total if total > 0 else 0.0
        results.append({
            "studio_id": studio_id,
            "total_credits": total,
            "foreign_credits": foreign,
            "foreign_share": share,
            "studio_fe": studio_fe.get(studio_id),
        })

    results.sort(key=lambda x: x["foreign_share"], reverse=True)
    return results
