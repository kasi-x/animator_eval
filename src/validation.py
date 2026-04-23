"""Data quality validation — integrity checks run before pipeline execution.

Detects orphan credits, missing data, and outliers to ensure
scoring reliability.
"""

import sqlite3
from dataclasses import dataclass, field

import structlog

logger = structlog.get_logger()


@dataclass
class ValidationResult:
    """Validation result container."""

    passed: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)
        self.passed = False

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)


def validate_referential_integrity(conn: sqlite3.Connection) -> ValidationResult:
    """Referential integrity check — detect orphan credits."""
    result = ValidationResult()

    # credits referencing non-existent person_id
    orphan_person = conn.execute("""
        SELECT COUNT(*) FROM credits c
        LEFT JOIN persons p ON c.person_id = p.id
        WHERE p.id IS NULL
    """).fetchone()[0]

    if orphan_person > 0:
        result.add_error(
            f"orphan_credits_person: {orphan_person} credits reference non-existent persons"
        )

    # credits referencing non-existent anime_id
    orphan_anime = conn.execute("""
        SELECT COUNT(*) FROM credits c
        LEFT JOIN anime a ON c.anime_id = a.id
        WHERE a.id IS NULL
    """).fetchone()[0]

    if orphan_anime > 0:
        result.add_error(
            f"orphan_credits_anime: {orphan_anime} credits reference non-existent anime"
        )

    result.stats["orphan_person_credits"] = orphan_person
    result.stats["orphan_anime_credits"] = orphan_anime

    return result


def validate_data_completeness(conn: sqlite3.Connection) -> ValidationResult:
    """Data completeness check — detect missing names and titles."""
    result = ValidationResult()

    # persons with both name_ja and name_en empty
    nameless = conn.execute("""
        SELECT COUNT(*) FROM persons
        WHERE name_ja = '' AND name_en = ''
    """).fetchone()[0]

    if nameless > 0:
        result.add_warning(
            f"nameless_persons: {nameless} persons have no name (ja or en)"
        )

    # anime with both title_ja and title_en empty
    titleless = conn.execute("""
        SELECT COUNT(*) FROM anime
        WHERE title_ja = '' AND title_en = ''
    """).fetchone()[0]

    if titleless > 0:
        result.add_warning(f"titleless_anime: {titleless} anime have no title")

    # anime with no year
    no_year = conn.execute("""
        SELECT COUNT(*) FROM anime WHERE year IS NULL
    """).fetchone()[0]

    if no_year > 0:
        result.add_warning(f"no_year_anime: {no_year} anime have no year")

    # anime with no score (display metadata from bronze src_anilist_anime via anime_external_ids)
    no_score = conn.execute("""
        SELECT COUNT(*)
        FROM anime a
        LEFT JOIN anime_external_ids ext ON ext.anime_id = a.id AND ext.source = 'anilist'
        LEFT JOIN src_anilist_anime b ON b.anilist_id = CAST(ext.external_id AS INTEGER)
        WHERE b.score IS NULL
    """).fetchone()[0]

    total_anime = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
    if total_anime > 0 and no_score / total_anime > 0.5:
        result.add_warning(
            f"low_score_coverage: {no_score}/{total_anime} anime have no score "
            f"({no_score / total_anime:.0%})"
        )

    result.stats.update(
        {
            "nameless_persons": nameless,
            "titleless_anime": titleless,
            "no_year_anime": no_year,
            "no_score_anime": no_score,
        }
    )

    return result


def validate_credit_distribution(conn: sqlite3.Connection) -> ValidationResult:
    """Credit distribution check — detect unusual skew."""
    result = ValidationResult()

    # persons with no credits
    no_credits = conn.execute("""
        SELECT COUNT(*) FROM persons p
        LEFT JOIN credits c ON p.id = c.person_id
        WHERE c.id IS NULL
    """).fetchone()[0]

    if no_credits > 0:
        result.add_warning(
            f"persons_without_credits: {no_credits} persons have no credits"
        )

    # anime with no credits
    no_anime_credits = conn.execute("""
        SELECT COUNT(*) FROM anime a
        LEFT JOIN credits c ON a.id = c.anime_id
        WHERE c.id IS NULL
    """).fetchone()[0]

    if no_anime_credits > 0:
        result.add_warning(
            f"anime_without_credits: {no_anime_credits} anime have no credits"
        )

    # persons with extreme credit counts (check if top 1% are outliers)
    top_credits = conn.execute("""
        SELECT person_id, COUNT(*) as cnt FROM credits
        GROUP BY person_id ORDER BY cnt DESC LIMIT 5
    """).fetchall()

    if top_credits:
        max_credits = top_credits[0][1]
        avg_credits = conn.execute("""
            SELECT AVG(cnt) FROM (
                SELECT COUNT(*) as cnt FROM credits GROUP BY person_id
            )
        """).fetchone()[0]

        if avg_credits and max_credits > avg_credits * 50:
            result.add_warning(
                f"credit_outlier: top person has {max_credits} credits "
                f"(avg={avg_credits:.1f}, ratio={max_credits / avg_credits:.0f}x)"
            )

    result.stats.update(
        {
            "persons_without_credits": no_credits,
            "anime_without_credits": no_anime_credits,
        }
    )

    return result


def validate_credit_quality(conn: sqlite3.Connection) -> ValidationResult:
    """Credit quality check — detect duplicates and abnormal patterns."""
    result = ValidationResult()

    # same person × same anime with many distinct roles (5+ may indicate an anomaly)
    multi_role = conn.execute("""
        SELECT person_id, anime_id, COUNT(DISTINCT role) as role_count
        FROM credits
        GROUP BY person_id, anime_id
        HAVING role_count >= 5
    """).fetchall()

    if multi_role:
        result.add_warning(
            f"multi_role_credits: {len(multi_role)} person-anime pairs have 5+ distinct roles"
        )

    # same-source duplicates (same person × anime × role across multiple episodes)
    source_dupes = conn.execute("""
        SELECT evidence_source AS source_code, COUNT(*) as total,
               COUNT(DISTINCT person_id || '|' || anime_id || '|' || role) as unique_count
        FROM credits
        WHERE evidence_source != ''
        GROUP BY source_code
    """).fetchall()

    for row in source_dupes:
        total, unique = row["total"], row["unique_count"]
        if total > unique * 2:
            result.add_warning(
                f"high_episode_density_{row['source_code']}: {total} credits but only {unique} unique person-anime-role combos"
            )

    result.stats["multi_role_pairs"] = len(multi_role)

    return result


def validate_data_freshness(
    conn: sqlite3.Connection,
    stale_years: int = 5,
) -> ValidationResult:
    """Data freshness check — detect stale data.

    Args:
        conn: DB connection
        stale_years: threshold in years; data older than this is considered stale
    """
    from datetime import datetime

    result = ValidationResult()
    current_year = datetime.now().year
    cutoff_year = current_year - stale_years

    # most recent anime year in DB
    latest = conn.execute(
        "SELECT MAX(year) FROM anime WHERE year IS NOT NULL"
    ).fetchone()[0]
    if latest and latest < cutoff_year:
        result.add_warning(
            f"stale_data: newest anime is from {latest}, "
            f"no data from last {stale_years} years"
        )

    # most recent credit year per data source
    source_freshness = conn.execute("""
        SELECT c.evidence_source AS source_code, MAX(a.year) as latest_year, COUNT(*) as credit_count
        FROM credits c
        JOIN anime a ON c.anime_id = a.id
        WHERE a.year IS NOT NULL AND c.evidence_source != ''
        GROUP BY source_code
    """).fetchall()

    stale_sources = []
    for row in source_freshness:
        if row["latest_year"] and row["latest_year"] < cutoff_year:
            stale_sources.append(row["source_code"])

    if stale_sources:
        result.add_warning(
            f"stale_sources: {', '.join(stale_sources)} have no data from last {stale_years} years"
        )

    # fraction of persons with no recent credits (inactivity detection)
    inactive = conn.execute(
        """
        SELECT COUNT(DISTINCT c.person_id) FROM credits c
        JOIN anime a ON c.anime_id = a.id
        WHERE a.year IS NOT NULL
        GROUP BY c.person_id
        HAVING MAX(a.year) < ?
    """,
        (cutoff_year,),
    ).fetchall()

    total_persons = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
    inactive_count = len(inactive)

    if total_persons > 0 and inactive_count / total_persons > 0.5:
        result.add_warning(
            f"high_inactivity: {inactive_count}/{total_persons} persons "
            f"({inactive_count / total_persons:.0%}) have no credits since {cutoff_year}"
        )

    # fraction of persons credited by only a single source
    single_source = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT person_id FROM credits
            WHERE evidence_source != ''
            GROUP BY person_id
            HAVING COUNT(DISTINCT evidence_source) = 1
        )
    """).fetchone()[0]

    credited_persons = conn.execute("""
        SELECT COUNT(DISTINCT person_id) FROM credits
        WHERE evidence_source != ''
    """).fetchone()[0]

    if credited_persons > 10 and single_source / credited_persons > 0.8:
        result.add_warning(
            f"low_source_diversity: {single_source}/{credited_persons} persons "
            f"({single_source / credited_persons:.0%}) have credits from only one source"
        )

    result.stats.update(
        {
            "latest_anime_year": latest or 0,
            "stale_sources": len(stale_sources),
            "inactive_persons": inactive_count,
            "single_source_persons": single_source,
        }
    )

    return result


def validate_all(conn: sqlite3.Connection) -> ValidationResult:
    """Run all validation checks and return a combined result."""
    combined = ValidationResult()

    checks = [
        ("referential_integrity", validate_referential_integrity),
        ("data_completeness", validate_data_completeness),
        ("credit_distribution", validate_credit_distribution),
        ("credit_quality", validate_credit_quality),
        ("data_freshness", validate_data_freshness),
    ]

    for name, check_fn in checks:
        result = check_fn(conn)
        combined.errors.extend(result.errors)
        combined.warnings.extend(result.warnings)
        combined.stats.update(result.stats)
        if not result.passed:
            combined.passed = False
        logger.info(
            "validation_check",
            check=name,
            passed=result.passed,
            errors=len(result.errors),
            warnings=len(result.warnings),
        )

    logger.info(
        "validation_complete",
        passed=combined.passed,
        total_errors=len(combined.errors),
        total_warnings=len(combined.warnings),
    )

    return combined
