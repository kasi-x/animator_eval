"""DuckDB-native feature pre-computation (§4.4 + §4 残務 pipeline Phase 1.5).

Replaces the SQLite compute_feat_* functions in database.py:

  feat_credit_activity         — per-person quarter/year gap statistics
  feat_career_annual           — per-person × career-year × role-category
  feat_person_role_progression — per-person × role-category progression
  feat_studio_affiliation      — per-person × year × studio work participation
"""

from __future__ import annotations

import datetime
import statistics
from typing import Any

import structlog

from src.analysis.io.mart_writer import gold_connect_write
from src.analysis.io.conformed_reader import conformed_connect

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# feat_credit_activity
# ---------------------------------------------------------------------------


def compute_feat_credit_activity_ddb(
    current_year: int | None = None,
    current_quarter: int | None = None,
) -> int:
    """Compute per-person credit-gap statistics and write to feat_credit_activity.

    Reads credits from silver.duckdb; writes to gold.duckdb.
    Returns number of rows written.
    """
    if current_year is None:
        current_year = datetime.datetime.now().year
    if current_quarter is None:
        current_quarter = (datetime.datetime.now().month - 1) // 3 + 1
    current_abs_q = current_year * 4 + current_quarter - 1

    log.info("feat_credit_activity_compute_start")

    with conformed_connect() as silver:
        # quarter-level gaps per person
        gap_rows: list[dict] = silver.execute("""
            WITH distinct_quarters AS (
                SELECT
                    person_id,
                    credit_year * 4 + credit_quarter - 1 AS abs_quarter
                FROM credits
                WHERE credit_year IS NOT NULL AND credit_quarter IS NOT NULL
                GROUP BY person_id, credit_year, credit_quarter
            ),
            with_lag AS (
                SELECT
                    person_id,
                    abs_quarter,
                    LAG(abs_quarter) OVER (
                        PARTITION BY person_id ORDER BY abs_quarter
                    ) AS prev_quarter
                FROM distinct_quarters
            )
            SELECT person_id, abs_quarter - prev_quarter AS gap
            FROM with_lag
            WHERE prev_quarter IS NOT NULL
            ORDER BY person_id, gap
        """).fetchall()

        # activity range (quarters)
        activity_rows: list[dict] = silver.execute("""
            SELECT
                person_id,
                MIN(credit_year * 4 + credit_quarter - 1) AS first_abs_quarter,
                MAX(credit_year * 4 + credit_quarter - 1) AS last_abs_quarter,
                COUNT(DISTINCT credit_year * 4 + credit_quarter - 1) AS active_quarters
            FROM credits
            WHERE credit_year IS NOT NULL AND credit_quarter IS NOT NULL
            GROUP BY person_id
        """).fetchall()

        # year-level gaps
        year_gap_rows: list[dict] = silver.execute("""
            WITH distinct_years AS (
                SELECT person_id, credit_year
                FROM credits
                WHERE credit_year IS NOT NULL
                GROUP BY person_id, credit_year
            ),
            with_lag AS (
                SELECT
                    person_id,
                    credit_year - LAG(credit_year) OVER (
                        PARTITION BY person_id ORDER BY credit_year
                    ) AS gap
                FROM distinct_years
            )
            SELECT
                person_id,
                COUNT(*) AS n_year_gaps,
                AVG(gap) AS mean_year_gap,
                MAX(gap) AS max_year_gap
            FROM with_lag
            WHERE gap IS NOT NULL
            GROUP BY person_id
        """).fetchall()

        active_years_rows: list[dict] = silver.execute("""
            SELECT person_id, COUNT(DISTINCT credit_year) AS active_years
            FROM credits
            WHERE credit_year IS NOT NULL
            GROUP BY person_id
        """).fetchall()

    # aggregate gap stats per person
    from collections import defaultdict

    gaps_by_person: dict[str, list[int]] = defaultdict(list)
    for r in gap_rows:
        gaps_by_person[r[0]].append(r[1])

    activity = {r[0]: (r[1], r[2], r[3]) for r in activity_rows}  # pid → (first, last, active_q)
    year_gaps = {r[0]: (r[1], r[2], r[3]) for r in year_gap_rows}
    active_years_map = {r[0]: r[1] for r in active_years_rows}

    rows: list[tuple] = []
    for pid, gaps in gaps_by_person.items():
        if pid not in activity:
            continue
        first_q, last_q, active_q = activity[pid]
        span = last_q - first_q if last_q is not None and first_q is not None else 0
        density = active_q / span if span > 0 else 1.0
        n_gaps = len(gaps)
        mean_g = statistics.mean(gaps) if gaps else None
        median_g = statistics.median(gaps) if gaps else None
        min_g = min(gaps) if gaps else None
        max_g = max(gaps) if gaps else None
        std_g = statistics.stdev(gaps) if len(gaps) >= 2 else 0.0
        consecutive = sum(1 for g in gaps if g == 1)
        consec_rate = consecutive / n_gaps if n_gaps > 0 else 0.0
        hiatuses = [g for g in gaps if g >= 4]
        n_hiatuses = len(hiatuses)
        longest_hiatus = max(hiatuses) if hiatuses else 0
        since_last = current_abs_q - last_q if last_q is not None else None

        yg = year_gaps.get(pid, (0, None, None))
        ay = active_years_map.get(pid, 0)

        rows.append((
            pid, first_q, last_q, span, active_q, density,
            n_gaps, mean_g, median_g, min_g, max_g, std_g,
            consecutive, consec_rate, n_hiatuses, longest_hiatus,
            since_last, ay, yg[0], yg[1], yg[2],
        ))

    # also add persons with no gaps (only 1 active quarter)
    for pid, (first_q, last_q, active_q) in activity.items():
        if pid not in gaps_by_person:
            span = last_q - first_q if last_q is not None and first_q is not None else 0
            since_last = current_abs_q - last_q if last_q is not None else None
            yg = year_gaps.get(pid, (0, None, None))
            ay = active_years_map.get(pid, 0)
            rows.append((
                pid, first_q, last_q, span, active_q, 1.0,
                0, None, None, None, None, 0.0,
                0, 0.0, 0, 0, since_last,
                ay, yg[0], yg[1], yg[2],
            ))

    with gold_connect_write() as gold:
        gold.execute("DELETE FROM feat_credit_activity")
        if rows:
            gold.executemany(
                """
                INSERT INTO feat_credit_activity (
                    person_id, first_abs_quarter, last_abs_quarter,
                    activity_span_quarters, active_quarters, density,
                    n_gaps, mean_gap_quarters, median_gap_quarters,
                    min_gap_quarters, max_gap_quarters, std_gap_quarters,
                    consecutive_quarters, consecutive_rate,
                    n_hiatuses, longest_hiatus_quarters, quarters_since_last_credit,
                    active_years, n_year_gaps, mean_year_gap, max_year_gap
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                rows,
            )

    log.info("feat_credit_activity_computed", rows=len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# feat_career_annual
# ---------------------------------------------------------------------------


def compute_feat_career_annual_ddb() -> int:
    """Aggregate per-person × career-year × role-category counts into feat_career_annual.

    Reads credits from silver.duckdb; writes to gold.duckdb.
    Returns number of rows written.
    """
    from src.utils.role_groups import ROLE_CATEGORY

    log.info("feat_career_annual_compute_start")

    # Build a VALUES clause for the role→category mapping
    cat_map_values = ",\n".join(
        f"('{role.replace(chr(39), chr(39)*2)}', '{cat}')"
        for role, cat in ROLE_CATEGORY.items()
    )

    with conformed_connect() as silver:
        rows: list[tuple] = silver.execute(f"""
            WITH role_cats AS (
                SELECT * FROM (VALUES
                    {cat_map_values}
                ) t(role, category)
            ),
            debut AS (
                SELECT person_id, MIN(credit_year) AS debut_year
                FROM credits
                WHERE credit_year IS NOT NULL
                GROUP BY person_id
            ),
            base AS (
                SELECT
                    c.person_id,
                    c.credit_year,
                    c.role,
                    COALESCE(rc.category, 'other') AS category,
                    COUNT(DISTINCT c.anime_id) AS n_works,
                    COUNT(*) AS n_credits
                FROM credits c
                LEFT JOIN role_cats rc ON rc.role = c.role
                WHERE c.credit_year IS NOT NULL
                GROUP BY c.person_id, c.credit_year, c.role, rc.category
            )
            SELECT
                b.person_id,
                b.credit_year - d.debut_year AS career_year,
                b.credit_year,
                SUM(b.n_works)   AS n_works,
                SUM(b.n_credits) AS n_credits,
                COUNT(DISTINCT b.role) AS n_roles,
                SUM(CASE WHEN b.category='direction'             THEN b.n_works ELSE 0 END) AS works_direction,
                SUM(CASE WHEN b.category='animation_supervision' THEN b.n_works ELSE 0 END) AS works_animation_supervision,
                SUM(CASE WHEN b.category='animation'             THEN b.n_works ELSE 0 END) AS works_animation,
                SUM(CASE WHEN b.category='design'                THEN b.n_works ELSE 0 END) AS works_design,
                SUM(CASE WHEN b.category='technical'             THEN b.n_works ELSE 0 END) AS works_technical,
                SUM(CASE WHEN b.category='art'                   THEN b.n_works ELSE 0 END) AS works_art,
                SUM(CASE WHEN b.category='sound'                 THEN b.n_works ELSE 0 END) AS works_sound,
                SUM(CASE WHEN b.category='writing'               THEN b.n_works ELSE 0 END) AS works_writing,
                SUM(CASE WHEN b.category='production'            THEN b.n_works ELSE 0 END) AS works_production,
                SUM(CASE WHEN b.category='production_management' THEN b.n_works ELSE 0 END) AS works_production_management,
                SUM(CASE WHEN b.category='finishing'             THEN b.n_works ELSE 0 END) AS works_finishing,
                SUM(CASE WHEN b.category='editing'               THEN b.n_works ELSE 0 END) AS works_editing,
                SUM(CASE WHEN b.category='settings'              THEN b.n_works ELSE 0 END) AS works_settings,
                SUM(CASE WHEN b.category='other'                 THEN b.n_works ELSE 0 END) AS works_other
            FROM base b
            JOIN debut d ON d.person_id = b.person_id
            GROUP BY b.person_id, b.credit_year, d.debut_year
            ORDER BY b.person_id, b.credit_year
        """).fetchall()

    with gold_connect_write() as gold:
        gold.execute("DELETE FROM feat_career_annual")
        if rows:
            gold.executemany(
                """
                INSERT INTO feat_career_annual (
                    person_id, career_year, credit_year,
                    n_works, n_credits, n_roles,
                    works_direction, works_animation_supervision, works_animation,
                    works_design, works_technical, works_art, works_sound,
                    works_writing, works_production, works_production_management,
                    works_finishing, works_editing, works_settings, works_other
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                rows,
            )

    log.info("feat_career_annual_computed", rows=len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# feat_person_role_progression
# ---------------------------------------------------------------------------


def compute_feat_person_role_progression_ddb(
    current_year: int | None = None,
    active_threshold_years: int = 3,
) -> int:
    """Compute per-person × role-category progression into feat_person_role_progression.

    Reads credits from silver.duckdb; writes to gold.duckdb.
    Returns number of rows written.
    """
    from src.utils.role_groups import ROLE_CATEGORY

    log.info("feat_person_role_progression_compute_start")

    if current_year is None:
        with conformed_connect() as s:
            row = s.execute(
                "SELECT MAX(credit_year) FROM credits WHERE credit_year IS NOT NULL"
            ).fetchone()
            current_year = row[0] if row and row[0] else 2024

    cat_map_values = ",\n".join(
        f"('{role.replace(chr(39), chr(39)*2)}', '{cat}')"
        for role, cat in ROLE_CATEGORY.items()
    )

    with conformed_connect() as silver:
        rows_raw: list[Any] = silver.execute(f"""
            WITH role_cats AS (
                SELECT * FROM (VALUES
                    {cat_map_values}
                ) t(role, category)
            ),
            debut AS (
                SELECT person_id, MIN(credit_year) AS debut_year
                FROM credits
                WHERE credit_year IS NOT NULL AND credit_year > 1900
                GROUP BY person_id
            ),
            base AS (
                SELECT
                    c.person_id,
                    COALESCE(rc.category, 'other') AS category,
                    c.credit_year,
                    COUNT(DISTINCT c.anime_id) AS n_works,
                    COUNT(*) AS n_credits
                FROM credits c
                LEFT JOIN role_cats rc ON rc.role = c.role
                WHERE c.credit_year IS NOT NULL AND c.credit_year > 1900
                GROUP BY c.person_id, rc.category, c.credit_year
            ),
            agg AS (
                SELECT
                    b.person_id,
                    b.category,
                    MIN(b.credit_year)   AS first_year,
                    MAX(b.credit_year)   AS last_year,
                    SUM(b.n_works)       AS n_works,
                    SUM(b.n_credits)     AS n_credits
                FROM base b
                GROUP BY b.person_id, b.category
            ),
            peak AS (
                SELECT person_id, category, credit_year AS peak_year
                FROM (
                    SELECT person_id, category, credit_year,
                        ROW_NUMBER() OVER (
                            PARTITION BY person_id, category
                            ORDER BY n_works DESC, credit_year DESC
                        ) AS rn
                    FROM base
                ) t
                WHERE rn = 1
            )
            SELECT
                a.person_id,
                a.category,
                a.first_year,
                a.last_year,
                p.peak_year,
                a.n_works,
                a.n_credits,
                a.first_year - d.debut_year AS career_year_first,
                CASE WHEN {current_year} - a.last_year <= {active_threshold_years} THEN 1 ELSE 0 END AS still_active
            FROM agg a
            JOIN peak p ON p.person_id = a.person_id AND p.category = a.category
            LEFT JOIN debut d ON d.person_id = a.person_id
        """).fetchall()

    with gold_connect_write() as gold:
        gold.execute("DELETE FROM feat_person_role_progression")
        if rows_raw:
            gold.executemany(
                """
                INSERT INTO feat_person_role_progression (
                    person_id, role_category, first_year, last_year,
                    peak_year, n_works, n_credits, career_year_first, still_active
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                rows_raw,
            )

    log.info("feat_person_role_progression_computed", rows=len(rows_raw))
    return len(rows_raw)


# ---------------------------------------------------------------------------
# feat_studio_affiliation
# ---------------------------------------------------------------------------


def compute_feat_studio_affiliation_ddb() -> int:
    """Aggregate per-person × year × studio participation into feat_studio_affiliation.

    Reads credits, anime_studios, studios from silver.duckdb; writes to gold.duckdb.
    Returns number of rows written. Returns 0 if anime_studios is absent from silver.
    """
    log.info("feat_studio_affiliation_compute_start")

    with conformed_connect() as silver:
        # Graceful check: skip if anime_studios not yet in silver
        tables = {
            r[0]
            for r in silver.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
        if "anime_studios" not in tables:
            log.info("feat_studio_affiliation_skipped", reason="anime_studios_not_in_silver")
            return 0

        rows: list[tuple] = silver.execute("""
            SELECT
                c.person_id,
                c.credit_year,
                ast.studio_id,
                MAX(COALESCE(s.name, '')) AS studio_name,
                COUNT(DISTINCT c.anime_id) AS n_works,
                COUNT(*)               AS n_credits,
                MAX(CAST(ast.is_main AS INTEGER)) AS is_main_studio
            FROM credits c
            INNER JOIN anime_studios ast ON ast.anime_id = c.anime_id
            LEFT JOIN studios s ON s.id = ast.studio_id
            WHERE c.credit_year IS NOT NULL
            GROUP BY c.person_id, c.credit_year, ast.studio_id
            ORDER BY c.person_id, c.credit_year
        """).fetchall()

    with gold_connect_write() as gold:
        gold.execute("DELETE FROM feat_studio_affiliation")
        if rows:
            gold.executemany(
                """
                INSERT INTO feat_studio_affiliation
                    (person_id, credit_year, studio_id, studio_name,
                     n_works, n_credits, is_main_studio)
                VALUES (?,?,?,?,?,?,?)
                """,
                rows,
            )

    log.info("feat_studio_affiliation_computed", rows=len(rows))
    return len(rows)
