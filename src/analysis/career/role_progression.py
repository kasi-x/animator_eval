"""Role progression analysis — time-to-advancement for animation career pipeline.

Computes person-level progression years across the four-stage animation pipeline:
  in_between (動画) → key_animator (原画) → animation_director (作監) → director (監督)

Functions:
- compute_progression_years(): per-person years between role-from and role-to
- km_role_tenure(): Kaplan-Meier fit on a progression DataFrame
- compute_studio_blockage(): studio-level blockage score vs. industry median (bootstrap CI)

All computations are structural (credit records only).  Viewer ratings are not used.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Literal

import numpy as np
import structlog

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

#: Ordered pipeline roles (low → high stage)
PIPELINE_ROLES: list[str] = [
    "in_between",        # 動画 — Stage 1
    "key_animator",      # 原画 — Stage 3
    "animation_director",# 作監 — Stage 5
    "director",          # 監督 — Stage 6
]

#: Human-readable labels (JP/EN)
PIPELINE_LABELS: dict[str, str] = {
    "in_between": "動画 / In-Between",
    "key_animator": "原画 / Key Animator",
    "animation_director": "作監 / Animation Director",
    "director": "監督 / Director",
}

#: Cohort bin size (years)
COHORT_BIN = 5

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ProgressionRecord:
    """Single person × role-pair progression measurement.

    duration_years is None when the person never reached role_to
    (right-censored observation).
    """

    person_id: str
    role_from: str
    role_to: str
    first_year_from: int
    first_year_to: int | None
    duration_years: float | None  # None = censored
    cohort_5y: int                # debut_year rounded down to nearest COHORT_BIN


@dataclass
class KMResult:
    """Kaplan-Meier curve for a single stratum.

    timeline, survival, ci_lower, ci_upper are parallel lists.
    """

    label: str
    n: int
    n_events: int
    timeline: list[float] = field(default_factory=list)
    survival: list[float] = field(default_factory=list)
    ci_lower: list[float] = field(default_factory=list)
    ci_upper: list[float] = field(default_factory=list)
    median_survival: float | None = None


@dataclass
class StudioBlockageRow:
    """Blockage score for a single studio relative to the industry median.

    Positive values = longer-than-median progression time (pipeline blockage).
    Negative values = faster-than-median progression (early advancement).
    """

    studio_id: str
    n_persons: int
    studio_median: float
    industry_median: float
    blockage_score: float       # studio_median - industry_median
    ci_low: float               # bootstrap 2.5th percentile
    ci_high: float              # bootstrap 97.5th percentile


# ---------------------------------------------------------------------------
# Step 1: progression_years
# ---------------------------------------------------------------------------


def compute_progression_years(
    conn: sqlite3.Connection,
    role_from: str,
    role_to: str,
    *,
    min_year: int = 1970,
    max_year: int = 2025,
) -> list[ProgressionRecord]:
    """Compute per-person years from first role_from credit to first role_to credit.

    Args:
        conn: SILVER-layer SQLite connection.
        role_from: Source role string (e.g. 'in_between').
        role_to: Target role string (e.g. 'key_animator').
        min_year: Earliest plausible debut year (filters noisy data).
        max_year: Latest year to treat as observed.

    Returns:
        List of ProgressionRecord.  Censored observations have duration_years=None.
    """
    if role_from not in PIPELINE_ROLES or role_to not in PIPELINE_ROLES:
        raise ValueError(
            f"role_from={role_from!r} and role_to={role_to!r} must be in PIPELINE_ROLES"
        )

    sql_from = """
        SELECT person_id, MIN(credit_year) AS first_year
        FROM credits
        WHERE role = ?
          AND credit_year IS NOT NULL
          AND credit_year >= ?
          AND credit_year <= ?
        GROUP BY person_id
    """
    sql_to = """
        SELECT person_id, MIN(credit_year) AS first_year
        FROM credits
        WHERE role = ?
          AND credit_year IS NOT NULL
          AND credit_year >= ?
          AND credit_year <= ?
        GROUP BY person_id
    """

    from_rows = {
        r[0]: r[1]
        for r in conn.execute(sql_from, (role_from, min_year, max_year)).fetchall()
    }
    to_rows = {
        r[0]: r[1]
        for r in conn.execute(sql_to, (role_to, min_year, max_year)).fetchall()
    }

    records: list[ProgressionRecord] = []
    for person_id, first_from in from_rows.items():
        cohort_5y = (first_from // COHORT_BIN) * COHORT_BIN

        first_to = to_rows.get(person_id)

        # Only count forward transitions (role_to occurs after role_from)
        if first_to is not None and first_to >= first_from:
            duration = float(first_to - first_from)
        else:
            duration = None
            first_to = None

        records.append(
            ProgressionRecord(
                person_id=person_id,
                role_from=role_from,
                role_to=role_to,
                first_year_from=first_from,
                first_year_to=first_to,
                duration_years=duration,
                cohort_5y=cohort_5y,
            )
        )

    log.debug(
        "progression_years_computed",
        role_from=role_from,
        role_to=role_to,
        n_from=len(from_rows),
        n_to=len(to_rows),
        n_records=len(records),
        n_observed=sum(1 for r in records if r.duration_years is not None),
    )
    return records


# ---------------------------------------------------------------------------
# Step 2: KM curve + cohort comparison
# ---------------------------------------------------------------------------


def km_role_tenure(
    records: list[ProgressionRecord],
    *,
    cohort_col: Literal["cohort_5y"] = "cohort_5y",
    min_cohort_size: int = 10,
) -> dict[str, KMResult]:
    """Fit Kaplan-Meier survival curves stratified by cohort.

    Args:
        records: Output of compute_progression_years().
        cohort_col: Stratification column (currently only 'cohort_5y').
        min_cohort_size: Cohorts smaller than this are excluded.

    Returns:
        Dict mapping cohort label → KMResult.
        Duration = years from first role_from credit.
        Event = reaching role_to.
        Censored = never reached role_to (duration set to max_year - first_from).
    """
    try:
        from lifelines import KaplanMeierFitter
    except ImportError as exc:
        raise ImportError(
            "lifelines is required for km_role_tenure(). "
            "Add lifelines>=0.30 to pixi.toml."
        ) from exc

    # Group by cohort
    cohort_groups: dict[int, list[ProgressionRecord]] = {}
    for rec in records:
        key = rec.cohort_5y
        cohort_groups.setdefault(key, []).append(rec)

    results: dict[str, KMResult] = {}

    for cohort_year, cohort_recs in sorted(cohort_groups.items()):
        if len(cohort_recs) < min_cohort_size:
            continue

        durations = []
        events = []
        for rec in cohort_recs:
            if rec.duration_years is not None:
                durations.append(rec.duration_years)
                events.append(True)
            else:
                # Censor at max observed window (25 years as reasonable career span)
                durations.append(25.0)
                events.append(False)

        if sum(events) == 0:
            continue

        kmf = KaplanMeierFitter()
        kmf.fit(durations, event_observed=events, label=str(cohort_year))

        sf = kmf.survival_function_
        ci = kmf.confidence_interval_survival_function_

        timeline = list(sf.index.astype(float))
        survival = list(sf.iloc[:, 0].astype(float))
        ci_lower = list(ci.iloc[:, 0].astype(float))
        ci_upper = list(ci.iloc[:, 1].astype(float))

        median_val = kmf.median_survival_time_
        if hasattr(median_val, "__iter__"):
            median_val = float(list(median_val)[0])
        else:
            median_val = float(median_val) if not np.isnan(float(median_val)) else None

        label = f"{cohort_year}–{cohort_year + COHORT_BIN - 1}"
        results[label] = KMResult(
            label=label,
            n=len(cohort_recs),
            n_events=int(sum(events)),
            timeline=timeline,
            survival=survival,
            ci_lower=ci_lower,
            ci_upper=ci_upper,
            median_survival=median_val,
        )

    return results


def logrank_cohort_comparison(
    records: list[ProgressionRecord],
    *,
    min_cohort_size: int = 10,
) -> dict:
    """Run pairwise log-rank tests between cohorts.

    Args:
        records: Output of compute_progression_years().
        min_cohort_size: Cohorts smaller than this are excluded.

    Returns:
        Dict with keys 'p_value' (overall), 'method', 'n_cohorts'.
        Returns empty dict if lifelines is unavailable or insufficient data.
    """
    try:
        from lifelines.statistics import multivariate_logrank_test
    except ImportError:
        return {}

    cohort_groups: dict[int, list[ProgressionRecord]] = {}
    for rec in records:
        cohort_groups.setdefault(rec.cohort_5y, []).append(rec)

    durations_all: list[float] = []
    events_all: list[bool] = []
    groups_all: list[int] = []

    for cohort_year, cohort_recs in cohort_groups.items():
        if len(cohort_recs) < min_cohort_size:
            continue
        for rec in cohort_recs:
            if rec.duration_years is not None:
                durations_all.append(rec.duration_years)
                events_all.append(True)
            else:
                durations_all.append(25.0)
                events_all.append(False)
            groups_all.append(cohort_year)

    if len(set(groups_all)) < 2:
        return {}

    try:
        result = multivariate_logrank_test(
            durations_all, groups_all, event_observed=events_all
        )
        return {
            "p_value": float(result.p_value),
            "test_statistic": float(result.test_statistic),
            "method": "multivariate_logrank_test (lifelines)",
            "n_cohorts": len(set(groups_all)),
        }
    except Exception as exc:
        log.warning("logrank_test_failed", error=str(exc))
        return {}


# ---------------------------------------------------------------------------
# Step 3: Studio blockage score
# ---------------------------------------------------------------------------


def _studio_affiliation_by_person(
    conn: sqlite3.Connection,
) -> dict[str, str]:
    """Return primary studio affiliation per person.

    Primary = studio with the most credits for that person.
    Uses the anime.studio_id field via the credits → anime join.
    Returns empty dict if studio data is unavailable.
    """
    sql = """
        SELECT c.person_id, a.studio_id, COUNT(*) AS cnt
        FROM credits c
        JOIN anime a ON c.anime_id = a.id
        WHERE a.studio_id IS NOT NULL AND a.studio_id != ''
        GROUP BY c.person_id, a.studio_id
    """
    try:
        rows = conn.execute(sql).fetchall()
    except Exception as exc:
        log.warning("studio_affiliation_query_failed", error=str(exc))
        return {}

    # Keep max-count studio per person
    best: dict[str, tuple[str, int]] = {}
    for person_id, studio_id, cnt in rows:
        prev = best.get(person_id)
        if prev is None or cnt > prev[1]:
            best[person_id] = (studio_id, cnt)

    return {pid: val[0] for pid, val in best.items()}


def compute_studio_blockage(
    conn: sqlite3.Connection,
    role_from: str = "in_between",
    role_to: str = "key_animator",
    *,
    n_bootstrap: int = 1000,
    rng_seed: int = 42,
    min_studio_persons: int = 5,
) -> list[StudioBlockageRow]:
    """Compute studio-level pipeline blockage score relative to industry median.

    blockage_score = studio_median(progression_years) - industry_median(progression_years)

    Positive values indicate slower-than-industry progression (blockage).
    Negative values indicate faster progression (early advancement).
    95% CI computed via bootstrap (n_bootstrap resamples).

    Args:
        conn: SILVER-layer SQLite connection.
        role_from: Source role in the progression pair.
        role_to: Target role in the progression pair.
        n_bootstrap: Number of bootstrap resamples for CI.
        rng_seed: Random seed for reproducibility.
        min_studio_persons: Studios with fewer observed progressors are excluded.

    Returns:
        List of StudioBlockageRow sorted by blockage_score descending.
    """
    records = compute_progression_years(conn, role_from, role_to)
    studio_map = _studio_affiliation_by_person(conn)

    # Observed durations only (censored excluded from median calculation)
    observed = [r for r in records if r.duration_years is not None]
    if not observed:
        log.warning("no_observed_progressions", role_from=role_from, role_to=role_to)
        return []

    all_durations = np.array([r.duration_years for r in observed], dtype=float)
    industry_median = float(np.median(all_durations))

    # Group by studio
    studio_durations: dict[str, list[float]] = {}
    for rec in observed:
        sid = studio_map.get(rec.person_id)
        if sid is None:
            continue
        studio_durations.setdefault(sid, []).append(rec.duration_years)  # type: ignore[arg-type]

    rng = np.random.default_rng(rng_seed)
    rows: list[StudioBlockageRow] = []

    for studio_id, durations in studio_durations.items():
        n = len(durations)
        if n < min_studio_persons:
            continue

        arr = np.array(durations, dtype=float)
        studio_median = float(np.median(arr))
        blockage_score = studio_median - industry_median

        # Bootstrap CI on blockage_score
        bootstrap_scores = np.array([
            np.median(rng.choice(arr, size=n, replace=True)) - industry_median
            for _ in range(n_bootstrap)
        ])
        ci_low = float(np.percentile(bootstrap_scores, 2.5))
        ci_high = float(np.percentile(bootstrap_scores, 97.5))

        rows.append(
            StudioBlockageRow(
                studio_id=studio_id,
                n_persons=n,
                studio_median=studio_median,
                industry_median=industry_median,
                blockage_score=blockage_score,
                ci_low=ci_low,
                ci_high=ci_high,
            )
        )

    rows.sort(key=lambda r: r.blockage_score, reverse=True)

    log.info(
        "studio_blockage_computed",
        role_from=role_from,
        role_to=role_to,
        n_studios=len(rows),
        industry_median=industry_median,
    )
    return rows


# ---------------------------------------------------------------------------
# Cohort funnel helper
# ---------------------------------------------------------------------------


def compute_role_counts(
    conn: sqlite3.Connection,
    roles: list[str] | None = None,
) -> dict[str, int]:
    """Return distinct person counts per role from the credits SILVER table.

    Args:
        conn: SILVER-layer SQLite connection.
        roles: Roles to query.  Defaults to PIPELINE_ROLES.

    Returns:
        Dict mapping role → distinct person count.
    """
    if roles is None:
        roles = PIPELINE_ROLES

    counts: dict[str, int] = {}
    for role in roles:
        row = conn.execute(
            "SELECT COUNT(DISTINCT person_id) FROM credits WHERE role = ?", (role,)
        ).fetchone()
        counts[role] = int(row[0]) if row else 0

    return counts
