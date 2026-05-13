"""Cross-source disagreement metrics + CUSUM drift detection for entity resolution monitoring.

# Architecture note (5-tier model)
# This module is a *special-case* that reads from TWO layers:
#   - Conformed (animetor.duckdb `conformed` schema): source of cross-source disagreement
#   - Mart      (animetor.duckdb `mart` schema):     write destination for audit snapshots
# Normal analysis modules read Resolved-only. This module explicitly audits the
# Conformed layer for disagreement *before* resolution, which is its stated purpose.
# See CLAUDE.md §Architecture / Five-Tier Database Model.

For each attribute (gender / hometown / birthday / role_label) we count person-level
source disagreements among non-null values, then store a weekly snapshot in
mart.meta_resolution_audit_weekly.

CUSUM (Page's cumulative sum test) detects monotone drift in disagreement rate.
When the CUSUM statistic exceeds `cusum_threshold` the module logs an alert;
the alert is the signal to schedule re-scrape or human review.
No automatic entity-resolution modifications are performed (H3 hard constraint).

Usage::

    from src.analysis.quality.resolution_drift import (
        compute_disagreement_metrics,
        run_cusum,
        CUSUMResult,
    )
    metrics = compute_disagreement_metrics(conformed_path)
    for attr, rate, n in metrics:
        result = run_cusum(history_rates=[...], new_rate=rate)
        if result.alert:
            log.warning("drift_alert", attribute=attr, cusum=result.cusum_value)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import structlog

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

#: Attributes monitored for cross-source disagreement.
MONITORED_ATTRIBUTES: tuple[str, ...] = (
    "gender",
    "hometown",
    "birthday",
    "role_label",
)

#: CUSUM allowance parameter k — half the minimum detectable shift in units of
#: disagreement rate (0–1). Set to 0.02 (2 pp) so shifts of 4 pp trigger alert.
CUSUM_ALLOWANCE: float = 0.02

#: CUSUM decision threshold h. Alert fires when S_t >= h.
#: 0.10 corresponds to ~5 consecutive 4-pp upward shifts before alarm.
CUSUM_THRESHOLD: float = 0.10


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DisagreementRow:
    """Single attribute disagreement observation for one source pair."""

    source_pair: str
    attribute: str
    disagreement_rate: float
    n_comparable: int


@dataclass(frozen=True)
class CUSUMResult:
    """Output of a single CUSUM update step."""

    cusum_value: float
    alert: bool
    n_history: int


# ---------------------------------------------------------------------------
# Conformed-layer disagreement computation
# ---------------------------------------------------------------------------

# SQL template: for each attribute, find persons present in >=2 sources and
# count those where at least two sources disagree (non-null values differ).
# Conformed schema uses `source_id` prefix like `anilist:123`, so we extract
# the source label from the id prefix via split(':')[0].
#
# The query strategy:
#   1. Group by canonical_person_id (from resolved audit or persons.canonical_id).
#      Because Conformed keeps source rows with `<source>:<id>` primary keys we
#      aggregate across persons that share the same canonical_id field.
#   2. Within each canonical group, collect distinct non-null attribute values
#      per (canonical_person, source_label) pair.
#   3. A disagreement = two sources provide non-null values that differ.

_PERSONS_AGREE_SQL = """
-- Cross-source gender disagreement in Conformed persons table.
-- Source label is extracted from persons.id prefix (e.g. 'anilist:12345' → 'anilist').
-- Only considers persons where canonical_id is set (entity resolution candidate).
WITH sourced AS (
    SELECT
        canonical_id,
        split_part(id, ':', 1) AS source_label,
        {col}                  AS attr_value
    FROM conformed.persons
    WHERE canonical_id IS NOT NULL
      AND {col} IS NOT NULL
      AND {col} != ''
),
grouped AS (
    SELECT
        canonical_id,
        COUNT(DISTINCT source_label)              AS n_sources,
        COUNT(DISTINCT attr_value)                AS n_distinct_values,
        COUNT(*)                                  AS n_observations
    FROM sourced
    GROUP BY canonical_id
    HAVING n_sources >= 2
)
SELECT
    COUNT(*)                                                           AS n_comparable,
    SUM(CASE WHEN n_distinct_values > 1 THEN 1 ELSE 0 END)            AS n_disagreements
FROM grouped
"""

_CREDITS_AGREE_SQL = """
-- Cross-source role_label disagreement: same (person_id, anime_id) pair credited
-- under different role labels by different evidence sources.
WITH sourced AS (
    SELECT
        person_id,
        anime_id,
        split_part(evidence_source, ':', 1) AS source_label,
        role                                AS attr_value
    FROM conformed.credits
    WHERE evidence_source IS NOT NULL
      AND evidence_source != ''
      AND role IS NOT NULL
      AND role != ''
),
grouped AS (
    SELECT
        person_id,
        anime_id,
        COUNT(DISTINCT source_label)   AS n_sources,
        COUNT(DISTINCT attr_value)     AS n_distinct_values
    FROM sourced
    GROUP BY person_id, anime_id
    HAVING n_sources >= 2
)
SELECT
    COUNT(*)                                                           AS n_comparable,
    SUM(CASE WHEN n_distinct_values > 1 THEN 1 ELSE 0 END)            AS n_disagreements
FROM grouped
"""

_PERSON_ATTR_COLS: dict[str, str] = {
    "gender": "gender",
    "hometown": "hometown",
    "birthday": "date_of_birth",
}


def _safe_rate(n_disagreements: int, n_comparable: int) -> float:
    """Return disagreement_rate as float in [0, 1]; 0.0 when n_comparable == 0."""
    if n_comparable == 0:
        return 0.0
    return n_disagreements / n_comparable


def _column_exists(conn, table_fqn: str, column: str) -> bool:
    """Return True if column exists in table (DuckDB: DESCRIBE returns column names)."""
    try:
        rows = conn.execute(f"DESCRIBE {table_fqn}").fetchall()
        names = {r[0].lower() for r in rows}
        return column.lower() in names
    except Exception:
        return False


def compute_disagreement_metrics(
    conformed_path: Path | str | None = None,
    *,
    source_pair: str = "all_sources",
) -> list[DisagreementRow]:
    """Query Conformed layer and return cross-source disagreement rates.

    Args:
        conformed_path: Path to animetor.duckdb. Uses DEFAULT_DB_PATH when None.
        source_pair: Label stored in the audit table identifying which source
            combination was evaluated. Default 'all_sources'.

    Returns:
        List of DisagreementRow, one per monitored attribute.
        Returns empty list when the Conformed schema is absent (graceful degradation).
    """
    import duckdb
    from src.analysis.io.conformed_reader import DEFAULT_DB_PATH

    path = str(conformed_path or DEFAULT_DB_PATH)
    results: list[DisagreementRow] = []

    try:
        conn = duckdb.connect(path, read_only=True)
    except Exception as exc:
        log.warning("resolution_drift_db_open_failed", path=path, error=str(exc))
        return results

    try:
        # Check conformed schema exists by probing conformed.persons
        try:
            conn.execute("SELECT 1 FROM conformed.persons LIMIT 0")
        except Exception:
            log.info("conformed_schema_absent", path=path)
            return results

        # Person-level attributes: gender / hometown / birthday
        for attr, col in _PERSON_ATTR_COLS.items():
            if not _column_exists(conn, "conformed.persons", col):
                log.debug("conformed_column_absent", table="persons", column=col)
                results.append(
                    DisagreementRow(
                        source_pair=source_pair,
                        attribute=attr,
                        disagreement_rate=0.0,
                        n_comparable=0,
                    )
                )
                continue

            try:
                sql = _PERSONS_AGREE_SQL.format(col=col)
                row = conn.execute(sql).fetchone()
                n_comparable = int(row[0]) if row and row[0] is not None else 0
                n_disagreements = int(row[1]) if row and row[1] is not None else 0
                rate = _safe_rate(n_disagreements, n_comparable)
                results.append(
                    DisagreementRow(
                        source_pair=source_pair,
                        attribute=attr,
                        disagreement_rate=rate,
                        n_comparable=n_comparable,
                    )
                )
                log.debug(
                    "disagreement_computed",
                    attribute=attr,
                    rate=rate,
                    n=n_comparable,
                )
            except Exception as exc:
                log.warning(
                    "disagreement_query_failed",
                    attribute=attr,
                    error=str(exc),
                )
                results.append(
                    DisagreementRow(
                        source_pair=source_pair,
                        attribute=attr,
                        disagreement_rate=0.0,
                        n_comparable=0,
                    )
                )

        # Credit-level attribute: role_label
        try:
            row = conn.execute(_CREDITS_AGREE_SQL).fetchone()
            n_comparable = int(row[0]) if row and row[0] is not None else 0
            n_disagreements = int(row[1]) if row and row[1] is not None else 0
            rate = _safe_rate(n_disagreements, n_comparable)
            results.append(
                DisagreementRow(
                    source_pair=source_pair,
                    attribute="role_label",
                    disagreement_rate=rate,
                    n_comparable=n_comparable,
                )
            )
            log.debug(
                "disagreement_computed",
                attribute="role_label",
                rate=rate,
                n=n_comparable,
            )
        except Exception as exc:
            log.warning(
                "disagreement_query_failed", attribute="role_label", error=str(exc)
            )
            results.append(
                DisagreementRow(
                    source_pair=source_pair,
                    attribute="role_label",
                    disagreement_rate=0.0,
                    n_comparable=0,
                )
            )

    finally:
        conn.close()

    return results


# ---------------------------------------------------------------------------
# CUSUM drift detection
# ---------------------------------------------------------------------------


def run_cusum(
    history_rates: Sequence[float],
    new_rate: float,
    *,
    allowance: float = CUSUM_ALLOWANCE,
    threshold: float = CUSUM_THRESHOLD,
) -> CUSUMResult:
    """Run Page's one-sided upper CUSUM to detect upward drift in disagreement rate.

    The statistic S_t is updated recursively::

        S_0 = 0
        S_t = max(0, S_{t-1} + (x_t - mu_0) - k)

    where mu_0 is estimated from `history_rates` (mean of past observations),
    k is the `allowance` parameter (half the minimum detectable shift), and
    x_t is `new_rate`.

    An alert fires when S_t >= `threshold`.

    Args:
        history_rates: Past disagreement rates for this attribute (chronological).
            When empty the baseline mu_0 is set to `new_rate` (no drift possible
            on first observation).
        new_rate: Newly observed disagreement rate to test.
        allowance: CUSUM slack parameter k (default CUSUM_ALLOWANCE = 0.02).
        threshold: Decision threshold h (default CUSUM_THRESHOLD = 0.10).

    Returns:
        CUSUMResult with cusum_value, alert flag, and n_history.
    """
    if not history_rates:
        # First-ever snapshot: no reference baseline, cannot declare drift.
        return CUSUMResult(cusum_value=0.0, alert=False, n_history=0)

    mu_0 = sum(history_rates) / len(history_rates)

    # Replay CUSUM over history to get S_{t-1}
    s = 0.0
    for x in history_rates:
        s = max(0.0, s + (x - mu_0) - allowance)

    # Update with new observation
    s = max(0.0, s + (new_rate - mu_0) - allowance)
    s = _round6(s)

    alert = s >= threshold
    if alert:
        log.warning(
            "resolution_drift_alert",
            cusum_value=s,
            threshold=threshold,
            new_rate=new_rate,
            baseline_mean=mu_0,
            n_history=len(history_rates),
        )

    return CUSUMResult(
        cusum_value=s,
        alert=alert,
        n_history=len(history_rates),
    )


def _round6(x: float) -> float:
    """Round to 6 decimal places to avoid floating-point accumulation noise."""
    return round(x, 6)


# ---------------------------------------------------------------------------
# Mart-layer persistence helpers
# ---------------------------------------------------------------------------

_MART_AUDIT_WEEKLY_DDL = """
CREATE TABLE IF NOT EXISTS meta_resolution_audit_weekly (
    week_start   DATE    NOT NULL,
    source_pair  TEXT    NOT NULL,
    attribute    TEXT    NOT NULL,
    disagreement_rate DOUBLE NOT NULL,
    n_comparable      BIGINT NOT NULL,
    cusum_value       DOUBLE NOT NULL DEFAULT 0.0,
    alert_fired       BOOLEAN NOT NULL DEFAULT FALSE,
    computed_at       TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (week_start, source_pair, attribute)
);
"""

_MART_AUDIT_WEEKLY_INDEX = """
CREATE INDEX IF NOT EXISTS idx_mraw_attr_week
    ON meta_resolution_audit_weekly (attribute, week_start);
"""


def ensure_audit_weekly_table(conn) -> None:
    """Create mart.meta_resolution_audit_weekly if absent.

    Caller is responsible for setting schema='mart' on the connection.
    """
    conn.execute(_MART_AUDIT_WEEKLY_DDL)
    try:
        conn.execute(_MART_AUDIT_WEEKLY_INDEX)
    except Exception:
        pass  # index may already exist


def write_snapshot(
    conn,
    week_start: str,
    rows: list[DisagreementRow],
    cusum_results: dict[str, CUSUMResult],
) -> int:
    """Upsert snapshot rows into mart.meta_resolution_audit_weekly.

    Args:
        conn: Open DuckDB connection with schema='mart'.
        week_start: ISO date string (YYYY-MM-DD) for the snapshot week.
        rows: Disagreement metrics from compute_disagreement_metrics().
        cusum_results: Map of attribute -> CUSUMResult from run_cusum().

    Returns:
        Number of rows upserted.
    """
    ensure_audit_weekly_table(conn)
    if not rows:
        return 0

    records = []
    for row in rows:
        cr = cusum_results.get(row.attribute, CUSUMResult(0.0, False, 0))
        records.append(
            (
                week_start,
                row.source_pair,
                row.attribute,
                row.disagreement_rate,
                row.n_comparable,
                cr.cusum_value,
                cr.alert,
            )
        )

    conn.executemany(
        """
        INSERT INTO meta_resolution_audit_weekly
            (week_start, source_pair, attribute, disagreement_rate,
             n_comparable, cusum_value, alert_fired)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (week_start, source_pair, attribute)
        DO UPDATE SET
            disagreement_rate = excluded.disagreement_rate,
            n_comparable      = excluded.n_comparable,
            cusum_value       = excluded.cusum_value,
            alert_fired       = excluded.alert_fired,
            computed_at       = now()
        """,
        records,
    )
    return len(records)


def load_history_rates(
    conn,
    attribute: str,
    *,
    source_pair: str = "all_sources",
    max_weeks: int = 52,
) -> list[float]:
    """Load past disagreement rates for CUSUM baseline.

    Args:
        conn: Open DuckDB connection with schema='mart'.
        attribute: Attribute name (e.g. 'gender').
        source_pair: Source-pair label.
        max_weeks: How many past weeks to load (default 52 = 1 year).

    Returns:
        List of rates in chronological order (oldest first).
    """
    try:
        ensure_audit_weekly_table(conn)
        rows = conn.execute(
            """
            SELECT disagreement_rate
            FROM meta_resolution_audit_weekly
            WHERE attribute   = ?
              AND source_pair = ?
            ORDER BY week_start ASC
            LIMIT ?
            """,
            [attribute, source_pair, max_weeks],
        ).fetchall()
        return [float(r[0]) for r in rows]
    except Exception as exc:
        log.warning("load_history_failed", attribute=attribute, error=str(exc))
        return []
