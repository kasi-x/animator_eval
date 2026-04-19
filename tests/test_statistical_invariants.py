"""Statistical invariant tests for gold/meta tables (Phase 4 Task 4-7 / V-4).

These tests check properties that must hold by construction, independent
of the exact data — e.g.:

* null-model p-values must follow a uniform distribution
* meta_policy_attrition's ATE must have the theoretical sign
* percentile columns in meta_common_person_parameters must be in [0, 100]

Tests are marked with ``requires_meta_tables`` and skip cleanly if the
corresponding tables/columns do not exist in the current database. This
lets CI run before Phase 1 populates the meta_* tables.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


# Module-level marker — individual sanity tests below opt out explicitly.
requires_meta_tables = pytest.mark.requires_meta_tables


def _resolve_db_path() -> Path | None:
    try:
        from src.utils.config import DB_PATH  # type: ignore[import-not-found]
        p = Path(DB_PATH)
        return p if p.exists() else None
    except Exception:
        return None


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone() is not None


def _column_names(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


@pytest.fixture(scope="module")
def conn() -> sqlite3.Connection:
    db_path = _resolve_db_path()
    if db_path is None:
        pytest.skip("no populated DB available for invariant tests")
    c = sqlite3.connect(str(db_path))
    try:
        yield c
    finally:
        c.close()


@requires_meta_tables
def test_null_model_pvalues_uniform(conn: sqlite3.Connection) -> None:
    """Null-model p-values should be approximately uniform on [0, 1].

    Iterates over every meta_* table, looking for a ``null_model_p``
    column. For each column with >= 30 non-null values, runs a KS test
    against the uniform distribution. Fails if p < 0.01 (very strong
    evidence of non-uniformity — indicates a bug in the null-model
    implementation, not a finding).
    """
    try:
        from scipy.stats import kstest
    except ImportError:
        pytest.skip("scipy not available")

    tables = [
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'meta_%'"
        )
    ]
    if not tables:
        pytest.skip("no meta_* tables exist yet")

    checked = 0
    failures: list[str] = []
    for t in tables:
        cols = _column_names(conn, t)
        if "null_model_p" not in cols:
            continue
        rows = conn.execute(
            f"SELECT null_model_p FROM {t} WHERE null_model_p IS NOT NULL"
        ).fetchall()
        values = [float(r[0]) for r in rows]
        if len(values) < 30:
            continue
        if any(v < 0 or v > 1 for v in values):
            failures.append(f"{t}.null_model_p out of [0,1] range")
            continue
        _stat, p = kstest(values, "uniform")
        checked += 1
        # Threshold is intentionally loose — we want to flag broken
        # implementations, not reject real distributional noise.
        if p < 0.01:
            failures.append(f"{t}.null_model_p non-uniform (KS p={p:.4g})")

    if not checked:
        pytest.skip("no null_model_p columns populated")
    assert not failures, "Null-model uniformity violations: " + "; ".join(failures)


@requires_meta_tables
def test_meta_policy_attrition_ate_sign(conn: sqlite3.Connection) -> None:
    """meta_policy_attrition's treatment effect must have the expected sign.

    Theoretical expectation (per §1.4 V-4 spec): policies labelled with
    ``expected_sign IN ('positive', 'negative')`` must have ATE whose sign
    matches. If the ``meta_policy_attrition`` table does not yet exist we
    skip with a clear marker rather than fail.
    """
    if not _table_exists(conn, "meta_policy_attrition"):
        pytest.skip("meta_policy_attrition not yet created (Phase 1 pending)")
    cols = _column_names(conn, "meta_policy_attrition")
    required = {"ate", "expected_sign"}
    missing = required - set(cols)
    if missing:
        pytest.skip(f"meta_policy_attrition missing columns {missing}")

    rows = conn.execute(
        """
        SELECT ate, expected_sign
        FROM meta_policy_attrition
        WHERE ate IS NOT NULL AND expected_sign IS NOT NULL
        """
    ).fetchall()
    if not rows:
        pytest.skip("meta_policy_attrition is empty")

    violations: list[str] = []
    for ate, sign in rows:
        ate_f = float(ate)
        if sign == "positive" and ate_f < 0:
            violations.append(f"ate={ate_f} with expected_sign=positive")
        elif sign == "negative" and ate_f > 0:
            violations.append(f"ate={ate_f} with expected_sign=negative")
    assert not violations, "ATE sign mismatches: " + "; ".join(violations)


@requires_meta_tables
def test_meta_common_person_parameters_pct_range(conn: sqlite3.Connection) -> None:
    """Every *_pct column in meta_common_person_parameters must be in [0, 100]."""
    if not _table_exists(conn, "meta_common_person_parameters"):
        pytest.skip("meta_common_person_parameters not yet created")

    cols = _column_names(conn, "meta_common_person_parameters")
    pct_cols = [c for c in cols if c.endswith("_pct") or c.endswith("_percentile")]
    if not pct_cols:
        pytest.skip("no percentile columns in meta_common_person_parameters")

    failures: list[str] = []
    for col in pct_cols:
        row = conn.execute(
            f"SELECT MIN({col}), MAX({col}), COUNT({col}) "
            f"FROM meta_common_person_parameters "
            f"WHERE {col} IS NOT NULL"
        ).fetchone()
        lo, hi, n = row
        if n == 0:
            continue
        if lo is None or hi is None:
            continue
        if lo < -1e-9 or hi > 100 + 1e-9:
            failures.append(f"{col}: [{lo}, {hi}]")
    assert not failures, "Percentile columns out of [0,100]: " + "; ".join(failures)


# ---------------------------------------------------------------------------
# Sanity tests that always run (no meta_* requirement) — validate the
# statistical-invariant test infrastructure itself.
# ---------------------------------------------------------------------------


def test_scipy_kstest_uniform_sanity() -> None:
    """Sanity: a truly uniform sample should not fail the KS gate."""
    try:
        import numpy as np
        from scipy.stats import kstest
    except ImportError:
        pytest.skip("scipy/numpy not available")
    rng = np.random.default_rng(42)
    sample = rng.uniform(0, 1, size=500).tolist()
    _stat, p = kstest(sample, "uniform")
    assert p > 0.01, "uniform sample failed KS self-check"
