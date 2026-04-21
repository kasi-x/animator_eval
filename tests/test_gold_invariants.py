"""Compatibility entrypoint for statistical invariants (V-4 checklist)."""

from __future__ import annotations

import sqlite3

import pytest

from tests import test_statistical_invariants as tsi


@pytest.fixture(scope="module")
def conn() -> sqlite3.Connection:
    db_path = tsi._resolve_db_path()
    if db_path is None:
        pytest.skip("no populated DB available for invariant tests")
    c = sqlite3.connect(str(db_path))
    try:
        yield c
    finally:
        c.close()


@tsi.requires_meta_tables
def test_null_model_pvalues_uniform(conn: sqlite3.Connection) -> None:
    tsi.test_null_model_pvalues_uniform(conn)


@tsi.requires_meta_tables
def test_meta_policy_attrition_ate_sign(conn: sqlite3.Connection) -> None:
    tsi.test_meta_policy_attrition_ate_sign(conn)


@tsi.requires_meta_tables
def test_meta_common_person_parameters_pct_range(conn: sqlite3.Connection) -> None:
    tsi.test_meta_common_person_parameters_pct_range(conn)
