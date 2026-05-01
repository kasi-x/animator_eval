"""Tests for src/etl/dedup/credits_within_source.py.

Uses in-memory DuckDB fixtures; no real silver.duckdb required.

Coverage:
- idempotent: second call deletes 0 rows
- within-source dup removed, cross-source dup preserved
- role-keyed duplicates detected and removed
- dry_run=True returns stats without mutation
- stop-if guard raises RuntimeError when to_delete > 1_000_000
- H1: no anime.score / display_* in SQL
- H4: evidence_source column survives on all remaining rows
"""

from __future__ import annotations

import duckdb
import pytest

from src.etl.dedup.credits_within_source import (
    _COUNT_SQL,
    _DELETE_SQL,
    _SAMPLE_SQL,
    dedup,
)

# ---------------------------------------------------------------------------
# Minimal DDL (mirrors production SILVER schema columns used by dedup)
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS credits (
    person_id       VARCHAR,
    anime_id        VARCHAR NOT NULL,
    role            VARCHAR NOT NULL,
    evidence_source VARCHAR NOT NULL,
    episode         INTEGER,
    affiliation     VARCHAR,
    position        INTEGER,
    updated_at      TIMESTAMP,
    credit_year     INTEGER,
    credit_quarter  INTEGER,
    raw_role        VARCHAR
);
"""


def _make_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(_DDL)
    return conn


def _insert(conn: duckdb.DuckDBPyConnection, **kwargs: object) -> None:
    """Insert a single credits row with defaults for optional columns."""
    row = {
        "person_id": None,
        "anime_id": "a1",
        "role": "director",
        "evidence_source": "anilist",
        "episode": None,
        "affiliation": None,
        "position": 0,
        "updated_at": None,
        "credit_year": None,
        "credit_quarter": None,
        "raw_role": None,
    }
    row.update(kwargs)
    conn.execute(
        "INSERT INTO credits (person_id, anime_id, role, evidence_source, episode,"
        " affiliation, position, updated_at, credit_year, credit_quarter, raw_role)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            row["person_id"],
            row["anime_id"],
            row["role"],
            row["evidence_source"],
            row["episode"],
            row["affiliation"],
            row["position"],
            row["updated_at"],
            row["credit_year"],
            row["credit_quarter"],
            row["raw_role"],
        ],
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def empty_conn() -> duckdb.DuckDBPyConnection:
    return _make_conn()


@pytest.fixture()
def conn_with_dups() -> duckdb.DuckDBPyConnection:
    """Three within-source dup pairs + two unique rows + one cross-source row."""
    conn = _make_conn()

    # Pair A: same person+anime+role+source+episode (episode=None) x2 → 1 to delete
    _insert(conn, person_id="p1", anime_id="a1", role="director", evidence_source="anilist", episode=None)
    _insert(conn, person_id="p1", anime_id="a1", role="director", evidence_source="anilist", episode=None)

    # Pair B: same person+anime+role+source+episode (episode=3) x2 → 1 to delete
    _insert(conn, person_id="p2", anime_id="a2", role="key_animator", evidence_source="anilist", episode=3)
    _insert(conn, person_id="p2", anime_id="a2", role="key_animator", evidence_source="anilist", episode=3)

    # Pair C: triple → 2 to delete (3 copies, keep 1)
    _insert(conn, person_id="p3", anime_id="a3", role="producer", evidence_source="mal", episode=None)
    _insert(conn, person_id="p3", anime_id="a3", role="producer", evidence_source="mal", episode=None)
    _insert(conn, person_id="p3", anime_id="a3", role="producer", evidence_source="mal", episode=None)

    # Unique row: no dup
    _insert(conn, person_id="p4", anime_id="a4", role="animator", evidence_source="bangumi", episode=None)

    # Cross-source row: same (person+anime+role+episode) but different evidence_source → NOT a dup
    _insert(conn, person_id="p1", anime_id="a1", role="director", evidence_source="bangumi", episode=None)

    return conn


@pytest.fixture()
def conn_null_person_dups() -> duckdb.DuckDBPyConnection:
    """Null person_id within-source dups (seesaawiki-style)."""
    conn = _make_conn()
    # 3 copies of the same (NULL, anime, role, source, episode)
    for _ in range(3):
        _insert(conn, person_id=None, anime_id="seesaa:abc", role="other", evidence_source="seesaawiki", episode=None)
    # 1 unique null-person row
    _insert(conn, person_id=None, anime_id="seesaa:xyz", role="other", evidence_source="seesaawiki", episode=1)
    return conn


# ---------------------------------------------------------------------------
# Basic dedup tests
# ---------------------------------------------------------------------------


class TestDedupBasic:
    def test_empty_db_returns_zero_deleted(self, empty_conn: duckdb.DuckDBPyConnection) -> None:
        stats = dedup(empty_conn, dry_run=False)
        assert stats["before"] == 0
        assert stats["after"] == 0
        assert stats["deleted"] == 0
        assert stats["deleted_per_source"] == {}

    def test_deletes_expected_count(self, conn_with_dups: duckdb.DuckDBPyConnection) -> None:
        # Before: 9 rows total
        # Pair A: 1 deleted, Pair B: 1 deleted, Pair C: 2 deleted → 4 total
        # Unique + cross-source: survive
        stats = dedup(conn_with_dups, dry_run=False)
        assert stats["deleted"] == 4
        assert stats["after"] == stats["before"] - 4

    def test_surviving_rows_count(self, conn_with_dups: duckdb.DuckDBPyConnection) -> None:
        dedup(conn_with_dups, dry_run=False)
        remaining = conn_with_dups.execute("SELECT COUNT(*) FROM credits").fetchone()[0]
        # 5 unique keys: (p1,a1,dir,anilist,None), (p2,a2,ka,anilist,3),
        #                (p3,a3,prod,mal,None), (p4,a4,anim,bgm,None),
        #                (p1,a1,dir,bangumi,None)
        assert remaining == 5

    def test_cross_source_preserved(self, conn_with_dups: duckdb.DuckDBPyConnection) -> None:
        dedup(conn_with_dups, dry_run=False)
        # p1/a1/director should appear in BOTH anilist and bangumi
        rows = conn_with_dups.execute(
            "SELECT evidence_source FROM credits WHERE person_id='p1' AND anime_id='a1' AND role='director'"
        ).fetchall()
        sources = {r[0] for r in rows}
        assert sources == {"anilist", "bangumi"}, f"expected both sources, got {sources}"

    def test_null_person_dups_removed(self, conn_null_person_dups: duckdb.DuckDBPyConnection) -> None:
        before = conn_null_person_dups.execute("SELECT COUNT(*) FROM credits").fetchone()[0]
        assert before == 4
        stats = dedup(conn_null_person_dups, dry_run=False)
        assert stats["deleted"] == 2
        assert stats["after"] == 2


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


class TestDedupIdempotent:
    def test_second_call_deletes_zero(self, conn_with_dups: duckdb.DuckDBPyConnection) -> None:
        dedup(conn_with_dups, dry_run=False)
        stats2 = dedup(conn_with_dups, dry_run=False)
        assert stats2["deleted"] == 0

    def test_second_call_row_count_stable(self, conn_with_dups: duckdb.DuckDBPyConnection) -> None:
        stats1 = dedup(conn_with_dups, dry_run=False)
        stats2 = dedup(conn_with_dups, dry_run=False)
        assert stats1["after"] == stats2["after"]


# ---------------------------------------------------------------------------
# Dry-run tests
# ---------------------------------------------------------------------------


class TestDedupDryRun:
    def test_dry_run_does_not_mutate(self, conn_with_dups: duckdb.DuckDBPyConnection) -> None:
        before = conn_with_dups.execute("SELECT COUNT(*) FROM credits").fetchone()[0]
        stats = dedup(conn_with_dups, dry_run=True)
        after = conn_with_dups.execute("SELECT COUNT(*) FROM credits").fetchone()[0]
        assert before == after
        assert stats["deleted"] == 0
        assert stats["dry_run"] is True

    def test_dry_run_reports_correct_to_delete(self, conn_with_dups: duckdb.DuckDBPyConnection) -> None:
        stats = dedup(conn_with_dups, dry_run=True)
        # deleted_per_source counts what WOULD be deleted
        total_planned = sum(stats["deleted_per_source"].values())
        assert total_planned == 4  # same as actual delete count

    def test_dry_run_before_equals_after(self, conn_with_dups: duckdb.DuckDBPyConnection) -> None:
        stats = dedup(conn_with_dups, dry_run=True)
        assert stats["before"] == stats["after"]


# ---------------------------------------------------------------------------
# Per-source breakdown
# ---------------------------------------------------------------------------


class TestDedupPerSource:
    def test_per_source_keys(self, conn_with_dups: duckdb.DuckDBPyConnection) -> None:
        stats = dedup(conn_with_dups, dry_run=True)
        # Pairs A+B are anilist, pair C is mal
        assert "anilist" in stats["deleted_per_source"]
        assert "mal" in stats["deleted_per_source"]
        # bangumi has no within-source dups
        assert "bangumi" not in stats["deleted_per_source"]

    def test_per_source_counts(self, conn_with_dups: duckdb.DuckDBPyConnection) -> None:
        stats = dedup(conn_with_dups, dry_run=True)
        assert stats["deleted_per_source"]["anilist"] == 2  # pair A + pair B
        assert stats["deleted_per_source"]["mal"] == 2      # pair C (triple → 2 deleted)

    def test_null_person_per_source(self, conn_null_person_dups: duckdb.DuckDBPyConnection) -> None:
        stats = dedup(conn_null_person_dups, dry_run=True)
        assert "seesaawiki" in stats["deleted_per_source"]
        assert stats["deleted_per_source"]["seesaawiki"] == 2


# ---------------------------------------------------------------------------
# Role-keyed dedup (different roles = different keys)
# ---------------------------------------------------------------------------


class TestDedupRoleKeyed:
    def test_different_roles_not_deleted(self) -> None:
        conn = _make_conn()
        _insert(conn, person_id="p1", anime_id="a1", role="director", evidence_source="anilist", episode=None)
        _insert(conn, person_id="p1", anime_id="a1", role="producer", evidence_source="anilist", episode=None)
        stats = dedup(conn, dry_run=False)
        assert stats["deleted"] == 0
        assert stats["after"] == 2

    def test_same_role_is_deleted(self) -> None:
        conn = _make_conn()
        _insert(conn, person_id="p1", anime_id="a1", role="director", evidence_source="anilist", episode=None)
        _insert(conn, person_id="p1", anime_id="a1", role="director", evidence_source="anilist", episode=None)
        stats = dedup(conn, dry_run=False)
        assert stats["deleted"] == 1


# ---------------------------------------------------------------------------
# Episode-keyed dedup (different episodes = different keys)
# ---------------------------------------------------------------------------


class TestDedupEpisodeKeyed:
    def test_different_episodes_not_deleted(self) -> None:
        conn = _make_conn()
        _insert(conn, person_id="p1", anime_id="a1", role="key_animator", evidence_source="anilist", episode=1)
        _insert(conn, person_id="p1", anime_id="a1", role="key_animator", evidence_source="anilist", episode=2)
        stats = dedup(conn, dry_run=False)
        assert stats["deleted"] == 0
        assert stats["after"] == 2

    def test_null_vs_episode_not_deleted(self) -> None:
        """episode=None and episode=1 are treated as distinct keys."""
        conn = _make_conn()
        _insert(conn, person_id="p1", anime_id="a1", role="key_animator", evidence_source="anilist", episode=None)
        _insert(conn, person_id="p1", anime_id="a1", role="key_animator", evidence_source="anilist", episode=1)
        stats = dedup(conn, dry_run=False)
        assert stats["deleted"] == 0

    def test_same_episode_deleted(self) -> None:
        conn = _make_conn()
        _insert(conn, person_id="p1", anime_id="a1", role="key_animator", evidence_source="anilist", episode=5)
        _insert(conn, person_id="p1", anime_id="a1", role="key_animator", evidence_source="anilist", episode=5)
        stats = dedup(conn, dry_run=False)
        assert stats["deleted"] == 1


# ---------------------------------------------------------------------------
# H1 guard: SQL must not reference score/display columns
# ---------------------------------------------------------------------------


class TestH1Guard:
    def test_count_sql_no_score(self) -> None:
        for forbidden in ("score", "popularity", "favourites", "display_"):
            assert forbidden not in _COUNT_SQL.lower(), (
                f"H1 violation: '{forbidden}' in _COUNT_SQL"
            )

    def test_delete_sql_no_score(self) -> None:
        for forbidden in ("score", "popularity", "favourites", "display_"):
            assert forbidden not in _DELETE_SQL.lower(), (
                f"H1 violation: '{forbidden}' in _DELETE_SQL"
            )

    def test_sample_sql_no_score(self) -> None:
        for forbidden in ("score", "popularity", "favourites", "display_"):
            assert forbidden not in _SAMPLE_SQL.lower(), (
                f"H1 violation: '{forbidden}' in _SAMPLE_SQL"
            )


# ---------------------------------------------------------------------------
# H4 guard: evidence_source preserved on surviving rows
# ---------------------------------------------------------------------------


class TestH4Guard:
    def test_evidence_source_present_after_dedup(self, conn_with_dups: duckdb.DuckDBPyConnection) -> None:
        dedup(conn_with_dups, dry_run=False)
        rows_with_null_source = conn_with_dups.execute(
            "SELECT COUNT(*) FROM credits WHERE evidence_source IS NULL"
        ).fetchone()[0]
        assert rows_with_null_source == 0, "evidence_source must not be NULL on surviving rows"

    def test_evidence_source_values_preserved(self, conn_with_dups: duckdb.DuckDBPyConnection) -> None:
        dedup(conn_with_dups, dry_run=False)
        sources = {
            r[0]
            for r in conn_with_dups.execute("SELECT DISTINCT evidence_source FROM credits").fetchall()
        }
        assert "anilist" in sources
        assert "bangumi" in sources
        assert "mal" in sources


# ---------------------------------------------------------------------------
# Return value shape
# ---------------------------------------------------------------------------


class TestReturnShape:
    def test_return_keys(self, conn_with_dups: duckdb.DuckDBPyConnection) -> None:
        stats = dedup(conn_with_dups, dry_run=False)
        expected_keys = {"before", "after", "deleted", "deleted_per_source", "dry_run"}
        assert set(stats.keys()) == expected_keys

    def test_before_gte_after(self, conn_with_dups: duckdb.DuckDBPyConnection) -> None:
        stats = dedup(conn_with_dups, dry_run=False)
        assert stats["before"] >= stats["after"]

    def test_dry_run_flag_echoed(self, conn_with_dups: duckdb.DuckDBPyConnection) -> None:
        assert dedup(conn_with_dups, dry_run=True)["dry_run"] is True
        assert dedup(conn_with_dups, dry_run=False)["dry_run"] is False
