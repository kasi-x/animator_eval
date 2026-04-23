"""Tests for DuckDB-native feat_precompute functions (§4.4)."""

from __future__ import annotations

import duckdb
import pytest


class TestComputeFeatCreditActivity:
    def test_returns_row_count(self, silver_gold_dbs):
        from src.analysis.feat_precompute import compute_feat_credit_activity_ddb

        n = compute_feat_credit_activity_ddb(current_year=2025, current_quarter=1)
        assert n > 0

    def test_rows_written_to_gold(self, silver_gold_dbs):
        from src.analysis.feat_precompute import compute_feat_credit_activity_ddb

        compute_feat_credit_activity_ddb(current_year=2025, current_quarter=1)
        with duckdb.connect(str(silver_gold_dbs)) as g:
            count = g.execute(
                "SELECT COUNT(*) FROM feat_credit_activity"
            ).fetchone()[0]
        assert count == 2  # p1 and p2

    def test_p1_has_gap_data(self, silver_gold_dbs):
        from src.analysis.feat_precompute import compute_feat_credit_activity_ddb

        compute_feat_credit_activity_ddb(current_year=2025, current_quarter=1)
        with duckdb.connect(str(silver_gold_dbs)) as g:
            row = g.execute(
                "SELECT n_gaps, active_years FROM feat_credit_activity WHERE person_id = 'p1'"
            ).fetchone()
        assert row is not None
        assert row[1] == 3  # 2015, 2016, 2018


class TestComputeFeatCareerAnnual:
    def test_returns_row_count(self, silver_gold_dbs):
        from src.analysis.feat_precompute import compute_feat_career_annual_ddb

        n = compute_feat_career_annual_ddb()
        assert n > 0

    def test_rows_in_gold(self, silver_gold_dbs):
        from src.analysis.feat_precompute import compute_feat_career_annual_ddb

        compute_feat_career_annual_ddb()
        with duckdb.connect(str(silver_gold_dbs)) as g:
            count = g.execute(
                "SELECT COUNT(*) FROM feat_career_annual"
            ).fetchone()[0]
        assert count > 0

    def test_career_year_starts_at_zero(self, silver_gold_dbs):
        from src.analysis.feat_precompute import compute_feat_career_annual_ddb

        compute_feat_career_annual_ddb()
        with duckdb.connect(str(silver_gold_dbs)) as g:
            row = g.execute(
                "SELECT MIN(career_year) FROM feat_career_annual WHERE person_id = 'p1'"
            ).fetchone()
        assert row[0] == 0

    def test_works_direction_populated(self, silver_gold_dbs):
        from src.analysis.feat_precompute import compute_feat_career_annual_ddb

        compute_feat_career_annual_ddb()
        with duckdb.connect(str(silver_gold_dbs)) as g:
            row = g.execute(
                "SELECT SUM(works_direction) FROM feat_career_annual WHERE person_id = 'p1'"
            ).fetchone()
        assert row[0] >= 1  # p1 has Director credits


class TestComputeFeatPersonRoleProgression:
    def test_returns_row_count(self, silver_gold_dbs):
        from src.analysis.feat_precompute import compute_feat_person_role_progression_ddb

        n = compute_feat_person_role_progression_ddb(current_year=2025)
        assert n > 0

    def test_rows_in_gold(self, silver_gold_dbs):
        from src.analysis.feat_precompute import compute_feat_person_role_progression_ddb

        compute_feat_person_role_progression_ddb(current_year=2025)
        with duckdb.connect(str(silver_gold_dbs)) as g:
            count = g.execute(
                "SELECT COUNT(*) FROM feat_person_role_progression"
            ).fetchone()[0]
        assert count > 0

    def test_p1_direction_category(self, silver_gold_dbs):
        from src.analysis.feat_precompute import compute_feat_person_role_progression_ddb

        compute_feat_person_role_progression_ddb(current_year=2025)
        with duckdb.connect(str(silver_gold_dbs)) as g:
            row = g.execute(
                """SELECT first_year, last_year FROM feat_person_role_progression
                   WHERE person_id = 'p1' AND role_category = 'direction'"""
            ).fetchone()
        assert row is not None
        assert row[0] == 2015
        assert row[1] == 2016

    def test_still_active_flag(self, silver_gold_dbs):
        from src.analysis.feat_precompute import compute_feat_person_role_progression_ddb

        compute_feat_person_role_progression_ddb(current_year=2019, active_threshold_years=3)
        with duckdb.connect(str(silver_gold_dbs)) as g:
            row = g.execute(
                """SELECT still_active FROM feat_person_role_progression
                   WHERE person_id = 'p1' AND role_category = 'direction'"""
            ).fetchone()
        # p1 direction last_year=2016, current=2019, diff=3 → still_active=1
        assert row[0] == 1


@pytest.fixture
def silver_gold_with_studios(tmp_path, monkeypatch):
    """silver.duckdb + gold.duckdb including studios + anime_studios tables."""
    import src.analysis.io.silver_reader as sr
    import src.analysis.io.gold_writer as gw

    silver_path = tmp_path / "silver.duckdb"
    gold_path = tmp_path / "gold.duckdb"

    with duckdb.connect(str(silver_path)) as c:
        c.execute("""
            CREATE TABLE anime (id TEXT PRIMARY KEY, title TEXT)
        """)
        c.execute("""
            CREATE TABLE persons (id TEXT PRIMARY KEY, name_ja TEXT, name_en TEXT)
        """)
        c.execute("""
            CREATE TABLE credits (
                person_id TEXT, anime_id TEXT, role TEXT,
                credit_year INTEGER, credit_quarter INTEGER, source TEXT
            )
        """)
        c.execute("""
            CREATE TABLE studios (
                id TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '',
                anilist_id INTEGER, is_animation_studio BOOLEAN,
                country_of_origin TEXT, favourites INTEGER,
                site_url TEXT, updated_at TIMESTAMP
            )
        """)
        c.execute("""
            CREATE TABLE anime_studios (
                anime_id TEXT NOT NULL, studio_id TEXT NOT NULL,
                is_main BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY (anime_id, studio_id)
            )
        """)
        c.executemany("INSERT INTO anime VALUES (?,?)", [("a1", "Anime1"), ("a2", "Anime2")])
        c.executemany("INSERT INTO persons VALUES (?,?,?)", [("p1", "A", "A"), ("p2", "B", "B")])
        c.executemany("INSERT INTO credits VALUES (?,?,?,?,?,?)", [
            ("p1", "a1", "director",     2015, 1, "test"),
            ("p1", "a2", "key_animator", 2016, 2, "test"),
            ("p2", "a1", "key_animator", 2015, 1, "test"),
        ])
        c.execute(
            "INSERT INTO studios VALUES (?,?,?,?,?,?,?,now())",
            ("s1", "スタジオA", 1, True, "JP", 100, None),
        )
        c.executemany("INSERT INTO anime_studios VALUES (?,?,?)", [
            ("a1", "s1", True),
            ("a2", "s1", False),
        ])

    with duckdb.connect(str(gold_path)) as c:
        from src.analysis.io.gold_writer import _DDL
        c.execute(_DDL)

    monkeypatch.setattr(sr, "DEFAULT_SILVER_PATH", silver_path)
    monkeypatch.setattr(gw, "DEFAULT_GOLD_DB_PATH", gold_path)
    return gold_path


class TestComputeFeatStudioAffiliation:
    def test_returns_row_count(self, silver_gold_with_studios):
        from src.analysis.feat_precompute import compute_feat_studio_affiliation_ddb

        n = compute_feat_studio_affiliation_ddb()
        assert n > 0

    def test_rows_written_to_gold(self, silver_gold_with_studios):
        from src.analysis.feat_precompute import compute_feat_studio_affiliation_ddb

        compute_feat_studio_affiliation_ddb()
        with duckdb.connect(str(silver_gold_with_studios)) as g:
            count = g.execute("SELECT COUNT(*) FROM feat_studio_affiliation").fetchone()[0]
        assert count == 3  # p1×2015×s1, p1×2016×s1, p2×2015×s1

    def test_studio_name_populated(self, silver_gold_with_studios):
        from src.analysis.feat_precompute import compute_feat_studio_affiliation_ddb

        compute_feat_studio_affiliation_ddb()
        with duckdb.connect(str(silver_gold_with_studios)) as g:
            row = g.execute(
                "SELECT studio_name FROM feat_studio_affiliation WHERE person_id='p1' AND credit_year=2015"
            ).fetchone()
        assert row is not None
        assert row[0] == "スタジオA"

    def test_is_main_flag(self, silver_gold_with_studios):
        from src.analysis.feat_precompute import compute_feat_studio_affiliation_ddb

        compute_feat_studio_affiliation_ddb()
        with duckdb.connect(str(silver_gold_with_studios)) as g:
            row = g.execute(
                "SELECT is_main_studio FROM feat_studio_affiliation WHERE person_id='p1' AND credit_year=2015"
            ).fetchone()
        # a1 → s1 is_main=True → is_main_studio=1
        assert row is not None
        assert row[0] == 1

    def test_skips_when_anime_studios_absent(self, tmp_path, monkeypatch):
        import src.analysis.io.silver_reader as sr
        import src.analysis.io.gold_writer as gw
        from src.analysis.feat_precompute import compute_feat_studio_affiliation_ddb
        from src.analysis.io.gold_writer import _DDL

        silver_path = tmp_path / "silver_no_studios.duckdb"
        gold_path = tmp_path / "gold_no_studios.duckdb"

        with duckdb.connect(str(silver_path)) as c:
            c.execute("CREATE TABLE credits (person_id TEXT, anime_id TEXT, role TEXT, credit_year INTEGER, credit_quarter INTEGER, source TEXT)")
        with duckdb.connect(str(gold_path)) as c:
            c.execute(_DDL)

        monkeypatch.setattr(sr, "DEFAULT_SILVER_PATH", silver_path)
        monkeypatch.setattr(gw, "DEFAULT_GOLD_DB_PATH", gold_path)

        n = compute_feat_studio_affiliation_ddb()
        assert n == 0
