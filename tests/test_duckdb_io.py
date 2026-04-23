"""Tests for DuckDB read-path functions (Phase A PoC)."""

import pytest

from src.database import get_connection, init_db


@pytest.fixture()
def small_db(tmp_path, monkeypatch):
    """SQLite DB with a small dataset for DuckDB round-trip tests."""
    import src.database as dm

    db_path = tmp_path / "duckdb_test.db"
    monkeypatch.setattr(dm, "DEFAULT_DB_PATH", db_path)

    conn = get_connection()
    init_db(conn)

    conn.executemany(
        "INSERT OR IGNORE INTO persons (id, name_ja, name_en) VALUES (?,?,?)",
        [("p0", "人物0", "Person 0"), ("p1", "人物1", "Person 1"), ("p2", "人物2", "Person 2")],
    )
    conn.executemany(
        "INSERT OR IGNORE INTO anime (id, title_ja, title_en, year, episodes) VALUES (?,?,?,?,?)",
        [("a1", "テストアニメ", "Test Anime", 2020, 12), ("a2", "別アニメ", "Another Anime", 2021, 24)],
    )
    conn.executemany(
        "INSERT OR IGNORE INTO credits (person_id, anime_id, role, raw_role, evidence_source, credit_year)"
        " VALUES (?,?,?,?,?,?)",
        [
            ("p0", "a1", "director", "", "test", 2020),
            ("p1", "a1", "key_animator", "", "test", 2020),
            ("p2", "a2", "director", "", "test", 2021),
        ],
    )
    conn.executemany(
        "INSERT OR IGNORE INTO anime_genres (anime_id, genre_name) VALUES (?,?)",
        [("a1", "Action"), ("a1", "Drama")],
    )
    conn.execute("INSERT OR IGNORE INTO studios (id, name) VALUES (1,'Studio A')")
    conn.execute(
        "INSERT OR IGNORE INTO anime_studios (anime_id, studio_id, is_main) VALUES ('a1',1,1)"
    )

    conn.commit()
    conn.close()

    return db_path


class TestLoadCreditsDdb:
    def test_returns_all_rows(self, small_db):
        from src.analysis.duckdb_io import load_credits_ddb

        rows = load_credits_ddb(small_db)
        assert len(rows) == 3

    def test_returns_dicts(self, small_db):
        from src.analysis.duckdb_io import load_credits_ddb

        rows = load_credits_ddb(small_db)
        assert all(isinstance(r, dict) for r in rows)

    def test_person_id_present(self, small_db):
        from src.analysis.duckdb_io import load_credits_ddb

        rows = load_credits_ddb(small_db)
        pids = {r["person_id"] for r in rows}
        assert pids == {"p0", "p1", "p2"}



class TestLoadAnimeJoinedDdb:
    def test_returns_all_anime(self, small_db):
        from src.analysis.duckdb_io import load_anime_joined_ddb

        rows = load_anime_joined_ddb(small_db)
        assert len(rows) == 2

    def test_genres_aggregated(self, small_db):
        from src.analysis.duckdb_io import load_anime_joined_ddb

        rows = load_anime_joined_ddb(small_db)
        a1 = next(r for r in rows if r["id"] == "a1")
        assert set(a1["genres"]) == {"Action", "Drama"}

    def test_studios_aggregated(self, small_db):
        from src.analysis.duckdb_io import load_anime_joined_ddb

        rows = load_anime_joined_ddb(small_db)
        a1 = next(r for r in rows if r["id"] == "a1")
        assert "Studio A" in a1["studios"]

    def test_no_genres_returns_empty_list(self, small_db):
        from src.analysis.duckdb_io import load_anime_joined_ddb

        rows = load_anime_joined_ddb(small_db)
        a2 = next(r for r in rows if r["id"] == "a2")
        assert a2["genres"] == []

    def test_title_preserved(self, small_db):
        from src.analysis.duckdb_io import load_anime_joined_ddb

        rows = load_anime_joined_ddb(small_db)
        a1 = next(r for r in rows if r["id"] == "a1")
        assert a1["title_en"] == "Test Anime"


class TestAggCreditsPerPersonDdb:
    def test_groups_by_person_year_role(self, small_db):
        from src.analysis.duckdb_io import agg_credits_per_person_ddb

        rows = agg_credits_per_person_ddb(small_db)
        assert len(rows) == 3  # one per (person_id, credit_year, role)

    def test_row_keys(self, small_db):
        from src.analysis.duckdb_io import agg_credits_per_person_ddb

        rows = agg_credits_per_person_ddb(small_db)
        for r in rows:
            assert "person_id" in r
            assert "credit_year" in r
            assert "role" in r
            assert "n_works" in r
            assert "n_credits" in r

    def test_counts_correct(self, small_db):
        from src.analysis.duckdb_io import agg_credits_per_person_ddb

        rows = agg_credits_per_person_ddb(small_db)
        p0 = next(r for r in rows if r["person_id"] == "p0")
        assert p0["n_works"] == 1
        assert p0["n_credits"] == 1

