"""Test v57 metadata population logic."""
import pytest
import duckdb
from pathlib import Path

from src.etl.populate_v57_metadata import populate_studios_country_of_origin


@pytest.fixture
def test_gold_db(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    """Create a test GOLD DB with minimal schema."""
    db_path = tmp_path / "test_gold.duckdb"
    conn = duckdb.connect(str(db_path))

    # Create minimal tables
    conn.execute("""
        CREATE TABLE anime (
            id VARCHAR PRIMARY KEY,
            title_ja VARCHAR,
            country_of_origin VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE studios (
            id VARCHAR PRIMARY KEY,
            name VARCHAR,
            country_of_origin VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE anime_studios (
            anime_id VARCHAR,
            studio_id VARCHAR,
            is_main INTEGER
        )
    """)

    return conn


def test_majority_vote_single_country(test_gold_db: duckdb.DuckDBPyConnection) -> None:
    """Studio with clear majority country."""
    conn = test_gold_db

    # Seed data
    conn.execute(
        "INSERT INTO anime VALUES ('anime:1', 'Title 1', 'JP'), ('anime:2', 'Title 2', 'JP'), ('anime:3', 'Title 3', 'US')"
    )
    conn.execute(
        "INSERT INTO studios VALUES ('studio:1', 'Studio A', NULL)"
    )
    conn.execute(
        "INSERT INTO anime_studios VALUES ('anime:1', 'studio:1', 1), ('anime:2', 'studio:1', 1), ('anime:3', 'studio:1', 0)"
    )

    result = populate_studios_country_of_origin(conn)

    assert result["studios_updated"] == 1
    assert result["studios_populated"] == 1

    row = conn.execute("SELECT country_of_origin FROM studios WHERE id = 'studio:1'").fetchone()
    assert row[0] == "JP"  # JP wins 2-1


def test_tie_broken_alphabetically(test_gold_db: duckdb.DuckDBPyConnection) -> None:
    """Tie between countries resolved alphabetically."""
    conn = test_gold_db

    conn.execute(
        "INSERT INTO anime VALUES ('anime:1', 'Title 1', 'US'), ('anime:2', 'Title 2', 'JP')"
    )
    conn.execute(
        "INSERT INTO studios VALUES ('studio:1', 'Studio B', NULL)"
    )
    conn.execute(
        "INSERT INTO anime_studios VALUES ('anime:1', 'studio:1', 1), ('anime:2', 'studio:1', 1)"
    )

    result = populate_studios_country_of_origin(conn)

    assert result["studios_updated"] == 1

    row = conn.execute("SELECT country_of_origin FROM studios WHERE id = 'studio:1'").fetchone()
    assert row[0] == "JP"  # JP < US alphabetically


def test_skip_null_and_empty_countries(test_gold_db: duckdb.DuckDBPyConnection) -> None:
    """Null/empty country_of_origin in anime are ignored."""
    conn = test_gold_db

    conn.execute(
        "INSERT INTO anime VALUES ('anime:1', 'Title 1', 'JP'), ('anime:2', 'Title 2', NULL), ('anime:3', 'Title 3', '')"
    )
    conn.execute(
        "INSERT INTO studios VALUES ('studio:1', 'Studio C', NULL)"
    )
    conn.execute(
        "INSERT INTO anime_studios VALUES ('anime:1', 'studio:1', 1), ('anime:2', 'studio:1', 0), ('anime:3', 'studio:1', 0)"
    )

    result = populate_studios_country_of_origin(conn)

    assert result["studios_updated"] == 1

    row = conn.execute("SELECT country_of_origin FROM studios WHERE id = 'studio:1'").fetchone()
    assert row[0] == "JP"


def test_no_anime_association(test_gold_db: duckdb.DuckDBPyConnection) -> None:
    """Studio with no anime associations stays NULL."""
    conn = test_gold_db

    conn.execute(
        "INSERT INTO studios VALUES ('studio:1', 'Studio D', NULL), ('studio:2', 'Studio E', NULL)"
    )
    # studio:2 has no anime associations

    result = populate_studios_country_of_origin(conn)

    assert result["studios_updated"] == 0
    assert result["studios_populated"] == 0

    row = conn.execute("SELECT country_of_origin FROM studios WHERE id = 'studio:2'").fetchone()
    assert row[0] is None


def test_already_set_studio_not_touched(test_gold_db: duckdb.DuckDBPyConnection) -> None:
    """Studio with existing country_of_origin is not updated."""
    conn = test_gold_db

    conn.execute(
        "INSERT INTO anime VALUES ('anime:1', 'Title 1', 'JP')"
    )
    conn.execute(
        "INSERT INTO studios VALUES ('studio:1', 'Studio F', 'US')"
    )
    conn.execute(
        "INSERT INTO anime_studios VALUES ('anime:1', 'studio:1', 1)"
    )

    result = populate_studios_country_of_origin(conn)

    assert result["studios_updated"] == 0

    row = conn.execute("SELECT country_of_origin FROM studios WHERE id = 'studio:1'").fetchone()
    assert row[0] == "US"  # unchanged


def test_multiple_studios(test_gold_db: duckdb.DuckDBPyConnection) -> None:
    """Multiple studios with different majorities."""
    conn = test_gold_db

    conn.execute(
        "INSERT INTO anime VALUES ('a1', 'A1', 'JP'), ('a2', 'A2', 'JP'), ('a3', 'A3', 'US'), ('a4', 'A4', 'US')"
    )
    conn.execute(
        "INSERT INTO studios VALUES ('s1', 'S1', NULL), ('s2', 'S2', NULL), ('s3', 'S3', NULL)"
    )
    conn.execute(
        "INSERT INTO anime_studios VALUES ('a1', 's1', 1), ('a2', 's1', 1), ('a3', 's2', 1), ('a4', 's2', 1)"
    )
    # s3 has no associations

    result = populate_studios_country_of_origin(conn)

    assert result["studios_updated"] == 2
    assert result["studios_populated"] == 2

    s1 = conn.execute("SELECT country_of_origin FROM studios WHERE id = 's1'").fetchone()[0]
    s2 = conn.execute("SELECT country_of_origin FROM studios WHERE id = 's2'").fetchone()[0]
    s3 = conn.execute("SELECT country_of_origin FROM studios WHERE id = 's3'").fetchone()[0]

    assert s1 == "JP"
    assert s2 == "US"
    assert s3 is None
