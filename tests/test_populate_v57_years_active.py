"""Test v57 years_active population logic."""
import pytest
import duckdb
from pathlib import Path

from src.etl.populate_v57_years_active import populate_persons_years_active


@pytest.fixture
def test_gold_db(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    """Create a test GOLD DB with minimal schema."""
    db_path = tmp_path / "test_gold.duckdb"
    conn = duckdb.connect(str(db_path))

    conn.execute("""
        CREATE TABLE persons (
            id VARCHAR PRIMARY KEY,
            name_ja VARCHAR,
            years_active VARCHAR DEFAULT '[]'
        )
    """)

    conn.execute("""
        CREATE TABLE credits (
            person_id VARCHAR,
            anime_id VARCHAR,
            role VARCHAR,
            credit_year VARCHAR
        )
    """)

    return conn


def test_single_credit_year(test_gold_db: duckdb.DuckDBPyConnection) -> None:
    """Person with single credit year."""
    conn = test_gold_db

    conn.execute("INSERT INTO persons VALUES ('p1', 'Person A', '[]')")
    conn.execute("INSERT INTO credits VALUES ('p1', 'a1', 'director', '2020')")

    result = populate_persons_years_active(conn)

    assert result["persons_updated"] == 1
    assert result["persons_populated"] == 1

    row = conn.execute("SELECT years_active FROM persons WHERE id = 'p1'").fetchone()
    assert row[0] == "2020-2020"


def test_career_span(test_gold_db: duckdb.DuckDBPyConnection) -> None:
    """Person with multi-year career."""
    conn = test_gold_db

    conn.execute("INSERT INTO persons VALUES ('p2', 'Person B', '[]')")
    conn.execute("INSERT INTO credits VALUES ('p2', 'a1', 'director', '1998')")
    conn.execute("INSERT INTO credits VALUES ('p2', 'a2', 'animator', '2005')")
    conn.execute("INSERT INTO credits VALUES ('p2', 'a3', 'director', '2023')")

    result = populate_persons_years_active(conn)

    assert result["persons_updated"] == 1

    row = conn.execute("SELECT years_active FROM persons WHERE id = 'p2'").fetchone()
    assert row[0] == "1998-2023"


def test_skip_null_and_invalid_years(test_gold_db: duckdb.DuckDBPyConnection) -> None:
    """Null/invalid years are ignored."""
    conn = test_gold_db

    conn.execute("INSERT INTO persons VALUES ('p3', 'Person C', '[]')")
    conn.execute("INSERT INTO credits VALUES ('p3', 'a1', 'role1', NULL)")
    conn.execute("INSERT INTO credits VALUES ('p3', 'a2', 'role2', '0')")
    conn.execute("INSERT INTO credits VALUES ('p3', 'a3', 'role3', '2010')")

    result = populate_persons_years_active(conn)

    assert result["persons_updated"] == 1

    row = conn.execute("SELECT years_active FROM persons WHERE id = 'p3'").fetchone()
    assert row[0] == "2010-2010"


def test_no_credits(test_gold_db: duckdb.DuckDBPyConnection) -> None:
    """Person with no credits stays empty."""
    conn = test_gold_db

    conn.execute("INSERT INTO persons VALUES ('p4', 'Person D', '[]')")
    # no credits for p4

    result = populate_persons_years_active(conn)

    assert result["persons_updated"] == 0
    assert result["persons_populated"] == 0

    row = conn.execute("SELECT years_active FROM persons WHERE id = 'p4'").fetchone()
    assert row[0] == "[]"


def test_already_set_person_not_touched(test_gold_db: duckdb.DuckDBPyConnection) -> None:
    """Person with existing years_active is not overwritten."""
    conn = test_gold_db

    conn.execute("INSERT INTO persons VALUES ('p5', 'Person E', '1990-2000')")
    conn.execute("INSERT INTO credits VALUES ('p5', 'a1', 'director', '2020')")

    result = populate_persons_years_active(conn)

    assert result["persons_updated"] == 0

    row = conn.execute("SELECT years_active FROM persons WHERE id = 'p5'").fetchone()
    assert row[0] == "1990-2000"  # unchanged


def test_multiple_persons(test_gold_db: duckdb.DuckDBPyConnection) -> None:
    """Multiple persons with different career spans."""
    conn = test_gold_db

    conn.execute("INSERT INTO persons VALUES ('p1', 'A', '[]')")
    conn.execute("INSERT INTO persons VALUES ('p2', 'B', '[]')")
    conn.execute("INSERT INTO persons VALUES ('p3', 'C', '[]')")

    conn.execute("INSERT INTO credits VALUES ('p1', 'a1', 'director', '2010')")
    conn.execute("INSERT INTO credits VALUES ('p1', 'a2', 'director', '2015')")

    conn.execute("INSERT INTO credits VALUES ('p2', 'a3', 'animator', '1995')")
    conn.execute("INSERT INTO credits VALUES ('p2', 'a4', 'animator', '2025')")

    # p3 has no credits

    result = populate_persons_years_active(conn)

    assert result["persons_updated"] == 2
    assert result["persons_populated"] == 2

    p1 = conn.execute("SELECT years_active FROM persons WHERE id = 'p1'").fetchone()[0]
    p2 = conn.execute("SELECT years_active FROM persons WHERE id = 'p2'").fetchone()[0]
    p3 = conn.execute("SELECT years_active FROM persons WHERE id = 'p3'").fetchone()[0]

    assert p1 == "2010-2015"
    assert p2 == "1995-2025"
    assert p3 == "[]"
