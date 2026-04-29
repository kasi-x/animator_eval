"""Tests for DuckDB SILVER layer reader (Card 05)."""

from __future__ import annotations

import duckdb
import pytest

# DDL mirrors src/etl/integrate_duckdb.py _DDL exactly so tests reflect reality.
_DDL = """
CREATE TABLE IF NOT EXISTS anime (
    id          VARCHAR PRIMARY KEY,
    title_ja    VARCHAR NOT NULL DEFAULT '',
    title_en    VARCHAR NOT NULL DEFAULT '',
    year        INTEGER,
    season      VARCHAR,
    quarter     INTEGER,
    episodes    INTEGER,
    format      VARCHAR,
    duration    INTEGER,
    start_date  VARCHAR,
    end_date    VARCHAR,
    status      VARCHAR,
    source_mat  VARCHAR,
    work_type   VARCHAR,
    scale_class VARCHAR,
    updated_at  TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS persons (
    id          VARCHAR PRIMARY KEY,
    name_ja     VARCHAR NOT NULL DEFAULT '',
    name_en     VARCHAR NOT NULL DEFAULT '',
    birth_date  VARCHAR,
    death_date  VARCHAR,
    website_url VARCHAR,
    updated_at  TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS credits (
    person_id       VARCHAR NOT NULL,
    anime_id        VARCHAR NOT NULL,
    role            VARCHAR NOT NULL,
    raw_role        VARCHAR,
    episode         INTEGER,
    evidence_source VARCHAR NOT NULL,
    updated_at      TIMESTAMP DEFAULT now()
);
"""


@pytest.fixture()
def silver_path(tmp_path):
    """Override the path-only `silver_path` from conftest.py with a populated DB.

    All tests in this module exercise the SILVER reader against a known fixture
    of 2 anime / 2 persons / 3 credits — the conftest fixture only returns the
    file path and leaves DB creation to the caller (most other consumers run
    `integrate()` which writes the file). silver_reader tests need actual rows.
    """
    path = tmp_path / "silver.duckdb"
    conn = duckdb.connect(str(path))
    for stmt in _DDL.split(";"):
        s = stmt.strip()
        if s:
            conn.execute(s)
    conn.executemany(
        "INSERT INTO anime (id, title_ja, title_en, year, format, episodes, duration, source_mat, scale_class)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("a1", "アニメA", "Anime A", 2020, "TV", 12, 24, "MANGA", "medium"),
            ("a2", "アニメB", "Anime B", 2021, "MOVIE", 1, 90, "ORIGINAL", "large"),
        ],
    )
    conn.executemany(
        "INSERT INTO persons (id, name_ja, name_en, birth_date, website_url)"
        " VALUES (?, ?, ?, ?, ?)",
        [
            ("p1", "山田太郎", "Taro Yamada", "1985-03-15", "https://example.com"),
            ("p2", "佐藤花子", "Hanako Sato", None, None),
        ],
    )
    conn.executemany(
        "INSERT INTO credits (person_id, anime_id, role, evidence_source)"
        " VALUES (?, ?, ?, ?)",
        [
            ("p1", "a1", "director", "anilist"),
            ("p1", "a2", "director", "anilist"),
            ("p2", "a1", "key_animator", "anilist"),
        ],
    )
    conn.commit()
    conn.close()
    return path


class TestSilverConnect:
    def test_opens_and_closes(self, silver_path):
        from src.analysis.io.silver_reader import silver_connect

        with silver_connect(silver_path) as conn:
            count = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
        assert count == 2

    def test_read_only_by_default(self, silver_path):
        from src.analysis.io.silver_reader import silver_connect

        with silver_connect(silver_path) as conn:
            with pytest.raises(Exception, match="read.only|Read-only"):
                conn.execute("DELETE FROM anime")

    def test_memory_limit_applied(self, silver_path):
        from src.analysis.io.silver_reader import silver_connect

        with silver_connect(silver_path, memory_limit="256MB") as conn:
            limit = conn.execute(
                "SELECT current_setting('memory_limit')"
            ).fetchone()[0]
        assert limit and "M" in limit.upper()

    def test_unavailable_path_raises(self, tmp_path):
        from src.analysis.io.silver_reader import silver_connect

        missing = tmp_path / "nonexistent.duckdb"
        with pytest.raises(Exception):
            with silver_connect(missing) as conn:
                conn.execute("SELECT 1")


class TestLoadPersonsSilver:
    def test_returns_person_models(self, silver_path):
        from src.analysis.io.silver_reader import load_persons_silver
        from src.runtime.models import Person

        persons = load_persons_silver(silver_path)
        assert len(persons) == 2
        assert all(isinstance(p, Person) for p in persons)

    def test_birth_date_mapped(self, silver_path):
        from src.analysis.io.silver_reader import load_persons_silver

        persons = load_persons_silver(silver_path)
        by_id = {p.id: p for p in persons}
        assert by_id["p1"].date_of_birth == "1985-03-15"
        assert by_id["p2"].date_of_birth is None

    def test_website_url_mapped(self, silver_path):
        from src.analysis.io.silver_reader import load_persons_silver

        persons = load_persons_silver(silver_path)
        by_id = {p.id: p for p in persons}
        assert by_id["p1"].site_url == "https://example.com"
        assert by_id["p2"].site_url is None

    def test_names_preserved(self, silver_path):
        from src.analysis.io.silver_reader import load_persons_silver

        persons = load_persons_silver(silver_path)
        by_id = {p.id: p for p in persons}
        assert by_id["p1"].name_ja == "山田太郎"
        assert by_id["p1"].name_en == "Taro Yamada"


class TestLoadAnimeSilver:
    def test_returns_anime_models(self, silver_path):
        from src.analysis.io.silver_reader import load_anime_silver
        from src.runtime.models import AnimeAnalysis

        anime_list = load_anime_silver(silver_path)
        assert len(anime_list) == 2
        assert all(isinstance(a, AnimeAnalysis) for a in anime_list)

    def test_source_mat_mapped(self, silver_path):
        from src.analysis.io.silver_reader import load_anime_silver

        anime_list = load_anime_silver(silver_path)
        by_id = {a.id: a for a in anime_list}
        assert by_id["a1"].original_work_type == "MANGA"
        assert by_id["a1"].source == "MANGA"

    def test_core_fields_present(self, silver_path):
        from src.analysis.io.silver_reader import load_anime_silver

        anime_list = load_anime_silver(silver_path)
        by_id = {a.id: a for a in anime_list}
        a1 = by_id["a1"]
        assert a1.year == 2020
        assert a1.format == "TV"
        assert a1.episodes == 12
        assert a1.duration == 24
        assert a1.scale_class == "medium"


class TestLoadCreditsSilver:
    def test_returns_credit_models(self, silver_path):
        from src.analysis.io.silver_reader import load_credits_silver
        from src.runtime.models import Credit

        credits = load_credits_silver(silver_path)
        assert len(credits) == 3
        assert all(isinstance(c, Credit) for c in credits)

    def test_evidence_source_mapped(self, silver_path):
        from src.analysis.io.silver_reader import load_credits_silver

        credits = load_credits_silver(silver_path)
        assert all(c.source == "anilist" for c in credits)
        assert all(c.evidence_source == "anilist" for c in credits)

    def test_role_parsed(self, silver_path):
        from src.analysis.io.silver_reader import load_credits_silver
        from src.runtime.models import Role

        credits = load_credits_silver(silver_path)
        roles = {c.role for c in credits}
        assert Role.DIRECTOR in roles
        assert Role.KEY_ANIMATOR in roles

    def test_unknown_role_skipped(self, tmp_path):
        from src.analysis.io.silver_reader import load_credits_silver

        path = tmp_path / "bad_role.duckdb"
        conn = duckdb.connect(str(path))
        for stmt in _DDL.split(";"):
            s = stmt.strip()
            if s:
                conn.execute(s)
        conn.execute(
            "INSERT INTO credits (person_id, anime_id, role, evidence_source)"
            " VALUES ('p1', 'a1', 'director', 'test'),"
            "        ('p1', 'a1', 'not_a_real_role', 'test')"
        )
        conn.close()
        credits = load_credits_silver(path)
        assert len(credits) == 1
        assert credits[0].role.value == "director"


class TestQuerySilver:
    def test_arbitrary_query(self, silver_path):
        from src.analysis.io.silver_reader import query_silver

        rows = query_silver("SELECT id FROM anime ORDER BY id", path=silver_path)
        assert [r["id"] for r in rows] == ["a1", "a2"]

    def test_parameterized_query(self, silver_path):
        from src.analysis.io.silver_reader import query_silver

        rows = query_silver(
            "SELECT id FROM persons WHERE id = ?", params=["p1"], path=silver_path
        )
        assert rows == [{"id": "p1"}]
