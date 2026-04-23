"""Tests for DuckDB GOLD layer writer/reader (Phase B)."""

import pytest


@pytest.fixture()
def gold_path(tmp_path):
    return tmp_path / "gold.duckdb"


SCORE_ROWS = [
    ("p1", 0.8, 0.5, 0.9, 0.3, 0.95, 0.6, 0.85),
    ("p2", 0.4, 0.3, 0.5, 0.2, 0.80, 0.4, 0.42),
    ("p3", 0.6, 0.4, 0.7, 0.5, 0.90, 0.5, 0.63),
]
HISTORY_ROWS = [(*r, 2025, 2) for r in SCORE_ROWS]


class TestGoldWriter:
    def test_write_person_scores_returns_count(self, gold_path):
        from src.analysis.gold_writer import GoldWriter

        with GoldWriter(gold_path) as gw:
            n = gw.write_person_scores(SCORE_ROWS)
        assert n == 3

    def test_write_score_history_returns_count(self, gold_path):
        from src.analysis.gold_writer import GoldWriter

        with GoldWriter(gold_path) as gw:
            n = gw.write_score_history(HISTORY_ROWS)
        assert n == 3

    def test_write_empty_is_noop(self, gold_path):
        from src.analysis.gold_writer import GoldWriter

        with GoldWriter(gold_path) as gw:
            assert gw.write_person_scores([]) == 0
            assert gw.write_score_history([]) == 0

    def test_upsert_updates_existing(self, gold_path):
        from src.analysis.gold_writer import GoldWriter, GoldReader

        with GoldWriter(gold_path) as gw:
            gw.write_person_scores(SCORE_ROWS)

        with GoldWriter(gold_path) as gw:
            gw.write_person_scores([("p1", 0.99, 0.5, 0.9, 0.3, 0.95, 0.6, 0.99)])

        row = GoldReader(gold_path).person_scores_for("p1")
        assert row is not None
        assert abs(row["iv_score"] - 0.99) < 1e-9

    def test_no_duplicate_on_upsert(self, gold_path):
        from src.analysis.gold_writer import GoldWriter, GoldReader

        with GoldWriter(gold_path) as gw:
            gw.write_person_scores(SCORE_ROWS)
            gw.write_person_scores(SCORE_ROWS)  # second write = upsert

        rows = GoldReader(gold_path).person_scores()
        assert len(rows) == 3


class TestGoldReader:
    def _setup(self, gold_path):
        from src.analysis.gold_writer import GoldWriter

        with GoldWriter(gold_path) as gw:
            gw.write_person_scores(SCORE_ROWS)
            gw.write_score_history(HISTORY_ROWS)

    def test_available_false_when_missing(self, tmp_path):
        from src.analysis.gold_writer import GoldReader

        reader = GoldReader(tmp_path / "nonexistent.duckdb")
        assert not reader.available()

    def test_available_true_after_write(self, gold_path):
        from src.analysis.gold_writer import GoldWriter, GoldReader

        with GoldWriter(gold_path) as gw:
            gw.write_person_scores(SCORE_ROWS)

        assert GoldReader(gold_path).available()

    def test_person_scores_ordered_by_iv_desc(self, gold_path):
        from src.analysis.gold_writer import GoldReader

        self._setup(gold_path)
        rows = GoldReader(gold_path).person_scores()
        iv_scores = [r["iv_score"] for r in rows]
        assert iv_scores == sorted(iv_scores, reverse=True)

    def test_person_scores_for_found(self, gold_path):
        from src.analysis.gold_writer import GoldReader

        self._setup(gold_path)
        row = GoldReader(gold_path).person_scores_for("p2")
        assert row is not None
        assert row["person_id"] == "p2"

    def test_person_scores_for_missing_returns_none(self, gold_path):
        from src.analysis.gold_writer import GoldReader

        self._setup(gold_path)
        assert GoldReader(gold_path).person_scores_for("nonexistent") is None

    def test_person_scores_returns_empty_when_unavailable(self, tmp_path):
        from src.analysis.gold_writer import GoldReader

        reader = GoldReader(tmp_path / "nonexistent.duckdb")
        assert reader.person_scores() == []

    def test_score_history_for_returns_rows(self, gold_path):
        from src.analysis.gold_writer import GoldReader

        self._setup(gold_path)
        hist = GoldReader(gold_path).score_history_for("p1")
        assert len(hist) == 1
        assert hist[0]["year"] == 2025
        assert hist[0]["quarter"] == 2

    def test_top_n_limits_results(self, gold_path):
        from src.analysis.gold_writer import GoldReader

        self._setup(gold_path)
        top2 = GoldReader(gold_path).top_n(2)
        assert len(top2) == 2

    def test_top_n_ordered_by_iv(self, gold_path):
        from src.analysis.gold_writer import GoldReader

        self._setup(gold_path)
        top2 = GoldReader(gold_path).top_n(2)
        assert top2[0]["iv_score"] >= top2[1]["iv_score"]

    def test_all_score_fields_present(self, gold_path):
        from src.analysis.gold_writer import GoldReader

        self._setup(gold_path)
        row = GoldReader(gold_path).person_scores_for("p1")
        for field in ("person_id", "person_fe", "studio_fe_exposure", "birank",
                      "patronage", "dormancy", "awcc", "iv_score"):
            assert field in row


class TestRankingQuery:
    """Cross-engine JOIN: DuckDB person_scores x SQLite persons/credits."""

    @pytest.fixture()
    def sqlite_path(self, tmp_path):
        import sqlite3
        path = tmp_path / "silver.db"
        conn = sqlite3.connect(str(path))
        conn.executescript("""
            CREATE TABLE persons (id TEXT PRIMARY KEY, name_ja TEXT, name_en TEXT);
            CREATE TABLE credits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                person_id TEXT, anime_id TEXT, role TEXT, credit_year INTEGER,
                evidence_source TEXT
            );
            INSERT INTO persons VALUES ('p1', '監督A', 'Director A');
            INSERT INTO persons VALUES ('p2', 'アニA', 'Animator A');
            INSERT INTO persons VALUES ('p3', '', 'Newbie');
            INSERT INTO credits (person_id, anime_id, role, credit_year, evidence_source)
                VALUES ('p1', 'a1', 'director', 2020, 'test');
            INSERT INTO credits (person_id, anime_id, role, credit_year, evidence_source)
                VALUES ('p1', 'a2', 'director', 2022, 'test');
            INSERT INTO credits (person_id, anime_id, role, credit_year, evidence_source)
                VALUES ('p2', 'a1', 'key_animator', 2020, 'test');
            INSERT INTO credits (person_id, anime_id, role, credit_year, evidence_source)
                VALUES ('p3', 'a1', 'key_animator', 2021, 'test');
        """)
        conn.commit()
        conn.close()
        return path

    def _setup_gold(self, gold_path):
        from src.analysis.gold_writer import GoldWriter
        with GoldWriter(gold_path) as gw:
            gw.write_person_scores(SCORE_ROWS)

    def test_returns_total_and_rows(self, gold_path, sqlite_path):
        from src.analysis.gold_writer import GoldReader
        self._setup_gold(gold_path)
        total, rows = GoldReader(gold_path).ranking_query(sqlite_path, limit=10)
        assert total == 3
        assert len(rows) == 3

    def test_rows_ordered_by_iv_desc(self, gold_path, sqlite_path):
        from src.analysis.gold_writer import GoldReader
        self._setup_gold(gold_path)
        _, rows = GoldReader(gold_path).ranking_query(sqlite_path, limit=10)
        scores = [r["iv_score"] for r in rows]
        assert scores == sorted(scores, reverse=True)

    def test_row_has_expected_fields(self, gold_path, sqlite_path):
        from src.analysis.gold_writer import GoldReader
        self._setup_gold(gold_path)
        _, rows = GoldReader(gold_path).ranking_query(sqlite_path, limit=10)
        row = rows[0]
        for field in ("person_id", "name_ja", "name_en", "iv_score",
                      "birank", "patronage", "person_fe", "awcc", "dormancy",
                      "first_year", "latest_year", "primary_role"):
            assert field in row, f"missing field: {field}"

    def test_limit_applied(self, gold_path, sqlite_path):
        from src.analysis.gold_writer import GoldReader
        self._setup_gold(gold_path)
        total, rows = GoldReader(gold_path).ranking_query(sqlite_path, limit=2)
        assert total == 3      # total is unaffected by limit
        assert len(rows) == 2

    def test_condition_filter(self, gold_path, sqlite_path):
        from src.analysis.gold_writer import GoldReader
        self._setup_gold(gold_path)
        # Filter to persons who have a director credit in sl.credits
        conds = ["EXISTS (SELECT 1 FROM sl.credits cr WHERE cr.person_id = s.person_id"
                 " AND cr.role = ?)"]
        total, rows = GoldReader(gold_path).ranking_query(
            sqlite_path, conditions=conds, params=["director"], limit=10
        )
        assert total == 1
        assert rows[0]["person_id"] == "p1"

    def test_primary_role_populated(self, gold_path, sqlite_path):
        from src.analysis.gold_writer import GoldReader
        self._setup_gold(gold_path)
        _, rows = GoldReader(gold_path).ranking_query(sqlite_path, limit=10)
        by_pid = {r["person_id"]: r for r in rows}
        assert by_pid["p1"]["primary_role"] == "director"

    def test_first_latest_year(self, gold_path, sqlite_path):
        from src.analysis.gold_writer import GoldReader
        self._setup_gold(gold_path)
        _, rows = GoldReader(gold_path).ranking_query(sqlite_path, limit=10)
        by_pid = {r["person_id"]: r for r in rows}
        assert by_pid["p1"]["first_year"] == 2020
        assert by_pid["p1"]["latest_year"] == 2022
