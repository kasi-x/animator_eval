"""CLI コマンドのテスト."""

import sqlite3

import pytest
from typer.testing import CliRunner

from src.cli import app

runner = CliRunner()


@pytest.fixture()
def populated_db(monkeypatch, tmp_path):
    """テスト用のDBを作成しmonkeypatchでCLIに注入する."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE persons (
            id TEXT PRIMARY KEY,
            name_ja TEXT DEFAULT '',
            name_en TEXT DEFAULT '',
            aliases TEXT DEFAULT '[]',
            mal_id INTEGER,
            anilist_id INTEGER,
            canonical_id TEXT,
            UNIQUE(mal_id),
            UNIQUE(anilist_id)
        );
        CREATE TABLE anime (
            id TEXT PRIMARY KEY,
            title_ja TEXT DEFAULT '',
            title_en TEXT DEFAULT '',
            year INTEGER,
            season TEXT,
            episodes INTEGER,
            mal_id INTEGER,
            anilist_id INTEGER,
            score REAL,
            UNIQUE(mal_id),
            UNIQUE(anilist_id)
        );
        CREATE TABLE credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            episode INTEGER DEFAULT -1,
            source TEXT DEFAULT '',
            UNIQUE(person_id, anime_id, role, episode)
        );
        CREATE TABLE scores (
            person_id TEXT PRIMARY KEY,
            authority REAL DEFAULT 0.0,
            trust REAL DEFAULT 0.0,
            skill REAL DEFAULT 0.0,
            composite REAL DEFAULT 0.0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.executemany(
        "INSERT INTO persons (id, name_ja, name_en) VALUES (?, ?, ?)",
        [
            ("p1", "荒木哲郎", "Tetsuro Araki"),
            ("p2", "今井有文", "Arifumi Imai"),
            ("p3", "浅野恭司", "Kyoji Asano"),
        ],
    )
    conn.executemany(
        "INSERT INTO anime (id, title_ja, title_en, year, score) VALUES (?, ?, ?, ?, ?)",
        [
            ("a1", "進撃の巨人", "Attack on Titan", 2013, 8.4),
            ("a2", "甲鉄城のカバネリ", "Kabaneri", 2016, 7.2),
        ],
    )
    conn.executemany(
        "INSERT INTO credits (person_id, anime_id, role, source) VALUES (?, ?, ?, ?)",
        [
            ("p1", "a1", "director", "anilist"),
            ("p2", "a1", "key_animator", "anilist"),
            ("p3", "a1", "character_designer", "anilist"),
            ("p1", "a2", "director", "anilist"),
            ("p2", "a2", "key_animator", "anilist"),
        ],
    )
    conn.executemany(
        "INSERT INTO scores (person_id, authority, trust, skill, composite) VALUES (?, ?, ?, ?, ?)",
        [
            ("p1", 85.0, 70.0, 60.0, 73.0),
            ("p2", 60.0, 80.0, 90.0, 74.5),
            ("p3", 40.0, 50.0, 55.0, 47.75),
        ],
    )
    conn.commit()
    conn.close()

    def patched_get(db_path=None):
        c = sqlite3.connect(str(db_path or tmp_path / "test.db"))
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr("src.database.get_connection", patched_get)
    return db_path


class TestStatsCommand:
    def test_stats_displays_tables(self, populated_db):
        result = runner.invoke(app, ["stats"])
        assert result.exit_code == 0
        assert "Persons" in result.output or "人物" in result.output
        assert "Anime" in result.output or "アニメ" in result.output
        assert "Credits" in result.output or "クレジット" in result.output

    def test_stats_shows_counts(self, populated_db):
        result = runner.invoke(app, ["stats"])
        assert "3" in result.output  # 3 persons
        assert "2" in result.output  # 2 anime

    def test_stats_lang_english(self, populated_db):
        """Stats command with --lang en shows English text."""
        result = runner.invoke(app, ["stats", "--lang", "en"])
        assert result.exit_code == 0
        assert "Database Statistics" in result.output
        assert "Persons" in result.output
        assert "Anime" in result.output
        assert "Credits" in result.output

    def test_stats_lang_japanese(self, populated_db):
        """Stats command with --lang ja shows Japanese text."""
        result = runner.invoke(app, ["stats", "--lang", "ja"])
        assert result.exit_code == 0
        assert "データベース統計" in result.output
        assert "人物" in result.output
        assert "アニメ" in result.output
        assert "クレジット" in result.output


class TestRankingCommand:
    def test_ranking_shows_scores(self, populated_db):
        result = runner.invoke(app, ["ranking"])
        assert result.exit_code == 0
        assert "Authority" in result.output
        assert "Trust" in result.output
        assert "Composite" in result.output

    def test_ranking_top_option(self, populated_db):
        result = runner.invoke(app, ["ranking", "--top", "2"])
        assert result.exit_code == 0

    def test_ranking_sort_by_authority(self, populated_db):
        result = runner.invoke(app, ["ranking", "--sort", "authority"])
        assert result.exit_code == 0
        assert "Authority" in result.output

    def test_ranking_invalid_sort(self, populated_db):
        result = runner.invoke(app, ["ranking", "--sort", "invalid"])
        assert result.exit_code == 1

    def test_ranking_empty_db(self, monkeypatch, tmp_path):
        import sqlite3

        db_path = tmp_path / "empty.db"
        conn = sqlite3.connect(str(db_path))
        conn.executescript("""
            CREATE TABLE persons (id TEXT PRIMARY KEY, name_ja TEXT DEFAULT '', name_en TEXT DEFAULT '', aliases TEXT DEFAULT '[]', mal_id INTEGER, anilist_id INTEGER, canonical_id TEXT);
            CREATE TABLE anime (id TEXT PRIMARY KEY, title_ja TEXT DEFAULT '', title_en TEXT DEFAULT '', year INTEGER, season TEXT, episodes INTEGER, mal_id INTEGER, anilist_id INTEGER, score REAL);
            CREATE TABLE credits (id INTEGER PRIMARY KEY AUTOINCREMENT, person_id TEXT, anime_id TEXT, role TEXT, episode INTEGER DEFAULT -1, source TEXT DEFAULT '');
            CREATE TABLE scores (person_id TEXT PRIMARY KEY, authority REAL DEFAULT 0.0, trust REAL DEFAULT 0.0, skill REAL DEFAULT 0.0, composite REAL DEFAULT 0.0, updated_at TIMESTAMP);
        """)
        conn.commit()
        conn.close()

        def patched_get(db_path=None):
            c = sqlite3.connect(str(tmp_path / "empty.db"))
            c.row_factory = sqlite3.Row
            return c

        monkeypatch.setattr("src.database.get_connection", patched_get)
        result = runner.invoke(app, ["ranking"])
        assert "No scores found" in result.output


class TestProfileCommand:
    def test_profile_existing_person(self, populated_db):
        result = runner.invoke(app, ["profile", "p1"])
        assert result.exit_code == 0
        assert "荒木哲郎" in result.output
        assert "Authority" in result.output

    def test_profile_not_found(self, populated_db):
        result = runner.invoke(app, ["profile", "nonexistent"])
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_profile_shows_credits(self, populated_db):
        result = runner.invoke(app, ["profile", "p2"])
        assert result.exit_code == 0
        assert "Credits" in result.output


class TestSearchCommand:
    def test_search_by_japanese_name(self, populated_db):
        result = runner.invoke(app, ["search", "荒木"])
        assert result.exit_code == 0
        assert "荒木哲郎" in result.output

    def test_search_by_english_name(self, populated_db):
        result = runner.invoke(app, ["search", "Araki"])
        assert result.exit_code == 0
        assert "Tetsuro Araki" in result.output

    def test_search_by_id(self, populated_db):
        result = runner.invoke(app, ["search", "p2"])
        assert result.exit_code == 0
        assert "今井有文" in result.output

    def test_search_no_results(self, populated_db):
        result = runner.invoke(app, ["search", "xyz_nobody"])
        assert "No results" in result.output

    def test_search_shows_composite_score(self, populated_db):
        result = runner.invoke(app, ["search", "荒木"])
        assert "73.0" in result.output


class TestCompareCommand:
    def test_compare_two_persons(self, populated_db):
        result = runner.invoke(app, ["compare", "p1", "p2"])
        assert result.exit_code == 0
        assert "Authority" in result.output or "authority" in result.output.lower()
        assert "荒木哲郎" in result.output
        assert "今井有文" in result.output

    def test_compare_shows_diff(self, populated_db):
        result = runner.invoke(app, ["compare", "p1", "p2"])
        assert result.exit_code == 0
        # Should show score differences
        assert "+" in result.output or "-" in result.output

    def test_compare_shows_roles(self, populated_db):
        result = runner.invoke(app, ["compare", "p1", "p2"])
        assert result.exit_code == 0
        assert "director" in result.output or "key_animator" in result.output

    def test_compare_person_not_found(self, populated_db):
        result = runner.invoke(app, ["compare", "p1", "nonexistent"])
        assert result.exit_code == 1
        assert "not found" in result.output


class TestHistoryCommand:
    @pytest.fixture()
    def db_with_history(self, monkeypatch, tmp_path):
        """Score history入りDB."""
        import src.database

        db_path = tmp_path / "history.db"
        monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", db_path)

        from src.database import (
            get_connection,
            init_db,
            save_score_history,
            upsert_person,
            upsert_score,
        )
        from src.models import Person, ScoreResult

        conn = get_connection()
        init_db(conn)
        upsert_person(conn, Person(id="p1", name_en="Test Person", name_ja="テスト"))
        score = ScoreResult(person_id="p1", authority=80.0, trust=70.0, skill=60.0)
        upsert_score(conn, score)
        save_score_history(conn, score)
        save_score_history(
            conn,
            ScoreResult(person_id="p1", authority=85.0, trust=72.0, skill=62.0),
        )
        conn.commit()
        conn.close()
        return db_path

    def test_history_shows_scores(self, db_with_history):
        result = runner.invoke(app, ["history", "p1"])
        assert result.exit_code == 0
        assert "80.0" in result.output or "85.0" in result.output
        assert "Score History" in result.output

    def test_history_person_not_found(self, db_with_history):
        result = runner.invoke(app, ["history", "nonexistent"])
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_history_no_history(self, db_with_history, monkeypatch, tmp_path):
        """Person exists but has no history."""
        import src.database

        from src.database import get_connection, init_db, upsert_person
        from src.models import Person

        db_path = tmp_path / "nohist.db"
        monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", db_path)
        conn = get_connection()
        init_db(conn)
        upsert_person(conn, Person(id="p99", name_en="No History"))
        conn.commit()
        conn.close()

        result = runner.invoke(app, ["history", "p99"])
        assert result.exit_code == 0
        assert "No score history" in result.output


class TestCrossvalCommand:
    def test_crossval_shows_results(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = {
            "n_folds": 5,
            "holdout_ratio": 0.2,
            "total_credits": 100,
            "avg_rank_correlation": 0.95,
            "min_rank_correlation": 0.88,
            "avg_top10_overlap": 0.9,
            "fold_results": [
                {
                    "fold": 1,
                    "credits_used": 80,
                    "correlation": 0.95,
                    "top10_overlap": 0.9,
                },
            ],
        }
        (tmp_path / "crossval.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["crossval"])
        assert result.exit_code == 0
        assert "Cross-Validation" in result.output
        assert "0.95" in result.output

    def test_crossval_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path / "empty")
        result = runner.invoke(app, ["crossval"])
        assert "No crossval.json" in result.output


class TestInfluenceCommand:
    def test_influence_shows_results(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = {
            "mentors": {
                "p1": {
                    "mentee_count": 5,
                    "nurture_rate": 40.0,
                    "influence_score": 200.0,
                    "mentees": [],
                }
            },
            "generation_chains": [["p1", "p2", "p3"]],
            "total_mentors": 1,
            "total_mentees": 5,
            "avg_nurture_rate": 40.0,
        }
        (tmp_path / "influence.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["influence"])
        assert result.exit_code == 0
        assert "Influence Tree" in result.output
        assert "40.0%" in result.output

    def test_influence_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path / "empty")
        result = runner.invoke(app, ["influence"])
        assert "No influence.json" in result.output


class TestStudiosCommand:
    def test_studios_shows_results(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = {
            "MAPPA": {"anime_count": 5, "person_count": 50, "avg_person_score": 65.0},
            "ufotable": {
                "anime_count": 3,
                "person_count": 30,
                "avg_person_score": 72.0,
            },
        }
        (tmp_path / "studios.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["studios"])
        assert result.exit_code == 0
        assert "Studio Analysis" in result.output
        assert "MAPPA" in result.output

    def test_studios_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path / "empty")
        result = runner.invoke(app, ["studios"])
        assert "No studios.json" in result.output


class TestVersatilityCommand:
    def test_versatility_shows_results(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = [
            {
                "person_id": "p1",
                "name": "Person 1",
                "composite": 70.0,
                "versatility": {"score": 75.0, "categories": 3, "roles": 5},
            },
        ]
        (tmp_path / "scores.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["versatility"])
        assert result.exit_code == 0
        assert "Versatility" in result.output
        assert "75" in result.output

    def test_versatility_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path / "empty")
        result = runner.invoke(app, ["versatility"])
        assert "No scores.json" in result.output


class TestDensityCommand:
    def test_density_shows_results(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = [
            {
                "person_id": "p1",
                "name": "Person 1",
                "composite": 70.0,
                "network": {"hub_score": 85.0, "collaborators": 20, "unique_anime": 10},
            },
        ]
        (tmp_path / "scores.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["density"])
        assert result.exit_code == 0
        assert "Hub Score" in result.output
        assert "85.0" in result.output

    def test_density_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path / "empty")
        result = runner.invoke(app, ["density"])
        assert "No scores.json" in result.output


class TestOutliersCommand:
    def test_outliers_shows_results(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = [
            {
                "person_id": f"p{i}",
                "name": f"Normal {i}",
                "authority": 50.0,
                "trust": 50.0,
                "skill": 50.0,
                "composite": 50.0,
            }
            for i in range(20)
        ] + [
            {
                "person_id": "p_high",
                "name": "Outlier",
                "authority": 99.0,
                "trust": 99.0,
                "skill": 99.0,
                "composite": 99.0,
            },
        ]
        (tmp_path / "scores.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["outliers"])
        assert result.exit_code == 0
        assert "Outlier Detection" in result.output

    def test_outliers_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path / "empty")
        result = runner.invoke(app, ["outliers"])
        assert "No scores.json" in result.output


class TestGrowthCommand:
    def test_growth_shows_results(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = {
            "trend_summary": {"rising": 2, "stable": 1},
            "total_persons": 3,
            "persons": {
                "p1": {
                    "trend": "rising",
                    "total_credits": 10,
                    "recent_credits": 7,
                    "activity_ratio": 0.7,
                    "career_span": 5,
                },
            },
        }
        (tmp_path / "growth.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["growth"])
        assert result.exit_code == 0
        assert "Growth Trends" in result.output
        assert "rising" in result.output

    def test_growth_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path / "empty")
        result = runner.invoke(app, ["growth"])
        assert "No growth.json" in result.output


class TestTeamsCommand:
    def test_teams_shows_results(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = {
            "high_score_teams": [
                {"title": "Hit Show", "year": 2023, "anime_score": 8.5, "team_size": 5}
            ],
            "total_high_score": 1,
            "role_combinations": [],
            "recommended_pairs": [],
            "team_size_stats": {"avg": 5.0, "min": 3, "max": 8},
        }
        (tmp_path / "teams.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["teams"])
        assert result.exit_code == 0
        assert "Team Composition" in result.output
        assert "Hit Show" in result.output

    def test_teams_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path / "empty")
        result = runner.invoke(app, ["teams"])
        assert "No teams.json" in result.output


class TestDecadesCommand:
    def test_decades_shows_results(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = {
            "decades": {
                "2020s": {
                    "credit_count": 100,
                    "unique_persons": 50,
                    "unique_anime": 20,
                    "avg_anime_score": 7.5,
                }
            },
            "year_by_year": {},
        }
        (tmp_path / "decades.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["decades"])
        assert result.exit_code == 0
        assert "Decade Analysis" in result.output
        assert "2020s" in result.output

    def test_decades_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path / "empty")
        result = runner.invoke(app, ["decades"])
        assert "No decades.json" in result.output


class TestTagsCommand:
    def test_tags_shows_results(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = {
            "tag_summary": {"veteran": 5, "rising_star": 3},
            "person_tags": {"p1": ["veteran"], "p2": ["rising_star"]},
        }
        (tmp_path / "tags.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["tags"])
        assert result.exit_code == 0
        assert "Tag Distribution" in result.output
        assert "veteran" in result.output

    def test_tags_filter(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = {
            "tag_summary": {"veteran": 1},
            "person_tags": {"p1": ["veteran"], "p2": ["rising_star"]},
        }
        (tmp_path / "tags.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["tags", "--tag", "veteran"])
        assert result.exit_code == 0
        assert "p1" in result.output

    def test_tags_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path / "empty")
        result = runner.invoke(app, ["tags"])
        assert "No tags.json" in result.output


class TestDataQualityCommand:
    def test_data_quality_shows_score(self, monkeypatch, tmp_path):
        import src.database

        db_path = tmp_path / "dq.db"
        monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", db_path)

        from src.database import get_connection, init_db

        conn = get_connection()
        init_db(conn)
        conn.execute(
            "INSERT INTO anime (id, title_en, year, score) VALUES ('a1', 'Test', 2024, 8.0)"
        )
        conn.execute("INSERT INTO persons (id, name_en) VALUES ('p1', 'Person')")
        conn.execute(
            "INSERT INTO credits (person_id, anime_id, role, source) VALUES ('p1', 'a1', 'director', 'test')"
        )
        conn.commit()
        conn.close()

        result = runner.invoke(app, ["data-quality"])
        assert result.exit_code == 0
        assert "Data Quality Score" in result.output
        assert "Overall" in result.output


class TestBridgesCommand:
    def test_bridges_shows_results(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = {
            "bridge_persons": [
                {
                    "person_id": "p2",
                    "cross_community_edges": 3,
                    "communities_connected": 2,
                    "bridge_score": 60,
                }
            ],
            "stats": {
                "total_persons": 6,
                "total_communities": 2,
                "total_cross_edges": 3,
                "bridge_person_count": 1,
            },
        }
        (tmp_path / "bridges.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["bridges"])
        assert result.exit_code == 0
        assert "Bridge" in result.output
        assert "p2" in result.output

    def test_bridges_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path / "empty")
        result = runner.invoke(app, ["bridges"])
        assert "No bridges.json" in result.output


class TestMentorshipsCommand:
    def test_mentorships_shows_results(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = {
            "mentorships": [
                {
                    "mentor_id": "p1",
                    "mentee_id": "p2",
                    "shared_works": 5,
                    "stage_gap": 3,
                    "confidence": 80,
                    "year_span": [2018, 2022],
                }
            ],
            "tree": {"tree": {"p1": ["p2"]}, "roots": ["p1"]},
            "total": 1,
        }
        (tmp_path / "mentorships.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["mentorships"])
        assert result.exit_code == 0
        assert "Mentorship" in result.output
        assert "p1" in result.output

    def test_mentorships_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path / "empty")
        result = runner.invoke(app, ["mentorships"])
        assert "No mentorships.json" in result.output


class TestMilestonesCommand:
    def test_milestones_shows_results(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = {
            "p1": [
                {
                    "type": "career_start",
                    "year": 2010,
                    "description": "初クレジット: First Show",
                },
            ],
        }
        (tmp_path / "milestones.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["milestones", "p1"])
        assert result.exit_code == 0
        assert "Milestones" in result.output
        assert "career_start" in result.output

    def test_milestones_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path / "empty")
        result = runner.invoke(app, ["milestones", "p1"])
        assert "No milestones.json" in result.output

    def test_milestones_person_not_found(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        (tmp_path / "milestones.json").write_text(json.dumps({"p1": []}))
        result = runner.invoke(app, ["milestones", "nonexistent"])
        assert "No milestones for" in result.output


class TestNetEvolutionCommand:
    def test_net_evolution_shows_results(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = {
            "years": [2020, 2021],
            "snapshots": {
                "2020": {
                    "active_persons": 5,
                    "cumulative_persons": 5,
                    "new_persons": 5,
                    "new_edges": 3,
                    "density": 0.3,
                },
                "2021": {
                    "active_persons": 8,
                    "cumulative_persons": 10,
                    "new_persons": 5,
                    "new_edges": 7,
                    "density": 0.25,
                },
            },
            "trends": {
                "person_growth": 5,
                "edge_growth": 10,
                "avg_new_persons_per_year": 5.0,
            },
        }
        (tmp_path / "network_evolution.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["net-evolution"])
        assert result.exit_code == 0
        assert "Network Evolution" in result.output
        assert "2020" in result.output

    def test_net_evolution_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path / "empty")
        result = runner.invoke(app, ["net-evolution"])
        assert "No network_evolution.json" in result.output


class TestGenreAffinityCommand:
    def test_genre_shows_results(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = {
            "p1": {
                "score_tiers": {"high_rated": 60.0, "mid_rated": 40.0},
                "eras": {"modern": 80.0, "2010s": 20.0},
                "primary_tier": "high_rated",
                "primary_era": "modern",
                "avg_anime_score": 8.0,
                "total_credits": 5,
            },
        }
        (tmp_path / "genre_affinity.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["genre-affinity", "p1"])
        assert result.exit_code == 0
        assert "Genre Affinity" in result.output
        assert "high_rated" in result.output

    def test_genre_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path / "empty")
        result = runner.invoke(app, ["genre-affinity", "p1"])
        assert "No genre_affinity.json" in result.output

    def test_genre_person_not_found(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        (tmp_path / "genre_affinity.json").write_text(json.dumps({"p1": {}}))
        result = runner.invoke(app, ["genre-affinity", "nonexistent"])
        assert "No genre data for" in result.output


class TestProductivityCommand:
    def test_productivity_shows_results(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path)
        data = {
            "p1": {
                "credits_per_year": 3.5,
                "total_credits": 14,
                "active_years": 4,
                "consistency_score": 0.8,
            },
        }
        (tmp_path / "productivity.json").write_text(json.dumps(data))
        result = runner.invoke(app, ["productivity"])
        assert result.exit_code == 0
        assert "Productivity" in result.output
        assert "3.5" in result.output

    def test_productivity_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.utils.config.JSON_DIR", tmp_path / "empty")
        result = runner.invoke(app, ["productivity"])
        assert "No productivity.json" in result.output
