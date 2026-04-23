"""CLI コマンドのテスト."""

import pytest
from typer.testing import CliRunner

from src.cli import app

runner = CliRunner()


@pytest.fixture()
def populated_duckdb(monkeypatch, tmp_path):
    """DuckDB test data for migrated CLI commands (stats/ranking/search/compare)."""
    import duckdb

    import src.analysis.gold_writer
    import src.analysis.silver_reader
    from src.analysis.gold_writer import _DDL

    silver_path = tmp_path / "silver.duckdb"
    gold_path = tmp_path / "gold.duckdb"

    monkeypatch.setattr(src.analysis.silver_reader, "DEFAULT_SILVER_PATH", silver_path)
    monkeypatch.setattr(src.analysis.gold_writer, "DEFAULT_GOLD_DB_PATH", gold_path)

    # Create silver.duckdb
    sconn = duckdb.connect(str(silver_path))
    sconn.execute(
        """CREATE TABLE persons (
            id VARCHAR PRIMARY KEY, name_ja VARCHAR DEFAULT '', name_en VARCHAR DEFAULT '',
            name_ko VARCHAR DEFAULT '', name_zh VARCHAR DEFAULT '',
            aliases VARCHAR DEFAULT '[]', image_medium VARCHAR
        )"""
    )
    sconn.execute(
        "CREATE TABLE anime (id VARCHAR PRIMARY KEY, title_ja VARCHAR DEFAULT '',"
        " title_en VARCHAR DEFAULT '', year INTEGER)"
    )
    sconn.execute(
        "CREATE TABLE credits (person_id VARCHAR, anime_id VARCHAR, role VARCHAR,"
        " credit_year INTEGER DEFAULT 0, evidence_source VARCHAR DEFAULT '')"
    )
    sconn.executemany(
        "INSERT INTO persons(id, name_ja, name_en) VALUES (?, ?, ?)",
        [
            ("p1", "荒木哲郎", "Tetsuro Araki"),
            ("p2", "今井有文", "Arifumi Imai"),
            ("p3", "浅野恭司", "Kyoji Asano"),
        ],
    )
    sconn.executemany(
        "INSERT INTO anime(id, title_ja, title_en, year) VALUES (?, ?, ?, ?)",
        [
            ("a1", "進撃の巨人", "Attack on Titan", 2013),
            ("a2", "甲鉄城のカバネリ", "Kabaneri", 2016),
        ],
    )
    sconn.executemany(
        "INSERT INTO credits(person_id, anime_id, role, credit_year, evidence_source)"
        " VALUES (?, ?, ?, ?, ?)",
        [
            ("p1", "a1", "director", 2013, "anilist"),
            ("p2", "a1", "key_animator", 2013, "anilist"),
            ("p3", "a1", "character_designer", 2013, "anilist"),
            ("p1", "a2", "director", 2016, "anilist"),
            ("p2", "a2", "key_animator", 2016, "anilist"),
        ],
    )
    sconn.close()

    # Create gold.duckdb
    gconn = duckdb.connect(str(gold_path))
    gconn.execute(_DDL)
    gconn.executemany(
        "INSERT INTO person_scores(person_id, birank, patronage, person_fe, iv_score)"
        " VALUES (?, ?, ?, ?, ?)",
        [
            ("p1", 85.0, 70.0, 60.0, 73.0),
            ("p2", 60.0, 80.0, 90.0, 74.5),
            ("p3", 40.0, 50.0, 55.0, 47.75),
        ],
    )
    gconn.close()

    return tmp_path


@pytest.fixture()
def populated_duckdb_with_history(monkeypatch, tmp_path):
    """DuckDB test data including score_history rows (for TestHistoryCommand)."""
    import duckdb

    import src.analysis.gold_writer
    import src.analysis.silver_reader
    from src.analysis.gold_writer import _DDL

    silver_path = tmp_path / "silver.duckdb"
    gold_path = tmp_path / "gold.duckdb"

    monkeypatch.setattr(src.analysis.silver_reader, "DEFAULT_SILVER_PATH", silver_path)
    monkeypatch.setattr(src.analysis.gold_writer, "DEFAULT_GOLD_DB_PATH", gold_path)

    # Create silver.duckdb (minimal: persons only needed for history)
    sconn = duckdb.connect(str(silver_path))
    sconn.execute(
        """CREATE TABLE persons (
            id VARCHAR PRIMARY KEY, name_ja VARCHAR DEFAULT '', name_en VARCHAR DEFAULT '',
            name_ko VARCHAR DEFAULT '', name_zh VARCHAR DEFAULT '',
            aliases VARCHAR DEFAULT '[]', image_medium VARCHAR
        )"""
    )
    sconn.execute(
        "CREATE TABLE anime (id VARCHAR PRIMARY KEY, title_ja VARCHAR DEFAULT '',"
        " title_en VARCHAR DEFAULT '', year INTEGER)"
    )
    sconn.execute(
        "CREATE TABLE credits (person_id VARCHAR, anime_id VARCHAR, role VARCHAR,"
        " credit_year INTEGER DEFAULT 0, evidence_source VARCHAR DEFAULT '')"
    )
    sconn.execute(
        "INSERT INTO persons(id, name_ja, name_en) VALUES ('p1', 'テスト', 'Test Person')"
    )
    sconn.close()

    # Create gold.duckdb with score_history
    gconn = duckdb.connect(str(gold_path))
    gconn.execute(_DDL)
    gconn.executemany(
        "INSERT INTO person_scores(person_id, birank, patronage, person_fe, iv_score)"
        " VALUES (?, ?, ?, ?, ?)",
        [("p1", 80.0, 70.0, 60.0, 73.0)],
    )
    gconn.executemany(
        "INSERT INTO score_history(person_id, year, quarter, iv_score, person_fe,"
        " birank, patronage, dormancy, awcc) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("p1", 2025, 1, 73.0, 60.0, 80.0, 70.0, 1.0, 0.5),
            ("p1", 2024, 4, 70.0, 58.0, 78.0, 68.0, 1.0, 0.5),
        ],
    )
    gconn.close()

    return tmp_path


@pytest.fixture()
def populated_db(monkeypatch, tmp_path):
    """テスト用のDBを作成しmonkeypatchでCLIに注入する."""
    import src.database as db_mod
    from src.database import get_connection, init_db

    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    conn.executemany(
        "INSERT INTO persons (id, name_ja, name_en) VALUES (?, ?, ?)",
        [
            ("p1", "荒木哲郎", "Tetsuro Araki"),
            ("p2", "今井有文", "Arifumi Imai"),
            ("p3", "浅野恭司", "Kyoji Asano"),
        ],
    )
    conn.executemany(
        "INSERT INTO anime (id, title_ja, title_en, year) VALUES (?, ?, ?, ?)",
        [
            ("a1", "進撃の巨人", "Attack on Titan", 2013),
            ("a2", "甲鉄城のカバネリ", "Kabaneri", 2016),
        ],
    )
    conn.executemany(
        "INSERT INTO credits (person_id, anime_id, role, evidence_source) VALUES (?, ?, ?, ?)",
        [
            ("p1", "a1", "director", "anilist"),
            ("p2", "a1", "key_animator", "anilist"),
            ("p3", "a1", "character_designer", "anilist"),
            ("p1", "a2", "director", "anilist"),
            ("p2", "a2", "key_animator", "anilist"),
        ],
    )
    conn.executemany(
        "INSERT INTO person_scores (person_id, birank, patronage, person_fe, iv_score) VALUES (?, ?, ?, ?, ?)",
        [
            ("p1", 85.0, 70.0, 60.0, 73.0),
            ("p2", 60.0, 80.0, 90.0, 74.5),
            ("p3", 40.0, 50.0, 55.0, 47.75),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(db_mod, "DEFAULT_DB_PATH", db_path)
    return db_path


class TestStatsCommand:
    def test_stats_displays_tables(self, populated_duckdb):
        result = runner.invoke(app, ["stats"])
        assert result.exit_code == 0
        assert "Persons" in result.output or "人物" in result.output
        assert "Anime" in result.output or "アニメ" in result.output
        assert "Credits" in result.output or "クレジット" in result.output

    def test_stats_shows_counts(self, populated_duckdb):
        result = runner.invoke(app, ["stats"])
        assert "3" in result.output  # 3 persons
        assert "2" in result.output  # 2 anime

    def test_stats_lang_english(self, populated_duckdb):
        """Stats command with --lang en shows English text."""
        result = runner.invoke(app, ["stats", "--lang", "en"])
        assert result.exit_code == 0
        assert "Database Statistics" in result.output
        assert "Persons" in result.output
        assert "Anime" in result.output
        assert "Credits" in result.output

    def test_stats_lang_japanese(self, populated_duckdb):
        """Stats command with --lang ja shows Japanese text."""
        result = runner.invoke(app, ["stats", "--lang", "ja"])
        assert result.exit_code == 0
        assert "データベース統計" in result.output
        assert "人物" in result.output
        assert "アニメ" in result.output
        assert "クレジット" in result.output


class TestRankingCommand:
    def test_ranking_shows_scores(self, populated_duckdb):
        result = runner.invoke(app, ["ranking"])
        assert result.exit_code == 0
        assert "BiRank" in result.output
        assert "Patronage" in result.output
        assert "IV Score" in result.output

    def test_ranking_top_option(self, populated_duckdb):
        result = runner.invoke(app, ["ranking", "--top", "2"])
        assert result.exit_code == 0

    def test_ranking_sort_by_birank(self, populated_duckdb):
        result = runner.invoke(app, ["ranking", "--sort", "birank"])
        assert result.exit_code == 0
        assert "BiRank" in result.output

    def test_ranking_invalid_sort(self, populated_duckdb):
        result = runner.invoke(app, ["ranking", "--sort", "invalid"])
        assert result.exit_code == 1

    def test_ranking_empty_db(self, monkeypatch, tmp_path):
        import src.analysis.gold_writer

        gold_path = tmp_path / "nonexistent_gold.duckdb"
        monkeypatch.setattr(src.analysis.gold_writer, "DEFAULT_GOLD_DB_PATH", gold_path)
        result = runner.invoke(app, ["ranking"])
        assert "No scores found" in result.output


class TestProfileCommand:
    def test_profile_existing_person(self, populated_db):
        result = runner.invoke(app, ["profile", "p1"])
        assert result.exit_code == 0
        assert "荒木哲郎" in result.output
        assert "BiRank" in result.output

    def test_profile_not_found(self, populated_db):
        result = runner.invoke(app, ["profile", "nonexistent"])
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_profile_shows_credits(self, populated_db):
        result = runner.invoke(app, ["profile", "p2"])
        assert result.exit_code == 0
        assert "Credits" in result.output


class TestSearchCommand:
    def test_search_by_japanese_name(self, populated_duckdb):
        result = runner.invoke(app, ["search", "荒木"])
        assert result.exit_code == 0
        assert "荒木哲郎" in result.output

    def test_search_by_english_name(self, populated_duckdb):
        result = runner.invoke(app, ["search", "Araki"])
        assert result.exit_code == 0
        assert "Tetsuro Araki" in result.output

    def test_search_by_id(self, populated_duckdb):
        result = runner.invoke(app, ["search", "p2"])
        assert result.exit_code == 0
        assert "今井有文" in result.output

    def test_search_no_results(self, populated_duckdb):
        result = runner.invoke(app, ["search", "xyz_nobody"])
        assert "No results" in result.output

    def test_search_shows_iv_score(self, populated_duckdb):
        result = runner.invoke(app, ["search", "荒木"])
        assert "73.0" in result.output


class TestCompareCommand:
    def test_compare_two_persons(self, populated_duckdb):
        result = runner.invoke(app, ["compare", "p1", "p2"])
        assert result.exit_code == 0
        assert "BiRank" in result.output or "birank" in result.output.lower()
        assert "荒木哲郎" in result.output
        assert "今井有文" in result.output

    def test_compare_shows_diff(self, populated_duckdb):
        result = runner.invoke(app, ["compare", "p1", "p2"])
        assert result.exit_code == 0
        # Should show score differences
        assert "+" in result.output or "-" in result.output

    def test_compare_shows_roles(self, populated_duckdb):
        result = runner.invoke(app, ["compare", "p1", "p2"])
        assert result.exit_code == 0
        assert "director" in result.output or "key_animator" in result.output

    def test_compare_person_not_found(self, populated_duckdb):
        result = runner.invoke(app, ["compare", "p1", "nonexistent"])
        assert result.exit_code == 1
        assert "not found" in result.output


class TestHistoryCommand:
    def test_history_shows_scores(self, populated_duckdb_with_history):
        result = runner.invoke(app, ["history", "p1"])
        assert result.exit_code == 0
        assert "73.0" in result.output or "70.0" in result.output
        assert "Score History" in result.output

    def test_history_person_not_found(self, populated_duckdb_with_history):
        result = runner.invoke(app, ["history", "nonexistent"])
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_history_no_history(self, monkeypatch, tmp_path):
        """Person exists in silver but has no score_history in gold."""
        import duckdb

        import src.analysis.gold_writer
        import src.analysis.silver_reader
        from src.analysis.gold_writer import _DDL

        silver_path = tmp_path / "silver_nohist.duckdb"
        gold_path = tmp_path / "gold_nohist.duckdb"

        monkeypatch.setattr(src.analysis.silver_reader, "DEFAULT_SILVER_PATH", silver_path)
        monkeypatch.setattr(src.analysis.gold_writer, "DEFAULT_GOLD_DB_PATH", gold_path)

        sconn = duckdb.connect(str(silver_path))
        sconn.execute(
            """CREATE TABLE persons (
                id VARCHAR PRIMARY KEY, name_ja VARCHAR DEFAULT '', name_en VARCHAR DEFAULT '',
                name_ko VARCHAR DEFAULT '', name_zh VARCHAR DEFAULT '',
                aliases VARCHAR DEFAULT '[]', image_medium VARCHAR
            )"""
        )
        sconn.execute(
            "CREATE TABLE anime (id VARCHAR PRIMARY KEY, title_ja VARCHAR DEFAULT '',"
            " title_en VARCHAR DEFAULT '', year INTEGER)"
        )
        sconn.execute(
            "CREATE TABLE credits (person_id VARCHAR, anime_id VARCHAR, role VARCHAR,"
            " credit_year INTEGER DEFAULT 0, evidence_source VARCHAR DEFAULT '')"
        )
        sconn.execute(
            "INSERT INTO persons(id, name_en) VALUES ('p99', 'No History')"
        )
        sconn.close()

        gconn = duckdb.connect(str(gold_path))
        gconn.execute(_DDL)
        gconn.close()

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
                "iv_score": 70.0,
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
                "iv_score": 70.0,
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
                "birank": 50.0,
                "patronage": 50.0,
                "person_fe": 50.0,
                "iv_score": 50.0,
            }
            for i in range(20)
        ] + [
            {
                "person_id": "p_high",
                "name": "Outlier",
                "birank": 99.0,
                "patronage": 99.0,
                "person_fe": 99.0,
                "iv_score": 99.0,
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
    def test_data_quality_shows_score(self, populated_duckdb):
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
