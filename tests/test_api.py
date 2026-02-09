"""FastAPI サーバーのテスト."""

import json

import pytest
from fastapi.testclient import TestClient

from src.api import app


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def scores_data(tmp_path, monkeypatch):
    """テスト用スコアデータをJSON_DIRに配置."""
    import src.api

    monkeypatch.setattr(src.api, "JSON_DIR", tmp_path)

    scores = [
        {
            "person_id": "p1",
            "name": "Director A",
            "name_ja": "監督A",
            "name_en": "Director A",
            "authority": 80.0,
            "trust": 70.0,
            "skill": 60.0,
            "composite": 71.0,
            "primary_role": "director",
            "total_credits": 10,
            "career": {"first_year": 2010, "latest_year": 2023, "active_years": 14, "highest_stage": 6, "highest_roles": ["director"]},
            "breakdown": {
                "authority": [{"anime_id": "a1", "title": "Anime 1", "year": 2023, "score": 8.5, "role": "director"}],
                "trust": [{"director_id": "p2", "shared_works": 3, "works": ["W1", "W2", "W3"]}],
            },
        },
        {
            "person_id": "p2",
            "name": "Animator B",
            "name_ja": "",
            "name_en": "Animator B",
            "authority": 50.0,
            "trust": 40.0,
            "skill": 55.0,
            "composite": 47.75,
            "primary_role": "animator",
            "total_credits": 5,
            "career": {"first_year": 2015, "latest_year": 2024, "active_years": 10, "highest_stage": 3, "highest_roles": ["key_animator"]},
        },
        {
            "person_id": "p3",
            "name": "Newbie C",
            "name_ja": "",
            "name_en": "Newbie C",
            "authority": 10.0,
            "trust": 5.0,
            "skill": 20.0,
            "composite": 10.75,
            "primary_role": "animator",
            "total_credits": 2,
        },
    ]
    (tmp_path / "scores.json").write_text(json.dumps(scores), encoding="utf-8")

    anime_stats = {
        "a1": {
            "title": "Anime 1",
            "year": 2023,
            "score": 8.5,
            "credit_count": 15,
            "unique_persons": 10,
            "avg_person_score": 60.0,
        },
        "a2": {
            "title": "Anime 2",
            "year": 2020,
            "score": 7.0,
            "credit_count": 8,
            "unique_persons": 6,
            "avg_person_score": 40.0,
        },
    }
    (tmp_path / "anime_stats.json").write_text(json.dumps(anime_stats), encoding="utf-8")

    summary = {
        "generated_at": "2024-01-01T00:00:00",
        "elapsed_seconds": 5.0,
        "data": {"persons": 3, "anime": 2, "credits": 17},
        "scores": {"top_composite": 71.0, "median_composite": 47.75},
    }
    (tmp_path / "summary.json").write_text(json.dumps(summary), encoding="utf-8")

    crossval = {
        "n_folds": 5,
        "holdout_ratio": 0.2,
        "total_credits": 17,
        "avg_rank_correlation": 0.95,
        "min_rank_correlation": 0.88,
        "avg_top10_overlap": 0.9,
        "fold_results": [
            {"fold": 1, "credits_used": 14, "correlation": 0.95, "top10_overlap": 0.9},
        ],
    }
    (tmp_path / "crossval.json").write_text(json.dumps(crossval), encoding="utf-8")

    studios = {
        "MAPPA": {"anime_count": 5, "person_count": 20, "credit_count": 50},
        "ufotable": {"anime_count": 3, "person_count": 15, "credit_count": 30},
    }
    (tmp_path / "studios.json").write_text(json.dumps(studios), encoding="utf-8")

    seasonal = {
        "by_season": {"winter": {"anime_count": 2}, "spring": {"anime_count": 3}},
        "total_with_season": 10,
    }
    (tmp_path / "seasonal.json").write_text(json.dumps(seasonal), encoding="utf-8")

    collaborations = [
        {
            "person_a": "p1", "person_b": "p2", "shared_works": 3,
            "shared_anime": ["a1", "a2", "a3"], "strength_score": 75.0,
        },
    ]
    (tmp_path / "collaborations.json").write_text(json.dumps(collaborations), encoding="utf-8")

    outliers_data = {
        "axis_outliers": {
            "composite": {
                "high": [{"person_id": "p1", "name": "Director A", "value": 71.0, "zscore": 2.8}],
                "low": [],
                "bounds": {"iqr_lower": 5.0, "iqr_upper": 80.0, "mean": 43.0, "std": 30.0},
            }
        },
        "total_outliers": 1,
        "outlier_person_ids": ["p1"],
    }
    (tmp_path / "outliers.json").write_text(json.dumps(outliers_data), encoding="utf-8")

    teams_data = {
        "high_score_teams": [{"anime_id": "a1", "title": "Hit", "year": 2023, "team_size": 5, "anime_score": 8.5}],
        "total_high_score": 1,
        "role_combinations": [],
        "recommended_pairs": [],
        "team_size_stats": {"avg": 5.0},
    }
    (tmp_path / "teams.json").write_text(json.dumps(teams_data), encoding="utf-8")

    growth_json = {
        "trend_summary": {"rising": 1, "stable": 1},
        "total_persons": 2,
        "persons": {
            "p1": {"trend": "rising", "total_credits": 10, "recent_credits": 7, "activity_ratio": 0.7, "career_span": 5},
            "p2": {"trend": "stable", "total_credits": 5, "recent_credits": 3, "activity_ratio": 0.6, "career_span": 8},
        },
    }
    (tmp_path / "growth.json").write_text(json.dumps(growth_json), encoding="utf-8")

    role_flow_data = {
        "nodes": [{"id": "Stage 1", "label": "Stage 1"}, {"id": "Stage 3", "label": "Stage 3"}],
        "links": [{"source": "Stage 1", "target": "Stage 3", "value": 5}],
        "total_transitions": 5,
    }
    (tmp_path / "role_flow.json").write_text(json.dumps(role_flow_data), encoding="utf-8")

    decades_data = {
        "decades": {"2020s": {"credit_count": 50, "unique_persons": 20, "avg_anime_score": 7.5}},
        "year_by_year": {2022: {"credits": 20, "persons": 10}},
    }
    (tmp_path / "decades.json").write_text(json.dumps(decades_data), encoding="utf-8")

    tags_data = {
        "tag_summary": {"veteran": 5, "rising_star": 3},
        "person_tags": {"p1": ["veteran", "hub"], "p2": ["rising_star"]},
    }
    (tmp_path / "tags.json").write_text(json.dumps(tags_data), encoding="utf-8")

    time_series_data = {
        "years": [2020, 2021, 2022],
        "series": {
            "active_persons": {2020: 10, 2021: 15, 2022: 20},
            "credit_count": {2020: 50, 2021: 70, 2022: 90},
        },
        "summary": {"peak_year": 2022, "peak_credits": 90},
    }
    (tmp_path / "time_series.json").write_text(json.dumps(time_series_data), encoding="utf-8")

    bridges_data = {
        "bridge_persons": [
            {"person_id": "p2", "cross_community_edges": 3, "communities_connected": 2, "bridge_score": 60}
        ],
        "cross_community_edges": [
            {"person_a": "p2", "person_b": "p4", "community_a": 0, "community_b": 1, "shared_works": 2}
        ],
        "community_connectivity": {"0-1": 3},
        "stats": {"total_persons": 6, "total_communities": 2, "total_cross_edges": 3, "bridge_person_count": 1},
    }
    (tmp_path / "bridges.json").write_text(json.dumps(bridges_data), encoding="utf-8")

    mentorship_data = {
        "mentorships": [
            {"mentor_id": "p1", "mentee_id": "p2", "shared_works": 5, "stage_gap": 3, "confidence": 80}
        ],
        "tree": {"tree": {"p1": ["p2"]}, "roots": ["p1"]},
        "total": 1,
    }
    (tmp_path / "mentorships.json").write_text(json.dumps(mentorship_data), encoding="utf-8")

    milestones_data = {
        "p1": [
            {"type": "career_start", "year": 2010, "description": "初クレジット: First Show"},
            {"type": "first_director", "year": 2015, "description": "初監督: Big Show"},
        ],
    }
    (tmp_path / "milestones.json").write_text(json.dumps(milestones_data), encoding="utf-8")

    net_evo_data = {
        "years": [2018, 2019, 2020],
        "snapshots": {
            "2018": {"active_persons": 2, "cumulative_persons": 2, "density": 0.5},
            "2019": {"active_persons": 3, "cumulative_persons": 3, "density": 0.33},
            "2020": {"active_persons": 4, "cumulative_persons": 4, "density": 0.5},
        },
        "trends": {"person_growth": 2, "edge_growth": 5},
    }
    (tmp_path / "network_evolution.json").write_text(json.dumps(net_evo_data), encoding="utf-8")

    genre_data = {
        "p1": {"score_tiers": {"high_rated": 66.7}, "eras": {"modern": 50.0}, "primary_tier": "high_rated", "primary_era": "modern", "avg_anime_score": 8.0, "total_credits": 3},
    }
    (tmp_path / "genre_affinity.json").write_text(json.dumps(genre_data), encoding="utf-8")

    prod_data = {
        "p1": {"total_credits": 10, "unique_anime": 8, "active_years": 5, "credits_per_year": 2.0, "peak_year": 2022, "peak_credits": 4, "consistency_score": 1.0},
    }
    (tmp_path / "productivity.json").write_text(json.dumps(prod_data), encoding="utf-8")

    return tmp_path


class TestHealth:
    def test_health_endpoint(self, client):
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "db_exists" in data
        assert "scores_exist" in data


class TestSummary:
    def test_summary_exists(self, client, scores_data):
        resp = client.get("/api/v1/summary")
        assert resp.status_code == 200
        data = resp.json()
        assert "generated_at" in data
        assert "data" in data

    def test_summary_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/summary")
        assert resp.status_code == 404


class TestListPersons:
    def test_list_all(self, client, scores_data):
        resp = client.get("/api/v1/persons")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        assert len(data["items"]) == 3

    def test_pagination(self, client, scores_data):
        resp = client.get("/api/v1/persons?page=1&per_page=2")
        data = resp.json()
        assert len(data["items"]) == 2
        assert data["pages"] == 2

        resp2 = client.get("/api/v1/persons?page=2&per_page=2")
        data2 = resp2.json()
        assert len(data2["items"]) == 1

    def test_sort_by_authority(self, client, scores_data):
        resp = client.get("/api/v1/persons?sort=authority")
        data = resp.json()
        values = [item["authority"] for item in data["items"]]
        assert values == sorted(values, reverse=True)

    def test_invalid_sort(self, client, scores_data):
        resp = client.get("/api/v1/persons?sort=invalid")
        assert resp.status_code == 400


class TestGetPerson:
    def test_found(self, client, scores_data):
        resp = client.get("/api/v1/persons/p1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["person_id"] == "p1"
        assert data["authority"] == 80.0
        assert "breakdown" in data

    def test_not_found(self, client, scores_data):
        resp = client.get("/api/v1/persons/nonexistent")
        assert resp.status_code == 404


class TestSimilar:
    def test_similar_persons(self, client, scores_data):
        resp = client.get("/api/v1/persons/p1/similar?top_n=2")
        assert resp.status_code == 200
        data = resp.json()
        assert data["person_id"] == "p1"
        assert len(data["similar"]) <= 2

    def test_similar_not_found(self, client, scores_data):
        resp = client.get("/api/v1/persons/nonexistent/similar")
        assert resp.status_code == 404


class TestRanking:
    def test_ranking_default(self, client, scores_data):
        resp = client.get("/api/v1/ranking")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3

    def test_ranking_role_filter(self, client, scores_data):
        resp = client.get("/api/v1/ranking?role=director")
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["person_id"] == "p1"

    def test_ranking_sort(self, client, scores_data):
        resp = client.get("/api/v1/ranking?sort=trust")
        data = resp.json()
        values = [item["trust"] for item in data["items"]]
        assert values == sorted(values, reverse=True)


class TestAnime:
    def test_list_anime(self, client, scores_data):
        resp = client.get("/api/v1/anime")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2

    def test_get_anime(self, client, scores_data):
        resp = client.get("/api/v1/anime/a1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["title"] == "Anime 1"

    def test_anime_not_found(self, client, scores_data):
        resp = client.get("/api/v1/anime/nonexistent")
        assert resp.status_code == 404

    def test_anime_sort_by_year(self, client, scores_data):
        resp = client.get("/api/v1/anime?sort=year")
        data = resp.json()
        assert data["items"][0]["year"] >= data["items"][1]["year"]


class TestStudios:
    def test_studios_exists(self, client, scores_data):
        resp = client.get("/api/v1/studios")
        assert resp.status_code == 200
        data = resp.json()
        assert "MAPPA" in data

    def test_studios_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/studios")
        assert resp.status_code == 404


class TestSeasonal:
    def test_seasonal_exists(self, client, scores_data):
        resp = client.get("/api/v1/seasonal")
        assert resp.status_code == 200
        data = resp.json()
        assert "by_season" in data

    def test_seasonal_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/seasonal")
        assert resp.status_code == 404


class TestCrossval:
    def test_crossval_exists(self, client, scores_data):
        resp = client.get("/api/v1/crossval")
        assert resp.status_code == 200
        data = resp.json()
        assert data["avg_rank_correlation"] == 0.95
        assert "fold_results" in data

    def test_crossval_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/crossval")
        assert resp.status_code == 404


class TestCollaborations:
    def test_collaborations_exists(self, client, scores_data):
        resp = client.get("/api/v1/collaborations")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 1
        assert data["items"][0]["person_a"] == "p1"

    def test_collaborations_filter_by_person(self, client, scores_data):
        resp = client.get("/api/v1/collaborations?person_id=p1")
        data = resp.json()
        assert data["total"] >= 1

    def test_collaborations_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/collaborations")
        assert resp.status_code == 404


class TestOutliers:
    def test_outliers_exists(self, client, scores_data):
        resp = client.get("/api/v1/outliers")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_outliers"] == 1

    def test_outliers_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/outliers")
        assert resp.status_code == 404


class TestTeams:
    def test_teams_exists(self, client, scores_data):
        resp = client.get("/api/v1/teams")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_high_score"] == 1

    def test_teams_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/teams")
        assert resp.status_code == 404


class TestGrowth:
    def test_growth_exists(self, client, scores_data):
        resp = client.get("/api/v1/growth")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2

    def test_growth_filter_by_trend(self, client, scores_data):
        resp = client.get("/api/v1/growth?trend=rising")
        data = resp.json()
        assert data["total"] == 1

    def test_growth_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/growth")
        assert resp.status_code == 404


class TestTimeSeries:
    def test_time_series_exists(self, client, scores_data):
        resp = client.get("/api/v1/time-series")
        assert resp.status_code == 200
        data = resp.json()
        assert data["years"] == [2020, 2021, 2022]

    def test_time_series_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/time-series")
        assert resp.status_code == 404


class TestDecades:
    def test_decades_exists(self, client, scores_data):
        resp = client.get("/api/v1/decades")
        assert resp.status_code == 200
        data = resp.json()
        assert "2020s" in data["decades"]

    def test_decades_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/decades")
        assert resp.status_code == 404


class TestTags:
    def test_tags_exists(self, client, scores_data):
        resp = client.get("/api/v1/tags")
        assert resp.status_code == 200
        data = resp.json()
        assert "tag_summary" in data

    def test_tags_filter(self, client, scores_data):
        resp = client.get("/api/v1/tags?tag=veteran")
        data = resp.json()
        assert data["count"] == 1
        assert "p1" in data["persons"]

    def test_tags_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/tags")
        assert resp.status_code == 404


class TestRoleFlow:
    def test_role_flow_exists(self, client, scores_data):
        resp = client.get("/api/v1/role-flow")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_transitions"] == 5

    def test_role_flow_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/role-flow")
        assert resp.status_code == 404


class TestCompare:
    def test_compare_two(self, client, scores_data):
        resp = client.get("/api/v1/compare?ids=p1,p2")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["persons"]) == 2

    def test_compare_invalid(self, client, scores_data):
        resp = client.get("/api/v1/compare?ids=p1")
        assert resp.status_code == 400


class TestDataQuality:
    def test_data_quality(self, client, tmp_path, monkeypatch):
        import src.database

        db_path = tmp_path / "quality.db"
        monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", db_path)

        from src.database import get_connection, init_db

        conn = get_connection()
        init_db(conn)
        conn.execute("INSERT INTO anime (id, title_en, year, score) VALUES ('a1', 'Test', 2024, 8.0)")
        conn.execute("INSERT INTO persons (id, name_en) VALUES ('p1', 'Person')")
        conn.execute("INSERT INTO credits (person_id, anime_id, role, source) VALUES ('p1', 'a1', 'director', 'test')")
        conn.commit()
        conn.close()

        resp = client.get("/api/v1/data-quality")
        assert resp.status_code == 200
        data = resp.json()
        assert "overall_score" in data
        assert "dimensions" in data


class TestPersonNetwork:
    def test_network(self, client, scores_data, monkeypatch, tmp_path):
        import src.database

        db_path = tmp_path / "net.db"
        monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", db_path)

        from src.database import get_connection, init_db

        conn = get_connection()
        init_db(conn)
        conn.execute("INSERT INTO persons (id, name_en) VALUES ('p1', 'Director')")
        conn.execute("INSERT INTO persons (id, name_en) VALUES ('p2', 'Animator')")
        conn.execute("INSERT INTO anime (id, title_en) VALUES ('a1', 'Test')")
        conn.execute("INSERT INTO credits (person_id, anime_id, role, source) VALUES ('p1', 'a1', 'director', 'test')")
        conn.execute("INSERT INTO credits (person_id, anime_id, role, source) VALUES ('p2', 'a1', 'key_animator', 'test')")
        conn.commit()
        conn.close()

        resp = client.get("/api/v1/persons/p1/network")
        assert resp.status_code == 200
        data = resp.json()
        assert data["center"] == "p1"
        assert data["total_nodes"] >= 1


class TestRecommend:
    def test_recommend(self, client, scores_data, monkeypatch, tmp_path):
        import src.database

        db_path = tmp_path / "rec.db"
        monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", db_path)

        from src.database import get_connection, init_db

        conn = get_connection()
        init_db(conn)
        conn.execute("INSERT INTO persons (id, name_en) VALUES ('p1', 'Director A')")
        conn.execute("INSERT INTO persons (id, name_en) VALUES ('p2', 'Animator B')")
        conn.execute("INSERT INTO anime (id, title_en) VALUES ('a1', 'Test')")
        conn.execute("INSERT INTO credits (person_id, anime_id, role, source) VALUES ('p1', 'a1', 'director', 'test')")
        conn.execute("INSERT INTO credits (person_id, anime_id, role, source) VALUES ('p2', 'a1', 'key_animator', 'test')")
        conn.commit()
        conn.close()

        resp = client.get("/api/v1/recommend?team=p1")
        assert resp.status_code == 200
        data = resp.json()
        assert "recommendations" in data


class TestPredict:
    def test_predict(self, client, scores_data, monkeypatch, tmp_path):
        import src.database

        db_path = tmp_path / "pred.db"
        monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", db_path)

        from src.database import get_connection, init_db

        conn = get_connection()
        init_db(conn)
        conn.execute("INSERT INTO persons (id, name_en) VALUES ('p1', 'Director')")
        conn.execute("INSERT INTO anime (id, title_en, score) VALUES ('a1', 'Test', 8.0)")
        conn.execute("INSERT INTO credits (person_id, anime_id, role, source) VALUES ('p1', 'a1', 'director', 'test')")
        conn.commit()
        conn.close()

        resp = client.get("/api/v1/predict?team=p1")
        assert resp.status_code == 200
        data = resp.json()
        assert "predicted_score" in data


class TestDbStats:
    def test_stats(self, client, tmp_path, monkeypatch):
        """DB統計は実際のDBが必要なので基本的な接続テスト."""
        import src.database

        db_path = tmp_path / "test_api.db"
        monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", db_path)

        from src.database import get_connection, init_db

        conn = get_connection()
        init_db(conn)
        conn.close()

        resp = client.get("/api/v1/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "stats" in data
        assert "data_sources" in data


class TestBridgesApi:
    def test_bridges_exists(self, client, scores_data):
        resp = client.get("/api/v1/bridges")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["bridge_persons"]) == 1
        assert data["stats"]["total_communities"] == 2

    def test_bridges_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/bridges")
        assert resp.status_code == 404


class TestMentorshipsApi:
    def test_mentorships_exists(self, client, scores_data):
        resp = client.get("/api/v1/mentorships")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["mentorships"][0]["mentor_id"] == "p1"

    def test_mentorships_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/mentorships")
        assert resp.status_code == 404


class TestMilestonesApi:
    def test_milestones_exists(self, client, scores_data):
        resp = client.get("/api/v1/persons/p1/milestones")
        assert resp.status_code == 200
        data = resp.json()
        assert data["person_id"] == "p1"
        assert len(data["milestones"]) == 2

    def test_milestones_not_found(self, client, scores_data):
        resp = client.get("/api/v1/persons/nonexistent/milestones")
        assert resp.status_code == 404

    def test_milestones_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/persons/p1/milestones")
        assert resp.status_code == 404


class TestNetworkEvolutionApi:
    def test_net_evo_exists(self, client, scores_data):
        resp = client.get("/api/v1/network-evolution")
        assert resp.status_code == 200
        data = resp.json()
        assert data["years"] == [2018, 2019, 2020]

    def test_net_evo_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/network-evolution")
        assert resp.status_code == 404


class TestGenreAffinityApi:
    def test_genre_exists(self, client, scores_data):
        resp = client.get("/api/v1/genre-affinity")
        assert resp.status_code == 200
        data = resp.json()
        assert "p1" in data

    def test_genre_by_person(self, client, scores_data):
        resp = client.get("/api/v1/genre-affinity?person_id=p1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["person_id"] == "p1"
        assert data["primary_tier"] == "high_rated"

    def test_genre_person_not_found(self, client, scores_data):
        resp = client.get("/api/v1/genre-affinity?person_id=nonexistent")
        assert resp.status_code == 404

    def test_genre_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/genre-affinity")
        assert resp.status_code == 404


class TestProductivityApi:
    def test_prod_exists(self, client, scores_data):
        resp = client.get("/api/v1/productivity")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["person_id"] == "p1"

    def test_prod_missing(self, client, tmp_path, monkeypatch):
        import src.api

        monkeypatch.setattr(src.api, "JSON_DIR", tmp_path / "empty")
        resp = client.get("/api/v1/productivity")
        assert resp.status_code == 404
