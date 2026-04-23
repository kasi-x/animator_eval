"""Tests for backfill_anilist_hometown script."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scripts.maintenance.backfill_anilist_hometown import (
    _fetch_person,
    _query_candidates,
    _update_person,
)
from src.db import get_connection, init_db


@pytest.fixture
def temp_db(tmp_path, monkeypatch):
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.db"
    monkeypatch.setattr("src.db.init.DEFAULT_DB_PATH", db_path)
    monkeypatch.setattr("src.utils.config.DB_PATH", db_path)

    conn = get_connection(db_path)
    init_db(conn)
    yield conn
    conn.close()


@pytest.fixture
def sample_persons(temp_db):
    """Insert sample persons with NULL hometown for testing."""
    temp_db.execute(
        """
        INSERT INTO persons (
            id, name_en, name_ja, anilist_id, hometown, nationality, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        ("p_korean_1", "Kim Song", "", 123456, None, "[]"),
    )
    temp_db.execute(
        """
        INSERT INTO persons (
            id, name_en, name_ja, anilist_id, hometown, nationality, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        ("p_chinese_1", "Li Wei", "", 234567, None, "[]"),
    )
    temp_db.execute(
        """
        INSERT INTO persons (
            id, name_en, name_ja, anilist_id, hometown, nationality, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        ("p_japanese_1", "Yamada Taro", "", 345678, None, "[]"),
    )
    temp_db.commit()


class TestQueryCandidates:
    """Test _query_candidates function."""

    def test_query_candidates_no_results(self, temp_db):
        """Test query when no candidates exist."""
        candidates = _query_candidates(temp_db, 0)
        assert candidates == []

    def test_query_candidates_with_results(self, temp_db, sample_persons):
        """Test query returns persons with NULL hometown."""
        candidates = _query_candidates(temp_db, 0)
        assert len(candidates) == 3
        # Check format: (anilist_id, person_id)
        assert all(isinstance(anilist_id, int) for anilist_id, _ in candidates)
        assert all(isinstance(person_id, str) for _, person_id in candidates)

    def test_query_candidates_with_limit(self, temp_db, sample_persons):
        """Test query respects limit parameter."""
        candidates = _query_candidates(temp_db, 2)
        assert len(candidates) == 2

    def test_query_candidates_skips_non_null_hometown(self, temp_db, sample_persons):
        """Test that persons with non-NULL hometown are excluded."""
        # Insert person with hometown
        temp_db.execute(
            """
            INSERT INTO persons (
                id, name_en, name_ja, anilist_id, hometown, nationality, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            ("p_has_hometown", "Some Name", "", 999999, "Tokyo, Japan", "[]"),
        )
        temp_db.commit()

        candidates = _query_candidates(temp_db, 0)
        assert len(candidates) == 3  # Only the 3 with NULL hometown
        anilist_ids = [anilist_id for anilist_id, _ in candidates]
        assert 999999 not in anilist_ids


def test_fetch_person_success():
    """Test successful fetch of person data from AniList."""
    mock_response = {
        "data": {
            "Staff": {
                "id": 123456,
                "name": {
                    "full": "김송",
                    "native": "김송",
                    "alternative": "Kim Song",
                },
                "homeTown": "Seoul, South Korea",
                "gender": "Male",
            }
        }
    }

    async def run_test():
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(
            return_value=MagicMock(json=lambda: mock_response)
        )
        result = await _fetch_person(mock_client, 123456)
        assert result is not None
        assert result["name"]["native"] == "김송"
        assert result["homeTown"] == "Seoul, South Korea"

    asyncio.run(run_test())


def test_fetch_person_null_hometown():
    """Test fetch when hometown is NULL in response."""
    mock_response = {
        "data": {
            "Staff": {
                "id": 234567,
                "name": {
                    "full": "Li Wei",
                    "native": "李伟",
                    "alternative": None,
                },
                "homeTown": None,
                "gender": "Male",
            }
        }
    }

    async def run_test():
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(
            return_value=MagicMock(json=lambda: mock_response)
        )
        result = await _fetch_person(mock_client, 234567)
        assert result is not None
        assert result["homeTown"] is None

    asyncio.run(run_test())


def test_fetch_person_error(capsys):
    """Test error handling in fetch."""

    async def run_test():
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=Exception("Network error"))
        result = await _fetch_person(mock_client, 999999)
        assert result is None

    asyncio.run(run_test())
    captured = capsys.readouterr()
    assert "fetch error" in captured.err


class TestUpdatePerson:
    """Test _update_person function."""

    def test_update_person_korean_name(self, temp_db, sample_persons):
        """Test that Korean name is correctly routed to name_ko."""
        staff_data = {
            "id": 123456,
            "name": {
                "full": "김송",
                "native": "김송",
                "alternative": "Kim Song",
            },
            "homeTown": "Seoul, South Korea",
        }

        _update_person(temp_db, "p_korean_1", 123456, staff_data)

        result = temp_db.execute(
            "SELECT name_ja, name_ko, name_zh, hometown, nationality FROM persons WHERE id = ?",
            ("p_korean_1",),
        ).fetchone()

        assert result["name_ko"] == "김송"
        assert result["name_ja"] == ""
        assert result["name_zh"] == ""
        assert result["hometown"] == "Seoul, South Korea"
        nationality = json.loads(result["nationality"])
        assert "KR" in nationality

    def test_update_person_chinese_name(self, temp_db, sample_persons):
        """Test that Chinese name is correctly routed to name_zh."""
        staff_data = {
            "id": 234567,
            "name": {
                "full": "Li Wei",
                "native": "李伟",
                "alternative": None,
            },
            "homeTown": "Beijing, China",
        }

        _update_person(temp_db, "p_chinese_1", 234567, staff_data)

        result = temp_db.execute(
            "SELECT name_ja, name_ko, name_zh, hometown, nationality FROM persons WHERE id = ?",
            ("p_chinese_1",),
        ).fetchone()

        assert result["name_zh"] == "李伟"
        assert result["name_ja"] == ""
        assert result["name_ko"] == ""
        assert result["hometown"] == "Beijing, China"
        nationality = json.loads(result["nationality"])
        assert "CN" in nationality

    def test_update_person_japanese_name(self, temp_db, sample_persons):
        """Test that Japanese name is correctly routed to name_ja."""
        staff_data = {
            "id": 345678,
            "name": {
                "full": "Yamada Taro",
                "native": "山田太郎",
                "alternative": None,
            },
            "homeTown": "Tokyo, Japan",
        }

        _update_person(temp_db, "p_japanese_1", 345678, staff_data)

        result = temp_db.execute(
            "SELECT name_ja, name_ko, name_zh, hometown, nationality FROM persons WHERE id = ?",
            ("p_japanese_1",),
        ).fetchone()

        assert result["name_ja"] == "山田太郎"
        assert result["name_ko"] == ""
        assert result["name_zh"] == ""
        assert result["hometown"] == "Tokyo, Japan"
        nationality = json.loads(result["nationality"])
        assert "JP" in nationality

    def test_update_person_preserves_existing_fields(self, temp_db):
        """Test that update preserves existing non-NULL fields."""
        # Insert person with some existing data
        temp_db.execute(
            """
            INSERT INTO persons (
                id, name_en, name_ja, name_ko, anilist_id, hometown, nationality, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            ("p_preserve", "John", "ジョン", "", 555555, None, "[]"),
        )
        temp_db.commit()

        staff_data = {
            "id": 555555,
            "name": {
                "full": "Kim",
                "native": "김",
                "alternative": None,
            },
            "homeTown": "Seoul, South Korea",
        }

        _update_person(temp_db, "p_preserve", 555555, staff_data)

        result = temp_db.execute(
            "SELECT name_ja, name_ko FROM persons WHERE id = ?",
            ("p_preserve",),
        ).fetchone()

        # name_ja should be preserved (was already set)
        assert result["name_ja"] == "ジョン"
        # name_ko should be updated
        assert result["name_ko"] == "김"

    def test_update_person_also_updates_src_anilist(self, temp_db, sample_persons):
        """Test that both persons and src_anilist_persons are updated."""
        # Insert corresponding src_anilist record (note: no updated_at column)
        temp_db.execute(
            """
            INSERT INTO src_anilist_persons (
                anilist_id, name_en, name_ja, hometown, nationality
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (123456, "Kim Song", "", None, "[]"),
        )
        temp_db.commit()

        staff_data = {
            "id": 123456,
            "name": {
                "full": "김송",
                "native": "김송",
                "alternative": "Kim Song",
            },
            "homeTown": "Seoul, South Korea",
        }

        _update_person(temp_db, "p_korean_1", 123456, staff_data)

        # Check src_anilist_persons
        src_result = temp_db.execute(
            "SELECT name_ko, hometown FROM src_anilist_persons WHERE anilist_id = ?",
            (123456,),
        ).fetchone()

        assert src_result["name_ko"] == "김송"
        assert src_result["hometown"] == "Seoul, South Korea"


class TestIntegration:
    """End-to-end integration tests."""

    def test_full_backfill_workflow_dry_run(self, temp_db, sample_persons, capsys):
        """Test full workflow with dry-run (no actual updates)."""
        from scripts.maintenance.backfill_anilist_hometown import _run
        from argparse import Namespace

        # Mock AniList responses
        mock_responses = [
            {
                "data": {
                    "Staff": {
                        "id": 123456,
                        "name": {"full": "Kim", "native": "김송", "alternative": "Kim Song"},
                        "homeTown": "Seoul, South Korea",
                    }
                }
            },
            {
                "data": {
                    "Staff": {
                        "id": 234567,
                        "name": {"full": "Li", "native": "李伟", "alternative": "Li Wei"},
                        "homeTown": "Beijing, China",
                    }
                }
            },
            {
                "data": {
                    "Staff": {
                        "id": 345678,
                        "name": {"full": "Yamada", "native": "山田太郎", "alternative": None},
                        "homeTown": "Tokyo, Japan",
                    }
                }
            },
        ]

        call_count = 0

        async def mock_post(*args, **kwargs):
            nonlocal call_count
            response_data = mock_responses[call_count]
            call_count += 1
            return MagicMock(json=lambda: response_data)

        args = Namespace(dry_run=True, limit=3)

        async def run_test():
            with patch("scripts.maintenance.backfill_anilist_hometown.httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()
                mock_client.post = mock_post
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_client_class.return_value = mock_client

                await _run(args)

        asyncio.run(run_test())

        # Verify DB was NOT updated (dry-run)
        result = temp_db.execute(
            "SELECT name_ko FROM persons WHERE id = ?",
            ("p_korean_1",),
        ).fetchone()
        assert result["name_ko"] == ""  # Should still be empty (dry-run)

        captured = capsys.readouterr()
        assert "[DRY]" in captured.out
        assert "Done" in captured.out
