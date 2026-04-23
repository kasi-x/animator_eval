"""Comprehensive scraper tests for retry, error handling, checkpoint, and edge cases.

Targets 70%+ coverage across:
- anilist_scraper.py (AniListClient, parsers, edge cases)
- mal_scraper.py (JikanClient, parsers, checkpoint)
- mediaarts_scraper.py (JSON-LD dump parser, GitHub download)
- jvmg_fetcher.py (WikidataClient, parsers, checkpoint)
- image_downloader.py (download_image, content validation, retry)
- retry.py (retry_async utility)
- exceptions.py (exception hierarchy)

All async tests use asyncio.run() wrappers since pytest-asyncio is not available.
"""

import asyncio
import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest



def _run(coro):
    """Helper to run async coroutines in sync tests."""
    return asyncio.run(coro)

# ---------------------------------------------------------------------------
# MAL/Jikan scraper tests
# ---------------------------------------------------------------------------


class TestJikanClient:
    """JikanClient now wraps RetryingHttpClient.
    These tests use httpx.MockTransport for end-to-end retry verification."""

    def test_get_success(self):
        from src.scrapers.mal_scraper import JikanClient

        def handler(req):
            return httpx.Response(200, json={"data": [{"mal_id": 1}]})

        client = JikanClient(transport=httpx.MockTransport(handler))
        # Disable throttle for fast tests
        client._http._delay = 0.0

        result = _run(client.get("/test"))
        assert result == {"data": [{"mal_id": 1}]}
        _run(client.close())

    def test_get_429_rate_limit(self):
        from src.scrapers.mal_scraper import JikanClient

        seq = [429, 200]
        idx = {"i": 0}

        def handler(req):
            s = seq[idx["i"]]
            idx["i"] += 1
            if s == 429:
                return httpx.Response(429, headers={"Retry-After": "1"}, text="x")
            return httpx.Response(200, json={"data": []})

        client = JikanClient(transport=httpx.MockTransport(handler))
        client._http._delay = 0.0

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await client.get("/test")

        result = _run(run())
        assert result == {"data": []}
        assert idx["i"] == 2
        _run(client.close())

    def test_get_all_attempts_fail(self):
        from src.scrapers.mal_scraper import JikanClient

        def handler(req):
            raise httpx.ConnectError("down")

        client = JikanClient(transport=httpx.MockTransport(handler))
        client._http._delay = 0.0

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await client.get("/test")

        with pytest.raises(httpx.ConnectError):
            _run(run())
        _run(client.close())

    def test_get_anime_staff(self):
        from src.scrapers.mal_scraper import JikanClient

        client = JikanClient()
        client.get = AsyncMock(return_value={"data": [{"name": "staff1"}]})

        result = _run(client.get_anime_staff(5114))
        assert result == [{"name": "staff1"}]
        _run(client.close())

    def test_get_top_anime(self):
        from src.scrapers.mal_scraper import JikanClient

        client = JikanClient()
        client.get = AsyncMock(return_value={"data": [], "pagination": {}})

        result = _run(client.get_top_anime(page=2, limit=10, type_filter="movie"))
        assert result == {"data": [], "pagination": {}}
        client.get.assert_called_once_with(
            "/top/anime", params={"page": 2, "limit": 10, "type": "movie"}
        )
        _run(client.close())

    def test_get_top_anime_no_type_filter(self):
        from src.scrapers.mal_scraper import JikanClient

        client = JikanClient()
        client.get = AsyncMock(return_value={"data": []})

        _run(client.get_top_anime(type_filter=""))
        call_args = client.get.call_args
        assert "type" not in call_args[1]["params"]
        _run(client.close())


class TestMALParsers:
    def test_parse_anime_data_year_from_aired_prop(self):
        from src.scrapers.mal_scraper import parse_anime_data

        raw = {
            "mal_id": 1,
            "titles": [],
            "title": "Test",
            "year": None,
            "aired": {"prop": {"from": {"year": 2005}}},
        }
        anime = parse_anime_data(raw)
        assert anime.year == 2005

    def test_parse_anime_data_english_fallback(self):
        from src.scrapers.mal_scraper import parse_anime_data

        raw = {
            "mal_id": 1,
            "titles": [{"type": "English", "title": "English Title"}],
            "title": "Default Title",
        }
        anime = parse_anime_data(raw)
        assert anime.title_en == "English Title"

    def test_parse_anime_data_no_titles(self):
        from src.scrapers.mal_scraper import parse_anime_data

        raw = {"mal_id": 1, "titles": [], "title": "Fallback"}
        anime = parse_anime_data(raw)
        assert anime.title_en == "Fallback"

    def test_parse_anime_data_null_aired(self):
        from src.scrapers.mal_scraper import parse_anime_data

        raw = {"mal_id": 1, "titles": [], "title": "Test", "aired": None}
        anime = parse_anime_data(raw)
        assert anime.year is None

    def test_parse_anime_data_null_prop(self):
        from src.scrapers.mal_scraper import parse_anime_data

        raw = {
            "mal_id": 1,
            "titles": [],
            "title": "Test",
            "year": None,
            "aired": {"prop": None},
        }
        anime = parse_anime_data(raw)
        assert anime.year is None

    def test_parse_staff_data_single_name(self):
        """Test name without comma (no split needed)."""
        from src.scrapers.mal_scraper import parse_staff_data

        staff_list = [
            {
                "person": {"mal_id": 10, "name": "CLAMP"},
                "positions": ["Original Creator"],
            }
        ]
        persons, credits = parse_staff_data(staff_list, "mal:1")
        assert persons[0].name_en == "CLAMP"

    def test_parse_staff_data_multiple_positions(self):
        from src.scrapers.mal_scraper import parse_staff_data

        staff_list = [
            {
                "person": {"mal_id": 1, "name": "Doe, John"},
                "positions": ["Director", "Storyboard", "Episode Director"],
            }
        ]
        persons, credits = parse_staff_data(staff_list, "mal:1")
        assert len(persons) == 1
        assert len(credits) == 3

    def test_parse_staff_data_empty_positions(self):
        from src.scrapers.mal_scraper import parse_staff_data

        staff_list = [{"person": {"mal_id": 1, "name": "Doe, John"}, "positions": []}]
        persons, credits = parse_staff_data(staff_list, "mal:1")
        assert len(persons) == 1
        assert len(credits) == 0


class TestMALCheckpoint:
    def test_load_checkpoint_nonexistent(self, tmp_path):
        from src.scrapers.mal_scraper import _load_checkpoint

        result = _load_checkpoint(tmp_path / "nonexistent.json")
        assert result is None

    def test_load_checkpoint_existing(self, tmp_path):
        from src.scrapers.mal_scraper import _load_checkpoint

        cp_file = tmp_path / "checkpoint.json"
        cp_file.write_text(json.dumps({"last_fetched_index": 10}))
        result = _load_checkpoint(cp_file)
        assert result["last_fetched_index"] == 10

    def test_save_checkpoint(self, tmp_path):
        from src.scrapers.mal_scraper import _save_checkpoint

        cp_file = tmp_path / "subdir" / "checkpoint.json"
        _save_checkpoint(cp_file, {"last_fetched_index": 5, "total_anime": 5})
        loaded = json.loads(cp_file.read_text())
        assert loaded["last_fetched_index"] == 5

    def test_delete_checkpoint(self, tmp_path):
        from src.scrapers.mal_scraper import _delete_checkpoint

        cp_file = tmp_path / "checkpoint.json"
        cp_file.write_text("{}")
        _delete_checkpoint(cp_file)
        assert not cp_file.exists()

    def test_delete_checkpoint_nonexistent(self, tmp_path):
        from src.scrapers.mal_scraper import _delete_checkpoint

        # Should not raise
        _delete_checkpoint(tmp_path / "nonexistent.json")


# ---------------------------------------------------------------------------
# MediaArts scraper tests
# ---------------------------------------------------------------------------


class TestMALFetchTopAnimeCredits:
    def test_fetch_top_anime_credits(self):
        from src.scrapers.mal_scraper import fetch_top_anime_credits

        top_anime_response = {
            "data": [
                {
                    "mal_id": 1,
                    "titles": [{"type": "Default", "title": "Test Anime"}],
                    "title": "Test Anime",
                    "year": 2020,
                }
            ]
        }
        staff_response = [
            {
                "person": {"mal_id": 10, "name": "Doe, John"},
                "positions": ["Director"],
            }
        ]

        with patch("src.scrapers.mal_scraper.JikanClient") as MockClient:
            instance = MockClient.return_value
            instance.get_top_anime = AsyncMock(return_value=top_anime_response)
            instance.get_anime_staff = AsyncMock(return_value=staff_response)
            instance.close = AsyncMock()

            anime, persons, credits = _run(fetch_top_anime_credits(n_anime=1))

        assert len(anime) == 1
        assert len(persons) == 1
        assert len(credits) == 1
