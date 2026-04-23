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

from src.runtime.models import Role


def _run(coro):
    """Helper to run async coroutines in sync tests."""
    return asyncio.run(coro)

# ---------------------------------------------------------------------------
# JVMG / Wikidata fetcher tests
# ---------------------------------------------------------------------------


class TestWikidataClient:
    """WikidataClient now wraps RetryingHttpClient (MockTransport-friendly)."""

    def test_query_success(self):
        from src.scrapers.jvmg_fetcher import WikidataClient

        def handler(req):
            return httpx.Response(
                200,
                json={"results": {"bindings": [{"anime": {"value": "http://wd/Q1"}}]}},
            )

        client = WikidataClient(transport=httpx.MockTransport(handler))
        client._http._delay = 0.0

        result = _run(client.query("SELECT ?anime WHERE {}"))
        assert len(result) == 1
        _run(client.close())

    def test_query_429_rate_limit(self):
        from src.scrapers.jvmg_fetcher import WikidataClient

        seq = [429, 200]
        idx = {"i": 0}

        def handler(req):
            s = seq[idx["i"]]
            idx["i"] += 1
            if s == 429:
                return httpx.Response(429, headers={"Retry-After": "1"}, text="x")
            return httpx.Response(200, json={"results": {"bindings": []}})

        client = WikidataClient(transport=httpx.MockTransport(handler))
        client._http._delay = 0.0

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await client.query("SELECT ?x WHERE {}")

        result = _run(run())
        assert result == []
        _run(client.close())

    def test_query_all_attempts_fail(self):
        from src.scrapers.jvmg_fetcher import WikidataClient

        def handler(req):
            raise httpx.ConnectError("timeout")

        client = WikidataClient(transport=httpx.MockTransport(handler))
        client._http._delay = 0.0

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await client.query("SELECT ?x WHERE {}")

        with pytest.raises(httpx.ConnectError):
            _run(run())
        _run(client.close())


class TestWikidataParsers:
    def test_parse_wikidata_results(self):
        from src.scrapers.jvmg_fetcher import parse_wikidata_results

        bindings = [
            {
                "anime": {"value": "http://wikidata.org/entity/Q100"},
                "animeLabel": {"value": "Cowboy Bebop"},
                "year": {"value": "1998"},
                "person": {"value": "http://wikidata.org/entity/Q200"},
                "personLabel": {"value": "Shinichiro Watanabe"},
                "personLabelJa": {"value": "渡辺信一郎"},
                "role": {"value": "director"},
            }
        ]
        anime_list, persons, credits = parse_wikidata_results(bindings)
        assert len(anime_list) == 1
        assert anime_list[0].id == "wd:Q100"
        assert anime_list[0].title_en == "Cowboy Bebop"
        assert anime_list[0].year == 1998
        assert len(persons) == 1
        assert persons[0].id == "wd:pQ200"
        assert persons[0].name_ja == "渡辺信一郎"
        assert credits[0].role == Role.DIRECTOR

    def test_parse_wikidata_results_missing_role(self):
        from src.scrapers.jvmg_fetcher import parse_wikidata_results

        bindings = [
            {
                "anime": {"value": "http://wd/Q1"},
                "animeLabel": {"value": "Test"},
                "person": {"value": "http://wd/Q2"},
                "personLabel": {"value": "Person"},
                "role": {"value": ""},
            }
        ]
        anime_list, persons, credits = parse_wikidata_results(bindings)
        assert credits[0].role == Role.SPECIAL

    def test_parse_wikidata_results_missing_uri(self):
        from src.scrapers.jvmg_fetcher import parse_wikidata_results

        bindings = [{"anime": {"value": ""}, "person": {"value": "http://wd/Q1"}}]
        anime_list, persons, credits = parse_wikidata_results(bindings)
        assert len(anime_list) == 0

    def test_parse_wikidata_results_float_year(self):
        from src.scrapers.jvmg_fetcher import parse_wikidata_results

        bindings = [
            {
                "anime": {"value": "http://wd/Q1"},
                "animeLabel": {"value": "Test"},
                "year": {"value": "2005.0"},
                "person": {"value": "http://wd/Q2"},
                "personLabel": {"value": "Person"},
                "role": {"value": "director"},
            }
        ]
        anime_list, _, _ = parse_wikidata_results(bindings)
        assert anime_list[0].year == 2005

    def test_parse_wikidata_results_deduplication(self):
        from src.scrapers.jvmg_fetcher import parse_wikidata_results

        bindings = [
            {
                "anime": {"value": "http://wd/Q1"},
                "animeLabel": {"value": "Test"},
                "person": {"value": "http://wd/Q2"},
                "personLabel": {"value": "Person"},
                "role": {"value": "director"},
            },
            {
                "anime": {"value": "http://wd/Q1"},
                "animeLabel": {"value": "Test"},
                "person": {"value": "http://wd/Q2"},
                "personLabel": {"value": "Person"},
                "role": {"value": "episode_director"},
            },
        ]
        anime_list, persons, credits = parse_wikidata_results(bindings)
        assert len(anime_list) == 1
        assert len(persons) == 1
        assert len(credits) == 2

    def test_parse_wikidata_results_empty_year(self):
        from src.scrapers.jvmg_fetcher import parse_wikidata_results

        bindings = [
            {
                "anime": {"value": "http://wd/Q1"},
                "animeLabel": {"value": "Test"},
                "year": {"value": ""},
                "person": {"value": "http://wd/Q2"},
                "personLabel": {"value": "Person"},
                "role": {"value": "director"},
            }
        ]
        anime_list, _, _ = parse_wikidata_results(bindings)
        assert anime_list[0].year is None


class TestJVMGCheckpoint:
    def test_load_checkpoint_nonexistent(self, tmp_path):
        from src.scrapers.jvmg_fetcher import _load_checkpoint

        result = _load_checkpoint(tmp_path / "nonexistent.json")
        assert result is None

    def test_load_checkpoint_existing(self, tmp_path):
        from src.scrapers.jvmg_fetcher import _load_checkpoint

        cp_file = tmp_path / "checkpoint.json"
        cp_file.write_text(json.dumps({"last_offset": 500}))
        result = _load_checkpoint(cp_file)
        assert result["last_offset"] == 500

    def test_save_checkpoint(self, tmp_path):
        from src.scrapers.jvmg_fetcher import _save_checkpoint

        cp_file = tmp_path / "sub" / "checkpoint.json"
        _save_checkpoint(cp_file, {"last_offset": 1000})
        loaded = json.loads(cp_file.read_text())
        assert loaded["last_offset"] == 1000

    def test_delete_checkpoint(self, tmp_path):
        from src.scrapers.jvmg_fetcher import _delete_checkpoint

        cp_file = tmp_path / "checkpoint.json"
        cp_file.write_text("{}")
        _delete_checkpoint(cp_file)
        assert not cp_file.exists()

    def test_delete_checkpoint_nonexistent(self, tmp_path):
        from src.scrapers.jvmg_fetcher import _delete_checkpoint

        _delete_checkpoint(tmp_path / "nonexistent.json")  # Should not raise


# ---------------------------------------------------------------------------
# Image downloader tests
# ---------------------------------------------------------------------------


class TestJVMGFetchAnimeStaff:
    def test_fetch_anime_staff_pagination(self):
        from src.scrapers.jvmg_fetcher import fetch_anime_staff

        page1 = [
            {
                "anime": {"value": f"http://wd/Q{i}"},
                "animeLabel": {"value": f"Anime {i}"},
                "person": {"value": f"http://wd/P{i}"},
                "personLabel": {"value": f"Person {i}"},
                "role": {"value": "director"},
            }
            for i in range(500)
        ]
        page2 = []  # Empty signals end of pagination

        with patch("src.scrapers.jvmg_fetcher.WikidataClient") as MockClient:
            instance = MockClient.return_value
            instance.query = AsyncMock(side_effect=[page1, page2])
            instance.close = AsyncMock()

            anime, persons, credits = _run(fetch_anime_staff(max_records=1000))

        assert len(credits) == 500


