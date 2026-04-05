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
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.models import Role
from src.scrapers.exceptions import (
    AuthenticationError,
    ContentValidationError,
    DataParseError,
    EndpointUnreachableError,
    RateLimitError,
    ScraperError,
)


def _run(coro):
    """Helper to run async coroutines in sync tests."""
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Exception hierarchy tests
# ---------------------------------------------------------------------------


class TestExceptions:
    def test_scraper_error_attributes(self):
        err = ScraperError(
            "boom", source="anilist", url="http://x", metadata={"k": "v"}
        )
        assert str(err) == "boom"
        assert err.source == "anilist"
        assert err.url == "http://x"
        assert err.metadata == {"k": "v"}

    def test_scraper_error_defaults(self):
        err = ScraperError("default")
        assert err.source == ""
        assert err.url == ""
        assert err.metadata == {}

    def test_rate_limit_error(self):
        err = RateLimitError(source="mal", retry_after=30.0)
        assert err.retry_after == 30.0
        assert isinstance(err, ScraperError)

    def test_rate_limit_error_defaults(self):
        err = RateLimitError()
        assert err.retry_after == 60.0
        assert str(err) == "Rate limited"

    def test_authentication_error(self):
        err = AuthenticationError("bad token", source="anilist")
        assert isinstance(err, ScraperError)
        assert err.source == "anilist"

    def test_data_parse_error(self):
        err = DataParseError("parse fail", source="mal")
        assert isinstance(err, ScraperError)

    def test_endpoint_unreachable_error(self):
        err = EndpointUnreachableError("down", source="wikidata", url="http://wd")
        assert isinstance(err, ScraperError)
        assert err.url == "http://wd"

    def test_content_validation_error(self):
        err = ContentValidationError("bad content", source="anilist")
        assert isinstance(err, ScraperError)


# ---------------------------------------------------------------------------
# retry_async tests
# ---------------------------------------------------------------------------


class TestRetryAsync:
    def test_success_on_first_attempt(self):
        from src.scrapers.retry import retry_async

        async def ok():
            return 42

        result = _run(retry_async(ok, max_attempts=3, source="test"))
        assert result == 42

    def test_success_after_transient_failure(self):
        from src.scrapers.retry import retry_async

        call_count = 0

        async def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ScraperError("transient", source="test")
            return "ok"

        result = _run(
            retry_async(flaky, max_attempts=5, base_delay=0.01, source="test")
        )
        assert result == "ok"
        assert call_count == 3

    def test_all_retries_exhausted(self):
        from src.scrapers.retry import retry_async

        async def always_fail():
            raise ScraperError("permanent", source="test")

        with pytest.raises(EndpointUnreachableError) as exc_info:
            _run(
                retry_async(always_fail, max_attempts=2, base_delay=0.01, source="test")
            )
        assert "Failed after 2 attempts" in str(exc_info.value)

    def test_rate_limit_error_respects_retry_after(self):
        from src.scrapers.retry import retry_async

        call_count = 0

        async def rate_limited_then_ok():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RateLimitError(source="test", retry_after=0.01)
            return "success"

        result = _run(
            retry_async(
                rate_limited_then_ok, max_attempts=3, base_delay=0.01, source="test"
            )
        )
        assert result == "success"
        assert call_count == 2

    def test_passes_args_and_kwargs(self):
        from src.scrapers.retry import retry_async

        async def add(a, b, extra=0):
            return a + b + extra

        result = _run(retry_async(add, 1, 2, extra=10, max_attempts=1, source="test"))
        assert result == 13


# ---------------------------------------------------------------------------
# AniList scraper tests
# ---------------------------------------------------------------------------


class TestAniListClient:
    def test_init_without_token(self):
        with patch("src.scrapers.anilist_scraper._env", {}):
            from src.scrapers.anilist_scraper import AniListClient

            client = AniListClient()
            assert client._access_token is None
            assert client.requests_remaining is None
            _run(client.close())

    def test_init_with_token(self):
        with patch(
            "src.scrapers.anilist_scraper._env", {"ANILIST_ACCESS_TOKEN": "test-token"}
        ):
            from src.scrapers.anilist_scraper import AniListClient

            client = AniListClient()
            assert client._access_token == "test-token"
            _run(client.close())

    def test_query_success(self):
        from src.scrapers.anilist_scraper import AniListClient

        mock_response = httpx.Response(
            200,
            json={"data": {"Page": {"media": []}}},
            headers={
                "X-RateLimit-Remaining": "89",
                "X-RateLimit-Reset": "0",
                "X-RateLimit-Limit": "90",
            },
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        client = AniListClient()
        client._client = AsyncMock()
        client._client.post = AsyncMock(return_value=mock_response)
        client._last_request_time = 0.0

        result = _run(client.query("query {}", {}))
        assert result == {"Page": {"media": []}}
        assert client.requests_remaining == 89
        assert client.rate_limit_max == 90
        _run(client.close())

    def test_query_429_rate_limit(self):
        """Test that 429 response triggers retry."""
        from src.scrapers.anilist_scraper import AniListClient

        rate_limit_response = httpx.Response(
            429,
            headers={"Retry-After": "1"},
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )
        success_response = httpx.Response(
            200,
            json={"data": {"result": "ok"}},
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        # probe response after 429 wait
        probe_response = httpx.Response(
            200,
            json={"data": {}},
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        client = AniListClient()
        client._client = AsyncMock()
        client._client.post = AsyncMock(
            side_effect=[rate_limit_response, probe_response, success_response]
        )

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await client.query("query {}", {})

        result = _run(run())
        assert result == {"result": "ok"}
        assert client._client.post.call_count == 3  # 429 + probe + success
        _run(client.close())

    def test_query_429_with_callback(self):
        """Test on_rate_limit callback is called during 429 wait."""
        from src.scrapers.anilist_scraper import AniListClient

        rate_limit_response = httpx.Response(
            429,
            headers={"Retry-After": "1"},
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )
        success_response = httpx.Response(
            200,
            json={"data": {"ok": True}},
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        probe_response = httpx.Response(
            200,
            json={"data": {}},
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        callback_calls = []

        client = AniListClient()
        client._client = AsyncMock()
        client._client.post = AsyncMock(
            side_effect=[rate_limit_response, probe_response, success_response]
        )
        client.on_rate_limit = lambda secs: callback_calls.append(secs)

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await client.query("query {}", {})

        _run(run())
        # Callback should have been called with seconds and then None
        assert None in callback_calls
        _run(client.close())

    def test_query_auth_error_fallback(self):
        """Test that 401 with token-related error disables auth and retries."""
        from src.scrapers.anilist_scraper import AniListClient

        auth_error_response = httpx.Response(
            401,
            json={"errors": [{"message": "Invalid token"}]},
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )
        success_response = httpx.Response(
            200,
            json={"data": {"ok": True}},
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        client = AniListClient()
        client._access_token = "bad-token"
        client._client = AsyncMock()
        client._client.post = AsyncMock(
            side_effect=[auth_error_response, success_response]
        )

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await client.query("query {}", {})

        result = _run(run())
        assert result == {"ok": True}
        assert client._access_token is None  # Token was disabled
        _run(client.close())

    def test_query_graphql_errors_logged(self):
        """Test that GraphQL-level errors are logged but data is returned."""
        from src.scrapers.anilist_scraper import AniListClient

        response = httpx.Response(
            200,
            json={
                "data": {"partial": True},
                "errors": [{"message": "field not found"}],
            },
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        client = AniListClient()
        client._client = AsyncMock()
        client._client.post = AsyncMock(return_value=response)

        result = _run(client.query("query {}", {}))
        assert result == {"partial": True}
        _run(client.close())

    def test_query_all_attempts_fail_raises(self):
        """Test EndpointUnreachableError after all 5 attempts fail."""
        from src.scrapers.anilist_scraper import AniListClient

        client = AniListClient()
        client._client = AsyncMock()
        client._client.post = AsyncMock(
            side_effect=httpx.ConnectError("connection refused")
        )

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await client.query("query {}", {})

        with pytest.raises(EndpointUnreachableError) as exc_info:
            _run(run())
        assert "5 attempts" in str(exc_info.value)
        _run(client.close())

    def test_get_top_anime(self):
        from src.scrapers.anilist_scraper import AniListClient

        client = AniListClient()
        client.query = AsyncMock(return_value={"Page": {"media": []}})

        result = _run(client.get_top_anime(page=2, per_page=10))
        assert result == {"Page": {"media": []}}
        client.query.assert_called_once()
        call_args = client.query.call_args
        assert call_args[0][1]["page"] == 2
        assert call_args[0][1]["perPage"] == 10
        _run(client.close())

    def test_get_top_anime_default_sort(self):
        from src.scrapers.anilist_scraper import AniListClient

        client = AniListClient()
        client.query = AsyncMock(return_value={"Page": {"media": []}})

        _run(client.get_top_anime())
        call_args = client.query.call_args
        assert call_args[0][1]["sort"] == ["POPULARITY_DESC"]
        _run(client.close())

    def test_get_anime_staff(self):
        from src.scrapers.anilist_scraper import AniListClient

        client = AniListClient()
        client.query = AsyncMock(return_value={"Media": {}})

        result = _run(client.get_anime_staff(12345, staff_page=2))
        assert result == {"Media": {}}
        _run(client.close())

    def test_get_anime_staff_minimal(self):
        from src.scrapers.anilist_scraper import AniListClient

        client = AniListClient()
        client.query = AsyncMock(return_value={"Media": {}})

        result = _run(client.get_anime_staff_minimal(12345))
        assert result == {"Media": {}}
        _run(client.close())

    def test_get_person_details(self):
        from src.scrapers.anilist_scraper import AniListClient

        client = AniListClient()
        client.query = AsyncMock(return_value={"Staff": {"id": 100}})

        result = _run(client.get_person_details(100))
        assert result == {"Staff": {"id": 100}}
        _run(client.close())

    def test_query_no_rate_limit_headers(self):
        """Test query works when no rate limit headers present."""
        from src.scrapers.anilist_scraper import AniListClient

        response = httpx.Response(
            200,
            json={"data": {"ok": True}},
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        client = AniListClient()
        client._client = AsyncMock()
        client._client.post = AsyncMock(return_value=response)

        result = _run(client.query("query {}", {}))
        assert result == {"ok": True}
        assert client.requests_remaining is None
        _run(client.close())

    def test_query_auth_error_non_token_related(self):
        """Test 400 error without token keywords does NOT disable auth.

        The 400 response's raise_for_status() raises HTTPStatusError, which is
        caught by the generic except httpx.HTTPError handler. After all 5 retries
        are exhausted, EndpointUnreachableError is raised.
        """
        from src.scrapers.anilist_scraper import AniListClient

        # 400 but error message does not contain "token"/"auth"/"invalid"
        error_response = httpx.Response(
            400,
            json={"errors": [{"message": "Syntax error in query"}]},
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        client = AniListClient()
        client._access_token = "some-token"
        client._client = AsyncMock()
        client._client.post = AsyncMock(return_value=error_response)

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await client.query("query {}", {})

        # After 5 retries with the same 400 error, EndpointUnreachableError is raised
        with pytest.raises(EndpointUnreachableError):
            _run(run())
        # Token should NOT be disabled (error was not auth-related)
        assert client._access_token == "some-token"
        _run(client.close())


class TestAniListParsers:
    def test_parse_anilist_person_full_data(self):
        from src.scrapers.anilist_scraper import parse_anilist_person

        staff = {
            "id": 100,
            "name": {
                "full": "Tetsuro Araki",
                "native": "荒木哲郎",
                "alternative": ["Araki T.", None, ""],
            },
            "image": {"large": "http://img/large.jpg", "medium": "http://img/med.jpg"},
            "dateOfBirth": {"year": 1976, "month": 11, "day": 21},
            "age": 47,
            "gender": "Male",
            "yearsActive": [2000, 2024],
            "homeTown": "Sayama, Saitama, Japan",
            "bloodType": "A",
            "description": "Japanese anime director",
            "favourites": 500,
            "siteUrl": "https://anilist.co/staff/100",
        }
        person = parse_anilist_person(staff)
        assert person.id == "anilist:p100"
        assert person.name_ja == "荒木哲郎"
        assert person.name_en == "Tetsuro Araki"
        assert "Araki T." in person.aliases
        assert "" not in person.aliases  # Filtered
        assert person.image_large == "http://img/large.jpg"
        assert person.date_of_birth == "1976-11-21"
        assert person.age == 47
        assert person.years_active == [2000, 2024]
        assert person.favourites == 500

    def test_parse_anilist_person_missing_id_raises(self):
        from src.scrapers.anilist_scraper import parse_anilist_person

        with pytest.raises(ValueError, match="Person ID is required"):
            parse_anilist_person({})

    def test_parse_anilist_person_minimal(self):
        from src.scrapers.anilist_scraper import parse_anilist_person

        staff = {"id": 999, "name": {}}
        person = parse_anilist_person(staff)
        assert person.id == "anilist:p999"
        assert person.name_ja == ""
        assert person.name_en == ""
        assert person.aliases == []
        assert person.date_of_birth is None
        assert person.image_large is None

    def test_parse_anilist_person_dob_missing_month_day(self):
        from src.scrapers.anilist_scraper import parse_anilist_person

        staff = {"id": 1, "dateOfBirth": {"year": 1990, "month": None, "day": None}}
        person = parse_anilist_person(staff)
        assert person.date_of_birth == "1990-01-01"

    def test_parse_anilist_person_dob_no_year(self):
        from src.scrapers.anilist_scraper import parse_anilist_person

        staff = {"id": 1, "dateOfBirth": {"year": None, "month": 5, "day": 10}}
        person = parse_anilist_person(staff)
        assert person.date_of_birth is None

    def test_parse_anilist_person_empty_years_active(self):
        from src.scrapers.anilist_scraper import parse_anilist_person

        staff = {"id": 1, "yearsActive": None}
        person = parse_anilist_person(staff)
        assert person.years_active == []

    def test_parse_anilist_person_years_active_with_nulls(self):
        from src.scrapers.anilist_scraper import parse_anilist_person

        staff = {"id": 1, "yearsActive": [2000, None, 2020]}
        person = parse_anilist_person(staff)
        assert person.years_active == [2000, 2020]

    def test_parse_anilist_person_empty_dob_object(self):
        from src.scrapers.anilist_scraper import parse_anilist_person

        staff = {"id": 1, "dateOfBirth": {}}
        person = parse_anilist_person(staff)
        assert person.date_of_birth is None

    def test_parse_anilist_anime_full_data(self):
        from src.scrapers.anilist_scraper import parse_anilist_anime

        raw = {
            "id": 16498,
            "title": {
                "romaji": "Shingeki no Kyojin",
                "english": "Attack on Titan",
                "native": "進撃の巨人",
            },
            "seasonYear": 2013,
            "season": "SPRING",
            "episodes": 25,
            "averageScore": 84,
            "coverImage": {
                "large": "http://cover/l.jpg",
                "extraLarge": "http://cover/xl.jpg",
                "medium": "http://cover/m.jpg",
            },
            "bannerImage": "http://banner.jpg",
            "description": "Giants attack humanity",
            "format": "TV",
            "status": "FINISHED",
            "startDate": {"year": 2013, "month": 4, "day": 7},
            "endDate": {"year": 2013, "month": 9, "day": 29},
            "duration": 24,
            "source": "MANGA",
            "genres": ["Action", "Drama"],
            "tags": [
                {"name": "Survival", "rank": 90},
                {"name": "Military", "rank": 80},
            ],
            "popularity": 500000,
            "favourites": 100000,
            "studios": {"nodes": [{"name": "WIT STUDIO"}, {"name": "Production I.G"}]},
        }
        anime = parse_anilist_anime(raw)
        assert anime.id == "anilist:16498"
        assert anime.title_ja == "進撃の巨人"
        assert anime.title_en == "Attack on Titan"
        assert anime.year == 2013
        assert anime.season == "spring"
        assert anime.score == pytest.approx(8.4)
        assert anime.cover_extra_large == "http://cover/xl.jpg"
        assert anime.banner == "http://banner.jpg"
        assert anime.start_date == "2013-04-07"
        assert anime.end_date == "2013-09-29"
        assert anime.studios == ["WIT STUDIO", "Production I.G"]
        assert len(anime.tags) == 2
        assert anime.genres == ["Action", "Drama"]
        assert anime.format == "TV"
        assert anime.status == "FINISHED"
        assert anime.duration == 24
        assert anime.source == "MANGA"

    def test_parse_anilist_anime_no_english_falls_back_to_romaji(self):
        from src.scrapers.anilist_scraper import parse_anilist_anime

        raw = {
            "id": 1,
            "title": {"romaji": "Test Romaji", "english": None, "native": None},
        }
        anime = parse_anilist_anime(raw)
        assert anime.title_en == "Test Romaji"

    def test_parse_anilist_anime_tags_limited_to_10(self):
        from src.scrapers.anilist_scraper import parse_anilist_anime

        raw = {
            "id": 1,
            "title": {},
            "tags": [{"name": f"tag{i}", "rank": i} for i in range(20)],
        }
        anime = parse_anilist_anime(raw)
        assert len(anime.tags) == 10

    def test_parse_anilist_anime_empty_start_end_dates(self):
        from src.scrapers.anilist_scraper import parse_anilist_anime

        raw = {
            "id": 1,
            "title": {},
            "startDate": {"year": None, "month": None, "day": None},
            "endDate": None,
        }
        anime = parse_anilist_anime(raw)
        assert anime.start_date is None
        assert anime.end_date is None

    def test_parse_anilist_anime_unknown_season(self):
        from src.scrapers.anilist_scraper import parse_anilist_anime

        raw = {"id": 1, "title": {}, "season": "UNKNOWN"}
        anime = parse_anilist_anime(raw)
        assert anime.season is None

    def test_parse_anilist_anime_studios_with_empty_names(self):
        from src.scrapers.anilist_scraper import parse_anilist_anime

        raw = {
            "id": 1,
            "title": {},
            "studios": {"nodes": [{"name": "Studio A"}, {"name": ""}, {"name": None}]},
        }
        anime = parse_anilist_anime(raw)
        assert anime.studios == ["Studio A"]

    def test_parse_anilist_staff_empty_edges(self):
        from src.scrapers.anilist_scraper import parse_anilist_staff

        persons, credits = parse_anilist_staff([], "anilist:1")
        assert persons == []
        assert credits == []

    def test_parse_anilist_staff_skips_missing_id(self):
        from src.scrapers.anilist_scraper import parse_anilist_staff

        edges = [{"role": "Director", "node": {"name": {"full": "NoId"}}}]
        persons, credits = parse_anilist_staff(edges, "anilist:1")
        assert len(persons) == 0

    def test_parse_anilist_staff_with_alternatives(self):
        from src.scrapers.anilist_scraper import parse_anilist_staff

        edges = [
            {
                "role": "Key Animation",
                "node": {
                    "id": 42,
                    "name": {
                        "full": "Test Person",
                        "native": "テスト",
                        "alternative": ["Alias1", "Alias2"],
                    },
                },
            }
        ]
        persons, credits = parse_anilist_staff(edges, "anilist:1")
        assert len(persons) == 1
        assert set(persons[0].aliases) == {"Alias1", "Alias2"}
        assert credits[0].role == Role.KEY_ANIMATOR

    def test_parse_anilist_voice_actors_empty(self):
        from src.scrapers.anilist_scraper import parse_anilist_voice_actors

        persons, credits = parse_anilist_voice_actors([], "anilist:1")
        assert persons == []
        assert credits == []

    def test_parse_anilist_voice_actors_none(self):
        from src.scrapers.anilist_scraper import parse_anilist_voice_actors

        persons, credits = parse_anilist_voice_actors(None, "anilist:1")
        assert persons == []
        assert credits == []

    def test_parse_anilist_voice_actors_deduplication(self):
        from src.scrapers.anilist_scraper import parse_anilist_voice_actors

        edges = [
            {
                "voiceActors": [
                    {"id": 1, "name": {"full": "VA One", "native": "声優一"}}
                ]
            },
            {
                "voiceActors": [
                    {"id": 1, "name": {"full": "VA One", "native": "声優一"}}
                ]
            },  # duplicate
            {"voiceActors": [{"id": 2, "name": {"full": "VA Two"}}]},
        ]
        persons, credits = parse_anilist_voice_actors(edges, "anilist:1")
        assert len(persons) == 2  # Deduped
        assert len(credits) == 2

    def test_parse_anilist_voice_actors_no_voice_actors_key(self):
        from src.scrapers.anilist_scraper import parse_anilist_voice_actors

        edges = [{"voiceActors": []}, {"role": "MAIN"}]
        persons, credits = parse_anilist_voice_actors(edges, "anilist:1")
        assert len(persons) == 0

    def test_parse_anilist_voice_actors_with_full_data(self):
        from src.scrapers.anilist_scraper import parse_anilist_voice_actors

        edges = [
            {
                "voiceActors": [
                    {
                        "id": 10,
                        "name": {
                            "full": "Yuki Kaji",
                            "native": "梶裕貴",
                            "alternative": ["Kaji"],
                        },
                        "image": {
                            "large": "http://img/l.jpg",
                            "medium": "http://img/m.jpg",
                        },
                        "dateOfBirth": {"year": 1985, "month": 9, "day": 3},
                        "age": 38,
                        "gender": "Male",
                        "yearsActive": [2004],
                        "homeTown": "Tokyo",
                        "bloodType": "O",
                        "description": "Voice actor",
                        "favourites": 5000,
                        "siteUrl": "https://anilist.co/staff/10",
                    }
                ]
            }
        ]
        persons, credits = parse_anilist_voice_actors(edges, "anilist:1")
        assert len(persons) == 1
        assert persons[0].name_ja == "梶裕貴"
        assert persons[0].date_of_birth == "1985-09-03"
        assert credits[0].role == Role.VOICE_ACTOR

    def test_parse_anilist_voice_actors_skips_missing_id(self):
        from src.scrapers.anilist_scraper import parse_anilist_voice_actors

        edges = [{"voiceActors": [{"name": {"full": "NoId VA"}}]}]
        persons, credits = parse_anilist_voice_actors(edges, "anilist:1")
        assert len(persons) == 0


class TestAniListBatchSave:
    def test_save_anime_batch_to_database(self):
        from src.scrapers.anilist_scraper import save_anime_batch_to_database

        mock_conn = MagicMock()
        with patch("src.database.upsert_anime") as mock_upsert_db:
            save_anime_batch_to_database(mock_conn, ["anime1", "anime2"])
            assert mock_upsert_db.call_count == 2

    def test_save_persons_batch_to_database(self):
        from src.scrapers.anilist_scraper import save_persons_batch_to_database

        mock_conn = MagicMock()
        with patch("src.database.upsert_person") as mock_upsert:
            save_persons_batch_to_database(mock_conn, ["p1", "p2", "p3"])
            assert mock_upsert.call_count == 3

    def test_save_credits_batch_to_database(self):
        from src.scrapers.anilist_scraper import save_credits_batch_to_database

        mock_conn = MagicMock()
        with patch("src.database.insert_credit") as mock_insert:
            save_credits_batch_to_database(mock_conn, ["c1"])
            mock_insert.assert_called_once_with(mock_conn, "c1")


# ---------------------------------------------------------------------------
# MAL/Jikan scraper tests
# ---------------------------------------------------------------------------


class TestJikanClient:
    def test_get_success(self):
        from src.scrapers.mal_scraper import JikanClient

        mock_response = httpx.Response(
            200,
            json={"data": [{"mal_id": 1}]},
            request=httpx.Request("GET", "https://api.jikan.moe/v4/test"),
        )

        client = JikanClient()
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=mock_response)

        result = _run(client.get("/test"))
        assert result == {"data": [{"mal_id": 1}]}
        _run(client.close())

    def test_get_429_rate_limit(self):
        from src.scrapers.mal_scraper import JikanClient

        rate_limited = httpx.Response(
            429,
            headers={"Retry-After": "1"},
            request=httpx.Request("GET", "https://api.jikan.moe/v4/test"),
        )
        success = httpx.Response(
            200,
            json={"data": []},
            request=httpx.Request("GET", "https://api.jikan.moe/v4/test"),
        )

        client = JikanClient()
        client._client = AsyncMock()
        client._client.get = AsyncMock(side_effect=[rate_limited, success])

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await client.get("/test")

        result = _run(run())
        assert result == {"data": []}
        _run(client.close())

    def test_get_all_attempts_fail(self):
        from src.scrapers.mal_scraper import JikanClient

        client = JikanClient()
        client._client = AsyncMock()
        client._client.get = AsyncMock(side_effect=httpx.ConnectError("down"))

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await client.get("/test")

        with pytest.raises(EndpointUnreachableError):
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


class TestMediaArtsJsonLdParser:
    def test_parse_jsonld_dump_basic(self, tmp_path):
        from src.scrapers.mediaarts_scraper import parse_jsonld_dump

        data = {
            "@graph": [
                {
                    "schema:identifier": "C10001",
                    "schema:name": "テスト作品",
                    "schema:datePublished": "2020-04-01",
                    "schema:contributor": "[監督]山田太郎 ／ [脚本]鈴木次郎",
                    "schema:productionCompany": "[アニメーション制作]マッドハウス",
                }
            ]
        }
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

        result = parse_jsonld_dump(json_path)
        assert len(result) == 1
        assert result[0]["id"] == "C10001"
        assert result[0]["title"] == "テスト作品"
        assert result[0]["year"] == 2020
        assert ("監督", "山田太郎") in result[0]["contributors"]
        assert ("脚本", "鈴木次郎") in result[0]["contributors"]
        assert "マッドハウス" in result[0]["studios"]

    def test_parse_jsonld_name_list(self, tmp_path):
        from src.scrapers.mediaarts_scraper import parse_jsonld_dump

        data = {
            "@graph": [
                {
                    "schema:identifier": "C10002",
                    "schema:name": [
                        "タイトル",
                        {"@value": "タイトル", "@language": "ja-hrkt"},
                    ],
                    "schema:datePublished": "2021",
                    "schema:contributor": "[監督]テスト太郎",
                }
            ]
        }
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

        result = parse_jsonld_dump(json_path)
        assert result[0]["title"] == "タイトル"

    def test_parse_jsonld_creator_and_contributor(self, tmp_path):
        from src.scrapers.mediaarts_scraper import parse_jsonld_dump

        data = {
            "@graph": [
                {
                    "schema:identifier": "C10003",
                    "schema:name": "テスト",
                    "schema:datePublished": "2022",
                    "schema:creator": "[総監督]湯山邦彦",
                    "schema:contributor": "[脚本]井上敏樹",
                    "ma:originalWorkCreator": "[原作]村上真紀",
                }
            ]
        }
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

        result = parse_jsonld_dump(json_path)
        roles = [r for r, _n in result[0]["contributors"]]
        assert "総監督" in roles
        assert "脚本" in roles
        assert "原作" in roles

    def test_parse_jsonld_no_identifier_skipped(self, tmp_path):
        from src.scrapers.mediaarts_scraper import parse_jsonld_dump

        data = {"@graph": [{"schema:name": "NoID"}]}
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

        result = parse_jsonld_dump(json_path)
        assert len(result) == 0

    def test_parse_jsonld_empty_graph(self, tmp_path):
        from src.scrapers.mediaarts_scraper import parse_jsonld_dump

        data = {"@graph": []}
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

        result = parse_jsonld_dump(json_path)
        assert result == []


class TestMediaArtsParsers:
    def test_parse_contributor_text(self):
        from src.scrapers.mediaarts_scraper import parse_contributor_text

        result = parse_contributor_text("[監督]山田太郎 / [脚本]鈴木次郎")
        assert len(result) == 2
        assert result[0] == ("監督", "山田太郎")
        assert result[1] == ("脚本", "鈴木次郎")

    def test_parse_contributor_text_fullwidth_slash(self):
        from src.scrapers.mediaarts_scraper import parse_contributor_text

        result = parse_contributor_text("[監督]山田太郎 ／ [脚本]鈴木次郎")
        assert len(result) == 2
        assert result[0] == ("監督", "山田太郎")
        assert result[1] == ("脚本", "鈴木次郎")

    def test_parse_contributor_text_empty(self):
        from src.scrapers.mediaarts_scraper import parse_contributor_text

        assert parse_contributor_text("") == []
        assert parse_contributor_text(None) == []


# ---------------------------------------------------------------------------
# JVMG / Wikidata fetcher tests
# ---------------------------------------------------------------------------


class TestWikidataClient:
    def test_query_success(self):
        from src.scrapers.jvmg_fetcher import WikidataClient

        mock_response = httpx.Response(
            200,
            json={"results": {"bindings": [{"anime": {"value": "http://wd/Q1"}}]}},
            request=httpx.Request("GET", "https://query.wikidata.org/sparql"),
        )

        client = WikidataClient()
        client._client = AsyncMock()
        client._client.get = AsyncMock(return_value=mock_response)

        result = _run(client.query("SELECT ?anime WHERE {}"))
        assert len(result) == 1
        _run(client.close())

    def test_query_429_rate_limit(self):
        from src.scrapers.jvmg_fetcher import WikidataClient

        rate_limited = httpx.Response(
            429,
            headers={"Retry-After": "1"},
            request=httpx.Request("GET", "https://query.wikidata.org/sparql"),
        )
        success = httpx.Response(
            200,
            json={"results": {"bindings": []}},
            request=httpx.Request("GET", "https://query.wikidata.org/sparql"),
        )

        client = WikidataClient()
        client._client = AsyncMock()
        client._client.get = AsyncMock(side_effect=[rate_limited, success])

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await client.query("SELECT ?x WHERE {}")

        result = _run(run())
        assert result == []
        _run(client.close())

    def test_query_all_attempts_fail(self):
        from src.scrapers.jvmg_fetcher import WikidataClient

        client = WikidataClient()
        client._client = AsyncMock()
        client._client.get = AsyncMock(side_effect=httpx.ConnectError("timeout"))

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await client.query("SELECT ?x WHERE {}")

        with pytest.raises(EndpointUnreachableError):
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
                "roleLabel": {"value": "director"},
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
                "roleLabel": {"value": ""},
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
                "roleLabel": {"value": "director"},
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
                "roleLabel": {"value": "director"},
            },
            {
                "anime": {"value": "http://wd/Q1"},
                "animeLabel": {"value": "Test"},
                "person": {"value": "http://wd/Q2"},
                "personLabel": {"value": "Person"},
                "roleLabel": {"value": "episode_director"},
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
                "roleLabel": {"value": "director"},
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


class TestImageDownloader:
    def test_download_image_empty_url(self):
        from src.scrapers.image_downloader import download_image

        client = AsyncMock()
        result = _run(download_image(client, "", Path("/tmp/test")))
        assert result is None

    def test_download_image_none_url(self):
        from src.scrapers.image_downloader import download_image

        client = AsyncMock()
        result = _run(download_image(client, None, Path("/tmp/test")))
        assert result is None

    def test_download_image_already_exists(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        # IMAGES_DIR must be patched so that save_path.relative_to(IMAGES_DIR.parent)
        # works correctly when using tmp_path
        images_dir = tmp_path / "images"
        images_dir.mkdir()
        save_dir = images_dir / "subdir"
        save_dir.mkdir()
        existing = save_dir / "test.jpg"
        existing.write_bytes(b"existing image")

        client = AsyncMock()

        async def run():
            with patch("src.scrapers.image_downloader.IMAGES_DIR", images_dir):
                return await download_image(
                    client, "http://example.com/img.jpg", save_dir, "test.jpg"
                )

        result = _run(run())
        assert result is not None
        # Client should not have been called (file already exists)
        client.get.assert_not_called()

    def test_download_image_success(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        image_content = b"\xff\xd8\xff\xe0" + b"\x00" * 2048  # JPEG-like content

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "image/jpeg"}
        mock_response.content = image_content

        client = AsyncMock()
        client.get = AsyncMock(return_value=mock_response)

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                with patch("src.scrapers.image_downloader.IMAGES_DIR", tmp_path):
                    return await download_image(
                        client, "http://example.com/img.jpg", save_dir, "test.jpg"
                    )

        result = _run(run())
        assert result is not None
        assert (save_dir / "test.jpg").exists()

    def test_download_image_invalid_content_type(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/html"}
        mock_response.content = b"<html>not an image</html>"

        client = AsyncMock()
        client.get = AsyncMock(return_value=mock_response)

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await download_image(
                    client, "http://example.com/img.jpg", save_dir, "test.jpg"
                )

        result = _run(run())
        assert result is None

    def test_download_image_too_small(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "image/jpeg"}
        mock_response.content = b"\xff\xd8"  # Too small (< 1024 bytes)

        client = AsyncMock()
        client.get = AsyncMock(return_value=mock_response)

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await download_image(
                    client, "http://example.com/img.jpg", save_dir, "test.jpg"
                )

        result = _run(run())
        assert result is None

    def test_download_image_http_error_retry(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        image_content = b"\xff\xd8\xff\xe0" + b"\x00" * 2048

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "image/jpeg"}
        mock_response.content = image_content

        client = AsyncMock()
        # First attempt fails, second succeeds
        client.get = AsyncMock(
            side_effect=[httpx.ConnectError("timeout"), mock_response]
        )

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                with patch("src.scrapers.image_downloader.IMAGES_DIR", tmp_path):
                    return await download_image(
                        client, "http://example.com/img.jpg", save_dir, "test.jpg"
                    )

        result = _run(run())
        assert result is not None

    def test_download_image_all_retries_fail(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        client = AsyncMock()
        client.get = AsyncMock(side_effect=httpx.ConnectError("timeout"))

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await download_image(
                    client, "http://example.com/img.jpg", save_dir, "test.jpg"
                )

        result = _run(run())
        assert result is None

    def test_download_image_429_rate_limit(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        image_content = b"\xff\xd8\xff\xe0" + b"\x00" * 2048

        rate_limited = MagicMock()
        rate_limited.status_code = 429
        rate_limited.headers = {"Retry-After": "1"}

        success = MagicMock()
        success.status_code = 200
        success.headers = {"content-type": "image/jpeg"}
        success.content = image_content

        client = AsyncMock()
        client.get = AsyncMock(side_effect=[rate_limited, success])

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                with patch("src.scrapers.image_downloader.IMAGES_DIR", tmp_path):
                    return await download_image(
                        client, "http://example.com/img.jpg", save_dir, "test.jpg"
                    )

        result = _run(run())
        assert result is not None

    def test_download_image_429_all_retries(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        rate_limited = MagicMock()
        rate_limited.status_code = 429
        rate_limited.headers = {"Retry-After": "1"}

        client = AsyncMock()
        client.get = AsyncMock(return_value=rate_limited)

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await download_image(
                    client, "http://example.com/img.jpg", save_dir, "test.jpg"
                )

        result = _run(run())
        assert result is None

    def test_download_image_non_200_status(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        not_found = MagicMock()
        not_found.status_code = 404
        not_found.headers = {}

        client = AsyncMock()
        client.get = AsyncMock(return_value=not_found)

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await download_image(
                    client, "http://example.com/img.jpg", save_dir, "test.jpg"
                )

        result = _run(run())
        assert result is None

    def test_download_image_general_exception(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        client = AsyncMock()
        client.get = AsyncMock(side_effect=RuntimeError("unexpected"))

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await download_image(
                    client, "http://example.com/img.jpg", save_dir, "test.jpg"
                )

        result = _run(run())
        assert result is None

    def test_download_image_auto_filename(self, tmp_path):
        """Test that filename is auto-generated from URL hash when not provided."""
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        image_content = b"\xff\xd8\xff\xe0" + b"\x00" * 2048

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "image/png"}
        mock_response.content = image_content

        client = AsyncMock()
        client.get = AsyncMock(return_value=mock_response)

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                with patch("src.scrapers.image_downloader.IMAGES_DIR", tmp_path):
                    return await download_image(
                        client, "http://example.com/photo.png", save_dir
                    )

        result = _run(run())
        assert result is not None
        # Check a file was created in save_dir
        files = list(save_dir.iterdir())
        assert len(files) == 1
        assert files[0].suffix == ".png"


class TestDownloadPersonImages:
    def test_download_person_images_no_progress(self):
        from src.scrapers.image_downloader import download_person_images

        persons = [("person:1", "http://img/large.jpg", "http://img/med.jpg")]

        async def run():
            with patch(
                "src.scrapers.image_downloader.download_image", new_callable=AsyncMock
            ) as mock_dl:
                mock_dl.return_value = "images/persons/person_1/large.png"
                return await download_person_images(persons, show_progress=False)

        results = _run(run())
        assert "person:1" in results
        assert results["person:1"]["large"] is not None

    def test_download_person_images_no_urls(self):
        from src.scrapers.image_downloader import download_person_images

        persons = [("person:1", None, None)]

        async def run():
            with patch(
                "src.scrapers.image_downloader.download_image", new_callable=AsyncMock
            ) as mock_dl:
                result = await download_person_images(persons, show_progress=False)
                return result, mock_dl

        results, mock_dl = _run(run())
        assert results["person:1"]["large"] is None
        assert results["person:1"]["medium"] is None
        mock_dl.assert_not_called()


class TestDownloadAnimeImages:
    def test_download_anime_images_no_progress(self):
        from src.scrapers.image_downloader import download_anime_images

        anime = [
            (
                "anime:1",
                "http://cover/l.jpg",
                "http://cover/xl.jpg",
                "http://banner.jpg",
            )
        ]

        async def run():
            with patch(
                "src.scrapers.image_downloader.download_image", new_callable=AsyncMock
            ) as mock_dl:
                mock_dl.return_value = "images/anime/anime_1/cover.jpg"
                return await download_anime_images(anime, show_progress=False)

        results = _run(run())
        assert "anime:1" in results
        assert results["anime:1"]["cover_large"] is not None

    def test_download_anime_images_prefers_xl_cover(self):
        from src.scrapers.image_downloader import download_anime_images

        anime = [("anime:1", "http://cover/l.jpg", "http://cover/xl.jpg", None)]

        async def run():
            with patch(
                "src.scrapers.image_downloader.download_image", new_callable=AsyncMock
            ) as mock_dl:
                mock_dl.return_value = "images/anime/anime_1/cover.jpg"
                await download_anime_images(anime, show_progress=False)
                return mock_dl

        mock_dl = _run(run())
        # Only one call for cover (xl preferred), no banner call
        assert mock_dl.call_count == 1
        call_url = mock_dl.call_args_list[0][0][1]
        assert call_url == "http://cover/xl.jpg"

    def test_download_anime_images_fallback_to_large(self):
        from src.scrapers.image_downloader import download_anime_images

        anime = [("anime:1", "http://cover/l.jpg", None, None)]

        async def run():
            with patch(
                "src.scrapers.image_downloader.download_image", new_callable=AsyncMock
            ) as mock_dl:
                mock_dl.return_value = "images/anime/anime_1/cover.jpg"
                await download_anime_images(anime, show_progress=False)
                return mock_dl

        mock_dl = _run(run())
        call_url = mock_dl.call_args_list[0][0][1]
        assert call_url == "http://cover/l.jpg"

    def test_download_anime_images_no_covers(self):
        from src.scrapers.image_downloader import download_anime_images

        anime = [("anime:1", None, None, None)]

        async def run():
            with patch(
                "src.scrapers.image_downloader.download_image", new_callable=AsyncMock
            ) as mock_dl:
                result = await download_anime_images(anime, show_progress=False)
                return result, mock_dl

        results, mock_dl = _run(run())
        assert results["anime:1"]["cover_large"] is None
        assert results["anime:1"]["banner"] is None
        mock_dl.assert_not_called()


# ---------------------------------------------------------------------------
# Integration-style tests for async fetch functions
# ---------------------------------------------------------------------------


class TestMediaArtsDownload:
    def test_download_cached(self, tmp_path):
        """Cached version skips download."""
        from src.scrapers.mediaarts_scraper import (
            ANIME_COLLECTION_FILES_PRIMARY,
            download_madb_dataset,
        )

        # Pre-create version file and JSON files
        (tmp_path / ".version").write_text("v1.2.12")
        for zip_name in ANIME_COLLECTION_FILES_PRIMARY:
            json_name = zip_name.replace("_json.zip", ".json")
            (tmp_path / json_name).write_text("{}")

        mock_release = {"tag_name": "v1.2.12", "assets": []}
        mock_resp = httpx.Response(
            200,
            json=mock_release,
            request=httpx.Request(
                "GET", "https://api.github.com/repos/x/releases/latest"
            ),
        )

        async def run():
            with patch("httpx.AsyncClient") as mock_cls:
                mock_client = AsyncMock()
                mock_client.get = AsyncMock(return_value=mock_resp)
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=False)
                mock_cls.return_value = mock_client
                return await download_madb_dataset(tmp_path, version="latest")

        result = _run(run())
        assert len(result) == len(ANIME_COLLECTION_FILES_PRIMARY)


class TestJVMGFetchAnimeStaff:
    def test_fetch_anime_staff_pagination(self):
        from src.scrapers.jvmg_fetcher import fetch_anime_staff

        page1 = [
            {
                "anime": {"value": f"http://wd/Q{i}"},
                "animeLabel": {"value": f"Anime {i}"},
                "person": {"value": f"http://wd/P{i}"},
                "personLabel": {"value": f"Person {i}"},
                "roleLabel": {"value": "director"},
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
