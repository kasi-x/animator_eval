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
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.runtime.models import Role
from src.scrapers.exceptions import (
    EndpointUnreachableError,
)


def _run(coro):
    """Helper to run async coroutines in sync tests."""
    return asyncio.run(coro)

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
    def test_save_anime_batch_to_bronze(self):
        from src.scrapers.anilist_scraper import save_anime_batch_to_bronze

        mock_bw = MagicMock()
        anime1 = MagicMock()
        anime1.model_dump.return_value = {"id": "anilist:1", "title_en": "Test"}
        save_anime_batch_to_bronze(mock_bw, [anime1])
        mock_bw.append.assert_called_once()

    def test_save_persons_batch_to_bronze(self):
        from src.scrapers.anilist_scraper import save_persons_batch_to_bronze

        mock_bw = MagicMock()
        p1 = MagicMock()
        p1.model_dump.return_value = {"id": "anilist:p1"}
        save_persons_batch_to_bronze(mock_bw, [p1, p1])
        assert mock_bw.append.call_count == 2

    def test_save_credits_batch_to_bronze(self):
        from src.scrapers.anilist_scraper import save_credits_batch_to_bronze

        mock_bw = MagicMock()
        c1 = MagicMock()
        c1.model_dump.return_value = {"person_id": "anilist:p1", "anime_id": "anilist:1"}
        save_credits_batch_to_bronze(mock_bw, [c1])
        mock_bw.append.assert_called_once()


