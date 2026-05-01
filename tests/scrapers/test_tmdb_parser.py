"""TMDB parser unit tests (synthetic fixtures, no network)."""

from __future__ import annotations

import json

import pytest

from src.runtime.models import Role
from src.scrapers.parsers.tmdb import (
    discover_results,
    parse_tmdb_anime,
    parse_tmdb_credits,
    parse_tmdb_person,
)
from src.scrapers.queries.tmdb import (
    detail_url,
    discover_url,
    find_by_external_id,
    person_url,
)


# ─── URL builders ─────────────────────────────────────────────────────────


def test_discover_url_tv_default() -> None:
    url = discover_url("tv", page=1)
    assert "with_genres=16" in url
    assert "page=1" in url
    assert url.startswith("https://api.themoviedb.org/3/discover/tv")


def test_discover_url_movie_year() -> None:
    url = discover_url("movie", page=3, year=2010)
    assert "primary_release_year=2010" in url
    assert "page=3" in url


def test_discover_url_tv_year_uses_first_air_date() -> None:
    url = discover_url("tv", page=1, year=2020)
    assert "first_air_date_year=2020" in url


def test_discover_url_invalid_media() -> None:
    with pytest.raises(ValueError, match="media must be 'tv' or 'movie'"):
        discover_url("anime", page=1)


def test_detail_url_tv_appends_aggregate_credits() -> None:
    url = detail_url("tv", 1535)
    assert url.endswith("aggregate_credits")
    assert "/tv/1535" in url


def test_detail_url_movie_appends_credits() -> None:
    url = detail_url("movie", 4935)
    assert url.endswith("external_ids,credits")
    assert "/movie/4935" in url


def test_person_url() -> None:
    assert person_url(200) == (
        "https://api.themoviedb.org/3/person/200?append_to_response=external_ids"
    )


def test_find_by_external_id_imdb() -> None:
    url = find_by_external_id("tt0877057", "imdb_id")
    assert "external_source=imdb_id" in url
    assert "/find/tt0877057" in url


# ─── parse_tmdb_anime: TV ─────────────────────────────────────────────────


@pytest.fixture
def tv_payload() -> dict:
    return {
        "id": 1535,
        "name": "Death Note",
        "original_name": "デスノート",
        "original_language": "ja",
        "first_air_date": "2006-10-04",
        "last_air_date": "2007-06-26",
        "number_of_episodes": 37,
        "number_of_seasons": 1,
        "episode_run_time": [23, 24],
        "origin_country": ["JP"],
        "status": "Ended",
        "genres": [{"id": 16, "name": "Animation"}, {"id": 80, "name": "Crime"}],
        "production_companies": [
            {"id": 5, "name": "Madhouse", "origin_country": "JP"}
        ],
        "overview": "Synopsis.",
        "poster_path": "/poster.jpg",
        "vote_average": 8.7,
        "vote_count": 1234,
        "popularity": 99.9,
        "external_ids": {
            "imdb_id": "tt0877057",
            "tvdb_id": 79481,
            "wikidata_id": "Q188072",
        },
        "aggregate_credits": {
            "cast": [
                {
                    "id": 100,
                    "name": "Mamoru Miyano",
                    "roles": [{"character": "Light Yagami", "episode_count": 37}],
                }
            ],
            "crew": [
                {
                    "id": 200,
                    "name": "Tetsuro Araki",
                    "department": "Directing",
                    "jobs": [{"job": "Director", "episode_count": 37}],
                }
            ],
        },
    }


def test_parse_tv_basic_fields(tv_payload: dict) -> None:
    rec = parse_tmdb_anime(tv_payload, "tv")
    assert rec.tmdb_id == 1535
    assert rec.media_type == "tv"
    assert rec.title == "Death Note"
    assert rec.original_title == "デスノート"
    assert rec.year == 2006
    assert rec.episodes == 37
    assert rec.seasons == 1
    assert rec.runtime == 23  # avg of [23, 24]
    assert rec.imdb_id == "tt0877057"
    assert rec.tvdb_id == 79481
    assert rec.wikidata_id == "Q188072"
    assert rec.original_lang == "ja"
    assert json.loads(rec.origin_countries) == ["JP"]
    assert "Animation" in json.loads(rec.genres)


def test_parse_tv_display_only_metrics(tv_payload: dict) -> None:
    """Hard Rule: vote_average / popularity → display_* only."""
    rec = parse_tmdb_anime(tv_payload, "tv")
    assert rec.display_vote_avg == 8.7
    assert rec.display_vote_count == 1234
    assert rec.display_popularity == 99.9
    d = rec.__dict__
    assert "vote_average" not in d
    assert "popularity" not in d


def test_parse_tv_credits_voice_and_director(tv_payload: dict) -> None:
    rec = parse_tmdb_anime(tv_payload, "tv")
    cast = [c for c in rec.credits if c.credit_type == "cast"]
    crew = [c for c in rec.credits if c.credit_type == "crew"]
    assert len(cast) == 1
    assert cast[0].role == Role.VOICE_ACTOR
    assert cast[0].character == "Light Yagami"
    assert cast[0].episode_count == 37
    assert len(crew) == 1
    assert crew[0].role == Role.DIRECTOR
    assert crew[0].department == "Directing"
    assert crew[0].job == "Director"
    assert crew[0].episode_count == 37


# ─── parse_tmdb_anime: Movie ──────────────────────────────────────────────


@pytest.fixture
def movie_payload() -> dict:
    return {
        "id": 4935,
        "title": "Howl's Moving Castle",
        "original_title": "ハウルの動く城",
        "original_language": "ja",
        "release_date": "2004-11-19",
        "runtime": 119,
        "production_countries": [{"iso_3166_1": "JP"}],
        "genres": [{"id": 16, "name": "Animation"}],
        "production_companies": [
            {"id": 10342, "name": "Studio Ghibli", "origin_country": "JP"}
        ],
        "vote_average": 8.4,
        "vote_count": 9999,
        "popularity": 33.0,
        "external_ids": {"imdb_id": "tt0347149"},
        "credits": {
            "cast": [{"id": 1234, "character": "Howl"}],
            "crew": [{"id": 5678, "department": "Directing", "job": "Director"}],
        },
    }


def test_parse_movie_basic(movie_payload: dict) -> None:
    rec = parse_tmdb_anime(movie_payload, "movie")
    assert rec.media_type == "movie"
    assert rec.year == 2004
    assert rec.runtime == 119
    assert rec.episodes is None
    assert rec.seasons is None
    assert rec.imdb_id == "tt0347149"
    assert json.loads(rec.origin_countries) == ["JP"]


def test_parse_movie_credits_flat(movie_payload: dict) -> None:
    rec = parse_tmdb_anime(movie_payload, "movie")
    assert len(rec.credits) == 2
    cast = next(c for c in rec.credits if c.credit_type == "cast")
    assert cast.character == "Howl"
    crew = next(c for c in rec.credits if c.credit_type == "crew")
    assert crew.job == "Director"
    assert crew.role == Role.DIRECTOR


# ─── parse_tmdb_anime: edge cases ─────────────────────────────────────────


def test_parse_invalid_media_type_raises() -> None:
    with pytest.raises(ValueError, match="media_type must be 'tv' or 'movie'"):
        parse_tmdb_anime({"id": 1}, "anime")


def test_parse_missing_id_raises() -> None:
    with pytest.raises(ValueError, match="tmdb id missing"):
        parse_tmdb_anime({"name": "x"}, "tv")


def test_parse_tv_no_episode_runtime() -> None:
    payload = {"id": 1, "name": "x", "first_air_date": "2020-01-01"}
    rec = parse_tmdb_anime(payload, "tv")
    assert rec.runtime is None
    assert rec.year == 2020


def test_parse_credits_skips_persons_without_id() -> None:
    raw = {
        "id": 1,
        "name": "x",
        "aggregate_credits": {
            "cast": [{"name": "no id", "roles": [{"character": "c"}]}],
            "crew": [{"id": 5, "department": "Directing", "jobs": [{"job": "Director"}]}],
        },
    }
    rec = parse_tmdb_anime(raw, "tv")
    assert len(rec.credits) == 1
    assert rec.credits[0].tmdb_person_id == 5


# ─── parse_tmdb_person ────────────────────────────────────────────────────


def test_parse_person_basic() -> None:
    raw = {
        "id": 200,
        "name": "Tetsuro Araki",
        "also_known_as": ["荒木哲郎", "Araki Tetsurō"],
        "gender": 2,
        "birthday": "1976-09-08",
        "deathday": None,
        "place_of_birth": "Tokyo, Japan",
        "biography": "Director.",
        "known_for_department": "Directing",
        "popularity": 12.5,
        "external_ids": {"imdb_id": "nm2999991"},
    }
    rec = parse_tmdb_person(raw)
    assert rec.tmdb_id == 200
    assert rec.gender == 2
    assert rec.known_for_dept == "Directing"
    assert rec.imdb_id == "nm2999991"
    assert rec.display_popularity == 12.5
    assert json.loads(rec.also_known_as) == ["荒木哲郎", "Araki Tetsurō"]


def test_parse_person_missing_id_raises() -> None:
    with pytest.raises(ValueError, match="tmdb person id missing"):
        parse_tmdb_person({"name": "x"})


# ─── discover_results ─────────────────────────────────────────────────────


def test_discover_results_basic() -> None:
    raw = {"results": [{"id": 1}, {"id": 2}], "total_pages": 7}
    ids, total = discover_results(raw)
    assert ids == [1, 2]
    assert total == 7


def test_discover_results_empty() -> None:
    ids, total = discover_results({"results": [], "total_pages": 0})
    assert ids == []
    assert total == 0


def test_parse_credits_helper_directly_on_movie() -> None:
    raw = {
        "credits": {
            "cast": [{"id": 1, "character": "C"}],
            "crew": [{"id": 2, "department": "Sound", "job": "Sound Director"}],
        }
    }
    out = parse_tmdb_credits(raw, "movie", anime_id=99)
    assert {c.tmdb_person_id for c in out} == {1, 2}
    assert all(c.tmdb_anime_id == 99 for c in out)
