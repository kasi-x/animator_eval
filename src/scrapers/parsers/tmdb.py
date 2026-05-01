"""TMDB v3 REST API JSON response parsers.

Hard Rule: vote_average / vote_count / popularity flow into ``display_*``
fields only. They MUST NOT enter scoring, edge weights, or optimization targets.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import structlog

from src.runtime.models import parse_role

log = structlog.get_logger()


@dataclass
class TmdbCreditEntry:
    tmdb_anime_id: int
    media_type: str  # 'tv' | 'movie'
    tmdb_person_id: int
    credit_type: str  # 'cast' | 'crew'
    role: str  # parse_role() output
    role_raw: str  # job (crew) or character (cast)
    character: str | None = None
    department: str | None = None
    job: str | None = None
    episode_count: int | None = None  # tv aggregate_credits only


@dataclass
class TmdbAnimeRecord:
    tmdb_id: int
    media_type: str  # 'tv' | 'movie'
    title: str = ""
    original_title: str = ""
    original_lang: str | None = None
    origin_countries: str = "[]"  # JSON array
    year: int | None = None
    first_air_date: str | None = None
    last_air_date: str | None = None
    release_date: str | None = None
    episodes: int | None = None
    seasons: int | None = None
    runtime: int | None = None
    status: str | None = None
    genres: str = "[]"  # JSON array of names
    production_companies: str = "[]"  # JSON array of {id,name,country}
    overview: str | None = None
    poster_path: str | None = None
    backdrop_path: str | None = None
    imdb_id: str | None = None
    tvdb_id: int | None = None
    wikidata_id: str | None = None
    display_vote_avg: float | None = None
    display_vote_count: int | None = None
    display_popularity: float | None = None
    credits: list[TmdbCreditEntry] = field(default_factory=list)


@dataclass
class TmdbPersonRecord:
    tmdb_id: int
    name: str = ""
    also_known_as: str = "[]"  # JSON array
    gender: int | None = None  # 0 unknown / 1 female / 2 male / 3 non-binary
    birthday: str | None = None
    deathday: str | None = None
    place_of_birth: str | None = None
    biography: str | None = None
    known_for_dept: str | None = None
    profile_path: str | None = None
    imdb_id: str | None = None
    display_popularity: float | None = None


def _year_from_date(date_str: str | None) -> int | None:
    if not date_str or len(date_str) < 4:
        return None
    try:
        return int(date_str[:4])
    except ValueError:
        return None


def _avg_runtime(episode_run_time: list[int] | None) -> int | None:
    if not episode_run_time:
        return None
    valid = [r for r in episode_run_time if isinstance(r, int) and r > 0]
    return sum(valid) // len(valid) if valid else None


def parse_tmdb_anime(raw: dict, media_type: str) -> TmdbAnimeRecord:
    """Parse /tv/{id} or /movie/{id} (with append_to_response=external_ids,credits)."""
    if media_type not in ("tv", "movie"):
        raise ValueError(f"media_type must be 'tv' or 'movie', got {media_type!r}")

    tmdb_id = raw.get("id")
    if not tmdb_id:
        raise ValueError("tmdb id missing")

    if media_type == "tv":
        title = raw.get("name") or ""
        original_title = raw.get("original_name") or ""
        first_air = raw.get("first_air_date") or None
        last_air = raw.get("last_air_date") or None
        release = None
        year = _year_from_date(first_air)
        episodes = raw.get("number_of_episodes")
        seasons = raw.get("number_of_seasons")
        runtime = _avg_runtime(raw.get("episode_run_time"))
        origin_countries = raw.get("origin_country") or []
    else:
        title = raw.get("title") or ""
        original_title = raw.get("original_title") or ""
        release = raw.get("release_date") or None
        first_air = None
        last_air = None
        year = _year_from_date(release)
        episodes = None
        seasons = None
        runtime = raw.get("runtime")
        origin_countries = [
            c.get("iso_3166_1")
            for c in (raw.get("production_countries") or [])
            if c.get("iso_3166_1")
        ]

    genres = [g.get("name") for g in (raw.get("genres") or []) if g.get("name")]
    production_companies = [
        {
            "id": pc.get("id"),
            "name": pc.get("name"),
            "country": pc.get("origin_country"),
        }
        for pc in (raw.get("production_companies") or [])
        if pc.get("id")
    ]

    ext = raw.get("external_ids") or {}
    tvdb_id = ext.get("tvdb_id")
    if tvdb_id is not None:
        try:
            tvdb_id = int(tvdb_id)
        except (TypeError, ValueError):
            tvdb_id = None

    record = TmdbAnimeRecord(
        tmdb_id=int(tmdb_id),
        media_type=media_type,
        title=title,
        original_title=original_title,
        original_lang=raw.get("original_language"),
        origin_countries=json.dumps(origin_countries, ensure_ascii=False),
        year=year,
        first_air_date=first_air,
        last_air_date=last_air,
        release_date=release,
        episodes=episodes,
        seasons=seasons,
        runtime=runtime,
        status=raw.get("status"),
        genres=json.dumps(genres, ensure_ascii=False),
        production_companies=json.dumps(production_companies, ensure_ascii=False),
        overview=raw.get("overview") or None,
        poster_path=raw.get("poster_path"),
        backdrop_path=raw.get("backdrop_path"),
        imdb_id=ext.get("imdb_id") or raw.get("imdb_id"),
        tvdb_id=tvdb_id,
        wikidata_id=ext.get("wikidata_id"),
        display_vote_avg=raw.get("vote_average"),
        display_vote_count=raw.get("vote_count"),
        display_popularity=raw.get("popularity"),
    )

    record.credits = parse_tmdb_credits(raw, media_type, record.tmdb_id)
    return record


def parse_tmdb_credits(
    raw: dict, media_type: str, anime_id: int
) -> list[TmdbCreditEntry]:
    """Extract cast + crew from a /tv or /movie detail (with credits appended)."""
    out: list[TmdbCreditEntry] = []

    if media_type == "tv":
        # aggregate_credits: roles/jobs are arrays per person across all episodes
        agg = raw.get("aggregate_credits") or {}
        for c in agg.get("cast") or []:
            person_id = c.get("id")
            if not person_id:
                continue
            for role_obj in c.get("roles") or []:
                character = role_obj.get("character") or ""
                ep_count = role_obj.get("episode_count")
                role_raw = character or "cast"
                out.append(
                    TmdbCreditEntry(
                        tmdb_anime_id=anime_id,
                        media_type=media_type,
                        tmdb_person_id=int(person_id),
                        credit_type="cast",
                        role=parse_role("Voice Actor"),
                        role_raw=role_raw,
                        character=character or None,
                        episode_count=ep_count,
                    )
                )
        for c in agg.get("crew") or []:
            person_id = c.get("id")
            if not person_id:
                continue
            for job_obj in c.get("jobs") or []:
                job = job_obj.get("job") or ""
                ep_count = job_obj.get("episode_count")
                department = c.get("department")
                role_raw = job or department or "crew"
                out.append(
                    TmdbCreditEntry(
                        tmdb_anime_id=anime_id,
                        media_type=media_type,
                        tmdb_person_id=int(person_id),
                        credit_type="crew",
                        role=parse_role(job or department or ""),
                        role_raw=role_raw,
                        department=department,
                        job=job or None,
                        episode_count=ep_count,
                    )
                )
    else:  # movie
        cr = raw.get("credits") or {}
        for c in cr.get("cast") or []:
            person_id = c.get("id")
            if not person_id:
                continue
            character = c.get("character") or ""
            role_raw = character or "cast"
            out.append(
                TmdbCreditEntry(
                    tmdb_anime_id=anime_id,
                    media_type=media_type,
                    tmdb_person_id=int(person_id),
                    credit_type="cast",
                    role=parse_role("Voice Actor"),
                    role_raw=role_raw,
                    character=character or None,
                )
            )
        for c in cr.get("crew") or []:
            person_id = c.get("id")
            if not person_id:
                continue
            job = c.get("job") or ""
            department = c.get("department")
            role_raw = job or department or "crew"
            out.append(
                TmdbCreditEntry(
                    tmdb_anime_id=anime_id,
                    media_type=media_type,
                    tmdb_person_id=int(person_id),
                    credit_type="crew",
                    role=parse_role(job or department or ""),
                    role_raw=role_raw,
                    department=department,
                    job=job or None,
                )
            )

    return out


def parse_tmdb_person(raw: dict) -> TmdbPersonRecord:
    """Parse /person/{id} (with append_to_response=external_ids)."""
    tmdb_id = raw.get("id")
    if not tmdb_id:
        raise ValueError("tmdb person id missing")

    aka = raw.get("also_known_as") or []
    ext = raw.get("external_ids") or {}

    return TmdbPersonRecord(
        tmdb_id=int(tmdb_id),
        name=raw.get("name") or "",
        also_known_as=json.dumps([a for a in aka if a], ensure_ascii=False),
        gender=raw.get("gender"),
        birthday=raw.get("birthday"),
        deathday=raw.get("deathday"),
        place_of_birth=raw.get("place_of_birth"),
        biography=raw.get("biography") or None,
        known_for_dept=raw.get("known_for_department"),
        profile_path=raw.get("profile_path"),
        imdb_id=ext.get("imdb_id") or raw.get("imdb_id"),
        display_popularity=raw.get("popularity"),
    )


def discover_results(raw: dict) -> tuple[list[int], int]:
    """Extract (tmdb_ids, total_pages) from a /discover response."""
    ids = [int(r["id"]) for r in (raw.get("results") or []) if r.get("id")]
    total_pages = int(raw.get("total_pages") or 0)
    return ids, total_pages
