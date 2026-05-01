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
    spoken_languages: str = "[]"  # JSON array of iso_639_1 codes
    year: int | None = None
    first_air_date: str | None = None
    last_air_date: str | None = None
    release_date: str | None = None
    episodes: int | None = None
    seasons: int | None = None
    runtime: int | None = None
    status: str | None = None
    type: str | None = None  # tv: Documentary/Reality/Scripted/etc.
    in_production: int | None = None  # tv only, 0/1
    adult: int | None = None  # 0/1
    genres: str = "[]"  # JSON array of names
    production_companies: str = "[]"  # JSON array of {id,name,country}
    production_countries: str = "[]"  # JSON array of {iso_3166_1,name}
    networks: str = "[]"  # tv only — JSON array of {id,name,origin_country}
    created_by: str = "[]"  # tv only — JSON array of {id,name,credit_id} (program creators)
    belongs_to_collection: str | None = None  # movie only — JSON {id,name,...}
    overview: str | None = None
    tagline: str | None = None
    homepage: str | None = None
    poster_path: str | None = None
    backdrop_path: str | None = None
    imdb_id: str | None = None
    tvdb_id: int | None = None
    wikidata_id: str | None = None
    facebook_id: str | None = None
    instagram_id: str | None = None
    twitter_id: str | None = None
    # H1 hard rule: vote_avg / popularity / budget / revenue → display_* only.
    display_vote_avg: float | None = None
    display_vote_count: int | None = None
    display_popularity: float | None = None
    display_budget: int | None = None  # movie only
    display_revenue: int | None = None  # movie only
    # JSON-blob extras (cross-source useful, kept raw to avoid lossy parsing).
    keywords: str = "[]"  # JSON array of {id,name}
    alternative_titles: str = "[]"  # JSON array of {iso_3166_1,title,type}
    translations: str = "[]"  # JSON array of {iso_3166_1,iso_639_1,name,overview,homepage,tagline}
    release_dates: str = "[]"  # movie only — JSON array per country: {iso_3166_1, releases:[{type,release_date,certification,note}]}
    content_ratings: str = "[]"  # tv only — JSON array of {iso_3166_1,rating}
    videos: str = "[]"  # JSON array of {id,site,key,name,type,iso_639_1,iso_3166_1,published_at}
    images: str = "{}"  # JSON dict {posters:[…], backdrops:[…], logos:[…]} with file_path/lang/votes
    watch_providers: str = "{}"  # JSON dict per-country {flatrate/buy/rent/free → providers}
    recommendation_ids: str = "[]"  # JSON array of recommended TMDB ids (this media_type)
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
    homepage: str | None = None
    adult: int | None = None
    imdb_id: str | None = None
    facebook_id: str | None = None
    instagram_id: str | None = None
    twitter_id: str | None = None
    tiktok_id: str | None = None
    youtube_id: str | None = None
    wikidata_id: str | None = None
    images: str = "[]"  # JSON array of {file_path,width,height,iso_639_1}
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

    networks: list[dict] = []
    created_by: list[dict] = []
    belongs_to_collection: str | None = None
    in_production: int | None = None
    type_: str | None = None
    release_dates: list[dict] = []
    content_ratings: list[dict] = []
    display_budget: int | None = None
    display_revenue: int | None = None
    production_countries_raw = raw.get("production_countries") or []

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
        type_ = raw.get("type")
        in_prod = raw.get("in_production")
        in_production = int(bool(in_prod)) if in_prod is not None else None
        networks = [
            {
                "id": n.get("id"),
                "name": n.get("name"),
                "origin_country": n.get("origin_country"),
            }
            for n in (raw.get("networks") or [])
            if n.get("id")
        ]
        created_by = [
            {
                "id": c.get("id"),
                "name": c.get("name"),
                "credit_id": c.get("credit_id"),
            }
            for c in (raw.get("created_by") or [])
            if c.get("id")
        ]
        content_ratings = [
            {"iso_3166_1": r.get("iso_3166_1"), "rating": r.get("rating")}
            for r in ((raw.get("content_ratings") or {}).get("results") or [])
        ]
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
            for c in production_countries_raw
            if c.get("iso_3166_1")
        ]
        coll = raw.get("belongs_to_collection")
        if coll:
            belongs_to_collection = json.dumps(
                {
                    "id": coll.get("id"),
                    "name": coll.get("name"),
                    "poster_path": coll.get("poster_path"),
                    "backdrop_path": coll.get("backdrop_path"),
                },
                ensure_ascii=False,
            )
        budget = raw.get("budget")
        revenue = raw.get("revenue")
        display_budget = int(budget) if budget else None
        display_revenue = int(revenue) if revenue else None
        release_dates = [
            {
                "iso_3166_1": r.get("iso_3166_1"),
                "releases": [
                    {
                        "type": d.get("type"),
                        "release_date": d.get("release_date"),
                        "certification": d.get("certification"),
                        "note": d.get("note"),
                    }
                    for d in (r.get("release_dates") or [])
                ],
            }
            for r in ((raw.get("release_dates") or {}).get("results") or [])
        ]

    genres = [g.get("name") for g in (raw.get("genres") or []) if g.get("name")]
    production_companies = [
        {
            "id": pc.get("id"),
            "name": pc.get("name"),
            "country": pc.get("origin_country"),
            "logo_path": pc.get("logo_path"),
        }
        for pc in (raw.get("production_companies") or [])
        if pc.get("id")
    ]
    production_countries = [
        {"iso_3166_1": c.get("iso_3166_1"), "name": c.get("name")}
        for c in production_countries_raw
        if c.get("iso_3166_1")
    ]
    spoken_languages = [
        s.get("iso_639_1") for s in (raw.get("spoken_languages") or [])
        if s.get("iso_639_1")
    ]

    ext = raw.get("external_ids") or {}
    tvdb_id = ext.get("tvdb_id")
    if tvdb_id is not None:
        try:
            tvdb_id = int(tvdb_id)
        except (TypeError, ValueError):
            tvdb_id = None

    # Sub-resources (append_to_response)
    keywords_block = raw.get("keywords") or {}
    keywords_list = (
        keywords_block.get("keywords")
        if media_type == "tv" and "keywords" in keywords_block
        else keywords_block.get("results") or keywords_block.get("keywords") or []
    )
    keywords = [
        {"id": k.get("id"), "name": k.get("name")}
        for k in (keywords_list or [])
        if k.get("id")
    ]

    alt_titles_block = raw.get("alternative_titles") or {}
    alt_titles_raw = (
        alt_titles_block.get("titles")
        or alt_titles_block.get("results")
        or []
    )
    alternative_titles = [
        {
            "iso_3166_1": t.get("iso_3166_1"),
            "title": t.get("title"),
            "type": t.get("type"),
        }
        for t in alt_titles_raw
        if t.get("title")
    ]

    translations_raw = (raw.get("translations") or {}).get("translations") or []
    translations = [
        {
            "iso_3166_1": t.get("iso_3166_1"),
            "iso_639_1": t.get("iso_639_1"),
            "name": t.get("name"),
            "english_name": t.get("english_name"),
            "data": {
                # subset of localized data we keep — name/title, overview, homepage, tagline.
                "name": (t.get("data") or {}).get("name"),
                "title": (t.get("data") or {}).get("title"),
                "overview": (t.get("data") or {}).get("overview"),
                "homepage": (t.get("data") or {}).get("homepage"),
                "tagline": (t.get("data") or {}).get("tagline"),
            },
        }
        for t in translations_raw
    ]

    videos_raw = (raw.get("videos") or {}).get("results") or []
    videos = [
        {
            "id": v.get("id"),
            "site": v.get("site"),
            "key": v.get("key"),
            "name": v.get("name"),
            "type": v.get("type"),
            "iso_639_1": v.get("iso_639_1"),
            "iso_3166_1": v.get("iso_3166_1"),
            "official": v.get("official"),
            "published_at": v.get("published_at"),
        }
        for v in videos_raw
    ]

    images_raw = raw.get("images") or {}

    def _img(arr_key: str) -> list[dict]:
        return [
            {
                "file_path": im.get("file_path"),
                "iso_639_1": im.get("iso_639_1"),
                "width": im.get("width"),
                "height": im.get("height"),
                "vote_average": im.get("vote_average"),
                "vote_count": im.get("vote_count"),
            }
            for im in (images_raw.get(arr_key) or [])
            if im.get("file_path")
        ]

    images = {
        "posters": _img("posters"),
        "backdrops": _img("backdrops"),
        "logos": _img("logos"),
    }

    wp_raw = (raw.get("watch/providers") or raw.get("watch_providers") or {}).get(
        "results"
    ) or {}
    watch_providers: dict = {}
    for country, info in wp_raw.items():
        watch_providers[country] = {
            "link": info.get("link"),
            "flatrate": [_provider_pick(p) for p in info.get("flatrate") or []],
            "buy": [_provider_pick(p) for p in info.get("buy") or []],
            "rent": [_provider_pick(p) for p in info.get("rent") or []],
            "free": [_provider_pick(p) for p in info.get("free") or []],
            "ads": [_provider_pick(p) for p in info.get("ads") or []],
        }

    rec_block = (raw.get("recommendations") or {}).get("results") or []
    recommendation_ids = [int(r["id"]) for r in rec_block if r.get("id")]

    record = TmdbAnimeRecord(
        tmdb_id=int(tmdb_id),
        media_type=media_type,
        title=title,
        original_title=original_title,
        original_lang=raw.get("original_language"),
        origin_countries=json.dumps(origin_countries, ensure_ascii=False),
        spoken_languages=json.dumps(spoken_languages, ensure_ascii=False),
        year=year,
        first_air_date=first_air,
        last_air_date=last_air,
        release_date=release,
        episodes=episodes,
        seasons=seasons,
        runtime=runtime,
        status=raw.get("status"),
        type=type_,
        in_production=in_production,
        adult=int(bool(raw.get("adult"))) if raw.get("adult") is not None else None,
        genres=json.dumps(genres, ensure_ascii=False),
        production_companies=json.dumps(production_companies, ensure_ascii=False),
        production_countries=json.dumps(production_countries, ensure_ascii=False),
        networks=json.dumps(networks, ensure_ascii=False),
        created_by=json.dumps(created_by, ensure_ascii=False),
        belongs_to_collection=belongs_to_collection,
        overview=raw.get("overview") or None,
        tagline=raw.get("tagline") or None,
        homepage=raw.get("homepage") or None,
        poster_path=raw.get("poster_path"),
        backdrop_path=raw.get("backdrop_path"),
        imdb_id=ext.get("imdb_id") or raw.get("imdb_id"),
        tvdb_id=tvdb_id,
        wikidata_id=ext.get("wikidata_id"),
        facebook_id=ext.get("facebook_id"),
        instagram_id=ext.get("instagram_id"),
        twitter_id=ext.get("twitter_id"),
        display_vote_avg=raw.get("vote_average"),
        display_vote_count=raw.get("vote_count"),
        display_popularity=raw.get("popularity"),
        display_budget=display_budget,
        display_revenue=display_revenue,
        keywords=json.dumps(keywords, ensure_ascii=False),
        alternative_titles=json.dumps(alternative_titles, ensure_ascii=False),
        translations=json.dumps(translations, ensure_ascii=False),
        release_dates=json.dumps(release_dates, ensure_ascii=False),
        content_ratings=json.dumps(content_ratings, ensure_ascii=False),
        videos=json.dumps(videos, ensure_ascii=False),
        images=json.dumps(images, ensure_ascii=False),
        watch_providers=json.dumps(watch_providers, ensure_ascii=False),
        recommendation_ids=json.dumps(recommendation_ids, ensure_ascii=False),
    )

    record.credits = parse_tmdb_credits(raw, media_type, record.tmdb_id)
    return record


def _provider_pick(p: dict) -> dict:
    return {
        "provider_id": p.get("provider_id"),
        "provider_name": p.get("provider_name"),
        "logo_path": p.get("logo_path"),
        "display_priority": p.get("display_priority"),
    }


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
    """Parse /person/{id} (with append_to_response=external_ids,images)."""
    tmdb_id = raw.get("id")
    if not tmdb_id:
        raise ValueError("tmdb person id missing")

    aka = raw.get("also_known_as") or []
    ext = raw.get("external_ids") or {}

    images_raw = (raw.get("images") or {}).get("profiles") or []
    images = [
        {
            "file_path": im.get("file_path"),
            "width": im.get("width"),
            "height": im.get("height"),
            "iso_639_1": im.get("iso_639_1"),
        }
        for im in images_raw
        if im.get("file_path")
    ]

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
        homepage=raw.get("homepage") or None,
        adult=int(bool(raw.get("adult"))) if raw.get("adult") is not None else None,
        imdb_id=ext.get("imdb_id") or raw.get("imdb_id"),
        facebook_id=ext.get("facebook_id"),
        instagram_id=ext.get("instagram_id"),
        twitter_id=ext.get("twitter_id"),
        tiktok_id=ext.get("tiktok_id"),
        youtube_id=ext.get("youtube_id"),
        wikidata_id=ext.get("wikidata_id"),
        images=json.dumps(images, ensure_ascii=False),
        display_popularity=raw.get("popularity"),
    )


def discover_results(raw: dict) -> tuple[list[int], int]:
    """Extract (tmdb_ids, total_pages) from a /discover response."""
    ids = [int(r["id"]) for r in (raw.get("results") or []) if r.get("id")]
    total_pages = int(raw.get("total_pages") or 0)
    return ids, total_pages
