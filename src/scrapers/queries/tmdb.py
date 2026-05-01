"""TMDB v3 REST API endpoints.

Authentication: v4 read access token (Bearer) preferred. Set ``TMDB_BEARER``.
v3 ``api_key=`` query param is supported as fallback (``TMDB_API_KEY``).

Rate limit: 50 req/sec (no daily quota since 2024).
Genre 16 = Animation. Discover paging caps at 500 pages (10 000 items per
filter combo); split by year if exceeded.
"""

from __future__ import annotations

from urllib.parse import urlencode

TMDB_BASE = "https://api.themoviedb.org/3"

ANIMATION_GENRE_ID = 16
DISCOVER_PAGE_LIMIT = 500  # TMDB hard cap


def discover_url(
    media: str,
    page: int = 1,
    *,
    year: int | None = None,
    sort_by: str = "primary_release_date.asc",
) -> str:
    """Build /discover/{tv|movie} URL with anime genre filter.

    Args:
        media: 'tv' or 'movie'
        page: 1-indexed
        year: optional year filter (first_air_date_year for tv,
              primary_release_year for movie). Use to break the 10 000-item
              page-cap when a single discover call would exceed it.
    """
    if media not in ("tv", "movie"):
        raise ValueError(f"media must be 'tv' or 'movie', got {media!r}")
    params: dict[str, str | int] = {
        "with_genres": ANIMATION_GENRE_ID,
        "page": page,
        "sort_by": sort_by,
        "include_adult": "false",
    }
    if year is not None:
        key = "first_air_date_year" if media == "tv" else "primary_release_year"
        params[key] = year
    return f"{TMDB_BASE}/discover/{media}?{urlencode(params)}"


_COMMON_APPEND = (
    "external_ids,keywords,alternative_titles,translations,videos,images,"
    "watch/providers,recommendations"
)
# TV-specific: aggregate_credits (per-episode roll-up), content_ratings.
# Movie-specific: credits (flat), release_dates (per-country theatrical/digital/physical).
_TV_APPEND = f"aggregate_credits,content_ratings,{_COMMON_APPEND}"
_MOVIE_APPEND = f"credits,release_dates,{_COMMON_APPEND}"


def detail_url(media: str, tmdb_id: int) -> str:
    """Detail + ALL append_to_response sub-resources in a single request.

    TMDB allows up to 20 appended sub-resources per call; the lists below
    stay under that cap. include_image_language=null,en,ja keeps the
    image set tractable (TMDB returns one row per language per file).
    """
    if media == "tv":
        append = _TV_APPEND
    elif media == "movie":
        append = _MOVIE_APPEND
    else:
        raise ValueError(f"media must be 'tv' or 'movie', got {media!r}")
    return (
        f"{TMDB_BASE}/{media}/{tmdb_id}"
        f"?append_to_response={append}"
        "&include_image_language=null,en,ja"
    )


def person_url(tmdb_person_id: int) -> str:
    return (
        f"{TMDB_BASE}/person/{tmdb_person_id}"
        "?append_to_response=external_ids,images"
    )


def find_by_external_id(external_id: str, source: str) -> str:
    """Reverse lookup by external ID (imdb_id / tvdb_id / wikidata_id).

    Useful for entity resolution against AniList / Bangumi etc.
    Source must be one of: imdb_id, tvdb_id, freebase_mid, freebase_id,
    tvrage_id, wikidata_id, facebook_id, instagram_id, twitter_id.
    """
    return f"{TMDB_BASE}/find/{external_id}?external_source={source}"
