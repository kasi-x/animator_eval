"""MyAnimeList / Jikan API response parsers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

from src.runtime.models import BronzeAnime, Credit, Person, parse_role
from src.scrapers.hash_utils import hash_anime_data


# ── string constants ──────────────────────────────────────────────────────────

class MalProducerKind:
    STUDIO = "studio"
    PRODUCER = "producer"
    LICENSOR = "licensor"


class MalGenreKind:
    GENRE = "genre"
    EXPLICIT = "explicit_genre"
    THEME = "theme"
    DEMOGRAPHIC = "demographic"


class MalRelationTargetType:
    ANIME = "anime"
    MANGA = "manga"


# ── dataclasses (Card 01) ─────────────────────────────────────────────────────

@dataclass
class MalAnimeRecord:
    """raw Jikan v4 /anime/{id}/full response 最小変換版。SILVER 解釈は別タスク。
    H1 (No viewer ratings in scoring): display_* 列は scoring / edge_weight に参入してはならない。
    """
    mal_id: int
    url: str | None
    title: str
    title_english: str | None
    title_japanese: str | None
    titles_alt_json: str
    synonyms_json: str
    type: str | None
    source: str | None
    episodes: int | None
    status: str | None
    airing: bool
    aired_from: str | None
    aired_to: str | None
    aired_string: str | None
    duration_raw: str | None
    rating: str | None
    season: str | None
    year: int | None
    broadcast_day: str | None
    broadcast_time: str | None
    broadcast_timezone: str | None
    broadcast_string: str | None
    synopsis: str | None
    background: str | None
    approved: bool
    display_score: float | None
    display_scored_by: int | None
    display_rank: int | None
    display_popularity: int | None
    display_members: int | None
    display_favorites: int | None
    image_url: str | None
    image_url_large: str | None
    trailer_youtube_id: str | None
    fetched_at: str
    content_hash: str


@dataclass
class MalAnimeGenre:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_id: int
    genre_id: int
    name: str
    kind: str


@dataclass
class MalAnimeRelation:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_id: int
    relation_type: str
    target_type: str
    target_mal_id: int
    target_name: str
    target_url: str | None


@dataclass
class MalAnimeTheme:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_id: int
    kind: str
    position: int
    raw_text: str


@dataclass
class MalAnimeExternal:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_id: int
    name: str
    url: str


@dataclass
class MalAnimeStreaming:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_id: int
    name: str
    url: str


@dataclass
class MalAnimeVideoPromo:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_id: int
    title: str
    youtube_id: str | None
    url: str | None
    embed_url: str | None
    image_url: str | None


@dataclass
class MalAnimeVideoEp:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_id: int
    mal_episode_id: int | None
    episode_label: str
    url: str | None
    image_url: str | None


@dataclass
class MalAnimeEpisode:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。
    H1 (No viewer ratings in scoring): display_* 列は scoring / edge_weight に参入してはならない。
    """
    mal_id: int
    episode_no: int
    title: str | None
    title_japanese: str | None
    title_romanji: str | None
    aired: str | None
    filler: bool
    recap: bool
    forum_url: str | None
    synopsis: str | None
    display_score: float | None


@dataclass
class MalAnimePicture:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_id: int
    image_url: str
    small_image_url: str | None
    large_image_url: str | None


@dataclass
class MalAnimeStatistics:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。
    H1 (No viewer ratings in scoring): display_* 列は scoring / edge_weight に参入してはならない。
    """
    mal_id: int
    display_watching: int
    display_completed: int
    display_on_hold: int
    display_dropped: int
    display_plan_to_watch: int
    display_total: int
    display_scores_json: str


@dataclass
class MalAnimeMoreinfo:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_id: int
    moreinfo: str | None


@dataclass
class MalAnimeRecommendation:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_id: int
    recommended_mal_id: int
    recommended_url: str
    votes: int


@dataclass
class MalAnimeStudio:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_id: int
    mal_producer_id: int
    name: str
    kind: str
    url: str | None


@dataclass
class MalStaffCredit:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_id: int
    mal_person_id: int
    person_name: str
    position: str


@dataclass
class MalAnimeCharacter:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。
    H1 (No viewer ratings in scoring): display_* 列は scoring / edge_weight に参入してはならない。
    """
    mal_id: int
    mal_character_id: int
    character_name: str
    character_url: str | None
    role: str
    display_favorites: int
    image_url: str | None


@dataclass
class MalVaCredit:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_id: int
    mal_character_id: int
    mal_person_id: int
    person_name: str
    language: str


@dataclass
class MalPerson:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。
    H1 (No viewer ratings in scoring): display_* 列は scoring / edge_weight に参入してはならない。
    """
    mal_person_id: int
    url: str
    name: str
    given_name: str | None
    family_name: str | None
    name_kanji: str | None
    alternate_names_json: str
    website_url: str | None
    birthday: str | None
    display_favorites: int
    about: str | None
    image_url: str | None
    fetched_at: str
    content_hash: str


@dataclass
class MalPersonPicture:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_person_id: int
    image_url: str


@dataclass
class MalCharacter:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。
    H1 (No viewer ratings in scoring): display_* 列は scoring / edge_weight に参入してはならない。
    """
    mal_character_id: int
    url: str
    name: str
    name_kanji: str | None
    nicknames_json: str
    display_favorites: int
    about: str | None
    image_url: str | None
    fetched_at: str
    content_hash: str


@dataclass
class MalCharacterPicture:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_character_id: int
    image_url: str


@dataclass
class MalProducer:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。
    H1 (No viewer ratings in scoring): display_* 列は scoring / edge_weight に参入してはならない。
    """
    mal_producer_id: int
    url: str
    titles_json: str
    title_default: str
    title_japanese: str | None
    established: str | None
    about: str | None
    count: int
    display_favorites: int
    image_url: str | None
    fetched_at: str
    content_hash: str


@dataclass
class MalProducerExternal:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_producer_id: int
    name: str
    url: str


@dataclass
class MalManga:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。
    H1 (No viewer ratings in scoring): display_* 列は scoring / edge_weight に参入してはならない。
    """
    mal_manga_id: int
    url: str
    title: str
    title_english: str | None
    title_japanese: str | None
    titles_alt_json: str
    type: str | None
    chapters: int | None
    volumes: int | None
    status: str | None
    publishing: bool
    published_from: str | None
    published_to: str | None
    synopsis: str | None
    background: str | None
    display_score: float | None
    display_scored_by: int | None
    display_rank: int | None
    display_popularity: int | None
    display_members: int | None
    display_favorites: int | None
    image_url: str | None
    fetched_at: str
    content_hash: str


@dataclass
class MalMangaAuthor:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_manga_id: int
    mal_person_id: int
    name: str
    role: str


@dataclass
class MalMangaSerialization:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_manga_id: int
    mal_magazine_id: int
    name: str
    url: str | None


@dataclass
class MalAnimeNews:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_id: int
    mal_news_id: int
    url: str
    title: str
    date: str
    author_username: str | None
    author_url: str | None
    forum_url: str | None
    intro: str | None
    image_url: str | None


@dataclass
class MalAnimeSchedule:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_id: int
    day_of_week: str
    snapshot_date: str


@dataclass
class MalMasterGenre:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    genre_id: int
    name: str
    url: str
    count: int
    kind: str


@dataclass
class MalMasterMagazine:
    """raw Jikan v4 response 最小変換版。SILVER 解釈は別タスク。"""
    mal_magazine_id: int
    name: str
    url: str
    count: int


# ── helpers ───────────────────────────────────────────────────────────────────

def _extract_image_url(images: dict) -> str | None:
    jpg = (images or {}).get("jpg") or {}
    return jpg.get("image_url")


def _extract_image_url_large(images: dict) -> str | None:
    jpg = (images or {}).get("jpg") or {}
    return jpg.get("large_image_url")


def _extract_titles(titles: list[dict]) -> tuple[str | None, str | None, str, str]:
    """Returns (title_english, title_japanese, titles_alt_json, synonyms_json)."""
    title_en = None
    title_ja = None
    synonyms = []
    for t in titles or []:
        kind = t.get("type", "")
        val = t.get("title") or ""
        if kind == "English":
            title_en = val
        elif kind == "Japanese":
            title_ja = val
        elif kind == "Synonym":
            synonyms.append(val)
    return (
        title_en,
        title_ja,
        json.dumps(titles or [], ensure_ascii=False),
        json.dumps(synonyms, ensure_ascii=False),
    )


def _extract_news_id(url: str) -> int:
    try:
        return int(url.rstrip("/").split("-")[0].split("/")[-1])
    except (ValueError, IndexError):
        return 0


# ── parsers (Card 02) ─────────────────────────────────────────────────────────

def parse_anime_full(raw: dict) -> tuple[
    MalAnimeRecord,
    list[MalAnimeGenre],
    list[MalAnimeRelation],
    list[MalAnimeTheme],
    list[MalAnimeExternal],
    list[MalAnimeStreaming],
    list[MalAnimeStudio],
]:
    """`/anime/{id}/full` 全フィールド抽出。
    H1: score / scored_by / rank / popularity / members / favorites は display_* prefix。
    """
    data = (raw.get("data") or {})
    mal_id: int = data.get("mal_id") or 0
    aired = (data.get("aired") or {})
    broadcast = (data.get("broadcast") or {})
    images = (data.get("images") or {})
    trailer = (data.get("trailer") or {})
    titles = data.get("titles") or []
    title_en, title_ja, titles_alt_json, synonyms_json = _extract_titles(titles)

    record = MalAnimeRecord(
        mal_id=mal_id,
        url=data.get("url"),
        title=data.get("title") or "",
        title_english=title_en,
        title_japanese=title_ja,
        titles_alt_json=titles_alt_json,
        synonyms_json=synonyms_json,
        type=data.get("type"),
        source=data.get("source"),
        episodes=data.get("episodes"),
        status=data.get("status"),
        airing=bool(data.get("airing")),
        aired_from=aired.get("from"),
        aired_to=aired.get("to"),
        aired_string=aired.get("string"),
        duration_raw=data.get("duration"),
        rating=data.get("rating"),
        season=data.get("season"),
        year=data.get("year"),
        broadcast_day=broadcast.get("day"),
        broadcast_time=broadcast.get("time"),
        broadcast_timezone=broadcast.get("timezone"),
        broadcast_string=broadcast.get("string"),
        synopsis=data.get("synopsis"),
        background=data.get("background"),
        approved=bool(data.get("approved")),
        display_score=data.get("score"),
        display_scored_by=data.get("scored_by"),
        display_rank=data.get("rank"),
        display_popularity=data.get("popularity"),
        display_members=data.get("members"),
        display_favorites=data.get("favorites"),
        image_url=_extract_image_url(images),
        image_url_large=_extract_image_url_large(images),
        trailer_youtube_id=trailer.get("youtube_id"),
        fetched_at=datetime.now(tz=timezone.utc).isoformat(),
        content_hash=hash_anime_data(data),
    )

    genres: list[MalAnimeGenre] = []
    kind_map = {
        "genres": MalGenreKind.GENRE,
        "explicit_genres": MalGenreKind.EXPLICIT,
        "themes": MalGenreKind.THEME,
        "demographics": MalGenreKind.DEMOGRAPHIC,
    }
    for field, kind in kind_map.items():
        for g in (data.get(field) or []):
            genres.append(MalAnimeGenre(
                mal_id=mal_id,
                genre_id=g.get("mal_id") or 0,
                name=g.get("name") or "",
                kind=kind,
            ))

    relations: list[MalAnimeRelation] = []
    for rel in (data.get("relations") or []):
        rel_type = rel.get("relation") or ""
        for entry in (rel.get("entry") or []):
            relations.append(MalAnimeRelation(
                mal_id=mal_id,
                relation_type=rel_type,
                target_type=entry.get("type") or "",
                target_mal_id=entry.get("mal_id") or 0,
                target_name=entry.get("name") or "",
                target_url=entry.get("url"),
            ))

    themes: list[MalAnimeTheme] = []
    theme = (data.get("theme") or {})
    for pos, text in enumerate(theme.get("openings") or []):
        themes.append(MalAnimeTheme(mal_id=mal_id, kind="opening", position=pos, raw_text=text))
    for pos, text in enumerate(theme.get("endings") or []):
        themes.append(MalAnimeTheme(mal_id=mal_id, kind="ending", position=pos, raw_text=text))

    externals: list[MalAnimeExternal] = []
    for e in (data.get("external") or []):
        if e.get("url"):
            externals.append(MalAnimeExternal(
                mal_id=mal_id, name=e.get("name") or "", url=e["url"],
            ))

    streamings: list[MalAnimeStreaming] = []
    for s in (data.get("streaming") or []):
        if s.get("url"):
            streamings.append(MalAnimeStreaming(
                mal_id=mal_id, name=s.get("name") or "", url=s["url"],
            ))

    studios: list[MalAnimeStudio] = []
    studio_kind_map = [
        ("studios", MalProducerKind.STUDIO),
        ("producers", MalProducerKind.PRODUCER),
        ("licensors", MalProducerKind.LICENSOR),
    ]
    for field, kind in studio_kind_map:
        for s in (data.get(field) or []):
            studios.append(MalAnimeStudio(
                mal_id=mal_id,
                mal_producer_id=s.get("mal_id") or 0,
                name=s.get("name") or "",
                kind=kind,
                url=s.get("url"),
            ))

    return record, genres, relations, themes, externals, streamings, studios


def parse_anime_external(mal_id: int, raw: dict) -> list[MalAnimeExternal]:
    """`/anime/{id}/external` レスポンス → MalAnimeExternal list。"""
    result = []
    for e in (raw.get("data") or []):
        if e.get("url"):
            result.append(MalAnimeExternal(
                mal_id=mal_id, name=e.get("name") or "", url=e["url"],
            ))
    return result


def parse_anime_streaming(mal_id: int, raw: dict) -> list[MalAnimeStreaming]:
    """`/anime/{id}/streaming` レスポンス → MalAnimeStreaming list。"""
    result = []
    for s in (raw.get("data") or []):
        if s.get("url"):
            result.append(MalAnimeStreaming(
                mal_id=mal_id, name=s.get("name") or "", url=s["url"],
            ))
    return result


def parse_anime_videos(
    mal_id: int, raw: dict
) -> tuple[list[MalAnimeVideoPromo], list[MalAnimeVideoEp]]:
    """`/anime/{id}/videos` レスポンス → (promos, episode_videos)。"""
    data = (raw.get("data") or {})
    promos: list[MalAnimeVideoPromo] = []
    for p in (data.get("promo") or []):
        trailer = (p.get("trailer") or {})
        images = (trailer.get("images") or {})
        promos.append(MalAnimeVideoPromo(
            mal_id=mal_id,
            title=p.get("title") or "",
            youtube_id=trailer.get("youtube_id"),
            url=trailer.get("url"),
            embed_url=trailer.get("embed_url"),
            image_url=images.get("image_url"),
        ))
    ep_videos: list[MalAnimeVideoEp] = []
    for e in (data.get("episodes") or []):
        images = (e.get("images") or {}).get("jpg") or {}
        ep_videos.append(MalAnimeVideoEp(
            mal_id=mal_id,
            mal_episode_id=e.get("mal_id"),
            episode_label=e.get("episode") or "",
            url=e.get("url"),
            image_url=images.get("image_url"),
        ))
    return promos, ep_videos


def parse_anime_episodes(mal_id: int, raw: dict) -> list[MalAnimeEpisode]:
    """`/anime/{id}/episodes` レスポンス → MalAnimeEpisode list。"""
    result = []
    for ep in (raw.get("data") or []):
        result.append(MalAnimeEpisode(
            mal_id=mal_id,
            episode_no=ep.get("mal_id") or 0,
            title=ep.get("title"),
            title_japanese=ep.get("title_japanese"),
            title_romanji=ep.get("title_romanji"),
            aired=ep.get("aired"),
            filler=bool(ep.get("filler")),
            recap=bool(ep.get("recap")),
            forum_url=ep.get("forum_url"),
            synopsis=ep.get("synopsis"),
            display_score=ep.get("score"),
        ))
    return result


def parse_anime_pictures(mal_id: int, raw: dict) -> list[MalAnimePicture]:
    """`/anime/{id}/pictures` レスポンス → MalAnimePicture list。"""
    result = []
    for p in (raw.get("data") or []):
        jpg = (p.get("jpg") or {})
        url = jpg.get("image_url") or jpg.get("large_image_url")
        if url:
            result.append(MalAnimePicture(
                mal_id=mal_id,
                image_url=url,
                small_image_url=jpg.get("small_image_url"),
                large_image_url=jpg.get("large_image_url"),
            ))
    return result


def parse_anime_statistics(mal_id: int, raw: dict) -> MalAnimeStatistics:
    """`/anime/{id}/statistics` レスポンス → MalAnimeStatistics。
    H1: 全列 display_* prefix — scoring path に参入してはならない。
    """
    data = (raw.get("data") or {})
    return MalAnimeStatistics(
        mal_id=mal_id,
        display_watching=data.get("watching") or 0,
        display_completed=data.get("completed") or 0,
        display_on_hold=data.get("on_hold") or 0,
        display_dropped=data.get("dropped") or 0,
        display_plan_to_watch=data.get("plan_to_watch") or 0,
        display_total=data.get("total") or 0,
        display_scores_json=json.dumps(data.get("scores") or [], ensure_ascii=False),
    )


def parse_anime_moreinfo(mal_id: int, raw: dict) -> MalAnimeMoreinfo:
    """`/anime/{id}/moreinfo` レスポンス → MalAnimeMoreinfo。"""
    data = (raw.get("data") or {})
    return MalAnimeMoreinfo(
        mal_id=mal_id,
        moreinfo=data.get("moreinfo"),
    )


def parse_anime_recommendations(
    mal_id: int, raw: dict
) -> list[MalAnimeRecommendation]:
    """`/anime/{id}/recommendations` レスポンス → MalAnimeRecommendation list。"""
    result = []
    for r in (raw.get("data") or []):
        entry = (r.get("entry") or {})
        rec_id = entry.get("mal_id")
        rec_url = entry.get("url") or ""
        if rec_id:
            result.append(MalAnimeRecommendation(
                mal_id=mal_id,
                recommended_mal_id=rec_id,
                recommended_url=rec_url,
                votes=r.get("votes") or 0,
            ))
    return result


def parse_anime_news(mal_id: int, raw: dict) -> list[MalAnimeNews]:
    """`/anime/{id}/news` レスポンス → MalAnimeNews list。"""
    result = []
    for n in (raw.get("data") or []):
        url = n.get("url") or ""
        images = (n.get("images") or {}).get("jpg") or {}
        result.append(MalAnimeNews(
            mal_id=mal_id,
            mal_news_id=_extract_news_id(url),
            url=url,
            title=n.get("title") or "",
            date=n.get("date") or "",
            author_username=n.get("author_username"),
            author_url=n.get("author_url"),
            forum_url=n.get("forum_url"),
            intro=n.get("excerpt"),
            image_url=images.get("image_url"),
        ))
    return result


def parse_anime_characters_va(
    mal_id: int, raw: dict
) -> tuple[list[MalAnimeCharacter], list[MalVaCredit]]:
    """`/anime/{id}/characters` レスポンス → (characters, va_credits)。"""
    characters: list[MalAnimeCharacter] = []
    va_credits: list[MalVaCredit] = []
    for entry in (raw.get("data") or []):
        char = (entry.get("character") or {})
        char_id = char.get("mal_id")
        if not char_id:
            continue
        images = (char.get("images") or {})
        characters.append(MalAnimeCharacter(
            mal_id=mal_id,
            mal_character_id=char_id,
            character_name=char.get("name") or "",
            character_url=char.get("url"),
            role=entry.get("role") or "",
            display_favorites=entry.get("favorites") or 0,
            image_url=_extract_image_url(images),
        ))
        for va in (entry.get("voice_actors") or []):
            person = (va.get("person") or {})
            person_id = person.get("mal_id")
            if person_id:
                va_credits.append(MalVaCredit(
                    mal_id=mal_id,
                    mal_character_id=char_id,
                    mal_person_id=person_id,
                    person_name=person.get("name") or "",
                    language=va.get("language") or "",
                ))
    return characters, va_credits


def parse_anime_staff_full(mal_id: int, raw: dict) -> list[MalStaffCredit]:
    """`/anime/{id}/staff` レスポンス → MalStaffCredit list。"""
    result = []
    for entry in (raw.get("data") or []):
        person = (entry.get("person") or {})
        person_id = person.get("mal_id")
        if not person_id:
            continue
        for pos in (entry.get("positions") or []):
            result.append(MalStaffCredit(
                mal_id=mal_id,
                mal_person_id=person_id,
                person_name=person.get("name") or "",
                position=pos.strip(),
            ))
    return result


def parse_person_full(raw: dict) -> MalPerson:
    """`/people/{id}/full`。static info (name/kanji/birthday/about/favorites) のみ抽出。
    anime / voices / manga 出演リストは credit 生成に使わない (Phase A で取得済)。
    H1: display_favorites は scoring / edge_weight に参入してはならない。
    """
    data = (raw.get("data") or {})
    images = (data.get("images") or {})
    alt_names = data.get("alternate_names") or []
    return MalPerson(
        mal_person_id=data.get("mal_id") or 0,
        url=data.get("url") or "",
        name=data.get("name") or "",
        given_name=data.get("given_name"),
        family_name=data.get("family_name"),
        name_kanji=data.get("name_kanji"),
        alternate_names_json=json.dumps(alt_names, ensure_ascii=False),
        website_url=data.get("website_url"),
        birthday=data.get("birthday"),
        display_favorites=data.get("favorites") or 0,
        about=data.get("about"),
        image_url=_extract_image_url(images),
        fetched_at=datetime.now(tz=timezone.utc).isoformat(),
        content_hash=hash_anime_data(data),
    )


def parse_person_pictures(
    mal_person_id: int, raw: dict
) -> list[MalPersonPicture]:
    """`/people/{id}/pictures` レスポンス → MalPersonPicture list。"""
    result = []
    for p in (raw.get("data") or []):
        jpg = (p.get("jpg") or {})
        url = jpg.get("image_url") or jpg.get("large_image_url")
        if url:
            result.append(MalPersonPicture(mal_person_id=mal_person_id, image_url=url))
    return result


def parse_character_full(raw: dict) -> MalCharacter:
    """`/characters/{id}/full`。anime / manga 出演リストは credit 生成に使わない。
    H1: display_favorites は scoring / edge_weight に参入してはならない。
    """
    data = (raw.get("data") or {})
    images = (data.get("images") or {})
    nicknames = data.get("nicknames") or []
    return MalCharacter(
        mal_character_id=data.get("mal_id") or 0,
        url=data.get("url") or "",
        name=data.get("name") or "",
        name_kanji=data.get("name_kanji"),
        nicknames_json=json.dumps(nicknames, ensure_ascii=False),
        display_favorites=data.get("favorites") or 0,
        about=data.get("about"),
        image_url=_extract_image_url(images),
        fetched_at=datetime.now(tz=timezone.utc).isoformat(),
        content_hash=hash_anime_data(data),
    )


def parse_character_pictures(
    mal_character_id: int, raw: dict
) -> list[MalCharacterPicture]:
    """`/characters/{id}/pictures` レスポンス → MalCharacterPicture list。"""
    result = []
    for p in (raw.get("data") or []):
        jpg = (p.get("jpg") or {})
        url = jpg.get("image_url") or jpg.get("large_image_url")
        if url:
            result.append(MalCharacterPicture(
                mal_character_id=mal_character_id, image_url=url,
            ))
    return result


def parse_producer_full(
    raw: dict,
) -> tuple[MalProducer, list[MalProducerExternal]]:
    """`/producers/{id}/full` → (producer, external_links)。
    H1: display_favorites は scoring / edge_weight に参入してはならない。
    """
    data = (raw.get("data") or {})
    producer_id = data.get("mal_id") or 0
    images = (data.get("images") or {})
    titles = data.get("titles") or []
    titles_json = json.dumps(titles, ensure_ascii=False)
    title_default = ""
    title_ja = None
    for t in titles:
        if t.get("type") == "Default":
            title_default = t.get("title") or ""
        elif t.get("type") == "Japanese":
            title_ja = t.get("title")

    producer = MalProducer(
        mal_producer_id=producer_id,
        url=data.get("url") or "",
        titles_json=titles_json,
        title_default=title_default,
        title_japanese=title_ja,
        established=data.get("established"),
        about=data.get("about"),
        count=data.get("count") or 0,
        display_favorites=data.get("favorites") or 0,
        image_url=_extract_image_url(images),
        fetched_at=datetime.now(tz=timezone.utc).isoformat(),
        content_hash=hash_anime_data(data),
    )
    externals = parse_producer_external(producer_id, raw)
    return producer, externals


def parse_producer_external(
    mal_producer_id: int, raw: dict
) -> list[MalProducerExternal]:
    """`/producers/{id}/external` (または full レスポンス) → MalProducerExternal list。"""
    data = (raw.get("data") or {})
    ext_list = data.get("external") or []
    # /producers/{id}/external レスポンスは data が list の場合もある
    if isinstance(data, list):
        ext_list = data
    elif isinstance(ext_list, list):
        pass
    result = []
    for e in ext_list:
        if e.get("url"):
            result.append(MalProducerExternal(
                mal_producer_id=mal_producer_id,
                name=e.get("name") or "",
                url=e["url"],
            ))
    return result


def parse_manga_full(raw: dict) -> tuple[
    MalManga,
    list[MalMangaAuthor],
    list[MalMangaSerialization],
    list[MalAnimeRelation],
]:
    """`/manga/{id}/full` → (manga, authors, serializations, relations)。
    H1: display_* 列は scoring / edge_weight に参入してはならない。
    """
    data = (raw.get("data") or {})
    manga_id = data.get("mal_id") or 0
    images = (data.get("images") or {})
    titles = data.get("titles") or []
    title_en, title_ja, titles_alt_json, _ = _extract_titles(titles)
    published = (data.get("published") or {})

    manga = MalManga(
        mal_manga_id=manga_id,
        url=data.get("url") or "",
        title=data.get("title") or "",
        title_english=title_en,
        title_japanese=title_ja,
        titles_alt_json=titles_alt_json,
        type=data.get("type"),
        chapters=data.get("chapters"),
        volumes=data.get("volumes"),
        status=data.get("status"),
        publishing=bool(data.get("publishing")),
        published_from=published.get("from"),
        published_to=published.get("to"),
        synopsis=data.get("synopsis"),
        background=data.get("background"),
        display_score=data.get("score"),
        display_scored_by=data.get("scored_by"),
        display_rank=data.get("rank"),
        display_popularity=data.get("popularity"),
        display_members=data.get("members"),
        display_favorites=data.get("favorites"),
        image_url=_extract_image_url(images),
        fetched_at=datetime.now(tz=timezone.utc).isoformat(),
        content_hash=hash_anime_data(data),
    )

    authors: list[MalMangaAuthor] = []
    for a in (data.get("authors") or []):
        pid = a.get("mal_id")
        role_raw = a.get("type") or ""
        if pid:
            authors.append(MalMangaAuthor(
                mal_manga_id=manga_id,
                mal_person_id=pid,
                name=a.get("name") or "",
                role=role_raw,
            ))

    serials: list[MalMangaSerialization] = []
    for s in (data.get("serializations") or []):
        sid = s.get("mal_id")
        if sid:
            serials.append(MalMangaSerialization(
                mal_manga_id=manga_id,
                mal_magazine_id=sid,
                name=s.get("name") or "",
                url=s.get("url"),
            ))

    relations: list[MalAnimeRelation] = []
    for rel in (data.get("relations") or []):
        rel_type = rel.get("relation") or ""
        for entry in (rel.get("entry") or []):
            relations.append(MalAnimeRelation(
                mal_id=manga_id,
                relation_type=rel_type,
                target_type=entry.get("type") or "",
                target_mal_id=entry.get("mal_id") or 0,
                target_name=entry.get("name") or "",
                target_url=entry.get("url"),
            ))

    return manga, authors, serials, relations


def parse_schedules(
    raw: dict, day_of_week: str, snapshot_date: str
) -> list[MalAnimeSchedule]:
    """`/schedules?filter={day}` → MalAnimeSchedule list。"""
    result = []
    for entry in (raw.get("data") or []):
        mal_id = entry.get("mal_id")
        if mal_id:
            result.append(MalAnimeSchedule(
                mal_id=mal_id,
                day_of_week=day_of_week,
                snapshot_date=snapshot_date,
            ))
    return result


def parse_master_genres(raw: dict, kind: str) -> list[MalMasterGenre]:
    """`/genres/anime?filter={kind}` → MalMasterGenre list。"""
    result = []
    for g in (raw.get("data") or []):
        gid = g.get("mal_id")
        if gid:
            result.append(MalMasterGenre(
                genre_id=gid,
                name=g.get("name") or "",
                url=g.get("url") or "",
                count=g.get("count") or 0,
                kind=kind,
            ))
    return result


def parse_master_magazines(raw: dict) -> list[MalMasterMagazine]:
    """`/magazines` → MalMasterMagazine list。"""
    result = []
    for m in (raw.get("data") or []):
        mid = m.get("mal_id")
        if mid:
            result.append(MalMasterMagazine(
                mal_magazine_id=mid,
                name=m.get("name") or "",
                url=m.get("url") or "",
                count=m.get("count") or 0,
            ))
    return result


# ── legacy parsers (互換維持) ─────────────────────────────────────────────────

def parse_anime_data(raw: dict) -> BronzeAnime:
    mal_id = raw.get("mal_id")
    titles = raw.get("titles", [])
    title_ja, title_en = "", ""
    synonyms: list[str] = []
    for t in titles:
        if t.get("type") == "Japanese":
            title_ja = t.get("title", "")
        elif t.get("type") == "Default":
            title_en = t.get("title", "")
        elif t.get("type") == "English" and not title_en:
            title_en = t.get("title", "")
        elif t.get("type") == "Synonym":
            synonyms.append(t.get("title", ""))
    if not title_en:
        title_en = raw.get("title", "")

    aired = raw.get("aired", {}) or {}
    prop = aired.get("prop", {}) or {}
    from_prop = prop.get("from", {}) or {}
    to_prop = prop.get("to", {}) or {}
    year = raw.get("year") or from_prop.get("year")

    def _build_date(p: dict) -> str | None:
        y, m, d = p.get("year"), p.get("month"), p.get("day")
        if not y:
            return None
        if m and d:
            return f"{y:04d}-{m:02d}-{d:02d}"
        if m:
            return f"{y:04d}-{m:02d}"
        return str(y)

    genres = [g["name"] for g in raw.get("genres", []) if g.get("name")]

    return BronzeAnime(
        id=f"mal:{mal_id}",
        title_ja=title_ja,
        title_en=title_en,
        year=year,
        season=raw.get("season"),
        episodes=raw.get("episodes"),
        mal_id=mal_id,
        score=raw.get("score"),
        format=raw.get("type"),
        status=raw.get("status"),
        start_date=_build_date(from_prop),
        end_date=_build_date(to_prop),
        genres=genres,
        synonyms=synonyms,
    )


def parse_staff_data(
    staff_list: list[dict], anime_id: str
) -> tuple[list[Person], list[Credit]]:
    persons, credits = [], []
    for entry in staff_list:
        person_data = entry.get("person", {})
        mal_person_id = person_data.get("mal_id")
        if not mal_person_id:
            continue
        person_id = f"mal:p{mal_person_id}"
        name = person_data.get("name", "")
        parts = name.split(", ", 1)
        name_en = f"{parts[1]} {parts[0]}" if len(parts) == 2 else name
        persons.append(Person(id=person_id, name_en=name_en, mal_id=mal_person_id))
        for pos in entry.get("positions", []):
            credits.append(
                Credit(
                    person_id=person_id,
                    anime_id=anime_id,
                    role=parse_role(pos),
                    source="mal",
                )
            )
    return persons, credits
