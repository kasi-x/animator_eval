"""Extended Jikan (MAL) parser unit tests — Card 02 coverage."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import pytest

from src.scrapers.parsers.mal import (
    MalAnimeEpisode,
    MalAnimeGenre,
    MalAnimeRecord,
    MalAnimeRelation,
    MalAnimeSchedule,
    MalAnimeStatistics,
    MalAnimeVideoEp,
    MalAnimeVideoPromo,
    MalCharacter,
    MalManga,
    MalMasterGenre,
    MalMasterMagazine,
    MalPerson,
    MalProducer,
    MalStaffCredit,
    MalVaCredit,
    parse_anime_characters_va,
    parse_anime_episodes,
    parse_anime_external,
    parse_anime_full,
    parse_anime_moreinfo,
    parse_anime_news,
    parse_anime_recommendations,
    parse_anime_staff_full,
    parse_anime_statistics,
    parse_anime_streaming,
    parse_anime_videos,
    parse_character_full,
    parse_manga_full,
    parse_master_genres,
    parse_master_magazines,
    parse_person_full,
    parse_producer_full,
    parse_schedules,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "scrapers" / "mal"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def anime_full_raw():
    return _load("anime_1_full.json")


@pytest.fixture
def anime_full_data(anime_full_raw):
    return anime_full_raw["data"]


@pytest.fixture
def staff_raw():
    return _load("anime_1_staff.json")


@pytest.fixture
def characters_raw():
    return _load("anime_1_characters.json")


@pytest.fixture
def episodes_raw():
    return _load("anime_1_episodes.json")


@pytest.fixture
def videos_raw():
    return _load("anime_1_videos.json")


@pytest.fixture
def pictures_raw():
    return _load("anime_1_pictures.json")


@pytest.fixture
def statistics_raw():
    return _load("anime_1_statistics.json")


@pytest.fixture
def moreinfo_raw():
    return _load("anime_1_moreinfo.json")


@pytest.fixture
def recommendations_raw():
    return _load("anime_1_recommendations.json")


@pytest.fixture
def news_raw():
    return _load("anime_1_news.json")


@pytest.fixture
def person_full_raw():
    return _load("people_1_full.json")


@pytest.fixture
def character_full_raw():
    return _load("characters_1_full.json")


@pytest.fixture
def producer_full_raw():
    return _load("producers_1_full.json")


@pytest.fixture
def manga_full_raw():
    return _load("manga_1_full.json")


@pytest.fixture
def genres_raw():
    return _load("genres_anime.json")


@pytest.fixture
def magazines_raw():
    return _load("magazines.json")


@pytest.fixture
def schedules_raw():
    return _load("schedules_filter_monday.json")


# ── parse_anime_full ──────────────────────────────────────────────────────────

def test_anime_full_returns_record(anime_full_raw):
    record, *_ = parse_anime_full(anime_full_raw)
    assert isinstance(record, MalAnimeRecord)
    assert record.mal_id == 1
    assert record.title == "Cowboy Bebop"


def test_anime_full_display_prefix(anime_full_raw):
    """H1: score 系 列はすべて display_* prefix 経由で格納されること。"""
    record, *_ = parse_anime_full(anime_full_raw)
    d = asdict(record)
    assert "score" not in d
    assert "display_score" in d
    assert "display_rank" in d
    assert "display_popularity" in d
    assert "display_members" in d
    assert "display_favorites" in d
    assert "display_scored_by" in d


def test_anime_full_display_score_not_none(anime_full_raw):
    record, *_ = parse_anime_full(anime_full_raw)
    assert record.display_score is not None
    assert record.display_score > 0


def test_anime_full_titles_alt_json(anime_full_raw):
    record, *_ = parse_anime_full(anime_full_raw)
    parsed = json.loads(record.titles_alt_json)
    assert isinstance(parsed, list)
    assert len(parsed) > 0


def test_anime_full_genres_four_kinds(anime_full_raw):
    _, genres, *_ = parse_anime_full(anime_full_raw)
    assert all(isinstance(g, MalAnimeGenre) for g in genres)
    assert len(genres) > 0


def test_anime_full_relations(anime_full_raw):
    _, _, relations, *_ = parse_anime_full(anime_full_raw)
    assert len(relations) > 0
    for r in relations:
        assert isinstance(r, MalAnimeRelation)
        assert r.mal_id == 1
        assert r.relation_type  # raw string, non-empty


def test_anime_full_themes_order_preserved(anime_full_raw):
    _, _, _, themes, *_ = parse_anime_full(anime_full_raw)
    openings = [t for t in themes if t.kind == "opening"]
    endings = [t for t in themes if t.kind == "ending"]
    assert len(openings) >= 1
    assert len(endings) >= 1
    positions = [t.position for t in openings]
    assert positions == sorted(positions)


def test_anime_full_studios_kind_separated(anime_full_raw):
    *_, studios = parse_anime_full(anime_full_raw)
    kinds = {s.kind for s in studios}
    assert "studio" in kinds
    assert all(s.kind in {"studio", "producer", "licensor"} for s in studios)


def test_anime_full_content_hash_stable(anime_full_raw):
    r1, *_ = parse_anime_full(anime_full_raw)
    r2, *_ = parse_anime_full(anime_full_raw)
    assert r1.content_hash == r2.content_hash
    assert len(r1.content_hash) == 40


def test_anime_full_empty_raw():
    r, genres, rels, themes, ext, stream, studios = parse_anime_full({})
    assert r.mal_id == 0
    assert genres == []
    assert rels == []


# ── parse_anime_staff_full ────────────────────────────────────────────────────

def test_staff_full_returns_credits(staff_raw):
    credits = parse_anime_staff_full(1, staff_raw)
    assert len(credits) > 0
    assert all(isinstance(c, MalStaffCredit) for c in credits)


def test_staff_full_raw_position(staff_raw):
    """position は正規化せず raw 文字列のまま保持。"""
    credits = parse_anime_staff_full(1, staff_raw)
    assert all(c.position for c in credits)
    assert all(c.mal_id == 1 for c in credits)


def test_staff_full_skips_missing_person_id():
    fake = {"data": [
        {"person": {"mal_id": None, "name": "Ghost"}, "positions": ["Director"]},
        {"person": {"mal_id": 42, "name": "Real"}, "positions": ["Animation Director"]},
    ]}
    credits = parse_anime_staff_full(99, fake)
    assert len(credits) == 1
    assert credits[0].mal_person_id == 42


# ── parse_anime_characters_va ─────────────────────────────────────────────────

def test_characters_va_returns_both(characters_raw):
    chars, vas = parse_anime_characters_va(1, characters_raw)
    assert len(chars) > 0
    assert len(vas) > 0


def test_characters_display_favorites_h1(characters_raw):
    chars, _ = parse_anime_characters_va(1, characters_raw)
    d = asdict(chars[0])
    assert "display_favorites" in d
    assert "favorites" not in d


def test_characters_va_language_raw(characters_raw):
    _, vas = parse_anime_characters_va(1, characters_raw)
    assert all(isinstance(v, MalVaCredit) for v in vas)
    assert all(v.language for v in vas)


# ── parse_anime_episodes ──────────────────────────────────────────────────────

def test_episodes_basic(episodes_raw):
    eps = parse_anime_episodes(1, episodes_raw)
    assert len(eps) > 0
    assert all(isinstance(e, MalAnimeEpisode) for e in eps)


def test_episodes_display_score_h1(episodes_raw):
    eps = parse_anime_episodes(1, episodes_raw)
    d = asdict(eps[0])
    assert "display_score" in d
    assert "score" not in d


def test_episodes_null_safe():
    raw = {"data": [{"mal_id": 1, "filler": False, "recap": False}]}
    eps = parse_anime_episodes(5, raw)
    assert eps[0].title is None
    assert eps[0].display_score is None


# ── parse_anime_videos ────────────────────────────────────────────────────────

def test_videos_returns_promos_and_eps(videos_raw):
    promos, ep_vids = parse_anime_videos(1, videos_raw)
    assert isinstance(promos, list)
    assert isinstance(ep_vids, list)
    assert all(isinstance(p, MalAnimeVideoPromo) for p in promos)
    assert all(isinstance(e, MalAnimeVideoEp) for e in ep_vids)


# ── parse_anime_external / streaming ─────────────────────────────────────────

def test_external_basic():
    raw = {"data": [{"name": "Official", "url": "http://example.com"}]}
    result = parse_anime_external(1, raw)
    assert result[0].name == "Official"
    assert result[0].url == "http://example.com"


def test_streaming_basic():
    raw = {"data": [{"name": "Crunchyroll", "url": "http://cr.com/anime/1"}]}
    result = parse_anime_streaming(1, raw)
    assert result[0].name == "Crunchyroll"


# ── parse_anime_statistics ────────────────────────────────────────────────────

def test_statistics_all_display_h1(statistics_raw):
    stat = parse_anime_statistics(1, statistics_raw)
    assert isinstance(stat, MalAnimeStatistics)
    d = asdict(stat)
    non_display = [k for k in d if not k.startswith("display_") and k != "mal_id"]
    assert non_display == [], f"non-display keys found: {non_display}"


def test_statistics_scores_json(statistics_raw):
    stat = parse_anime_statistics(1, statistics_raw)
    scores = json.loads(stat.display_scores_json)
    assert isinstance(scores, list)


# ── parse_anime_moreinfo ──────────────────────────────────────────────────────

def test_moreinfo_basic(moreinfo_raw):
    m = parse_anime_moreinfo(1, moreinfo_raw)
    assert m.mal_id == 1


def test_moreinfo_null_safe():
    m = parse_anime_moreinfo(99, {"data": {}})
    assert m.moreinfo is None


# ── parse_anime_recommendations ───────────────────────────────────────────────

def test_recommendations_basic(recommendations_raw):
    recs = parse_anime_recommendations(1, recommendations_raw)
    assert len(recs) > 0
    assert all(r.recommended_mal_id > 0 for r in recs)
    assert all(r.votes > 0 for r in recs)


def test_recommendations_empty():
    recs = parse_anime_recommendations(1, {"data": []})
    assert recs == []


# ── parse_anime_news ──────────────────────────────────────────────────────────

def test_news_basic(news_raw):
    items = parse_anime_news(1, news_raw)
    assert len(items) > 0
    assert all(item.mal_id == 1 for item in items)
    assert all(item.title for item in items)


def test_news_intro_maps_excerpt(news_raw):
    items = parse_anime_news(1, news_raw)
    assert any(item.intro is not None for item in items)


# ── parse_person_full ─────────────────────────────────────────────────────────

def test_person_full_basic(person_full_raw):
    p = parse_person_full(person_full_raw)
    assert isinstance(p, MalPerson)
    assert p.mal_person_id == 1
    assert p.name


def test_person_display_favorites_h1(person_full_raw):
    p = parse_person_full(person_full_raw)
    d = asdict(p)
    assert "display_favorites" in d
    assert "favorites" not in d


def test_person_alternate_names_json(person_full_raw):
    p = parse_person_full(person_full_raw)
    names = json.loads(p.alternate_names_json)
    assert isinstance(names, list)


def test_person_full_null_safe():
    p = parse_person_full({"data": {"mal_id": 5, "name": "Test"}})
    assert p.given_name is None
    assert p.about is None
    assert json.loads(p.alternate_names_json) == []


# ── parse_character_full ──────────────────────────────────────────────────────

def test_character_full_basic(character_full_raw):
    c = parse_character_full(character_full_raw)
    assert isinstance(c, MalCharacter)
    assert c.mal_character_id == 1
    assert c.name


def test_character_display_favorites_h1(character_full_raw):
    c = parse_character_full(character_full_raw)
    d = asdict(c)
    assert "display_favorites" in d
    assert "favorites" not in d


# ── parse_producer_full ───────────────────────────────────────────────────────

def test_producer_full_basic(producer_full_raw):
    prod, externals = parse_producer_full(producer_full_raw)
    assert isinstance(prod, MalProducer)
    assert prod.mal_producer_id == 1
    assert prod.title_default


def test_producer_display_favorites_h1(producer_full_raw):
    prod, _ = parse_producer_full(producer_full_raw)
    d = asdict(prod)
    assert "display_favorites" in d
    assert "favorites" not in d


def test_producer_external_inline(producer_full_raw):
    _, externals = parse_producer_full(producer_full_raw)
    assert len(externals) > 0
    assert all(e.mal_producer_id == 1 for e in externals)


def test_producer_titles_json(producer_full_raw):
    prod, _ = parse_producer_full(producer_full_raw)
    titles = json.loads(prod.titles_json)
    assert isinstance(titles, list)
    assert any(t.get("type") == "Default" for t in titles)


# ── parse_manga_full ──────────────────────────────────────────────────────────

def test_manga_full_basic(manga_full_raw):
    manga, authors, serials, rels = parse_manga_full(manga_full_raw)
    assert isinstance(manga, MalManga)
    assert manga.mal_manga_id == 1
    assert manga.title


def test_manga_display_prefix_h1(manga_full_raw):
    manga, *_ = parse_manga_full(manga_full_raw)
    d = asdict(manga)
    assert "display_score" in d
    assert "display_rank" in d
    assert "score" not in d
    assert "rank" not in d


def test_manga_authors(manga_full_raw):
    _, authors, *_ = parse_manga_full(manga_full_raw)
    assert len(authors) > 0
    assert all(a.mal_manga_id == 1 for a in authors)
    assert all(a.role for a in authors)


def test_manga_serializations(manga_full_raw):
    _, _, serials, _ = parse_manga_full(manga_full_raw)
    assert len(serials) > 0
    assert all(s.mal_manga_id == 1 for s in serials)


def test_manga_relations(manga_full_raw):
    _, _, _, rels = parse_manga_full(manga_full_raw)
    assert len(rels) > 0
    assert all(r.mal_id == 1 for r in rels)


# ── parse_schedules ───────────────────────────────────────────────────────────

def test_schedules_basic(schedules_raw):
    items = parse_schedules(schedules_raw, "monday", "2026-04-25")
    assert len(items) > 0
    assert all(isinstance(s, MalAnimeSchedule) for s in items)
    assert all(s.day_of_week == "monday" for s in items)
    assert all(s.snapshot_date == "2026-04-25" for s in items)


# ── parse_master_genres / magazines ──────────────────────────────────────────

def test_master_genres_basic(genres_raw):
    genres = parse_master_genres(genres_raw, "genre")
    assert len(genres) > 0
    assert all(isinstance(g, MalMasterGenre) for g in genres)
    assert all(g.kind == "genre" for g in genres)


def test_master_magazines_basic(magazines_raw):
    mags = parse_master_magazines(magazines_raw)
    assert len(mags) > 0
    assert all(isinstance(m, MalMasterMagazine) for m in mags)
    assert all(m.name for m in mags)


# ── H1 regression: no raw score key in dataclass dicts ───────────────────────

def test_h1_no_raw_score_in_anime_full(anime_full_raw):
    record, *_ = parse_anime_full(anime_full_raw)
    fields = asdict(record).keys()
    for forbidden in ("score", "popularity", "rank", "members", "favorites", "scored_by"):
        assert forbidden not in fields, f"H1 violation: '{forbidden}' in MalAnimeRecord fields"


def test_h1_no_raw_score_in_statistics(statistics_raw):
    stat = parse_anime_statistics(1, statistics_raw)
    fields = asdict(stat).keys()
    for forbidden in ("watching", "completed", "on_hold", "dropped", "plan_to_watch",
                      "total", "scores_json"):
        assert forbidden not in fields, f"H1 violation: '{forbidden}' in MalAnimeStatistics fields"
