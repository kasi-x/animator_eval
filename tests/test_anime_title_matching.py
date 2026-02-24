"""Tests for anime title matching (MADB ↔ AniList)."""

import pytest

from src.analysis.anime_title_matching import (
    AnimeMatch,
    match_anime_titles,
    normalize_anime_title,
)


# ---------- normalize_anime_title ----------


class TestNormalizeAnimeTitle:
    def test_nfkc_normalization(self):
        # Fullwidth → halfwidth
        assert normalize_anime_title("ガンダム") == "ガンダム"

    def test_strip_tv_suffix(self):
        assert normalize_anime_title("進撃の巨人 (TV)") == "進撃の巨人"

    def test_strip_ova_suffix(self):
        assert normalize_anime_title("AKIRA [OVA]") == "AKIRA"

    def test_strip_season_suffix(self):
        assert (
            normalize_anime_title("僕のヒーローアカデミア（第2期）")
            == "僕のヒーローアカデミア"
        )

    def test_strip_movie_suffix(self):
        assert normalize_anime_title("ドラゴンボール（劇場版）") == "ドラゴンボール"

    def test_whitespace_normalization(self):
        assert normalize_anime_title("  進撃  の  巨人  ") == "進撃 の 巨人"

    def test_punctuation_normalization(self):
        # ・ and ～ and : are normalized to spaces
        assert normalize_anime_title("Fate～stay night") == "Fate stay night"

    def test_empty_string(self):
        assert normalize_anime_title("") == ""

    def test_none_like_empty(self):
        assert normalize_anime_title("") == ""

    def test_preserves_core_title(self):
        assert (
            normalize_anime_title("新世紀エヴァンゲリオン") == "新世紀エヴァンゲリオン"
        )


# ---------- match_anime_titles ----------


def _madb(id: str, title: str, year: int | None = None) -> dict:
    return {"id": id, "title": title, "year": year}


def _anilist(
    id: str,
    title_ja: str = "",
    title_en: str = "",
    year: int | None = None,
    synonyms: list[str] | None = None,
) -> dict:
    return {
        "id": id,
        "title_ja": title_ja,
        "title_en": title_en,
        "year": year,
        "synonyms": synonyms or [],
    }


class TestExactMatch:
    def test_exact_match_ja(self):
        madb = [_madb("madb:C1", "進撃の巨人", 2013)]
        anilist = [_anilist("anilist:1", title_ja="進撃の巨人", year=2013)]
        matches = match_anime_titles(madb, anilist)
        assert len(matches) == 1
        assert matches[0].strategy == "exact"
        assert matches[0].madb_anime_id == "madb:C1"
        assert matches[0].anilist_anime_id == "anilist:1"
        assert matches[0].score == 100.0

    def test_exact_match_en(self):
        madb = [_madb("madb:C1", "Cowboy Bebop", 1998)]
        anilist = [_anilist("anilist:1", title_en="Cowboy Bebop", year=1998)]
        matches = match_anime_titles(madb, anilist)
        assert len(matches) == 1
        assert matches[0].strategy == "exact"

    def test_exact_match_with_suffix_stripped(self):
        """MADB has (TV) suffix, AniList does not."""
        madb = [_madb("madb:C1", "進撃の巨人 (TV)", 2013)]
        anilist = [_anilist("anilist:1", title_ja="進撃の巨人", year=2013)]
        matches = match_anime_titles(madb, anilist)
        assert len(matches) == 1
        assert matches[0].strategy == "exact"


class TestFuzzyMatch:
    def test_fuzzy_above_threshold(self):
        # Slightly different titles
        madb = [_madb("madb:C1", "機動戦士ガンダム SEED", 2002)]
        anilist = [_anilist("anilist:1", title_ja="機動戦士ガンダムSEED", year=2002)]
        matches = match_anime_titles(madb, anilist, threshold=80)
        assert len(matches) == 1
        assert matches[0].strategy == "fuzzy"
        assert matches[0].score >= 80

    def test_fuzzy_below_threshold(self):
        # Very different titles
        madb = [_madb("madb:C1", "ドラゴンボール", 1986)]
        anilist = [_anilist("anilist:1", title_ja="ワンピース", year=1999)]
        matches = match_anime_titles(madb, anilist, threshold=90)
        assert len(matches) == 0


class TestYearValidation:
    def test_year_match(self):
        madb = [_madb("madb:C1", "AKIRA", 1988)]
        anilist = [_anilist("anilist:1", title_ja="AKIRA", year=1988)]
        matches = match_anime_titles(madb, anilist)
        assert len(matches) == 1

    def test_year_within_tolerance(self):
        madb = [_madb("madb:C1", "AKIRA", 1987)]
        anilist = [_anilist("anilist:1", title_ja="AKIRA", year=1988)]
        matches = match_anime_titles(madb, anilist, year_tolerance=1)
        assert len(matches) == 1

    def test_year_outside_tolerance(self):
        madb = [_madb("madb:C1", "AKIRA", 1985)]
        anilist = [_anilist("anilist:1", title_ja="AKIRA", year=1988)]
        matches = match_anime_titles(madb, anilist, year_tolerance=1)
        assert len(matches) == 0

    def test_year_missing_madb(self):
        """Missing year on MADB side should still allow match."""
        madb = [_madb("madb:C1", "AKIRA", None)]
        anilist = [_anilist("anilist:1", title_ja="AKIRA", year=1988)]
        matches = match_anime_titles(madb, anilist)
        assert len(matches) == 1

    def test_year_missing_anilist(self):
        """Missing year on AniList side should still allow match."""
        madb = [_madb("madb:C1", "AKIRA", 1988)]
        anilist = [_anilist("anilist:1", title_ja="AKIRA", year=None)]
        matches = match_anime_titles(madb, anilist)
        assert len(matches) == 1

    def test_year_missing_both(self):
        madb = [_madb("madb:C1", "AKIRA", None)]
        anilist = [_anilist("anilist:1", title_ja="AKIRA", year=None)]
        matches = match_anime_titles(madb, anilist)
        assert len(matches) == 1


class TestOneToOneEnforcement:
    def test_no_duplicate_anilist_match(self):
        """Two MADB anime should not match the same AniList anime."""
        madb = [
            _madb("madb:C1", "AKIRA", 1988),
            _madb("madb:C2", "AKIRA", 1988),
        ]
        anilist = [_anilist("anilist:1", title_ja="AKIRA", year=1988)]
        matches = match_anime_titles(madb, anilist)
        # Only one should match (first wins)
        assert len(matches) == 1
        assert matches[0].madb_anime_id == "madb:C1"

    def test_no_duplicate_madb_match(self):
        """Same MADB anime should not match two AniList anime."""
        madb = [_madb("madb:C1", "AKIRA", 1988)]
        anilist = [
            _anilist("anilist:1", title_ja="AKIRA", year=1988),
            _anilist("anilist:2", title_ja="AKIRA", year=1988),
        ]
        matches = match_anime_titles(madb, anilist)
        # Ambiguous — should skip (0 matches) since two AniList have same title+year
        assert len(matches) == 0


class TestAmbiguityRejection:
    def test_multiple_candidates_same_year_rejected(self):
        """Multiple AniList candidates with same title+year should be rejected."""
        madb = [_madb("madb:C1", "ルパン三世", 2015)]
        anilist = [
            _anilist("anilist:1", title_ja="ルパン三世", year=2015),
            _anilist("anilist:2", title_ja="ルパン三世", year=2015),
        ]
        matches = match_anime_titles(madb, anilist)
        assert len(matches) == 0

    def test_multiple_candidates_different_year_resolved(self):
        """Multiple AniList candidates with different years should resolve."""
        madb = [_madb("madb:C1", "ルパン三世", 2015)]
        anilist = [
            _anilist("anilist:1", title_ja="ルパン三世", year=2015),
            _anilist("anilist:2", title_ja="ルパン三世", year=1971),
        ]
        matches = match_anime_titles(madb, anilist, year_tolerance=1)
        assert len(matches) == 1
        assert matches[0].anilist_anime_id == "anilist:1"


class TestSynonymMatch:
    def test_match_via_synonym(self):
        madb = [_madb("madb:C1", "Attack on Titan", 2013)]
        anilist = [
            _anilist(
                "anilist:1",
                title_ja="進撃の巨人",
                year=2013,
                synonyms=["Attack on Titan"],
            )
        ]
        matches = match_anime_titles(madb, anilist)
        assert len(matches) == 1
        assert matches[0].strategy == "exact"


class TestEmptyInput:
    def test_empty_madb(self):
        matches = match_anime_titles([], [_anilist("anilist:1", title_ja="AKIRA")])
        assert matches == []

    def test_empty_anilist(self):
        matches = match_anime_titles([_madb("madb:C1", "AKIRA")], [])
        assert matches == []

    def test_both_empty(self):
        matches = match_anime_titles([], [])
        assert matches == []

    def test_madb_empty_title(self):
        matches = match_anime_titles(
            [_madb("madb:C1", "", 2000)],
            [_anilist("anilist:1", title_ja="AKIRA", year=2000)],
        )
        assert matches == []


class TestAnimeMatchDataclass:
    def test_frozen(self):
        m = AnimeMatch(
            madb_anime_id="madb:C1",
            anilist_anime_id="anilist:1",
            madb_title="AKIRA",
            anilist_title="AKIRA",
            score=100.0,
            strategy="exact",
        )
        with pytest.raises(AttributeError):
            m.score = 50.0  # type: ignore[misc]

    def test_fields(self):
        m = AnimeMatch(
            madb_anime_id="madb:C1",
            anilist_anime_id="anilist:1",
            madb_title="A",
            anilist_title="B",
            score=95.0,
            strategy="fuzzy",
        )
        assert m.madb_anime_id == "madb:C1"
        assert m.anilist_anime_id == "anilist:1"
        assert m.score == 95.0
        assert m.strategy == "fuzzy"


class TestMultipleMatches:
    def test_batch_matching(self):
        """Multiple MADB anime should match independently."""
        madb = [
            _madb("madb:C1", "進撃の巨人", 2013),
            _madb("madb:C2", "AKIRA", 1988),
            _madb("madb:C3", "ドラゴンボール", 1986),
        ]
        anilist = [
            _anilist("anilist:1", title_ja="進撃の巨人", year=2013),
            _anilist("anilist:2", title_ja="AKIRA", year=1988),
            _anilist("anilist:3", title_ja="ドラゴンボール", year=1986),
        ]
        matches = match_anime_titles(madb, anilist)
        assert len(matches) == 3
        matched_madb = {m.madb_anime_id for m in matches}
        matched_anilist = {m.anilist_anime_id for m in matches}
        assert matched_madb == {"madb:C1", "madb:C2", "madb:C3"}
        assert matched_anilist == {"anilist:1", "anilist:2", "anilist:3"}
