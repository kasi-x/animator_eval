"""Unit tests for the §10.1 extended seesaawiki parser functions.

Covers: parse_episode_titles, parse_gross_studios, parse_theme_songs,
parse_production_committee, parse_original_work_info,
parse_credit_listing_positions.

All tests are pure unit tests — no network calls, no file I/O.
"""
from __future__ import annotations

import pytest

from src.scrapers.parsers.seesaawiki import (
    CommitteeMember,
    CreditWithPosition,
    EpisodeTitle,
    GrossStudio,
    OriginalWorkInfo,
    ThemeSong,
    parse_credit_listing_positions,
    parse_episode_titles,
    parse_gross_studios,
    parse_original_work_info,
    parse_production_committee,
    parse_theme_songs,
)


# ---------------------------------------------------------------------------
# parse_episode_titles
# ---------------------------------------------------------------------------


class TestParseEpisodeTitles:
    def test_inline_title_kagi_kagi(self):
        body = "第1話「夏の終わりのサヨナラ」\n脚本：山田太郎"
        titles = parse_episode_titles(body)
        assert len(titles) == 1
        assert titles[0].episode == 1
        assert titles[0].title == "夏の終わりのサヨナラ"

    def test_inline_title_kagi_dai_kai(self):
        # 第N回 variant
        body = "第2回「いつわりの愛」\n\n脚本：田中一郎"
        titles = parse_episode_titles(body)
        assert titles[0].episode == 2
        assert titles[0].title == "いつわりの愛"

    def test_inline_title_hash_prefix(self):
        body = "#3「タイトル三」\n脚本：鈴木"
        titles = parse_episode_titles(body)
        assert titles[0].episode == 3
        assert titles[0].title == "タイトル三"

    def test_multiple_episodes(self):
        body = (
            "第1話「エピソード一」\n脚本：A\n\n"
            "第2話「エピソード二」\n脚本：B\n\n"
            "第3話「エピソード三」\n脚本：C"
        )
        titles = parse_episode_titles(body)
        assert len(titles) == 3
        assert [t.episode for t in titles] == [1, 2, 3]
        assert [t.title for t in titles] == ["エピソード一", "エピソード二", "エピソード三"]

    def test_multiple_titles_on_episode_header(self):
        # Hidamari-style: 第N話「title1」「title2」 — only first title extracted
        body = "第4話「3月16日〜23日 まろやかツナ風味」「10月31日 ガガガガ」"
        titles = parse_episode_titles(body)
        assert len(titles) == 1
        assert titles[0].episode == 4
        assert "まろやかツナ風味" in titles[0].title

    def test_episode_without_title_not_included(self):
        body = "第5話\n\n脚本：山田"
        titles = parse_episode_titles(body)
        assert len(titles) == 0

    def test_sorted_by_episode_number(self):
        body = "第3話「三」\n第1話「一」\n第2話「二」"
        titles = parse_episode_titles(body)
        assert [t.episode for t in titles] == [1, 2, 3]

    def test_deduplicates_episode_numbers(self):
        body = "第1話「タイトル一」\n第1話「重複」"
        titles = parse_episode_titles(body)
        assert len(titles) == 1
        assert titles[0].title == "タイトル一"

    def test_returns_episode_title_instances(self):
        body = "第1話「test」"
        titles = parse_episode_titles(body)
        assert all(isinstance(t, EpisodeTitle) for t in titles)

    def test_empty_body_returns_empty(self):
        assert parse_episode_titles("") == []

    def test_body_with_no_episodes(self):
        body = "脚本：山田太郎\n監督：鈴木一郎"
        assert parse_episode_titles(body) == []


# ---------------------------------------------------------------------------
# parse_gross_studios
# ---------------------------------------------------------------------------


class TestParseGrossStudios:
    def test_episode_level_gross_studio(self):
        body = "第2話「タイトル」\n\n脚本：A\n\n制作協力：スタジオパストラル"
        studios = parse_gross_studios(body)
        assert len(studios) == 1
        assert studios[0].studio_name == "スタジオパストラル"
        assert studios[0].episode == 2

    def test_series_level_gross_studio_no_episode_header(self):
        body = "監督：山田\n制作協力：京都アニメーション"
        studios = parse_gross_studios(body)
        assert len(studios) == 1
        assert studios[0].studio_name == "京都アニメーション"
        assert studios[0].episode is None

    def test_multiple_episodes_different_studios(self):
        body = (
            "第1話「A」\n制作協力：スタジオA\n\n"
            "第2話「B」\n制作協力：スタジオB"
        )
        studios = parse_gross_studios(body)
        assert len(studios) == 2
        names = {s.studio_name for s in studios}
        assert "スタジオA" in names
        assert "スタジオB" in names

    def test_same_studio_repeated_different_episodes(self):
        body = (
            "第1話「A」\n制作協力：ガイナックス\n\n"
            "第2話「B」\n制作協力：ガイナックス"
        )
        studios = parse_gross_studios(body)
        # Different episodes → 2 records
        assert len(studios) == 2

    def test_deduplicates_same_episode_same_studio(self):
        body = "第1話「A」\n制作協力：スタジオパストラル\n制作協力：スタジオパストラル"
        studios = parse_gross_studios(body)
        assert len(studios) == 1

    def test_colon_style(self):
        body = "第3話「C」\n制作協力：スタジオパストラル"
        studios = parse_gross_studios(body)
        assert studios[0].studio_name == "スタジオパストラル"

    def test_space_style(self):
        body = "第3話「C」\n制作協力　スタジオパストラル"
        studios = parse_gross_studios(body)
        assert studios[0].studio_name == "スタジオパストラル"

    def test_returns_gross_studio_instances(self):
        body = "制作協力：テストスタジオ"
        studios = parse_gross_studios(body)
        assert all(isinstance(s, GrossStudio) for s in studios)

    def test_empty_body_returns_empty(self):
        assert parse_gross_studios("") == []


# ---------------------------------------------------------------------------
# parse_theme_songs
# ---------------------------------------------------------------------------


class TestParseThemeSongs:
    def _body_with_theme(self, lines: list[str]) -> str:
        return "\n".join(lines)

    def test_main_theme_with_credits(self):
        body = self._body_with_theme([
            "主題歌",
            "「ワイワイワールド」",
            "作詞：河岸亜砂",
            "作曲：菊池俊輔",
            "編曲：たかしまあきひこ",
            "唄：水森亜土",
        ])
        songs = parse_theme_songs(body)
        assert len(songs) == 4
        roles = {s.role for s in songs}
        assert "lyrics" in roles
        assert "music" in roles
        assert "arrangement" in roles
        assert "artist" in roles

    def test_inline_title_on_section_header(self):
        body = "主題歌「星空のエンジェル・クィーン」\n作詞：MOKO NANRI\nうた：デラ・セダカ"
        songs = parse_theme_songs(body)
        assert songs[0].song_title == "星空のエンジェル・クィーン"
        assert all(s.song_title == "星空のエンジェル・クィーン" for s in songs)

    def test_song_type_op_ed_insert(self):
        body = (
            "オープニングテーマ\n「OP曲名」\n作詞：A\n\n"
            "エンディングテーマ\n「ED曲名」\n作曲：B\n\n"
            "挿入歌\n「挿入歌名」\n歌：C"
        )
        songs = parse_theme_songs(body)
        types = {s.song_type for s in songs}
        assert "OP" in types
        assert "ED" in types
        assert "insert" in types

    def test_second_song_title_updates_current_title(self):
        body = (
            "主題歌\n"
            "「曲A」\n作詞：山田\n"
            "「曲B」\n作曲：鈴木"
        )
        songs = parse_theme_songs(body)
        titles = [s.song_title for s in songs]
        assert "曲A" in titles
        assert "曲B" in titles

    def test_multiple_names_split_by_separator(self):
        body = "主題歌\n「test」\n歌：A・B・C"
        songs = parse_theme_songs(body)
        names = [s.name for s in songs if s.role == "artist"]
        assert "A" in names
        assert "B" in names
        assert "C" in names

    def test_returns_theme_song_instances(self):
        body = "主題歌\n「test」\n作詞：X"
        songs = parse_theme_songs(body)
        assert all(isinstance(s, ThemeSong) for s in songs)

    def test_empty_body_returns_empty(self):
        assert parse_theme_songs("") == []

    def test_no_theme_section_returns_empty(self):
        body = "監督：山田\n脚本：鈴木"
        assert parse_theme_songs(body) == []

    def test_uta_role_maps_to_artist(self):
        body = "主題歌\n「test」\n唄：歌手名"
        songs = parse_theme_songs(body)
        assert songs[0].role == "artist"
        assert songs[0].name == "歌手名"


# ---------------------------------------------------------------------------
# parse_production_committee
# ---------------------------------------------------------------------------


class TestParseProductionCommittee:
    def test_dot_separated_members(self):
        body = "製作：ひだまり荘管理組合・TBS"
        members = parse_production_committee(body)
        names = [m.member_name for m in members]
        assert "ひだまり荘管理組合" in names
        assert "TBS" in names

    def test_comma_separated_members(self):
        body = "製作：光坂高校演劇部、TBS"
        members = parse_production_committee(body)
        names = [m.member_name for m in members]
        assert "光坂高校演劇部" in names
        assert "TBS" in names

    def test_single_committee_name(self):
        body = "製作：B型H系製作委員会"
        members = parse_production_committee(body)
        assert len(members) == 1
        assert members[0].member_name == "B型H系製作委員会"

    def test_colon_and_space_variants(self):
        body = "製作　光坂高校演劇部\n製作：TBS"
        members = parse_production_committee(body)
        names = [m.member_name for m in members]
        assert "光坂高校演劇部" in names
        assert "TBS" in names

    def test_deduplicates_members(self):
        body = "製作：TBS・NHK\n製作：TBS"
        members = parse_production_committee(body)
        names = [m.member_name for m in members]
        assert names.count("TBS") == 1

    def test_returns_committee_member_instances(self):
        body = "製作：テスト委員会"
        members = parse_production_committee(body)
        assert all(isinstance(m, CommitteeMember) for m in members)

    def test_empty_body_returns_empty(self):
        assert parse_production_committee("") == []

    def test_no_committee_line_returns_empty(self):
        body = "監督：山田\n脚本：鈴木"
        assert parse_production_committee(body) == []


# ---------------------------------------------------------------------------
# parse_original_work_info
# ---------------------------------------------------------------------------


class TestParseOriginalWorkInfo:
    def test_simple_author_colon(self):
        body = "原作：鳥山明"
        info = parse_original_work_info(body)
        assert info is not None
        assert info.author == "鳥山明"

    def test_author_with_publisher_inline(self):
        body = "原作：上村純子(集英社「週刊少年マガジン」連載)"
        info = parse_original_work_info(body)
        assert info is not None
        assert info.author == "上村純子"
        assert info.publisher == "集英社"
        assert info.magazine == "週刊少年マガジン"
        assert info.serialization_type == "serialized"

    def test_author_with_publisher_on_next_line(self):
        body = "原作　永井ゆうじ\n　　　(小学館「月刊コロコロコミック」掲載)"
        info = parse_original_work_info(body)
        assert info is not None
        assert info.author == "永井ゆうじ"
        assert info.publisher == "小学館"
        assert info.magazine == "月刊コロコロコミック"
        assert info.serialization_type == "serialized"

    def test_author_with_label_not_magazine(self):
        body = "原作：那波マオ(講談社「KCデザート」刊)"
        info = parse_original_work_info(body)
        assert info is not None
        assert info.publisher == "講談社"
        assert info.label == "KCデザート"
        assert info.serialization_type == "published"

    def test_returns_none_when_no_original_work(self):
        body = "監督：山田\n脚本：鈴木"
        assert parse_original_work_info(body) == None  # noqa: E711

    def test_author_without_publisher(self):
        body = "原作：蒼樹うめ"
        info = parse_original_work_info(body)
        assert info is not None
        assert info.author == "蒼樹うめ"
        assert info.publisher is None

    def test_footnote_marker_stripped_from_author(self):
        body = "原作：前田みのる*1"
        info = parse_original_work_info(body)
        assert info is not None
        assert info.author == "前田みのる"

    def test_returns_original_work_info_instance(self):
        body = "原作：test"
        result = parse_original_work_info(body)
        assert isinstance(result, OriginalWorkInfo)

    def test_space_separator_variant(self):
        body = "原作　蒼樹うめ"
        info = parse_original_work_info(body)
        assert info is not None
        assert info.author == "蒼樹うめ"

    def test_first_original_work_only(self):
        body = "原作：著者A\n原作：著者B"
        info = parse_original_work_info(body)
        assert info.author == "著者A"


# ---------------------------------------------------------------------------
# parse_credit_listing_positions
# ---------------------------------------------------------------------------


class TestParseCreditListingPositions:
    def test_positions_are_zero_based_monotonic(self):
        body = (
            "脚本：山田\n"
            "監督：鈴木\n"
            "作画監督：田中"
        )
        results = parse_credit_listing_positions(body)
        positions = [r.source_listing_position for r in results]
        assert positions == list(range(len(results)))

    def test_episode_credits_get_episode_number(self):
        body = "第3話「タイトル」\n脚本：山田\n作画監督：鈴木"
        results = parse_credit_listing_positions(body)
        assert all(r.episode == 3 for r in results)

    def test_series_credits_get_none_episode(self):
        body = "監督：山田\n脚本：鈴木"
        results = parse_credit_listing_positions(body)
        assert all(r.episode is None for r in results)

    def test_episode_resets_after_header(self):
        body = (
            "監督：山田\n\n"
            "第1話「A」\n脚本：A脚本\n\n"
            "第2話「B」\n脚本：B脚本"
        )
        results = parse_credit_listing_positions(body)
        series_credits = [r for r in results if r.episode is None]
        ep1_credits = [r for r in results if r.episode == 1]
        ep2_credits = [r for r in results if r.episode == 2]
        assert len(series_credits) == 1
        assert len(ep1_credits) == 1
        assert len(ep2_credits) == 1

    def test_cast_section_excluded(self):
        body = "監督：山田\nキャスト\n主人公（CV：鈴木）\nスタッフ\n脚本：田中"
        results = parse_credit_listing_positions(body)
        roles = {r.credit.role for r in results}
        assert "監督" in roles
        assert "脚本" in roles
        # Cast section is excluded
        assert all("CV" not in r.credit.role for r in results)

    def test_position_counter_is_global_across_episodes(self):
        body = (
            "監督：山田\n"
            "第1話「A」\n脚本：A\n"
            "第2話「B」\n脚本：B"
        )
        results = parse_credit_listing_positions(body)
        positions = [r.source_listing_position for r in results]
        # Positions must be strictly increasing
        assert positions == sorted(positions)
        assert positions[0] == 0

    def test_returns_credit_with_position_instances(self):
        body = "監督：山田"
        results = parse_credit_listing_positions(body)
        assert all(isinstance(r, CreditWithPosition) for r in results)

    def test_empty_body_returns_empty(self):
        assert parse_credit_listing_positions("") == []

    def test_multiple_names_same_role_get_sequential_positions(self):
        body = "原画：山田一郎、鈴木二郎、田中三郎"
        results = parse_credit_listing_positions(body)
        assert len(results) == 3
        positions = [r.source_listing_position for r in results]
        assert positions == [0, 1, 2]
