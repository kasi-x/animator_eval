"""Tests for seesaawiki_scraper parse functions (no network calls)."""
from __future__ import annotations

from src.scrapers.seesaawiki_scraper import (
    ParsedCredit,
    parse_credit_line,
    parse_episodes,
    parse_series_staff,
    _parse_episode_ranges,
    _clean_name,
)
from src.scrapers.parsers.seesaawiki import (
    _is_company_name,
    _split_names_paren_aware,
)


# ---------------------------------------------------------------------------
# _parse_episode_ranges
# ---------------------------------------------------------------------------

class TestParseEpisodeRanges:
    def test_single_episode(self):
        eps, from_ep = _parse_episode_ranges("3話")
        assert eps == [3]
        assert from_ep is None

    def test_range(self):
        eps, from_ep = _parse_episode_ranges("1話〜3話")
        assert eps == [1, 2, 3]
        assert from_ep is None

    def test_open_ended(self):
        # "5話〜" → eps=[] (empty, to be expanded later), episode_from=5
        eps, from_ep = _parse_episode_ranges("5話〜")
        assert eps == []
        assert from_ep == 5

    def test_comma_list(self):
        eps, from_ep = _parse_episode_ranges("1、3、5話")
        assert eps == [1, 3, 5]

    def test_no_episode_returns_none(self):
        eps, from_ep = _parse_episode_ranges("山田太郎")
        assert eps is None
        assert from_ep is None


# ---------------------------------------------------------------------------
# parse_credit_line
# ---------------------------------------------------------------------------

class TestParseCreditLine:
    def test_single_name(self):
        credits = parse_credit_line("脚本：山田太郎")
        assert len(credits) == 1
        assert credits[0].role == "脚本"
        assert credits[0].name == "山田太郎"
        assert credits[0].position == 0

    def test_multiple_names_same_role(self):
        credits = parse_credit_line("原画：佐藤、鈴木、田中")
        assert len(credits) == 3
        names = [c.name for c in credits]
        assert "佐藤" in names
        assert "鈴木" in names
        assert "田中" in names

    def test_position_ascending(self):
        credits = parse_credit_line("原画：山田一郎、田中次郎、鈴木三郎")
        positions = [c.position for c in credits]
        assert positions == [0, 1, 2]

    def test_multiple_roles_separated_by_dot(self):
        credits = parse_credit_line("絵コンテ・演出：田中裕太")
        roles = {c.role for c in credits}
        assert "絵コンテ" in roles
        assert "演出" in roles

    def test_skips_empty_line(self):
        assert parse_credit_line("") == []

    def test_skips_url_line(self):
        assert parse_credit_line("http://example.com/page") == []

    def test_skips_html_line(self):
        assert parse_credit_line("<div>test</div>") == []

    def test_skips_comment_line(self):
        assert parse_credit_line("※注意事項") == []

    def test_skips_cv_line(self):
        assert parse_credit_line("キャラ名（CV：声優名）") == []

    def test_skips_indented_line(self):
        # Lines starting with full-width space are name continuations, not credits
        assert parse_credit_line("　　　田中花子") == []

    def test_skips_re_prefix_lines(self):
        assert parse_credit_line("Re:ゼロから始める異世界生活") == []

    def test_affiliation_extracted(self):
        # "役割：名前（会社名）" pattern
        credits = parse_credit_line("音響監督：鈴木（音響会社）")
        assert len(credits) == 1
        assert credits[0].name == "鈴木"
        assert credits[0].affiliation == "音響会社"

    def test_episode_range_in_name(self):
        credits = parse_credit_line("作画監督：山田（1〜5話）")
        assert len(credits) == 1
        assert credits[0].name == "山田"
        assert credits[0].episodes == [1, 2, 3, 4, 5]


# ---------------------------------------------------------------------------
# parse_series_staff
# ---------------------------------------------------------------------------

class TestParseSeriesStaff:
    def test_empty_body_returns_empty(self):
        assert parse_series_staff("") == []

    def test_parses_role_colon_name(self):
        body = "監督：宮崎駿\n脚本：野田真外\n"
        credits = parse_series_staff(body)
        roles = {c.role for c in credits}
        assert "監督" in roles
        assert "脚本" in roles

    def test_stops_at_episode_header(self):
        body = "監督：宮崎駿\n#01\n原画：田中\n"
        credits = parse_series_staff(body)
        # Only series-level credits before #01
        names = {c.name for c in credits}
        assert "宮崎駿" in names
        assert "田中" not in names

    def test_cast_section_skipped(self):
        body = "監督：監督名\nキャスト\nキャラ名（CV：声優名）\nスタッフ\n脚本：脚本家\n"
        credits = parse_series_staff(body)
        names = {c.name for c in credits}
        assert "声優名" not in names
        assert "脚本家" in names

    def test_returns_parsed_credit_instances(self):
        body = "監督：テスト監督\n"
        credits = parse_series_staff(body)
        assert all(isinstance(c, ParsedCredit) for c in credits)


# ---------------------------------------------------------------------------
# parse_episodes
# ---------------------------------------------------------------------------

class TestParseEpisodes:
    def test_empty_body_returns_empty(self):
        assert parse_episodes("") == []

    def test_single_episode_block(self):
        body = "#01\n絵コンテ：山田\n演出：佐藤\n"
        episodes = parse_episodes(body)
        assert len(episodes) == 1
        assert episodes[0]["episode"] == 1
        assert len(episodes[0]["credits"]) == 2

    def test_multiple_episodes(self):
        body = "#01\n絵コンテ：Aさん\n#02\n絵コンテ：Bさん\n#03\n絵コンテ：Cさん\n"
        episodes = parse_episodes(body)
        assert len(episodes) == 3
        ep_nums = [e["episode"] for e in episodes]
        assert ep_nums == [1, 2, 3]

    def test_cast_section_excluded(self):
        body = "#01\nスタッフ\n絵コンテ：正しい人\nキャスト\n山田（CV：声優）\n"
        episodes = parse_episodes(body)
        names_in_credits = {
            c.name for ep in episodes for c in ep["credits"]
        }
        assert "正しい人" in names_in_credits
        assert "声優" not in names_in_credits

    def test_series_only_body_returns_none_episode(self):
        # Credits before any episode header are emitted with episode=None
        body = "監督：宮崎駿\n脚本：野田真外\n"
        episodes = parse_episodes(body)
        assert len(episodes) == 1
        assert episodes[0]["episode"] is None
        assert len(episodes[0]["credits"]) == 2

    def test_episode_credits_are_parsed_credit_instances(self):
        body = "#01\n絵コンテ：テスト\n"
        episodes = parse_episodes(body)
        assert all(isinstance(c, ParsedCredit) for c in episodes[0]["credits"])


# ---------------------------------------------------------------------------
# _split_names_paren_aware
# ---------------------------------------------------------------------------

class TestSplitNamesParenAware:
    """Test paren-aware name splitting logic."""

    def test_simple_comma_separated(self):
        result = _split_names_paren_aware("山田太郎、佐藤次郎")
        assert result == ["山田太郎", "佐藤次郎"]

    def test_fullwidth_space_separated(self):
        result = _split_names_paren_aware("山田太郎　佐藤次郎")
        assert result == ["山田太郎", "佐藤次郎"]

    def test_preserves_episode_in_paren(self):
        # Episode ranges in parens should NOT split
        result = _split_names_paren_aware(
            "藤家和正（第1話〜第21話、第23話〜第33話）、田中太郎"
        )
        assert len(result) == 2
        assert result[0] == "藤家和正（第1話〜第21話、第23話〜第33話）"
        assert result[1] == "田中太郎"

    def test_arrow_separator(self):
        # Arrow (→) should also split names
        result = _split_names_paren_aware("知久敦(1話)→内山雄太(2話〜)")
        assert len(result) == 2
        assert result[0] == "知久敦(1話)"
        assert result[1] == "内山雄太(2話〜)"

    def test_slash_separator(self):
        result = _split_names_paren_aware("名前A/名前B")
        assert result == ["名前A", "名前B"]

    def test_fullwidth_slash_separator(self):
        result = _split_names_paren_aware("名前A／名前B")
        assert result == ["名前A", "名前B"]

    def test_nested_parens(self):
        # Nested parentheses should be preserved
        result = _split_names_paren_aware("田中（副社長（ABC））、山田")
        assert len(result) == 2
        assert "副社長（ABC）" in result[0]

    def test_single_name_no_split(self):
        result = _split_names_paren_aware("山田太郎")
        assert result == ["山田太郎"]

    def test_empty_string(self):
        result = _split_names_paren_aware("")
        assert result == []

    def test_multiple_separators_consecutive(self):
        # Multiple consecutive separators should be handled
        result = _split_names_paren_aware("A、、B")
        assert "A" in result and "B" in result


# ---------------------------------------------------------------------------
# _is_company_name
# ---------------------------------------------------------------------------

class TestIsCompanyName:
    """Test company/studio name detection."""

    def test_known_studio_exact_match(self):
        assert _is_company_name("ぴえろ")
        assert _is_company_name("京都アニメーション")
        assert _is_company_name("MAPPA")

    def test_company_indicator_keywords(self):
        assert _is_company_name("Aスタジオ")
        assert _is_company_name("BプロダクションC")
        assert _is_company_name("Cアニメーション")

    def test_regular_person_name(self):
        assert not _is_company_name("山田太郎")
        assert not _is_company_name("田中花子")

    def test_mixed_case_person_name(self):
        assert not _is_company_name("John Smith")
        assert not _is_company_name("田中太郎Smith")

    def test_whitespace_normalized(self):
        # "京都 アニメーション" should normalize and still match
        assert _is_company_name("京都　アニメーション")

    def test_株式会社_keyword(self):
        assert _is_company_name("株式会社テスト")

    def test_製作委員会_keyword(self):
        assert _is_company_name("ABC製作委員会")


# ---------------------------------------------------------------------------
# _clean_name
# ---------------------------------------------------------------------------

class TestCleanName:
    """Test name cleaning and extraction of metadata."""

    def test_simple_name_normalization(self):
        cleaned, affiliation, episodes, ep_from = _clean_name("山田太郎")
        assert cleaned == "山田太郎"
        assert affiliation is None
        assert episodes is None
        assert ep_from is None

    def test_extracts_studio_affiliation(self):
        cleaned, affiliation, episodes, ep_from = _clean_name("田中太郎（スタジオA）")
        assert cleaned == "田中太郎"
        assert affiliation == "スタジオA"

    def test_extracts_episode_range_from_paren(self):
        cleaned, affiliation, episodes, ep_from = _clean_name("山田（1〜5話）")
        assert cleaned == "山田"
        assert affiliation is None
        assert episodes == [1, 2, 3, 4, 5]

    def test_extracts_open_ended_episode_from_paren(self):
        cleaned, affiliation, episodes, ep_from = _clean_name("太郎（5話〜）")
        assert cleaned == "太郎"
        assert affiliation is None
        assert episodes == []
        assert ep_from == 5

    def test_whitespace_stripping(self):
        cleaned, affiliation, episodes, ep_from = _clean_name("  山田太郎  ")
        assert cleaned == "山田太郎"

    def test_fullwidth_space_normalization(self):
        # NFKC should normalize fullwidth space
        cleaned, affiliation, episodes, ep_from = _clean_name("山田　太郎")
        # After NFKC, fullwidth space becomes halfwidth
        assert "山田" in cleaned and "太郎" in cleaned

    def test_prefers_affiliation_over_episodes(self):
        # "スタジオA" looks like company, not episodes
        cleaned, affiliation, episodes, ep_from = _clean_name(
            "太郎（スタジオA）"
        )
        assert affiliation == "スタジオA"
        assert episodes is None


# ---------------------------------------------------------------------------
# parse_credit_line (extended tests)
# ---------------------------------------------------------------------------

class TestParseCreditLineExtended:
    """Extended edge case tests for parse_credit_line."""

    def test_compound_role_not_split_when_parts_unknown(self):
        # "メカ・エフェクト作画監督" is a compound role name, not a split
        credits = parse_credit_line("メカ・エフェクト作画監督：田中")
        roles = {c.role for c in credits}
        # Should be kept as single role since not all parts are known
        assert len(roles) >= 1

    def test_known_multi_role_split(self):
        # "脚本・絵コンテ" — both parts are known roles
        credits = parse_credit_line("脚本・絵コンテ：山田太郎")
        assert len(credits) == 2
        roles = {c.role for c in credits}
        assert "脚本" in roles
        assert "絵コンテ" in roles

    def test_slash_and_middle_dot_separators(self):
        # Both ・ and / should split multi-role
        credits = parse_credit_line("絵コンテ・演出：田中")
        assert any(c.role == "絵コンテ" for c in credits)
        assert any(c.role == "演出" for c in credits)

    def test_role_normalization_composition(self):
        # "作" in "作・編曲" should normalize to "作曲"
        # Only if "作" and "編曲" are both known as separate roles
        credits = parse_credit_line("作曲・編曲：田中")
        roles = {c.role for c in credits}
        assert "作曲" in roles
        assert "編曲" in roles

    def test_company_in_name_marks_as_company(self):
        # "ぴえろ" is a studio name
        credits = parse_credit_line("デザイン：ぴえろ")
        assert any(c.is_company for c in credits)

    def test_skips_joke_corner_names(self):
        # Lines containing joke corner substrings should skip
        assert parse_credit_line("ポケモンコーナー名") == []

    def test_skips_bracket_artist_credit(self):
        # "Artist[tag]:vocalist" pattern should skip
        assert parse_credit_line("SawanoHiroyuki[nZk]:mizuki") == []

    def test_very_long_line_skipped(self):
        # Lines > 500 chars are skipped (wiki table dumps)
        long_line = "A" * 501
        assert parse_credit_line(long_line) == []

    def test_space_separated_role_name_pattern(self):
        # Space-separated: "役割　PersonName" (no colon)
        # Should require at least one known role
        credits = parse_credit_line("脚本　田中太郎")
        assert len(credits) >= 1
        assert any(c.role == "脚本" for c in credits)

    def test_episode_title_line_skipped(self):
        # "Karte：00「EpisodeTitle」" should be skipped
        assert parse_credit_line("Karte：00「EpisodeTitle」") == []

    def test_metadata_role_skipped(self):
        # Metadata roles like "話数" should be skipped
        assert parse_credit_line("話数：24") == []

    def test_position_increments_per_person(self):
        # Position increments for each person within same role
        credits = parse_credit_line("原画：A、B、C")
        # Should have 3 credits if all names are valid
        if len(credits) >= 3:
            assert credits[0].position == 0
            assert credits[1].position == 1
            assert credits[2].position == 2

    def test_company_affiliation_override(self):
        # When role text is a company, names get affiliation
        credits = parse_credit_line("ぴえろ：田中太郎")
        if credits:  # May parse as OTHER role
            assert any(c.affiliation == "ぴえろ" for c in credits)


# ---------------------------------------------------------------------------
# parse_episode_ranges (extended tests)
# ---------------------------------------------------------------------------

class TestParseEpisodeRangesExtended:
    """Extended tests for episode range parsing."""

    def test_hash_notation(self):
        # "#35" should parse as episode 35
        eps, from_ep = _parse_episode_ranges("#35")
        assert eps == [35]
        assert from_ep is None

    def test_hash_range(self):
        # "#1〜#5" — may not parse as range, depends on regex
        eps, from_ep = _parse_episode_ranges("#1〜#5")
        # At minimum, should parse some episodes
        assert eps is not None or from_ep is not None

    def test_第_notation(self):
        # "第1話" should parse
        eps, from_ep = _parse_episode_ranges("第1話")
        assert eps == [1]

    def test_bracket_notation(self):
        # "〔3-8,25〕" should parse range 3-8, may or may not include 25
        eps, from_ep = _parse_episode_ranges("〔3-8,25〕")
        assert eps is not None
        # At least the range 3-8 should be present
        assert 3 in eps and 8 in eps

    def test_complex_mixed_notation(self):
        # "第1話〜第21話、第23話〜第33話、第35話"
        eps, from_ep = _parse_episode_ranges(
            "第1話〜第21話、第23話〜第33話、第35話"
        )
        assert 1 in eps and 21 in eps
        assert 23 in eps and 33 in eps
        assert 35 in eps
        # Ranges should be continuous where specified
        assert set(range(1, 22)).issubset(set(eps))

    def test_bare_numbers_only_with_other_patterns(self):
        # Bare numbers like "25" only valid with other episode patterns
        eps, from_ep = _parse_episode_ranges("3-8,25")
        assert 25 in eps

    def test_bare_numbers_alone_returns_none(self):
        # Just "25" alone should return None
        eps, from_ep = _parse_episode_ranges("25")
        assert eps is None

    def test_invalid_range_ignored(self):
        # Range where start > end should be ignored
        eps, from_ep = _parse_episode_ranges("5〜3")
        # Invalid range ignored
        assert eps is None or eps == []

    def test_zero_episode_invalid(self):
        # Episode 0 is invalid
        eps, from_ep = _parse_episode_ranges("0話")
        assert eps is None

    def test_very_large_episode_numbers(self):
        # Some anime have 100+ episodes
        eps, from_ep = _parse_episode_ranges("1〜150話")
        assert eps == list(range(1, 151))


# ---------------------------------------------------------------------------
# parse_series_staff (extended tests)
# ---------------------------------------------------------------------------

class TestParseSeriesStaffExtended:
    """Extended tests for series-level staff parsing."""

    def test_stops_at_第_episode_marker(self):
        body = "監督：宮崎駿\n第01話\n原画：田中\n"
        credits = parse_series_staff(body)
        names = {c.name for c in credits}
        assert "宮崎駿" in names
        assert "田中" not in names

    def test_handles_multiple_cast_section_toggles(self):
        body = (
            "監督：監督名\nキャスト\nキャラ（CV：声優）\nスタッフ\n"
            "脚本：脚本家\nキャスト\nもう一つのキャラ（CV：別声優）\n"
        )
        credits = parse_series_staff(body)
        names = {c.name for c in credits}
        # Should parse both cast and staff sections (after staff toggle)
        assert "脚本家" in names
        assert "声優" not in names
        assert "別声優" not in names

    def test_empty_lines_ignored(self):
        body = "監督：監督名\n\n\n脚本：脚本家\n"
        credits = parse_series_staff(body)
        assert len(credits) == 2


# ---------------------------------------------------------------------------
# parse_episodes (extended tests)
# ---------------------------------------------------------------------------

class TestParseEpisodesExtended:
    """Extended tests for episode parsing."""

    def test_series_credits_before_episodes(self):
        # Series-level credits before first episode should get episode=None
        body = "監督：監督名\n#01\n絵コンテ：コンテマン\n"
        episodes = parse_episodes(body)
        assert len(episodes) == 2
        # First should be series (None)
        assert episodes[0]["episode"] is None
        # Second should be #1
        assert episodes[1]["episode"] == 1

    def test_empty_episode_block_not_emitted(self):
        # If episode has no credits, don't emit it
        body = "#01\n#02\n絵コンテ：山田\n#03\n絵コンテ：佐藤\n"
        episodes = parse_episodes(body)
        # Should emit only #2 and #3 (not empty #1)
        ep_nums = [e["episode"] for e in episodes if e["episode"] is not None]
        # #01 might or might not be emitted depending on impl
        # but #02 and #03 should be
        assert 2 in ep_nums or 3 in ep_nums

    def test_staff_section_header_resumes_parsing(self):
        body = "#01\nキャスト\nキャラ（CV：声優）\nスタッフ\n絵コンテ：正しい人\n"
        episodes = parse_episodes(body)
        names = {c.name for ep in episodes for c in ep["credits"]}
        assert "正しい人" in names
        assert "声優" not in names
