"""Tests for seesaawiki_scraper parse functions (no network calls)."""
from __future__ import annotations

from src.scrapers.seesaawiki_scraper import (
    ParsedCredit,
    parse_credit_line,
    parse_episodes,
    parse_series_staff,
    _parse_episode_ranges,
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
