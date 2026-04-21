"""tests/test_cooccurrence_groups.py — 共同制作集団分析のユニットテスト."""

import pytest

from src.analysis.cooccurrence_groups import (
    COOCCURRENCE_ROLES,
    compute_cooccurrence_groups,
)
from src.models import BronzeAnime as Anime, Credit, Role


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _anime(aid: str, year: int) -> Anime:
    return Anime(id=aid, title_en=f"Anime {aid}", year=year)


def _credit(pid: str, aid: str, role: Role) -> Credit:
    return Credit(person_id=pid, anime_id=aid, role=role)


def _make_basic_data():
    """3人 × 3作品の基本テストデータ."""
    anime_map = {
        "a1": _anime("a1", 2015),
        "a2": _anime("a2", 2017),
        "a3": _anime("a3", 2020),
    }
    credits = [
        # p1 = director, p2 = char designer, p3 = art director — 全3作品
        _credit("p1", "a1", Role.DIRECTOR),
        _credit("p2", "a1", Role.CHARACTER_DESIGNER),
        _credit("p3", "a1", Role.BACKGROUND_ART),
        _credit("p1", "a2", Role.DIRECTOR),
        _credit("p2", "a2", Role.CHARACTER_DESIGNER),
        _credit("p3", "a2", Role.BACKGROUND_ART),
        _credit("p1", "a3", Role.DIRECTOR),
        _credit("p2", "a3", Role.CHARACTER_DESIGNER),
        _credit("p3", "a3", Role.BACKGROUND_ART),
    ]
    return anime_map, credits


# ---------------------------------------------------------------------------
# COOCCURRENCE_ROLES の確認
# ---------------------------------------------------------------------------


class TestCooccurrenceRoles:
    def test_director_included(self):
        assert Role.DIRECTOR in COOCCURRENCE_ROLES

    def test_key_animator_excluded(self):
        assert Role.KEY_ANIMATOR not in COOCCURRENCE_ROLES

    def test_episode_director_excluded(self):
        assert Role.EPISODE_DIRECTOR not in COOCCURRENCE_ROLES

    def test_in_between_excluded(self):
        assert Role.IN_BETWEEN not in COOCCURRENCE_ROLES

    def test_cooccurrence_roles_count(self):
        assert len(COOCCURRENCE_ROLES) == 9


# ---------------------------------------------------------------------------
# 基本動作確認
# ---------------------------------------------------------------------------


class TestBasicDetection:
    def test_detects_three_person_group(self):
        anime_map, credits = _make_basic_data()
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=3)
        groups = result["groups"]
        assert len(groups) >= 1

    def test_group_members_correct(self):
        anime_map, credits = _make_basic_data()
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=3)
        member_sets = [frozenset(g["members"]) for g in result["groups"]]
        assert frozenset({"p1", "p2", "p3"}) in member_sets

    def test_shared_works_count(self):
        anime_map, credits = _make_basic_data()
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=3)
        group = next(
            g
            for g in result["groups"]
            if frozenset(g["members"]) == frozenset({"p1", "p2", "p3"})
        )
        assert group["shared_works"] == 3

    def test_shared_anime_listed(self):
        anime_map, credits = _make_basic_data()
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=3)
        group = next(
            g
            for g in result["groups"]
            if frozenset(g["members"]) == frozenset({"p1", "p2", "p3"})
        )
        assert set(group["shared_anime"]) == {"a1", "a2", "a3"}

    def test_roles_populated(self):
        anime_map, credits = _make_basic_data()
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=3)
        group = next(
            g
            for g in result["groups"]
            if frozenset(g["members"]) == frozenset({"p1", "p2", "p3"})
        )
        assert "director" in group["roles"]["p1"]
        assert "character_designer" in group["roles"]["p2"]

    def test_size_field(self):
        anime_map, credits = _make_basic_data()
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=3)
        for g in result["groups"]:
            assert g["size"] == len(g["members"])


# ---------------------------------------------------------------------------
# min_shared_works しきい値フィルタ
# ---------------------------------------------------------------------------


class TestThresholdFilter:
    def test_below_threshold_not_detected(self):
        """2作品の共同制作はデフォルト (min=3) でヒットしない."""
        anime_map = {
            "a1": _anime("a1", 2020),
            "a2": _anime("a2", 2021),
        }
        credits = [
            _credit("p1", "a1", Role.DIRECTOR),
            _credit("p2", "a1", Role.CHARACTER_DESIGNER),
            _credit("p3", "a1", Role.BACKGROUND_ART),
            _credit("p1", "a2", Role.DIRECTOR),
            _credit("p2", "a2", Role.CHARACTER_DESIGNER),
            _credit("p3", "a2", Role.BACKGROUND_ART),
        ]
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=3)
        # 2作品しかないのでグループは検出されない
        assert result["groups"] == []

    def test_threshold_two_detects(self):
        """min_shared_works=2 ならば2作品でも検出される."""
        anime_map = {
            "a1": _anime("a1", 2020),
            "a2": _anime("a2", 2021),
        }
        credits = [
            _credit("p1", "a1", Role.DIRECTOR),
            _credit("p2", "a1", Role.CHARACTER_DESIGNER),
            _credit("p3", "a1", Role.BACKGROUND_ART),
            _credit("p1", "a2", Role.DIRECTOR),
            _credit("p2", "a2", Role.CHARACTER_DESIGNER),
            _credit("p3", "a2", Role.BACKGROUND_ART),
        ]
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=2)
        assert len(result["groups"]) >= 1

    def test_non_core_roles_ignored(self):
        """KEY_ANIMATOR のみのクレジットはフィルタされる."""
        anime_map = {f"a{i}": _anime(f"a{i}", 2020 + i) for i in range(1, 4)}
        credits = (
            [
                # コアロール以外のみ
                _credit("p1", aid, Role.KEY_ANIMATOR)
                for aid in ("a1", "a2", "a3")
            ]
            + [_credit("p2", aid, Role.KEY_ANIMATOR) for aid in ("a1", "a2", "a3")]
            + [_credit("p3", aid, Role.KEY_ANIMATOR) for aid in ("a1", "a2", "a3")]
        )
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=3)
        assert result["groups"] == []


# ---------------------------------------------------------------------------
# is_active フラグ
# ---------------------------------------------------------------------------


class TestIsActiveFlag:
    def test_is_active_recent_year(self):
        """直近年（2022以降）に活動があれば is_active=True."""
        anime_map = {
            "a1": _anime("a1", 2020),
            "a2": _anime("a2", 2022),
            "a3": _anime("a3", 2023),
        }
        credits = [
            _credit("p1", "a1", Role.DIRECTOR),
            _credit("p2", "a1", Role.CHARACTER_DESIGNER),
            _credit("p3", "a1", Role.BACKGROUND_ART),
            _credit("p1", "a2", Role.DIRECTOR),
            _credit("p2", "a2", Role.CHARACTER_DESIGNER),
            _credit("p3", "a2", Role.BACKGROUND_ART),
            _credit("p1", "a3", Role.DIRECTOR),
            _credit("p2", "a3", Role.CHARACTER_DESIGNER),
            _credit("p3", "a3", Role.BACKGROUND_ART),
        ]
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=3)
        group = next(
            g
            for g in result["groups"]
            if frozenset(g["members"]) == frozenset({"p1", "p2", "p3"})
        )
        assert group["is_active"] is True

    def test_is_active_old_group(self):
        """古いグループ（last_year < 2022）は is_active=False."""
        anime_map = {
            "a1": _anime("a1", 2010),
            "a2": _anime("a2", 2012),
            "a3": _anime("a3", 2015),
        }
        credits = [
            _credit("p1", "a1", Role.DIRECTOR),
            _credit("p2", "a1", Role.CHARACTER_DESIGNER),
            _credit("p3", "a1", Role.BACKGROUND_ART),
            _credit("p1", "a2", Role.DIRECTOR),
            _credit("p2", "a2", Role.CHARACTER_DESIGNER),
            _credit("p3", "a2", Role.BACKGROUND_ART),
            _credit("p1", "a3", Role.DIRECTOR),
            _credit("p2", "a3", Role.CHARACTER_DESIGNER),
            _credit("p3", "a3", Role.BACKGROUND_ART),
        ]
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=3)
        group = next(
            g
            for g in result["groups"]
            if frozenset(g["members"]) == frozenset({"p1", "p2", "p3"})
        )
        assert group["is_active"] is False


# ---------------------------------------------------------------------------
# temporal_slices の構造
# ---------------------------------------------------------------------------


class TestTemporalSlices:
    def test_slice_keys_present(self):
        anime_map, credits = _make_basic_data()
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=3)
        slices = result["temporal_slices"]
        assert isinstance(slices, list)
        assert len(slices) > 0
        for s in slices:
            assert "period" in s
            assert "active_group_count" in s
            assert "top_groups" in s

    def test_slice_periods_cover_expected_years(self):
        anime_map, credits = _make_basic_data()
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=3)
        periods = [s["period"] for s in result["temporal_slices"]]
        # 2015-2019 と 2020- のスライスが含まれているか
        assert any("2015" in p for p in periods)
        assert any("2020" in p for p in periods)

    def test_group_active_in_correct_period(self):
        """2015・2017・2020年のグループは 2015-2019 と 2020- で active."""
        anime_map, credits = _make_basic_data()
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=3)
        slices = {s["period"]: s for s in result["temporal_slices"]}
        assert slices["2015-2019"]["active_group_count"] >= 1
        assert slices["2020-"]["active_group_count"] >= 1


# ---------------------------------------------------------------------------
# 4人グループの検出
# ---------------------------------------------------------------------------


class TestFourPersonGroup:
    def test_four_person_group_detected(self):
        anime_map = {f"a{i}": _anime(f"a{i}", 2018 + i) for i in range(1, 4)}
        credits = (
            [_credit("p1", aid, Role.DIRECTOR) for aid in ("a1", "a2", "a3")]
            + [
                _credit("p2", aid, Role.CHARACTER_DESIGNER)
                for aid in ("a1", "a2", "a3")
            ]
            + [_credit("p3", aid, Role.BACKGROUND_ART) for aid in ("a1", "a2", "a3")]
            + [_credit("p4", aid, Role.FINISHING) for aid in ("a1", "a2", "a3")]
        )
        result = compute_cooccurrence_groups(
            credits, anime_map, min_shared_works=3, max_group_size=4
        )
        sizes = [g["size"] for g in result["groups"]]
        assert 4 in sizes

    def test_max_group_size_respected(self):
        """max_group_size=3 のとき 4人グループは検出されない."""
        anime_map = {f"a{i}": _anime(f"a{i}", 2018 + i) for i in range(1, 4)}
        credits = [
            _credit(f"p{j}", aid, list(COOCCURRENCE_ROLES)[j])
            for aid in ("a1", "a2", "a3")
            for j in range(4)
        ]
        result = compute_cooccurrence_groups(
            credits, anime_map, min_shared_works=3, max_group_size=3
        )
        for g in result["groups"]:
            assert g["size"] <= 3


# ---------------------------------------------------------------------------
# サマリー構造
# ---------------------------------------------------------------------------


class TestSummary:
    def test_summary_keys(self):
        anime_map, credits = _make_basic_data()
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=3)
        summary = result["summary"]
        assert "total_groups" in summary
        assert "by_size" in summary
        assert "active_groups" in summary

    def test_summary_total_groups_matches(self):
        anime_map, credits = _make_basic_data()
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=3)
        assert result["summary"]["total_groups"] == len(result["groups"])

    def test_params_preserved(self):
        anime_map, credits = _make_basic_data()
        result = compute_cooccurrence_groups(
            credits, anime_map, min_shared_works=3, max_group_size=4
        )
        assert result["params"]["min_shared_works"] == 3
        assert result["params"]["max_group_size"] == 4


# ---------------------------------------------------------------------------
# エッジケース
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_credits(self):
        result = compute_cooccurrence_groups([], {}, min_shared_works=3)
        assert result["groups"] == []
        assert result["summary"]["total_groups"] == 0

    def test_iv_scores_optional(self):
        anime_map, credits = _make_basic_data()
        # iv_scores を渡さなくてもクラッシュしない
        result = compute_cooccurrence_groups(credits, anime_map, min_shared_works=3)
        assert result["groups"][0]["avg_iv_score"] == 0.0

    def test_iv_scores_used(self):
        anime_map, credits = _make_basic_data()
        scores = {"p1": 80.0, "p2": 60.0, "p3": 70.0}
        result = compute_cooccurrence_groups(
            credits, anime_map, iv_scores=scores, min_shared_works=3
        )
        group = next(
            g
            for g in result["groups"]
            if frozenset(g["members"]) == frozenset({"p1", "p2", "p3"})
        )
        expected = round((80.0 + 60.0 + 70.0) / 3, 1)
        assert group["avg_iv_score"] == pytest.approx(expected, abs=0.1)
