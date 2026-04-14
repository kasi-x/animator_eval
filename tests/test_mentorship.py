"""mentorship モジュールのテスト."""

from src.analysis.mentorship import (
    infer_mentorships,
    build_mentorship_tree,
    _compute_confidence,
)
from src.models import Anime, Credit, Role


def _make_mentorship_data():
    anime_map = {
        "a1": Anime(id="a1", title_en="Show 1", year=2018),
        "a2": Anime(id="a2", title_en="Show 2", year=2019),
        "a3": Anime(id="a3", title_en="Show 3", year=2020),
        "a4": Anime(id="a4", title_en="Show 4", year=2021),
    }
    # Director p1 works with in-between p2 on 4 shows
    credits = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a1", role=Role.IN_BETWEEN),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a2", role=Role.IN_BETWEEN),
        Credit(person_id="p1", anime_id="a3", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a3", role=Role.IN_BETWEEN),
        Credit(person_id="p1", anime_id="a4", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a4", role=Role.KEY_ANIMATOR),
    ]
    return credits, anime_map


class TestInferMentorships:
    def test_empty(self):
        result = infer_mentorships([], {})
        assert result == []

    def test_detects_mentorship(self):
        credits, anime_map = _make_mentorship_data()
        result = infer_mentorships(credits, anime_map, min_shared_works=3)
        assert len(result) >= 1
        mentor_ids = [m["mentor_id"] for m in result]
        assert "p1" in mentor_ids

    def test_mentee_is_correct(self):
        credits, anime_map = _make_mentorship_data()
        result = infer_mentorships(credits, anime_map, min_shared_works=3)
        p1_mentorship = next(m for m in result if m["mentor_id"] == "p1")
        assert p1_mentorship["mentee_id"] == "p2"

    def test_shared_works_count(self):
        credits, anime_map = _make_mentorship_data()
        result = infer_mentorships(credits, anime_map, min_shared_works=3)
        p1_mentorship = next(m for m in result if m["mentor_id"] == "p1")
        assert p1_mentorship["shared_works"] >= 3

    def test_min_shared_works_filter(self):
        credits, anime_map = _make_mentorship_data()
        result = infer_mentorships(credits, anime_map, min_shared_works=10)
        assert result == []

    def test_year_span(self):
        credits, anime_map = _make_mentorship_data()
        result = infer_mentorships(credits, anime_map, min_shared_works=3)
        p1_mentorship = next(m for m in result if m["mentor_id"] == "p1")
        assert p1_mentorship["year_span"] is not None
        assert p1_mentorship["year_span"][0] <= p1_mentorship["year_span"][1]

    def test_confidence_present(self):
        credits, anime_map = _make_mentorship_data()
        result = infer_mentorships(credits, anime_map, min_shared_works=3)
        for m in result:
            assert 0 <= m["confidence"] <= 100

    def test_sorted_by_confidence(self):
        credits, anime_map = _make_mentorship_data()
        result = infer_mentorships(credits, anime_map, min_shared_works=3)
        if len(result) > 1:
            for i in range(len(result) - 1):
                assert result[i]["confidence"] >= result[i + 1]["confidence"]

    def test_same_stage_no_mentorship(self):
        """同じステージの人同士はメンターシップにならない."""
        anime_map = {"a1": Anime(id="a1", title_en="Show", year=2020)}
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR),
            Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        ]
        result = infer_mentorships(credits, anime_map, min_shared_works=1)
        assert result == []


class TestBuildMentorshipTree:
    def test_empty(self):
        result = build_mentorship_tree([])
        assert result["tree"] == {}
        assert result["roots"] == []

    def test_simple_tree(self):
        mentorships = [
            {"mentor_id": "p1", "mentee_id": "p2"},
            {"mentor_id": "p1", "mentee_id": "p3"},
            {"mentor_id": "p2", "mentee_id": "p4"},
        ]
        result = build_mentorship_tree(mentorships)
        assert "p1" in result["roots"]
        assert "p2" not in result["roots"]  # p2 is a mentee
        assert "p4" in result["tree"]["p2"]


class TestComputeConfidence:
    def test_low_values(self):
        assert _compute_confidence(1, 1, 1) > 0

    def test_high_values(self):
        assert _compute_confidence(10, 5, 10) == 100

    def test_bounded(self):
        assert _compute_confidence(100, 100, 100) == 100

    def test_confidence_span_vs_count(self):
        """Same number of years but different spans yield different scores.

        B16: year_span measures max(years)-min(years), not len(years).
        3 years spanning 2018-2020 (span=3) should score higher than
        3 years spanning 2020-2020 (span=1) for the time component.
        """
        # 3 shared works, stage_gap=2 in both cases
        # Only difference is year_span
        score_wide_span = _compute_confidence(shared_works=3, stage_gap=2, year_span=3)
        score_narrow_span = _compute_confidence(
            shared_works=3, stage_gap=2, year_span=1
        )
        assert score_wide_span > score_narrow_span
