"""recommendation モジュールのテスト."""

from src.analysis.recommendation import recommend_for_team
from src.models import Credit, Role


def _make_data():
    results = [
        {"person_id": "p1", "name": "Director", "authority": 80, "trust": 70, "skill": 60, "composite": 71},
        {"person_id": "p2", "name": "Animator A", "authority": 50, "trust": 90, "skill": 40, "composite": 58},
        {"person_id": "p3", "name": "Animator B", "authority": 30, "trust": 40, "skill": 80, "composite": 47},
        {"person_id": "p4", "name": "Designer", "authority": 60, "trust": 60, "skill": 70, "composite": 63},
        {"person_id": "p5", "name": "Newcomer", "authority": 10, "trust": 10, "skill": 20, "composite": 13},
    ]
    credits = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p3", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p4", anime_id="a1", role=Role.CHARACTER_DESIGNER),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="p4", anime_id="a2", role=Role.CHARACTER_DESIGNER),
    ]
    return results, credits


class TestRecommendForTeam:
    def test_recommends_candidates(self):
        results, credits = _make_data()
        recs = recommend_for_team(["p1"], results, credits, top_n=5)
        assert len(recs) >= 1
        # Should not include team member
        assert all(r["person_id"] != "p1" for r in recs)

    def test_sorted_by_compatibility(self):
        results, credits = _make_data()
        recs = recommend_for_team(["p1"], results, credits)
        for i in range(len(recs) - 1):
            assert recs[i]["compatibility_score"] >= recs[i + 1]["compatibility_score"]

    def test_collaboration_bonus(self):
        results, credits = _make_data()
        recs = recommend_for_team(["p1"], results, credits)
        # p4 worked with p1 on a1 and a2 (shared_projects = 2)
        p4_rec = next((r for r in recs if r["person_id"] == "p4"), None)
        assert p4_rec is not None
        assert p4_rec["shared_projects"] == 2

    def test_empty_team(self):
        results, credits = _make_data()
        recs = recommend_for_team([], results, credits)
        assert recs == []

    def test_empty_results(self):
        _, credits = _make_data()
        recs = recommend_for_team(["p1"], [], credits)
        assert recs == []

    def test_top_n(self):
        results, credits = _make_data()
        recs = recommend_for_team(["p1"], results, credits, top_n=2)
        assert len(recs) <= 2

    def test_has_reasons(self):
        results, credits = _make_data()
        recs = recommend_for_team(["p1"], results, credits)
        # At least some candidates should have reasons
        with_reasons = [r for r in recs if r["reasons"]]
        assert len(with_reasons) >= 1
