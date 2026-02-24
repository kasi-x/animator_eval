"""person_tags モジュールのテスト."""

from src.analysis.person_tags import compute_person_tags


def _make_results():
    return [
        {
            "person_id": "p1",
            "authority": 90,
            "trust": 85,
            "skill": 80,
            "composite": 86,
            "career": {
                "active_years": 20,
                "highest_stage": 6,
                "highest_roles": ["director"],
            },
            "growth": {"trend": "stable"},
            "network": {"hub_score": 90},
            "versatility": {"score": 80, "categories": 5},
        },
        {
            "person_id": "p2",
            "authority": 60,
            "trust": 50,
            "skill": 40,
            "composite": 52,
            "career": {
                "active_years": 2,
                "highest_stage": 3,
                "highest_roles": ["key_animator"],
            },
            "growth": {"trend": "rising"},
            "network": {"hub_score": 30},
            "versatility": {"score": 20, "categories": 1},
        },
        {
            "person_id": "p3",
            "authority": 40,
            "trust": 30,
            "skill": 20,
            "composite": 32,
            "career": {"active_years": 8, "highest_stage": 4},
            "growth": {"trend": "inactive"},
            "network": {"hub_score": 10},
            "versatility": {"score": 50, "categories": 3},
        },
        # Need at least 3 entries for percentile calculation
        *[
            {
                "person_id": f"px{i}",
                "authority": 30 + i,
                "trust": 30 + i,
                "skill": 30 + i,
                "composite": 30 + i,
                "career": {"active_years": 5, "highest_stage": 3},
            }
            for i in range(7)
        ],
    ]


class TestComputePersonTags:
    def test_veteran_tag(self):
        results = _make_results()
        tags = compute_person_tags(results)
        assert "veteran" in tags["p1"]

    def test_newcomer_tag(self):
        results = _make_results()
        tags = compute_person_tags(results)
        assert "newcomer" in tags["p2"]

    def test_rising_star_tag(self):
        results = _make_results()
        tags = compute_person_tags(results)
        assert "rising_star" in tags["p2"]

    def test_hub_tag(self):
        results = _make_results()
        tags = compute_person_tags(results)
        assert "hub" in tags["p1"]

    def test_generalist_tag(self):
        results = _make_results()
        tags = compute_person_tags(results)
        assert "generalist" in tags["p1"]

    def test_specialist_tag(self):
        results = _make_results()
        tags = compute_person_tags(results)
        assert "specialist" in tags["p2"]

    def test_inactive_tag(self):
        results = _make_results()
        tags = compute_person_tags(results)
        assert "inactive" in tags["p3"]

    def test_director_class_tag(self):
        results = _make_results()
        tags = compute_person_tags(results)
        assert "director_class" in tags["p1"]

    def test_high_score_tags(self):
        results = _make_results()
        tags = compute_person_tags(results)
        # p1 should have high_authority, high_trust, high_skill
        assert "high_authority" in tags["p1"]
        assert "high_trust" in tags["p1"]

    def test_empty(self):
        tags = compute_person_tags([])
        assert tags == {}

    def test_returns_all_persons(self):
        results = _make_results()
        tags = compute_person_tags(results)
        for r in results:
            assert r["person_id"] in tags
