"""team_composition モジュールのテスト."""

from src.analysis.team_composition import analyze_team_patterns
from src.models import Anime, Credit, Role

# Roles used to populate large teams
_FILLER_ROLES = [
    Role.KEY_ANIMATOR,
    Role.IN_BETWEEN,
    Role.EPISODE_DIRECTOR,
    Role.EPISODE_DIRECTOR,
    Role.CHARACTER_DESIGNER,
    Role.BACKGROUND_ART,
    Role.FINISHING,
    Role.PHOTOGRAPHY_DIRECTOR,
    Role.PHOTOGRAPHY_DIRECTOR,
    Role.PRODUCER,
    Role.SOUND_DIRECTOR,
    Role.MUSIC,
    Role.BACKGROUND_ART,
]


def _make_data():
    """Build test data where a1 (18 staff) and a2 (16 staff) pass min_staff=15,
    while a3 (2 staff) and a4 (1 staff) do not."""
    anime_map = {
        "a1": Anime(id="a1", title_en="Hit Show", year=2020, score=8.5),
        "a2": Anime(id="a2", title_en="Another Hit", year=2022, score=8.0),
        "a3": Anime(id="a3", title_en="Average Show", year=2021, score=6.0),
        "a4": Anime(id="a4", title_en="No Score", year=2019),
    }
    credits = [
        # Large team (a1): p1 director, p2 key, p3 anim_dir + 15 filler → 18 unique
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p3", anime_id="a1", role=Role.ANIMATION_DIRECTOR),
    ]
    # Add 15 more unique staff to a1 (total 18)
    for i, role in enumerate(_FILLER_ROLES):
        credits.append(Credit(person_id=f"a1_extra_{i}", anime_id="a1", role=role))
    credits.append(Credit(person_id="a1_extra_13", anime_id="a1", role=Role.LAYOUT))
    credits.append(
        Credit(person_id="a1_extra_14", anime_id="a1", role=Role.CGI_DIRECTOR)
    )

    # Large team (a2): p1 director, p2 key + 14 filler → 16 unique
    credits.append(Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR))
    credits.append(Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR))
    for i, role in enumerate(_FILLER_ROLES):
        credits.append(Credit(person_id=f"a2_extra_{i}", anime_id="a2", role=role))
    credits.append(Credit(person_id="a2_extra_13", anime_id="a2", role=Role.LAYOUT))

    # Small team (a3): only 2 staff — below min_staff
    credits.extend(
        [
            Credit(person_id="p4", anime_id="a3", role=Role.DIRECTOR),
            Credit(person_id="p5", anime_id="a3", role=Role.KEY_ANIMATOR),
        ]
    )

    # Tiny team (a4): only 1 staff — below min_staff
    credits.append(Credit(person_id="p1", anime_id="a4", role=Role.DIRECTOR))

    return credits, anime_map


class TestAnalyzeTeamPatterns:
    def test_finds_high_score_teams(self):
        credits, anime_map = _make_data()
        result = analyze_team_patterns(credits, anime_map, min_staff=15)
        assert result["total_high_score"] == 2  # a1 (18 staff) and a2 (16 staff)

    def test_team_size(self):
        credits, anime_map = _make_data()
        result = analyze_team_patterns(credits, anime_map, min_staff=15)
        teams = result["high_score_teams"]
        a1_team = next(t for t in teams if t["anime_id"] == "a1")
        assert a1_team["team_size"] == 18  # p1, p2, p3 + 15 filler

    def test_role_combinations(self):
        credits, anime_map = _make_data()
        result = analyze_team_patterns(credits, anime_map, min_staff=15)
        assert len(result["role_combinations"]) > 0

    def test_recommended_pairs(self):
        credits, anime_map = _make_data()
        result = analyze_team_patterns(credits, anime_map, min_staff=15)
        # p1-p2 appear in both large teams (a1 and a2)
        pairs = result["recommended_pairs"]
        if pairs:
            pair_pids = [(p["person_a"], p["person_b"]) for p in pairs]
            assert ("p1", "p2") in pair_pids

    def test_team_size_stats(self):
        credits, anime_map = _make_data()
        result = analyze_team_patterns(credits, anime_map)
        stats = result["team_size_stats"]
        assert "avg" in stats
        assert "min" in stats
        assert "max" in stats

    def test_with_person_scores(self):
        credits, anime_map = _make_data()
        scores = {"p1": 80.0, "p2": 60.0, "p3": 50.0}
        result = analyze_team_patterns(
            credits, anime_map, person_scores=scores, min_staff=15
        )
        teams = result["high_score_teams"]
        a1_team = next(t for t in teams if t["anime_id"] == "a1")
        assert "avg_person_score" in a1_team

    def test_empty(self):
        result = analyze_team_patterns([], {})
        assert result["total_high_score"] == 0

    def test_min_staff_filter(self):
        credits, anime_map = _make_data()
        # a1 has 18 staff, a2 has 16 — only a1 passes min_staff=17
        result = analyze_team_patterns(credits, anime_map, min_staff=17)
        assert result["total_high_score"] == 1  # Only a1

    def test_sorted_by_score(self):
        credits, anime_map = _make_data()
        result = analyze_team_patterns(credits, anime_map, min_staff=15)
        teams = result["high_score_teams"]
        for i in range(len(teams) - 1):
            assert (teams[i].get("anime_score") or 0) >= (
                teams[i + 1].get("anime_score") or 0
            )
