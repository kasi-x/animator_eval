"""Direct tests for patronage_dormancy.py — exponential decay, grace period, patronage (T02)."""

import math

import pytest

from src.models import AnimeAnalysis as Anime, Credit, Role


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _credit(person_id: str, anime_id: str, role: str = "key_animator") -> Credit:
    return Credit(person_id=person_id, anime_id=anime_id, role=Role(role),
                  raw_role=role)


def _anime(anime_id: str, year: int | None, episodes: int = 12) -> Anime:
    return Anime(id=anime_id, title_en=f"Anime {anime_id}", year=year,
                 episodes=episodes)


# ---------------------------------------------------------------------------
# compute_dormancy_penalty tests
# ---------------------------------------------------------------------------

class TestDormancyPenalty:
    def test_within_grace_period_no_penalty(self):
        """Active 1.5 fractional-years ago, grace_period=2 → D = 1.0.

        anime.year=2024, current_year=2025 → gap ≈ 1.5 yrs (Q2 fallback) < grace_period.
        """
        from src.analysis.scoring.patronage_dormancy import compute_dormancy_penalty
        credits = [_credit("p1", "a1")]
        anime_map = {"a1": _anime("a1", year=2024)}
        result = compute_dormancy_penalty(credits, anime_map, current_year=2025,
                                          grace_period=2.0)
        assert result["p1"] == pytest.approx(1.0)

    def test_gap_exceeds_grace_period_decay_applied(self):
        """anime.year=2020, current_year=2025 → gap ≈ 5.5, effective ≈ 3.5, D < 1."""
        from src.analysis.scoring.patronage_dormancy import compute_dormancy_penalty
        credits = [_credit("p1", "a1")]
        anime_map = {"a1": _anime("a1", year=2020)}
        result = compute_dormancy_penalty(credits, anime_map, current_year=2025,
                                          decay_rate=0.5, grace_period=2.0)
        assert "p1" in result
        assert result["p1"] < 1.0
        assert result["p1"] > 0.0

    def test_decay_formula_matches_expected(self):
        """Verify D = exp(-rate × effective_gap) matches implementation.

        With Q2 fallback: last_f = year + 0.25, current_ref = current_year + 0.75.
        gap = (2025+0.75) - (2020+0.25) = 5.5
        effective = 5.5 - 2.0 = 3.5 → D = exp(-0.5 * 3.5) ≈ 0.174
        """
        from src.analysis.scoring.patronage_dormancy import compute_dormancy_penalty
        credits = [_credit("p1", "a1")]
        anime_map = {"a1": _anime("a1", year=2020)}
        result = compute_dormancy_penalty(credits, anime_map, current_year=2025,
                                          decay_rate=0.5, grace_period=2.0)
        expected = math.exp(-0.5 * 3.5)
        assert result["p1"] == pytest.approx(expected, rel=0.01)

    def test_higher_decay_rate_lowers_d(self):
        """Monotonicity: higher decay_rate → lower D for same gap."""
        from src.analysis.scoring.patronage_dormancy import compute_dormancy_penalty
        credits = [_credit("p1", "a1")]
        anime_map = {"a1": _anime("a1", year=2015)}
        d_low = compute_dormancy_penalty(credits, anime_map, current_year=2025,
                                         decay_rate=0.1)
        d_high = compute_dormancy_penalty(credits, anime_map, current_year=2025,
                                          decay_rate=1.0)
        assert d_low["p1"] > d_high["p1"]

    def test_uses_most_recent_credit(self):
        """Person with credits in 2010 and 2024 → dormancy uses 2024 (within grace).

        anime.year=2024: gap = 2025.75 - 2024.25 = 1.5 < grace_period=2.0 → D=1.0.
        Without the 2024 credit, the 2010 credit would give gap ≈ 15.5 → D ≈ 0.
        """
        from src.analysis.scoring.patronage_dormancy import compute_dormancy_penalty
        credits = [_credit("p1", "a1"), _credit("p1", "a2")]
        anime_map = {
            "a1": _anime("a1", year=2010),
            "a2": _anime("a2", year=2024),
        }
        result = compute_dormancy_penalty(credits, anime_map, current_year=2025,
                                          grace_period=2.0)
        assert result["p1"] == pytest.approx(1.0)

    def test_person_with_no_valid_year_excluded(self):
        """Credit on anime with year=None → person not in result."""
        from src.analysis.scoring.patronage_dormancy import compute_dormancy_penalty
        credits = [_credit("p1", "a1")]
        anime_map = {"a1": _anime("a1", year=None)}
        result = compute_dormancy_penalty(credits, anime_map, current_year=2025)
        assert "p1" not in result

    def test_empty_inputs_return_empty(self):
        from src.analysis.scoring.patronage_dormancy import compute_dormancy_penalty
        assert compute_dormancy_penalty([], {}, current_year=2025) == {}


# ---------------------------------------------------------------------------
# compute_patronage_premium tests
# ---------------------------------------------------------------------------

class TestPatronagePremium:
    def test_empty_inputs_return_empty(self):
        from src.analysis.scoring.patronage_dormancy import compute_patronage_premium
        assert compute_patronage_premium([], {}, {}) == {}

    def test_no_director_birank_yields_zero(self):
        """Without BiRank for director, patronage formula = 0."""
        from src.analysis.scoring.patronage_dormancy import compute_patronage_premium
        anime_map = {"a1": _anime("a1", year=2020)}
        credits = [
            _credit("dir1", "a1", "director"),
            _credit("p1", "a1", "key_animator"),
        ]
        result = compute_patronage_premium(credits, anime_map, director_birank_scores={})
        # No BiRank → PR_d = 0 → Π_i = 0
        assert result.get("p1", 0.0) == pytest.approx(0.0)

    def test_higher_director_birank_yields_more_patronage(self):
        """Person who worked with a high-BiRank director gets more patronage."""
        from src.analysis.scoring.patronage_dormancy import compute_patronage_premium
        anime_map = {"a1": _anime("a1", year=2020), "a2": _anime("a2", year=2020)}
        credits = [
            _credit("dir_high", "a1", "director"),
            _credit("dir_low", "a2", "director"),
            _credit("p_high", "a1", "key_animator"),
            _credit("p_low", "a2", "key_animator"),
        ]
        birank = {"dir_high": 1.0, "dir_low": 0.1}
        result = compute_patronage_premium(credits, anime_map, birank)
        assert result.get("p_high", 0) > result.get("p_low", 0)

    def test_repeat_collaborations_increase_patronage(self):
        """Π_i = Σ PR_d × log(1+N_id) — more collabs with same director → higher score."""
        from src.analysis.scoring.patronage_dormancy import compute_patronage_premium
        n = 4
        anime_map = {f"a{i}": _anime(f"a{i}", year=2020 + i) for i in range(n)}
        credits_repeat = [_credit("dir1", f"a{i}", "director") for i in range(n)]
        credits_repeat += [_credit("p_repeat", f"a{i}", "key_animator") for i in range(n)]

        credits_single = [_credit("dir1", "a0", "director"), _credit("p_single", "a0", "key_animator")]

        birank = {"dir1": 1.0}

        repeat_anime = {f"a{i}": _anime(f"a{i}", year=2020 + i) for i in range(n)}
        single_anime = {"a0": _anime("a0", year=2020)}

        r_repeat = compute_patronage_premium(credits_repeat, repeat_anime, birank)
        r_single = compute_patronage_premium(credits_single, single_anime, birank)

        assert r_repeat.get("p_repeat", 0) > r_single.get("p_single", 0)


# ---------------------------------------------------------------------------
# compute_patronage_and_dormancy integration test
# ---------------------------------------------------------------------------

class TestPatronageAndDormancy:
    def test_returns_result_with_both_components(self):
        from src.analysis.scoring.patronage_dormancy import compute_patronage_and_dormancy
        anime_map = {"a1": _anime("a1", year=2023)}
        credits = [
            _credit("dir1", "a1", "director"),
            _credit("p1", "a1", "key_animator"),
        ]
        result = compute_patronage_and_dormancy(
            credits, anime_map, director_birank_scores={"dir1": 0.8}, current_year=2025
        )
        assert hasattr(result, "patronage_premium")
        assert hasattr(result, "dormancy_penalty")
        assert "p1" in result.dormancy_penalty
