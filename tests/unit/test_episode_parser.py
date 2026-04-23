"""Tests for episode parser utility."""

from src.utils.episode_parser import parse_episodes


class TestParseEpisodes:
    def test_single_episode(self):
        assert parse_episodes("Key Animation (ep 10)") == {10}

    def test_single_episode_dot_format(self):
        assert parse_episodes("Key Animation (ep. 2)") == {2}

    def test_episode_list(self):
        assert parse_episodes("Episode Director (eps 2, 18, 22)") == {2, 18, 22}

    def test_episode_range(self):
        result = parse_episodes("Key Animation (eps 1-12)")
        assert result == set(range(1, 13))

    def test_mixed_range_and_singles(self):
        result = parse_episodes("Key Animation (eps 1-3, 7, 10)")
        assert result == {1, 2, 3, 7, 10}

    def test_skip_op_ed(self):
        result = parse_episodes("Key Animation (OP24, eps 903, 1000)")
        assert result == {903, 1000}

    def test_skip_ed(self):
        result = parse_episodes("Key Animation (ED3, ep 5)")
        assert result == {5}

    def test_no_episodes(self):
        assert parse_episodes("Director") == set()

    def test_empty_string(self):
        assert parse_episodes("") == set()

    def test_none_safe(self):
        """None input should not crash (though type hint says str)."""
        assert parse_episodes("") == set()

    def test_eps_with_spaces(self):
        assert parse_episodes("Storyboard (eps 1, 3, 5)") == {1, 3, 5}

    def test_large_episode_numbers(self):
        result = parse_episodes("Key Animation (ep 1000)")
        assert result == {1000}

    def test_range_with_en_dash(self):
        result = parse_episodes("Key Animation (eps 1\u20133)")
        assert result == {1, 2, 3}

    def test_no_false_positives_on_parenthetical_without_ep(self):
        """Parentheticals without 'ep' should not be parsed."""
        assert parse_episodes("Animation Director (Assistant)") == set()

    def test_case_insensitive(self):
        assert parse_episodes("Key Animation (EP 5)") == {5}
        assert parse_episodes("Key Animation (Ep 5)") == {5}

    def test_multiple_parentheticals(self):
        """Only the ep-containing parenthetical should be parsed."""
        result = parse_episodes("Key Animation (Chief) (ep 3)")
        assert result == {3}

    def test_ops_only_returns_empty(self):
        """If only OP/ED tokens exist, return empty."""
        assert parse_episodes("Key Animation (OP1, ED2)") == set()
