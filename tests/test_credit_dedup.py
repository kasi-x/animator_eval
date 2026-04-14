"""クレジット重複マージのテスト."""

from src.models import Credit, Role
from src.pipeline_phases.entity_resolution import _merge_duplicate_credits


class TestMergeDuplicateCredits:
    def test_merge_same_person_anime_role(self):
        """Same (person, anime, role) from two sources should merge to one."""
        credits = [
            Credit(
                person_id="p1",
                anime_id="a1",
                role=Role.KEY_ANIMATOR,
                raw_role="Key Animation",
                source="anilist",
            ),
            Credit(
                person_id="p1",
                anime_id="a1",
                role=Role.KEY_ANIMATOR,
                raw_role="原画",
                source="madb",
            ),
        ]
        merged = _merge_duplicate_credits(credits)
        assert len(merged) == 1
        assert merged[0].person_id == "p1"
        assert merged[0].anime_id == "a1"
        assert merged[0].role == Role.KEY_ANIMATOR

    def test_episode_info_preserved(self):
        """MADB episode info should be preserved after merge."""
        credits = [
            Credit(
                person_id="p1",
                anime_id="a1",
                role=Role.KEY_ANIMATOR,
                episode=-1,
                source="anilist",
            ),
            Credit(
                person_id="p1",
                anime_id="a1",
                role=Role.KEY_ANIMATOR,
                episode=18,
                source="madb",
            ),
        ]
        merged = _merge_duplicate_credits(credits)
        # Should keep the specific episode, not the unknown one
        assert len(merged) == 1
        assert merged[0].episode == 18

    def test_unknown_episode_replaced(self):
        """episode=-1 should be replaced by specific episode numbers."""
        credits = [
            Credit(
                person_id="p1",
                anime_id="a1",
                role=Role.KEY_ANIMATOR,
                episode=-1,
                source="anilist",
            ),
            Credit(
                person_id="p1",
                anime_id="a1",
                role=Role.KEY_ANIMATOR,
                episode=5,
                source="madb",
            ),
            Credit(
                person_id="p1",
                anime_id="a1",
                role=Role.KEY_ANIMATOR,
                episode=10,
                source="madb",
            ),
        ]
        merged = _merge_duplicate_credits(credits)
        # Two specific episodes should be preserved
        assert len(merged) == 2
        episodes = sorted([c.episode for c in merged])
        assert episodes == [5, 10]

    def test_different_anime_not_merged(self):
        """Credits for different anime should NOT be merged."""
        credits = [
            Credit(
                person_id="p1",
                anime_id="a1",
                role=Role.KEY_ANIMATOR,
                source="anilist",
            ),
            Credit(
                person_id="p1",
                anime_id="a2",
                role=Role.KEY_ANIMATOR,
                source="anilist",
            ),
        ]
        merged = _merge_duplicate_credits(credits)
        assert len(merged) == 2

    def test_source_provenance_kept(self):
        """Source info from both sides should be preserved."""
        credits = [
            Credit(
                person_id="p1",
                anime_id="a1",
                role=Role.KEY_ANIMATOR,
                source="anilist",
            ),
            Credit(
                person_id="p1",
                anime_id="a1",
                role=Role.KEY_ANIMATOR,
                source="madb",
            ),
        ]
        merged = _merge_duplicate_credits(credits)
        assert len(merged) == 1
        # Source should contain both
        assert "anilist" in merged[0].source
        assert "madb" in merged[0].source

    def test_raw_role_prefers_anilist(self):
        """AniList raw_role (English) should be preferred over MADB (Japanese)."""
        credits = [
            Credit(
                person_id="p1",
                anime_id="a1",
                role=Role.KEY_ANIMATOR,
                raw_role="Key Animation",
                source="anilist",
            ),
            Credit(
                person_id="p1",
                anime_id="a1",
                role=Role.KEY_ANIMATOR,
                raw_role="原画",
                source="madb",
            ),
        ]
        merged = _merge_duplicate_credits(credits)
        assert merged[0].raw_role == "Key Animation"

    def test_no_duplicates_passthrough(self):
        """Credits without duplicates should pass through unchanged."""
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="anilist"),
            Credit(
                person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="anilist"
            ),
            Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="anilist"),
        ]
        merged = _merge_duplicate_credits(credits)
        assert len(merged) == 3

    def test_different_roles_not_merged(self):
        """Same person+anime but different roles should NOT be merged."""
        credits = [
            Credit(
                person_id="p1",
                anime_id="a1",
                role=Role.KEY_ANIMATOR,
                source="anilist",
            ),
            Credit(
                person_id="p1",
                anime_id="a1",
                role=Role.ANIMATION_DIRECTOR,
                source="anilist",
            ),
        ]
        merged = _merge_duplicate_credits(credits)
        assert len(merged) == 2
