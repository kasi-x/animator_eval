"""Tests for centralized role grouping module."""

from src.models import Role
from src.utils.role_groups import (
    ANIMATOR_ROLES,
    CORE_TEAM_ROLES,
    DIRECTOR_ROLES,
    MENTEE_ROLES,
    ROLE_CATEGORY,
    SKILL_EVALUATED_ROLES,
    get_role_category,
    is_animator_role,
    is_core_team_role,
    is_director_role,
    is_mentee_role,
    is_skill_evaluated_role,
)


class TestRoleGroups:
    """Test role group constants."""

    def test_director_roles_contains_expected(self):
        """Director roles should contain all director-level positions."""
        assert Role.DIRECTOR in DIRECTOR_ROLES
        assert Role.EPISODE_DIRECTOR in DIRECTOR_ROLES
        assert Role.CHIEF_ANIMATION_DIRECTOR in DIRECTOR_ROLES
        assert len(DIRECTOR_ROLES) == 3

    def test_animator_roles_contains_expected(self):
        """Animator roles should contain all animator positions."""
        assert Role.ANIMATION_DIRECTOR in ANIMATOR_ROLES
        assert Role.KEY_ANIMATOR in ANIMATOR_ROLES
        assert Role.SECOND_KEY_ANIMATOR in ANIMATOR_ROLES
        assert Role.IN_BETWEEN in ANIMATOR_ROLES
        assert Role.CHARACTER_DESIGNER in ANIMATOR_ROLES
        assert Role.STORYBOARD in ANIMATOR_ROLES
        assert Role.LAYOUT in ANIMATOR_ROLES
        assert Role.EFFECTS in ANIMATOR_ROLES
        assert len(ANIMATOR_ROLES) == 8

    def test_mentee_roles_contains_expected(self):
        """Mentee roles should contain junior/entry-level positions."""
        assert Role.IN_BETWEEN in MENTEE_ROLES
        assert Role.SECOND_KEY_ANIMATOR in MENTEE_ROLES
        assert Role.KEY_ANIMATOR in MENTEE_ROLES
        assert Role.LAYOUT in MENTEE_ROLES
        assert Role.EFFECTS in MENTEE_ROLES
        assert len(MENTEE_ROLES) == 5

    def test_skill_evaluated_roles_contains_expected(self):
        """Skill-evaluated roles should include production staff."""
        assert Role.CHIEF_ANIMATION_DIRECTOR in SKILL_EVALUATED_ROLES
        assert Role.ANIMATION_DIRECTOR in SKILL_EVALUATED_ROLES
        assert Role.KEY_ANIMATOR in SKILL_EVALUATED_ROLES
        assert Role.SECOND_KEY_ANIMATOR in SKILL_EVALUATED_ROLES
        assert Role.CHARACTER_DESIGNER in SKILL_EVALUATED_ROLES
        assert Role.STORYBOARD in SKILL_EVALUATED_ROLES
        assert Role.EPISODE_DIRECTOR in SKILL_EVALUATED_ROLES
        assert Role.ART_DIRECTOR in SKILL_EVALUATED_ROLES
        assert Role.EFFECTS in SKILL_EVALUATED_ROLES
        assert Role.LAYOUT in SKILL_EVALUATED_ROLES
        assert len(SKILL_EVALUATED_ROLES) == 10

    def test_core_team_roles_contains_expected(self):
        """Core team roles should include high-value positions."""
        assert Role.DIRECTOR in CORE_TEAM_ROLES
        assert Role.CHIEF_ANIMATION_DIRECTOR in CORE_TEAM_ROLES
        assert Role.ANIMATION_DIRECTOR in CORE_TEAM_ROLES
        assert Role.CHARACTER_DESIGNER in CORE_TEAM_ROLES
        assert Role.KEY_ANIMATOR in CORE_TEAM_ROLES
        assert Role.STORYBOARD in CORE_TEAM_ROLES
        assert Role.EPISODE_DIRECTOR in CORE_TEAM_ROLES
        assert len(CORE_TEAM_ROLES) == 7

    def test_role_groups_are_frozen(self):
        """Role groups should be immutable frozensets."""
        assert isinstance(DIRECTOR_ROLES, frozenset)
        assert isinstance(ANIMATOR_ROLES, frozenset)
        assert isinstance(MENTEE_ROLES, frozenset)
        assert isinstance(SKILL_EVALUATED_ROLES, frozenset)
        assert isinstance(CORE_TEAM_ROLES, frozenset)


class TestRoleCategorization:
    """Test role categorization mapping."""

    def test_direction_category(self):
        """Direction roles should map to 'direction'."""
        assert ROLE_CATEGORY[Role.DIRECTOR] == "direction"
        assert ROLE_CATEGORY[Role.EPISODE_DIRECTOR] == "direction"
        assert ROLE_CATEGORY[Role.STORYBOARD] == "direction"
        assert ROLE_CATEGORY[Role.SERIES_COMPOSITION] == "direction"

    def test_animation_supervision_category(self):
        """Animation supervision roles should have own category."""
        assert ROLE_CATEGORY[Role.CHIEF_ANIMATION_DIRECTOR] == "animation_supervision"
        assert ROLE_CATEGORY[Role.ANIMATION_DIRECTOR] == "animation_supervision"

    def test_animation_category(self):
        """Animation roles should map to 'animation'."""
        assert ROLE_CATEGORY[Role.KEY_ANIMATOR] == "animation"
        assert ROLE_CATEGORY[Role.SECOND_KEY_ANIMATOR] == "animation"
        assert ROLE_CATEGORY[Role.IN_BETWEEN] == "animation"
        assert ROLE_CATEGORY[Role.LAYOUT] == "animation"

    def test_design_category(self):
        """Design roles should map to 'design'."""
        assert ROLE_CATEGORY[Role.CHARACTER_DESIGNER] == "design"
        assert ROLE_CATEGORY[Role.MECHANICAL_DESIGNER] == "design"
        assert ROLE_CATEGORY[Role.ART_DIRECTOR] == "design"
        assert ROLE_CATEGORY[Role.COLOR_DESIGNER] == "design"

    def test_technical_category(self):
        """Technical roles should map to 'technical'."""
        assert ROLE_CATEGORY[Role.EFFECTS] == "technical"
        assert ROLE_CATEGORY[Role.CGI_DIRECTOR] == "technical"
        assert ROLE_CATEGORY[Role.PHOTOGRAPHY_DIRECTOR] == "technical"

    def test_art_category(self):
        """Art roles should map to 'art'."""
        assert ROLE_CATEGORY[Role.BACKGROUND_ART] == "art"

    def test_sound_category(self):
        """Sound roles should map to 'sound'."""
        assert ROLE_CATEGORY[Role.SOUND_DIRECTOR] == "sound"
        assert ROLE_CATEGORY[Role.MUSIC] == "sound"

    def test_writing_category(self):
        """Writing roles should map to 'writing'."""
        assert ROLE_CATEGORY[Role.SCREENPLAY] == "writing"
        assert ROLE_CATEGORY[Role.ORIGINAL_CREATOR] == "writing"

    def test_production_category(self):
        """Production roles should map to 'production'."""
        assert ROLE_CATEGORY[Role.PRODUCER] == "production"

    def test_other_category(self):
        """Other roles should map to 'other'."""
        assert ROLE_CATEGORY[Role.OTHER] == "other"


class TestHelperFunctions:
    """Test helper functions."""

    def test_is_director_role(self):
        """is_director_role should identify director positions."""
        assert is_director_role(Role.DIRECTOR)
        assert is_director_role(Role.EPISODE_DIRECTOR)
        assert is_director_role(Role.CHIEF_ANIMATION_DIRECTOR)
        assert not is_director_role(Role.KEY_ANIMATOR)
        assert not is_director_role(Role.PRODUCER)

    def test_is_animator_role(self):
        """is_animator_role should identify animator positions."""
        assert is_animator_role(Role.ANIMATION_DIRECTOR)
        assert is_animator_role(Role.KEY_ANIMATOR)
        assert is_animator_role(Role.SECOND_KEY_ANIMATOR)
        assert is_animator_role(Role.IN_BETWEEN)
        assert not is_animator_role(Role.DIRECTOR)
        assert not is_animator_role(Role.PRODUCER)

    def test_is_mentee_role(self):
        """is_mentee_role should identify junior positions."""
        assert is_mentee_role(Role.IN_BETWEEN)
        assert is_mentee_role(Role.SECOND_KEY_ANIMATOR)
        assert is_mentee_role(Role.KEY_ANIMATOR)
        assert not is_mentee_role(Role.DIRECTOR)
        assert not is_mentee_role(Role.CHIEF_ANIMATION_DIRECTOR)

    def test_get_role_category(self):
        """get_role_category should return correct category."""
        assert get_role_category(Role.DIRECTOR) == "direction"
        assert get_role_category(Role.KEY_ANIMATOR) == "animation"
        assert get_role_category(Role.CHARACTER_DESIGNER) == "design"
        assert get_role_category(Role.SOUND_DIRECTOR) == "sound"
        assert get_role_category(Role.OTHER) == "other"

    def test_is_skill_evaluated_role(self):
        """is_skill_evaluated_role should identify skill-scored positions."""
        assert is_skill_evaluated_role(Role.KEY_ANIMATOR)
        assert is_skill_evaluated_role(Role.ANIMATION_DIRECTOR)
        assert is_skill_evaluated_role(Role.CHARACTER_DESIGNER)
        assert not is_skill_evaluated_role(Role.PRODUCER)
        assert not is_skill_evaluated_role(Role.SOUND_DIRECTOR)

    def test_is_core_team_role(self):
        """is_core_team_role should identify core positions."""
        assert is_core_team_role(Role.DIRECTOR)
        assert is_core_team_role(Role.CHIEF_ANIMATION_DIRECTOR)
        assert is_core_team_role(Role.KEY_ANIMATOR)
        assert not is_core_team_role(Role.IN_BETWEEN)
        assert not is_core_team_role(Role.PRODUCER)


class TestNoOverlap:
    """Test that role groups don't have unexpected overlaps."""

    def test_director_animator_disjoint(self):
        """Director and animator roles should be disjoint sets."""
        # DIRECTOR_ROLES and ANIMATOR_ROLES are completely disjoint by design
        # CHIEF_ANIMATION_DIRECTOR is in DIRECTOR_ROLES but not ANIMATOR_ROLES
        overlap = DIRECTOR_ROLES & ANIMATOR_ROLES
        assert overlap == set()  # No overlap expected

    def test_all_skill_roles_have_category(self):
        """All skill-evaluated roles should have a category."""
        for role in SKILL_EVALUATED_ROLES:
            assert role in ROLE_CATEGORY
            assert ROLE_CATEGORY[role] != "other"

    def test_all_core_roles_have_category(self):
        """All core team roles should have a category."""
        for role in CORE_TEAM_ROLES:
            assert role in ROLE_CATEGORY
            assert ROLE_CATEGORY[role] != "other"
