"""Centralized role groupings and categorization.

Single source of truth for all role-related constants across analysis modules.
This module eliminates duplication of role definitions in circles.py, trust.py,
influence.py, explain.py, graph.py, versatility.py, skill.py, and team_composition.py.
"""

from src.models import Role

# =============================================================================
# Role Groups (frozensets for immutability and set operations)
# =============================================================================

DIRECTOR_ROLES: frozenset[Role] = frozenset(
    {
        Role.DIRECTOR,
        Role.EPISODE_DIRECTOR,
        Role.CHIEF_ANIMATION_DIRECTOR,
    }
)

ANIMATOR_ROLES: frozenset[Role] = frozenset(
    {
        Role.ANIMATION_DIRECTOR,
        Role.KEY_ANIMATOR,
        Role.SECOND_KEY_ANIMATOR,
        Role.IN_BETWEEN,
        Role.CHARACTER_DESIGNER,
        Role.STORYBOARD,
        Role.LAYOUT,
        Role.EFFECTS,
    }
)

MENTEE_ROLES: frozenset[Role] = frozenset(
    {
        Role.IN_BETWEEN,
        Role.SECOND_KEY_ANIMATOR,
        Role.KEY_ANIMATOR,
        Role.LAYOUT,
        Role.EFFECTS,
    }
)

SKILL_EVALUATED_ROLES: frozenset[Role] = frozenset(
    {
        Role.CHIEF_ANIMATION_DIRECTOR,
        Role.ANIMATION_DIRECTOR,
        Role.KEY_ANIMATOR,
        Role.SECOND_KEY_ANIMATOR,
        Role.CHARACTER_DESIGNER,
        Role.STORYBOARD,
        Role.EPISODE_DIRECTOR,
        Role.ART_DIRECTOR,
        Role.EFFECTS,
        Role.LAYOUT,
    }
)

CORE_TEAM_ROLES: frozenset[Role] = frozenset(
    {
        Role.DIRECTOR,
        Role.CHIEF_ANIMATION_DIRECTOR,
        Role.ANIMATION_DIRECTOR,
        Role.CHARACTER_DESIGNER,
        Role.KEY_ANIMATOR,
        Role.STORYBOARD,
        Role.EPISODE_DIRECTOR,
    }
)

# Roles that typically span the entire series (through-line staff)
THROUGH_ROLES: frozenset[Role] = frozenset(
    {
        Role.DIRECTOR,
        Role.CHARACTER_DESIGNER,
        Role.ART_DIRECTOR,
        Role.SERIES_COMPOSITION,
        Role.SOUND_DIRECTOR,
        Role.MUSIC,
        Role.COLOR_DESIGNER,
        Role.PHOTOGRAPHY_DIRECTOR,
        Role.CGI_DIRECTOR,
        Role.PRODUCER,
        Role.ORIGINAL_CREATOR,
        Role.MECHANICAL_DESIGNER,
        Role.CHIEF_ANIMATION_DIRECTOR,
    }
)

# Roles that are typically per-episode
EPISODIC_ROLES: frozenset[Role] = frozenset(
    {
        Role.KEY_ANIMATOR,
        Role.SECOND_KEY_ANIMATOR,
        Role.IN_BETWEEN,
        Role.EPISODE_DIRECTOR,
        Role.STORYBOARD,
        Role.ANIMATION_DIRECTOR,
        Role.LAYOUT,
        Role.EFFECTS,
        Role.BACKGROUND_ART,
        Role.SCREENPLAY,
    }
)

# =============================================================================
# Role Categorization (unified mapping across all modules)
# =============================================================================

ROLE_CATEGORY: dict[Role, str] = {
    # Direction (5 roles)
    Role.DIRECTOR: "direction",
    Role.EPISODE_DIRECTOR: "direction",
    Role.STORYBOARD: "direction",
    Role.SERIES_COMPOSITION: "direction",
    # Animation Supervision (2 roles)
    Role.CHIEF_ANIMATION_DIRECTOR: "animation_supervision",
    Role.ANIMATION_DIRECTOR: "animation_supervision",
    # Animation (4 roles)
    Role.KEY_ANIMATOR: "animation",
    Role.SECOND_KEY_ANIMATOR: "animation",
    Role.IN_BETWEEN: "animation",
    Role.LAYOUT: "animation",
    # Design (4 roles)
    Role.CHARACTER_DESIGNER: "design",
    Role.MECHANICAL_DESIGNER: "design",
    Role.ART_DIRECTOR: "design",
    Role.COLOR_DESIGNER: "design",
    # Technical (3 roles)
    Role.EFFECTS: "technical",
    Role.CGI_DIRECTOR: "technical",
    Role.PHOTOGRAPHY_DIRECTOR: "technical",
    # Art (1 role)
    Role.BACKGROUND_ART: "art",
    # Sound (2 roles)
    Role.SOUND_DIRECTOR: "sound",
    Role.MUSIC: "sound",
    # Writing (2 roles)
    Role.SCREENPLAY: "writing",
    Role.ORIGINAL_CREATOR: "writing",
    # Production (1 role)
    Role.PRODUCER: "production",
    # Other
    Role.OTHER: "other",
}

# =============================================================================
# Helper Functions (prose-like names for readability)
# =============================================================================


def is_director_role(role: Role) -> bool:
    """Check if a role is a director-level position.

    Args:
        role: The role to check

    Returns:
        True if role is Director, Episode Director, or Chief Animation Director
    """
    return role in DIRECTOR_ROLES


def is_animator_role(role: Role) -> bool:
    """Check if a role is an animator position.

    Args:
        role: The role to check

    Returns:
        True if role is in the animator role set
    """
    return role in ANIMATOR_ROLES


def is_mentee_role(role: Role) -> bool:
    """Check if a role is a junior/mentee position.

    Junior roles are typically entry-level or mid-level positions where
    staff learn from more experienced directors and supervisors.

    Args:
        role: The role to check

    Returns:
        True if role is a mentee-level position
    """
    return role in MENTEE_ROLES


def get_role_category(role: Role) -> str:
    """Get the category for a role.

    Categories include: direction, animation_supervision, animation, design,
    technical, art, sound, writing, production, and other.

    Args:
        role: The role to categorize

    Returns:
        Category string, or "other" if role is not mapped
    """
    return ROLE_CATEGORY.get(role, "other")


def is_skill_evaluated_role(role: Role) -> bool:
    """Check if this role is included in skill score calculation.

    Skill-evaluated roles are those where individual contribution can be
    measured through project participation and collaboration patterns.

    Args:
        role: The role to check

    Returns:
        True if role is included in skill scoring
    """
    return role in SKILL_EVALUATED_ROLES


def is_core_team_role(role: Role) -> bool:
    """Check if this role is considered a core team position.

    Core team roles are high-value positions that significantly influence
    production quality and team dynamics.

    Args:
        role: The role to check

    Returns:
        True if role is a core team position
    """
    return role in CORE_TEAM_ROLES
