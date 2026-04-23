"""Centralized role groupings and categorization.

Single source of truth for all role-related constants across analysis modules.
This module eliminates duplication of role definitions in circles.py, trust.py,
influence.py, explain.py, graph.py, versatility.py, skill.py, and team_composition.py.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.models import Role

if TYPE_CHECKING:
    from src.models import Credit

# =============================================================================
# Role Groups (frozensets for immutability and set operations)
# =============================================================================

# DIRECTOR_ROLES: supervisory roles used for patronage and trust relationships.
# ANIMATION_DIRECTOR (作画監督) is included because they directly supervise key
# animators (giving patronage to them), making them part of the patronage hierarchy.
# However, animation directors also RECEIVE patronage from senior directors
# (DIRECTOR, EPISODE_DIRECTOR) — see patronage_dormancy.py for the two-tier logic.
# ROLE_CATEGORY correctly maps them to "animation_supervision" (not "direction")
# for career-stage analysis.
DIRECTOR_ROLES: frozenset[Role] = frozenset(
    {
        Role.DIRECTOR,
        Role.EPISODE_DIRECTOR,
        Role.ANIMATION_DIRECTOR,  # supervisory — included for patronage giving
    }
)

ANIMATOR_ROLES: frozenset[Role] = frozenset(
    {
        Role.ANIMATION_DIRECTOR,
        Role.KEY_ANIMATOR,
        Role.SECOND_KEY_ANIMATOR,
        Role.IN_BETWEEN,
        Role.CHARACTER_DESIGNER,
        Role.LAYOUT,
        Role.PHOTOGRAPHY_DIRECTOR,  # photography + effects combined
    }
)

MENTEE_ROLES: frozenset[Role] = frozenset(
    {
        Role.IN_BETWEEN,
        Role.SECOND_KEY_ANIMATOR,
        Role.KEY_ANIMATOR,
        Role.LAYOUT,
    }
)

SKILL_EVALUATED_ROLES: frozenset[Role] = frozenset(
    {
        Role.ANIMATION_DIRECTOR,
        Role.KEY_ANIMATOR,
        Role.CHARACTER_DESIGNER,
        Role.EPISODE_DIRECTOR,
        Role.BACKGROUND_ART,
        Role.PHOTOGRAPHY_DIRECTOR,
        Role.LAYOUT,
    }
)

CORE_TEAM_ROLES: frozenset[Role] = frozenset(
    {
        Role.DIRECTOR,
        Role.ANIMATION_DIRECTOR,
        Role.CHARACTER_DESIGNER,
        Role.KEY_ANIMATOR,
        Role.EPISODE_DIRECTOR,
    }
)

# Roles that typically span the entire series (through-line staff)
THROUGH_ROLES: frozenset[Role] = frozenset(
    {
        Role.DIRECTOR,
        Role.CHARACTER_DESIGNER,
        Role.BACKGROUND_ART,
        Role.SCREENPLAY,
        Role.SOUND_DIRECTOR,
        Role.MUSIC,
        Role.FINISHING,
        Role.PHOTOGRAPHY_DIRECTOR,
        Role.CGI_DIRECTOR,
        Role.PRODUCER,
        Role.ORIGINAL_CREATOR,
        Role.ANIMATION_DIRECTOR,
    }
)

# Roles that are typically per-episode
EPISODIC_ROLES: frozenset[Role] = frozenset(
    {
        Role.KEY_ANIMATOR,
        Role.SECOND_KEY_ANIMATOR,
        Role.IN_BETWEEN,
        Role.EPISODE_DIRECTOR,
        Role.ANIMATION_DIRECTOR,
        Role.LAYOUT,
        Role.PHOTOGRAPHY_DIRECTOR,
        Role.BACKGROUND_ART,
        Role.SCREENPLAY,
    }
)

# =============================================================================
# Role Categorization (unified mapping across all modules)
# =============================================================================

NON_PRODUCTION_ROLES: frozenset[Role] = frozenset(
    {
        Role.VOICE_ACTOR,
        Role.ORIGINAL_CREATOR,  # original creator — not a production staff member
        Role.MUSIC,  # composer / performer — not animation production staff
        Role.LOCALIZATION,  # localization staff — outside the Japanese production process
        Role.OTHER,  # credits with unidentifiable role — excluded from scoring
        Role.SPECIAL,  # special thanks / guests — outside normal production
    }
)


def is_production_credit(credit: Credit) -> bool:
    """Check if a credit is for production work (not voice acting, theme songs, etc.)."""
    return credit.role not in NON_PRODUCTION_ROLES


ROLE_CATEGORY: dict[Role, str] = {
    # Direction
    Role.DIRECTOR: "direction",
    Role.EPISODE_DIRECTOR: "direction",
    # Animation Supervision
    Role.ANIMATION_DIRECTOR: "animation_supervision",
    # Animation
    Role.KEY_ANIMATOR: "animation",
    Role.SECOND_KEY_ANIMATOR: "animation",
    Role.IN_BETWEEN: "animation",
    Role.LAYOUT: "animation",
    # Design
    Role.CHARACTER_DESIGNER: "design",
    # Technical (photography + effects + CG)
    Role.PHOTOGRAPHY_DIRECTOR: "technical",
    Role.CGI_DIRECTOR: "technical",
    # Art (background art)
    Role.BACKGROUND_ART: "art",
    # Sound
    Role.SOUND_DIRECTOR: "sound",
    Role.MUSIC: "sound",
    # Writing
    Role.SCREENPLAY: "writing",
    Role.ORIGINAL_CREATOR: "writing",
    # Production
    Role.PRODUCER: "production",
    Role.PRODUCTION_MANAGER: "production_management",
    # Finishing (paint + color design + QC)
    Role.FINISHING: "finishing",
    # Editing
    Role.EDITING: "editing",
    # Settings
    Role.SETTINGS: "settings",
    # Non-production
    Role.VOICE_ACTOR: "non_production",
    Role.LOCALIZATION: "non_production",
    Role.OTHER: "non_production",  # unidentifiable role
    Role.SPECIAL: "non_production",  # special thanks etc.
}

# =============================================================================
# Career Stage Hierarchy (single source of truth)
# =============================================================================
# Numeric career path for animation production. Low → high.
#
# Animation track:  in_between(1) → 2nd_key(2) → key/layout(3) → char_design(4) → anim_dir(5) → director(6)
# Direction track:  episode_director(5) → director(6)
# Technical track:  photography/CG(3) → photo_dir/CGI_dir(5=dept director, equivalent to anim_dir)
# Production track: prod_manager(2) → producer(5)
#
# Layout is part of the key animation process (layout pass → key animation clean-up),
# not a distinct career step from second key animation. Stage 3 = same as key animator.
#
# Photography director, CGI director, and sound director are department-level supervisors
# (equivalent to animation director = Stage 5). Distinct from the overall director (Stage 6).
#
# Non-production roles (ORIGINAL_CREATOR, MUSIC, VOICE_ACTOR, SPECIAL) are excluded
# from the pipeline via NON_PRODUCTION_ROLES, so Stage 0.

CAREER_STAGE: dict[Role, int] = {
    # Animation track
    Role.IN_BETWEEN: 1,  # in-between animation
    Role.SECOND_KEY_ANIMATOR: 2,  # second key animation
    Role.KEY_ANIMATOR: 3,  # key animation
    Role.LAYOUT: 3,  # layout (part of the key animation process)
    Role.CHARACTER_DESIGNER: 4,  # character design
    Role.ANIMATION_DIRECTOR: 5,  # animation director / chief animation director (dept supervisor)
    Role.EPISODE_DIRECTOR: 5,  # episode director / storyboard
    Role.DIRECTOR: 6,  # overall director
    # Technical track — dept supervisor = equivalent to animation director
    Role.PHOTOGRAPHY_DIRECTOR: 5,  # photography director
    Role.CGI_DIRECTOR: 5,  # CGI director
    # Art / Sound / Writing
    Role.BACKGROUND_ART: 3,  # background art
    Role.SOUND_DIRECTOR: 5,  # sound director (dept supervisor)
    Role.SCREENPLAY: 4,  # screenplay / series composition
    # Production management
    Role.PRODUCTION_MANAGER: 2,  # production manager / desk
    Role.PRODUCER: 5,  # producer
    # Finishing / Editing / Settings
    Role.FINISHING: 3,  # finishing / color design
    Role.EDITING: 3,  # editing
    Role.SETTINGS: 3,  # settings / prop sheets
    # Non-production — excluded from pipeline, Stage 0
    Role.ORIGINAL_CREATOR: 0,  # original creator (non-production)
    Role.MUSIC: 0,  # music (non-production)
    Role.VOICE_ACTOR: 0,  # voice actor (non-production)
    Role.LOCALIZATION: 0,  # 各国語版スタッフ（非制作）
    Role.OTHER: 0,  # ロール特定不可（非制作）
    Role.SPECIAL: 0,  # スペシャルサンクス等（非制作）
}

# String-keyed version for modules that work with role.value strings
CAREER_STAGE_BY_VALUE: dict[str, int] = {
    role.value: stage for role, stage in CAREER_STAGE.items()
}


def get_career_stage(role: Role) -> int:
    """Get the career stage number for a role (0 if unknown)."""
    return CAREER_STAGE.get(role, 0)


# =============================================================================
# Helper Functions (prose-like names for readability)
# =============================================================================


def is_director_role(role: Role) -> bool:
    """Check if a role is a director-level position."""
    return role in DIRECTOR_ROLES


def is_animator_role(role: Role) -> bool:
    """Check if a role is an animator position."""
    return role in ANIMATOR_ROLES


def is_mentee_role(role: Role) -> bool:
    """Check if a role is a junior/mentee position."""
    return role in MENTEE_ROLES


def get_role_category(role: Role) -> str:
    """Get the category for a role."""
    return ROLE_CATEGORY.get(role, "non_production")


def is_skill_evaluated_role(role: Role) -> bool:
    """Check if this role is included in skill score calculation."""
    return role in SKILL_EVALUATED_ROLES


def is_core_team_role(role: Role) -> bool:
    """Check if this role is considered a core team position."""
    return role in CORE_TEAM_ROLES


def generate_core_team_pairs(
    staff: dict[str, Role],
) -> list[tuple[str, str]]:
    """Generate collaboration pairs using CORE_TEAM star topology.

    Instead of all-pairs O(n²), generates:
    - CORE_TEAM ↔ CORE_TEAM: all pairs (k*(k-1)/2)
    - CORE_TEAM ↔ non-CORE_TEAM: star edges (n_non_core × k)
    - non-CORE_TEAM ↔ non-CORE_TEAM: no edges

    Fallback: if no CORE_TEAM members exist, generates all pairs.
    """
    core = [pid for pid, role in staff.items() if role in CORE_TEAM_ROLES]
    non_core = [pid for pid, role in staff.items() if role not in CORE_TEAM_ROLES]

    # Fallback: no core team → all pairs (small anime, O(n²) is fine)
    if not core:
        all_pids = sorted(staff.keys())
        pairs = []
        for i, a in enumerate(all_pids):
            for b in all_pids[i + 1 :]:
                pairs.append((a, b) if a < b else (b, a))
        return pairs

    pairs: list[tuple[str, str]] = []

    # Core ↔ Core: all pairs
    core_sorted = sorted(core)
    for i, a in enumerate(core_sorted):
        for b in core_sorted[i + 1 :]:
            pairs.append((a, b))

    # Core ↔ Non-core: star edges
    for nc in non_core:
        for c in core_sorted:
            pairs.append((c, nc) if c < nc else (nc, c))

    return pairs
