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

DIRECTOR_ROLES: frozenset[Role] = frozenset(
    {
        Role.DIRECTOR,
        Role.EPISODE_DIRECTOR,
        Role.ANIMATION_DIRECTOR,  # supervisory — included for patronage
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
        Role.PHOTOGRAPHY_DIRECTOR,  # 撮影+エフェクト統合
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
        Role.ORIGINAL_CREATOR,  # 原作者 — 制作スタッフではない
        Role.MUSIC,  # 作曲家・演奏者 — アニメーション制作スタッフではない
        Role.LOCALIZATION,  # 各国語版スタッフ — 日本の制作工程外
        Role.SPECIAL,  # 制作工程外 + 分類不能
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
    # Technical (撮影+エフェクト+CG)
    Role.PHOTOGRAPHY_DIRECTOR: "technical",
    Role.CGI_DIRECTOR: "technical",
    # Art (美術+背景)
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
    # Finishing (仕上+色彩+検査)
    Role.FINISHING: "finishing",
    # Editing
    Role.EDITING: "editing",
    # Settings
    Role.SETTINGS: "settings",
    # Non-production
    Role.VOICE_ACTOR: "non_production",
    Role.LOCALIZATION: "non_production",
    Role.SPECIAL: "non_production",
}

# =============================================================================
# Career Stage Hierarchy (single source of truth)
# =============================================================================
# アニメーション制作のキャリアパスを数値化。低→高。
#
# アニメーター系: 動画(1) → 第二原画(2) → 原画/レイアウト(3) → キャラデ(4) → 作監(5) → 監督(6)
# 演出系:        演出(5) → 監督(6)
# 技術系:        撮影/CG(3) → 撮影監督/CG監督(5=部門監督、作監相当)
# 制作系:        制作進行(2) → プロデューサー(5)
#
# レイアウトは原画工程の一部（レイアウト作業 → 原画清書）であり、
# 第二原画とは異なるキャリアステップではない。Stage 3 = 原画と同格。
#
# 撮影監督・CGI監督・音響監督は部門監督（作画監督と同格 = Stage 5）。
# 全体統括の監督（Stage 6）とは区別する。
#
# 非制作職 (ORIGINAL_CREATOR, MUSIC, VOICE_ACTOR, SPECIAL) は
# NON_PRODUCTION_ROLES でパイプラインから除外されるため Stage 0。

CAREER_STAGE: dict[Role, int] = {
    # Animation track
    Role.IN_BETWEEN: 1,           # 動画
    Role.SECOND_KEY_ANIMATOR: 2,  # 第二原画
    Role.KEY_ANIMATOR: 3,         # 原画
    Role.LAYOUT: 3,               # レイアウト（原画工程の一部）
    Role.CHARACTER_DESIGNER: 4,   # キャラクターデザイン
    Role.ANIMATION_DIRECTOR: 5,   # 作画監督・総作画監督（部門監督）
    Role.EPISODE_DIRECTOR: 5,     # 演出・絵コンテ
    Role.DIRECTOR: 6,             # 監督（全体統括）
    # Technical track — 部門監督 = 作画監督相当
    Role.PHOTOGRAPHY_DIRECTOR: 5, # 撮影監督
    Role.CGI_DIRECTOR: 5,         # CGI監督
    # Art / Sound / Writing
    Role.BACKGROUND_ART: 3,       # 美術・背景
    Role.SOUND_DIRECTOR: 5,       # 音響監督（部門監督）
    Role.SCREENPLAY: 4,           # 脚本・シリーズ構成
    # Production management
    Role.PRODUCTION_MANAGER: 2,   # 制作進行・制作デスク
    Role.PRODUCER: 5,             # プロデューサー
    # Finishing / Editing / Settings
    Role.FINISHING: 3,            # 仕上げ・色彩設計
    Role.EDITING: 3,              # 編集
    Role.SETTINGS: 3,             # 設定
    # Non-production — パイプラインで除外されるため Stage 0
    Role.ORIGINAL_CREATOR: 0,     # 原作（非制作）
    Role.MUSIC: 0,                # 音楽（非制作）
    Role.VOICE_ACTOR: 0,          # 声優（非制作）
    Role.LOCALIZATION: 0,         # 各国語版スタッフ（非制作）
    Role.SPECIAL: 0,              # その他（非制作）
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
