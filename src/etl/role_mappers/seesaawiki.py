"""Role mapper for SeesaaWiki credits.

SeesaaWiki emits Japanese role strings — either pre-normalized (already passed
through the parser's _normalize_role) or raw Japanese text.  Both forms are
handled by the shared ROLE_MAP in src.runtime.models, which covers all known
SeesaaWiki-specific role variants.

Input: Japanese role string (e.g. "作画監督", "仕上げ", "演出").
Output: Role.value string (e.g. "animation_director", "finishing", "episode_director").
Falls back to Role.OTHER.value for unknown strings.
"""
from __future__ import annotations

from src.etl.role_mappers import register
from src.runtime.models import ROLE_MAP, Role


def _lookup(raw: str) -> str:
    """Resolve a SeesaaWiki role string to a normalized Role.value."""
    normalised = raw.strip().lower()
    # Try exact match first (covers both raw JA and already-normalised strings).
    role = ROLE_MAP.get(raw.strip()) or ROLE_MAP.get(normalised)
    if role is not None:
        return role.value
    return Role.OTHER.value


@register("seesaawiki")
def map_seesaawiki_role(raw: str) -> str:
    """Map a SeesaaWiki raw role string to a normalized Role.value."""
    return _lookup(raw)
