"""Role mapper for MyAnimeList (MAL) credits.

MAL emits English job title strings similar to AniList/ANN.
Examples: "director", "key animation", "character design".
Delegates to the shared ROLE_MAP which covers common English role strings.

Input: English role string from MAL API.
Output: Role.value string.  Falls back to Role.OTHER.value for unknown strings.
"""
from __future__ import annotations

from src.etl.role_mappers import register
from src.runtime.models import ROLE_MAP, Role


def _lookup(raw: str) -> str:
    """Resolve a MAL role string to a normalized Role.value."""
    role = ROLE_MAP.get(raw.strip()) or ROLE_MAP.get(raw.strip().lower())
    if role is not None:
        return role.value
    return Role.OTHER.value


@register("mal")
def map_mal_role(raw: str) -> str:
    """Map a MAL raw role string to a normalized Role.value."""
    return _lookup(raw)
