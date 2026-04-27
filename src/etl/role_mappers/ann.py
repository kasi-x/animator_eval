"""Role mapper for Anime News Network (ANN) credits.

ANN emits English job title strings similar to AniList.
Examples: "director", "key animation", "series composition".
Delegates to the shared ROLE_MAP which covers common English role strings.

Input: English role string from ANN.
Output: Role.value string.  Falls back to Role.OTHER.value for unknown strings.
"""
from __future__ import annotations

from src.etl.role_mappers import register
from src.runtime.models import ROLE_MAP, Role


def _lookup(raw: str) -> str:
    """Resolve an ANN role string to a normalized Role.value."""
    role = ROLE_MAP.get(raw.strip()) or ROLE_MAP.get(raw.strip().lower())
    if role is not None:
        return role.value
    return Role.OTHER.value


@register("ann")
def map_ann_role(raw: str) -> str:
    """Map an ANN raw role string to a normalized Role.value."""
    return _lookup(raw)
