"""Role mapper for AniList credits.

AniList emits English job title strings (lower-cased or mixed-case) in the
`raw_role` column (e.g. "Key Animation (eps 1-3)") and pre-normalized Role.value
strings in the `role` column (e.g. "key_animator").

The ETL loader passes the `role` column through this mapper, so the mapper must
handle both forms:

  1. Role.value strings (already normalized) → returned unchanged.
  2. Human-readable English strings → looked up in the shared ROLE_MAP.
  3. Unknown strings → Role.OTHER.value.

Input: Role.value string or English role string from AniList.
Output: Role.value string.
"""
from __future__ import annotations

from src.etl.role_mappers import register
from src.runtime.models import ROLE_MAP, Role

_VALID_ROLE_VALUES: frozenset[str] = frozenset(r.value for r in Role)


def _lookup(raw: str) -> str:
    """Resolve an AniList role string to a normalized Role.value.

    Pass-through for already-normalized Role.value strings; delegates to
    ROLE_MAP for human-readable English strings.
    """
    stripped = raw.strip()
    if stripped in _VALID_ROLE_VALUES:
        return stripped
    role = ROLE_MAP.get(stripped) or ROLE_MAP.get(stripped.lower())
    if role is not None:
        return role.value
    return Role.OTHER.value


@register("anilist")
def map_anilist_role(raw: str) -> str:
    """Map an AniList raw role string to a normalized Role.value."""
    return _lookup(raw)
