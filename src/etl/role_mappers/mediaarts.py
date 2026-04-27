"""Role mapper for MediaArts Database (MADB) credits.

MediaArts bronze already writes both `role` (pre-normalized) and `raw_role`
(original Japanese text), so this mapper acts as a pass-through validator:
if the value is already a known Role.value it is returned unchanged; otherwise
ROLE_MAP is tried as a fallback for any Japanese strings that slipped through.

Input: role string from MediaArts bronze (expected to already be a Role.value).
Output: Role.value string.  Falls back to Role.OTHER.value for unknown strings.
"""
from __future__ import annotations

from src.etl.role_mappers import register
from src.runtime.models import ROLE_MAP, Role

_VALID_ROLE_VALUES: frozenset[str] = frozenset(r.value for r in Role)


def _lookup(raw: str) -> str:
    """Validate a MediaArts role value, attempting ROLE_MAP fallback if needed."""
    stripped = raw.strip()
    if stripped in _VALID_ROLE_VALUES:
        return stripped
    # Attempt Japanese text lookup via shared ROLE_MAP
    role = ROLE_MAP.get(stripped) or ROLE_MAP.get(stripped.lower())
    if role is not None:
        return role.value
    return Role.OTHER.value


@register("mediaarts")
def map_mediaarts_role(raw: str) -> str:
    """Map a MediaArts role string to a normalized Role.value."""
    return _lookup(raw)
