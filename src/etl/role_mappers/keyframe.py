"""Role mapper for Keyframe.app credits.

Keyframe bronze writes a normalized Role.value in the `role` column and the
original Japanese string in `raw_role`.  This mapper is applied to the `role`
column, which may already be a valid Role.value string.

Resolution order:
  1. If the input is already a valid Role.value string, return it unchanged.
  2. Otherwise delegate to the SeesaaWiki mapper (same ROLE_MAP covers all
     known Japanese role strings from both sources).
  3. Fall back to Role.OTHER.value for unknown strings.

Input: Role.value string (e.g. "animation_director") or Japanese role string
       (e.g. "作画監督", "演出").
Output: Role.value string.
"""
from __future__ import annotations

from src.etl.role_mappers import register
from src.etl.role_mappers.seesaawiki import map_seesaawiki_role
from src.runtime.models import Role

_VALID_ROLE_VALUES: frozenset[str] = frozenset(r.value for r in Role)


def _lookup(raw: str) -> str:
    """Resolve a Keyframe role string to a normalized Role.value.

    Pass-through for already-normalized Role.value strings; delegates to the
    SeesaaWiki mapper for Japanese strings.
    """
    normalized = raw.strip()
    if normalized in _VALID_ROLE_VALUES:
        return normalized
    return map_seesaawiki_role(normalized)


@register("keyframe")
def map_keyframe_role(raw: str) -> str:
    """Map a Keyframe raw role string to a normalized Role.value."""
    return _lookup(raw)
