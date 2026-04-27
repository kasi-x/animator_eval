"""Role mapper for Sakuga@wiki credits.

Sakuga@wiki emits Japanese role strings in the `role_raw` column, similar in
vocabulary to SeesaaWiki (原画, 動画, 作画監督, etc.).

The mapping is identical to SeesaaWiki (shared ROLE_MAP covers all known
Japanese role strings from both sources).  Override here if source-specific
variants are discovered.

Input: Japanese role string from `role_raw` (e.g. "原画", "作画監督").
Output: Role.value string.  Falls back to Role.OTHER.value for unknown strings.
"""
from __future__ import annotations

from src.etl.role_mappers import register
from src.etl.role_mappers.seesaawiki import map_seesaawiki_role


@register("sakuga_atwiki")
def map_sakuga_atwiki_role(raw: str) -> str:
    """Map a Sakuga@wiki raw role string to a normalized Role.value.

    Delegates to the SeesaaWiki mapper: same ROLE_MAP covers all known
    Japanese role strings from Sakuga@wiki.  Override here if source-specific
    variants are discovered.
    """
    return map_seesaawiki_role(raw)
