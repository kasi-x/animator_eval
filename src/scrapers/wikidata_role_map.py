"""Wikidata property → role-token mapping for anime staff credits.

Shared between jvmg_fetcher.py (Wikidata SPARQL scraper) and the planned
wikidata_world_scraper.py. Values are plain role tokens consumable by
`src.models.parse_role`, so the mapping stays authoritative regardless of
which scraper constructs credits.

Each entry pairs a Wikidata property ID with a token accepted by
`parse_role()`. The token must appear in `ROLE_MAP` in `src/models.py` —
otherwise `parse_role()` falls back to `Role.SPECIAL`, silently dropping
the credit's role signal.

Verified entries (2026-04-23):

- P57   "director"     → Role.DIRECTOR
- P58   "screenplay"   → Role.SCREENPLAY     (Wikidata label: "screenwriter")
- P1040 "film editor"  → Role.EDITING
- P3174 "art director" → Role.BACKGROUND_ART

Pending verification:

- P10800 (purportedly "animation director") — property ID unverified on
  wikidata.org. "animation director" is a valid ROLE_MAP key, so
  re-enable once the property ID is confirmed.
"""

from __future__ import annotations


WIKIDATA_ROLE_MAP: dict[str, str] = {
    "P57": "director",
    "P58": "screenplay",
    "P1040": "film editor",
    "P3174": "art director",
}


__all__ = ["WIKIDATA_ROLE_MAP"]
