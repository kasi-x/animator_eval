"""ETL layer: bronze → silver integration.

Public API:
- upsert_canonical_anime: unified write path for scrapers (silver canonical)
- integrate_anilist / integrate_ann / integrate_allcinema / integrate_seesaawiki /
  integrate_keyframe / run_integration: source-specific and full integration entry points

Internal helpers (names starting with '_') are not exported and must not
be imported by code outside src/etl/.
"""
from src.etl.integrate import (
    integrate_allcinema,
    integrate_anilist,
    integrate_ann,
    integrate_keyframe,
    integrate_seesaawiki,
    run_integration,
    upsert_canonical_anime,
)

__all__ = [
    "upsert_canonical_anime",
    "integrate_allcinema",
    "integrate_anilist",
    "integrate_ann",
    "integrate_keyframe",
    "integrate_seesaawiki",
    "run_integration",
]
