"""Column-level source priority declarations for the Resolved layer (Phase 2a).

Each entry in ANIME_RANKING / PERSONS_RANKING / STUDIOS_RANKING maps a
canonical field name → ordered list of source prefixes from highest to lowest
priority.

Selection algorithm (implemented in resolve_*.py):
1. Walk the priority list in order.
2. Use the first non-NULL, non-empty value found.
3. If multiple conformed rows from the same priority tier exist,
   apply majority vote: pick the value agreed upon by 3+ rows.
   Tie-break: fall back to the value from the first (highest-priority) row.

Source prefix conventions match conformed.anime / conformed.persons `id` prefixes:
  anilist  → `id LIKE 'anilist:%'`
  mal      → `id LIKE 'mal:%'`
  mediaarts / madb → `id LIKE 'madb:%'`
  ann      → `id LIKE 'ann:%'`
  bgm      → `id LIKE 'bgm:%'`
  seesaa   → `id LIKE 'seesaa:%'`  (seesaawiki)
  keyframe → `id LIKE 'keyframe:%'`
  sakuga   → `id LIKE 'sakuga:%'`
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# anime field → ordered source list
# ---------------------------------------------------------------------------
ANIME_RANKING: dict[str, list[str]] = {
    # Japanese title: SeesaaWiki has the most authoritative JA titles
    "title_ja": ["seesaa", "anilist", "madb", "mal", "ann", "bgm", "keyframe"],
    # English title: AniList / MAL are best for EN
    "title_en": ["anilist", "mal", "ann", "bgm", "seesaa", "madb", "keyframe"],
    # Year: MediaArts DB (madb) has authoritative JP broadcast dates
    "year": ["madb", "anilist", "mal", "ann", "bgm", "seesaa", "keyframe"],
    # Season/quarter: aligned with year source
    "season": ["madb", "anilist", "mal", "ann", "bgm", "seesaa", "keyframe"],
    "quarter": ["madb", "anilist", "mal", "ann", "bgm", "seesaa", "keyframe"],
    # Episodes: AniList tends to be complete; madb is also strong
    "episodes": ["anilist", "madb", "mal", "ann", "bgm", "seesaa", "keyframe"],
    # Format (TV / MOVIE / OVA etc.): AniList has the cleanest enum
    "format": ["anilist", "mal", "madb", "ann", "bgm", "seesaa", "keyframe"],
    # Duration (minutes per episode): AniList / MAL
    "duration": ["anilist", "mal", "madb", "ann", "bgm", "seesaa", "keyframe"],
    # Dates
    "start_date": ["anilist", "madb", "mal", "ann", "bgm", "seesaa", "keyframe"],
    "end_date": ["anilist", "madb", "mal", "ann", "bgm", "seesaa", "keyframe"],
    # Status: AniList is most up-to-date
    "status": ["anilist", "mal", "madb", "ann", "bgm", "seesaa", "keyframe"],
    # Source material
    "source_mat": ["anilist", "mal", "madb", "ann", "bgm", "seesaa", "keyframe"],
    # Work type / scale_class: madb most accurate for JP productions
    "work_type": ["madb", "anilist", "mal", "ann", "bgm", "seesaa", "keyframe"],
    "scale_class": ["madb", "anilist", "mal", "ann", "bgm", "seesaa", "keyframe"],
    # Country of origin: AniList
    "country_of_origin": ["anilist", "mal", "madb", "ann", "bgm", "seesaa", "keyframe"],
}

# ---------------------------------------------------------------------------
# persons field → ordered source list
# ---------------------------------------------------------------------------
PERSONS_RANKING: dict[str, list[str]] = {
    # JA name: SeesaaWiki has deep JA coverage; AniList next
    "name_ja": ["seesaa", "anilist", "madb", "mal", "bgm", "ann", "keyframe"],
    # EN name: AniList / MAL / ANN are best for romaji/English
    "name_en": ["anilist", "mal", "ann", "bgm", "seesaa", "madb", "keyframe"],
    "name_ko": ["bgm", "anilist", "mal", "ann"],
    "name_zh": ["bgm", "anilist", "mal", "ann"],
    # Gender: Bangumi has explicit gender data; AniList next
    "gender": ["bgm", "anilist", "ann", "mal", "seesaa", "madb", "keyframe"],
    # Birth date: AniList / ANN have structured dates
    "birth_date": ["anilist", "ann", "mal", "bgm", "seesaa", "madb", "keyframe"],
    "death_date": ["anilist", "ann", "mal", "bgm", "seesaa", "madb", "keyframe"],
    # Nationality
    "nationality": ["anilist", "ann", "mal", "bgm", "seesaa", "madb", "keyframe"],
}

# ---------------------------------------------------------------------------
# studios field → ordered source list
# ---------------------------------------------------------------------------
STUDIOS_RANKING: dict[str, list[str]] = {
    # Name: AniList has the largest cross-referenced studio set
    "name": ["anilist", "mal", "seesaa", "madb", "bgm", "ann", "keyframe"],
    # Country: AniList explicit country_of_origin
    "country_of_origin": ["anilist", "mal", "madb", "bgm", "ann", "seesaa", "keyframe"],
    "is_animation_studio": ["anilist", "mal", "madb", "bgm", "ann", "seesaa", "keyframe"],
}

# ---------------------------------------------------------------------------
# Helper: extract source prefix from a conformed ID string
# ---------------------------------------------------------------------------


def source_prefix(conformed_id: str) -> str:
    """Return the source prefix from a conformed ID (e.g. 'anilist' from 'anilist:a123')."""
    if ":" in conformed_id:
        return conformed_id.split(":", 1)[0]
    return conformed_id


def rank_for_field(
    field: str,
    entity_type: str,  # 'anime' | 'person' | 'studio'
) -> list[str]:
    """Return the priority-ordered source list for a given entity type and field."""
    mapping: dict[str, dict[str, list[str]]] = {
        "anime": ANIME_RANKING,
        "person": PERSONS_RANKING,
        "studio": STUDIOS_RANKING,
    }
    table = mapping.get(entity_type, {})
    return table.get(field, [])
