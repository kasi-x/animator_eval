"""Voice Actor Replacement Difficulty Index (RDI).

Measures how difficult it would be to replace a VA based on:
- Niche uniqueness (low overlap with similar VAs)
- Genre rarity (specialization in uncommon genres)
- Character exclusivity (fraction of solo-voiced characters)
- Franchise lock-in (long-running series commitment)
"""

import math
from collections import defaultdict
from dataclasses import dataclass

import numpy as np
import structlog

from src.models import AnimeAnalysis as Anime, CharacterVoiceActor

logger = structlog.get_logger()


@dataclass
class ReplacementDifficulty:
    """Replacement difficulty metrics for a single VA.

    Attributes:
        person_id: voice actor ID
        niche_uniqueness: 1 - max(Jaccard similarity with peers in same tier)
        genre_rarity: weighted rarity of VA's genres
        character_exclusivity: fraction of characters only this VA voices
        franchise_lock_in: commitment to long-running series
        rdi: composite Replacement Difficulty Index (0-1)
    """

    person_id: str = ""
    niche_uniqueness: float = 0.0
    genre_rarity: float = 0.0
    character_exclusivity: float = 0.0
    franchise_lock_in: float = 0.0
    rdi: float = 0.0


def _normalize_0_1(values: list[float]) -> list[float]:
    """Min-max normalize to [0, 1]."""
    if not values:
        return []
    mn, mx = min(values), max(values)
    rng = mx - mn
    if rng < 1e-10:
        return [0.5] * len(values)
    return [(v - mn) / rng for v in values]


def compute_replacement_difficulty(
    va_credits: list[CharacterVoiceActor],
    anime_map: dict[str, Anime],
    casting_tiers: dict[str, str] | None = None,
    min_characters: int = 5,
) -> dict[str, ReplacementDifficulty]:
    """Compute Replacement Difficulty Index for all qualifying VAs.

    RDI = 0.3*niche_uniqueness + 0.25*genre_rarity
        + 0.25*character_exclusivity + 0.2*franchise_lock_in

    Args:
        va_credits: all character_voice_actor records
        anime_map: anime_id -> Anime
        casting_tiers: person_id -> casting tier (for peer comparison)
        min_characters: minimum unique characters to include VA

    Returns:
        person_id -> ReplacementDifficulty
    """
    casting_tiers = casting_tiers or {}

    # Build per-VA data
    va_anime: dict[str, set[str]] = defaultdict(set)
    va_chars: dict[str, set[str]] = defaultdict(set)
    va_genres: dict[str, set[str]] = defaultdict(set)
    char_vas: dict[str, set[str]] = defaultdict(set)

    # Track franchise participation: (person_id, character_id) -> set of anime_ids
    va_char_anime: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )

    for cva in va_credits:
        pid = cva.person_id
        va_anime[pid].add(cva.anime_id)
        va_chars[pid].add(cva.character_id)
        char_vas[cva.character_id].add(pid)
        va_char_anime[pid][cva.character_id].add(cva.anime_id)

        anime = anime_map.get(cva.anime_id)
        if anime and anime.genres:
            va_genres[pid].update(anime.genres)

    # Count genre frequency across all anime (for rarity computation)
    genre_anime_count: dict[str, int] = defaultdict(int)
    for anime in anime_map.values():
        if anime.genres:
            for genre in anime.genres:
                genre_anime_count[genre] += 1
    total_anime = max(len(anime_map), 1)

    # Qualifying VAs
    qualifying = [pid for pid in va_chars if len(va_chars[pid]) >= min_characters]
    if not qualifying:
        return {}

    # 1. Niche Uniqueness: 1 - max(Jaccard similarity with peers in same casting tier)
    tier_groups: dict[str, list[str]] = defaultdict(list)
    for pid in qualifying:
        tier = casting_tiers.get(pid, "unknown")
        tier_groups[tier].append(pid)

    raw_uniqueness: dict[str, float] = {}
    for pid in qualifying:
        tier = casting_tiers.get(pid, "unknown")
        peers = tier_groups.get(tier, [])
        my_anime = va_anime[pid]

        max_jaccard = 0.0
        for peer in peers:
            if peer == pid:
                continue
            peer_anime = va_anime[peer]
            intersection = len(my_anime & peer_anime)
            union = len(my_anime | peer_anime)
            if union > 0:
                jaccard = intersection / union
                max_jaccard = max(max_jaccard, jaccard)

        raw_uniqueness[pid] = 1.0 - max_jaccard

    # 2. Genre Rarity: weighted inverse frequency of VA's genres
    raw_genre_rarity: dict[str, float] = {}
    for pid in qualifying:
        genres = va_genres.get(pid, set())
        if not genres:
            raw_genre_rarity[pid] = 0.0
            continue
        # IDF-like rarity
        rarities = []
        for genre in genres:
            count = genre_anime_count.get(genre, 1)
            rarities.append(math.log(total_anime / count))
        raw_genre_rarity[pid] = float(np.mean(rarities))

    # 3. Character Exclusivity: fraction of characters only this VA voices
    raw_exclusivity: dict[str, float] = {}
    for pid in qualifying:
        chars = va_chars[pid]
        if not chars:
            raw_exclusivity[pid] = 0.0
            continue
        exclusive = sum(1 for c in chars if len(char_vas[c]) == 1)
        raw_exclusivity[pid] = exclusive / len(chars)

    # 4. Franchise Lock-in: average anime count per character (multi-entry characters)
    raw_franchise: dict[str, float] = {}
    for pid in qualifying:
        char_anime_counts = [
            len(anime_ids) for anime_ids in va_char_anime[pid].values()
        ]
        if not char_anime_counts:
            raw_franchise[pid] = 0.0
            continue
        # Fraction of characters appearing in multiple anime, weighted by count
        multi_entry = [c for c in char_anime_counts if c >= 2]
        if not multi_entry:
            raw_franchise[pid] = 0.0
        else:
            raw_franchise[pid] = (
                len(multi_entry) / len(char_anime_counts) * math.log1p(sum(multi_entry))
            )

    # Normalize all components
    pids = sorted(qualifying)
    norm_uniq = _normalize_0_1([raw_uniqueness[p] for p in pids])
    norm_rarity = _normalize_0_1([raw_genre_rarity[p] for p in pids])
    norm_excl = _normalize_0_1([raw_exclusivity[p] for p in pids])
    norm_fran = _normalize_0_1([raw_franchise[p] for p in pids])

    # Compute RDI
    results: dict[str, ReplacementDifficulty] = {}
    for i, pid in enumerate(pids):
        rdi = (
            0.30 * norm_uniq[i]
            + 0.25 * norm_rarity[i]
            + 0.25 * norm_excl[i]
            + 0.20 * norm_fran[i]
        )
        results[pid] = ReplacementDifficulty(
            person_id=pid,
            niche_uniqueness=raw_uniqueness[pid],
            genre_rarity=raw_genre_rarity[pid],
            character_exclusivity=raw_exclusivity[pid],
            franchise_lock_in=raw_franchise[pid],
            rdi=rdi,
        )

    logger.info("va_replacement_difficulty_computed", persons=len(results))
    return results
