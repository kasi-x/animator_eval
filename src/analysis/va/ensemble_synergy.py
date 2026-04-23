"""Voice Actor Ensemble Synergy — co-starring pair analysis.

Measures how well VA pairs perform together across shared anime,
using structural data (shared works, co-main frequency, genre diversity).
"""

import math
from collections import defaultdict
from dataclasses import dataclass

import structlog

from src.runtime.models import AnimeAnalysis as Anime, CharacterVoiceActor

logger = structlog.get_logger()


@dataclass
class VASynergy:
    """Synergy metrics for a VA pair.

    Attributes:
        va_a: first VA ID (canonical order: a < b)
        va_b: second VA ID
        shared_anime: number of anime both VAs appeared in
        co_main_count: number where both had MAIN roles
        distinct_genres: number of distinct genres across shared anime
        synergy_score: composite synergy metric
    """

    va_a: str = ""
    va_b: str = ""
    shared_anime: int = 0
    co_main_count: int = 0
    distinct_genres: int = 0
    synergy_score: float = 0.0


def compute_va_ensemble_synergy(
    va_credits: list[CharacterVoiceActor],
    anime_map: dict[str, Anime],
    min_shared: int = 3,
) -> list[VASynergy]:
    """Compute ensemble synergy for VA pairs.

    synergy(a,b) = sqrt(shared_anime) * (1 + co_main_count/shared_anime)
                   * log(1 + distinct_genres_shared)

    Args:
        va_credits: all character_voice_actor records
        anime_map: anime_id -> Anime
        min_shared: minimum shared anime to report a pair

    Returns:
        List of VASynergy (sorted by synergy_score descending).
    """
    # Build anime -> {va_id: best_role} mapping
    anime_va_roles: dict[str, dict[str, str]] = defaultdict(dict)
    for cva in va_credits:
        aid = cva.anime_id
        pid = cva.person_id
        role = cva.character_role.upper()
        # Keep best role (MAIN > SUPPORTING > BACKGROUND)
        _ROLE_RANK = {"MAIN": 3, "SUPPORTING": 2, "BACKGROUND": 1}
        cur = anime_va_roles[aid].get(pid, "")
        if _ROLE_RANK.get(role, 0) > _ROLE_RANK.get(cur, 0):
            anime_va_roles[aid][pid] = role

    # Build per-anime genre set
    anime_genres: dict[str, set[str]] = {}
    for aid, anime in anime_map.items():
        if anime.genres:
            anime_genres[aid] = set(anime.genres)

    # Find shared anime per VA pair (using star topology: only pair with MAIN VAs)
    pair_shared: dict[tuple[str, str], set[str]] = defaultdict(set)
    for aid, va_roles in anime_va_roles.items():
        if len(va_roles) < 2:
            continue
        main_vas = [v for v, r in va_roles.items() if r == "MAIN"]
        other_vas = [v for v, r in va_roles.items() if r != "MAIN"]

        # Main <-> Main
        for i, a in enumerate(main_vas):
            for b in main_vas[i + 1 :]:
                key = (a, b) if a < b else (b, a)
                pair_shared[key].add(aid)

        # Main <-> Other
        for m in main_vas:
            for o in other_vas:
                key = (m, o) if m < o else (o, m)
                pair_shared[key].add(aid)

    # Compute synergy for qualifying pairs
    results: list[VASynergy] = []
    for (va_a, va_b), shared_aids in pair_shared.items():
        n_shared = len(shared_aids)
        if n_shared < min_shared:
            continue

        # Co-main count
        co_main = 0
        for aid in shared_aids:
            roles = anime_va_roles.get(aid, {})
            if roles.get(va_a) == "MAIN" and roles.get(va_b) == "MAIN":
                co_main += 1

        # Distinct genres across shared anime
        all_genres: set[str] = set()
        for aid in shared_aids:
            all_genres.update(anime_genres.get(aid, set()))
        n_genres = len(all_genres)

        # Synergy formula
        synergy = (
            math.sqrt(n_shared) * (1.0 + co_main / n_shared) * math.log1p(n_genres)
        )

        results.append(
            VASynergy(
                va_a=va_a,
                va_b=va_b,
                shared_anime=n_shared,
                co_main_count=co_main,
                distinct_genres=n_genres,
                synergy_score=synergy,
            )
        )

    results.sort(key=lambda s: s.synergy_score, reverse=True)
    logger.info("va_ensemble_synergy_computed", pairs=len(results))
    return results
