"""Voice Actor Trust + Patronage — sound director relationship scoring.

VA Trust: Σ_sd(log(1+n_collabs) × role_escalation × time_decay)
VA Patronage: Σ_sd(BiRank_sd × log(1+N_id))

Role escalation: SUPPORTING→MAIN = +0.3 bonus (trust deepening).
"""

import math
from collections import defaultdict

import structlog

from src.analysis.va.graph import _char_role_weight
from src.runtime.models import AnimeAnalysis as Anime, CharacterVoiceActor, Credit, Role

logger = structlog.get_logger()

DECAY_HALF_LIFE_YEARS = 3.0
DECAY_LAMBDA = math.log(2) / DECAY_HALF_LIFE_YEARS


def compute_va_trust(
    va_credits: list[CharacterVoiceActor],
    production_credits: list[Credit],
    anime_map: dict[str, Anime],
    current_year: int = 2026,
) -> dict[str, float]:
    """Compute trust scores for voice actors based on sound director relationships.

    Trust = Σ_sd(log(1+n_collabs) × role_escalation × time_decay)

    Args:
        va_credits: character_voice_actor records
        production_credits: production credits (for sound directors)
        anime_map: anime_id → Anime
        current_year: reference year

    Returns:
        va_id → trust score
    """
    # Sound directors per anime
    anime_sd: dict[str, set[str]] = defaultdict(set)
    for c in production_credits:
        if c.role == Role.SOUND_DIRECTOR:
            anime_sd[c.anime_id].add(c.person_id)

    # VA-SD collaboration tracking
    # (va, sd) → list of (year, best_role_weight)
    va_sd_collabs: dict[tuple[str, str], list[tuple[int, float]]] = defaultdict(list)

    for cva in va_credits:
        anime = anime_map.get(cva.anime_id)
        if not anime or not anime.year:
            continue
        w = _char_role_weight(cva.character_role)
        for sd_id in anime_sd.get(cva.anime_id, set()):
            va_sd_collabs[(cva.person_id, sd_id)].append((anime.year, w))

    # Detect role escalation per (VA, SD) pair
    va_sd_escalation: dict[tuple[str, str], float] = {}
    for (va_id, sd_id), collabs in va_sd_collabs.items():
        sorted_collabs = sorted(collabs, key=lambda x: x[0])
        # Check if role escalated (e.g., SUPPORTING → MAIN)
        if len(sorted_collabs) >= 2:
            first_w = sorted_collabs[0][1]
            last_w = sorted_collabs[-1][1]
            if last_w > first_w:
                va_sd_escalation[(va_id, sd_id)] = 0.3
            else:
                va_sd_escalation[(va_id, sd_id)] = 0.0
        else:
            va_sd_escalation[(va_id, sd_id)] = 0.0

    # Compute trust
    trust: dict[str, float] = {}
    for (va_id, sd_id), collabs in va_sd_collabs.items():
        n_collabs = len(collabs)
        escalation = va_sd_escalation.get((va_id, sd_id), 0.0)

        # Time decay: use most recent collaboration year
        most_recent = max(c[0] for c in collabs)
        years_ago = current_year - most_recent
        time_decay = math.exp(-DECAY_LAMBDA * max(0, years_ago))

        contribution = math.log1p(n_collabs) * (1.0 + escalation) * time_decay

        if va_id not in trust:
            trust[va_id] = 0.0
        trust[va_id] += contribution

    logger.info("va_trust_computed", va_persons=len(trust))
    return trust


def compute_va_patronage(
    va_credits: list[CharacterVoiceActor],
    production_credits: list[Credit],
    anime_map: dict[str, Anime],
    sd_birank_scores: dict[str, float],
) -> dict[str, float]:
    """Compute patronage premium for voice actors.

    Patronage = Σ_sd(BiRank_sd × log(1+N_id))

    Uses the production pipeline's BiRank scores for sound directors.

    Args:
        va_credits: character_voice_actor records
        production_credits: production credits
        anime_map: anime_id → Anime
        sd_birank_scores: sound_director_id → BiRank score

    Returns:
        va_id → patronage score
    """
    # Sound directors per anime
    anime_sd: dict[str, set[str]] = defaultdict(set)
    for c in production_credits:
        if c.role == Role.SOUND_DIRECTOR:
            anime_sd[c.anime_id].add(c.person_id)

    # Count VA-SD collaborations
    va_sd_count: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for cva in va_credits:
        anime = anime_map.get(cva.anime_id)
        if not anime:
            continue
        for sd_id in anime_sd.get(cva.anime_id, set()):
            va_sd_count[cva.person_id][sd_id] += 1

    # Compute patronage
    patronage: dict[str, float] = {}
    for va_id, sd_counts in va_sd_count.items():
        total = 0.0
        for sd_id, n_collabs in sd_counts.items():
            br = sd_birank_scores.get(sd_id, 0.0)
            total += br * math.log1p(n_collabs)
        patronage[va_id] = total

    logger.info("va_patronage_computed", va_persons=len(patronage))
    return patronage
