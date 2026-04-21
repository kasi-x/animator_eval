"""Voice Actor Character Diversity — CDI, casting range, genre versatility.

Measures how diverse a VA's character portfolio is across genres,
gender range, and role balance.
"""

import math
from collections import defaultdict
from dataclasses import dataclass

import structlog

from src.models import AnimeAnalysis as Anime, Character, CharacterVoiceActor

logger = structlog.get_logger()


@dataclass
class CharacterDiversityMetrics:
    """Character diversity metrics for a single VA.

    Attributes:
        person_id: voice actor ID
        genre_entropy: Shannon entropy of genre distribution
        gender_range: fraction of characters with gender different from VA
        role_balance: entropy of MAIN/SUPPORTING/BACKGROUND distribution
        unique_characters: number of unique characters voiced
        cdi: composite Character Diversity Index (0-1)
        casting_tier: lead_specialist / versatile / ensemble / newcomer
        main_count: number of MAIN roles
        supporting_count: number of SUPPORTING roles
        background_count: number of BACKGROUND roles
    """

    person_id: str = ""
    genre_entropy: float = 0.0
    gender_range: float = 0.0
    role_balance: float = 0.0
    unique_characters: int = 0
    cdi: float = 0.0
    casting_tier: str = "newcomer"
    main_count: int = 0
    supporting_count: int = 0
    background_count: int = 0


def _shannon_entropy(counts: list[int]) -> float:
    """Shannon entropy from a list of counts."""
    total = sum(counts)
    if total == 0:
        return 0.0
    probs = [c / total for c in counts if c > 0]
    return -sum(p * math.log2(p) for p in probs)


def _normalize_0_1(values: list[float]) -> list[float]:
    """Min-max normalize to [0, 1]."""
    if not values:
        return []
    mn, mx = min(values), max(values)
    rng = mx - mn
    if rng < 1e-10:
        return [0.5] * len(values)
    return [(v - mn) / rng for v in values]


def _classify_casting_tier(
    main_count: int,
    supporting_count: int,
    total_chars: int,
) -> str:
    """Classify VA casting tier based on role distribution."""
    if total_chars < 10:
        return "newcomer"
    main_frac = main_count / total_chars if total_chars > 0 else 0.0
    supporting_frac = supporting_count / total_chars if total_chars > 0 else 0.0
    if main_frac >= 0.4 and total_chars >= 20:
        return "lead_specialist"
    if 0.15 <= main_frac <= 0.4 and total_chars >= 30:
        return "versatile"
    if supporting_frac >= 0.6 and total_chars >= 20:
        return "ensemble"
    return "newcomer"


def compute_character_diversity(
    va_credits: list[CharacterVoiceActor],
    anime_map: dict[str, Anime],
    character_map: dict[str, Character],
    person_gender: dict[str, str] | None = None,
) -> dict[str, CharacterDiversityMetrics]:
    """Compute character diversity metrics for all VAs.

    CDI = 0.35*norm(genre_entropy) + 0.25*norm(gender_range)
        + 0.25*norm(role_balance) + 0.15*norm(log(1+unique_chars))

    Args:
        va_credits: all character_voice_actor records
        anime_map: anime_id -> Anime
        character_map: character_id -> Character
        person_gender: person_id -> gender string (optional)

    Returns:
        person_id -> CharacterDiversityMetrics
    """
    person_gender = person_gender or {}

    # Build per-VA data
    va_genres: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    va_roles: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    va_chars: dict[str, set[str]] = defaultdict(set)
    va_char_genders: dict[str, list[str]] = defaultdict(list)

    for cva in va_credits:
        pid = cva.person_id
        va_chars[pid].add(cva.character_id)

        # Role counts
        role_upper = cva.character_role.upper()
        if role_upper in ("MAIN", "SUPPORTING", "BACKGROUND"):
            va_roles[pid][role_upper] += 1
        else:
            va_roles[pid]["BACKGROUND"] += 1

        # Genre counts from anime
        anime = anime_map.get(cva.anime_id)
        if anime and anime.genres:
            for genre in anime.genres:
                va_genres[pid][genre] += 1

        # Character gender
        char = character_map.get(cva.character_id)
        if char and char.gender:
            va_char_genders[pid].append(char.gender)

    all_pids = set(va_chars.keys())
    if not all_pids:
        return {}

    # Compute raw metrics per VA
    raw_metrics: dict[str, dict[str, float]] = {}
    for pid in all_pids:
        # Genre entropy
        genre_counts = list(va_genres[pid].values())
        genre_ent = _shannon_entropy(genre_counts) if genre_counts else 0.0

        # Role balance entropy
        role_counts = [
            va_roles[pid].get("MAIN", 0),
            va_roles[pid].get("SUPPORTING", 0),
            va_roles[pid].get("BACKGROUND", 0),
        ]
        role_ent = _shannon_entropy(role_counts)

        # Gender range: fraction of characters with different gender from VA
        va_gender = person_gender.get(pid, "")
        char_genders = va_char_genders.get(pid, [])
        if va_gender and char_genders:
            diff_gender = sum(1 for g in char_genders if g.lower() != va_gender.lower())
            gender_range = diff_gender / len(char_genders)
        else:
            gender_range = 0.0

        unique_chars = len(va_chars[pid])

        raw_metrics[pid] = {
            "genre_entropy": genre_ent,
            "gender_range": gender_range,
            "role_balance": role_ent,
            "unique_chars": unique_chars,
            "log_chars": math.log1p(unique_chars),
        }

    # Normalize across all VAs
    pids = sorted(raw_metrics.keys())
    genre_ents = [raw_metrics[p]["genre_entropy"] for p in pids]
    gender_ranges = [raw_metrics[p]["gender_range"] for p in pids]
    role_ents = [raw_metrics[p]["role_balance"] for p in pids]
    log_chars = [raw_metrics[p]["log_chars"] for p in pids]

    norm_ge = _normalize_0_1(genre_ents)
    norm_gr = _normalize_0_1(gender_ranges)
    norm_rb = _normalize_0_1(role_ents)
    norm_lc = _normalize_0_1(log_chars)

    # Build results
    results: dict[str, CharacterDiversityMetrics] = {}
    for i, pid in enumerate(pids):
        main_c = va_roles[pid].get("MAIN", 0)
        supp_c = va_roles[pid].get("SUPPORTING", 0)
        bg_c = va_roles[pid].get("BACKGROUND", 0)
        total = main_c + supp_c + bg_c

        cdi = (
            0.35 * norm_ge[i]
            + 0.25 * norm_gr[i]
            + 0.25 * norm_rb[i]
            + 0.15 * norm_lc[i]
        )

        results[pid] = CharacterDiversityMetrics(
            person_id=pid,
            genre_entropy=raw_metrics[pid]["genre_entropy"],
            gender_range=raw_metrics[pid]["gender_range"],
            role_balance=raw_metrics[pid]["role_balance"],
            unique_characters=int(raw_metrics[pid]["unique_chars"]),
            cdi=cdi,
            casting_tier=_classify_casting_tier(main_c, supp_c, total),
            main_count=main_c,
            supporting_count=supp_c,
            background_count=bg_c,
        )

    logger.info("va_character_diversity_computed", persons=len(results))
    return results
