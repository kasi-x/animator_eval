"""Phase 6B: VA Supplementary Metrics — diversity, synergy, replacement difficulty."""

import structlog

from src.analysis.va.character_diversity import compute_character_diversity
from src.analysis.va.ensemble_synergy import compute_va_ensemble_synergy
from src.analysis.va.replacement_difficulty import compute_replacement_difficulty
from src.pipeline_phases.pipeline_types import VAScoresResult

logger = structlog.get_logger()


def compute_va_supplementary_metrics_phase(
    va_credits: list,
    characters: list,
    character_map: dict,
    anime_map: dict,
    persons: list,
) -> VAScoresResult:
    """Compute VA supplementary metrics.

    Args:
        va_credits: VA credit records
        characters: Character records
        character_map: Character metadata lookup
        anime_map: Anime metadata lookup
        persons: Person records (for gender info)

    Returns:
        VAScoresResult with diversity, synergy, and replacement difficulty.
    """
    result = VAScoresResult()

    if not va_credits:
        logger.info("va_supplementary_skipped", reason="no_va_credits")
        return result

    # Character diversity
    person_gender = {p.id: p.gender for p in persons if p.gender}
    diversity = compute_character_diversity(
        va_credits, anime_map, character_map, person_gender=person_gender
    )
    result.va_character_diversity = diversity

    # Ensemble synergy
    result.va_ensemble_synergy = compute_va_ensemble_synergy(
        va_credits, anime_map, min_shared=3
    )

    # Replacement difficulty
    casting_tiers = {pid: m.casting_tier for pid, m in diversity.items()}
    rdi = compute_replacement_difficulty(
        va_credits, anime_map, casting_tiers=casting_tiers
    )
    result.va_replacement_difficulty = {pid: rd.rdi for pid, rd in rdi.items()}

    logger.info(
        "va_supplementary_complete",
        diversity=len(result.va_character_diversity),
        synergy_pairs=len(result.va_ensemble_synergy)
        if isinstance(result.va_ensemble_synergy, list)
        else 0,
        rdi=len(result.va_replacement_difficulty),
    )
    return result
