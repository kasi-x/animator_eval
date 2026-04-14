"""Phase 6B: VA Supplementary Metrics — diversity, synergy, replacement difficulty."""

import structlog

from src.analysis.va.character_diversity import compute_character_diversity
from src.analysis.va.ensemble_synergy import compute_va_ensemble_synergy
from src.analysis.va.replacement_difficulty import compute_replacement_difficulty
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def compute_va_supplementary_metrics_phase(context: PipelineContext) -> None:
    """Compute VA supplementary metrics.

    Args:
        context: Pipeline context (must have VA core scores)

    Updates context fields:
        - va_character_diversity
        - va_ensemble_synergy
        - va_replacement_difficulty
    """
    if not context.va_credits:
        logger.debug("va_supplementary_skipped", reason="no_va_credits")
        return

    # 1. Character Diversity Index
    logger.info("step_start", step="va_character_diversity")
    with context.monitor.measure("va_character_diversity"):
        # Get person genders for gender_range computation
        person_gender = {p.id: p.gender for p in context.persons if p.gender}
        diversity = compute_character_diversity(
            context.va_credits,
            context.anime_map,
            context.character_map,
            person_gender=person_gender,
        )
        context.va_character_diversity = diversity

    # 2. Ensemble Synergy
    logger.info("step_start", step="va_ensemble_synergy")
    with context.monitor.measure("va_ensemble_synergy"):
        synergy = compute_va_ensemble_synergy(
            context.va_credits, context.anime_map, min_shared=3
        )
        context.va_ensemble_synergy = synergy

    # 3. Replacement Difficulty
    logger.info("step_start", step="va_replacement_difficulty")
    with context.monitor.measure("va_replacement_difficulty"):
        # Get casting tiers from diversity results
        casting_tiers = {pid: m.casting_tier for pid, m in diversity.items()}
        rdi = compute_replacement_difficulty(
            context.va_credits,
            context.anime_map,
            casting_tiers=casting_tiers,
        )
        context.va_replacement_difficulty = {pid: rd.rdi for pid, rd in rdi.items()}

    logger.info(
        "va_supplementary_complete",
        diversity=len(context.va_character_diversity),
        synergy_pairs=len(context.va_ensemble_synergy)
        if isinstance(context.va_ensemble_synergy, list)
        else 0,
        rdi=len(context.va_replacement_difficulty),
    )
