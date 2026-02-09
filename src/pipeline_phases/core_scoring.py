"""Phase 5: Core Scoring — Authority, Trust, Skill + Normalization."""
import structlog

from src.analysis.normalize import normalize_all_axes
from src.analysis.pagerank import compute_authority_scores
from src.analysis.skill import compute_skill_scores
from src.analysis.trust import compute_trust_scores
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def compute_core_scores_phase(context: PipelineContext) -> None:
    """Compute authority, trust, and skill scores; then normalize to 0-100 scale.

    Args:
        context: Pipeline context

    Updates context fields:
        - authority_scores: Dict[person_id, float] (0-100)
        - trust_scores: Dict[person_id, float] (0-100)
        - skill_scores: Dict[person_id, float] (0-100)
    """
    # Authority (PageRank)
    logger.info("step_start", step="authority_pagerank")
    with context.monitor.measure("authority_pagerank"):
        context.authority_scores = compute_authority_scores(context.person_anime_graph)
    context.monitor.increment_counter("persons_with_authority", len(context.authority_scores))

    # Trust (repeat engagement)
    logger.info("step_start", step="trust_repeat_engagement")
    with context.monitor.measure("trust_scores"):
        context.trust_scores = compute_trust_scores(context.credits, context.anime_map)
    context.monitor.increment_counter("persons_with_trust", len(context.trust_scores))

    # Skill (OpenSkill)
    logger.info("step_start", step="skill_openskill")
    with context.monitor.measure("skill_scores"):
        context.skill_scores = compute_skill_scores(context.credits, context.anime_map)
    context.monitor.increment_counter("persons_with_skill", len(context.skill_scores))

    # Normalization (0-100)
    logger.info("step_start", step="score_normalization")
    with context.monitor.measure("score_normalization"):
        context.authority_scores, context.trust_scores, context.skill_scores = normalize_all_axes(
            context.authority_scores,
            context.trust_scores,
            context.skill_scores,
        )

    context.monitor.record_memory("after_scoring")
