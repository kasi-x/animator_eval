"""Phase 5B: VA Core Scoring — AKM, BiRank, Trust, Patronage, Dormancy, IV."""

import structlog

from src.analysis.birank import compute_birank
from src.analysis.patronage_dormancy import compute_dormancy_penalty
from src.analysis.va_akm import estimate_va_akm
from src.analysis.va_integrated_value import (
    compute_va_integrated_value,
    compute_va_sd_exposure,
)
from src.analysis.va_trust import compute_va_patronage, compute_va_trust
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def compute_va_core_scores_phase(context: PipelineContext) -> None:
    """Compute VA core scoring components.

    Args:
        context: Pipeline context (must have VA graphs built)

    Updates context fields:
        - va_person_fe, va_sd_fe (AKM)
        - va_birank_scores (BiRank)
        - va_trust_scores, va_patronage_scores (Trust/Patronage)
        - va_dormancy_scores (Dormancy)
        - va_awcc_scores (placeholder)
        - va_iv_scores (Integrated Value)
    """
    if not context.va_credits:
        logger.debug("va_scoring_skipped", reason="no_va_credits")
        return

    # 1. VA AKM (person FE + sound director FE)
    logger.info("step_start", step="va_akm_estimation")
    with context.monitor.measure("va_akm"):
        akm_result = estimate_va_akm(
            context.va_credits, context.credits, context.anime_map
        )
        context.va_person_fe = akm_result.person_fe
        context.va_sd_fe = akm_result.sd_fe

    # 2. VA BiRank (on VA-Anime bipartite graph)
    logger.info("step_start", step="va_birank")
    with context.monitor.measure("va_birank"):
        if context.va_anime_graph and context.va_anime_graph.number_of_edges() > 0:
            birank_result = compute_birank(context.va_anime_graph)
            # Only keep VA nodes (bipartite=0)
            context.va_birank_scores = {
                pid: score
                for pid, score in birank_result.person_scores.items()
                if pid in context.va_person_ids
            }
        else:
            context.va_birank_scores = {}

    # 3. VA Trust (sound director repeat casting)
    logger.info("step_start", step="va_trust")
    with context.monitor.measure("va_trust"):
        context.va_trust_scores = compute_va_trust(
            context.va_credits,
            context.credits,
            context.anime_map,
            current_year=context.current_year,
        )

    # 4. VA Patronage (sound director BiRank backing)
    logger.info("step_start", step="va_patronage")
    with context.monitor.measure("va_patronage"):
        # Use production BiRank for sound directors
        sd_birank = {
            pid: context.birank_person_scores.get(pid, 0.0)
            for pid in context.va_sd_fe
        }
        context.va_patronage_scores = compute_va_patronage(
            context.va_credits,
            context.credits,
            context.anime_map,
            sd_birank,
        )

    # 5. VA Dormancy (same mechanism as production staff)
    logger.info("step_start", step="va_dormancy")
    with context.monitor.measure("va_dormancy"):
        # Build VA-specific credit-like records for dormancy
        # Dormancy uses credits' anime years — we convert va_credits to a format it expects
        from src.models import Credit, Role

        va_pseudo_credits = [
            Credit(
                person_id=cva.person_id,
                anime_id=cva.anime_id,
                role=Role.VOICE_ACTOR,
            )
            for cva in context.va_credits
        ]
        context.va_dormancy_scores = compute_dormancy_penalty(
            va_pseudo_credits, context.anime_map, current_year=context.current_year
        )

    # 6. VA AWCC (placeholder — use 0 for now; VA community bridging is less applicable)
    context.va_awcc_scores = {pid: 0.0 for pid in context.va_person_ids}

    # 7. VA Integrated Value
    logger.info("step_start", step="va_integrated_value")
    with context.monitor.measure("va_iv"):
        sd_exposure = compute_va_sd_exposure(
            context.va_sd_fe,
            # Build sd_assignments from VA AKM data
            _build_sd_assignments(context),
        )
        context.va_iv_scores = compute_va_integrated_value(
            person_fe=context.va_person_fe,
            birank=context.va_birank_scores,
            sd_exposure=sd_exposure,
            awcc=context.va_awcc_scores,
            patronage=context.va_patronage_scores,
            dormancy=context.va_dormancy_scores,
        )

    logger.info(
        "va_core_scoring_complete",
        va_person_fe=len(context.va_person_fe),
        va_birank=len(context.va_birank_scores),
        va_iv=len(context.va_iv_scores),
    )


def _build_sd_assignments(
    context: PipelineContext,
) -> dict[str, dict[int, str]]:
    """Build VA -> {year -> sound_director_id} mapping from credits."""
    from collections import defaultdict

    from src.models import Role

    # Sound directors per anime
    anime_sd: dict[str, str] = {}
    for c in context.credits:
        if c.role == Role.SOUND_DIRECTOR:
            anime_sd[c.anime_id] = c.person_id

    # VA -> {year -> sd}
    assignments: dict[str, dict[int, str]] = defaultdict(dict)
    for cva in context.va_credits:
        anime = context.anime_map.get(cva.anime_id)
        if anime and anime.year:
            sd = anime_sd.get(cva.anime_id)
            if sd:
                assignments[cva.person_id][anime.year] = sd

    return dict(assignments)
