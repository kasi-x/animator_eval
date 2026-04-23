"""Phase 5B: VA Core Scoring — AKM, BiRank, Trust, Patronage, Dormancy, IV."""

from collections import defaultdict

import structlog

from src.analysis.scoring.birank import compute_birank
from src.analysis.scoring.patronage_dormancy import compute_dormancy_penalty
from src.analysis.va.akm import estimate_va_akm
from src.analysis.va.integrated_value import (
    compute_va_integrated_value,
    compute_va_sd_exposure,
)
from src.analysis.va.pipeline._common import skip_if_no_va_credits, va_step
from src.analysis.va.trust import compute_va_patronage, compute_va_trust
from src.runtime.models import Credit, Role
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def compute_va_core_scores_phase(context: PipelineContext) -> None:
    """Compute VA core scoring components.

    Updates context fields: va_person_fe, va_sd_fe, va_birank_scores,
    va_trust_scores, va_patronage_scores, va_dormancy_scores, va_awcc_scores,
    va_iv_scores.
    """
    if skip_if_no_va_credits(context, "va_scoring_skipped"):
        return
    _run_va_akm(context)
    _run_va_birank(context)
    _run_va_trust(context)
    _run_va_patronage(context)
    _run_va_dormancy(context)
    _run_va_awcc_placeholder(context)
    _run_va_iv(context)
    logger.info(
        "va_core_scoring_complete",
        va_person_fe=len(context.va_person_fe),
        va_birank=len(context.va_birank_scores),
        va_iv=len(context.va_iv_scores),
    )


def _run_va_akm(context: PipelineContext) -> None:
    with va_step(context, "va_akm"):
        akm_result = estimate_va_akm(
            context.va_credits, context.credits, context.anime_map
        )
        context.va_person_fe = akm_result.person_fe
        context.va_sd_fe = akm_result.sd_fe


def _run_va_birank(context: PipelineContext) -> None:
    with va_step(context, "va_birank"):
        if context.va_anime_graph and context.va_anime_graph.number_of_edges() > 0:
            birank_result = compute_birank(context.va_anime_graph)
            context.va_birank_scores = {
                pid: score
                for pid, score in birank_result.person_scores.items()
                if pid in context.va_person_ids
            }
        else:
            context.va_birank_scores = {}


def _run_va_trust(context: PipelineContext) -> None:
    with va_step(context, "va_trust"):
        context.va_trust_scores = compute_va_trust(
            context.va_credits,
            context.credits,
            context.anime_map,
            current_year=context.current_year,
        )


def _run_va_patronage(context: PipelineContext) -> None:
    with va_step(context, "va_patronage"):
        sd_birank = {
            pid: context.birank_person_scores.get(pid, 0.0) for pid in context.va_sd_fe
        }
        context.va_patronage_scores = compute_va_patronage(
            context.va_credits,
            context.credits,
            context.anime_map,
            sd_birank,
        )


def _run_va_dormancy(context: PipelineContext) -> None:
    with va_step(context, "va_dormancy"):
        context.va_dormancy_scores = compute_dormancy_penalty(
            _va_credits_to_pseudo_credits(context.va_credits),
            context.anime_map,
            current_year=context.current_year,
        )


def _va_credits_to_pseudo_credits(va_credits) -> list[Credit]:
    """Convert VA credit records to Credit objects with Role.VOICE_ACTOR for dormancy computation."""
    return [
        Credit(
            person_id=cva.person_id,
            anime_id=cva.anime_id,
            role=Role.VOICE_ACTOR,
        )
        for cva in va_credits
    ]


def _run_va_awcc_placeholder(context: PipelineContext) -> None:
    """Set VA AWCC scores to 0.0 for all VA persons.

    VA AWCC is structurally 0: voice actors work within a single production unit
    and do not bridge separate communities the way animation staff do.
    """
    context.va_awcc_scores = {pid: 0.0 for pid in context.va_person_ids}


def _run_va_iv(context: PipelineContext) -> None:
    with va_step(context, "va_iv"):
        sd_exposure = compute_va_sd_exposure(
            context.va_sd_fe,
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


def _build_sd_assignments(
    context: PipelineContext,
) -> dict[str, dict[int, str]]:
    """Build VA -> {year -> sound_director_id} mapping from credits."""
    anime_sd: dict[str, str] = {}
    for c in context.credits:
        if c.role == Role.SOUND_DIRECTOR:
            anime_sd[c.anime_id] = c.person_id

    assignments: dict[str, dict[int, str]] = defaultdict(dict)
    for cva in context.va_credits:
        anime = context.anime_map.get(cva.anime_id)
        if anime and anime.year:
            sd = anime_sd.get(cva.anime_id)
            if sd:
                assignments[cva.person_id][anime.year] = sd

    return dict(assignments)
