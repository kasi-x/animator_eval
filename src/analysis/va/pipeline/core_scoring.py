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
from src.analysis.va.trust import compute_va_patronage, compute_va_trust
from src.pipeline_phases.pipeline_types import VAScoresResult
from src.runtime.models import Credit, Role

logger = structlog.get_logger()


def compute_va_core_scores_phase(
    va_credits: list,
    credits: list,
    anime_map: dict,
    va_person_ids: set,
    va_anime_graph,
    birank_person_scores: dict,
    current_year: int,
) -> VAScoresResult:
    """Compute VA core scoring components.

    Args:
        va_credits: VA credit records
        credits: All credits
        anime_map: Anime metadata lookup
        va_person_ids: Set of VA person IDs
        va_anime_graph: VA-anime bipartite graph
        birank_person_scores: Production staff BiRank scores (for sound director patronage)
        current_year: Current year for dormancy computation

    Returns:
        VAScoresResult with all VA score components.
    """
    result = VAScoresResult()

    if not va_credits:
        logger.info("va_core_scoring_skipped", reason="no_va_credits")
        return result

    _run_va_akm(result, va_credits, credits, anime_map)
    _run_va_birank(result, va_anime_graph, va_person_ids)
    _run_va_trust(result, va_credits, credits, anime_map, current_year)
    _run_va_patronage(result, va_credits, credits, anime_map, birank_person_scores)
    _run_va_dormancy(result, va_credits, anime_map, current_year)
    _run_va_awcc_placeholder(result, va_person_ids)
    _run_va_iv(result, va_credits, credits, anime_map)

    logger.info(
        "va_core_scoring_complete",
        va_person_fe=len(result.va_person_fe),
        va_birank=len(result.va_birank_scores),
        va_iv=len(result.va_iv_scores),
    )
    return result


def _run_va_akm(
    result: VAScoresResult,
    va_credits: list,
    credits: list,
    anime_map: dict,
) -> None:
    """Estimate VA AKM (person/sound-director fixed effects)."""
    akm_result = estimate_va_akm(va_credits, credits, anime_map)
    result.va_person_fe = akm_result.person_fe
    result.va_sd_fe = akm_result.sd_fe


def _run_va_birank(
    result: VAScoresResult,
    va_anime_graph,
    va_person_ids: set,
) -> None:
    """Compute weighted BiRank on the VA-anime bipartite graph."""
    if va_anime_graph and va_anime_graph.number_of_edges() > 0:
        birank_result = compute_birank(va_anime_graph)
        result.va_birank_scores = {
            pid: score
            for pid, score in birank_result.person_scores.items()
            if pid in va_person_ids
        }
    else:
        result.va_birank_scores = {}


def _run_va_trust(
    result: VAScoresResult,
    va_credits: list,
    credits: list,
    anime_map: dict,
    current_year: int,
) -> None:
    """Compute VA trust scores from co-credit history."""
    result.va_trust_scores = compute_va_trust(
        va_credits, credits, anime_map, current_year=current_year
    )


def _run_va_patronage(
    result: VAScoresResult,
    va_credits: list,
    credits: list,
    anime_map: dict,
    birank_person_scores: dict,
) -> None:
    """Compute VA patronage scores from sound-director affinity."""
    sd_birank = {
        pid: birank_person_scores.get(pid, 0.0) for pid in result.va_sd_fe
    }
    result.va_patronage_scores = compute_va_patronage(
        va_credits, credits, anime_map, sd_birank
    )


def _run_va_dormancy(
    result: VAScoresResult,
    va_credits: list,
    anime_map: dict,
    current_year: int,
) -> None:
    """Compute dormancy penalty from VA credit timeline."""
    result.va_dormancy_scores = compute_dormancy_penalty(
        _va_credits_to_pseudo_credits(va_credits),
        anime_map,
        current_year=current_year,
    )


def _run_va_awcc_placeholder(
    result: VAScoresResult,
    va_person_ids: set,
) -> None:
    """VA AWCC is not yet implemented; set to 0.0 for all persons."""
    result.va_awcc_scores = {pid: 0.0 for pid in va_person_ids}


def _run_va_iv(
    result: VAScoresResult,
    va_credits: list,
    credits: list,
    anime_map: dict,
) -> None:
    """Combine score components into the VA integrated value."""
    sd_exposure = compute_va_sd_exposure(
        result.va_sd_fe,
        _build_sd_assignments(va_credits, credits, anime_map),
    )
    result.va_iv_scores = compute_va_integrated_value(
        person_fe=result.va_person_fe,
        birank=result.va_birank_scores,
        sd_exposure=sd_exposure,
        awcc=result.va_awcc_scores,
        patronage=result.va_patronage_scores,
        dormancy=result.va_dormancy_scores,
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


def _build_sd_assignments(
    va_credits: list,
    credits: list,
    anime_map: dict,
) -> dict[str, dict[int, str]]:
    """Build VA -> {year -> sound_director_id} mapping from credits."""
    anime_sd: dict[str, str] = {}
    for c in credits:
        if c.role == Role.SOUND_DIRECTOR:
            anime_sd[c.anime_id] = c.person_id

    assignments: dict[str, dict[int, str]] = defaultdict(dict)
    for cva in va_credits:
        anime = anime_map.get(cva.anime_id)
        if anime and anime.year:
            sd = anime_sd.get(cva.anime_id)
            if sd:
                assignments[cva.person_id][anime.year] = sd

    return dict(assignments)
