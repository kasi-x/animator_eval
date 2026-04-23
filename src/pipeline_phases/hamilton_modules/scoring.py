"""Phase 5: Core Scoring nodes for Hamilton DAG (H-4).

H-4 pattern: nodes take EntityResolutionResult / GraphsResult typed bags instead of
ctx: PipelineContext.  ctx_core_populated bridges the results back to ctx for
Phase 9 (analysis_modules) and Phase 10 (export_and_viz) compatibility.
"""

from __future__ import annotations

import datetime
import math
from typing import Any

from hamilton.function_modifiers import tag

from src.pipeline_phases.pipeline_types import CoreScoresResult, EntityResolutionResult, GraphsResult

NODE_NAMES: list[str] = [
    "akm_estimation",
    "bipartite_enhanced",
    "birank_computation",
    "knowledge_spanners_computation",
    "patronage_premium_computation",
    "dormancy_penalty_computation",
    "birank_rescaled",
    "integrated_value_computation",
    "core_scores",
    "ctx_core_populated",
]

_BIRANK_REF_POPULATION = 10000


@tag(stage="phase5", cost="expensive", domain="scoring")
def akm_estimation(entity_resolved: EntityResolutionResult, graphs_result: GraphsResult) -> dict:
    """AKM person/studio fixed effects estimation.

    Returns dict: akm_result, person_fe, studio_fe, studio_assignments.
    Depends on graphs_result to enforce post-entity-resolution execution order.
    """
    from src.analysis.scoring.akm import estimate_akm, infer_studio_assignment

    akm_result = estimate_akm(entity_resolved.resolved_credits, entity_resolved.anime_map)
    studio_assignments = akm_result.studio_assignments or infer_studio_assignment(
        entity_resolved.resolved_credits, entity_resolved.anime_map
    )
    return {
        "akm_result": akm_result,
        "person_fe": akm_result.person_fe,
        "studio_fe": akm_result.studio_fe,
        "studio_assignments": studio_assignments,
    }


@tag(stage="phase5", cost="moderate", domain="scoring")
def bipartite_enhanced(graphs_result: GraphsResult, akm_estimation: dict) -> dict:
    """Enhance bipartite graph edge weights using AKM person FE scores (in-place mutation).

    Returns quality_calibration dict.
    Mutates graphs_result.person_anime_graph in-place — same object as ctx.person_anime_graph.
    """
    from src.analysis.graph import enhance_bipartite_quality

    enhance_bipartite_quality(
        graphs_result.person_anime_graph, akm_estimation["person_fe"], role_damping=None
    )
    return graphs_result.person_anime_graph.graph.get("_quality_calibration", {})


@tag(stage="phase5", cost="expensive", domain="scoring")
def birank_computation(graphs_result: GraphsResult, bipartite_enhanced: dict) -> dict:
    """BiRank (bipartite PageRank) computation on the quality-enhanced graph.

    Returns dict: birank_result, person_scores (probability space), anime_scores (probability space).
    Takes bipartite_enhanced to ensure graph quality enhancement ran first.
    """
    from src.analysis.scoring.birank import compute_birank

    birank_result = compute_birank(graphs_result.person_anime_graph)
    return {
        "birank_result": birank_result,
        "person_scores": birank_result.person_scores,
        "anime_scores": birank_result.anime_scores,
    }


@tag(stage="phase5", cost="moderate", domain="scoring")
def knowledge_spanners_computation(graphs_result: GraphsResult, birank_computation: dict) -> dict:
    """Knowledge spanner scores: AWCC (cross-community connections) + NDI (diversity index).

    Returns {pid: KnowledgeSpannerMetrics}.
    betweenness_cache is empty at Phase 5 (populated later in Phase 6 centrality_metrics).
    """
    from src.analysis.network.knowledge_spanners import compute_knowledge_spanners

    return compute_knowledge_spanners(
        graphs_result.collaboration_graph,
        graphs_result.community_map,
        betweenness_cache={},
    )


@tag(stage="phase5", cost="cheap", domain="scoring")
def patronage_premium_computation(
    entity_resolved: EntityResolutionResult, birank_computation: dict
) -> dict:
    """Patronage premium: director BiRank scores weight animators they repeatedly hire.

    Returns {pid: float}.
    Uses probability-space birank (pre-rescaling) consistent with the original ordering.
    """
    from src.analysis.scoring.patronage_dormancy import compute_patronage_premium

    return compute_patronage_premium(
        entity_resolved.resolved_credits,
        entity_resolved.anime_map,
        birank_computation["person_scores"],
    )


@tag(stage="phase5", cost="cheap", domain="scoring")
def dormancy_penalty_computation(
    entity_resolved: EntityResolutionResult, patronage_premium_computation: dict
) -> dict:
    """Dormancy penalty: reduces scores for persons inactive in recent years.

    Returns {pid: float} in [0, 1].
    Takes patronage_premium_computation for sequential ordering within Phase 5.
    """
    from src.analysis.scoring.patronage_dormancy import compute_dormancy_penalty

    return compute_dormancy_penalty(
        entity_resolved.resolved_credits,
        entity_resolved.anime_map,
        current_year=datetime.datetime.now().year,
    )


@tag(stage="phase5", cost="cheap", domain="scoring")
def birank_rescaled(birank_computation: dict, dormancy_penalty_computation: dict) -> dict:
    """Rescale BiRank from probability space to log expected-count space.

    Step 1: ×N_ref=10000 (→ expected-count space, mean≈1).
    Step 2: log1p(x) (→ compress power-law tail).

    Returns dict: person_scores (log space), anime_scores (log space).
    Takes dormancy_penalty_computation for ordering — birank_rescaled runs after dormancy.
    """
    person_scores = birank_computation["person_scores"]
    anime_scores = birank_computation["anime_scores"]
    if person_scores:
        person_scores = {
            pid: math.log1p(s * _BIRANK_REF_POPULATION) for pid, s in person_scores.items()
        }
        anime_scores = {
            aid: math.log1p(s * _BIRANK_REF_POPULATION) for aid, s in anime_scores.items()
        }
    return {"person_scores": person_scores, "anime_scores": anime_scores}


@tag(stage="phase5", cost="moderate", domain="scoring")
def integrated_value_computation(
    entity_resolved: EntityResolutionResult,
    akm_estimation: dict,
    birank_rescaled: dict,
    knowledge_spanners_computation: dict,
    patronage_premium_computation: dict,
    dormancy_penalty_computation: dict,
) -> dict:
    """Integrated Value: PCA-weighted combination of all 5 scoring components.

    IV_i = (λ1·θ_i + λ2·birank_i + λ3·studio_i + λ4·awcc_i + λ5·patronage_i) × D_i

    λ are fixed prior weights (no anime.score optimization).
    D is dormancy multiplier.

    Computes iv_scores_historical (no dormancy) and iv_scores (with dormancy).

    Returns dict: iv_scores, iv_scores_historical, iv_lambda_weights, iv_component_std,
                  iv_component_mean, pca_variance_explained.
    """
    from src.analysis.scoring.integrated_value import (
        compute_integrated_value,
        compute_integrated_value_full,
        compute_studio_exposure,
    )

    studio_exposure = compute_studio_exposure(
        akm_estimation["person_fe"],
        akm_estimation["studio_fe"],
        studio_assignments=akm_estimation["studio_assignments"],
    )
    awcc_scores = {pid: m.awcc for pid, m in knowledge_spanners_computation.items()}
    no_dormancy = {pid: 1.0 for pid in dormancy_penalty_computation}

    iv_result = compute_integrated_value_full(
        akm_estimation["person_fe"],
        birank_rescaled["person_scores"],
        studio_exposure,
        awcc_scores,
        patronage_premium_computation,
        no_dormancy,
        entity_resolved.resolved_credits,
        entity_resolved.anime_map,
    )

    iv_scores = compute_integrated_value(
        akm_estimation["person_fe"],
        birank_rescaled["person_scores"],
        studio_exposure,
        awcc_scores,
        patronage_premium_computation,
        dormancy_penalty_computation,
        iv_result.lambda_weights,
        component_std=iv_result.component_std,
        component_mean=iv_result.component_mean,
    )

    return {
        "iv_scores": iv_scores,
        "iv_scores_historical": iv_result.iv_scores,
        "iv_lambda_weights": iv_result.lambda_weights,
        "iv_component_std": iv_result.component_std,
        "iv_component_mean": iv_result.component_mean,
        "pca_variance_explained": iv_result.pca_variance_explained,
    }


@tag(stage="phase5", cost="cheap", domain="scoring")
def core_scores(
    akm_estimation: dict,
    bipartite_enhanced: dict,
    birank_computation: dict,
    knowledge_spanners_computation: dict,
    patronage_premium_computation: dict,
    dormancy_penalty_computation: dict,
    birank_rescaled: dict,
    integrated_value_computation: dict,
    graphs_result: GraphsResult,
) -> CoreScoresResult:
    """Aggregate all Phase 5 outputs into a CoreScoresResult typed bag."""
    return CoreScoresResult(
        akm_result=akm_estimation["akm_result"],
        person_fe=akm_estimation["person_fe"],
        studio_fe=akm_estimation["studio_fe"],
        studio_assignments=akm_estimation["studio_assignments"],
        quality_calibration=bipartite_enhanced,
        birank_result=birank_computation["birank_result"],
        birank_person_scores=birank_rescaled["person_scores"],
        birank_anime_scores=birank_rescaled["anime_scores"],
        community_map=graphs_result.community_map,
        knowledge_spanner_scores=knowledge_spanners_computation,
        patronage_scores=patronage_premium_computation,
        dormancy_scores=dormancy_penalty_computation,
        iv_scores=integrated_value_computation["iv_scores"],
        iv_scores_historical=integrated_value_computation["iv_scores_historical"],
        iv_lambda_weights=integrated_value_computation["iv_lambda_weights"],
        iv_component_std=integrated_value_computation["iv_component_std"],
        iv_component_mean=integrated_value_computation["iv_component_mean"],
        pca_variance_explained=integrated_value_computation["pca_variance_explained"],
    )


@tag(stage="phase5", cost="cheap", domain="scoring")
def ctx_core_populated(core_scores: CoreScoresResult, ctx: Any) -> CoreScoresResult:
    """H-4 bridge: copy CoreScoresResult fields to ctx for Phase 9/10 compatibility.

    Returns the same CoreScoresResult for typed downstream access.
    """
    ctx.akm_result = core_scores.akm_result
    ctx.person_fe = core_scores.person_fe
    ctx.studio_fe = core_scores.studio_fe
    ctx.studio_assignments = core_scores.studio_assignments
    ctx.quality_calibration = core_scores.quality_calibration
    ctx.birank_result = core_scores.birank_result
    ctx.birank_person_scores = core_scores.birank_person_scores
    ctx.birank_anime_scores = core_scores.birank_anime_scores
    ctx.community_map = core_scores.community_map
    ctx.knowledge_spanner_scores = core_scores.knowledge_spanner_scores
    ctx.patronage_scores = core_scores.patronage_scores
    ctx.dormancy_scores = core_scores.dormancy_scores
    ctx.iv_scores = core_scores.iv_scores
    ctx.iv_scores_historical = core_scores.iv_scores_historical
    ctx.iv_lambda_weights = core_scores.iv_lambda_weights
    ctx.iv_component_std = core_scores.iv_component_std
    ctx.iv_component_mean = core_scores.iv_component_mean
    ctx.pca_variance_explained = core_scores.pca_variance_explained
    return core_scores
