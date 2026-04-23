"""Phase 5: Core Scoring nodes for Hamilton DAG (H-2).

Each function is a Hamilton node covering one sub-step of core_scoring.py.
All nodes take ctx: PipelineContext (H-1/H-2 pattern); H-4 will replace ctx
with explicit typed inputs.

Ordering is enforced via chained parameters: each node takes the output of the
node that must complete before it, so Hamilton builds the correct execution order.
"""

from __future__ import annotations

import math
from typing import Any

from hamilton.function_modifiers import tag

from src.pipeline_phases.context import PipelineContext

NODE_NAMES: list[str] = [
    "akm_estimation",
    "bipartite_enhanced",
    "birank_computation",
    "knowledge_spanners_computation",
    "patronage_premium_computation",
    "dormancy_penalty_computation",
    "birank_rescaled",
    "integrated_value_computation",
]

_BIRANK_REF_POPULATION = 10000


@tag(stage="phase5", cost="expensive", domain="scoring")
def akm_estimation(ctx: PipelineContext, graphs_built: Any) -> Any:
    """AKM person/studio fixed effects estimation.

    Writes: ctx.akm_result, ctx.person_fe, ctx.studio_fe, ctx.studio_assignments.
    Depends on graphs_built to enforce post-entity-resolution execution order.
    """
    from src.analysis.scoring.akm import estimate_akm, infer_studio_assignment

    ctx.akm_result = estimate_akm(ctx.credits, ctx.anime_map)
    ctx.person_fe = ctx.akm_result.person_fe
    ctx.studio_fe = ctx.akm_result.studio_fe
    ctx.studio_assignments = ctx.akm_result.studio_assignments or infer_studio_assignment(
        ctx.credits, ctx.anime_map
    )
    return ctx.akm_result


@tag(stage="phase5", cost="moderate", domain="scoring")
def bipartite_enhanced(ctx: PipelineContext, akm_estimation: Any) -> Any:
    """Enhance bipartite graph edge weights using AKM person FE scores.

    Writes: ctx.quality_calibration.
    Depends on akm_estimation to enforce post-AKM execution.
    """
    from src.analysis.graph import enhance_bipartite_quality

    enhance_bipartite_quality(ctx.person_anime_graph, ctx.person_fe, role_damping=None)
    ctx.quality_calibration = ctx.person_anime_graph.graph.get("_quality_calibration", {})
    return ctx.quality_calibration


@tag(stage="phase5", cost="expensive", domain="scoring")
def birank_computation(ctx: PipelineContext, bipartite_enhanced: Any) -> Any:
    """BiRank (bipartite PageRank) computation.

    Writes: ctx.birank_result, ctx.birank_person_scores (probability space),
            ctx.birank_anime_scores (probability space).
    Depends on bipartite_enhanced to run after graph quality enhancement.
    """
    from src.analysis.scoring.birank import compute_birank

    ctx.birank_result = compute_birank(ctx.person_anime_graph)
    ctx.birank_person_scores = ctx.birank_result.person_scores
    ctx.birank_anime_scores = ctx.birank_result.anime_scores
    return ctx.birank_result


@tag(stage="phase5", cost="moderate", domain="scoring")
def knowledge_spanners_computation(ctx: PipelineContext, birank_computation: Any) -> Any:
    """Knowledge spanner scores: AWCC (cross-community connections) + NDI (diversity index).

    Writes: ctx.knowledge_spanner_scores.
    Depends on birank_computation to ensure birank scores exist in ctx.
    """
    from src.analysis.network.knowledge_spanners import compute_knowledge_spanners

    ctx.knowledge_spanner_scores = compute_knowledge_spanners(
        ctx.collaboration_graph,
        ctx.community_map,
        betweenness_cache=ctx.betweenness_cache,
    )
    return ctx.knowledge_spanner_scores


@tag(stage="phase5", cost="cheap", domain="scoring")
def patronage_premium_computation(ctx: PipelineContext, birank_computation: Any) -> Any:
    """Patronage premium: director BiRank scores weight animators they repeatedly hire.

    Writes: ctx.patronage_scores.
    Depends on birank_computation (uses director BiRank as patronage weights).
    """
    from src.analysis.scoring.patronage_dormancy import compute_patronage_premium

    ctx.patronage_scores = compute_patronage_premium(
        ctx.credits, ctx.anime_map, ctx.birank_person_scores
    )
    return ctx.patronage_scores


@tag(stage="phase5", cost="cheap", domain="scoring")
def dormancy_penalty_computation(ctx: PipelineContext, patronage_premium_computation: Any) -> Any:
    """Dormancy penalty: reduces scores for persons inactive in recent years.

    Writes: ctx.dormancy_scores.
    Depends on patronage_premium_computation for sequential ordering in Phase 5.
    """
    from src.analysis.scoring.patronage_dormancy import compute_dormancy_penalty

    ctx.dormancy_scores = compute_dormancy_penalty(
        ctx.credits, ctx.anime_map, current_year=ctx.current_year
    )
    return ctx.dormancy_scores


@tag(stage="phase5", cost="cheap", domain="scoring")
def birank_rescaled(ctx: PipelineContext, dormancy_penalty_computation: Any) -> Any:
    """Rescale BiRank from probability space to log expected-count space.

    Step 1: ×N_ref (→ expected-count space, mean≈1).
    Step 2: log(1+x) (→ compress power-law tail).

    Uses fixed N_ref=10000 for dataset-size-independent comparability.

    Writes: ctx.birank_person_scores (log space), ctx.birank_anime_scores (log space).
    """
    n_birank = len(ctx.birank_person_scores)
    if n_birank > 0:
        ctx.birank_person_scores = {
            pid: math.log1p(score * _BIRANK_REF_POPULATION)
            for pid, score in ctx.birank_person_scores.items()
        }
        ctx.birank_anime_scores = {
            aid: math.log1p(score * _BIRANK_REF_POPULATION)
            for aid, score in ctx.birank_anime_scores.items()
        }
    return ctx.birank_person_scores


@tag(stage="phase5", cost="moderate", domain="scoring")
def integrated_value_computation(
    ctx: PipelineContext,
    birank_rescaled: Any,
    knowledge_spanners_computation: Any,
    patronage_premium_computation: Any,
    dormancy_penalty_computation: Any,
) -> Any:
    """Integrated Value: PCA-weighted combination of all 5 scoring components.

    IV_i = (λ1·θ_i + λ2·birank_i + λ3·studio_i + λ4·awcc_i + λ5·patronage_i) × D_i

    λ are fixed prior weights (no anime.score optimization).
    D is dormancy multiplier.

    Computes iv_scores_historical (no dormancy) and iv_scores (with dormancy).

    Writes: ctx.iv_scores_historical, ctx.iv_lambda_weights, ctx.iv_component_std,
            ctx.iv_component_mean, ctx.pca_variance_explained, ctx.iv_scores.
    """
    from src.analysis.scoring.integrated_value import (
        compute_integrated_value,
        compute_integrated_value_full,
        compute_studio_exposure,
    )

    studio_exposure = compute_studio_exposure(
        ctx.person_fe,
        ctx.studio_fe,
        studio_assignments=ctx.studio_assignments,
    )
    awcc_scores = {pid: m.awcc for pid, m in ctx.knowledge_spanner_scores.items()}
    no_dormancy = {pid: 1.0 for pid in ctx.dormancy_scores}

    iv_result = compute_integrated_value_full(
        ctx.person_fe,
        ctx.birank_person_scores,
        studio_exposure,
        awcc_scores,
        ctx.patronage_scores,
        no_dormancy,
        ctx.credits,
        ctx.anime_map,
    )
    ctx.iv_scores_historical = iv_result.iv_scores
    ctx.iv_lambda_weights = iv_result.lambda_weights
    ctx.iv_component_std = iv_result.component_std
    ctx.iv_component_mean = iv_result.component_mean
    ctx.pca_variance_explained = iv_result.pca_variance_explained

    ctx.iv_scores = compute_integrated_value(
        ctx.person_fe,
        ctx.birank_person_scores,
        studio_exposure,
        awcc_scores,
        ctx.patronage_scores,
        ctx.dormancy_scores,
        ctx.iv_lambda_weights,
        component_std=ctx.iv_component_std,
        component_mean=ctx.iv_component_mean,
    )
    return ctx.iv_scores
