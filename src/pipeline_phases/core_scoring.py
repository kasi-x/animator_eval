"""Phase 5: Core Scoring — 8-component structural estimation framework.

Components:
1. AKM fixed effects (person FE θ_i + studio FE ψ_j)
2. BiRank (bipartite PageRank)
3. Knowledge Spanners (AWCC + NDI)
4. Patronage Premium
5. Dormancy Penalty
6. Integrated Value (CV-optimized weighted combination)
"""

import structlog

from src.analysis.akm import estimate_akm, infer_studio_assignment
from src.analysis.birank import compute_birank
from src.analysis.integrated_value import (
    compute_integrated_value_full,
    compute_studio_exposure,
)
from src.analysis.knowledge_spanners import compute_knowledge_spanners
from src.analysis.patronage_dormancy import (
    compute_dormancy_penalty,
    compute_patronage_premium,
)
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def compute_core_scores_phase(context: PipelineContext) -> None:
    """Compute 8-component structural scores.

    Args:
        context: Pipeline context

    Updates context fields:
        - akm_result, person_fe, studio_fe, studio_assignments
        - birank_result, birank_person_scores, birank_anime_scores
        - knowledge_spanner_scores
        - patronage_scores, dormancy_scores
        - iv_scores, iv_lambda_weights
    """
    # 1. AKM estimation (person FE + studio FE)
    logger.info("step_start", step="akm_estimation")
    with context.monitor.measure("akm_estimation"):
        context.akm_result = estimate_akm(context.credits, context.anime_map)
        context.person_fe = context.akm_result.person_fe
        context.studio_fe = context.akm_result.studio_fe
        context.studio_assignments = infer_studio_assignment(
            context.credits, context.anime_map
        )
    context.monitor.increment_counter("akm_persons", len(context.person_fe))
    context.monitor.increment_counter("akm_studios", len(context.studio_fe))

    # 2. BiRank (bipartite PageRank)
    logger.info("step_start", step="birank")
    with context.monitor.measure("birank"):
        context.birank_result = compute_birank(context.person_anime_graph)
        # Keep probability-space scores for patronage computation (step 4).
        # Rescaling to expected-count space happens after patronage, before IV.
        context.birank_person_scores = context.birank_result.person_scores
        context.birank_anime_scores = context.birank_result.anime_scores
    context.monitor.increment_counter(
        "birank_persons", len(context.birank_person_scores)
    )

    # 3. Knowledge Spanners (AWCC + NDI)
    logger.info("step_start", step="knowledge_spanners")
    with context.monitor.measure("knowledge_spanners"):
        context.knowledge_spanner_scores = compute_knowledge_spanners(
            context.collaboration_graph,
            context.community_map,
            betweenness_cache=context.betweenness_cache,
        )
    context.monitor.increment_counter(
        "knowledge_spanners", len(context.knowledge_spanner_scores)
    )

    # 4. Patronage Premium
    logger.info("step_start", step="patronage_premium")
    with context.monitor.measure("patronage_premium"):
        # Use BiRank scores for directors
        context.patronage_scores = compute_patronage_premium(
            context.credits, context.anime_map, context.birank_person_scores
        )
    context.monitor.increment_counter("patronage_persons", len(context.patronage_scores))

    # 5. Dormancy Penalty
    logger.info("step_start", step="dormancy_penalty")
    with context.monitor.measure("dormancy_penalty"):
        context.dormancy_scores = compute_dormancy_penalty(
            context.credits, context.anime_map, current_year=context.current_year
        )
    context.monitor.increment_counter("dormancy_persons", len(context.dormancy_scores))

    # 5b. Rescale BiRank from probability space (sum=1) to expected-count space
    # (mean=1).  Done AFTER patronage (which uses raw probability-space BiRank
    # as director weights in Π=Σ PR_d·log(1+N)), but BEFORE IV computation.
    # Raw BiRank ~1/N per person (~1.7e-5 for 58K).  Other IV components live
    # on [-6, +10] scales.  Without rescaling, BiRank's max normalized value
    # is ~0.07 vs ~6 for person_fe → 15% lambda weight contributes ~0% of IV
    # variance.  ×N gives mean=1.0 and std≈2, comparable to other components.
    n_birank = len(context.birank_person_scores)
    if n_birank > 0:
        context.birank_person_scores = {
            pid: score * n_birank
            for pid, score in context.birank_person_scores.items()
        }
        context.birank_anime_scores = {
            aid: score * len(context.birank_anime_scores)
            for aid, score in context.birank_anime_scores.items()
        }
        logger.info(
            "birank_rescaled_to_expected_count",
            n_persons=n_birank,
            max_score=round(max(context.birank_person_scores.values()), 4),
            mean_score=round(
                sum(context.birank_person_scores.values()) / n_birank, 4
            ),
        )

    # 6. Integrated Value with CV weight optimization
    # First compute iv_scores_historical (dormancy=1.0 for all)
    logger.info("step_start", step="integrated_value")
    with context.monitor.measure("integrated_value"):
        studio_exposure = compute_studio_exposure(
            context.person_fe,
            context.studio_fe,
            studio_assignments=context.studio_assignments,
        )
        awcc_scores = {
            pid: m.awcc
            for pid, m in context.knowledge_spanner_scores.items()
        }
        # Historical IV: no dormancy applied (dormancy=1.0 for everyone)
        no_dormancy = {pid: 1.0 for pid in context.dormancy_scores}
        iv_result = compute_integrated_value_full(
            context.person_fe,
            context.birank_person_scores,
            studio_exposure,
            awcc_scores,
            context.patronage_scores,
            no_dormancy,
            context.credits,
            context.anime_map,
        )
        context.iv_scores_historical = iv_result.iv_scores
        context.iv_lambda_weights = iv_result.lambda_weights
        iv_component_std = iv_result.component_std
        iv_component_mean = iv_result.component_mean
        # Store for Phase 6 reuse (fix B02)
        context.iv_component_std = iv_component_std
        context.iv_component_mean = iv_component_mean

        # Current IV: apply raw dormancy (career-aware dormancy applied in Phase 6)
        from src.analysis.integrated_value import compute_integrated_value

        context.iv_scores = compute_integrated_value(
            context.person_fe,
            context.birank_person_scores,
            studio_exposure,
            awcc_scores,
            context.patronage_scores,
            context.dormancy_scores,
            context.iv_lambda_weights,
            component_std=iv_component_std,
            component_mean=iv_component_mean,
        )
    context.monitor.increment_counter("iv_persons", len(context.iv_scores))

    context.monitor.record_memory("after_scoring")
