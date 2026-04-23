"""Phase 5: Core Scoring — 8-component structural estimation framework.

Components:
1. AKM fixed effects (person FE θ_i + studio FE ψ_j)
2. BiRank (bipartite PageRank)
3. Knowledge Spanners (AWCC + NDI)
4. Patronage Premium
5. Dormancy Penalty
6. Integrated Value (CV-optimized weighted combination)
"""

import math

import structlog

from src.analysis.scoring.akm import estimate_akm, infer_studio_assignment
from src.analysis.scoring.birank import compute_birank
from src.analysis.graph import enhance_bipartite_quality
from src.analysis.scoring.integrated_value import (
    compute_integrated_value_full,
    compute_studio_exposure,
)
from src.analysis.network.knowledge_spanners import compute_knowledge_spanners
from src.analysis.scoring.patronage_dormancy import (
    compute_dormancy_penalty,
    compute_patronage_premium,
)

logger = structlog.get_logger()


def compute_core_scores_phase(context: dict) -> None:
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
        context.studio_assignments = (
            context.akm_result.studio_assignments
            or infer_studio_assignment(context.credits, context.anime_map)
        )
    context.monitor.increment_counter("akm_persons", len(context.person_fe))
    context.monitor.increment_counter("akm_studios", len(context.studio_fe))

    # 1b. Enhance bipartite graph with staff quality signal from AKM
    # Re-weights edges: role^damping × production_scale × staff_quality_boost
    # Parameters (role_damping, blend, top_fraction) are calibrated from data.
    logger.info("step_start", step="bipartite_quality_enhance")
    with context.monitor.measure("bipartite_quality_enhance"):
        enhance_bipartite_quality(
            context.person_anime_graph,
            context.person_fe,
            role_damping=None,  # auto-calibrate from data
        )
        # Retrieve calibration results stored on graph by enhance_bipartite_quality
        context.quality_calibration = context.person_anime_graph.graph.get(
            "_quality_calibration", {}
        )

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
    # D18: BiRank → Patronage → IV → (BiRank is NOT recomputed using IV).
    # The flow is one-directional: BiRank scores directors, patronage uses those
    # director BiRank scores as weights, then IV combines all 5 components.
    # There is no feedback loop because BiRank is computed once (step 2) and
    # never updated with IV scores. The apparent circularity is a pipeline
    # ordering concern, not an actual circular dependency.
    logger.info("step_start", step="patronage_premium")
    with context.monitor.measure("patronage_premium"):
        # Use BiRank scores for directors
        context.patronage_scores = compute_patronage_premium(
            context.credits, context.anime_map, context.birank_person_scores
        )
    context.monitor.increment_counter(
        "patronage_persons", len(context.patronage_scores)
    )

    # 5. Dormancy Penalty
    logger.info("step_start", step="dormancy_penalty")
    with context.monitor.measure("dormancy_penalty"):
        context.dormancy_scores = compute_dormancy_penalty(
            context.credits, context.anime_map, current_year=context.current_year
        )
    context.monitor.increment_counter("dormancy_persons", len(context.dormancy_scores))

    # 5b. Rescale BiRank from probability space (sum=1) to log expected-count
    # space.  Done AFTER patronage (which uses raw probability-space BiRank
    # as director weights in Π=Σ PR_d·log(1+N)), but BEFORE IV computation.
    #
    # Step 1: ×N_ref → expected-count space (mean≈1).
    #   Uses a fixed reference population (N_ref=10000) instead of actual N
    #   so that scores remain comparable across different dataset sizes.
    # Step 2: log(1+x) → compress power-law tail.
    #
    # BiRank follows a power law: a few hub nodes get scores 100-200× the mean.
    # Without log transform, z-score normalization cannot fix this — BiRank's
    # outlier range (~228) dwarfs person_fe's range (~24), making IV ≈ BiRank.
    # log(1+x) maps [0, 228] → [0, 5.4], comparable to person_fe's [-13, 10].
    _BIRANK_REF_POPULATION = 10000  # fixed reference — dataset-size-independent
    n_birank = len(context.birank_person_scores)
    n_anime = len(context.birank_anime_scores)
    if n_birank > 0:
        context.birank_person_scores = {
            pid: math.log1p(score * _BIRANK_REF_POPULATION)
            for pid, score in context.birank_person_scores.items()
        }
        context.birank_anime_scores = {
            aid: math.log1p(score * _BIRANK_REF_POPULATION)
            for aid, score in context.birank_anime_scores.items()
        }
        br_vals = list(context.birank_person_scores.values())
        logger.info(
            "birank_rescaled_log_expected_count",
            n_persons=n_birank,
            n_anime=n_anime,
            ref_population=_BIRANK_REF_POPULATION,
            max_score=round(max(br_vals), 4),
            mean_score=round(sum(br_vals) / n_birank, 4),
            median_score=round(sorted(br_vals)[n_birank // 2], 4),
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
            pid: m.awcc for pid, m in context.knowledge_spanner_scores.items()
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
        context.pca_variance_explained = iv_result.pca_variance_explained

        # Current IV: apply raw dormancy (career-aware dormancy applied in Phase 6)
        from src.analysis.scoring.integrated_value import compute_integrated_value

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
