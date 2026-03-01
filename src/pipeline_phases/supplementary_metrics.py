"""Phase 6: Supplementary Metrics — additional scoring dimensions."""

import structlog

from src.analysis.career import batch_career_analysis
from src.analysis.career_friction import estimate_career_friction
from src.analysis.circles import find_director_circles
from src.analysis.era_effects import compute_era_and_difficulty
from src.analysis.graph import (
    calculate_network_centrality_scores,
    determine_primary_role_for_each_person,
)
from src.analysis.growth import compute_growth_trends
from src.analysis.network_density import compute_network_density
from src.analysis.peer_effects import estimate_peer_effects_2sls
from src.analysis.trust import batch_detect_engagement_decay
from src.analysis.versatility import compute_versatility

# Advanced metrics
from src.analysis.growth_acceleration import (
    compute_growth_metrics,
    compute_adjusted_person_fe_with_growth,
)
from src.analysis.anime_value import compute_anime_values
from src.analysis.contribution_attribution import compute_contribution_attribution
from src.analysis.potential_value import compute_potential_value_scores
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def compute_supplementary_metrics_phase(context: PipelineContext) -> None:
    """Compute supplementary metrics beyond the core scores.

    Metrics computed:
    - Engagement decay (directors whose engagement with an animator has declined)
    - Role classification (primary role category per person)
    - Career analysis (career stage, trajectory, peaks)
    - Director circles (frequent collaborator groups)
    - Versatility (breadth of role categories and specific roles)
    - Centrality (betweenness, closeness, degree in collaboration network)
    - Network density (collaborator count, hub score)
    - Growth trends (career trajectory: rising, stable, declining)
    - Peer effects (2SLS estimation)
    - Career friction (observed vs expected transitions)
    - Era effects (year fixed effects, difficulty proxy)

    Args:
        context: Pipeline context

    Updates context fields:
        - decay_results, role_profiles, career_data, circles, versatility
        - centrality, network_density, growth_data
        - peer_effect_result, career_friction, era_effects
        - studio_bias_metrics, growth_acceleration_data, anime_values
        - contribution_data, potential_value_scores
    """
    # Engagement Decay Detection (optimized: batch processing)
    logger.info("step_start", step="engagement_decay")
    with context.monitor.measure("engagement_decay"):
        context.decay_results = batch_detect_engagement_decay(
            context.credits, context.anime_map
        )

    # Role Classification
    logger.info("step_start", step="role_classification")
    with context.monitor.measure("role_classification"):
        context.role_profiles = determine_primary_role_for_each_person(context.credits)

    # Career Analysis
    logger.info("step_start", step="career_analysis")
    with context.monitor.measure("career_analysis"):
        context.career_data = batch_career_analysis(context.credits, context.anime_map)

    # Career-aware dormancy: now that career_data is available, re-compute dormancy
    # to protect veterans from harsh dormancy penalties
    logger.info("step_start", step="career_aware_dormancy")
    with context.monitor.measure("career_aware_dormancy"):
        from src.analysis.patronage_dormancy import compute_career_aware_dormancy
        from src.analysis.integrated_value import compute_integrated_value

        career_dormancy = compute_career_aware_dormancy(
            raw_dormancy=context.dormancy_scores,
            iv_scores_historical=context.iv_scores_historical,
            career_data=context.career_data,
        )
        context.dormancy_scores = career_dormancy

        # Re-compute current IV with career-aware dormancy
        # Fix B01: Use compute_studio_exposure() for consistent year-weighted calculation
        from src.analysis.integrated_value import compute_studio_exposure
        studio_exposure = compute_studio_exposure(
            context.person_fe,
            context.studio_fe,
            studio_assignments=context.studio_assignments,
        )
        awcc_scores = {
            pid: m.awcc
            for pid, m in context.knowledge_spanner_scores.items()
        }
        # Fix B02: Pass component_std and component_mean for consistent normalization
        context.iv_scores = compute_integrated_value(
            context.person_fe,
            context.birank_person_scores,
            studio_exposure,
            awcc_scores,
            context.patronage_scores,
            career_dormancy,
            context.iv_lambda_weights,
            component_std=context.iv_component_std,
            component_mean=context.iv_component_mean,
        )

    # Director Circles
    logger.info("step_start", step="director_circles")
    with context.monitor.measure("director_circles"):
        context.circles = find_director_circles(context.credits, context.anime_map)
    context.monitor.increment_counter("circles_found", len(context.circles))

    # Versatility
    logger.info("step_start", step="versatility")
    with context.monitor.measure("versatility"):
        context.versatility = compute_versatility(context.credits)

    # Centrality Metrics (collaboration graph already built in Phase 4)
    logger.info("step_start", step="centrality_metrics")
    with context.monitor.measure("centrality_metrics"):
        person_ids = {p.id for p in context.persons}
        context.centrality = calculate_network_centrality_scores(
            context.collaboration_graph, person_ids
        )
    # Extract betweenness cache for reuse (avoids duplicate computation in potential_value)
    context.betweenness_cache = {
        pid: metrics["betweenness"]
        for pid, metrics in context.centrality.items()
        if "betweenness" in metrics
    }
    context.monitor.record_memory("after_centrality")

    # Network Density
    logger.info("step_start", step="network_density")
    with context.monitor.measure("network_density"):
        context.network_density = compute_network_density(context.credits)

    # Growth Trends (pre-compute for result entries)
    logger.info("step_start", step="growth_trends_precompute")
    with context.monitor.measure("growth_trends"):
        context.growth_data = compute_growth_trends(context.credits, context.anime_map)

    # ========== New 8-Component Supplementary Metrics ==========

    # Peer Effects Estimation (2SLS)
    logger.info("step_start", step="peer_effects")
    with context.monitor.measure("peer_effects"):
        context.peer_effect_result = estimate_peer_effects_2sls(
            context.credits,
            context.anime_map,
            context.iv_scores,
            context.collaboration_graph,
        )
    logger.info(
        "peer_effects_complete",
        identified=context.peer_effect_result.identified
        if context.peer_effect_result
        else False,
    )

    # Career Friction
    logger.info("step_start", step="career_friction")
    with context.monitor.measure("career_friction"):
        friction_result = estimate_career_friction(
            context.credits,
            context.anime_map,
            person_scores=context.iv_scores,
            studio_fe=context.studio_fe,
        )
        context.career_friction = friction_result.friction_index

    # Era Effects
    logger.info("step_start", step="era_effects")
    with context.monitor.measure("era_effects"):
        context.era_effects = compute_era_and_difficulty(
            context.credits, context.anime_map, context.iv_scores
        )

    # ========== Advanced Metrics (Existing) ==========

    # Build person_scores dict for advanced metrics
    person_scores = {
        pid: {
            "person_fe": context.person_fe.get(pid, 0),
            "birank": context.birank_person_scores.get(pid, 0),
            "patronage": context.patronage_scores.get(pid, 0),
            "iv_score": context.iv_scores.get(pid, 0),
        }
        for pid in set(context.person_fe)
        | set(context.birank_person_scores)
        | set(context.iv_scores)
    }

    # Growth Acceleration
    logger.info("step_start", step="growth_acceleration")
    with context.monitor.measure("growth_acceleration"):
        growth_metrics = compute_growth_metrics(context.credits, context.anime_map)
        adjusted_skills = compute_adjusted_person_fe_with_growth(
            person_scores, growth_metrics, growth_weight=0.3
        )
        context.growth_acceleration_data = {
            "growth_metrics": {pid: vars(m) for pid, m in growth_metrics.items()},
            "adjusted_skills": adjusted_skills,
        }
        logger.info("growth_acceleration_computed", persons=len(growth_metrics))

    # Anime Value Assessment
    logger.info("step_start", step="anime_value_assessment")
    with context.monitor.measure("anime_value_assessment"):
        anime_values = compute_anime_values(
            context.anime_list, context.credits, person_scores
        )
        context.anime_values = {aid: vars(v) for aid, v in anime_values.items()}
        logger.info("anime_values_computed", anime=len(anime_values))

    # Contribution Attribution (sample top 100 anime by value for performance)
    logger.info("step_start", step="contribution_attribution")
    with context.monitor.measure("contribution_attribution"):
        # Sort anime by composite value
        top_anime = sorted(
            anime_values.items(),
            key=lambda x: x[1].composite_value,
            reverse=True,
        )[:100]

        # Pre-index credits by anime for O(1) lookup (PERF-4 optimization)
        from collections import defaultdict

        anime_credits_index: dict[str, list] = defaultdict(list)
        for c in context.credits:
            anime_credits_index[c.anime_id].append(c)

        all_contributions = {}
        for anime_id, anime_value_metrics in top_anime:
            # O(1) lookup instead of O(n) scan (PERF-4 optimization)
            anime_credits = anime_credits_index.get(anime_id, [])
            if anime_credits:
                contributions = compute_contribution_attribution(
                    anime_id,
                    anime_value_metrics.composite_value,
                    anime_credits,
                    person_scores,
                )
                # Convert to dict and handle Enum serialization
                all_contributions[anime_id] = {
                    pid: {**vars(contrib), "role": contrib.role.value}
                    for pid, contrib in contributions.items()
                }

        context.contribution_data = all_contributions
        logger.info("contribution_attribution_computed", anime=len(all_contributions))

    # Potential Value Score (integrates all adjusted scores)
    logger.info("step_start", step="potential_value_scoring")
    with context.monitor.measure("potential_value_scoring"):
        # Prepare inputs — handle case where studio_bias_correction is not run
        debiased_dict = {}
        growth_dict = {
            pid: {
                "growth_velocity": m.growth_velocity,
                "momentum_score": m.momentum_score,
                "career_years": m.career_years,
            }
            for pid, m in growth_metrics.items()
        }

        potential_scores = compute_potential_value_scores(
            person_scores,
            debiased_dict,
            growth_dict,
            adjusted_skills,
            context.collaboration_graph,
            betweenness_cache=context.betweenness_cache,
        )
        # Convert to dict and handle Enum serialization
        context.potential_value_scores = {
            pid: {**vars(p), "category": p.category.value}
            for pid, p in potential_scores.items()
        }
        logger.info("potential_value_computed", persons=len(potential_scores))
