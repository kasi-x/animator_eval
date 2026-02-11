"""Phase 6: Supplementary Metrics — additional scoring dimensions."""
import structlog

from src.analysis.career import batch_career_analysis
from src.analysis.circles import find_director_circles
from src.analysis.graph import (
    calculate_network_centrality_scores,
    create_person_collaboration_network,
    determine_primary_role_for_each_person,
)
from src.analysis.growth import compute_growth_trends
from src.analysis.network_density import compute_network_density
from src.analysis.trust import batch_detect_engagement_decay
from src.analysis.versatility import compute_versatility
# Advanced metrics
from src.analysis.studio_bias_correction import (
    compute_studio_bias_metrics,
    compute_studio_disparity,
    compute_studio_prestige,
    debias_authority_scores,
)
from src.analysis.growth_acceleration import (
    compute_growth_metrics,
    compute_adjusted_skill_with_growth,
)
from src.analysis.anime_value import compute_anime_values
from src.analysis.contribution_attribution import compute_contribution_attribution
from src.analysis.potential_value import compute_potential_value_scores
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def compute_supplementary_metrics_phase(context: PipelineContext) -> None:
    """Compute supplementary metrics beyond the three core scores.

    Metrics computed:
    - Engagement decay (directors whose engagement with an animator has declined)
    - Role classification (primary role category per person)
    - Career analysis (career stage, trajectory, peaks)
    - Director circles (frequent collaborator groups)
    - Versatility (breadth of role categories and specific roles)
    - Centrality (betweenness, closeness, degree in collaboration network)
    - Network density (collaborator count, hub score)
    - Growth trends (career trajectory: rising, stable, declining)

    Args:
        context: Pipeline context

    Updates context fields:
        - decay_results: Dict[person_id, List[decay_event]]
        - role_profiles: Dict[person_id, role_info]
        - career_data: Dict[person_id, CareerSnapshot]
        - circles: Dict[director_id, DirectorCircle]
        - versatility: Dict[person_id, versatility_metrics]
        - centrality: Dict[person_id, centrality_metrics]
        - network_density: Dict[person_id, density_metrics]
        - growth_data: Dict[person_id, growth_metrics]
        - collaboration_graph: NetworkX graph (side effect)
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

    # Director Circles
    logger.info("step_start", step="director_circles")
    with context.monitor.measure("director_circles"):
        context.circles = find_director_circles(context.credits, context.anime_map)
    context.monitor.increment_counter("circles_found", len(context.circles))

    # Versatility
    logger.info("step_start", step="versatility")
    with context.monitor.measure("versatility"):
        context.versatility = compute_versatility(context.credits)

    # Centrality Metrics (requires collaboration graph)
    logger.info("step_start", step="centrality_metrics")
    with context.monitor.measure("centrality_metrics"):
        context.collaboration_graph = create_person_collaboration_network(
            context.persons, context.credits
        )
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

    # ========== Advanced Metrics (New) ==========

    # Build person_scores dict for advanced metrics
    person_scores = {
        pid: {
            "authority": context.authority_scores.get(pid, 0),
            "trust": context.trust_scores.get(pid, 0),
            "skill": context.skill_scores.get(pid, 0),
            "composite": (
                context.authority_scores.get(pid, 0) * 0.4
                + context.trust_scores.get(pid, 0) * 0.3
                + context.skill_scores.get(pid, 0) * 0.3
            ),
        }
        for pid in set(context.authority_scores) | set(context.trust_scores) | set(context.skill_scores)
    }

    # Studio Bias Correction
    logger.info("step_start", step="studio_bias_correction")
    with context.monitor.measure("studio_bias_correction"):
        bias_metrics = compute_studio_bias_metrics(context.credits, context.anime_map)
        studio_prestige = compute_studio_prestige(context.credits, context.anime_map, person_scores)
        debiased_scores = debias_authority_scores(
            person_scores, bias_metrics, studio_prestige, debias_strength=0.3
        )
        disparity = compute_studio_disparity(
            context.credits, context.anime_map, person_scores
        )
        context.studio_bias_metrics = {
            "bias_metrics": {pid: vars(m) for pid, m in bias_metrics.items()},
            "studio_prestige": studio_prestige,
            "debiased_scores": {pid: vars(d) for pid, d in debiased_scores.items()},
            "studio_disparity": {s: vars(d) for s, d in disparity.items()},
        }
        logger.info(
            "studio_bias_computed",
            persons=len(bias_metrics),
            studios=len(studio_prestige),
            disparity_studios=len(disparity),
        )

    # Growth Acceleration
    logger.info("step_start", step="growth_acceleration")
    with context.monitor.measure("growth_acceleration"):
        growth_metrics = compute_growth_metrics(context.credits, context.anime_map)
        adjusted_skills = compute_adjusted_skill_with_growth(
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
        anime_values = compute_anime_values(context.anime_list, context.credits, person_scores)
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
                    anime_id, anime_value_metrics.composite_value, anime_credits, person_scores
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
        # Prepare inputs
        debiased_dict = {
            pid: {"debiased_authority": d.debiased_authority}
            for pid, d in debiased_scores.items()
        }
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
