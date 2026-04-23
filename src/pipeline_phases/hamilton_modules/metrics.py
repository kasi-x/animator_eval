"""Phase 6: Supplementary Metrics nodes for Hamilton DAG (H-2).

Each function is a Hamilton node for one sub-step of supplementary_metrics.py.
All nodes use ctx: PipelineContext (H-2 pattern); H-4 decomposes to explicit inputs.

Execution order is enforced via chained parameters.
"""

from __future__ import annotations

from typing import Any

from hamilton.function_modifiers import tag

from src.pipeline_phases.context import PipelineContext

NODE_NAMES: list[str] = [
    "engagement_decay",
    "role_classification",
    "career_analysis",
    "career_aware_dormancy_recomputed",
    "director_circles",
    "versatility_computed",
    "centrality_metrics",
    "network_density_computed",
    "growth_trends_precomputed",
    "peer_effects_estimated",
    "career_friction_estimated",
    "era_effects_computed",
    "growth_acceleration_computed",
    "anime_values_computed",
    "contribution_attribution_computed",
    "potential_value_computed",
    "career_tracks_inferred",
]


@tag(stage="phase6", cost="moderate", domain="metrics")
def engagement_decay(ctx: PipelineContext, integrated_value_computation: Any) -> Any:
    """Engagement decay detection: directors whose hiring rate for an animator dropped.

    Writes: ctx.decay_results.
    Depends on integrated_value_computation to run after Phase 5 completes.
    """
    from src.analysis.network.trust import batch_detect_engagement_decay

    ctx.decay_results = batch_detect_engagement_decay(ctx.credits, ctx.anime_map)
    return ctx.decay_results


@tag(stage="phase6", cost="cheap", domain="metrics")
def role_classification(ctx: PipelineContext, engagement_decay: Any) -> Any:
    """Primary role classification per person (animator / director / etc.).

    Writes: ctx.role_profiles.
    """
    from src.analysis.graph import determine_primary_role_for_each_person

    ctx.role_profiles = determine_primary_role_for_each_person(ctx.credits)
    return ctx.role_profiles


@tag(stage="phase6", cost="moderate", domain="metrics")
def career_analysis(ctx: PipelineContext, role_classification: Any) -> Any:
    """Career trajectory analysis: stage, peak, active years.

    Writes: ctx.career_data.
    """
    from src.analysis.career import batch_career_analysis

    ctx.career_data = batch_career_analysis(ctx.credits, ctx.anime_map)
    return ctx.career_data


@tag(stage="phase6", cost="cheap", domain="metrics")
def career_aware_dormancy_recomputed(ctx: PipelineContext, career_analysis: Any) -> Any:
    """Re-compute dormancy and IV with career-aware dormancy (protects veterans).

    Veterans with long track records get softer dormancy penalties than newcomers.

    Writes: ctx.dormancy_scores (updated), ctx.iv_scores (updated).
    """
    from src.analysis.scoring.patronage_dormancy import compute_career_aware_dormancy
    from src.analysis.scoring.integrated_value import compute_integrated_value, compute_studio_exposure

    career_dormancy = compute_career_aware_dormancy(
        raw_dormancy=ctx.dormancy_scores,
        iv_scores_historical=ctx.iv_scores_historical,
        career_data=ctx.career_data,
    )
    ctx.dormancy_scores = career_dormancy

    studio_exposure = compute_studio_exposure(
        ctx.person_fe,
        ctx.studio_fe,
        studio_assignments=ctx.studio_assignments,
    )
    awcc_scores = {pid: m.awcc for pid, m in ctx.knowledge_spanner_scores.items()}
    ctx.iv_scores = compute_integrated_value(
        ctx.person_fe,
        ctx.birank_person_scores,
        studio_exposure,
        awcc_scores,
        ctx.patronage_scores,
        career_dormancy,
        ctx.iv_lambda_weights,
        component_std=ctx.iv_component_std,
        component_mean=ctx.iv_component_mean,
    )
    return ctx.dormancy_scores


@tag(stage="phase6", cost="moderate", domain="metrics")
def director_circles(ctx: PipelineContext, career_aware_dormancy_recomputed: Any) -> Any:
    """Director circles: frequent collaborator groups around each director.

    Writes: ctx.circles.
    """
    from src.analysis.network.circles import find_director_circles

    ctx.circles = find_director_circles(ctx.credits, ctx.anime_map)
    return ctx.circles


@tag(stage="phase6", cost="cheap", domain="metrics")
def versatility_computed(ctx: PipelineContext, director_circles: Any) -> Any:
    """Versatility: breadth of role categories and specific roles per person.

    Writes: ctx.versatility.
    """
    from src.analysis.versatility import compute_versatility

    ctx.versatility = compute_versatility(ctx.credits)
    return ctx.versatility


@tag(stage="phase6", cost="expensive", domain="metrics")
def centrality_metrics(ctx: PipelineContext, versatility_computed: Any) -> Any:
    """Network centrality: betweenness, closeness, degree in collaboration graph.

    Writes: ctx.centrality, ctx.betweenness_cache.
    """
    from src.analysis.graph import calculate_network_centrality_scores

    if ctx.collaboration_graph is not None:
        person_ids = {p.id for p in ctx.persons}
        ctx.centrality = calculate_network_centrality_scores(
            ctx.collaboration_graph, person_ids
        )
    ctx.betweenness_cache = {
        pid: metrics["betweenness"]
        for pid, metrics in ctx.centrality.items()
        if "betweenness" in metrics
    }
    return ctx.centrality


@tag(stage="phase6", cost="cheap", domain="metrics")
def network_density_computed(ctx: PipelineContext, centrality_metrics: Any) -> Any:
    """Network density: collaborator count, unique anime, hub score per person.

    Writes: ctx.network_density.
    """
    from src.analysis.network.network_density import compute_network_density

    ctx.network_density = compute_network_density(ctx.credits)
    return ctx.network_density


@tag(stage="phase6", cost="moderate", domain="metrics")
def growth_trends_precomputed(ctx: PipelineContext, network_density_computed: Any) -> Any:
    """Growth trends: rising / stable / declining career trajectory.

    Writes: ctx.growth_data.
    """
    from src.analysis.growth import compute_growth_trends

    ctx.growth_data = compute_growth_trends(ctx.credits, ctx.anime_map)
    return ctx.growth_data


@tag(stage="phase6", cost="moderate", domain="metrics")
def peer_effects_estimated(ctx: PipelineContext, growth_trends_precomputed: Any) -> Any:
    """Peer effects (2SLS): spillover from collaborating with high-IV persons.

    Writes: ctx.peer_effect_result.
    """
    from src.analysis.network.peer_effects import estimate_peer_effects_2sls

    if ctx.collaboration_graph is not None:
        ctx.peer_effect_result = estimate_peer_effects_2sls(
            ctx.credits,
            ctx.anime_map,
            ctx.iv_scores,
            ctx.collaboration_graph,
        )
    return ctx.peer_effect_result


@tag(stage="phase6", cost="cheap", domain="metrics")
def career_friction_estimated(ctx: PipelineContext, peer_effects_estimated: Any) -> Any:
    """Career friction: observed vs expected career stage transitions.

    Writes: ctx.career_friction.
    """
    from src.analysis.career_friction import estimate_career_friction

    friction_result = estimate_career_friction(
        ctx.credits,
        ctx.anime_map,
        person_scores=ctx.iv_scores,
        studio_fe=ctx.studio_fe,
    )
    ctx.career_friction = friction_result.friction_index
    return ctx.career_friction


@tag(stage="phase6", cost="cheap", domain="metrics")
def era_effects_computed(ctx: PipelineContext, career_friction_estimated: Any) -> Any:
    """Era fixed effects: year-level difficulty proxy (crowding / production volume).

    Writes: ctx.era_effects.
    """
    from src.analysis.causal.era_effects import compute_era_and_difficulty

    ctx.era_effects = compute_era_and_difficulty(ctx.credits, ctx.anime_map, ctx.iv_scores)
    return ctx.era_effects


@tag(stage="phase6", cost="cheap", domain="metrics")
def growth_acceleration_computed(ctx: PipelineContext, era_effects_computed: Any) -> Any:
    """Growth acceleration: velocity and momentum of career IV trajectory.

    Writes: ctx.growth_acceleration_data.
    """
    from src.analysis.growth_acceleration import (
        compute_growth_metrics,
        compute_adjusted_person_fe_with_growth,
    )

    person_scores = {
        pid: {
            "person_fe": ctx.person_fe.get(pid, 0),
            "birank": ctx.birank_person_scores.get(pid, 0),
            "patronage": ctx.patronage_scores.get(pid, 0),
            "iv_score": ctx.iv_scores.get(pid, 0),
        }
        for pid in set(ctx.person_fe) | set(ctx.birank_person_scores) | set(ctx.iv_scores)
    }
    growth_metrics = compute_growth_metrics(ctx.credits, ctx.anime_map)
    adjusted_skills = compute_adjusted_person_fe_with_growth(
        person_scores, growth_metrics, growth_weight=0.3
    )
    ctx.growth_acceleration_data = {
        "growth_metrics": {pid: vars(m) for pid, m in growth_metrics.items()},
        "adjusted_skills": adjusted_skills,
    }
    return ctx.growth_acceleration_data


@tag(stage="phase6", cost="moderate", domain="metrics")
def anime_values_computed(ctx: PipelineContext, growth_acceleration_computed: Any) -> Any:
    """Anime value assessment: composite structural value per anime title.

    Writes: ctx.anime_values.
    """
    from src.analysis.anime_value import compute_anime_values

    person_scores = {
        pid: {
            "person_fe": ctx.person_fe.get(pid, 0),
            "birank": ctx.birank_person_scores.get(pid, 0),
            "patronage": ctx.patronage_scores.get(pid, 0),
            "iv_score": ctx.iv_scores.get(pid, 0),
        }
        for pid in set(ctx.person_fe) | set(ctx.birank_person_scores) | set(ctx.iv_scores)
    }
    anime_values_raw = compute_anime_values(ctx.anime_list, ctx.credits, person_scores)
    ctx.anime_values = {aid: vars(v) for aid, v in anime_values_raw.items()}
    return ctx.anime_values


@tag(stage="phase6", cost="moderate", domain="metrics")
def contribution_attribution_computed(ctx: PipelineContext, anime_values_computed: Any) -> Any:
    """Contribution attribution: per-person share of each anime's structural value.

    Samples top 100 anime by composite value for performance.

    Writes: ctx.contribution_data.
    """
    from collections import defaultdict
    from src.analysis.anime_value import compute_anime_values
    from src.analysis.contribution_attribution import compute_contribution_attribution

    person_scores = {
        pid: {
            "person_fe": ctx.person_fe.get(pid, 0),
            "birank": ctx.birank_person_scores.get(pid, 0),
            "patronage": ctx.patronage_scores.get(pid, 0),
            "iv_score": ctx.iv_scores.get(pid, 0),
        }
        for pid in set(ctx.person_fe) | set(ctx.birank_person_scores) | set(ctx.iv_scores)
    }
    anime_values_raw = compute_anime_values(ctx.anime_list, ctx.credits, person_scores)
    top_anime = sorted(anime_values_raw.items(), key=lambda x: x[1].composite_value, reverse=True)[:100]

    anime_credits_index: dict[str, list] = defaultdict(list)
    for c in ctx.credits:
        anime_credits_index[c.anime_id].append(c)

    all_contributions: dict = {}
    for anime_id, anime_val in top_anime:
        anime_credits = anime_credits_index.get(anime_id, [])
        if anime_credits:
            contribs = compute_contribution_attribution(
                anime_id, anime_val.composite_value, anime_credits, person_scores
            )
            all_contributions[anime_id] = {
                pid: {**vars(c), "role": c.role.value} for pid, c in contribs.items()
            }
    ctx.contribution_data = all_contributions
    return ctx.contribution_data


@tag(stage="phase6", cost="cheap", domain="metrics")
def potential_value_computed(ctx: PipelineContext, contribution_attribution_computed: Any) -> Any:
    """Potential value: forward-looking score integrating growth, network position, and trajectory.

    Writes: ctx.potential_value_scores.
    """
    from src.analysis.scoring.potential_value import compute_potential_value_scores

    person_scores = {
        pid: {
            "person_fe": ctx.person_fe.get(pid, 0),
            "birank": ctx.birank_person_scores.get(pid, 0),
            "patronage": ctx.patronage_scores.get(pid, 0),
            "iv_score": ctx.iv_scores.get(pid, 0),
        }
        for pid in set(ctx.person_fe) | set(ctx.birank_person_scores) | set(ctx.iv_scores)
    }
    growth_data = ctx.growth_acceleration_data.get("growth_metrics", {})
    growth_dict = {
        pid: {k: v for k, v in m.items() if k in ("growth_velocity", "momentum_score", "career_years")}
        for pid, m in growth_data.items()
    }
    adjusted_skills = ctx.growth_acceleration_data.get("adjusted_skills", {})

    if ctx.collaboration_graph is not None:
        potential_scores = compute_potential_value_scores(
            person_scores,
            {},
            growth_dict,
            adjusted_skills,
            ctx.collaboration_graph,
            betweenness_cache=ctx.betweenness_cache,
        )
        ctx.potential_value_scores = {
            pid: {**vars(p), "category": p.category.value} for pid, p in potential_scores.items()
        }
    return ctx.potential_value_scores


@tag(stage="phase6", cost="cheap", domain="metrics")
def career_tracks_inferred(ctx: PipelineContext, potential_value_computed: Any) -> Any:
    """Career track inference: animator / director / animator_director / etc.

    Writes: ctx.career_tracks.
    """
    from src.analysis.network.multilayer import infer_all_career_tracks

    ctx.career_tracks = infer_all_career_tracks(ctx.credits, ctx.anime_map)
    return ctx.career_tracks
