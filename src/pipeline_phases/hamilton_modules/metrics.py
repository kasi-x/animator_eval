"""Phase 6: Supplementary Metrics nodes for Hamilton DAG (H-4).

H-4 pattern: nodes take EntityResolutionResult / GraphsResult / CoreScoresResult typed
bags instead of ctx: dict.  ctx_metrics_populated bridges the results back
to ctx for Phase 9/10 compatibility.
"""

from __future__ import annotations

from typing import Any

from hamilton.function_modifiers import tag

from src.pipeline_phases.pipeline_types import (
    CoreScoresResult,
    EntityResolutionResult,
    GraphsResult,
    SupplementaryMetricsResult,
)

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
    "supplementary_metrics",
    "ctx_metrics_populated",
]


def _build_person_scores(core: CoreScoresResult, updated_iv: dict) -> dict:
    """Build per-person score dict used by growth / anime_value / contribution nodes."""
    all_pids = set(core.person_fe) | set(core.birank_person_scores) | set(updated_iv)
    return {
        pid: {
            "person_fe": core.person_fe.get(pid, 0),
            "birank": core.birank_person_scores.get(pid, 0),
            "patronage": core.patronage_scores.get(pid, 0),
            "iv_score": updated_iv.get(pid, 0),
        }
        for pid in all_pids
    }


@tag(stage="phase6", cost="moderate", domain="metrics")
def engagement_decay(
    entity_resolved: EntityResolutionResult, integrated_value_computation: dict
) -> dict:
    """Engagement decay detection: directors whose hiring rate for an animator dropped.

    Takes integrated_value_computation to run after Phase 5 completes.
    """
    from src.analysis.network.trust import batch_detect_engagement_decay

    return batch_detect_engagement_decay(
        entity_resolved.resolved_credits, entity_resolved.anime_map
    )


@tag(stage="phase6", cost="cheap", domain="metrics")
def role_classification(entity_resolved: EntityResolutionResult, engagement_decay: dict) -> dict:
    """Primary role classification per person (animator / director / etc.)."""
    from src.analysis.graph import determine_primary_role_for_each_person

    return determine_primary_role_for_each_person(entity_resolved.resolved_credits)


@tag(stage="phase6", cost="moderate", domain="metrics")
def career_analysis(entity_resolved: EntityResolutionResult, role_classification: dict) -> dict:
    """Career trajectory analysis: stage, peak, active years."""
    from src.analysis.career import batch_career_analysis

    return batch_career_analysis(entity_resolved.resolved_credits, entity_resolved.anime_map)


@tag(stage="phase6", cost="cheap", domain="metrics")
def career_aware_dormancy_recomputed(
    ctx_core_populated: CoreScoresResult, career_analysis: dict
) -> dict:
    """Re-compute dormancy and IV with career-aware dormancy (protects veterans).

    Veterans with long track records get softer dormancy penalties than newcomers.

    Returns dict: dormancy_scores (updated), iv_scores (updated).
    """
    from src.analysis.scoring.integrated_value import compute_integrated_value, compute_studio_exposure
    from src.analysis.scoring.patronage_dormancy import compute_career_aware_dormancy

    career_dormancy = compute_career_aware_dormancy(
        raw_dormancy=ctx_core_populated.dormancy_scores,
        iv_scores_historical=ctx_core_populated.iv_scores_historical,
        career_data=career_analysis,
    )
    studio_exposure = compute_studio_exposure(
        ctx_core_populated.person_fe,
        ctx_core_populated.studio_fe,
        studio_assignments=ctx_core_populated.studio_assignments,
    )
    awcc_scores = {pid: m.awcc for pid, m in ctx_core_populated.knowledge_spanner_scores.items()}
    iv_scores = compute_integrated_value(
        ctx_core_populated.person_fe,
        ctx_core_populated.birank_person_scores,
        studio_exposure,
        awcc_scores,
        ctx_core_populated.patronage_scores,
        career_dormancy,
        ctx_core_populated.iv_lambda_weights,
        component_std=ctx_core_populated.iv_component_std,
        component_mean=ctx_core_populated.iv_component_mean,
    )
    return {"dormancy_scores": career_dormancy, "iv_scores": iv_scores}


@tag(stage="phase6", cost="moderate", domain="metrics")
def director_circles(
    entity_resolved: EntityResolutionResult, career_aware_dormancy_recomputed: dict
) -> dict:
    """Director circles: frequent collaborator groups around each director."""
    from src.analysis.network.circles import find_director_circles

    return find_director_circles(entity_resolved.resolved_credits, entity_resolved.anime_map)


@tag(stage="phase6", cost="cheap", domain="metrics")
def versatility_computed(entity_resolved: EntityResolutionResult, director_circles: dict) -> dict:
    """Versatility: breadth of role categories and specific roles per person."""
    from src.analysis.versatility import compute_versatility

    return compute_versatility(entity_resolved.resolved_credits)


@tag(stage="phase6", cost="expensive", domain="metrics")
def centrality_metrics(
    graphs_result: GraphsResult,
    entity_resolved: EntityResolutionResult,
    versatility_computed: dict,
) -> dict:
    """Network centrality: betweenness, closeness, degree in collaboration graph.

    Returns dict: centrality ({pid: metrics}), betweenness_cache ({pid: float}).
    """
    from src.analysis.graph import calculate_network_centrality_scores

    centrality: dict = {}
    if graphs_result.collaboration_graph is not None:
        person_ids = {p.id for p in entity_resolved.persons}
        centrality = calculate_network_centrality_scores(
            graphs_result.collaboration_graph, person_ids
        )
    betweenness_cache = {
        pid: metrics["betweenness"]
        for pid, metrics in centrality.items()
        if "betweenness" in metrics
    }
    return {"centrality": centrality, "betweenness_cache": betweenness_cache}


@tag(stage="phase6", cost="cheap", domain="metrics")
def network_density_computed(
    entity_resolved: EntityResolutionResult, centrality_metrics: dict
) -> dict:
    """Network density: collaborator count, unique anime, hub score per person."""
    from src.analysis.network.network_density import compute_network_density

    return compute_network_density(entity_resolved.resolved_credits)


@tag(stage="phase6", cost="moderate", domain="metrics")
def growth_trends_precomputed(
    entity_resolved: EntityResolutionResult, network_density_computed: dict
) -> dict:
    """Growth trends: rising / stable / declining career trajectory."""
    from src.analysis.growth import compute_growth_trends

    return compute_growth_trends(entity_resolved.resolved_credits, entity_resolved.anime_map)


@tag(stage="phase6", cost="moderate", domain="metrics")
def peer_effects_estimated(
    entity_resolved: EntityResolutionResult,
    graphs_result: GraphsResult,
    career_aware_dormancy_recomputed: dict,
    growth_trends_precomputed: dict,
) -> Any:
    """Peer effects (2SLS): spillover from collaborating with high-IV persons.

    Uses updated iv_scores from career_aware_dormancy_recomputed.
    """
    from src.analysis.network.peer_effects import estimate_peer_effects_2sls

    if graphs_result.collaboration_graph is not None:
        return estimate_peer_effects_2sls(
            entity_resolved.resolved_credits,
            entity_resolved.anime_map,
            career_aware_dormancy_recomputed["iv_scores"],
            graphs_result.collaboration_graph,
        )
    return None


@tag(stage="phase6", cost="cheap", domain="metrics")
def career_friction_estimated(
    entity_resolved: EntityResolutionResult,
    ctx_core_populated: CoreScoresResult,
    career_aware_dormancy_recomputed: dict,
    peer_effects_estimated: Any,
) -> dict:
    """Career friction: observed vs expected career stage transitions."""
    from src.analysis.career_friction import estimate_career_friction

    friction_result = estimate_career_friction(
        entity_resolved.resolved_credits,
        entity_resolved.anime_map,
        person_scores=career_aware_dormancy_recomputed["iv_scores"],
        studio_fe=ctx_core_populated.studio_fe,
    )
    return friction_result.friction_index


@tag(stage="phase6", cost="cheap", domain="metrics")
def era_effects_computed(
    entity_resolved: EntityResolutionResult,
    career_aware_dormancy_recomputed: dict,
    career_friction_estimated: dict,
) -> Any:
    """Era fixed effects: year-level difficulty proxy (crowding / production volume)."""
    from src.analysis.causal.era_effects import compute_era_and_difficulty

    return compute_era_and_difficulty(
        entity_resolved.resolved_credits,
        entity_resolved.anime_map,
        career_aware_dormancy_recomputed["iv_scores"],
    )


@tag(stage="phase6", cost="cheap", domain="metrics")
def growth_acceleration_computed(
    entity_resolved: EntityResolutionResult,
    ctx_core_populated: CoreScoresResult,
    career_aware_dormancy_recomputed: dict,
    era_effects_computed: Any,
) -> dict:
    """Growth acceleration: velocity and momentum of career IV trajectory."""
    from src.analysis.growth_acceleration import (
        compute_adjusted_person_fe_with_growth,
        compute_growth_metrics,
    )

    person_scores = _build_person_scores(
        ctx_core_populated, career_aware_dormancy_recomputed["iv_scores"]
    )
    growth_metrics = compute_growth_metrics(entity_resolved.resolved_credits, entity_resolved.anime_map)
    adjusted_skills = compute_adjusted_person_fe_with_growth(
        person_scores, growth_metrics, growth_weight=0.3
    )
    return {
        "growth_metrics": {pid: vars(m) for pid, m in growth_metrics.items()},
        "adjusted_skills": adjusted_skills,
    }


@tag(stage="phase6", cost="moderate", domain="metrics")
def anime_values_computed(
    entity_resolved: EntityResolutionResult,
    ctx_core_populated: CoreScoresResult,
    career_aware_dormancy_recomputed: dict,
    growth_acceleration_computed: dict,
) -> dict:
    """Anime value assessment: composite structural value per anime title."""
    from src.analysis.anime_value import compute_anime_values

    person_scores = _build_person_scores(
        ctx_core_populated, career_aware_dormancy_recomputed["iv_scores"]
    )
    anime_values_raw = compute_anime_values(
        entity_resolved.anime_list, entity_resolved.resolved_credits, person_scores
    )
    return {aid: vars(v) for aid, v in anime_values_raw.items()}


@tag(stage="phase6", cost="moderate", domain="metrics")
def contribution_attribution_computed(
    entity_resolved: EntityResolutionResult,
    ctx_core_populated: CoreScoresResult,
    career_aware_dormancy_recomputed: dict,
    anime_values_computed: dict,
) -> dict:
    """Contribution attribution: per-person share of each anime's structural value.

    Samples top 100 anime by composite value for performance.
    """
    from collections import defaultdict

    from src.analysis.anime_value import compute_anime_values
    from src.analysis.contribution_attribution import compute_contribution_attribution

    person_scores = _build_person_scores(
        ctx_core_populated, career_aware_dormancy_recomputed["iv_scores"]
    )
    anime_values_raw = compute_anime_values(
        entity_resolved.anime_list, entity_resolved.resolved_credits, person_scores
    )
    top_anime = sorted(
        anime_values_raw.items(), key=lambda x: x[1].composite_value, reverse=True
    )[:100]

    anime_credits_index: dict[str, list] = defaultdict(list)
    for c in entity_resolved.resolved_credits:
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
    return all_contributions


@tag(stage="phase6", cost="cheap", domain="metrics")
def potential_value_computed(
    graphs_result: GraphsResult,
    ctx_core_populated: CoreScoresResult,
    career_aware_dormancy_recomputed: dict,
    growth_acceleration_computed: dict,
    contribution_attribution_computed: dict,
    centrality_metrics: dict,
) -> dict:
    """Potential value: forward-looking score integrating growth, network position, and trajectory."""
    from src.analysis.scoring.potential_value import compute_potential_value_scores

    person_scores = _build_person_scores(
        ctx_core_populated, career_aware_dormancy_recomputed["iv_scores"]
    )
    growth_data = growth_acceleration_computed.get("growth_metrics", {})
    growth_dict = {
        pid: {
            k: v
            for k, v in m.items()
            if k in ("growth_velocity", "momentum_score", "career_years")
        }
        for pid, m in growth_data.items()
    }
    adjusted_skills = growth_acceleration_computed.get("adjusted_skills", {})
    betweenness_cache = centrality_metrics.get("betweenness_cache", {})

    if graphs_result.collaboration_graph is not None:
        potential_scores = compute_potential_value_scores(
            person_scores,
            {},
            growth_dict,
            adjusted_skills,
            graphs_result.collaboration_graph,
            betweenness_cache=betweenness_cache,
        )
        return {
            pid: {**vars(p), "category": p.category.value} for pid, p in potential_scores.items()
        }
    return {}


@tag(stage="phase6", cost="cheap", domain="metrics")
def career_tracks_inferred(
    entity_resolved: EntityResolutionResult, potential_value_computed: dict
) -> dict:
    """Career track inference: animator / director / animator_director / etc."""
    from src.analysis.network.multilayer import infer_all_career_tracks

    return infer_all_career_tracks(entity_resolved.resolved_credits, entity_resolved.anime_map)


@tag(stage="phase6", cost="cheap", domain="metrics")
def supplementary_metrics(
    engagement_decay: dict,
    role_classification: dict,
    career_analysis: dict,
    career_aware_dormancy_recomputed: dict,
    director_circles: dict,
    versatility_computed: dict,
    centrality_metrics: dict,
    network_density_computed: dict,
    growth_trends_precomputed: dict,
    peer_effects_estimated: Any,
    career_friction_estimated: dict,
    era_effects_computed: Any,
    growth_acceleration_computed: dict,
    anime_values_computed: dict,
    contribution_attribution_computed: dict,
    potential_value_computed: dict,
    career_tracks_inferred: dict,
    ctx_core_populated: CoreScoresResult,
) -> SupplementaryMetricsResult:
    """Aggregate all Phase 6 outputs into a SupplementaryMetricsResult typed bag."""
    return SupplementaryMetricsResult(
        decay_results=engagement_decay,
        role_profiles=role_classification,
        career_data=career_analysis,
        career_tracks=career_tracks_inferred,
        career_friction=career_friction_estimated,
        circles=director_circles,
        centrality=centrality_metrics["centrality"],
        network_density=network_density_computed,
        betweenness_cache=centrality_metrics["betweenness_cache"],
        versatility=versatility_computed,
        growth_data=growth_trends_precomputed,
        growth_acceleration_data=growth_acceleration_computed,
        anime_values=anime_values_computed,
        contribution_data=contribution_attribution_computed,
        potential_value_scores=potential_value_computed,
        studio_bias_metrics={},
        era_effects=era_effects_computed,
        peer_effect_result=peer_effects_estimated,
        dormancy_scores=career_aware_dormancy_recomputed["dormancy_scores"],
        iv_scores=career_aware_dormancy_recomputed["iv_scores"],
        knowledge_spanner_scores=ctx_core_populated.knowledge_spanner_scores,
    )


@tag(stage="phase6", cost="cheap", domain="metrics")
def ctx_metrics_populated(
    supplementary_metrics: SupplementaryMetricsResult,
    ctx_core_populated: CoreScoresResult,
    ctx: Any,
) -> SupplementaryMetricsResult:
    """H-4 bridge: copy SupplementaryMetricsResult fields to ctx for Phase 9/10 compatibility.

    Also updates ctx.dormancy_scores and ctx.iv_scores with career-aware values,
    overwriting the Phase 5 values written by ctx_core_populated.

    Returns the same SupplementaryMetricsResult for typed downstream access.
    """
    ctx.decay_results = supplementary_metrics.decay_results
    ctx.role_profiles = supplementary_metrics.role_profiles
    ctx.career_data = supplementary_metrics.career_data
    ctx.career_tracks = supplementary_metrics.career_tracks
    ctx.career_friction = supplementary_metrics.career_friction
    ctx.circles = supplementary_metrics.circles
    ctx.centrality = supplementary_metrics.centrality
    ctx.network_density = supplementary_metrics.network_density
    ctx.betweenness_cache = supplementary_metrics.betweenness_cache
    ctx.versatility = supplementary_metrics.versatility
    ctx.growth_data = supplementary_metrics.growth_data
    ctx.growth_acceleration_data = supplementary_metrics.growth_acceleration_data
    ctx.anime_values = supplementary_metrics.anime_values
    ctx.contribution_data = supplementary_metrics.contribution_data
    ctx.potential_value_scores = supplementary_metrics.potential_value_scores
    ctx.era_effects = supplementary_metrics.era_effects
    ctx.peer_effect_result = supplementary_metrics.peer_effect_result
    # Override Phase 5 values with career-aware updates
    ctx.dormancy_scores = supplementary_metrics.dormancy_scores
    ctx.iv_scores = supplementary_metrics.iv_scores
    ctx.knowledge_spanner_scores = supplementary_metrics.knowledge_spanner_scores
    return supplementary_metrics
