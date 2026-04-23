"""Phase 10: Export and Visualization — declarative registry-based exports.

This module implements a declarative export system using ExportSpec objects.
All 28 JSON exports are defined in a single registry for easy maintenance.
"""

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

import structlog

from src.pipeline_phases.pipeline_types import ExportContext
from src.utils.json_io import save_json_to_file

logger = structlog.get_logger()


@dataclass
class ExportSpec:
    """Declarative export configuration for a single JSON file.

    Each spec defines:
    - filename: Output filename (e.g., "scores.json")
    - data_getter: Function that extracts data from PipelineContext
    - transformer: Optional function to transform data before export
    - condition: Optional function to determine if export should happen
    - log_message: Message to log when export succeeds
    - log_metrics: Optional dict of metrics to include in log
    """

    filename: str
    data_getter: Callable[[dict], Any]
    transformer: Callable[[Any, dict], dict | list] | None = None
    condition: Callable[[Any], bool] | None = None
    log_message: str = "export_saved"
    log_metrics: Callable[[Any], dict[str, Any]] | None = None


def _build_pid_to_name(context: dict) -> dict[str, str]:
    """Build person_id → display name mapping from context results."""
    return {r["person_id"]: r["name"] or r["person_id"] for r in context.results}


def _transform_collaborations(data: list, context: dict) -> list:
    """Add person names to collaboration pairs."""
    if not data:
        return []
    pid_to_name = _build_pid_to_name(context)
    for item in data:
        if "person_a" in item:
            item["name_a"] = pid_to_name.get(item["person_a"], item["person_a"])
        if "person_b" in item:
            item["name_b"] = pid_to_name.get(item["person_b"], item["person_b"])
    return data


def _transform_mentorships(data: list, context: dict) -> list:
    """Add person names to mentorship pairs."""
    if not data:
        return []
    pid_to_name = _build_pid_to_name(context)
    for item in data:
        if "mentor_id" in item:
            item["mentor_name"] = pid_to_name.get(item["mentor_id"], item["mentor_id"])
        if "mentee_id" in item:
            item["mentee_name"] = pid_to_name.get(item["mentee_id"], item["mentee_id"])
    return data


def _transform_bridges(data: dict, context: dict) -> dict:
    """Add person names to bridge analysis results."""
    if not data:
        return {}
    pid_to_name = _build_pid_to_name(context)
    if "bridge_persons" in data:
        for bp in data["bridge_persons"]:
            if "person_id" in bp:
                bp["name"] = pid_to_name.get(bp["person_id"], bp["person_id"])
    if "cross_community_edges" in data:
        for edge in data["cross_community_edges"]:
            if "person_a" in edge:
                edge["name_a"] = pid_to_name.get(edge["person_a"], edge["person_a"])
            if "person_b" in edge:
                edge["name_b"] = pid_to_name.get(edge["person_b"], edge["person_b"])
    return data


def _transform_influence(data: dict, context: dict) -> dict:
    """Add person names to influence tree."""
    if not data:
        return {}
    pid_to_name = _build_pid_to_name(context)
    if "mentors" in data:
        for mid, mentor_data in data["mentors"].items():
            mentor_data["name"] = pid_to_name.get(mid, mid)
            for mentee in mentor_data.get("mentees", []):
                if "mentee_id" in mentee:
                    mentee["name"] = pid_to_name.get(
                        mentee["mentee_id"], mentee["mentee_id"]
                    )
    return data


def _transform_teams(data: dict, context: dict) -> dict:
    """Add person names to team analysis results."""
    if not data:
        return {}
    pid_to_name = _build_pid_to_name(context)
    for team in data.get("high_score_teams", []):
        if "roles" in team:
            named_roles = {}
            for role_name, pids in team["roles"].items():
                named_roles[role_name] = [
                    {"person_id": pid, "name": pid_to_name.get(pid, pid)}
                    for pid in pids
                ]
            team["roles"] = named_roles
    for pair in data.get("recommended_pairs", []):
        if "person_a" in pair:
            pair["name_a"] = pid_to_name.get(pair["person_a"], pair["person_a"])
        if "person_b" in pair:
            pair["name_b"] = pid_to_name.get(pair["person_b"], pair["person_b"])
    return data


def _transform_mentorship_tree(data: dict, context: dict) -> dict:
    """Add person names to mentorship tree."""
    if not data:
        return {}
    pid_to_name = _build_pid_to_name(context)
    if "tree" in data:
        named_tree = {}
        for mentor_id, mentee_ids in data["tree"].items():
            named_tree[mentor_id] = {
                "mentor_name": pid_to_name.get(mentor_id, mentor_id),
                "mentees": [
                    {"mentee_id": mid, "name": pid_to_name.get(mid, mid)}
                    for mid in mentee_ids
                ],
            }
        data["tree"] = named_tree
    if "roots" in data:
        data["roots"] = [
            {"person_id": pid, "name": pid_to_name.get(pid, pid)}
            for pid in data["roots"]
        ]
    return data


def _transform_cooccurrence_groups(data: dict, context: dict) -> dict:
    """Add member names and anime titles to cooccurrence groups."""
    if not data:
        return {}
    pid_to_name = _build_pid_to_name(context)
    aid_to_title = {anime.id: anime.display_title for anime in context.anime_list}
    for group in data.get("groups", []):
        group["member_names"] = [
            pid_to_name.get(pid, pid) for pid in group.get("members", [])
        ]
        group["shared_anime_titles"] = [
            aid_to_title.get(aid, aid) for aid in group.get("shared_anime", [])
        ]
    # Also update top_groups in temporal_slices
    for ts in data.get("temporal_slices", []):
        for tg in ts.get("top_groups", []):
            tg["member_names"] = [
                pid_to_name.get(pid, pid) for pid in tg.get("members", [])
            ]
    return data


def _transform_temporal_pagerank(data: dict, context: dict) -> dict:
    """Add person names to temporal pagerank timeline, foresight and promotion data."""
    if not data:
        return {}
    pid_to_name = _build_pid_to_name(context)
    if "authority_timelines" in data:
        for pid, tdata in data["authority_timelines"].items():
            tdata["name"] = pid_to_name.get(pid, pid)
    if "foresight_scores" in data:
        for pid, fdata in data["foresight_scores"].items():
            fdata["name"] = pid_to_name.get(pid, pid)
    if "promotion_credits" in data:
        for pid, pdata in data["promotion_credits"].items():
            pdata["name"] = pid_to_name.get(pid, pid)
    return data


def _transform_circles(data: dict, context: dict) -> dict:
    """Transform circles data with name lookups."""
    if not data:
        return {}

    pid_to_name = _build_pid_to_name(context)
    circles_output = {}
    for dir_id, circle in data.items():
        circle_dict = asdict(circle)
        circles_output[dir_id] = {
            "director_name": pid_to_name.get(dir_id, dir_id),
            **circle_dict,
            "members": [
                {
                    **member,
                    "name": pid_to_name.get(member["person_id"], member["person_id"]),
                }
                for member in circle_dict["members"]
            ],
        }
    return circles_output


def _transform_growth(data: dict, context: dict) -> dict:
    """Transform growth data with trend summary and names."""
    if not data:
        return {}

    pid_to_name = _build_pid_to_name(context)

    # Summarize trend counts
    trend_counts: dict[str, int] = {}
    for gd in data.values():
        trend_counts[gd.trend] = trend_counts.get(gd.trend, 0) + 1

    # Convert top 200 by activity_ratio to dicts with names
    return {
        "trend_summary": trend_counts,
        "total_persons": len(data),
        "persons": {
            pid: {**asdict(metrics), "name": pid_to_name.get(pid, pid)}
            for pid, metrics in sorted(
                data.items(),
                key=lambda x: x[1].activity_ratio,
                reverse=True,
            )[:200]
        },
    }


def _transform_tags(data: dict, context: dict) -> dict:
    """Transform person tags with tag summary."""
    if not data:
        return {}

    # Build tag summary
    tag_summary: dict[str, int] = {}
    for tags_list in data.values():
        for tag in tags_list:
            tag_summary[tag] = tag_summary.get(tag, 0) + 1

    return {
        "tag_summary": dict(sorted(tag_summary.items(), key=lambda x: -x[1])),
        "person_tags": data,
    }


def _transform_productivity(data: dict, context: dict) -> dict:
    """Transform productivity dataclass instances to dicts."""
    if not data:
        return {}
    return {pid: asdict(metrics) for pid, metrics in data.items()}


def _transform_transitions(data: dict, context: dict) -> dict:
    """Transform role transitions with dataclass to dict conversion."""
    if not data:
        return {}

    # Transitions data is already in dict format from analysis_modules.py
    # It was already converted using asdict() there, so just return as-is
    return data


def _transform_summary(data: None, context: dict) -> dict:
    """Build pipeline summary from context and elapsed time."""
    from datetime import datetime
    from src.analysis.graph import compute_graph_summary

    # Get elapsed time from context or compute it
    elapsed = getattr(context, "_elapsed", 0.0)

    # Use cached graph summary if graph was freed, otherwise compute
    graph_summary = context.analysis_results.get("_graph_summary", {})
    if not graph_summary and context.collaboration_graph:
        graph_summary = compute_graph_summary(context.collaboration_graph)

    # Get crossval results
    crossval_data = context.analysis_results.get("crossval", {})
    crossval_summary = {}
    if crossval_data:
        crossval_summary = {
            "avg_rank_correlation": crossval_data.get("avg_rank_correlation", 0),
            "avg_top10_overlap": crossval_data.get("avg_top10_overlap", 0),
        }

    return {
        "generated_at": datetime.now().isoformat(),
        "elapsed_seconds": round(elapsed, 2),
        "mode": "full",
        "data": {
            "persons": len(context.persons),
            "anime": len(context.anime_list),
            "credits": len(context.credits),
            "scored_persons": len(context.results),
        },
        "scores": {
            "top_iv_score": context.results[0]["iv_score"] if context.results else 0,
            "median_iv_score": (
                context.results[len(context.results) // 2]["iv_score"]
                if context.results
                else 0
            ),
        },
        "graph": graph_summary,
        "crossval": crossval_summary,
    }


# Registry of all 26 pipeline exports
EXPORT_REGISTRY: list[ExportSpec] = [
    # Main results
    ExportSpec(
        filename="scores.json",
        data_getter=lambda ctx: ctx.results,
        log_message="scores_saved",
        log_metrics=lambda data: {"persons": len(data)} if data else {},
    ),
    # Director circles (with name lookups)
    ExportSpec(
        filename="circles.json",
        data_getter=lambda ctx: ctx.circles,
        transformer=_transform_circles,
        log_message="circles_saved",
        log_metrics=lambda data: {"directors": len(data)} if data else {},
    ),
    # Anime statistics
    ExportSpec(
        filename="anime_stats.json",
        data_getter=lambda ctx: ctx.analysis_results.get("anime_stats"),
        log_message="anime_stats_saved",
        log_metrics=lambda data: {"anime": len(data)} if data else {},
    ),
    # Studios
    ExportSpec(
        filename="studios.json",
        data_getter=lambda ctx: ctx.analysis_results.get("studios"),
        log_message="studios_saved",
        log_metrics=lambda data: {"studios": len(data)} if data else {},
    ),
    # Seasonal trends (conditional on by_season data)
    ExportSpec(
        filename="seasonal.json",
        data_getter=lambda ctx: ctx.analysis_results.get("seasonal"),
        condition=lambda data: data.get("by_season") if data else False,
        log_message="seasonal_saved",
    ),
    # Collaborations (with names)
    ExportSpec(
        filename="collaborations.json",
        data_getter=lambda ctx: ctx.analysis_results.get("collaborations"),
        transformer=_transform_collaborations,
        log_message="collaborations_saved",
        log_metrics=lambda data: {"pairs": len(data)} if data else {},
    ),
    # Outliers
    ExportSpec(
        filename="outliers.json",
        data_getter=lambda ctx: ctx.analysis_results.get("outliers"),
        log_message="outliers_saved",
    ),
    # Team patterns (with names)
    ExportSpec(
        filename="teams.json",
        data_getter=lambda ctx: ctx.analysis_results.get("teams"),
        transformer=_transform_teams,
        log_message="teams_saved",
        log_metrics=lambda data: {"patterns": len(data)} if data else {},
    ),
    # Time series
    ExportSpec(
        filename="time_series.json",
        data_getter=lambda ctx: ctx.analysis_results.get("time_series"),
        log_message="time_series_saved",
    ),
    # Decades
    ExportSpec(
        filename="decades.json",
        data_getter=lambda ctx: ctx.analysis_results.get("decades"),
        log_message="decades_saved",
    ),
    # Growth trends (with trend summary)
    ExportSpec(
        filename="growth.json",
        data_getter=lambda ctx: ctx.growth_data,
        transformer=_transform_growth,
        log_message="growth_saved",
        log_metrics=lambda data: (
            {"persons": data.get("total_persons", 0)} if data else {}
        ),
    ),
    # Person tags (with tag summary)
    ExportSpec(
        filename="tags.json",
        data_getter=lambda ctx: ctx.analysis_results.get("tags"),
        transformer=_transform_tags,
        log_message="tags_saved",
        log_metrics=lambda data: (
            {"unique_tags": len(data.get("tag_summary", {}))} if data else {}
        ),
    ),
    # Role transitions
    ExportSpec(
        filename="transitions.json",
        data_getter=lambda ctx: ctx.analysis_results.get("transitions"),
        transformer=_transform_transitions,
        log_message="transitions_saved",
    ),
    # Role flow
    ExportSpec(
        filename="role_flow.json",
        data_getter=lambda ctx: ctx.analysis_results.get("role_flow"),
        log_message="role_flow_saved",
    ),
    # Bridges (with names)
    ExportSpec(
        filename="bridges.json",
        data_getter=lambda ctx: ctx.analysis_results.get("bridges"),
        transformer=_transform_bridges,
        log_message="bridges_saved",
        log_metrics=lambda data: (
            {"bridges": len(data)} if isinstance(data, list) else {}
        ),
    ),
    # Mentorships (with names)
    ExportSpec(
        filename="mentorships.json",
        data_getter=lambda ctx: ctx.analysis_results.get("mentorships"),
        transformer=_transform_mentorships,
        log_message="mentorships_saved",
        log_metrics=lambda data: {"pairs": len(data)} if data else {},
    ),
    # Mentorship tree (with names)
    ExportSpec(
        filename="mentorship_tree.json",
        data_getter=lambda ctx: ctx.analysis_results.get("mentorship_tree"),
        transformer=_transform_mentorship_tree,
        log_message="mentorship_tree_saved",
    ),
    # Milestones
    ExportSpec(
        filename="milestones.json",
        data_getter=lambda ctx: ctx.analysis_results.get("milestones"),
        log_message="milestones_saved",
        log_metrics=lambda data: {"records": len(data)} if data else {},
    ),
    # Network evolution
    ExportSpec(
        filename="network_evolution.json",
        data_getter=lambda ctx: ctx.analysis_results.get("network_evolution"),
        log_message="network_evolution_saved",
    ),
    # Genre affinity
    ExportSpec(
        filename="genre_affinity.json",
        data_getter=lambda ctx: ctx.analysis_results.get("genre_affinity"),
        log_message="genre_affinity_saved",
        log_metrics=lambda data: {"persons": len(data)} if data else {},
    ),
    # Productivity (convert dataclass to dict)
    ExportSpec(
        filename="productivity.json",
        data_getter=lambda ctx: ctx.analysis_results.get("productivity"),
        transformer=_transform_productivity,
        log_message="productivity_saved",
        log_metrics=lambda data: {"persons": len(data)} if data else {},
    ),
    # Influence tree (with names)
    ExportSpec(
        filename="influence.json",
        data_getter=lambda ctx: ctx.analysis_results.get("influence"),
        transformer=_transform_influence,
        log_message="influence_saved",
    ),
    # Cross-validation (conditional)
    ExportSpec(
        filename="crossval.json",
        data_getter=lambda ctx: ctx.analysis_results.get("crossval"),
        log_message="crossval_saved",
    ),
    # ========== Advanced Metrics (New) ==========
    # Studio bias correction
    ExportSpec(
        filename="studio_bias.json",
        data_getter=lambda ctx: ctx.studio_bias_metrics,
        log_message="studio_bias_saved",
        log_metrics=lambda data: {
            "persons": len(data.get("bias_metrics", {})) if data else 0,
            "studios": len(data.get("studio_prestige", {})) if data else 0,
        },
    ),
    # Anime value assessment
    ExportSpec(
        filename="anime_values.json",
        data_getter=lambda ctx: ctx.anime_values,
        log_message="anime_values_saved",
        log_metrics=lambda data: {"anime": len(data)} if data else {},
    ),
    # Contribution attribution
    ExportSpec(
        filename="contributions.json",
        data_getter=lambda ctx: ctx.contribution_data,
        log_message="contributions_saved",
        log_metrics=lambda data: {"anime": len(data)} if data else {},
    ),
    # Potential value scores
    ExportSpec(
        filename="potential_value.json",
        data_getter=lambda ctx: ctx.potential_value_scores,
        log_message="potential_value_saved",
        log_metrics=lambda data: {"persons": len(data)} if data else {},
    ),
    # Performance monitoring
    ExportSpec(
        filename="performance.json",
        data_getter=lambda ctx: ctx.analysis_results.get("performance"),
        log_message="performance_saved",
    ),
    # Bias detection report
    ExportSpec(
        filename="bias_report.json",
        data_getter=lambda ctx: ctx.analysis_results.get("bias_report"),
        log_message="bias_report_saved",
        log_metrics=lambda data: (
            {"total_biases": data.get("summary", {}).get("total_biases_detected", 0)}
            if data
            else {}
        ),
    ),
    # Fair compensation analysis
    ExportSpec(
        filename="fair_compensation.json",
        data_getter=lambda ctx: ctx.analysis_results.get("fair_compensation"),
        log_message="fair_compensation_saved",
        log_metrics=lambda data: {"anime": data.get("total_anime", 0)} if data else {},
    ),
    # Comprehensive insights report
    ExportSpec(
        filename="insights_report.json",
        data_getter=lambda ctx: ctx.analysis_results.get("insights_report"),
        log_message="insights_report_saved",
        log_metrics=lambda data: (
            {
                "recommendations": len(data.get("recommendations", [])),
                "findings": len(data.get("key_findings", [])),
            }
            if data
            else {}
        ),
    ),
    # Causal studio identification (selection vs treatment vs brand)
    ExportSpec(
        filename="causal_identification.json",
        data_getter=lambda ctx: ctx.analysis_results.get("causal_identification"),
        log_message="causal_identification_saved",
        log_metrics=lambda data: (
            {
                "trajectories": data.get("sample_statistics", {}).get(
                    "total_trajectories", 0
                ),
                "transitions": data.get("sample_statistics", {}).get(
                    "total_transitions", 0
                ),
                "dominant_effect": data.get("conclusion", {}).get(
                    "dominant_effect", "unknown"
                ),
            }
            if data
            else {}
        ),
    ),
    # Structural estimation (fixed effects + DID)
    ExportSpec(
        filename="structural_estimation.json",
        data_getter=lambda ctx: ctx.analysis_results.get("structural_estimation"),
        log_message="structural_estimation_saved",
        log_metrics=lambda data: (
            {
                "fe_beta": data.get("fixed_effects", {}).get("beta", 0),
                "fe_pvalue": data.get("fixed_effects", {}).get("p_value", 1),
                "did_beta": data.get("difference_in_differences", {}).get("beta", 0),
                "did_pvalue": data.get("difference_in_differences", {}).get(
                    "p_value", 1
                ),
                "preferred_method": data.get("preferred_estimate", {}).get(
                    "method", "unknown"
                ),
            }
            if data
            else {}
        ),
    ),
    # Individual contribution profiles (Layer 2)
    ExportSpec(
        filename="individual_profiles.json",
        data_getter=lambda ctx: ctx.analysis_results.get("individual_profiles"),
        log_message="individual_profiles_saved",
        log_metrics=lambda data: (
            {
                "persons": data.get("total_persons", 0),
                "r_squared": data.get("model_r_squared"),
            }
            if data
            else {}
        ),
    ),
    # Credit statistics (person_id level analysis)
    ExportSpec(
        filename="credit_stats.json",
        data_getter=lambda ctx: ctx.analysis_results.get("credit_stats"),
        log_message="credit_stats_saved",
        log_metrics=lambda data: (
            {
                "total_credits": data.get("summary", {}).get("total_credits", 0),
                "unique_persons": data.get("summary", {}).get("unique_persons", 0),
                "collaboration_pairs": data.get("collaboration_stats", {}).get(
                    "total_pairs", 0
                ),
            }
            if data
            else {}
        ),
    ),
    # Co-occurrence groups (recurring core staff teams, with names)
    ExportSpec(
        filename="cooccurrence_groups.json",
        data_getter=lambda ctx: ctx.analysis_results.get("cooccurrence_groups"),
        transformer=_transform_cooccurrence_groups,
        log_message="cooccurrence_groups_saved",
        log_metrics=lambda data: (
            {
                "groups": data.get("summary", {}).get("total_groups", 0),
                "active": data.get("summary", {}).get("active_groups", 0),
            }
            if data
            else {}
        ),
    ),
    # Temporal PageRank (yearly authority timelines, foresight, promotions, with names)
    ExportSpec(
        filename="temporal_pagerank.json",
        data_getter=lambda ctx: ctx.analysis_results.get("temporal_pagerank"),
        transformer=_transform_temporal_pagerank,
        log_message="temporal_pagerank_saved",
        log_metrics=lambda data: (
            {
                "years": len(data.get("years_computed", [])),
                "persons": data.get("total_persons", 0),
                "foresight": len(data.get("foresight_scores", {})),
                "promotions": len(data.get("promotion_credits", {})),
            }
            if data
            else {}
        ),
    ),
    # Synergy scores (sequel chain pairings)
    ExportSpec(
        filename="synergy_scores.json",
        data_getter=lambda ctx: ctx.analysis_results.get("synergy_scores"),
        log_message="synergy_scores_saved",
        log_metrics=lambda data: (
            {
                "active_pairs": data.get("summary", {}).get("synergy_active_pairs", 0),
                "franchises": data.get("summary", {}).get("franchise_count", 0),
            }
            if data
            else {}
        ),
    ),
    # ========== 8-Component Structural Estimation ==========
    # AKM diagnostics
    ExportSpec(
        filename="akm_diagnostics.json",
        data_getter=lambda ctx: ctx.analysis_results.get("akm_diagnostics"),
        log_message="akm_diagnostics_saved",
        log_metrics=lambda data: (
            {"r_squared": data.get("r_squared", 0), "movers": data.get("n_movers", 0)}
            if data
            else {}
        ),
    ),
    # IV weights
    ExportSpec(
        filename="iv_weights.json",
        data_getter=lambda ctx: ctx.analysis_results.get("iv_weights"),
        log_message="iv_weights_saved",
    ),
    # Derived parameters report
    ExportSpec(
        filename="derived_params.json",
        data_getter=lambda ctx: ctx.analysis_results.get("derived_params"),
        log_message="derived_params_saved",
        log_metrics=lambda data: (
            {"sections": len(data.get("sections", {}))} if data else {}
        ),
    ),
    # Knowledge spanners
    ExportSpec(
        filename="knowledge_spanners.json",
        data_getter=lambda ctx: ctx.analysis_results.get("knowledge_spanners"),
        log_message="knowledge_spanners_saved",
        log_metrics=lambda data: {"persons": len(data)} if data else {},
    ),
    # Career friction
    ExportSpec(
        filename="career_friction.json",
        data_getter=lambda ctx: ctx.analysis_results.get("career_friction_report"),
        log_message="career_friction_saved",
        log_metrics=lambda data: (
            {"persons": data.get("total_persons", 0)} if data else {}
        ),
    ),
    # Era effects
    ExportSpec(
        filename="era_effects.json",
        data_getter=lambda ctx: ctx.analysis_results.get("era_effects"),
        log_message="era_effects_saved",
        log_metrics=lambda data: {"years": len(data.get("era_fe", {}))} if data else {},
    ),
    # Studio timeseries
    ExportSpec(
        filename="studio_timeseries.json",
        data_getter=lambda ctx: ctx.analysis_results.get("studio_timeseries"),
        log_message="studio_timeseries_saved",
        log_metrics=lambda data: (
            {"studios": data.get("studios_analyzed", 0)} if data else {}
        ),
    ),
    # Expected ability
    ExportSpec(
        filename="expected_ability.json",
        data_getter=lambda ctx: ctx.analysis_results.get("expected_ability"),
        log_message="expected_ability_saved",
        log_metrics=lambda data: (
            {
                "persons": data.get("total_persons", 0),
                "r_squared": data.get("model_r_squared"),
            }
            if data
            else {}
        ),
    ),
    # Compatibility groups
    ExportSpec(
        filename="compatibility_groups.json",
        data_getter=lambda ctx: ctx.analysis_results.get(
            "compatibility_groups_analysis"
        ),
        log_message="compatibility_groups_saved",
        log_metrics=lambda data: (
            {
                "pairs": data.get("total_pairs_analyzed", 0),
                "groups": len(data.get("compatible_groups", [])),
            }
            if data
            else {}
        ),
    ),
    # ========== Voice Actor Scores ==========
    ExportSpec(
        filename="va_scores.json",
        data_getter=lambda ctx: ctx.va_results,
        log_message="va_scores_saved",
        log_metrics=lambda data: {"persons": len(data)} if data else {},
    ),
    ExportSpec(
        filename="va_synergy.json",
        data_getter=lambda ctx: ctx.va_ensemble_synergy,
        log_message="va_synergy_saved",
        log_metrics=lambda data: {"pairs": len(data)} if isinstance(data, list) else {},
    ),
    # ========== Studio Analysis ==========
    ExportSpec(
        filename="studio_talent_density.json",
        data_getter=lambda ctx: ctx.analysis_results.get("studio_talent_density"),
        log_message="studio_talent_density_saved",
        log_metrics=lambda data: {"studios": len(data)} if data else {},
    ),
    ExportSpec(
        filename="studio_network.json",
        data_getter=lambda ctx: ctx.analysis_results.get("studio_network"),
        log_message="studio_network_saved",
    ),
    ExportSpec(
        filename="talent_pipeline.json",
        data_getter=lambda ctx: ctx.analysis_results.get("talent_pipeline"),
        log_message="talent_pipeline_saved",
    ),
    ExportSpec(
        filename="studio_clustering.json",
        data_getter=lambda ctx: ctx.analysis_results.get("studio_clustering"),
        log_message="studio_clustering_saved",
        log_metrics=lambda data: (
            {"clusters": len(data.get("cluster_names", {}))} if data else {}
        ),
    ),
    # ========== Genre Analysis ==========
    ExportSpec(
        filename="genre_ecosystem.json",
        data_getter=lambda ctx: ctx.analysis_results.get("genre_ecosystem"),
        log_message="genre_ecosystem_saved",
        log_metrics=lambda data: (
            {"genres": len(data.get("trends", {}))} if data else {}
        ),
    ),
    ExportSpec(
        filename="genre_network.json",
        data_getter=lambda ctx: ctx.analysis_results.get("genre_network"),
        log_message="genre_network_saved",
        log_metrics=lambda data: (
            {"families": len(data.get("family_names", {}))} if data else {}
        ),
    ),
    ExportSpec(
        filename="genre_quality.json",
        data_getter=lambda ctx: ctx.analysis_results.get("genre_quality"),
        log_message="genre_quality_saved",
        log_metrics=lambda data: (
            {"genres": len(data.get("quality", {}))} if data else {}
        ),
    ),
    # Pipeline summary (special case - uses elapsed time)
    ExportSpec(
        filename="summary.json",
        data_getter=lambda ctx: None,  # Summary is built by transformer
        transformer=_transform_summary,
        log_message="summary_saved",
    ),
    # ========== New audience-driven analysis outputs ==========
    ExportSpec(
        filename="entry_cohort_attrition.json",
        data_getter=lambda ctx: ctx.analysis_results.get("entry_cohort_attrition"),
        log_message="entry_cohort_attrition_saved",
        log_metrics=lambda data: {"n_cohort": data.get("n_cohort", 0)} if data else {},
    ),
    ExportSpec(
        filename="monopsony_analysis.json",
        data_getter=lambda ctx: ctx.analysis_results.get("monopsony_analysis"),
        log_message="monopsony_analysis_saved",
    ),
    ExportSpec(
        filename="gender_bottleneck.json",
        data_getter=lambda ctx: ctx.analysis_results.get("gender_bottleneck"),
        log_message="gender_bottleneck_saved",
    ),
    ExportSpec(
        filename="generational_health.json",
        data_getter=lambda ctx: ctx.analysis_results.get("generational_health"),
        log_message="generational_health_saved",
    ),
    ExportSpec(
        filename="studio_benchmark_cards.json",
        data_getter=lambda ctx: ctx.analysis_results.get("studio_benchmark_cards"),
        log_message="studio_benchmark_cards_saved",
        log_metrics=lambda data: {"n_studios": len(data)} if data else {},
    ),
    ExportSpec(
        filename="director_value_add.json",
        data_getter=lambda ctx: ctx.analysis_results.get("director_value_add"),
        log_message="director_value_add_saved",
    ),
    ExportSpec(
        filename="attrition_risk_model.json",
        data_getter=lambda ctx: ctx.analysis_results.get("attrition_risk_model"),
        condition=lambda data: bool(data and data.get("c_index", 0) >= 0.70),
        log_message="attrition_risk_model_saved",
        log_metrics=lambda data: {"c_index": data.get("c_index", 0)} if data else {},
    ),
    ExportSpec(
        filename="succession_matrix.json",
        data_getter=lambda ctx: ctx.analysis_results.get("succession_matrix"),
        log_message="succession_matrix_saved",
    ),
    ExportSpec(
        filename="team_chemistry.json",
        data_getter=lambda ctx: ctx.analysis_results.get("team_chemistry"),
        log_message="team_chemistry_saved",
    ),
    ExportSpec(
        filename="undervalued_talent.json",
        data_getter=lambda ctx: ctx.analysis_results.get("undervalued_talent"),
        log_message="undervalued_talent_saved",
    ),
    ExportSpec(
        filename="genre_whitespace.json",
        data_getter=lambda ctx: ctx.analysis_results.get("genre_whitespace"),
        log_message="genre_whitespace_saved",
    ),
    ExportSpec(
        filename="team_templates.json",
        data_getter=lambda ctx: ctx.analysis_results.get("team_templates"),
        log_message="team_templates_saved",
    ),
    ExportSpec(
        filename="trust_entry.json",
        data_getter=lambda ctx: ctx.analysis_results.get("trust_entry"),
        log_message="trust_entry_saved",
    ),
    ExportSpec(
        filename="independent_units.json",
        data_getter=lambda ctx: ctx.analysis_results.get("independent_units"),
        log_message="independent_units_saved",
    ),
    ExportSpec(
        filename="person_parameters.json",
        data_getter=lambda ctx: ctx.analysis_results.get("person_parameters"),
        log_message="person_parameters_saved",
        log_metrics=lambda data: {"persons": len(data)} if data else {},
    ),
]


def export_single_result_file(
    spec: ExportSpec,
    context: dict,
    json_dir: Path,
) -> bool:
    """Export a single result file using its ExportSpec.

    Args:
        spec: Export specification
        context: Pipeline context
        json_dir: Directory to write JSON files

    Returns:
        True if exported, False if skipped
    """
    # Get data from context
    data = spec.data_getter(context)

    # Skip if already flushed to disk by Phase 9 memory optimization
    flushed = getattr(context, "_flushed_tasks", set())
    key = spec.filename.replace(".json", "")
    if not data and key in flushed:
        logger.debug("export_skipped_already_flushed", filename=spec.filename)
        return True  # Count as exported (already on disk)

    # Check condition (if specified)
    if spec.condition and not spec.condition(data):
        logger.debug("export_skipped_condition", filename=spec.filename)
        return False

    # Skip if empty (unless transformer will create data)
    if not data and not spec.transformer:
        logger.debug("export_skipped_empty", filename=spec.filename)
        return False

    # Transform if needed
    if spec.transformer:
        data = spec.transformer(data, context)

    # Skip if still empty after transformation
    if not data:
        logger.debug("export_skipped_empty_after_transform", filename=spec.filename)
        return False

    # Save to file
    file_path = json_dir / spec.filename
    save_json_to_file(data, file_path)

    # Log with optional metrics
    log_context = {"path": str(file_path)}
    if spec.log_metrics:
        metrics = spec.log_metrics(data)
        log_context.update(metrics)

    logger.info(spec.log_message, **log_context)
    return True


def export_and_visualize_phase(context: ExportContext | dict, elapsed: float = 0.0) -> None:
    """Export all results to JSON and generate visualizations.

    Uses declarative registry to export all 26 JSON files.

    Args:
        context: Export context (lightweight) or pipeline context (full)
        elapsed: Pipeline elapsed time in seconds
    """
    from src.utils.config import JSON_DIR

    logger.info("step_start", step="json_export")
    context.monitor.record_memory("before_export")

    # Release betweenness cache (can be large, no longer needed)
    import gc

    context.betweenness_cache = {}
    gc.collect()

    # Store elapsed time in context for summary export
    context._elapsed = elapsed

    with context.monitor.measure("json_export"):
        # Ensure JSON directory exists
        JSON_DIR.mkdir(parents=True, exist_ok=True)

        # Export all registered files, releasing memory after each write
        exported_count = 0
        for spec in EXPORT_REGISTRY:
            if export_single_result_file(spec, context, JSON_DIR):
                exported_count += 1
            # Release analysis_results entry after export to free memory
            # (analysis_results can hold 500MB+ total)
            key = spec.filename.replace(".json", "")
            if key in context.analysis_results:
                del context.analysis_results[key]

        logger.info(
            "exports_complete",
            files_exported=exported_count,
            total_specs=len(EXPORT_REGISTRY),
        )

    # Persist computed features to DB (feat_* tables)
    _persist_features_to_db(context)

    # Generate visualizations if requested
    if context.visualize:
        logger.info("step_start", step="visualization")
        _generate_visualizations(context)


def _persist_features_to_db(context: dict) -> None:
    """Persist pipeline computation results to feat_* tables in gold.duckdb.

    Writes the same data as the JSON export to the GOLD layer so it can be
    reused by report generators and the API without re-running the pipeline.
    All writes are best-effort — failures do not abort the overall pipeline.
    """
    import datetime

    import duckdb

    from src.analysis.io.gold_writer import DEFAULT_GOLD_DB_PATH, _DDL

    if not context.results:
        return

    try:
        with context.monitor.measure("feat_db_persist"):
            gold_path = str(DEFAULT_GOLD_DB_PATH)
            DEFAULT_GOLD_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            conn = duckdb.connect(gold_path)
            conn.execute("SET memory_limit='2GB'")
            conn.execute(_DDL)

            now = datetime.datetime.now(datetime.timezone.utc)

            # --- feat_career ---
            career_rows: list[tuple] = []
            for d in context.results:
                car = d.get("career") or {}
                grw = d.get("growth") or {}
                career_rows.append((
                    d["person_id"],
                    car.get("first_year"),
                    car.get("latest_year"),
                    car.get("active_years"),
                    d.get("total_credits"),
                    car.get("highest_stage"),
                    d.get("primary_role"),
                    d.get("career_track"),
                    car.get("peak_year"),
                    car.get("peak_credits"),
                    grw.get("trend"),
                    d.get("growth_score"),
                    grw.get("activity_ratio"),
                    grw.get("recent_credits"),
                    now,
                ))
            if career_rows:
                conn.executemany(
                    """
                    INSERT INTO feat_career (
                        person_id, first_year, latest_year, active_years, total_credits,
                        highest_stage, primary_role, career_track,
                        peak_year, peak_credits,
                        growth_trend, growth_score, activity_ratio, recent_credits,
                        updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT (person_id) DO UPDATE SET
                        first_year=excluded.first_year,
                        latest_year=excluded.latest_year,
                        active_years=excluded.active_years,
                        total_credits=excluded.total_credits,
                        highest_stage=excluded.highest_stage,
                        primary_role=excluded.primary_role,
                        career_track=excluded.career_track,
                        peak_year=excluded.peak_year,
                        peak_credits=excluded.peak_credits,
                        growth_trend=excluded.growth_trend,
                        growth_score=excluded.growth_score,
                        activity_ratio=excluded.activity_ratio,
                        recent_credits=excluded.recent_credits,
                        updated_at=excluded.updated_at
                    """,
                    career_rows,
                )

            # --- agg_person_career (L2: raw aggregates) ---
            agg_career_rows: list[tuple] = []
            for d in context.results:
                car = d.get("career") or {}
                grw = d.get("growth") or {}
                agg_career_rows.append((
                    d["person_id"],
                    car.get("first_year"),
                    car.get("latest_year"),
                    car.get("active_years"),
                    d.get("total_credits"),
                    grw.get("recent_credits"),
                    car.get("highest_stage"),
                    d.get("primary_role"),
                    car.get("peak_year"),
                    car.get("peak_credits"),
                    now,
                ))
            if agg_career_rows:
                conn.executemany(
                    """
                    INSERT INTO agg_person_career (
                        person_id, first_year, latest_year, active_years, total_credits,
                        recent_credits, highest_stage, primary_role, peak_year, peak_credits,
                        updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT (person_id) DO UPDATE SET
                        first_year=excluded.first_year,
                        latest_year=excluded.latest_year,
                        active_years=excluded.active_years,
                        total_credits=excluded.total_credits,
                        recent_credits=excluded.recent_credits,
                        highest_stage=excluded.highest_stage,
                        primary_role=excluded.primary_role,
                        peak_year=excluded.peak_year,
                        peak_credits=excluded.peak_credits,
                        updated_at=excluded.updated_at
                    """,
                    agg_career_rows,
                )

            # --- feat_career_scores (L3: derived scores) ---
            career_score_rows: list[tuple] = []
            for d in context.results:
                grw = d.get("growth") or {}
                career_score_rows.append((
                    d["person_id"],
                    d.get("career_track"),
                    grw.get("trend"),
                    d.get("growth_score"),
                    grw.get("activity_ratio"),
                    now,
                ))
            if career_score_rows:
                conn.executemany(
                    """
                    INSERT INTO feat_career_scores (
                        person_id, career_track, growth_trend, growth_score, activity_ratio,
                        updated_at
                    ) VALUES (?,?,?,?,?,?)
                    ON CONFLICT (person_id) DO UPDATE SET
                        career_track=excluded.career_track,
                        growth_trend=excluded.growth_trend,
                        growth_score=excluded.growth_score,
                        activity_ratio=excluded.activity_ratio,
                        updated_at=excluded.updated_at
                    """,
                    career_score_rows,
                )

            # --- feat_network (base: birank/patronage/centrality from results) ---
            network_rows: list[tuple] = []
            for d in context.results:
                c = d.get("centrality") or {}
                net = d.get("network") or {}
                network_rows.append((
                    d["person_id"],
                    d.get("birank"),
                    d.get("patronage"),
                    d.get("bridge_score"),
                    d.get("n_bridge_communities"),
                    c.get("degree"),
                    c.get("betweenness"),
                    c.get("closeness"),
                    c.get("eigenvector"),
                    net.get("hub_score"),
                    net.get("collaborators"),
                    net.get("unique_anime"),
                    now,
                ))
            if network_rows:
                conn.executemany(
                    """
                    INSERT INTO feat_network (
                        person_id, birank, patronage,
                        bridge_score, n_bridge_communities,
                        degree_centrality, betweenness_centrality,
                        closeness_centrality, eigenvector_centrality,
                        hub_score, n_collaborators, n_unique_anime,
                        updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT (person_id) DO UPDATE SET
                        birank=excluded.birank,
                        patronage=excluded.patronage,
                        bridge_score=excluded.bridge_score,
                        n_bridge_communities=excluded.n_bridge_communities,
                        degree_centrality=excluded.degree_centrality,
                        betweenness_centrality=excluded.betweenness_centrality,
                        closeness_centrality=excluded.closeness_centrality,
                        eigenvector_centrality=excluded.eigenvector_centrality,
                        hub_score=excluded.hub_score,
                        n_collaborators=excluded.n_collaborators,
                        n_unique_anime=excluded.n_unique_anime,
                        updated_at=excluded.updated_at
                    """,
                    network_rows,
                )

            # --- agg_person_network (L2: raw aggregates) ---
            agg_network_rows: list[tuple] = []
            for d in context.results:
                net = d.get("network") or {}
                agg_network_rows.append((
                    d["person_id"],
                    net.get("collaborators"),
                    net.get("unique_anime"),
                    d.get("n_bridge_communities"),
                    now,
                ))
            if agg_network_rows:
                conn.executemany(
                    """
                    INSERT INTO agg_person_network (
                        person_id, n_collaborators, n_unique_anime, n_bridge_communities, updated_at
                    ) VALUES (?,?,?,?,?)
                    ON CONFLICT (person_id) DO UPDATE SET
                        n_collaborators=excluded.n_collaborators,
                        n_unique_anime=excluded.n_unique_anime,
                        n_bridge_communities=excluded.n_bridge_communities,
                        updated_at=excluded.updated_at
                    """,
                    agg_network_rows,
                )

            # --- feat_network_scores (L3: derived scores) ---
            network_score_rows: list[tuple] = []
            for d in context.results:
                c = d.get("centrality") or {}
                net = d.get("network") or {}
                network_score_rows.append((
                    d["person_id"],
                    d.get("birank"),
                    d.get("patronage"),
                    c.get("degree"),
                    c.get("betweenness"),
                    c.get("closeness"),
                    c.get("eigenvector"),
                    net.get("hub_score"),
                    d.get("bridge_score"),
                    now,
                ))
            if network_score_rows:
                conn.executemany(
                    """
                    INSERT INTO feat_network_scores (
                        person_id, birank, patronage, degree_centrality, betweenness_centrality,
                        closeness_centrality, eigenvector_centrality, hub_score, bridge_score,
                        updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT (person_id) DO UPDATE SET
                        birank=excluded.birank,
                        patronage=excluded.patronage,
                        degree_centrality=excluded.degree_centrality,
                        betweenness_centrality=excluded.betweenness_centrality,
                        closeness_centrality=excluded.closeness_centrality,
                        eigenvector_centrality=excluded.eigenvector_centrality,
                        hub_score=excluded.hub_score,
                        bridge_score=excluded.bridge_score,
                        updated_at=excluded.updated_at
                    """,
                    network_score_rows,
                )

            # --- feat_network bridge update (from bridges analysis) ---
            bridges_analysis = context.analysis_results.get("bridges") or {}
            bridge_persons = bridges_analysis.get("bridge_persons") or []
            if bridge_persons:
                bridge_updates: list[tuple] = [
                    (
                        bp.get("bridge_score"),
                        bp.get("communities_connected"),
                        now,
                        bp["person_id"],
                    )
                    for bp in bridge_persons
                    if "person_id" in bp
                ]
                if bridge_updates:
                    conn.executemany(
                        """
                        UPDATE feat_network
                        SET bridge_score = ?,
                            n_bridge_communities = ?,
                            updated_at = ?
                        WHERE person_id = ?
                        """,
                        bridge_updates,
                    )
                    # Also update feat_network_scores (L3 scores)
                    conn.executemany(
                        """
                        UPDATE feat_network_scores
                        SET bridge_score = ?,
                            updated_at = ?
                        WHERE person_id = ?
                        """,
                        [
                            (bp.get("bridge_score"), now, bp["person_id"])
                            for bp in bridge_persons
                            if "person_id" in bp
                        ],
                    )

            # --- feat_genre_affinity ---
            genre_data = context.analysis_results.get("genre_affinity") or {}
            genre_rows: list[tuple] = []
            if isinstance(genre_data, dict):
                for pid, genres in genre_data.items():
                    if isinstance(genres, dict):
                        for genre, info in genres.items():
                            score = info.get("score") if isinstance(info, dict) else info
                            work_count = info.get("count") if isinstance(info, dict) else None
                            genre_rows.append((pid, genre, score, work_count, now))
            if genre_rows:
                conn.executemany(
                    """
                    INSERT INTO feat_genre_affinity
                        (person_id, genre, affinity_score, work_count, updated_at)
                    VALUES (?,?,?,?,?)
                    ON CONFLICT (person_id, genre) DO UPDATE SET
                        affinity_score=excluded.affinity_score,
                        work_count=excluded.work_count,
                        updated_at=excluded.updated_at
                    """,
                    genre_rows,
                )

            # --- feat_contribution ---
            contrib_data = context.analysis_results.get("individual_profiles") or {}
            contrib_rows: list[tuple] = []
            if isinstance(contrib_data, dict):
                for pid, prof in contrib_data.items():
                    if isinstance(prof, dict):
                        contrib_rows.append((
                            pid,
                            prof.get("peer_percentile"),
                            prof.get("opportunity_residual"),
                            prof.get("consistency"),
                            prof.get("independent_value"),
                            now,
                        ))
            elif isinstance(contrib_data, list):
                for prof in contrib_data:
                    if isinstance(prof, dict) and "person_id" in prof:
                        contrib_rows.append((
                            prof["person_id"],
                            prof.get("peer_percentile"),
                            prof.get("opportunity_residual"),
                            prof.get("consistency"),
                            prof.get("independent_value"),
                            now,
                        ))
            if contrib_rows:
                conn.executemany(
                    """
                    INSERT INTO feat_contribution (
                        person_id, peer_percentile, opportunity_residual,
                        consistency_score, independent_value, updated_at
                    ) VALUES (?,?,?,?,?,?)
                    ON CONFLICT (person_id) DO UPDATE SET
                        peer_percentile=excluded.peer_percentile,
                        opportunity_residual=excluded.opportunity_residual,
                        consistency_score=excluded.consistency_score,
                        independent_value=excluded.independent_value,
                        updated_at=excluded.updated_at
                    """,
                    contrib_rows,
                )

            # --- agg_milestones ---
            milestones_raw = _get_analysis_result(context, "milestones", {})
            if isinstance(milestones_raw, dict) and milestones_raw:
                milestone_rows: list[tuple] = []
                for person_id, events in milestones_raw.items():
                    if not isinstance(events, list):
                        continue
                    for ev in events:
                        if not isinstance(ev, dict):
                            continue
                        milestone_rows.append((
                            person_id,
                            ev.get("type") or "",
                            int(ev.get("year") or 0),
                            ev.get("anime_id") or "",
                            ev.get("anime_title"),
                            ev.get("description"),
                        ))
                if milestone_rows:
                    conn.executemany(
                        """
                        INSERT INTO agg_milestones
                            (person_id, event_type, year, anime_id, anime_title, description)
                        VALUES (?,?,?,?,?,?)
                        ON CONFLICT (person_id, event_type, year, anime_id) DO UPDATE SET
                            anime_title=excluded.anime_title,
                            description=excluded.description
                        """,
                        milestone_rows,
                    )

            # --- agg_director_circles ---
            if context.circles:
                _persist_director_circles_duckdb(conn, context.circles, now)

            # --- feat_mentorships ---
            mentorships_raw = _get_analysis_result(context, "mentorships", [])
            if isinstance(mentorships_raw, list) and mentorships_raw:
                mentorship_rows: list[tuple] = [
                    (
                        m["mentor_id"],
                        m["mentee_id"],
                        m.get("n_shared_works") or 0,
                        m.get("hit_rate"),
                        m.get("mentor_stage"),
                        m.get("mentee_stage"),
                        m.get("first_year"),
                        m.get("latest_year"),
                    )
                    for m in mentorships_raw
                    if isinstance(m, dict) and m.get("mentor_id") and m.get("mentee_id")
                ]
                if mentorship_rows:
                    conn.executemany(
                        """
                        INSERT INTO feat_mentorships (
                            mentor_id, mentee_id, n_shared_works, hit_rate,
                            mentor_stage, mentee_stage, first_year, latest_year
                        ) VALUES (?,?,?,?,?,?,?,?)
                        ON CONFLICT (mentor_id, mentee_id) DO UPDATE SET
                            n_shared_works=excluded.n_shared_works,
                            hit_rate=excluded.hit_rate,
                            mentor_stage=excluded.mentor_stage,
                            mentee_stage=excluded.mentee_stage,
                            first_year=excluded.first_year,
                            latest_year=excluded.latest_year
                        """,
                        mentorship_rows,
                    )

            logger.info(
                "feat_tables_persisted",
                scores=len(context.results),
                genre_rows=len(genre_rows),
                contrib_rows=len(contrib_rows),
            )

            # feat_causal_estimates: persist causal inference results from pipeline context
            try:
                _persist_causal_estimates_duckdb(conn, context, now)
            except Exception:
                logger.exception("feat_causal_estimates_skipped")

            # feat_cluster_membership: aggregate multiple clustering dimensions into one row
            try:
                _persist_cluster_membership_duckdb(conn, context, now)
            except Exception:
                logger.exception("feat_cluster_membership_skipped")

            # feat_birank_annual: annual BiRank snapshots (1980 onwards)
            try:
                _persist_birank_annual_duckdb(conn, context, now)
            except Exception:
                logger.exception("feat_birank_annual_skipped")

            conn.close()
    except Exception:
        logger.exception("feat_db_persist_failed")


def _persist_director_circles_duckdb(conn: Any, circles_dict: dict, now: Any) -> None:
    """Write agg_director_circles rows from context.circles into DuckDB."""
    import dataclasses
    import json as _json

    batch: list[tuple] = []
    for director_id, circle in circles_dict.items():
        if dataclasses.is_dataclass(circle) and not isinstance(circle, type):
            circle = dataclasses.asdict(circle)
        if not isinstance(circle, dict):
            continue
        for member in circle.get("members", []):
            if dataclasses.is_dataclass(member) and not isinstance(member, type):
                member = dataclasses.asdict(member)
            if not isinstance(member, dict):
                continue
            pid = member.get("person_id")
            if not pid:
                continue
            roles = member.get("roles") or []
            batch.append((
                pid,
                director_id,
                member.get("shared_works") or 0,
                member.get("hit_rate"),
                _json.dumps(roles, ensure_ascii=False) if isinstance(roles, list) else roles,
                member.get("latest_year"),
            ))
    if not batch:
        return
    conn.executemany(
        """
        INSERT INTO agg_director_circles
            (person_id, director_id, shared_works, hit_rate, roles, latest_year)
        VALUES (?,?,?,?,?,?)
        ON CONFLICT (person_id, director_id) DO UPDATE SET
            shared_works=excluded.shared_works,
            hit_rate=excluded.hit_rate,
            roles=excluded.roles,
            latest_year=excluded.latest_year
        """,
        batch,
    )


def _persist_causal_estimates_duckdb(conn: Any, context: dict, now: Any) -> None:
    """Persist causal inference results to feat_causal_estimates in DuckDB.

    Collects per-person values from PipelineContext.peer_effect_result,
    career_friction, and era_effects. Skips the debut-year lookup that required
    a SQLite-only credits.credit_year query (era_fe mapping is omitted here;
    era_deflated_iv defaults to None).
    """
    # peer effect: person_peer_boost (per-person)
    peer_boosts: dict[str, float] = {}
    if context.peer_effect_result is not None and context.peer_effect_result.identified:
        peer_boosts = context.peer_effect_result.person_peer_boost or {}

    # career friction index (per-person)
    friction_index: dict[str, float] = context.career_friction or {}

    all_pids = (
        set(peer_boosts)
        | set(friction_index)
        | set(context.iv_scores)
    )
    if not all_pids:
        return

    rows: list[tuple] = []
    for pid in all_pids:
        iv = context.iv_scores.get(pid)
        rows.append((
            pid,
            round(peer_boosts.get(pid, 0.0), 6),
            round(friction_index.get(pid, 0.0), 6),
            None,   # era_fe: requires debut-year lookup via SQLite credits — deferred to §4.4
            None,   # era_deflated_iv: depends on era_fe
            None,   # opportunity_residual: not available here
            round(iv, 6) if iv is not None else None,
            now,
        ))

    conn.executemany(
        """
        INSERT INTO feat_causal_estimates (
            person_id, peer_effect_boost, career_friction,
            era_fe, era_deflated_iv, opportunity_residual,
            iv_score, updated_at
        ) VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT (person_id) DO UPDATE SET
            peer_effect_boost=excluded.peer_effect_boost,
            career_friction=excluded.career_friction,
            era_fe=CASE WHEN excluded.era_fe IS NOT NULL THEN excluded.era_fe ELSE era_fe END,
            era_deflated_iv=CASE WHEN excluded.era_deflated_iv IS NOT NULL THEN excluded.era_deflated_iv ELSE era_deflated_iv END,
            opportunity_residual=CASE WHEN excluded.opportunity_residual IS NOT NULL THEN excluded.opportunity_residual ELSE opportunity_residual END,
            iv_score=excluded.iv_score,
            updated_at=excluded.updated_at
        """,
        rows,
    )
    logger.info("feat_causal_estimates_written_duckdb", count=len(rows))


def _persist_cluster_membership_duckdb(conn: Any, context: dict, now: Any) -> None:
    """Persist each clustering dimension to feat_cluster_membership in DuckDB.

    - community_map (Phase 4 graph communities)
    - career_tracks (Phase 6)
    - growth_trend (Phase 9 analysis: growth)
    - studio_cluster (Phase 9 analysis: studio_clustering)
    - cooccurrence_group (Phase 9 analysis: cooccurrence_groups)
    """
    growth_data = _get_analysis_result(context, "growth", {})
    studio_clustering = _get_analysis_result(context, "studio_clustering", {})
    cooccurrence_groups = _get_analysis_result(context, "cooccurrence_groups", {})

    if not (
        context.community_map
        or context.career_tracks
        or growth_data
        or studio_clustering
        or cooccurrence_groups
    ):
        return

    # --- growth_trend per person ---
    growth_trend_map: dict[str, str] = {}
    persons_data = growth_data.get("persons", {}) if isinstance(growth_data, dict) else {}
    if isinstance(persons_data, dict):
        for pid, info in persons_data.items():
            if isinstance(info, dict) and "trend" in info:
                growth_trend_map[pid] = info["trend"]
    elif isinstance(persons_data, list):
        for info in persons_data:
            if isinstance(info, dict) and "person_id" in info and "trend" in info:
                growth_trend_map[info["person_id"]] = info["trend"]

    # --- studio cluster per person ---
    studio_cluster_map: dict[str, tuple] = {}
    assignments: dict = {}
    if isinstance(studio_clustering, dict):
        assignments = studio_clustering.get("assignments", {})
    # Build person→studio from feat_studio_affiliation if available in DuckDB
    studio_affiliation: dict[str, str] = {}
    try:
        rows_sa = conn.execute("""
            SELECT person_id, studio_name
            FROM feat_studio_affiliation
            WHERE is_main_studio = 1
               OR n_works = (
                   SELECT MAX(s2.n_works) FROM feat_studio_affiliation s2
                   WHERE s2.person_id = feat_studio_affiliation.person_id
               )
            GROUP BY person_id, studio_name
        """).fetchall()
        for row in rows_sa:
            studio_affiliation[row[0]] = row[1]
    except Exception:
        pass
    for pid, studio_name in studio_affiliation.items():
        info = assignments.get(studio_name)
        if info and isinstance(info, dict):
            studio_cluster_map[pid] = (info.get("cluster_id"), info.get("cluster_name"))

    # --- cooccurrence_group_id per person ---
    cooccurrence_map: dict[str, int] = {}
    groups: list = []
    if isinstance(cooccurrence_groups, dict):
        groups = cooccurrence_groups.get("groups", [])
    for idx, group in enumerate(groups):
        if isinstance(group, dict):
            for pid in group.get("members", []):
                cooccurrence_map[pid] = idx

    all_pids: set[str] = (
        set(context.community_map or {})
        | set(context.career_tracks or {})
        | set(growth_trend_map)
        | set(studio_cluster_map)
        | set(cooccurrence_map)
    )
    if not all_pids:
        return

    community_map = context.community_map or {}
    career_tracks = context.career_tracks or {}
    rows_out: list[tuple] = []
    for pid in all_pids:
        sc = studio_cluster_map.get(pid, (None, None))
        rows_out.append((
            pid,
            community_map.get(pid),
            career_tracks.get(pid),
            growth_trend_map.get(pid),
            sc[0],
            sc[1],
            cooccurrence_map.get(pid),
            now,
        ))

    conn.executemany(
        """
        INSERT INTO feat_cluster_membership (
            person_id, community_id, career_track, growth_trend,
            studio_cluster_id, studio_cluster_name, cooccurrence_group_id,
            updated_at
        ) VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT (person_id) DO UPDATE SET
            community_id=excluded.community_id,
            career_track=excluded.career_track,
            growth_trend=excluded.growth_trend,
            studio_cluster_id=excluded.studio_cluster_id,
            studio_cluster_name=excluded.studio_cluster_name,
            cooccurrence_group_id=excluded.cooccurrence_group_id,
            updated_at=excluded.updated_at
        """,
        rows_out,
    )
    logger.info("feat_cluster_membership_written_duckdb", count=len(rows_out))


def _persist_birank_annual_duckdb(conn: Any, context: dict, now: Any) -> None:
    """Persist annual BiRank snapshots to feat_birank_annual in DuckDB.

    No-ops when temporal_pagerank returned None (skipped due to no data changes).
    birank_compute_state (SQLite incremental caching table) is not written here —
    that table is SQLite-only and will be migrated in §4.4.
    """
    _BIRANK_ANNUAL_MIN_YEAR = 1980

    temporal_pr = _get_analysis_result(context, "temporal_pagerank", None)
    if not temporal_pr:
        return

    birank_timelines = temporal_pr.get("birank_timelines", {})
    if not birank_timelines:
        return

    rows: list[tuple] = []
    for pid, tl in birank_timelines.items():
        for snap in tl.get("snapshots", []):
            year = snap.get("year")
            birank = snap.get("birank")
            if year is None or birank is None or year < _BIRANK_ANNUAL_MIN_YEAR:
                continue
            rows.append((
                pid,
                year,
                float(birank),
                snap.get("raw_pagerank"),
                snap.get("graph_size"),
                snap.get("n_credits_cumulative"),
                now,
            ))

    if not rows:
        return

    conn.executemany(
        """
        INSERT INTO feat_birank_annual (
            person_id, year, birank, raw_pagerank,
            graph_size, n_credits_cumulative, updated_at
        ) VALUES (?,?,?,?,?,?,?)
        ON CONFLICT (person_id, year) DO UPDATE SET
            birank=excluded.birank,
            raw_pagerank=excluded.raw_pagerank,
            graph_size=excluded.graph_size,
            n_credits_cumulative=excluded.n_credits_cumulative,
            updated_at=excluded.updated_at
        """,
        rows,
    )
    logger.info("feat_birank_annual_written_duckdb", count=len(rows), min_year=_BIRANK_ANNUAL_MIN_YEAR)


def _get_analysis_result(context: dict, key: str, default: Any) -> Any:
    """Get analysis result from context, loading from JSON if flushed to disk."""
    import json as _json

    from src.utils.config import JSON_DIR

    val = context.analysis_results.get(key)
    if val is not None:
        return val
    # May have been flushed to JSON by Phase 9 OOM prevention
    json_path = JSON_DIR / f"{key}.json"
    if json_path.exists():
        try:
            with open(json_path, encoding="utf-8") as f:
                return _json.load(f)
        except Exception:
            pass
    return default


def _generate_visualizations(context: dict) -> None:
    """Generate visualizations using matplotlib and plotly.

    Args:
        context: Pipeline context
    """
    from src.analysis.visualize import (
        plot_anime_stats,
        plot_bridge_analysis,
        plot_collaboration_network,
        plot_collaboration_strength,
        plot_crossval_stability,
        plot_decade_comparison,
        plot_genre_affinity,
        plot_growth_trends,
        plot_influence_tree,
        plot_milestone_summary,
        plot_network_evolution,
        plot_outlier_summary,
        plot_performance_metrics,
        plot_productivity_distribution,
        plot_role_flow_sankey,
        plot_score_distribution,
        plot_seasonal_trends,
        plot_studio_comparison,
        plot_tag_summary,
        plot_time_series,
        plot_top_persons_radar,
        plot_transition_heatmap,
    )

    try:
        # Basic score visualizations
        scores_dict = {r["person_id"]: r for r in context.results}
        plot_score_distribution(scores_dict)
        plot_top_persons_radar(context.results, top_n=min(10, len(context.results)))

        # Collaboration network
        if context.collaboration_graph:
            iv_scores_for_plot = {
                r["person_id"]: r["iv_score"] for r in context.results
            }
            plot_collaboration_network(
                context.collaboration_graph,
                iv_scores_for_plot,
                top_n=min(50, len(context.results)),
            )

        # Growth trends
        if context.growth_data:
            trend_counts: dict[str, int] = {}
            for gd in context.growth_data.values():
                trend_counts[gd.trend] = trend_counts.get(gd.trend, 0) + 1
            plot_growth_trends({"trend_summary": trend_counts})

        # Network evolution
        network_evolution = _get_analysis_result(context, "network_evolution", {})
        if network_evolution.get("years"):
            plot_network_evolution(network_evolution)

        # Decade comparison
        decade_data = _get_analysis_result(context, "decades", {})
        if decade_data.get("decades"):
            plot_decade_comparison(decade_data)

        # Role flow
        role_flow = _get_analysis_result(context, "role_flow", {})
        if role_flow.get("links"):
            plot_role_flow_sankey(role_flow)

        # Time series
        time_series = _get_analysis_result(context, "time_series", {})
        if time_series.get("years"):
            plot_time_series(time_series)

        # Productivity (convert dataclass instances to dicts for visualization)
        productivity = _get_analysis_result(context, "productivity", {})
        if productivity:
            productivity_dicts = {
                pid: (
                    asdict(metrics)
                    if hasattr(metrics, "__dataclass_fields__")
                    else metrics
                )
                for pid, metrics in productivity.items()
            }
            plot_productivity_distribution(productivity_dicts)

        # Influence tree
        influence = _get_analysis_result(context, "influence", {})
        if influence.get("total_mentors", 0) > 0:
            plot_influence_tree(influence)

        # Milestones
        milestones = _get_analysis_result(context, "milestones", {})
        if milestones:
            plot_milestone_summary(milestones)

        # Seasonal trends
        seasonal = _get_analysis_result(context, "seasonal", {})
        if seasonal.get("by_season"):
            plot_seasonal_trends(seasonal)

        # Bridge analysis
        bridges = _get_analysis_result(context, "bridges", {})
        if bridges.get("bridge_persons"):
            plot_bridge_analysis(bridges)

        # Collaboration strength
        collaborations = _get_analysis_result(context, "collaborations", [])
        if collaborations:
            plot_collaboration_strength(collaborations[:100])

        # Tag summary
        person_tags = _get_analysis_result(context, "tags", {})
        if person_tags:
            # Build tags_data structure for visualization
            tag_summary: dict[str, int] = {}
            for tags_list in person_tags.values():
                for tag in tags_list:
                    tag_summary[tag] = tag_summary.get(tag, 0) + 1
            tags_data = {
                "tag_summary": dict(sorted(tag_summary.items(), key=lambda x: -x[1])),
                "person_tags": person_tags,
            }
            plot_tag_summary(tags_data)

        # Studio comparison
        studios = _get_analysis_result(context, "studios", {})
        if studios:
            plot_studio_comparison(studios)

        # Outlier summary
        outliers = _get_analysis_result(context, "outliers", {})
        if outliers:
            plot_outlier_summary(outliers)

        # Transition heatmap
        transitions = _get_analysis_result(context, "transitions", {})
        if transitions.get("transitions"):
            plot_transition_heatmap(transitions)

        # Anime stats
        anime_stats = _get_analysis_result(context, "anime_stats", {})
        if anime_stats:
            plot_anime_stats(anime_stats)

        # Genre affinity
        genre_affinity = _get_analysis_result(context, "genre_affinity", {})
        if genre_affinity:
            plot_genre_affinity(genre_affinity)

        # Cross-validation stability
        crossval = _get_analysis_result(context, "crossval", {})
        if crossval:
            plot_crossval_stability(crossval)

        # Performance metrics
        performance = _get_analysis_result(context, "performance", {})
        if performance:
            plot_performance_metrics(performance)

        # Generate interactive dashboard (HTML)
        from src.utils.config import JSON_DIR
        from src.runtime.report import generate_visual_dashboard

        generate_visual_dashboard(
            results=context.results,
            output_path=JSON_DIR.parent / "dashboard.html",
        )

        logger.info("visualizations_generated")
    except Exception as e:
        logger.exception("Visualization failed (non-critical)")
        logger.warning("visualization_failed", error=str(e))
