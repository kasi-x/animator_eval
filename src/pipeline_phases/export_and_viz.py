"""Phase 10: Export and Visualization — declarative registry-based exports.

This module implements a declarative export system using ExportSpec objects.
All 28 JSON exports are defined in a single registry for easy maintenance.
"""

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

import structlog

from src.pipeline_phases.context import PipelineContext
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
    data_getter: Callable[[PipelineContext], Any]
    transformer: Callable[[Any, PipelineContext], dict | list] | None = None
    condition: Callable[[Any], bool] | None = None
    log_message: str = "export_saved"
    log_metrics: Callable[[Any], dict[str, Any]] | None = None


def _build_pid_to_name(context: PipelineContext) -> dict[str, str]:
    """Build person_id → display name mapping from context results."""
    return {r["person_id"]: r["name"] or r["person_id"] for r in context.results}


def _transform_collaborations(data: list, context: PipelineContext) -> list:
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


def _transform_mentorships(data: list, context: PipelineContext) -> list:
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


def _transform_bridges(data: dict, context: PipelineContext) -> dict:
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


def _transform_influence(data: dict, context: PipelineContext) -> dict:
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


def _transform_teams(data: dict, context: PipelineContext) -> dict:
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


def _transform_mentorship_tree(data: dict, context: PipelineContext) -> dict:
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


def _transform_cooccurrence_groups(data: dict, context: PipelineContext) -> dict:
    """Add member names and anime titles to cooccurrence groups."""
    if not data:
        return {}
    pid_to_name = _build_pid_to_name(context)
    aid_to_title = {
        anime.id: anime.display_title for anime in context.anime_list
    }
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


def _transform_temporal_pagerank(data: dict, context: PipelineContext) -> dict:
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


def _transform_circles(data: dict, context: PipelineContext) -> dict:
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


def _transform_growth(data: dict, context: PipelineContext) -> dict:
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


def _transform_tags(data: dict, context: PipelineContext) -> dict:
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


def _transform_productivity(data: dict, context: PipelineContext) -> dict:
    """Transform productivity dataclass instances to dicts."""
    if not data:
        return {}
    return {pid: asdict(metrics) for pid, metrics in data.items()}


def _transform_transitions(data: dict, context: PipelineContext) -> dict:
    """Transform role transitions with dataclass to dict conversion."""
    if not data:
        return {}

    # Transitions data is already in dict format from analysis_modules.py
    # It was already converted using asdict() there, so just return as-is
    return data


def _transform_summary(data: None, context: PipelineContext) -> dict:
    """Build pipeline summary from context and elapsed time."""
    from datetime import datetime
    from src.analysis.graph import compute_graph_summary

    # Get elapsed time from context or compute it
    elapsed = getattr(context, "_elapsed", 0.0)

    # Compute graph summary if collaboration graph available
    graph_summary = {}
    if context.collaboration_graph:
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
        log_metrics=lambda data: (
            {"years": len(data.get("era_fe", {}))} if data else {}
        ),
    ),
    # Pipeline summary (special case - uses elapsed time)
    ExportSpec(
        filename="summary.json",
        data_getter=lambda ctx: None,  # Summary is built by transformer
        transformer=_transform_summary,
        log_message="summary_saved",
    ),
]


def export_single_result_file(
    spec: ExportSpec,
    context: PipelineContext,
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


def export_and_visualize_phase(context: PipelineContext, elapsed: float = 0.0) -> None:
    """Export all results to JSON and generate visualizations.

    Uses declarative registry to export all 26 JSON files.

    Args:
        context: Pipeline context
        elapsed: Pipeline elapsed time in seconds
    """
    from src.utils.config import JSON_DIR

    logger.info("step_start", step="json_export")
    context.monitor.record_memory("before_export")

    # Store elapsed time in context for summary export
    context._elapsed = elapsed

    with context.monitor.measure("json_export"):
        # Ensure JSON directory exists
        JSON_DIR.mkdir(parents=True, exist_ok=True)

        # Export all registered files
        exported_count = 0
        for spec in EXPORT_REGISTRY:
            if export_single_result_file(spec, context, JSON_DIR):
                exported_count += 1

        logger.info(
            "exports_complete",
            files_exported=exported_count,
            total_specs=len(EXPORT_REGISTRY),
        )

    # Generate visualizations if requested
    if context.visualize:
        logger.info("step_start", step="visualization")
        _generate_visualizations(context)


def _generate_visualizations(context: PipelineContext) -> None:
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
            iv_scores_for_plot = {r["person_id"]: r["iv_score"] for r in context.results}
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
        network_evolution = context.analysis_results.get("network_evolution", {})
        if network_evolution.get("years"):
            plot_network_evolution(network_evolution)

        # Decade comparison
        decade_data = context.analysis_results.get("decades", {})
        if decade_data.get("decades"):
            plot_decade_comparison(decade_data)

        # Role flow
        role_flow = context.analysis_results.get("role_flow", {})
        if role_flow.get("links"):
            plot_role_flow_sankey(role_flow)

        # Time series
        time_series = context.analysis_results.get("time_series", {})
        if time_series.get("years"):
            plot_time_series(time_series)

        # Productivity (convert dataclass instances to dicts for visualization)
        productivity = context.analysis_results.get("productivity", {})
        if productivity:
            productivity_dicts = {
                pid: asdict(metrics) for pid, metrics in productivity.items()
            }
            plot_productivity_distribution(productivity_dicts)

        # Influence tree
        influence = context.analysis_results.get("influence", {})
        if influence.get("total_mentors", 0) > 0:
            plot_influence_tree(influence)

        # Milestones
        milestones = context.analysis_results.get("milestones", {})
        if milestones:
            plot_milestone_summary(milestones)

        # Seasonal trends
        seasonal = context.analysis_results.get("seasonal", {})
        if seasonal.get("by_season"):
            plot_seasonal_trends(seasonal)

        # Bridge analysis
        bridges = context.analysis_results.get("bridges", {})
        if bridges.get("bridge_persons"):
            plot_bridge_analysis(bridges)

        # Collaboration strength
        collaborations = context.analysis_results.get("collaborations", [])
        if collaborations:
            plot_collaboration_strength(collaborations[:100])

        # Tag summary
        person_tags = context.analysis_results.get("tags", {})
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
        studios = context.analysis_results.get("studios", {})
        if studios:
            plot_studio_comparison(studios)

        # Outlier summary
        outliers = context.analysis_results.get("outliers", {})
        if outliers:
            plot_outlier_summary(outliers)

        # Transition heatmap
        transitions = context.analysis_results.get("transitions", {})
        if transitions.get("transitions"):
            plot_transition_heatmap(transitions)

        # Anime stats
        anime_stats = context.analysis_results.get("anime_stats", {})
        if anime_stats:
            plot_anime_stats(anime_stats)

        # Genre affinity
        genre_affinity = context.analysis_results.get("genre_affinity", {})
        if genre_affinity:
            plot_genre_affinity(genre_affinity)

        # Cross-validation stability
        crossval = context.analysis_results.get("crossval", {})
        if crossval:
            plot_crossval_stability(crossval)

        # Performance metrics
        performance = context.analysis_results.get("performance", {})
        if performance:
            plot_performance_metrics(performance)

        # Generate interactive dashboard (HTML)
        from src.utils.config import JSON_DIR
        from src.report import generate_visual_dashboard

        generate_visual_dashboard(
            results=context.results,
            output_path=JSON_DIR.parent / "dashboard.html",
        )

        logger.info("visualizations_generated")
    except Exception as e:
        logger.exception("Visualization failed (non-critical)")
        logger.warning("visualization_failed", error=str(e))
