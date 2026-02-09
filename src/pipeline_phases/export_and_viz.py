"""Phase 10: Export and Visualization — save results to JSON and generate visualizations."""
from dataclasses import asdict

import structlog

from src.pipeline_phases.context import PipelineContext
from src.utils.json_io import save_pipeline_json_if_data_present

logger = structlog.get_logger()


def export_and_visualize_phase(context: PipelineContext, elapsed: float = 0.0) -> None:
    """Export all results to JSON files and generate visualizations.

    This phase:
    1. Exports scores.json (main results)
    2. Exports circles.json (director circles with names)
    3. Exports all analysis results from context.analysis_results
    4. Generates visualizations if context.visualize is True

    Args:
        context: Pipeline context
        elapsed: Pipeline elapsed time in seconds
    """
    logger.info("step_start", step="json_export")
    context.monitor.record_memory("before_export")

    with context.monitor.measure("json_export"):
        # Export main scores
        save_pipeline_json_if_data_present(
            "scores.json",
            context.results,
            log_message="scores_saved",
            persons=len(context.results),
        )

        # Export director circles (with name lookups)
        if context.circles:
            pid_to_name = {r["person_id"]: r["name"] or r["person_id"] for r in context.results}
            circles_output = {}
            for dir_id, circle in context.circles.items():
                circle_dict = asdict(circle)
                circles_output[dir_id] = {
                    "director_name": pid_to_name.get(dir_id, dir_id),
                    **circle_dict,
                    "members": [
                        {**member, "name": pid_to_name.get(member["person_id"], member["person_id"])}
                        for member in circle_dict["members"]
                    ],
                }
            save_pipeline_json_if_data_present(
                "circles.json",
                circles_output,
                log_message="circles_saved",
                directors=len(circles_output),
            )

        # Export all analysis results
        _export_analysis_results(context, elapsed)

    # Generate visualizations if requested
    if context.visualize:
        logger.info("step_start", step="visualization")
        _generate_visualizations(context)


def _export_analysis_results(context: PipelineContext, elapsed: float) -> None:
    """Export all analysis module results to JSON files.

    Args:
        context: Pipeline context with analysis_results populated
        elapsed: Pipeline elapsed time in seconds
    """
    # Export anime_stats
    save_pipeline_json_if_data_present(
        "anime_stats.json",
        context.analysis_results.get("anime_stats"),
        log_message="anime_stats_saved",
        anime=len(context.analysis_results.get("anime_stats", {})),
    )

    # Export studios
    save_pipeline_json_if_data_present(
        "studios.json",
        context.analysis_results.get("studios"),
        log_message="studios_saved",
        studios=len(context.analysis_results.get("studios", {})),
    )

    # Export seasonal trends
    seasonal_data = context.analysis_results.get("seasonal")
    save_pipeline_json_if_data_present(
        "seasonal.json",
        seasonal_data,
        condition=seasonal_data.get("by_season") if seasonal_data else False,
        log_message="seasonal_saved",
    )

    # Export collaborations
    save_pipeline_json_if_data_present(
        "collaborations.json",
        context.analysis_results.get("collaborations"),
        log_message="collaborations_saved",
        pairs=len(context.analysis_results.get("collaborations", [])),
    )

    # Export outliers
    save_pipeline_json_if_data_present(
        "outliers.json",
        context.analysis_results.get("outliers"),
        log_message="outliers_saved",
    )

    # Export team patterns
    save_pipeline_json_if_data_present(
        "teams.json",
        context.analysis_results.get("teams"),
        log_message="teams_saved",
        patterns=len(context.analysis_results.get("teams", [])),
    )

    # Export time series
    save_pipeline_json_if_data_present(
        "time_series.json",
        context.analysis_results.get("time_series"),
        log_message="time_series_saved",
    )

    # Export decades
    save_pipeline_json_if_data_present(
        "decades.json",
        context.analysis_results.get("decades"),
        log_message="decades_saved",
    )

    # Export growth trends (with trend summary)
    if context.growth_data:
        # Summarize trend counts (access dataclass fields as attributes)
        trend_counts: dict[str, int] = {}
        for gd in context.growth_data.values():
            trend_counts[gd.trend] = trend_counts.get(gd.trend, 0) + 1
        # Convert dataclass instances to dicts for JSON serialization
        growth_output = {
            "trend_summary": trend_counts,
            "total_persons": len(context.growth_data),
            "persons": {
                pid: asdict(data)
                for pid, data in sorted(
                    context.growth_data.items(),
                    key=lambda x: x[1].activity_ratio,
                    reverse=True,
                )[:200]
            },
        }
        save_pipeline_json_if_data_present(
            "growth.json",
            growth_output,
            log_message="growth_saved",
            persons=len(context.growth_data),
        )

    # Export person tags (with tag summary)
    person_tags = context.analysis_results.get("tags", {})
    if person_tags:
        # Build tag summary (count of each tag)
        tag_summary: dict[str, int] = {}
        for tags_list in person_tags.values():
            for tag in tags_list:
                tag_summary[tag] = tag_summary.get(tag, 0) + 1
        tags_data = {
            "tag_summary": dict(sorted(tag_summary.items(), key=lambda x: -x[1])),
            "person_tags": person_tags,
        }
        save_pipeline_json_if_data_present(
            "tags.json",
            tags_data,
            log_message="tags_saved",
            unique_tags=len(tag_summary),
        )

    # Export role transitions
    save_pipeline_json_if_data_present(
        "transitions.json",
        context.analysis_results.get("transitions"),
        log_message="transitions_saved",
    )

    # Export role flow
    save_pipeline_json_if_data_present(
        "role_flow.json",
        context.analysis_results.get("role_flow"),
        log_message="role_flow_saved",
    )

    # Export bridges
    save_pipeline_json_if_data_present(
        "bridges.json",
        context.analysis_results.get("bridges"),
        log_message="bridges_saved",
        bridges=len(context.analysis_results.get("bridges", [])),
    )

    # Export mentorships
    save_pipeline_json_if_data_present(
        "mentorships.json",
        context.analysis_results.get("mentorships"),
        log_message="mentorships_saved",
        pairs=len(context.analysis_results.get("mentorships", [])),
    )

    # Export mentorship tree
    save_pipeline_json_if_data_present(
        "mentorship_tree.json",
        context.analysis_results.get("mentorship_tree"),
        log_message="mentorship_tree_saved",
    )

    # Export milestones
    save_pipeline_json_if_data_present(
        "milestones.json",
        context.analysis_results.get("milestones"),
        log_message="milestones_saved",
        records=len(context.analysis_results.get("milestones", [])),
    )

    # Export network evolution
    save_pipeline_json_if_data_present(
        "network_evolution.json",
        context.analysis_results.get("network_evolution"),
        log_message="network_evolution_saved",
    )

    # Export genre affinity
    save_pipeline_json_if_data_present(
        "genre_affinity.json",
        context.analysis_results.get("genre_affinity"),
        log_message="genre_affinity_saved",
        persons=len(context.analysis_results.get("genre_affinity", [])),
    )

    # Export productivity (convert dataclass instances to dicts)
    productivity_data = context.analysis_results.get("productivity", {})
    if productivity_data:
        productivity_output = {pid: asdict(metrics) for pid, metrics in productivity_data.items()}
    else:
        productivity_output = {}
    save_pipeline_json_if_data_present(
        "productivity.json",
        productivity_output,
        log_message="productivity_saved",
        persons=len(productivity_output),
    )

    # Export influence tree
    save_pipeline_json_if_data_present(
        "influence.json",
        context.analysis_results.get("influence"),
        log_message="influence_saved",
    )

    # Export cross-validation (conditional)
    save_pipeline_json_if_data_present(
        "crossval.json",
        context.analysis_results.get("crossval"),
        log_message="crossval_saved",
    )

    # Export performance monitoring summary
    save_pipeline_json_if_data_present(
        "performance.json",
        context.analysis_results.get("performance"),
        log_message="performance_saved",
    )

    # Export pipeline summary (with elapsed time)
    # Note: elapsed is passed via a closure or we compute it here
    # For now, we'll add a placeholder and update in pipeline.py
    from datetime import datetime

    from src.analysis.graph import compute_graph_summary

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

    summary = {
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
            "top_composite": context.results[0]["composite"] if context.results else 0,
            "median_composite": (
                context.results[len(context.results) // 2]["composite"] if context.results else 0
            ),
        },
        "graph": graph_summary,
        "crossval": crossval_summary,
    }
    save_pipeline_json_if_data_present(
        "summary.json",
        summary,
        log_message="summary_saved",
    )


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
            composite_scores = {r["person_id"]: r["composite"] for r in context.results}
            plot_collaboration_network(
                context.collaboration_graph, composite_scores, top_n=min(50, len(context.results))
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

        # Productivity
        productivity = context.analysis_results.get("productivity", {})
        if productivity:
            plot_productivity_distribution(productivity)

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
