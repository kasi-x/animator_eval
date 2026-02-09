"""Phase 9: Analysis Modules — independent analyses producing JSON outputs.

This phase runs 18+ independent analysis modules that don't depend on each
other. Each analysis reads from context and writes to context.analysis_results.

Future optimization: These modules can be parallelized for 4-6x speedup.
"""
import structlog

from src.analysis.anime_stats import compute_anime_stats
from src.analysis.bridges import detect_bridges
from src.analysis.collaboration_strength import compute_collaboration_strength
from src.analysis.crossval import cross_validate_scores
from src.analysis.decade_analysis import compute_decade_analysis
from src.analysis.genre_affinity import compute_genre_affinity
from src.analysis.graphml_export import export_graphml
from src.analysis.influence import compute_influence_tree
from src.analysis.mentorship import build_mentorship_tree, infer_mentorships
from src.analysis.milestones import compute_milestones
from src.analysis.network_evolution import compute_network_evolution
from src.analysis.outliers import detect_outliers
from src.analysis.person_tags import compute_person_tags
from src.analysis.productivity import compute_productivity
from src.analysis.role_flow import compute_role_flow
from src.analysis.seasonal import compute_seasonal_trends
from src.analysis.studio import compute_studio_analysis
from src.analysis.team_composition import analyze_team_patterns
from src.analysis.time_series import compute_time_series
from src.analysis.transitions import compute_role_transitions
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def run_analysis_modules_phase(context: PipelineContext) -> None:
    """Run all independent analysis modules.

    Each analysis module reads from context and produces output stored in
    context.analysis_results dict. These analyses are independent and could
    be parallelized in the future.

    Args:
        context: Pipeline context

    Updates context.analysis_results with keys:
        - anime_stats: Anime quality statistics
        - studios: Studio performance analysis
        - seasonal: Seasonal activity patterns
        - collaborations: Strongest collaboration pairs
        - outliers: Outlier detection results
        - teams: Team composition patterns
        - graphml: GraphML export status
        - time_series: Time series analysis
        - decades: Decade analysis
        - tags: Person tags
        - role_flow: Role flow analysis
        - bridges: Bridge detection in network
        - mentorships: Mentor-mentee relationships
        - mentorship_tree: Mentorship influence tree
        - milestones: Career milestones
        - network_evolution: Network evolution over time
        - genre_affinity: Genre affinity scores
        - productivity: Productivity metrics
        - transitions: Role transitions
        - influence: Influence tree
        - crossval: Cross-validation results (conditional)
        - performance: Performance monitoring summary
    """
    # Build composite scores map for analyses
    composite_scores = context.composite_scores

    # Anime statistics
    anime_quality_statistics = compute_anime_stats(
        context.credits, context.anime_map, composite_scores
    )
    context.analysis_results["anime_stats"] = anime_quality_statistics

    # Studio analysis
    studio_performance_analysis = compute_studio_analysis(
        context.credits, context.anime_map, composite_scores
    )
    context.analysis_results["studios"] = studio_performance_analysis

    # Seasonal trends
    seasonal_activity_patterns = compute_seasonal_trends(
        context.credits, context.anime_map, composite_scores
    )
    context.analysis_results["seasonal"] = seasonal_activity_patterns

    # Collaboration strength
    logger.info("step_start", step="collaboration_strength")
    with context.monitor.measure("collaboration_strength"):
        strongest_collaboration_pairs = compute_collaboration_strength(
            context.credits,
            context.anime_map,
            min_shared=2,
            person_scores=composite_scores,
        )
    context.analysis_results["collaborations"] = strongest_collaboration_pairs[:500] if strongest_collaboration_pairs else []

    # Outlier detection
    logger.info("step_start", step="outlier_detection")
    outlier_data = detect_outliers(context.results)
    context.analysis_results["outliers"] = outlier_data

    # Team composition
    logger.info("step_start", step="team_composition")
    with context.monitor.measure("team_composition"):
        # Get top-scoring persons (composite >= 70.0)
        top_persons = {r["person_id"]: r["composite"] for r in context.results if r["composite"] >= 70.0}
        team_patterns = analyze_team_patterns(
            context.credits,
            context.anime_map,
            person_scores=top_persons,
            min_score=70.0,
        )
    context.analysis_results["teams"] = team_patterns

    # GraphML export
    logger.info("step_start", step="graphml_export")
    scores_for_graphml = {
        r["person_id"]: {
            "authority": r["authority"],
            "trust": r["trust"],
            "skill": r["skill"],
            "composite": r["composite"],
            "primary_role": r.get("primary_role", ""),
        }
        for r in context.results
    }
    graphml_file = export_graphml(context.persons, context.credits, person_scores=scores_for_graphml)
    logger.info("graphml_exported", path=str(graphml_file))
    context.analysis_results["graphml"] = {"path": str(graphml_file)}

    # Time series
    logger.info("step_start", step="time_series")
    with context.monitor.measure("time_series"):
        time_series_data = compute_time_series(context.credits, context.anime_map)
    context.analysis_results["time_series"] = time_series_data

    # Decade analysis
    logger.info("step_start", step="decade_analysis")
    with context.monitor.measure("decade_analysis"):
        decade_summary = compute_decade_analysis(context.credits, context.anime_map, composite_scores)
    context.analysis_results["decades"] = decade_summary

    # Person tags (auto-labeling)
    logger.info("step_start", step="person_tags")
    with context.monitor.measure("person_tags"):
        person_tag_assignments = compute_person_tags(context.results)
        # Add tags to result entries
        if person_tag_assignments:
            for r in context.results:
                pid = r["person_id"]
                if pid in person_tag_assignments:
                    r["tags"] = person_tag_assignments[pid]
    context.analysis_results["tags"] = person_tag_assignments

    # Role transitions
    from dataclasses import asdict

    transitions = compute_role_transitions(context.credits, context.anime_map)
    # Convert dataclass objects to dicts for JSON serialization
    role_transition_patterns = {
        "transitions": [asdict(t) for t in transitions["transitions"]],
        "career_paths": [asdict(p) for p in transitions["career_paths"]],
        "avg_time_to_stage": {
            stage: asdict(stats) for stage, stats in transitions["avg_time_to_stage"].items()
        },
        "total_persons_analyzed": transitions["total_persons_analyzed"],
    }
    context.analysis_results["transitions"] = role_transition_patterns

    # Role flow
    logger.info("step_start", step="role_flow")
    with context.monitor.measure("role_flow"):
        role_flow_data = compute_role_flow(context.credits, context.anime_map)
    context.analysis_results["role_flow"] = role_flow_data

    # Bridge detection
    logger.info("step_start", step="bridge_detection")
    with context.monitor.measure("bridge_detection"):
        bridge_data = detect_bridges(context.credits)
        context.analysis_results["bridges"] = bridge_data

    # Mentorship inference
    logger.info("step_start", step="mentorship_inference")
    with context.monitor.measure("mentorship_inference"):
        mentorships = infer_mentorships(context.credits, context.anime_map, min_shared_works=3)
        context.analysis_results["mentorships"] = mentorships

        # Build mentorship tree from mentorships
        mentorship_tree_data = build_mentorship_tree(mentorships)
        context.analysis_results["mentorship_tree"] = mentorship_tree_data

    # Milestones
    logger.info("step_start", step="milestones")
    with context.monitor.measure("milestones"):
        milestone_records = compute_milestones(context.credits, context.anime_map)
    context.analysis_results["milestones"] = milestone_records

    # Network evolution
    logger.info("step_start", step="network_evolution")
    with context.monitor.measure("network_evolution"):
        network_evolution_data = compute_network_evolution(
            context.credits,
            context.anime_map,
        )
    context.analysis_results["network_evolution"] = network_evolution_data

    # Genre affinity
    logger.info("step_start", step="genre_affinity")
    with context.monitor.measure("genre_affinity"):
        person_genre_specialization = compute_genre_affinity(context.credits, context.anime_map)
        # Save top 200 by total_credits
        if person_genre_specialization:
            genre_affinity_data = dict(
                sorted(person_genre_specialization.items(), key=lambda x: x[1]["total_credits"], reverse=True)[:200]
            )
        else:
            genre_affinity_data = {}
    context.analysis_results["genre_affinity"] = genre_affinity_data

    # Productivity
    logger.info("step_start", step="productivity")
    with context.monitor.measure("productivity"):
        productivity_metrics = compute_productivity(context.credits, context.anime_map)
    context.analysis_results["productivity"] = productivity_metrics

    # Influence tree
    logger.info("step_start", step="influence_tree")
    with context.monitor.measure("influence_tree"):
        influence_analysis = compute_influence_tree(
            context.credits,
            context.anime_map,
            composite_scores,
        )
    context.analysis_results["influence"] = influence_analysis

    # Cross-validation (conditional: expensive operation, skip if too many persons)
    if len(context.results) < 200:
        logger.info("step_start", step="cross_validation")
        with context.monitor.measure("cross_validation"):
            xval_results = cross_validate_scores(
                context.persons,
                context.anime_list,
                context.credits,
                n_folds=5 if len(context.results) >= 100 else 3,
            )
        context.analysis_results["crossval"] = xval_results
    else:
        logger.info("cross_validation_skipped", reason="too_many_persons", count=len(context.results))
        context.analysis_results["crossval"] = {}

    # Performance monitoring summary
    context.analysis_results["performance"] = context.monitor.get_summary()
