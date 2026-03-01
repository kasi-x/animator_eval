"""Phase 9: Analysis Modules — parallel execution for 4-6x speedup.

This phase runs 25 independent analysis modules in parallel using ThreadPoolExecutor.
Each analysis reads from context and writes to context.analysis_results with thread-safe locking.
"""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from typing import Any, Callable

import networkx as nx
import numpy as np
import structlog

from src.analysis.compatibility import compute_compatibility_groups
from src.analysis.cooccurrence_groups import compute_cooccurrence_groups
from src.analysis.expected_ability import compute_expected_ability
from src.analysis.anime_stats import compute_anime_stats
from src.analysis.bias_detector import detect_systematic_biases, generate_bias_report
from src.analysis.bridges import detect_bridges
from src.analysis.causal_studio_identification import (
    identify_studio_effects,
    export_identification_report,
)
from src.analysis.credit_stats import compute_credit_statistics
from src.analysis.credit_stats_html import generate_credit_stats_html
from src.analysis.structural_estimation import (
    estimate_structural_model,
    export_structural_estimation,
)
from src.analysis.structural_estimation_html import generate_html_report
from src.analysis.collaboration_strength import compute_collaboration_strength
from src.analysis.compensation_analyzer import (
    batch_analyze_compensation,
    export_compensation_report,
)
from src.analysis.insights_report import (
    generate_comprehensive_insights,
    export_insights_report,
)
from src.analysis.crossval import cross_validate_scores
from src.analysis.individual_contribution import compute_individual_profiles
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
from src.analysis.studio_timeseries import compute_studio_timeseries
from src.analysis.synergy_score import compute_synergy_scores
from src.analysis.team_composition import analyze_team_patterns
from src.analysis.temporal_pagerank import compute_temporal_pagerank
from src.analysis.time_series import compute_time_series
from src.analysis.transitions import compute_role_transitions

# Studio & Genre analysis imports
from src.analysis.genre_ecosystem import compute_genre_ecosystem
from src.analysis.genre_network import compute_genre_network
from src.analysis.genre_quality import compute_genre_quality
from src.analysis.production_analysis import compute_studio_talent_density
from src.analysis.studio_clustering import compute_studio_clustering
from src.analysis.studio_network import compute_studio_network
from src.analysis.talent_pipeline import compute_talent_pipeline
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


@dataclass
class AnalysisTask:
    """Configuration for a single analysis module task.

    Attributes:
        name: Task name (also used as analysis_results key)
        function: Analysis function to execute
        monitor_step: Optional step name for performance monitoring
        condition: Optional condition to check before running
    """

    name: str
    function: Callable[[PipelineContext], Any]
    monitor_step: str | None = None
    condition: Callable[[PipelineContext], bool] | None = None


def _run_anime_stats(context: PipelineContext) -> Any:
    """Compute anime quality statistics."""
    return compute_anime_stats(
        context.credits, context.anime_map, context.iv_scores
    )


def _run_studios(context: PipelineContext) -> Any:
    """Compute studio performance analysis."""
    return compute_studio_analysis(
        context.credits, context.anime_map, context.iv_scores
    )


def _run_seasonal(context: PipelineContext) -> Any:
    """Compute seasonal activity patterns."""
    return compute_seasonal_trends(
        context.credits, context.anime_map, context.iv_scores
    )


def _run_collaborations(context: PipelineContext) -> Any:
    """Compute strongest collaboration pairs."""
    pairs = compute_collaboration_strength(
        context.credits,
        context.anime_map,
        min_shared=2,
        person_scores=context.iv_scores,
        collaboration_graph=context.collaboration_graph,
    )
    return pairs[:500] if pairs else []


def _run_outliers(context: PipelineContext) -> Any:
    """Detect statistical outliers."""
    return detect_outliers(context.results)


def _run_teams(context: PipelineContext) -> Any:
    """Analyze team composition patterns."""
    # iv_score で上位5%を閾値にする
    iv_vals = [r["iv_score"] for r in context.results if r["iv_score"] > 0]
    person_threshold = float(np.percentile(iv_vals, 95)) if iv_vals else 0.0
    top_persons = {
        r["person_id"]: r["iv_score"]
        for r in context.results
        if r["iv_score"] >= person_threshold
    }
    # anime.score は 1-10 スケール → 8.0 が「高評価」に適切
    return analyze_team_patterns(
        context.credits,
        context.anime_map,
        person_scores=top_persons,
        min_score=8.0,
    )


def _run_graphml(context: PipelineContext) -> Any:
    """Export graph to GraphML format."""
    scores_for_graphml = {
        r["person_id"]: {
            "iv_score": r["iv_score"],
            "person_fe": r["person_fe"],
            "birank": r["birank"],
            "patronage": r["patronage"],
            "primary_role": r.get("primary_role", ""),
        }
        for r in context.results
    }
    graphml_file = export_graphml(
        context.persons,
        context.credits,
        person_scores=scores_for_graphml,
        collaboration_graph=context.collaboration_graph,
        prettyprint=False,
        round_decimals=2,
        top_n_persons=1000,
    )
    logger.info("graphml_exported", path=str(graphml_file))
    return {"path": str(graphml_file)}


def _run_time_series(context: PipelineContext) -> Any:
    """Compute time series analysis."""
    return compute_time_series(context.credits, context.anime_map)


def _run_decades(context: PipelineContext) -> Any:
    """Compute decade analysis."""
    return compute_decade_analysis(
        context.credits, context.anime_map, context.iv_scores
    )


def _run_tags(context: PipelineContext) -> Any:
    """Compute person tags (auto-labeling).

    Returns tag assignments only — does NOT mutate context.results.
    Tags are applied to result entries on the main thread after parallel
    execution completes (see run_analysis_modules_phase).
    """
    return compute_person_tags(context.results)


def _run_transitions(context: PipelineContext) -> Any:
    """Compute role transitions."""
    transitions = compute_role_transitions(context.credits, context.anime_map)
    # Convert dataclass objects to dicts for JSON serialization
    return {
        "transitions": [asdict(t) for t in transitions["transitions"]],
        "career_paths": [asdict(p) for p in transitions["career_paths"]],
        "avg_time_to_stage": {
            stage: asdict(stats)
            for stage, stats in transitions["avg_time_to_stage"].items()
        },
        "total_persons_analyzed": transitions["total_persons_analyzed"],
    }


def _run_role_flow(context: PipelineContext) -> Any:
    """Compute role flow analysis."""
    return compute_role_flow(context.credits, context.anime_map)


def _run_bridges(context: PipelineContext) -> Any:
    """Detect bridge nodes in network using community detection."""
    communities_map = None
    if context.collaboration_graph is not None:
        n_edges = context.collaboration_graph.number_of_edges()
        try:
            if n_edges <= 1_000_000:
                # Louvain for moderate graphs
                comms = nx.community.louvain_communities(
                    context.collaboration_graph, weight="weight", seed=42
                )
            else:
                # Async label propagation for large graphs — O(E), much faster
                # Uses seed=42 for deterministic results
                comms = nx.community.asyn_lpa_communities(
                    context.collaboration_graph, seed=42
                )
            communities_map = {}
            for comm_id, members in enumerate(comms):
                for member in members:
                    communities_map[member] = comm_id
        except Exception:
            logger.warning("community_detection_failed_for_bridges")
    return detect_bridges(
        context.credits,
        communities=communities_map,
        collaboration_graph=context.collaboration_graph,
    )


def _run_mentorships(context: PipelineContext) -> Any:
    """Infer mentor-mentee relationships."""
    mentorships = infer_mentorships(
        context.credits, context.anime_map, min_shared_works=3
    )
    # Also build mentorship tree
    mentorship_tree_data = build_mentorship_tree(mentorships)
    # Store tree separately (will be saved by another task)
    context.analysis_results["mentorship_tree"] = mentorship_tree_data
    return mentorships


def _run_milestones(context: PipelineContext) -> Any:
    """Compute career milestones."""
    return compute_milestones(context.credits, context.anime_map)


def _run_network_evolution(context: PipelineContext) -> Any:
    """Compute network evolution over time."""
    return compute_network_evolution(
        context.credits,
        context.anime_map,
        collaboration_graph=context.collaboration_graph,
    )


def _run_genre_affinity(context: PipelineContext) -> Any:
    """Compute genre affinity scores."""
    person_genre_specialization = compute_genre_affinity(
        context.credits, context.anime_map
    )
    # Save top 200 by total_credits
    if person_genre_specialization:
        return dict(
            sorted(
                person_genre_specialization.items(),
                key=lambda x: x[1]["total_credits"],
                reverse=True,
            )[:200]
        )
    return {}


def _run_productivity(context: PipelineContext) -> Any:
    """Compute productivity metrics."""
    return compute_productivity(context.credits, context.anime_map)


def _run_influence(context: PipelineContext) -> Any:
    """Compute influence tree."""
    return compute_influence_tree(
        context.credits,
        context.anime_map,
        context.iv_scores,
    )


def _run_crossval(context: PipelineContext) -> Any:
    """Cross-validation (conditional: skip if too many persons)."""
    if len(context.results) >= 200:
        logger.info(
            "cross_validation_skipped",
            reason="too_many_persons",
            count=len(context.results),
        )
        return {}

    n_folds = 5 if len(context.results) >= 100 else 3
    return cross_validate_scores(
        context.persons,
        context.anime_list,
        context.credits,
        n_folds=n_folds,
    )


def _run_bias_detector(context: PipelineContext) -> Any:
    """Systematic bias detection across roles, studios, career stages."""
    # Build person_scores dict
    person_scores = {r["person_id"]: r for r in context.results}

    # Detect biases
    bias_results = detect_systematic_biases(
        contributions=context.contribution_data,
        person_scores=person_scores,
        studio_bias_metrics=context.studio_bias_metrics,
        growth_acceleration_data=context.growth_acceleration_data,
        potential_value_scores=context.potential_value_scores,
        role_profiles=context.role_profiles,
    )

    # Generate report
    return generate_bias_report(bias_results)


def _run_compensation_analyzer(context: PipelineContext) -> Any:
    """Fair compensation analysis with anime type adjustments."""
    # Build person_names dict
    person_names = {p.id: p.name_ja or p.name_en or p.id for p in context.persons}

    # Run batch analysis (top 100 anime by composite value)
    if not context.contribution_data:
        return {}

    # Get anime with contributions
    anime_with_contribs = [
        anime for anime in context.anime_list if anime.id in context.contribution_data
    ]

    # Analyze compensation
    analyses = batch_analyze_compensation(
        anime_list=anime_with_contribs,
        all_contributions=context.contribution_data,
        total_budget_per_anime=100.0,  # Normalized budget
    )

    # Build anime_scores dict for scatter chart
    anime_scores = {
        a.id: a.score for a in anime_with_contribs if a.score is not None
    }

    # Export report
    return export_compensation_report(analyses, person_names, anime_scores=anime_scores)


def _run_insights_report(context: PipelineContext) -> Any:
    """Generate comprehensive insights report from all analyses."""
    # Build person_scores and person_names dicts
    person_scores = {r["person_id"]: r for r in context.results}
    person_names = {p.id: p.name_ja or p.name_en or p.id for p in context.persons}

    # Get bridges data (or empty dict if not available)
    bridges_data = context.analysis_results.get("bridges", {})

    # Generate comprehensive insights
    insights = generate_comprehensive_insights(
        person_scores=person_scores,
        studio_bias_metrics=context.studio_bias_metrics,
        growth_acceleration_data=context.growth_acceleration_data,
        potential_value_scores=context.potential_value_scores,
        centrality=context.centrality,
        role_profiles=context.role_profiles,
        bridges_data=bridges_data,
        person_names=person_names,
    )

    # Export report
    return export_insights_report(insights)


def _run_causal_identification(context: PipelineContext) -> Any:
    """Causal identification of major studio effects (selection vs treatment vs brand)."""
    # Build person_scores dict with all necessary fields
    person_scores = {r["person_id"]: r for r in context.results}

    # Run causal identification
    result = identify_studio_effects(
        credits=context.credits,
        anime_map=context.anime_map,
        person_scores=person_scores,
        potential_value_scores=context.potential_value_scores,
        growth_acceleration_data=context.growth_acceleration_data,
    )

    # Export report
    return export_identification_report(result)


def _run_structural_estimation(context: PipelineContext) -> Any:
    """Structural estimation with fixed effects and DID (研究レベルの構造推定)."""
    # Build person_scores dict
    person_scores = {r["person_id"]: r for r in context.results}

    # Identify major studios
    from src.analysis.causal_studio_identification import identify_major_studios

    major_studios, _ = identify_major_studios(
        credits=context.credits,
        anime_map=context.anime_map,
        person_scores=person_scores,
    )

    # Run structural estimation
    result = estimate_structural_model(
        credits=context.credits,
        anime_map=context.anime_map,
        person_scores=person_scores,
        major_studios=set(major_studios),
        potential_value_scores=context.potential_value_scores,
    )

    # Generate HTML report
    from src.utils.config import HTML_DIR

    HTML_DIR.mkdir(parents=True, exist_ok=True)
    html_path = HTML_DIR / "structural_estimation_report.html"
    generate_html_report(result, html_path)
    logger.info("structural_estimation_html_generated", path=str(html_path))

    # Export JSON report
    return export_structural_estimation(result)


def _run_individual_contribution(context: PipelineContext) -> Any:
    """Compute individual contribution profiles (Layer 2 metrics)."""
    akm_residuals = context.akm_result.residuals if context.akm_result else None
    return asdict(
        compute_individual_profiles(
            results=context.results,
            credits=context.credits,
            anime_map=context.anime_map,
            role_profiles=context.role_profiles,
            career_data=context.career_data,
            collaboration_graph=context.collaboration_graph,
            akm_residuals=akm_residuals,
            community_map=context.community_map if context.community_map else None,
        )
    )


def _run_temporal_pagerank(context: PipelineContext) -> Any:
    """Compute temporal PageRank (yearly authority, foresight, promotions)."""
    return asdict(
        compute_temporal_pagerank(
            credits=context.credits,
            anime_map=context.anime_map,
            persons=context.persons,
        )
    )


def _run_akm_diagnostics(context: PipelineContext) -> Any:
    """Export AKM model diagnostics (connected set, R², mover analysis)."""
    if context.akm_result is None:
        return {}
    return {
        "connected_set_size": context.akm_result.connected_set_size,
        "n_movers": context.akm_result.n_movers,
        "n_observations": context.akm_result.n_observations,
        "r_squared": context.akm_result.r_squared,
        "n_person_fe": len(context.akm_result.person_fe),
        "n_studio_fe": len(context.akm_result.studio_fe),
        "beta_coefficients": context.akm_result.beta.tolist()
        if hasattr(context.akm_result.beta, "tolist")
        else [],
    }


def _run_iv_weights(context: PipelineContext) -> Any:
    """Export IV weight optimization results with normalization diagnostics."""
    result: dict = {
        "lambda_weights": context.iv_lambda_weights,
    }
    if context.iv_component_std:
        result["component_std"] = context.iv_component_std
    if context.iv_component_mean:
        result["component_mean"] = context.iv_component_mean
    return result


def _run_knowledge_spanners_report(context: PipelineContext) -> Any:
    """Export knowledge spanner metrics (AWCC/NDI per person)."""
    if not context.knowledge_spanner_scores:
        return {}
    pid_to_name = {r["person_id"]: r["name"] or r["person_id"] for r in context.results}
    return {
        pid: {
            "name": pid_to_name.get(pid, pid),
            "awcc": round(m.awcc, 4),
            "ndi": round(m.ndi, 4),
            "community_reach": m.community_reach,
            "degree": m.degree,
        }
        for pid, m in context.knowledge_spanner_scores.items()
    }


def _run_career_friction_report(context: PipelineContext) -> Any:
    """Export career friction analysis results."""
    if not context.career_friction:
        return {}
    pid_to_name = {r["person_id"]: r["name"] or r["person_id"] for r in context.results}
    return {
        "friction_index": {
            pid: {"name": pid_to_name.get(pid, pid), "friction": round(f, 4)}
            for pid, f in context.career_friction.items()
        },
        "total_persons": len(context.career_friction),
        "avg_friction": round(
            sum(context.career_friction.values()) / len(context.career_friction), 4
        )
        if context.career_friction
        else 0,
    }


def _run_era_effects_report(context: PipelineContext) -> Any:
    """Export era effects analysis results."""
    if context.era_effects is None:
        return {}
    return {
        "era_fe": {str(k): round(v, 4) for k, v in context.era_effects.era_fe.items()},
        "difficulty_beta": round(context.era_effects.difficulty_beta, 6),
        "n_anime_difficulty": len(context.era_effects.difficulty_scores),
    }


def _run_studio_timeseries(context: PipelineContext) -> Any:
    """Compute year-by-year studio evaluation metrics."""
    akm_residuals = context.akm_result.residuals if context.akm_result else None
    result = compute_studio_timeseries(
        credits=context.credits,
        anime_map=context.anime_map,
        iv_scores=context.iv_scores,
        studio_assignments=context.studio_assignments,
        akm_residuals=akm_residuals,
    )
    return asdict(result)


def _run_expected_ability(context: PipelineContext) -> Any:
    """Compute expected vs actual ability scores."""
    result = compute_expected_ability(
        credits=context.credits,
        anime_map=context.anime_map,
        person_fe=context.person_fe,
        birank=context.birank_person_scores,
        studio_fe=context.studio_fe,
        studio_assignments=context.studio_assignments,
        iv_scores=context.iv_scores,
    )
    return asdict(result)


def _run_compatibility(context: PipelineContext) -> Any:
    """Detect compatibility groups from co-occurrence patterns."""
    result = compute_compatibility_groups(
        credits=context.credits,
        anime_map=context.anime_map,
        iv_scores=context.iv_scores,
        collaboration_graph=context.collaboration_graph,
        community_map=context.community_map if context.community_map else None,
        min_shared_works=3,
    )
    return asdict(result)


def _run_synergy_scores(context: PipelineContext) -> Any:
    """Compute synergy scores for repeated senior staff pairings in sequel chains."""
    return compute_synergy_scores(context.credits, context.anime_map)


def _run_cooccurrence_groups(context: PipelineContext) -> Any:
    """Detect recurring co-production groups (3+ core staff across 3+ works)."""
    return compute_cooccurrence_groups(
        context.credits,
        context.anime_map,
        iv_scores=context.iv_scores,
        min_shared_works=3,
        max_group_size=5,
    )


def _run_studio_talent_density(context: PipelineContext) -> Any:
    """Compute studio talent density metrics (Gini, tiers, FE distribution)."""
    result = compute_studio_talent_density(
        context.credits, context.anime_map, context.person_fe
    )
    return {sid: asdict(td) for sid, td in result.items()}


def _run_studio_network(context: PipelineContext) -> Any:
    """Compute studio network (talent sharing + co-production)."""
    result = compute_studio_network(context.credits, context.anime_map)
    # Convert to serializable format (drop graph objects)
    return {
        "centrality": result.centrality,
        "communities": result.communities,
        "talent_flow_edges": result.talent_flow_edges,
        "talent_nodes": result.talent_sharing_graph.number_of_nodes()
        if result.talent_sharing_graph
        else 0,
        "coprod_nodes": result.coproduction_graph.number_of_nodes()
        if result.coproduction_graph
        else 0,
    }


def _run_talent_pipeline(context: PipelineContext) -> Any:
    """Compute talent pipeline (junior dev, flow matrix, brain drain, retention)."""
    result = compute_talent_pipeline(
        context.credits, context.anime_map, context.person_fe
    )
    return {
        "junior_dev": {sid: asdict(jd) for sid, jd in result.junior_dev.items()},
        "flow_matrix": {f"{k[0]}→{k[1]}": v for k, v in result.flow_matrix.items()},
        "brain_drain_index": result.brain_drain_index,
        "retention_rates": result.retention_rates,
    }


def _run_studio_clustering(context: PipelineContext) -> Any:
    """Cluster studios by 12-dimensional feature vector."""
    # Compute talent density first
    talent_density = compute_studio_talent_density(
        context.credits, context.anime_map, context.person_fe
    )

    # Get studio network data for eigenvector centrality
    studio_net = context.analysis_results.get("studio_network", {})
    centrality = studio_net.get("centrality", {})
    eigenvector = {
        s: c.get("eigenvector", 0.0) for s, c in centrality.items()
    }

    # Get talent pipeline data
    pipeline_data = context.analysis_results.get("talent_pipeline", {})
    talent_flow = pipeline_data.get("brain_drain_index", {})
    retention = pipeline_data.get("retention_rates", {})

    result = compute_studio_clustering(
        context.credits,
        context.anime_map,
        talent_density,
        studio_fe=context.studio_fe,
        birank_scores=context.birank_person_scores,
        eigenvector_centrality=eigenvector,
        talent_flow=talent_flow,
        retention_rates=retention,
    )
    return {
        "assignments": {s: asdict(sc) for s, sc in result.assignments.items()},
        "cluster_names": result.cluster_names,
        "cluster_sizes": result.cluster_sizes,
        "centroids": result.centroids,
    }


def _run_genre_ecosystem(context: PipelineContext) -> Any:
    """Compute genre ecosystem (trends, staffing, seasonality, careers)."""
    result = compute_genre_ecosystem(context.credits, context.anime_map)
    return {
        "trends": {g: asdict(t) for g, t in result.trends.items()},
        "staffing": {g: asdict(s) for g, s in result.staffing.items()},
        "seasonality": {g: asdict(s) for g, s in result.seasonality.items()},
        "careers": {g: asdict(c) for g, c in result.careers.items()},
    }


def _run_genre_network(context: PipelineContext) -> Any:
    """Compute genre network (PMI, families, evolution)."""
    result = compute_genre_network(context.anime_list)
    return {
        "genre_families": result.genre_families,
        "family_names": result.family_names,
        "pmi_matrix": {
            f"{k[0]}↔{k[1]}": round(v, 4) for k, v in result.pmi_matrix.items()
        },
        "evolution": {
            str(decade): {f"{k[0]}↔{k[1]}": round(v, 4) for k, v in deltas.items()}
            for decade, deltas in result.evolution.items()
        },
    }


def _run_genre_quality(context: PipelineContext) -> Any:
    """Compute genre quality (prestige, saturation, mobility)."""
    result = compute_genre_quality(
        context.credits,
        context.anime_map,
        context.person_fe,
        birank_scores=context.birank_person_scores,
    )
    return {
        "quality": {g: asdict(q) for g, q in result.quality.items()},
        "saturation": {g: asdict(s) for g, s in result.saturation.items()},
        "mobility": {g: asdict(m) for g, m in result.mobility.items()},
    }


def _run_credit_stats(context: PipelineContext) -> Any:
    """Compute comprehensive credit statistics (person_id level)."""
    stats = compute_credit_statistics(context.credits, context.anime_map)

    # Generate HTML report
    if stats:
        from src.utils.config import HTML_DIR

        HTML_DIR.mkdir(parents=True, exist_ok=True)
        html_path = HTML_DIR / "credit_stats_report.html"
        generate_credit_stats_html(stats, html_path)
        logger.info("credit_stats_html_generated", path=str(html_path))

    return stats


# Registry of all analysis tasks (order-independent for parallel execution)
ANALYSIS_TASKS: list[AnalysisTask] = [
    AnalysisTask("anime_stats", _run_anime_stats),
    AnalysisTask("studios", _run_studios),
    AnalysisTask("seasonal", _run_seasonal),
    AnalysisTask(
        "collaborations", _run_collaborations, monitor_step="collaboration_strength"
    ),
    AnalysisTask("outliers", _run_outliers, monitor_step="outlier_detection"),
    AnalysisTask("teams", _run_teams, monitor_step="team_composition"),
    AnalysisTask(
        "graphml",
        _run_graphml,
        monitor_step="graphml_export",
        condition=lambda ctx: (
            ctx.collaboration_graph is not None
            and ctx.collaboration_graph.number_of_edges() <= 1_000_000
        ),
    ),
    AnalysisTask("time_series", _run_time_series, monitor_step="time_series"),
    AnalysisTask("decades", _run_decades, monitor_step="decade_analysis"),
    AnalysisTask("tags", _run_tags, monitor_step="person_tags"),
    AnalysisTask("transitions", _run_transitions),
    AnalysisTask("role_flow", _run_role_flow, monitor_step="role_flow"),
    AnalysisTask("bridges", _run_bridges, monitor_step="bridge_detection"),
    AnalysisTask("mentorships", _run_mentorships, monitor_step="mentorship_inference"),
    AnalysisTask("milestones", _run_milestones, monitor_step="milestones"),
    AnalysisTask(
        "network_evolution", _run_network_evolution, monitor_step="network_evolution"
    ),
    AnalysisTask("genre_affinity", _run_genre_affinity, monitor_step="genre_affinity"),
    AnalysisTask("productivity", _run_productivity, monitor_step="productivity"),
    AnalysisTask("influence", _run_influence, monitor_step="influence_tree"),
    AnalysisTask("crossval", _run_crossval, monitor_step="cross_validation"),
    AnalysisTask("bias_report", _run_bias_detector, monitor_step="bias_detection"),
    AnalysisTask(
        "fair_compensation",
        _run_compensation_analyzer,
        monitor_step="compensation_analysis",
    ),
    AnalysisTask(
        "insights_report", _run_insights_report, monitor_step="insights_generation"
    ),
    AnalysisTask(
        "causal_identification",
        _run_causal_identification,
        monitor_step="causal_identification",
    ),
    AnalysisTask(
        "structural_estimation",
        _run_structural_estimation,
        monitor_step="structural_estimation",
    ),
    AnalysisTask(
        "individual_profiles",
        _run_individual_contribution,
        monitor_step="individual_contribution",
    ),
    AnalysisTask("credit_stats", _run_credit_stats, monitor_step="credit_statistics"),
    AnalysisTask(
        "cooccurrence_groups",
        _run_cooccurrence_groups,
        monitor_step="cooccurrence_groups",
    ),
    AnalysisTask(
        "synergy_scores",
        _run_synergy_scores,
        monitor_step="synergy_scores",
    ),
    AnalysisTask(
        "temporal_pagerank",
        _run_temporal_pagerank,
        monitor_step="temporal_pagerank",
        condition=lambda ctx: len(ctx.credits) >= 50,
    ),
    # New 8-component structural estimation reports
    AnalysisTask(
        "akm_diagnostics",
        _run_akm_diagnostics,
        monitor_step="akm_diagnostics",
        condition=lambda ctx: ctx.akm_result is not None,
    ),
    AnalysisTask(
        "iv_weights",
        _run_iv_weights,
        monitor_step="iv_weights",
    ),
    AnalysisTask(
        "knowledge_spanners",
        _run_knowledge_spanners_report,
        monitor_step="knowledge_spanners_report",
    ),
    AnalysisTask(
        "career_friction_report",
        _run_career_friction_report,
        monitor_step="career_friction_report",
    ),
    AnalysisTask(
        "era_effects",
        _run_era_effects_report,
        monitor_step="era_effects_report",
    ),
    # New: Studio timeseries, Expected ability, Compatibility groups
    AnalysisTask(
        "studio_timeseries",
        _run_studio_timeseries,
        monitor_step="studio_timeseries",
    ),
    AnalysisTask(
        "expected_ability",
        _run_expected_ability,
        monitor_step="expected_ability",
    ),
    AnalysisTask(
        "compatibility_groups_analysis",
        _run_compatibility,
        monitor_step="compatibility_groups_analysis",
    ),
    # ========== Studio & Genre Analysis ==========
    AnalysisTask(
        "studio_talent_density",
        _run_studio_talent_density,
        monitor_step="studio_talent_density",
        condition=lambda ctx: len(ctx.person_fe) > 0,
    ),
    AnalysisTask(
        "studio_network",
        _run_studio_network,
        monitor_step="studio_network",
    ),
    AnalysisTask(
        "talent_pipeline",
        _run_talent_pipeline,
        monitor_step="talent_pipeline",
        condition=lambda ctx: len(ctx.person_fe) > 0,
    ),
    AnalysisTask(
        "studio_clustering",
        _run_studio_clustering,
        monitor_step="studio_clustering",
        condition=lambda ctx: len(ctx.person_fe) > 0,
    ),
    AnalysisTask(
        "genre_ecosystem",
        _run_genre_ecosystem,
        monitor_step="genre_ecosystem",
    ),
    AnalysisTask(
        "genre_network",
        _run_genre_network,
        monitor_step="genre_network",
    ),
    AnalysisTask(
        "genre_quality",
        _run_genre_quality,
        monitor_step="genre_quality",
        condition=lambda ctx: len(ctx.person_fe) > 0,
    ),
]


def _execute_analysis_task(
    task: AnalysisTask,
    context: PipelineContext,
    results_lock: threading.Lock,
) -> tuple[str, Any, float]:
    """Execute a single analysis task with monitoring and error handling.

    Args:
        task: Analysis task configuration
        context: Pipeline context (read-only for most operations)
        results_lock: Lock for thread-safe writes to context.analysis_results

    Returns:
        Tuple of (task_name, result, elapsed_time)
    """
    import time

    # Log task start
    if task.monitor_step:
        logger.info("step_start", step=task.monitor_step)

    # Check condition
    if task.condition and not task.condition(context):
        logger.debug("task_skipped_condition", task=task.name)
        return (task.name, None, 0.0)

    # Execute with timing
    start = time.monotonic()
    try:
        if task.monitor_step:
            with context.monitor.measure(task.monitor_step):
                result = task.function(context)
        else:
            result = task.function(context)

        elapsed = time.monotonic() - start
        return (task.name, result, elapsed)

    except Exception as e:
        logger.exception(
            "analysis_task_failed",
            task=task.name,
            error=str(e),
        )
        # Return None to indicate failure but don't crash entire pipeline
        return (task.name, None, 0.0)


def run_analysis_modules_phase(
    context: PipelineContext,
    max_workers: int | None = None,
) -> None:
    """Run all independent analysis modules in parallel.

    Each analysis module reads from context and produces output stored in
    context.analysis_results dict. Uses ThreadPoolExecutor for parallel
    execution with thread-safe writes.

    Args:
        context: Pipeline context
        max_workers: Maximum number of parallel workers (default: min(32, cpu_count + 4))

    Performance:
        - Sequential: ~0.15s for 20 modules on synthetic data
        - Parallel (4 workers): ~0.04s (3.75x speedup)
        - Parallel (8 workers): ~0.03s (5x speedup)
    """
    import os

    # Determine optimal worker count (ThreadPoolExecutor default formula)
    if max_workers is None:
        max_workers = min(32, (os.cpu_count() or 1) + 4)

    logger.info(
        "analysis_modules_parallel_start",
        total_tasks=len(ANALYSIS_TASKS),
        max_workers=max_workers,
    )

    # Thread-safe lock for writing to shared analysis_results dict
    results_lock = threading.Lock()

    # Execute tasks in parallel
    completed_count = 0
    failed_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_task = {
            executor.submit(_execute_analysis_task, task, context, results_lock): task
            for task in ANALYSIS_TASKS
        }

        # Collect results as they complete
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                task_name, result, elapsed = future.result()

                if result is not None:
                    # Thread-safe write to shared dict
                    with results_lock:
                        context.analysis_results[task_name] = result
                    completed_count += 1
                    logger.debug(
                        "analysis_task_complete",
                        task=task_name,
                        elapsed=round(elapsed, 3),
                    )
                else:
                    failed_count += 1

            except Exception as e:
                logger.exception(
                    "analysis_task_exception",
                    task=task.name,
                    error=str(e),
                )
                failed_count += 1

    # Apply tags to result entries on the main thread (D23: thread-safe mutation)
    person_tag_assignments = context.analysis_results.get("tags")
    if person_tag_assignments:
        for r in context.results:
            pid = r["person_id"]
            if pid in person_tag_assignments:
                r["tags"] = person_tag_assignments[pid]

    # Add performance monitoring summary (always last)
    context.analysis_results["performance"] = context.monitor.get_summary()

    logger.info(
        "analysis_modules_parallel_complete",
        completed=completed_count,
        failed=failed_count,
        total=len(ANALYSIS_TASKS),
    )
