"""Phase 9: Analysis Modules — parallel execution for 4-6x speedup.

This phase runs 25 independent analysis modules in parallel using ThreadPoolExecutor.
Each analysis reads from context and writes to context.analysis_results with thread-safe locking.

Memory optimization: Tasks that need the collaboration graph run first as "batch 1".
After batch 1 completes, the graph is freed (potentially ~71 GB for large datasets)
before running the remaining tasks in "batch 2".
"""

import gc
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

import networkx as nx
import numpy as np
import structlog

from src.analysis.compatibility import compute_compatibility_groups
from src.analysis.cooccurrence_groups import compute_cooccurrence_groups
from src.analysis.scoring.expected_ability import compute_expected_ability
from src.analysis.anime_stats import compute_anime_stats
from src.analysis.bias_detector import detect_systematic_biases, generate_bias_report
from src.analysis.network.bridges import detect_bridges
from src.analysis.causal.studio_identification import (
    identify_studio_effects,
    export_identification_report,
)
from src.analysis.credit_stats import compute_credit_statistics
from src.analysis.credit_stats_html import generate_credit_stats_html
from src.analysis.causal.structural_estimation import (
    estimate_structural_model,
    export_structural_estimation,
)
from src.analysis.causal.structural_estimation_html import generate_html_report
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
from src.analysis.scoring.individual_contribution import compute_individual_profiles
from src.analysis.decade_analysis import compute_decade_analysis
from src.analysis.genre.affinity import compute_genre_affinity
from src.analysis.graphml_export import export_graphml
from src.analysis.influence import compute_influence_tree
from src.analysis.mentorship import build_mentorship_tree, infer_mentorships
from src.analysis.milestones import compute_milestones
from src.analysis.network.network_evolution import compute_network_evolution
from src.analysis.outliers import detect_outliers
from src.analysis.person_tags import compute_person_tags
from src.analysis.productivity import compute_productivity
from src.analysis.role_flow import compute_role_flow
from src.analysis.seasonal import compute_seasonal_trends
from src.analysis.studio.profile import compute_studio_analysis
from src.analysis.studio.timeseries import compute_studio_timeseries
from src.analysis.synergy_score import compute_synergy_scores
from src.analysis.team_composition import analyze_team_patterns
from src.analysis.causal.dml import run_dml_analysis
from src.analysis.network.temporal_pagerank import compute_temporal_pagerank
from src.analysis.time_series import compute_time_series
from src.analysis.transitions import compute_role_transitions

# Studio & Genre analysis imports
from src.analysis.genre.ecosystem import compute_genre_ecosystem
from src.analysis.genre.network import compute_genre_network
from src.analysis.genre.quality import compute_genre_quality
from src.analysis.production_analysis import StudioTalentDensity, compute_studio_talent_density
from src.analysis.studio.clustering import compute_studio_clustering
from src.analysis.studio.network import compute_studio_network
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
        needs_collab_graph: Whether this task reads context.collaboration_graph.
            Tasks with needs_collab_graph=True run in batch 1 (before graph is freed).
    """

    name: str
    function: Callable[[PipelineContext], Any]
    monitor_step: str | None = None
    condition: Callable[[PipelineContext], bool] | None = None
    needs_collab_graph: bool = False
    memory_heavy: bool = False  # Run sequentially in batch 2b to avoid OOM


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
    # Skip if no graph — the O(n²) fallback is impractical for 167K+ persons
    if context.collaboration_graph is None:
        logger.info("collaborations_skipped", reason="no_graph")
        return []
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
    return analyze_team_patterns(
        context.credits,
        context.anime_map,
        person_scores=top_persons,
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
    # Reuse pre-computed community_map from Phase 4 (avoids redundant community
    # detection and works with both NetworkX and SparseCollaborationGraph)
    communities_map = context.community_map if context.community_map else None
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
    mentorship_tree_data = build_mentorship_tree(mentorships)
    # Return both — main thread splits under lock (thread safety)
    return {"mentorships": mentorships, "mentorship_tree": mentorship_tree_data}


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
    person_scores = context._shared_person_scores

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
    person_names = context._shared_person_names

    # Run batch analysis (top 100 anime by composite value)
    if not context.contribution_data:
        return {}

    # Get anime with contributions
    anime_with_contribs = [
        anime for anime in context.anime_map.values() if anime.id in context.contribution_data
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
    person_scores = context._shared_person_scores
    person_names = context._shared_person_names

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
    person_scores = context._shared_person_scores

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
    person_scores = context._shared_person_scores

    # Identify major studios
    from src.analysis.causal.studio_identification import identify_major_studios

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
        "weight_method": "PCA_PC1",
        "pca_variance_explained": context.pca_variance_explained,
    }
    if context.iv_component_std:
        result["component_std"] = context.iv_component_std
    if context.iv_component_mean:
        result["component_mean"] = context.iv_component_mean
    if context.quality_calibration:
        result["quality_calibration"] = context.quality_calibration
    return result


def _run_derived_params_report(context: PipelineContext) -> Any:
    """Build comprehensive report of all data-derived pipeline parameters."""
    import statistics

    report: dict = {
        "description": "パイプライン計算過程で動的に導出された全パラメータ",
        "sections": {},
    }

    # --- 1. BiRank Quality Calibration ---
    cal = context.quality_calibration or {}
    report["sections"]["birank_quality_calibration"] = {
        "title": "BiRank品質キャリブレーション",
        "description": "enhance_bipartite_quality()で推定されたパラメータ",
        "parameters": {
            "role_damping": {
                "value": cal.get("role_damping"),
                "method": "1 - |Spearman ρ(role_weight, person_fe)|",
                "interpretation": "役職階層の圧縮度。低い=役職を重視、高い=内容を重視",
                "range": "[0.1, 0.9]",
            },
            "blend": {
                "value": cal.get("blend"),
                "method": "チーム内person_fe変動係数(CV)中央値から導出",
                "interpretation": "加重平均vs上澄みのバランス。高い=加重平均重視、低い=上澄み重視",
                "range": "[0.2, 0.8]",
            },
            "top_fraction": {
                "value": cal.get("top_fraction"),
                "method": "person_fe分布の歪度(skewness)から導出",
                "interpretation": "上位何%をエリート層とみなすか",
                "range": "[0.10, 0.40]",
            },
        },
        "diagnostics": {
            "edges_reweighted": cal.get("edges_reweighted"),
            "anime_with_directors": cal.get("anime_with_directors"),
            "anime_with_quality_non_dir": cal.get("anime_with_quality_non_dir"),
            "anime_with_quality_dir": cal.get("anime_with_quality_dir"),
            "boost_non_dir_range": cal.get("boost_non_dir_range"),
            "boost_dir_range": cal.get("boost_dir_range"),
        },
    }

    # --- 2. IV PCA Weights ---
    report["sections"]["iv_pca_weights"] = {
        "title": "Integrated Value PCA重み",
        "description": "PCA第1主成分の負荷量から導出されたλ重み",
        "parameters": {
            name: {
                "lambda_weight": round(w, 4),
                "component_std": round(
                    (context.iv_component_std or {}).get(name, 0), 4
                ),
                "component_mean": round(
                    (context.iv_component_mean or {}).get(name, 0), 4
                ),
            }
            for name, w in sorted(context.iv_lambda_weights.items())
        },
        "diagnostics": {
            "pca_variance_explained": round(context.pca_variance_explained, 4),
            "weight_method": "PCA_PC1",
            "n_components": len(context.iv_lambda_weights),
        },
    }

    # --- 3. Staff Scale Baseline ---
    # Use cached staff sets (graph may have been freed for memory optimization)
    staff_sets = context.analysis_results.get("_anime_staff_sets", {})
    if not staff_sets and context.person_anime_graph:
        staff_sets = context.person_anime_graph.graph.get("_anime_staff_sets", {})
    staff_counts = [len(pids) for pids in staff_sets.values()] if staff_sets else []
    if staff_counts:
        median_staff = statistics.median(staff_counts)
        report["sections"]["staff_scale"] = {
            "title": "スタッフ規模ベースライン",
            "description": "アニメ毎のスタッフ数中央値をベースラインとして使用",
            "parameters": {
                "median_staff_count": round(median_staff, 1),
                "method": "全アニメのスタッフ数中央値",
                "interpretation": f"スタッフ{round(median_staff)}人の作品がscale=1.0",
            },
            "diagnostics": {
                "n_anime": len(staff_counts),
                "min_staff": min(staff_counts),
                "max_staff": max(staff_counts),
                "mean_staff": round(statistics.mean(staff_counts), 1),
                "p25_staff": round(sorted(staff_counts)[len(staff_counts) // 4], 1),
                "p75_staff": round(
                    sorted(staff_counts)[3 * len(staff_counts) // 4], 1
                ),
            },
        }

    # --- 4. BiRank Rescaling ---
    n_birank = len(context.birank_person_scores)
    if n_birank > 0:
        br_vals = list(context.birank_person_scores.values())
        report["sections"]["birank_rescaling"] = {
            "title": "BiRankリスケーリング",
            "description": "確率空間(sum=1)から期待値空間(mean=1)への変換",
            "parameters": {
                "rescale_factor": n_birank,
                "method": "×N (人数倍)",
                "interpretation": "他のIV成分と同スケールにするための正規化",
            },
            "diagnostics": {
                "n_persons": n_birank,
                "min_score": round(min(br_vals), 6),
                "max_score": round(max(br_vals), 4),
                "mean_score": round(statistics.mean(br_vals), 4),
                "median_score": round(statistics.median(br_vals), 4),
            },
        }

    # --- 5. AKM Distribution ---
    if context.person_fe:
        pfe_vals = list(context.person_fe.values())
        report["sections"]["akm_person_fe"] = {
            "title": "AKM個人固定効果の分布",
            "description": "log(production_scale) = θ_i + ψ_j + ε — θ_iの分布",
            "diagnostics": {
                "n_persons": len(pfe_vals),
                "mean": round(statistics.mean(pfe_vals), 4),
                "median": round(statistics.median(pfe_vals), 4),
                "std": round(statistics.stdev(pfe_vals), 4) if len(pfe_vals) > 1 else 0,
                "min": round(min(pfe_vals), 4),
                "max": round(max(pfe_vals), 4),
            },
        }
    if context.studio_fe:
        sfe_vals = list(context.studio_fe.values())
        report["sections"]["akm_studio_fe"] = {
            "title": "AKMスタジオ固定効果の分布",
            "description": "log(production_scale) = θ_i + ψ_j + ε — ψ_jの分布",
            "diagnostics": {
                "n_studios": len(sfe_vals),
                "mean": round(statistics.mean(sfe_vals), 4),
                "median": round(statistics.median(sfe_vals), 4),
                "std": round(statistics.stdev(sfe_vals), 4) if len(sfe_vals) > 1 else 0,
                "min": round(min(sfe_vals), 4),
                "max": round(max(sfe_vals), 4),
            },
        }

    # --- 6. Dormancy ---
    if context.dormancy_scores:
        d_vals = list(context.dormancy_scores.values())
        active = sum(1 for v in d_vals if v >= 0.99)
        report["sections"]["dormancy"] = {
            "title": "休眠ペナルティの分布",
            "description": "D_i = exp(-λ × max(0, gap - grace))",
            "parameters": {
                "decay_rate": 0.5,
                "grace_period_years": 2.0,
            },
            "diagnostics": {
                "n_persons": len(d_vals),
                "active_persons": active,
                "active_fraction": round(active / len(d_vals), 4) if d_vals else 0,
                "mean": round(statistics.mean(d_vals), 4),
                "median": round(statistics.median(d_vals), 4),
            },
        }

    # --- 7. Patronage ---
    if context.patronage_scores:
        pat_vals = list(context.patronage_scores.values())
        nonzero = sum(1 for v in pat_vals if v > 0)
        report["sections"]["patronage"] = {
            "title": "パトロネージプレミアムの分布",
            "description": "Π_i = Σ PR_d × log(1 + N_shared)",
            "diagnostics": {
                "n_persons": len(pat_vals),
                "nonzero": nonzero,
                "nonzero_fraction": round(nonzero / len(pat_vals), 4) if pat_vals else 0,
                "mean": round(statistics.mean(pat_vals), 4),
                "max": round(max(pat_vals), 4),
            },
        }

    return report


def _run_dml_analysis(context: PipelineContext) -> Any:
    """Run DML causal inference: OLS vs DML comparison for each parameter."""
    report = run_dml_analysis(
        context.credits, context.anime_map,
        context.person_fe, context.studio_fe,
    )
    return report.to_dict()


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
        max_group_size=3,  # capped from 5: C(n,5) causes OOM on large datasets
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
    # Reuse talent density from studio_talent_density task if available
    cached_td = context.analysis_results.get("studio_talent_density")
    if cached_td:
        talent_density = {
            s: StudioTalentDensity(**v) for s, v in cached_td.items()
        }
    else:
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
    result = compute_genre_network(list(context.anime_map.values()))
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
        "collaborations", _run_collaborations, monitor_step="collaboration_strength",
        needs_collab_graph=True, memory_heavy=True,
    ),
    AnalysisTask("outliers", _run_outliers, monitor_step="outlier_detection"),
    AnalysisTask("teams", _run_teams, monitor_step="team_composition", memory_heavy=True),
    AnalysisTask(
        "graphml",
        _run_graphml,
        monitor_step="graphml_export",
        condition=lambda ctx: (
            ctx.collaboration_graph is not None
            and ctx.collaboration_graph.number_of_edges() <= 1_000_000
        ),
        needs_collab_graph=True,
    ),
    AnalysisTask("time_series", _run_time_series, monitor_step="time_series"),
    AnalysisTask("decades", _run_decades, monitor_step="decade_analysis"),
    AnalysisTask("tags", _run_tags, monitor_step="person_tags"),
    AnalysisTask("transitions", _run_transitions),
    AnalysisTask("role_flow", _run_role_flow, monitor_step="role_flow"),
    AnalysisTask("bridges", _run_bridges, monitor_step="bridge_detection",
                 needs_collab_graph=True, memory_heavy=True),
    AnalysisTask("mentorships", _run_mentorships, monitor_step="mentorship_inference",
                 memory_heavy=True),
    AnalysisTask("milestones", _run_milestones, monitor_step="milestones"),
    AnalysisTask(
        "network_evolution", _run_network_evolution, monitor_step="network_evolution",
        needs_collab_graph=True,
    ),
    AnalysisTask("genre_affinity", _run_genre_affinity, monitor_step="genre_affinity"),
    AnalysisTask("productivity", _run_productivity, monitor_step="productivity"),
    AnalysisTask("influence", _run_influence, monitor_step="influence_tree", memory_heavy=True),
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
        memory_heavy=True,
    ),
    AnalysisTask(
        "structural_estimation",
        _run_structural_estimation,
        monitor_step="structural_estimation",
        memory_heavy=True,
    ),
    AnalysisTask(
        "individual_profiles",
        _run_individual_contribution,
        monitor_step="individual_contribution",
        memory_heavy=True,
        # No needs_collab_graph: compute_independent_value has a credit-based
        # fallback for finding collaborators, avoiding OOM from holding the
        # 44M-edge graph during 30 min of computation.
    ),
    AnalysisTask("credit_stats", _run_credit_stats, monitor_step="credit_statistics"),
    AnalysisTask(
        "cooccurrence_groups",
        _run_cooccurrence_groups,
        monitor_step="cooccurrence_groups",
        memory_heavy=True,
    ),
    AnalysisTask(
        "synergy_scores",
        _run_synergy_scores,
        monitor_step="synergy_scores",
        memory_heavy=True,
    ),
    AnalysisTask(
        "temporal_pagerank",
        _run_temporal_pagerank,
        monitor_step="temporal_pagerank",
        memory_heavy=True,
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
        "derived_params",
        _run_derived_params_report,
        monitor_step="derived_params",
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
        memory_heavy=True,
    ),
    AnalysisTask(
        "compatibility_groups_analysis",
        _run_compatibility,
        monitor_step="compatibility_groups_analysis",
        memory_heavy=True,
        # No needs_collab_graph: compatibility.py accepts graph but never uses it.
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
        memory_heavy=True,
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
    # DML causal inference (OLS vs DML comparison)
    AnalysisTask(
        "dml_analysis",
        _run_dml_analysis,
        monitor_step="dml_analysis",
        condition=lambda ctx: ctx.akm_result is not None and len(ctx.person_fe) > 50,
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


def _flush_result_to_json(task_name: str, data: Any, json_dir: Path) -> bool:
    """Write analysis result to JSON and return True if successful.

    Results flushed here are marked as _FLUSHED in context.analysis_results
    so Phase 10 export skips re-writing them.
    """
    import dataclasses
    import json as json_mod

    def _default_serializer(obj: Any) -> Any:
        """Serialize dataclass instances to dicts; fall back to str."""
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return dataclasses.asdict(obj)
        return str(obj)

    # Map task names to their export filenames
    filename = f"{task_name}.json"
    filepath = json_dir / filename
    try:
        json_dir.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json_mod.dump(data, f, ensure_ascii=False, default=_default_serializer)
        logger.debug("analysis_result_flushed", task=task_name, path=str(filepath))
        return True
    except (TypeError, ValueError, OSError) as e:
        logger.warning("analysis_result_flush_failed", task=task_name, error=str(e))
        return False


# Results read by later tasks within Phase 9 — must stay in memory
_KEEP_IN_MEMORY = frozenset({
    "bridges",           # read by _run_insights_report
    "_anime_staff_sets", # read by _run_akm_diagnostics
    "_graph_summary",    # used by export phase
    "studio_talent_density",  # read by _run_studio_clustering
    "studio_network",    # read by _run_studio_clustering
    "talent_pipeline",   # read by _run_studio_clustering
    "tags",              # read by run_analysis_modules_phase (tag assignment)
    "performance",       # added at end
    "crossval",          # read by export summary
})

def _run_task_batch(
    tasks: list[AnalysisTask],
    context: PipelineContext,
    results_lock: threading.Lock,
    max_workers: int,
    batch_label: str,
    json_dir: Path | None = None,
) -> tuple[int, int]:
    """Run a batch of analysis tasks in parallel.

    If json_dir is provided, large results are flushed to JSON and evicted
    from memory to prevent OOM on datasets with 150K+ persons.

    Returns (completed_count, failed_count).
    """
    completed = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(_execute_analysis_task, task, context, results_lock): task
            for task in tasks
        }

        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                task_name, result, elapsed = future.result()

                if result is not None:
                    with results_lock:
                        if task_name == "mentorships" and isinstance(result, dict) and "mentorship_tree" in result:
                            context.analysis_results["mentorship_tree"] = result["mentorship_tree"]
                            context.analysis_results[task_name] = result["mentorships"]
                        else:
                            context.analysis_results[task_name] = result

                    # Eagerly flush to JSON and evict from memory if safe
                    if json_dir and task_name not in _KEEP_IN_MEMORY:
                        data = context.analysis_results.get(task_name)
                        if data is not None and _flush_result_to_json(task_name, data, json_dir):
                            with results_lock:
                                context.analysis_results[task_name] = None
                                if not hasattr(context, "_flushed_tasks"):
                                    context._flushed_tasks = set()
                                context._flushed_tasks.add(task_name)

                    completed += 1
                    logger.debug(
                        "analysis_task_complete",
                        task=task_name,
                        batch=batch_label,
                        elapsed=round(elapsed, 3),
                    )
                else:
                    failed += 1

                # Reclaim intermediate memory after each task to reduce OOM risk
                del result
                gc.collect()

            except Exception as e:
                logger.exception(
                    "analysis_task_exception",
                    task=task.name,
                    batch=batch_label,
                    error=str(e),
                )
                failed += 1

    return completed, failed


def run_analysis_modules_phase(
    context: PipelineContext,
    max_workers: int | None = None,
) -> None:
    """Run all independent analysis modules in parallel.

    Memory optimization: splits tasks into two batches.
      Batch 1: tasks that need the collaboration graph.
      (free collaboration graph after batch 1 to reclaim memory)
      Batch 2: all remaining tasks.

    Args:
        context: Pipeline context
        max_workers: Maximum number of parallel workers (default: min(32, cpu_count + 4))
    """
    import os

    from src.utils.config import JSON_DIR

    if max_workers is None:
        max_workers = min(32, (os.cpu_count() or 1) + 4)

    # Pre-build shared dicts — avoids 1-2 GB duplicate per task
    context._shared_person_scores = {r["person_id"]: r for r in context.results}
    context._shared_person_names = {p.id: p.name_ja or p.name_en or p.id for p in context.persons}

    # Free redundant anime_list — anime_map has same data, all tasks now use anime_map
    context.anime_list = []
    gc.collect()
    logger.info("phase9_memory_freed", freed="anime_list")

    # Split tasks into graph-dependent (batch 1) and graph-independent (batch 2)
    batch1_tasks = [t for t in ANALYSIS_TASKS if t.needs_collab_graph]
    batch2_tasks = [t for t in ANALYSIS_TASKS if not t.needs_collab_graph]

    has_large_graph = (
        context.collaboration_graph is not None
        and context.collaboration_graph.number_of_edges() > 1_000_000
    )

    logger.info(
        "analysis_modules_parallel_start",
        total_tasks=len(ANALYSIS_TASKS),
        batch1_tasks=len(batch1_tasks),
        batch2_tasks=len(batch2_tasks),
        max_workers=max_workers,
        large_graph_optimization=has_large_graph,
    )

    results_lock = threading.Lock()
    completed_count = 0
    failed_count = 0

    if has_large_graph and batch1_tasks:
        # Batch 1: graph-dependent tasks run sequentially to avoid memory spikes
        # (each task may hold intermediate data proportional to graph size)
        b1_workers = 1
        logger.info(
            "analysis_batch1_start",
            tasks=[t.name for t in batch1_tasks],
            workers=b1_workers,
        )
        c, f = _run_task_batch(batch1_tasks, context, results_lock, b1_workers, "batch1", json_dir=JSON_DIR)
        completed_count += c
        failed_count += f

        # Cache graph summary for Phase 10 before freeing
        from src.analysis.graph import compute_graph_summary

        n_edges = context.collaboration_graph.number_of_edges()
        n_nodes = context.collaboration_graph.number_of_nodes()
        context.analysis_results["_graph_summary"] = compute_graph_summary(
            context.collaboration_graph
        )

        # Cache person_anime_graph staff sets for akm_diagnostics, then free both graphs
        if context.person_anime_graph:
            context.analysis_results["_anime_staff_sets"] = (
                context.person_anime_graph.graph.get("_anime_staff_sets", {})
            )
            pa_edges = context.person_anime_graph.number_of_edges()
            context.person_anime_graph = None
        else:
            pa_edges = 0

        # Free the collaboration graph to reclaim memory
        context.collaboration_graph = None
        gc.collect()
        logger.info(
            "collaboration_graph_freed",
            freed_edges=n_edges,
            freed_nodes=n_nodes,
            person_anime_edges_freed=pa_edges,
        )

        # Batch 2a: lightweight tasks run in parallel
        b2_light = [t for t in batch2_tasks if not t.memory_heavy]
        # Batch 2b: memory-heavy tasks run sequentially (OOM risk)
        b2_heavy = [t for t in batch2_tasks if t.memory_heavy]

        if b2_light:
            b2a_workers = min(max_workers, len(b2_light))
            logger.info(
                "analysis_batch2a_start",
                tasks=len(b2_light),
                workers=b2a_workers,
            )
            c, f = _run_task_batch(
                b2_light, context, results_lock, b2a_workers, "batch2", json_dir=JSON_DIR
            )
            completed_count += c
            failed_count += f

        if b2_heavy:
            logger.info(
                "analysis_batch2b_start",
                tasks=[t.name for t in b2_heavy],
                workers=1,
            )
            c, f = _run_task_batch(
                b2_heavy, context, results_lock, 1, "batch2", json_dir=JSON_DIR
            )
            completed_count += c
            failed_count += f
    else:
        # No large graph: still split light/heavy to prevent OOM from concurrent heavy tasks.
        # Limit light workers too — even "light" tasks can use 1-5 GB each, and 32 concurrent
        # tasks on a 123 GB machine leaves no headroom.
        all_light = [t for t in ANALYSIS_TASKS if not t.memory_heavy]
        all_heavy = [t for t in ANALYSIS_TASKS if t.memory_heavy]

        if all_light:
            # Sequential execution: with 167K persons + 2.9M credits, even 2 workers
            # can push memory past 123 GB.  Sequential + eager flush keeps RSS ~40 GB.
            light_workers = 1
            c, f = _run_task_batch(all_light, context, results_lock, light_workers, "all", json_dir=JSON_DIR)
            completed_count += c
            failed_count += f

        if all_heavy:
            logger.info(
                "analysis_heavy_sequential",
                tasks=[t.name for t in all_heavy],
            )
            c, f = _run_task_batch(all_heavy, context, results_lock, 1, "all", json_dir=JSON_DIR)
            completed_count += c
            failed_count += f

    # Apply tags to result entries on the main thread (D23: thread-safe mutation)
    person_tag_assignments = context.analysis_results.get("tags")
    if person_tag_assignments:
        for r in context.results:
            pid = r["person_id"]
            if pid in person_tag_assignments:
                r["tags"] = person_tag_assignments[pid]

    # Clean up shared dicts
    for attr in ("_shared_person_scores", "_shared_person_names"):
        if hasattr(context, attr):
            delattr(context, attr)

    # Add performance monitoring summary (always last)
    context.analysis_results["performance"] = context.monitor.get_summary()

    logger.info(
        "analysis_modules_parallel_complete",
        completed=completed_count,
        failed=failed_count,
        total=len(ANALYSIS_TASKS),
    )
