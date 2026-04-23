"""Causal inference and structural estimation nodes.

Hamilton nodes for causal identification, structural estimation, DML,
compensation, bias detection, attrition, and market analysis.

H-1 notes:
  - Nodes that require a DB connection (attrition/gender/generational) return {}
    in H-1; they will be wired properly in H-3 (Phase 1-4 Hamilton migration).
  - Nodes requiring intermediate outputs not yet in ctx (undervalued_talent,
    compensation, bias_detection) also return {} and are marked for H-2 rewiring.
"""

from __future__ import annotations

from typing import Any

import structlog

from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


NODE_NAMES: list[str] = [
    "causal_identification",
    "structural_estimation",
    "dml_analysis",
    "compensation",
    "bias_detection",
    "entry_cohort_attrition",
    "generational_health",
    "attrition_risk",
    "monopsony",
    "gender_bottleneck",
    "undervalued_talent",
    "succession_matrix",
    "director_value_add",
    "team_chemistry",
    "team_templates",
]


def causal_identification(ctx: PipelineContext) -> Any:
    """Identify causal studio effects via DiD."""
    from src.analysis.causal.studio_identification import (
        identify_studio_effects,
        export_identification_report,
    )
    person_scores = {r["person_id"]: r for r in ctx.results}
    result = identify_studio_effects(
        ctx.credits, ctx.anime_map, person_scores,
        potential_value_scores=ctx.potential_value_scores,
        growth_acceleration_data=ctx.growth_acceleration_data,
    )
    export_identification_report(result)
    return result


def structural_estimation(ctx: PipelineContext) -> Any:
    """Structural estimation with fixed effects and DiD (research-grade).

    Requires major_studios set derived from credits; computed inline for H-1.
    """
    from src.analysis.causal.structural_estimation import (
        estimate_structural_model,
        export_structural_estimation,
    )
    person_scores = {r["person_id"]: r for r in ctx.results}
    major_studios: set[str] = {
        a.studios[0] for a in ctx.anime_map.values()
        if getattr(a, "studios", None)
    }
    result = estimate_structural_model(
        ctx.credits, ctx.anime_map, person_scores, major_studios,
        potential_value_scores=ctx.potential_value_scores,
    )
    export_structural_estimation(result)
    return result


def dml_analysis(ctx: PipelineContext) -> Any:
    """Double Machine Learning causal analysis."""
    from src.analysis.causal.dml import run_dml_analysis
    return run_dml_analysis(ctx.credits, ctx.anime_map, ctx.person_fe, ctx.studio_fe)


def compensation(ctx: PipelineContext) -> Any:
    """Batch compensation analysis.

    H-1 stub: batch_analyze_compensation requires a pre-computed
    all_contributions dict not yet available in PipelineContext.
    Will be wired in H-2.
    """
    logger.debug("compensation_node_skipped", reason="h1_stub")
    return {}


def bias_detection(ctx: PipelineContext) -> Any:
    """Detect systematic biases in score distribution."""
    from src.analysis.bias_detector import detect_systematic_biases, generate_bias_report
    person_scores = {r["person_id"]: r for r in ctx.results}
    result = detect_systematic_biases(
        ctx.contribution_data, person_scores, ctx.studio_bias_metrics,
        ctx.growth_acceleration_data, ctx.potential_value_scores, ctx.role_profiles,
    )
    generate_bias_report(result)
    return result


def entry_cohort_attrition(ctx: PipelineContext) -> Any:
    """Entry cohort attrition analysis.

    H-1 stub: requires a DB connection for cohort survival computation.
    Will be wired in H-3 (Phase 1-4 Hamilton migration).
    """
    logger.debug("entry_cohort_attrition_skipped", reason="h1_needs_conn")
    return {}


def generational_health(ctx: PipelineContext) -> Any:
    """Generational health metrics.

    H-1 stub: requires a DB connection.
    """
    logger.debug("generational_health_skipped", reason="h1_needs_conn")
    return {}


def attrition_risk(ctx: PipelineContext) -> Any:
    """Attrition risk model.

    H-1 stub: requires a DB connection.
    """
    logger.debug("attrition_risk_skipped", reason="h1_needs_conn")
    return {}


def monopsony(ctx: PipelineContext) -> Any:
    """Monopsony market power analysis (policy brief input)."""
    from src.analysis.market.monopsony import run_monopsony_analysis
    return run_monopsony_analysis(ctx.studio_assignments, ctx.person_fe)


def gender_bottleneck(ctx: PipelineContext) -> Any:
    """Gender bottleneck analysis.

    H-1 stub: requires a DB connection.
    """
    logger.debug("gender_bottleneck_skipped", reason="h1_needs_conn")
    return {}


def undervalued_talent(ctx: PipelineContext) -> Any:
    """Undervalued talent identification.

    H-1 stub: run_undervalued_talent requires expected_ability dict
    from causal_identification output, not yet wired in H-1.
    """
    logger.debug("undervalued_talent_skipped", reason="h1_missing_expected_ability")
    return {}


def succession_matrix(ctx: PipelineContext) -> Any:
    """Succession matrix — who can replace whom (HR brief input)."""
    if not ctx.studio_assignments or not ctx.person_fe:
        return {}
    from src.analysis.talent.succession import run_succession_matrix
    try:
        return run_succession_matrix(ctx.person_fe, ctx.credits, ctx.studio_assignments)
    except (IndexError, ValueError):
        # Synthetic data has too-sparse studio_assignments to build full matrix.
        return {}


def director_value_add(ctx: PipelineContext) -> Any:
    """Director value-add: mentor contribution to animator trajectories."""
    from src.analysis.mentor.director_value_add import run_director_value_add
    # mentorships list comes from the mentorships node; use empty list for H-1.
    return run_director_value_add([], ctx.person_fe, ctx.credits, ctx.anime_map)


def team_chemistry(ctx: PipelineContext) -> Any:
    """Team chemistry analysis — collaboration effectiveness."""
    from src.analysis.team.chemistry import run_team_chemistry
    return run_team_chemistry(ctx.credits, ctx.anime_map, ctx.iv_scores)


def team_templates(ctx: PipelineContext) -> Any:
    """Cluster team composition patterns into archetypes."""
    from src.analysis.team.templates import cluster_team_patterns
    return cluster_team_patterns(ctx.credits, ctx.anime_map, ctx.iv_scores)
