"""Causal inference and structural estimation nodes.

Hamilton nodes for causal identification, structural estimation, DML,
compensation, bias detection, attrition, and market analysis.
"""

from __future__ import annotations

from typing import Any

from src.pipeline_phases.context import PipelineContext


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
    result = identify_studio_effects(ctx.credits, ctx.anime_map, ctx.results)
    export_identification_report(result)
    return result


def structural_estimation(ctx: PipelineContext) -> Any:
    """Structural estimation with fixed effects and DID (research-grade)."""
    from src.analysis.causal.structural_estimation import (
        estimate_structural_model,
        export_structural_estimation,
    )
    result = estimate_structural_model(ctx.results, ctx.credits, ctx.anime_map)
    export_structural_estimation(result)
    return result


def dml_analysis(ctx: PipelineContext) -> Any:
    """Double Machine Learning causal analysis."""
    from src.analysis.causal.dml import run_dml_analysis
    return run_dml_analysis(ctx.results, ctx.credits, ctx.anime_map)


def compensation(ctx: PipelineContext) -> Any:
    """Batch compensation analysis for all scoreable persons."""
    from src.analysis.compensation_analyzer import (
        batch_analyze_compensation,
        export_compensation_report,
    )
    result = batch_analyze_compensation(ctx.results, ctx.credits, ctx.anime_map)
    export_compensation_report(result)
    return result


def bias_detection(ctx: PipelineContext) -> Any:
    """Detect systematic biases in score distribution."""
    from src.analysis.bias_detector import detect_systematic_biases, generate_bias_report
    result = detect_systematic_biases(ctx.results, ctx.credits, ctx.anime_map)
    generate_bias_report(result)
    return result


def entry_cohort_attrition(ctx: PipelineContext) -> Any:
    """Entry cohort attrition analysis (policy brief input)."""
    from src.analysis.attrition.entry_cohort_attrition import run_entry_cohort_attrition
    return run_entry_cohort_attrition(ctx.credits, ctx.anime_map)


def generational_health(ctx: PipelineContext) -> Any:
    """Generational health metrics (policy brief input)."""
    from src.analysis.attrition.generational_health import run_generational_health
    return run_generational_health(ctx.credits, ctx.anime_map, ctx.results)


def attrition_risk(ctx: PipelineContext) -> Any:
    """Attrition risk model (HR brief input)."""
    from src.analysis.attrition.attrition_risk_model import run_attrition_risk_model
    return run_attrition_risk_model(ctx.credits, ctx.anime_map, ctx.results)


def monopsony(ctx: PipelineContext) -> Any:
    """Monopsony market power analysis (policy brief input)."""
    from src.analysis.market.monopsony import run_monopsony_analysis
    return run_monopsony_analysis(ctx.credits, ctx.anime_map, ctx.results)


def gender_bottleneck(ctx: PipelineContext) -> Any:
    """Gender bottleneck analysis (policy brief input)."""
    from src.analysis.gender.bottleneck import run_gender_bottleneck
    return run_gender_bottleneck(ctx.credits, ctx.anime_map, ctx.results)


def undervalued_talent(ctx: PipelineContext) -> Any:
    """Undervalued talent identification (business brief input)."""
    from src.analysis.talent.undervalued import run_undervalued_talent
    return run_undervalued_talent(ctx.results, ctx.credits, ctx.anime_map)


def succession_matrix(ctx: PipelineContext) -> Any:
    """Succession matrix — who can replace whom (HR brief input)."""
    from src.analysis.talent.succession import run_succession_matrix
    return run_succession_matrix(ctx.results, ctx.credits, ctx.anime_map)


def director_value_add(ctx: PipelineContext) -> Any:
    """Director value-add: mentor contribution to animator trajectories."""
    from src.analysis.mentor.director_value_add import run_director_value_add
    return run_director_value_add(ctx.credits, ctx.anime_map, ctx.results)


def team_chemistry(ctx: PipelineContext) -> Any:
    """Team chemistry analysis — collaboration effectiveness."""
    from src.analysis.team.chemistry import run_team_chemistry
    return run_team_chemistry(ctx.credits, ctx.anime_map, ctx.results)


def team_templates(ctx: PipelineContext) -> Any:
    """Cluster team composition patterns into archetypes."""
    from src.analysis.team.templates import cluster_team_patterns
    return cluster_team_patterns(ctx.credits, ctx.anime_map, ctx.results)
