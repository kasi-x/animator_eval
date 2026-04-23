"""Core analysis nodes — anime statistics, persons, teams, credits.

Each function is a Hamilton node: pure function, typed return, snake_case name.
All take ctx: PipelineContext for H-1 PoC; H-2 will decompose to explicit inputs.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from src.pipeline_phases.context import PipelineContext


NODE_NAMES: list[str] = [
    "anime_stats",
    "seasonal",
    "outliers",
    "teams",
    "crossval",
    "role_flow",
    "transitions",
    "time_series",
    "decade_analysis",
    "bridges",
    "mentorships",
    "milestones",
]


def anime_stats(ctx: PipelineContext) -> Any:
    """Compute anime quality statistics."""
    from src.analysis.anime_stats import compute_anime_stats
    return compute_anime_stats(ctx.credits, ctx.anime_map, ctx.iv_scores)


def seasonal(ctx: PipelineContext) -> Any:
    """Compute seasonal activity patterns."""
    from src.analysis.seasonal import compute_seasonal_trends
    return compute_seasonal_trends(ctx.credits, ctx.anime_map, ctx.iv_scores)


def outliers(ctx: PipelineContext) -> Any:
    """Detect statistical outliers in score distribution."""
    from src.analysis.outliers import detect_outliers
    return detect_outliers(ctx.results)


def teams(ctx: PipelineContext) -> Any:
    """Analyze team composition patterns."""
    from src.analysis.team_composition import analyze_team_patterns
    iv_vals = [r["iv_score"] for r in ctx.results if r["iv_score"] > 0]
    person_threshold = float(np.percentile(iv_vals, 95)) if iv_vals else 0.0
    top_persons = {
        r["person_id"]: r["iv_score"]
        for r in ctx.results
        if r["iv_score"] >= person_threshold
    }
    return analyze_team_patterns(ctx.credits, ctx.anime_map, person_scores=top_persons)


def crossval(ctx: PipelineContext) -> Any:
    """Cross-validate scores for statistical reliability (skipped when >200 persons)."""
    from src.analysis.crossval import cross_validate_scores
    if len(ctx.results) >= 200:
        return {}
    n_folds = 5 if len(ctx.results) >= 100 else 3
    return cross_validate_scores(ctx.persons, ctx.anime_list, ctx.credits, n_folds=n_folds)


def role_flow(ctx: PipelineContext) -> Any:
    """Compute role transition flows (career progression patterns)."""
    from src.analysis.role_flow import compute_role_flow
    return compute_role_flow(ctx.credits, ctx.anime_map)


def transitions(ctx: PipelineContext) -> Any:
    """Compute role transition matrix."""
    from src.analysis.transitions import compute_role_transitions
    return compute_role_transitions(ctx.credits)


def time_series(ctx: PipelineContext) -> Any:
    """Compute time-series scores for trend analysis."""
    from src.analysis.time_series import compute_time_series
    return compute_time_series(ctx.results, ctx.credits, ctx.anime_map)


def decade_analysis(ctx: PipelineContext) -> Any:
    """Compute decade-level statistics."""
    from src.analysis.decade_analysis import compute_decade_analysis
    return compute_decade_analysis(ctx.credits, ctx.anime_map)


def bridges(ctx: PipelineContext) -> Any:
    """Detect bridge persons connecting communities."""
    from src.analysis.network.bridges import detect_bridges
    if ctx.collaboration_graph is None:
        return {}
    return detect_bridges(ctx.collaboration_graph, ctx.results)


def mentorships(ctx: PipelineContext) -> Any:
    """Infer and build mentorship tree."""
    from src.analysis.mentorship import build_mentorship_tree, infer_mentorships
    inferred = infer_mentorships(ctx.credits, ctx.results)
    return build_mentorship_tree(inferred)


def milestones(ctx: PipelineContext) -> Any:
    """Compute career milestones for each person."""
    from src.analysis.milestones import compute_milestones
    return compute_milestones(ctx.credits, ctx.results, ctx.anime_map)
