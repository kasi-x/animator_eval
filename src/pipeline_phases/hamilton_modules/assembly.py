"""Phase 7+8: Result Assembly and Post-Processing nodes for Hamilton DAG (H-5).

H-5 pattern: nodes take typed bags (EntityResolutionResult, CoreScoresResult,
SupplementaryMetricsResult) instead of ctx: PipelineContext.
"""

from __future__ import annotations

from hamilton.function_modifiers import tag

from src.pipeline_phases.pipeline_types import (
    CoreScoresResult,
    EntityResolutionResult,
    GraphsResult,
    SupplementaryMetricsResult,
)

NODE_NAMES: list[str] = [
    "results_assembled",
    "results_post_processed",
]


@tag(stage="phase7", cost="moderate", domain="assembly")
def results_assembled(
    entity_resolved: EntityResolutionResult,
    graphs_result: GraphsResult,
    ctx_core_populated: CoreScoresResult,
    ctx_metrics_populated: SupplementaryMetricsResult,
) -> list[dict]:
    """Assemble comprehensive result dict for each scored person (Phase 7).

    Includes: score components, score_layers, centrality, career, network,
    growth, versatility, breakdown (top contributing factors).
    Also writes score rows to gold.duckdb.

    Depends on ctx_metrics_populated (last Phase 6 bridge node).
    """
    from src.pipeline_phases.result_assembly import assemble_result_entries

    return assemble_result_entries(
        entity_resolved, graphs_result, ctx_core_populated, ctx_metrics_populated
    )


@tag(stage="phase8", cost="cheap", domain="assembly")
def results_post_processed(
    results_assembled: list[dict],
    entity_resolved: EntityResolutionResult,
    ctx_core_populated: CoreScoresResult,
) -> list[dict]:
    """Post-process results: percentile ranks, confidence intervals (Phase 8).

    Adds *_pct fields (iv_score_pct, person_fe_pct, etc.) and confidence field.
    Mutates results_assembled in-place.
    """
    from src.pipeline_phases.post_processing import post_process_results

    post_process_results(
        results_assembled, entity_resolved.resolved_credits, ctx_core_populated.akm_result
    )
    return results_assembled
