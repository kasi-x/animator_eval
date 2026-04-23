"""Phase 7+8: Result Assembly and Post-Processing nodes for Hamilton DAG (H-2).

Two nodes:
  - results_assembled: builds comprehensive per-person result dicts (Phase 7)
  - results_post_processed: adds percentiles, confidence intervals (Phase 8)

Both use ctx: PipelineContext (H-2 pattern); H-4 decomposes to explicit inputs.
"""

from __future__ import annotations

from typing import Any

from hamilton.function_modifiers import tag

from src.pipeline_phases.context import PipelineContext

NODE_NAMES: list[str] = [
    "results_assembled",
    "results_post_processed",
]


@tag(stage="phase7", cost="moderate", domain="assembly")
def results_assembled(ctx: PipelineContext, career_tracks_inferred: Any) -> list[dict]:
    """Assemble comprehensive result dict for each scored person (Phase 7).

    Includes: score components, score_layers, centrality, career, network,
    growth, versatility, breakdown (top contributing factors).
    Also writes score rows to gold.duckdb.

    Writes: ctx.results.
    Depends on career_tracks_inferred (last Phase 6 node).
    """
    from src.pipeline_phases.result_assembly import assemble_result_entries

    assemble_result_entries(ctx)
    return ctx.results


@tag(stage="phase8", cost="cheap", domain="assembly")
def results_post_processed(ctx: PipelineContext, results_assembled: Any) -> list[dict]:
    """Post-process results: percentile ranks, confidence intervals (Phase 8).

    Adds *_pct fields (iv_score_pct, person_fe_pct, etc.) and confidence field.

    Writes: ctx.results (updated in-place).
    Depends on results_assembled.
    """
    from src.pipeline_phases.post_processing import post_process_results

    post_process_results(ctx)
    return ctx.results
