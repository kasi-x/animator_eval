"""Phase 1+2: Data Loading and Validation nodes for Hamilton DAG (H-3/H-4).

Nodes:
  - ctx: creates PipelineContext from primitive inputs (H-4 DAG entry point)
  - raw_data_loaded: loads persons, anime, credits from silver.duckdb (Phase 1)
  - data_validated: runs quality checks (Phase 2)

H-4: pipeline.py passes {"visualize": bool, "dry_run": bool} as Driver inputs.
     Hamilton computes ctx from those, then all downstream nodes receive ctx.
     Tests can still override ctx directly: dr.execute(..., inputs={"ctx": ctx_fixture}).
"""

from __future__ import annotations

from typing import Any

from hamilton.function_modifiers import tag

from src.pipeline_phases.context import PipelineContext

NODE_NAMES: list[str] = [
    "ctx",
    "raw_data_loaded",
    "data_validated",
]


@tag(stage="init", cost="cheap", domain="loading")
def ctx(visualize: bool, dry_run: bool) -> PipelineContext:
    """Create PipelineContext from primitive pipeline inputs (H-4 DAG entry point).

    H-4: pipeline.py passes visualize/dry_run; all downstream nodes receive ctx
    via this computed node rather than from external input.
    Tests override with inputs={"ctx": fixture} which bypasses this node.
    """
    return PipelineContext(visualize=visualize, dry_run=dry_run)


@tag(stage="phase1", cost="moderate", domain="loading")
def raw_data_loaded(ctx: PipelineContext) -> Any:
    """Load persons, anime, and credits from silver.duckdb (Phase 1).

    Writes: ctx.persons, ctx.anime_list, ctx.credits, ctx.anime_map,
            ctx.va_credits, ctx.characters, ctx.character_map, ctx.va_person_ids.
    """
    from src.pipeline_phases.data_loading import load_pipeline_data

    load_pipeline_data(ctx)
    return {
        "person_count": len(ctx.persons),
        "anime_count": len(ctx.anime_list),
        "credit_count": len(ctx.credits),
    }


@tag(stage="phase2", cost="cheap", domain="loading")
def data_validated(ctx: PipelineContext, raw_data_loaded: Any) -> Any:
    """Run data quality checks against silver.duckdb (Phase 2).

    Returns a ValidationResult (passed, errors, warnings).
    Depends on raw_data_loaded to run after Phase 1.
    """
    from src.pipeline_phases.validation import run_validation_phase

    return run_validation_phase(ctx)
