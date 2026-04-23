"""Phase 1+2: Data Loading and Validation nodes for Hamilton DAG (H-5).

Nodes:
  - raw_data_loaded: loads persons, anime, credits from silver.duckdb (Phase 1)
  - data_validated: runs quality checks (Phase 2)

H-5: pipeline.py passes {"visualize": bool, "dry_run": bool} as Driver inputs.
     Hamilton loads data and returns typed LoadedData.
     Tests pass loaded_data directly: dr.execute(..., inputs={"loaded_data": fixture}).
"""

from __future__ import annotations

from typing import Any

from hamilton.function_modifiers import tag

from src.pipeline_phases.pipeline_types import LoadedData

NODE_NAMES: list[str] = [
    "raw_data_loaded",
    "data_validated",
]


@tag(stage="phase1", cost="moderate", domain="loading")
def raw_data_loaded(visualize: bool, dry_run: bool) -> LoadedData:
    """Load persons, anime, and credits from silver.duckdb (Phase 1).

    Returns: LoadedData with persons, anime_list, credits, anime_map.
    """
    from src.pipeline_phases.data_loading import load_pipeline_data

    return load_pipeline_data(visualize, dry_run)


@tag(stage="phase2", cost="cheap", domain="loading")
def data_validated(loaded_data: LoadedData, raw_data_loaded: LoadedData) -> Any:
    """Run data quality checks against silver.duckdb (Phase 2).

    Returns a ValidationResult (passed, errors, warnings).
    Depends on raw_data_loaded to run after Phase 1.
    """
    from src.pipeline_phases.validation import run_validation_phase

    return run_validation_phase(loaded_data)
