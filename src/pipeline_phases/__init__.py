# ruff: noqa: E402
"""Pipeline phases — modular decomposition of scoring pipeline.

This package decomposes the monolithic run_scoring_pipeline() function into
10 clear phases, each in its own module:

1. data_loading: Load persons, anime, credits from database
2. validation: Data quality checks
3. entity_resolution: Deduplicate person identities
4. graph_construction: Build person-anime and collaboration networks
5. core_scoring: Authority, Trust, Skill + Normalization
6. supplementary_metrics: Decay, role, career, circles, versatility, centrality, density, growth
7. result_assembly: Build comprehensive result dictionaries
8. post_processing: Percentiles, confidence, stability
9. analysis_modules: 18+ independent analyses (parallelizable)
10. export_and_viz: JSON export + visualization

Each phase is <200 lines, testable in isolation, and has explicit
inputs/outputs via the PipelineContext dataclass.
"""

from src.utils.import_guard import install_display_lookup_boundary_guard

install_display_lookup_boundary_guard()

from src.pipeline_phases.analysis_modules import run_analysis_modules_phase
from src.pipeline_phases.context import PipelineCheckpoint, PipelineContext
from src.pipeline_phases.core_scoring import compute_core_scores_phase
from src.pipeline_phases.data_loading import load_pipeline_data
from src.pipeline_phases.entity_resolution import run_entity_resolution
from src.pipeline_phases.export_and_viz import export_and_visualize_phase
from src.pipeline_phases.graph_construction import build_graphs_phase
from src.pipeline_phases.post_processing import post_process_results
from src.pipeline_phases.result_assembly import assemble_result_entries
from src.pipeline_phases.supplementary_metrics import (
    compute_supplementary_metrics_phase,
)
from src.pipeline_phases.va_core_scoring import compute_va_core_scores_phase
from src.pipeline_phases.va_graph_construction import build_va_graphs_phase
from src.pipeline_phases.va_result_assembly import assemble_va_results
from src.pipeline_phases.va_supplementary_metrics import (
    compute_va_supplementary_metrics_phase,
)
from src.pipeline_phases.validation import run_validation_phase

__all__ = [
    "PipelineCheckpoint",
    "PipelineContext",
    "load_pipeline_data",
    "run_validation_phase",
    "run_entity_resolution",
    "build_graphs_phase",
    "compute_core_scores_phase",
    "compute_supplementary_metrics_phase",
    "assemble_result_entries",
    "post_process_results",
    "run_analysis_modules_phase",
    "export_and_visualize_phase",
    # VA pipeline phases
    "build_va_graphs_phase",
    "compute_va_core_scores_phase",
    "compute_va_supplementary_metrics_phase",
    "assemble_va_results",
]
