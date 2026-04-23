"""VA pipeline phases — orchestration wrappers around src/analysis/va/ algorithms.

Re-exported from src.pipeline_phases for use by src/pipeline.py.
"""

from src.analysis.va.pipeline.core_scoring import compute_va_core_scores_phase
from src.analysis.va.pipeline.graph_construction import build_va_graphs_phase
from src.analysis.va.pipeline.result_assembly import assemble_va_results
from src.analysis.va.pipeline.supplementary_metrics import (
    compute_va_supplementary_metrics_phase,
)

__all__ = [
    "build_va_graphs_phase",
    "compute_va_core_scores_phase",
    "compute_va_supplementary_metrics_phase",
    "assemble_va_results",
]
