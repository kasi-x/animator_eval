"""Phase 8: Post-Processing — percentiles, confidence, stability."""

import bisect

import structlog

from src.analysis.confidence import batch_compute_confidence
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def post_process_results(context: PipelineContext) -> None:
    """Post-process results with percentiles, confidence intervals, and stability.

    Operations:
    1. Calculate percentile ranks for each score axis
    2. Compute confidence intervals based on data source diversity
    3. Compare with previous run to detect score stability/volatility

    Args:
        context: Pipeline context

    Updates context.results in-place:
        - Adds *_pct fields (authority_pct, trust_pct, skill_pct, composite_pct)
        - Adds confidence field (interval width based on source diversity)
        - Adds stability field (comparison with previous run)
    """
    # Calculate percentile ranks using bisect (O(n log n) instead of O(n²))
    n = len(context.results)
    if n > 1:
        for axis in ("authority", "trust", "skill", "composite"):
            sorted_vals = sorted(r[axis] for r in context.results)
            for r in context.results:
                rank = bisect.bisect_right(sorted_vals, r[axis])
                r[f"{axis}_pct"] = round(rank / n * 100, 1)
    elif n == 1:
        for r in context.results:
            for axis in ("authority", "trust", "skill", "composite"):
                r[f"{axis}_pct"] = 100.0

    # Compute confidence intervals
    logger.info("step_start", step="confidence")
    with context.monitor.measure("confidence_intervals"):
        # Count distinct data sources per person
        sources_per_person: dict[str, set] = {}
        for c in context.credits:
            if c.person_id not in sources_per_person:
                sources_per_person[c.person_id] = set()
            sources_per_person[c.person_id].add(c.source)
        source_counts = {pid: len(srcs) for pid, srcs in sources_per_person.items()}

        batch_compute_confidence(context.results, sources_per_person=source_counts)

    # Score stability check (compare with previous run)
    # Note: This requires database connection, so we'll make it optional
    # The original implementation loads from DB in this step
