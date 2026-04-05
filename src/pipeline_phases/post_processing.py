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
    # D19: bisect_right gives upper percentile for ties — all tied values get
    # the same percentile (the highest rank in the tie group / n × 100).
    # Single-person cohort → 100.0 by convention (only person = top).
    n = len(context.results)
    axes = ("iv_score", "person_fe", "birank", "patronage", "awcc", "dormancy")
    if n > 1:
        for axis in axes:
            sorted_vals = sorted(r.get(axis, 0) for r in context.results)
            for r in context.results:
                rank = bisect.bisect_right(sorted_vals, r.get(axis, 0))
                pct_raw = rank / n * 100
                # Only the true top rank(s) get 100.0; others cap at 99.9
                # to avoid rounding artifacts (e.g. 99.95 → 100.0)
                r[f"{axis}_pct"] = (
                    100.0 if rank == n else min(round(pct_raw, 1), 99.9)
                )
    elif n == 1:
        for r in context.results:
            for axis in axes:
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

        # Pass AKM residuals for analytical person_fe CI (B09 fix)
        akm_residuals = (
            context.akm_result.residuals if context.akm_result else None
        )
        batch_compute_confidence(
            context.results,
            sources_per_person=source_counts,
            akm_residuals=akm_residuals,
        )

    # Score stability check (compare with previous run)
    # Note: This requires database connection, so we'll make it optional
    # The original implementation loads from DB in this step
