"""Phase 8: Post-Processing — percentiles, confidence, stability."""

import bisect
from typing import Any

import structlog

from src.analysis.confidence import batch_compute_confidence

logger = structlog.get_logger()


def post_process_results(results: list[dict], credits: list, akm_result: Any) -> None:
    """Post-process results with percentiles, confidence intervals, and stability.

    Operations:
    1. Calculate percentile ranks for each score axis
    2. Compute confidence intervals based on data source diversity
    3. Compare with previous run to detect score stability/volatility

    Args:
        results: List of result dicts (mutated in-place).
        credits: List of Credit objects (for source diversity count).
        akm_result: AKM result object (for analytical person_fe CI).

    Mutates results in-place:
        - Adds *_pct fields (iv_score_pct, person_fe_pct, etc.)
        - Adds confidence field (interval width based on source diversity)
        - Adds stability field (comparison with previous run)
    """
    n = len(results)
    axes = ("iv_score", "person_fe", "birank", "patronage", "awcc", "dormancy")
    if n > 1:
        for axis in axes:
            sorted_vals = sorted(r.get(axis, 0) for r in results)
            for r in results:
                rank = bisect.bisect_right(sorted_vals, r.get(axis, 0))
                pct_raw = rank / n * 100
                r[f"{axis}_pct"] = 100.0 if rank == n else min(round(pct_raw, 1), 99.9)
    elif n == 1:
        for r in results:
            for axis in axes:
                r[f"{axis}_pct"] = 100.0

    logger.info("step_start", step="confidence")
    sources_per_person: dict[str, set] = {}
    for c in credits:
        if c.person_id not in sources_per_person:
            sources_per_person[c.person_id] = set()
        sources_per_person[c.person_id].add(c.source)
    source_counts = {pid: len(srcs) for pid, srcs in sources_per_person.items()}

    akm_residuals = akm_result.residuals if akm_result else None
    batch_compute_confidence(
        results,
        sources_per_person=source_counts,
        akm_residuals=akm_residuals,
    )
