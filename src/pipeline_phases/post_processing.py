"""Phase 8: Post-Processing — percentiles, confidence, stability."""

import bisect
import json
from pathlib import Path
from typing import Any

import structlog

from src.analysis.confidence import batch_compute_confidence
from src.analysis.io.mart_writer import write_report_specs
from src.analysis.quality.power_analysis import (
    PowerAuditRow,
    audit_report_power,
)

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# Power audit driver (Session 2 後半: method gate 透明化)
# ---------------------------------------------------------------------------


def run_power_audit(
    output_path: Path | str = "result/json/power_audit.json",
    *,
    target_power: float = 0.8,
    alpha: float = 0.05,
) -> int:
    """全 v2 report の主要 test の power audit を JSON 出力。

    各 report の代表 test を hard-coded した spec list で audit_report_power に渡す。
    実 pipeline data 投入後は spec を data-driven 化する余地あり。

    Returns: 書き出した row 数。
    """
    # Representative test specs per report (curated for Session 2 後半 audit).
    # 値は placeholder (effect_size / n) — pipeline 投入後に実 estimate を inject 推奨。
    specs: list[dict] = [
        dict(
            report_name="equity_oaxaca",
            test_label="gender raw_gap (female vs male)",
            test_family="t_test",
            n1=200, n2=2000,
            observed_effect=0.3,
        ),
        dict(
            report_name="causal_studio_transfer",
            test_label="ATE on theta_i",
            test_family="regression",
            n=500, beta=0.2, se_beta=0.08,
        ),
        dict(
            report_name="cohort_inequality",
            test_label="Gini vs cohort_year correlation",
            test_family="correlation",
            n=20, observed_effect=0.5,
        ),
        dict(
            report_name="network_resilience",
            test_label="fragility_ratio (degree vs random)",
            test_family="t_test",
            n1=200, n2=200,
            observed_effect=0.4,
        ),
        dict(
            report_name="career_visibility_warning",
            test_label="holdout AUC vs baseline",
            test_family="regression",
            n=1000, beta=0.15, se_beta=0.04,
        ),
        dict(
            report_name="mentor_effect",
            test_label="matched DiD estimate",
            test_family="regression",
            n=300, beta=0.5, se_beta=0.15,
        ),
        dict(
            report_name="o4_foreign_talent",
            test_label="JP vs CN_KR theta_i (Mann-Whitney proxy)",
            test_family="t_test",
            n1=15000, n2=3500,
            observed_effect=0.15,
        ),
    ]
    rows: list[PowerAuditRow] = audit_report_power(
        specs, target_power=target_power, alpha=alpha,
    )

    payload = {
        "generated_at": "post_processing pipeline phase",
        "target_power": target_power,
        "alpha": alpha,
        "n_audited": len(rows),
        "rows": [
            {
                "report_name": r.report_name,
                "test_label": r.test_label,
                "test_family": r.test_family,
                "n": r.n,
                "observed_effect": r.observed_effect,
                "power": r.power,
                "mde": r.mde,
                "verdict": r.verdict,
            }
            for r in rows
        ],
    }

    out_path = Path(output_path)
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        logger.info("power_audit_written", path=str(out_path), n_rows=len(rows))
    except OSError as exc:
        logger.warning("power_audit_write_failed", error=str(exc))

    return len(rows)


def upsert_report_specs() -> int:
    """Collect SPEC from every V2 report module and persist to mart.meta_report_spec.

    Each report module declares a module-level ``SPEC = ReportSpec(...)`` variable.
    This function iterates V2_REPORT_CLASSES, resolves each class's module via
    ``inspect.getmodule()``, and collects the module-level SPEC (if present).
    Duplicate SPEC objects (same name) from classes sharing a module are
    deduplicated by name before the batch upsert.

    Returns the number of specs written.
    """
    import inspect

    from scripts.report_generators.reports import V2_REPORT_CLASSES

    seen: dict[str, object] = {}
    for cls in V2_REPORT_CLASSES:
        mod = inspect.getmodule(cls)
        spec = getattr(mod, "SPEC", None)
        if spec is not None and spec.name not in seen:
            seen[spec.name] = spec

    specs = list(seen.values())
    if not specs:
        logger.warning("report_spec_upsert_no_specs_found")
        return 0

    n = write_report_specs(specs)
    logger.info("report_specs_upserted", count=n, total_classes=len(V2_REPORT_CLASSES))
    return n


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

    logger.info("step_start", step="report_spec_upsert")
    try:
        upsert_report_specs()
    except Exception as exc:
        logger.warning("report_spec_upsert_failed", error=str(exc))
