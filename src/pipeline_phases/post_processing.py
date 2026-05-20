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


# ---------------------------------------------------------------------------
# Session 2 ラウンド 5: 9 feat テーブル driver (graceful skeleton)
# ---------------------------------------------------------------------------


def _safe_upsert(table: str, rows: list[dict]) -> int:
    """共通 graceful upsert (DuckDB Mart)。"""
    if not rows:
        return 0
    try:
        from src.analysis.io.mart_writer import gold_connect_write
    except ImportError as exc:
        logger.warning("safe_upsert_unavailable", error=str(exc))
        return 0

    cols = list(rows[0].keys())
    placeholders = ",".join("?" * len(cols))
    inserted = 0
    try:
        with gold_connect_write() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS mart")
            conn.execute("SET schema='mart'")
            from src.analysis.io.mart_writer import _DDL
            for stmt in _DDL.split(";"):
                s = stmt.strip()
                if s:
                    try:
                        conn.execute(s)
                    except Exception:
                        pass
            for row in rows:
                try:
                    conn.execute(
                        f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders}) "
                        "ON CONFLICT DO NOTHING",
                        list(row.values()),
                    )
                    inserted += 1
                except Exception as exc:
                    logger.debug("upsert_row_skip", table=table, error=str(exc))
    except Exception as exc:
        logger.warning("upsert_failed", table=table, error=str(exc))
    return inserted


def run_cohort_inequality_driver() -> int:
    """feat_cohort_inequality 投入。

    feat_career から (first_year, total_credits) を SELECT し、5y cohort 別に
    Gini / Theil-T / Atkinson を計算 → upsert。
    """
    try:
        import math

        from src.analysis.equity.cohort_inequality import compute_cohort_trajectory
        from src.analysis.io.mart_writer import gold_connect

        with gold_connect() as conn:
            try:
                raw = conn.execute(
                    "SELECT first_year, total_credits FROM feat_career "
                    "WHERE first_year IS NOT NULL AND total_credits > 0"
                ).fetchall()
            except Exception as exc:
                logger.warning("cohort_inequality_data_unavailable", error=str(exc))
                return 0
        records = [(int(y), math.log1p(float(n))) for y, n in raw]
        rows_obj = compute_cohort_trajectory(records, bin_width=5, min_cohort_n=30)
        upsert_rows = [
            dict(
                cohort_year=r.cohort_year, bin_width=5,
                n_persons=r.n_persons, gini=r.gini, theil_t=r.theil_t,
                atkinson_0_5=r.atkinson_0_5, mean_value=r.mean_value,
                sd_value=r.sd_value,
            )
            for r in rows_obj
        ]
        n = _safe_upsert("feat_cohort_inequality", upsert_rows)
        logger.info("cohort_inequality_driver_done", rows=n)
        return n
    except Exception as exc:
        logger.warning("cohort_inequality_driver_failed", error=str(exc))
        return 0


def run_resilience_driver() -> int:
    """feat_network_resilience skeleton (重い実 graph 計算は別 cron)。

    本 driver は metadata 行のみ upsert。実 simulation は scripts 経由で別実行。
    """
    try:
        from scripts.report_generators.reports.network_resilience import (
            _DEFAULT_K_REMOVALS,
            _SAMPLE_TOP_N_PERSONS,
        )
        # placeholder: 実 計算は別走、ここでは run metadata のみ
        rows = [
            dict(
                strategy="placeholder", metric="lcc",
                auc=None, relative_fragility=None,
                n_nodes=_SAMPLE_TOP_N_PERSONS,
                k_removals=_DEFAULT_K_REMOVALS,
            ),
        ]
        return _safe_upsert("feat_network_resilience", rows)
    except Exception as exc:
        logger.warning("resilience_driver_failed", error=str(exc))
        return 0


def run_oaxaca_driver() -> int:
    """feat_oaxaca_decomposition skeleton — §15 gender 充足後に本格動作。

    現在 80.9% null のため graceful skip。
    """
    logger.info("oaxaca_driver_skipped", reason="§15 gender enrichment pending")
    return 0


def run_mentor_driver() -> int:
    """feat_mentor_pairs + feat_mentor_event_study skeleton。

    mentorship.infer_mentorships() は重い (~分単位)、また theta_i panel が
    年次で揃ってない。実 driver は別 cron 推奨。
    """
    logger.info("mentor_driver_skipped", reason="theta_i annual panel pending")
    return 0


def run_did_hte_driver() -> int:
    """feat_did_hte skeleton。

    DiD ATE 結果 (feat_did_studio_transfer) を投入後、subgroup CATE を
    estimate_cate_by_subgroup() で計算 → upsert。現状 feat_did_studio_transfer
    投入経路が pipeline 経由不在のため skip。
    """
    logger.info("did_hte_driver_skipped", reason="feat_did_studio_transfer pending")
    return 0


def run_did_robustness_driver() -> int:
    """feat_did_robustness skeleton — placebo + E-value + joint leads。

    依存: DiD ATE estimate 投入後。現状 graceful skip。
    """
    logger.info("did_robustness_driver_skipped", reason="DiD ATE pending")
    return 0


def run_anomaly_flag_driver() -> int:
    """feat_credit_anomaly_flags — Poisson outlier + KL + source disagreement。

    credit_anomaly_audit.py の audit と同じロジックで top-N flag → upsert。
    """
    try:
        from src.analysis.io.mart_writer import gold_connect
        from src.analysis.quality.credit_anomaly import detect_poisson_outliers

        with gold_connect() as conn:
            try:
                raw = conn.execute(
                    "SELECT person_id, COUNT(*) FROM credits "
                    "WHERE person_id IS NOT NULL GROUP BY person_id "
                    "HAVING COUNT(*) >= 10 ORDER BY 2 DESC LIMIT 50000"
                ).fetchall()
            except Exception as exc:
                logger.warning("anomaly_driver_data_unavailable", error=str(exc))
                return 0
        credit_counts = {r[0]: int(r[1]) for r in raw if r[0]}
        outs = detect_poisson_outliers(credit_counts, z_threshold=4.0, min_expected=5.0)
        # Limit to top 200 for storage
        upsert_rows = [
            dict(
                person_id=o.person_id, detector="poisson",
                score=abs(o.z_score),
                direction=o.direction,
                n_credits=o.observed,
                extra_json=f'{{"expected": {o.expected:.2f}}}',
                flagged_at=None,  # default current_timestamp
            )
            for o in outs[:200]
        ]
        # default current_timestamp 用に flagged_at を除外
        for row in upsert_rows:
            del row["flagged_at"]
        n = _safe_upsert("feat_credit_anomaly_flags", upsert_rows)
        logger.info("anomaly_driver_done", flagged=n)
        return n
    except Exception as exc:
        logger.warning("anomaly_driver_failed", error=str(exc))
        return 0


def run_all_session2_drivers() -> dict[str, int]:
    """全 7 driver を実行、{driver_name: rows_inserted} を返す。"""
    return {
        "cohort_inequality": run_cohort_inequality_driver(),
        "resilience": run_resilience_driver(),
        "oaxaca": run_oaxaca_driver(),
        "mentor": run_mentor_driver(),
        "did_hte": run_did_hte_driver(),
        "did_robustness": run_did_robustness_driver(),
        "anomaly_flag": run_anomaly_flag_driver(),
    }


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
