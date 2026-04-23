"""Pipeline entry point — thin Hamilton Driver wrapper (H-4).

Phases 1-9 execute via Hamilton DAG.  The Driver receives two primitive
inputs (visualize, dry_run) and computes PipelineContext internally via the
ctx node in hamilton_modules/loading.py.

Phase 10 (export) and the VA sub-pipeline run after the DAG because they need
the fully-populated ctx object and are not yet converted to Hamilton nodes.
"""

from __future__ import annotations

import time

import structlog

from src.utils.config import JSON_DIR  # noqa: F401 — imported for test monkeypatch compatibility

logger = structlog.get_logger()


def _build_driver(*, with_checkpoint: bool = False):
    from pathlib import Path

    from hamilton import driver
    from src.pipeline_phases.hamilton_modules import (
        assembly, causal, core, genre, loading, metrics,
        network, resolution, scoring, studio,
    )
    from src.pipeline_phases.lifecycle import CheckpointHook, TimingHook

    adapters: list = [TimingHook()]
    if with_checkpoint:
        adapters.append(CheckpointHook(Path(JSON_DIR)))

    return (
        driver.Builder()
        .with_modules(loading, resolution, scoring, metrics, assembly,
                      core, studio, genre, network, causal)
        .with_adapters(*adapters)
        .build()
    )


def _build_phase14_driver():
    """Driver for Phases 1-4 only (loading + resolution).  Used on resume."""
    from hamilton import driver
    from src.pipeline_phases.hamilton_modules import loading, resolution
    from src.pipeline_phases.lifecycle import TimingHook

    return (
        driver.Builder()
        .with_modules(loading, resolution)
        .with_adapters(TimingHook())
        .build()
    )


def run_scoring_pipeline(
    visualize: bool = False,
    dry_run: bool = False,
    enable_websocket: bool = True,
    incremental: bool = False,
    resume: bool = False,
) -> list[dict]:
    """Run the scoring pipeline.

    Precondition: credit data must already exist in the database.

    Args:
        visualize: generate matplotlib/Plotly visualizations
        dry_run: data validation only, no scoring
        enable_websocket: broadcast progress to connected WS clients
        incremental: skip pipeline when no credit changes since last run
        resume: attempt to resume from last checkpoint; re-runs Phases 1-4,
                restores Phase 5-8 scores/results from checkpoint, then runs
                Phase 9 analysis.  Falls back to full run if no valid checkpoint.

    Returns:
        list[dict] scoring results
    """
    import datetime

    from src.analysis.feat_precompute import (
        compute_feat_career_annual_ddb,
        compute_feat_credit_activity_ddb,
        compute_feat_person_role_progression_ddb,
    )
    from src.utils.performance import reset_monitor

    t_start = time.monotonic()
    reset_monitor()

    # ── Incremental mode (JSON cache only — DB change detection removed) ──────
    if incremental and not dry_run:
        from src.utils.json_io import load_person_scores_from_json

        cached = load_person_scores_from_json()
        if cached:
            logger.info("incremental_cached", persons=len(cached))
            return cached
        logger.info("incremental_fallback", reason="no_cached_results")

    # ── Phase 1.5: pre-aggregate derived features ────────────────────────────
    current_year = datetime.datetime.now().year
    current_quarter = (datetime.datetime.now().month - 1) // 3 + 1
    for fn, label in [
        (
            lambda: compute_feat_credit_activity_ddb(
                current_year=current_year, current_quarter=current_quarter
            ),
            "credit_activity",
        ),
        (compute_feat_career_annual_ddb, "career_annual"),
        (
            lambda: compute_feat_person_role_progression_ddb(current_year=current_year),
            "role_progression",
        ),
    ]:
        try:
            fn()
        except Exception:
            logger.exception("feat_skipped", label=label)

    # ── WebSocket broadcaster ─────────────────────────────────────────────────
    ws_broadcaster = None
    if enable_websocket:
        try:
            from src.infra.websocket import PipelineProgressBroadcaster

            ws_broadcaster = PipelineProgressBroadcaster()
            ws_broadcaster.start_pipeline(total_phases=10)
        except Exception:
            pass

    # ── Hamilton: Phases 1-9 ─────────────────────────────────────────────────
    from src.pipeline_phases.analysis_modules import run_analysis_modules_phase
    from src.pipeline_phases.context import PipelineCheckpoint

    if dry_run:
        dr = _build_driver()
        result = dr.execute(
            ["data_validated"],
            inputs={"visualize": visualize, "dry_run": True},
        )
        validation = result.get("data_validated")
        elapsed = time.monotonic() - t_start
        logger.info(
            "dry_run_complete",
            elapsed=round(elapsed, 2),
            validation_passed=getattr(validation, "passed", None),
            errors=len(getattr(validation, "errors", [])),
            warnings=len(getattr(validation, "warnings", [])),
        )
        if ws_broadcaster:
            ws_broadcaster.complete_pipeline(0, elapsed)
        return []

    # ── Resume path ───────────────────────────────────────────────────────────
    checkpoint_store = PipelineCheckpoint(JSON_DIR)
    checkpoint = checkpoint_store.load() if resume else None
    resumed = False

    if checkpoint is not None and checkpoint.get("last_completed_phase", 0) >= 7:
        # Phases 1-4: re-run to reconstruct raw data / graphs
        dr14 = _build_phase14_driver()
        result14 = dr14.execute(
            ["graphs_built", "ctx"],
            inputs={"visualize": visualize, "dry_run": False},
        )
        ctx = result14["ctx"]

        if checkpoint_store.is_compatible(checkpoint, ctx):
            checkpoint_store.restore_to_context(checkpoint, ctx)
            logger.info(
                "resume_from_checkpoint",
                phase=checkpoint["last_completed_phase"],
                persons=len(ctx.results),
            )
            run_analysis_modules_phase(ctx)
            resumed = True
        else:
            logger.warning("resume_checkpoint_incompatible", falling_back="full_run")

    if not resumed:
        # Full Phase 1-8 run with CheckpointHook enabled
        dr = _build_driver(with_checkpoint=True)
        result = dr.execute(
            ["results_post_processed", "ctx"],
            inputs={"visualize": visualize, "dry_run": False},
        )
        ctx = result["ctx"]
        run_analysis_modules_phase(ctx)

    elapsed = time.monotonic() - t_start

    # ── VA Pipeline (Phases 4B-7B) ────────────────────────────────────────────
    if ctx.va_credits:
        from src.pipeline_phases import (
            assemble_va_results,
            build_va_graphs_phase,
            compute_va_core_scores_phase,
            compute_va_supplementary_metrics_phase,
        )

        build_va_graphs_phase(ctx)
        compute_va_core_scores_phase(ctx)
        compute_va_supplementary_metrics_phase(ctx)
        assemble_va_results(ctx)

    # ── Phase 9 post-step: persist meta_common_person_parameters ─────────────
    person_params = ctx.analysis_results.get("person_parameters")
    if person_params:
        from src.analysis.person_parameters import populate_meta_common_person_parameters

        populate_meta_common_person_parameters(person_params)

    # ── Phase 10: Export & Visualization ─────────────────────────────────────
    from src.pipeline_phases import export_and_visualize_phase

    export_and_visualize_phase(ctx, elapsed)

    # ── Checkpoint cleanup ────────────────────────────────────────────────────
    from src.pipeline_phases.context import PipelineCheckpoint

    PipelineCheckpoint(JSON_DIR).delete()

    # ── Performance report ────────────────────────────────────────────────────
    ctx.monitor.record_memory("pipeline_end")
    ctx.monitor.log_summary()
    perf_path = JSON_DIR / f"performance_{time.strftime('%Y%m%d_%H%M%S')}.json"
    ctx.monitor.export_report(perf_path)
    for old in sorted(JSON_DIR.glob("performance_*.json"))[:-10]:
        old.unlink(missing_ok=True)

    logger.info("pipeline_complete", elapsed=round(elapsed, 2), persons=len(ctx.results))

    if ws_broadcaster:
        ws_broadcaster.complete_pipeline(len(ctx.results), elapsed)

    return ctx.results


def main() -> None:
    """Entry point."""
    import argparse

    from src.infra.log import setup_logging

    setup_logging()

    parser = argparse.ArgumentParser(description="Animetor Eval パイプライン")
    parser.add_argument("--visualize", action="store_true", help="可視化を生成")
    parser.add_argument("--dry-run", action="store_true", help="データ検証のみ（スコア計算なし）")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="増分モード: データ変化がなければキャッシュ結果を返す",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="クラッシュ再開 (H-4では常にフル実行; CheckpointHook実装後に有効化)",
    )
    args = parser.parse_args()

    run_scoring_pipeline(
        visualize=args.visualize,
        dry_run=args.dry_run,
        incremental=args.incremental,
        resume=args.resume,
    )


if __name__ == "__main__":
    main()
