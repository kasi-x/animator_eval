"""Integrated pipeline — data collection → entity resolution → graph construction → scoring → export.

Orchestrator that runs all phases sequentially.

This refactored version decomposes the monolithic pipeline into 10 modular
phases in src/pipeline_phases/, reducing this file from 930→150 lines while
maintaining identical functionality.
"""

import time

import structlog

from src.database import (
    compute_feat_career_annual,
    compute_feat_credit_activity,
    compute_feat_person_role_progression,
    compute_feat_studio_affiliation,
    db_connection,
    get_connection,
    has_credits_changed_since_last_run,
    init_db,
    record_pipeline_run,
)
from src.utils.config import JSON_DIR  # noqa: F401 - Imported for test monkeypatch compatibility
from src.pipeline_phases import (
    PipelineCheckpoint,
    PipelineContext,
    assemble_result_entries,
    assemble_va_results,
    build_graphs_phase,
    build_va_graphs_phase,
    compute_core_scores_phase,
    compute_supplementary_metrics_phase,
    compute_va_core_scores_phase,
    compute_va_supplementary_metrics_phase,
    export_and_visualize_phase,
    load_pipeline_data,
    post_process_results,
    run_analysis_modules_phase,
    run_entity_resolution,
    run_validation_phase,
)
from src.utils.performance import reset_monitor

logger = structlog.get_logger()


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
        visualize: whether to generate visualizations
        dry_run: if True, perform data validation only without computing scores
        enable_websocket: enable WebSocket progress broadcasting (default: True)
        incremental: if True, return cached results when no data has changed since last run
        resume: if True, resume from the last crash checkpoint

    Returns:
        list[dict]: scoring results (person_id, composite, authority, trust, skill, etc.)

    Pipeline Phases:
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
    """
    # Initialize
    t_start = time.monotonic()
    reset_monitor()
    context = PipelineContext(visualize=visualize, dry_run=dry_run)
    context.monitor.record_memory("pipeline_start")

    # Initialize WebSocket broadcaster (if enabled)
    ws_broadcaster = None
    if enable_websocket:
        from src.websocket_manager import PipelineProgressBroadcaster

        ws_broadcaster = PipelineProgressBroadcaster()
        ws_broadcaster.start_pipeline(total_phases=10)

    # Checkpoint manager for crash resume
    checkpoint_mgr = PipelineCheckpoint(JSON_DIR)
    resume_from_phase = 0  # 0 = start from beginning

    # Database connection
    conn = get_connection()
    init_db(conn)

    # ETL: src_* → canonical tables
    from src.etl.integrate import run_integration

    run_integration(conn)

    # Incremental mode: skip pipeline if data hasn't changed
    if incremental and not dry_run:
        if not has_credits_changed_since_last_run(conn):
            conn.close()
            logger.info("incremental_skip", reason="no_credit_changes")
            # Load cached results from last export
            from src.utils.json_io import load_person_scores_from_json

            cached = load_person_scores_from_json()
            if cached:
                logger.info("incremental_cached", persons=len(cached))
                if ws_broadcaster:
                    ws_broadcaster.complete_pipeline(len(cached), 0.0)
                return cached
            # Cached results missing — fall through to full pipeline
            logger.info("incremental_fallback", reason="no_cached_results")
            conn = get_connection()
        else:
            logger.info("incremental_detected_changes", mode="full_recompute")

    # Phase 1: Data Loading
    logger.info("step_start", step="data_loading")
    if ws_broadcaster:
        ws_broadcaster.update_phase(1, "Data Loading", "running")

    phase_start = time.monotonic()
    load_pipeline_data(context, conn)

    if ws_broadcaster:
        ws_broadcaster.complete_phase(
            1, "Data Loading", (time.monotonic() - phase_start) * 1000
        )

    if not context.credits:
        logger.warning("No credits in DB. Run scrapers first.")
        conn.close()
        if ws_broadcaster:
            ws_broadcaster.error_phase(1, "Data Loading", "No credits in database")
        return []

    # Phase 1.5: pre-aggregate derived features (best-effort, non-blocking)
    # Computed from raw credit data; reusable by subsequent phases and report generators
    try:
        compute_feat_credit_activity(
            conn,
            current_year=context.current_year,
            current_quarter=context.current_quarter,
        )
    except Exception:
        logger.exception("feat_credit_activity_skipped")
    try:
        compute_feat_career_annual(conn)
    except Exception:
        logger.exception("feat_career_annual_skipped")
    try:
        compute_feat_studio_affiliation(conn)
    except Exception:
        logger.exception("feat_studio_affiliation_skipped")
    try:
        compute_feat_person_role_progression(conn, current_year=context.current_year)
    except Exception:
        logger.exception("feat_person_role_progression_skipped")
    # NOTE: compute_feat_credit_contribution / feat_work_context / feat_causal_estimates depend on
    # feat_person_scores, so they are called in export_and_visualize_phase (Phase 10)

    # Phase 2: Validation
    logger.info("step_start", step="validation")
    if ws_broadcaster:
        ws_broadcaster.update_phase(2, "Validation", "running")

    phase_start = time.monotonic()
    validation = run_validation_phase(context, conn)

    if ws_broadcaster:
        ws_broadcaster.complete_phase(
            2, "Validation", (time.monotonic() - phase_start) * 1000
        )

    if dry_run:
        conn.close()
        elapsed = time.monotonic() - t_start
        logger.info(
            "dry_run_complete",
            elapsed=round(elapsed, 2),
            persons=len(context.persons),
            anime=len(context.anime_list),
            credits=len(context.credits),
            validation_passed=validation.passed,
            errors=len(validation.errors),
            warnings=len(validation.warnings),
        )
        if ws_broadcaster:
            ws_broadcaster.complete_pipeline(0, elapsed)
        return []

    # Phase 3: Entity Resolution
    logger.info("step_start", step="entity_resolution")
    if ws_broadcaster:
        ws_broadcaster.update_phase(3, "Entity Resolution", "running")

    phase_start = time.monotonic()
    run_entity_resolution(context, conn=conn)

    if ws_broadcaster:
        ws_broadcaster.complete_phase(
            3, "Entity Resolution", (time.monotonic() - phase_start) * 1000
        )

    # Early resume check: skip graph construction if checkpoint covers phases 5+
    # Graph construction consumes ~80 GB for 43M edges — skip when not needed.
    if resume and not dry_run:
        saved = checkpoint_mgr.load()
        if saved and checkpoint_mgr.is_compatible(saved, context):
            resume_from_phase = checkpoint_mgr.restore_to_context(saved, context)
            logger.info("pipeline_resuming", from_phase=resume_from_phase + 1)
        elif saved:
            logger.info("checkpoint_incompatible", reason="data_changed")

    # Phase 4: Graph Construction (skip if resuming from phase 5+)
    if resume_from_phase < 4:
        logger.info("step_start", step="graph_construction")
        if ws_broadcaster:
            ws_broadcaster.update_phase(4, "Graph Construction", "running")

        phase_start = time.monotonic()
        build_graphs_phase(context)

        if ws_broadcaster:
            ws_broadcaster.complete_phase(
                4, "Graph Construction", (time.monotonic() - phase_start) * 1000
            )

        # Phase 4B: VA Graph Construction (parallel-safe, runs after production graphs)
        if context.va_credits:
            build_va_graphs_phase(context)
    else:
        logger.info("phase_skipped", phase=4, reason="checkpoint_resume")

    # Phase 5: Core Scoring (Authority, Trust, Skill + Normalize)
    if resume_from_phase < 5:
        if ws_broadcaster:
            ws_broadcaster.update_phase(5, "Core Scoring", "running")

        phase_start = time.monotonic()
        compute_core_scores_phase(context)

        if ws_broadcaster:
            ws_broadcaster.complete_phase(
                5, "Core Scoring", (time.monotonic() - phase_start) * 1000
            )

        checkpoint_mgr.save(5, context)

        # Phase 5B: VA Core Scoring (after production scoring provides BiRank for SD)
        if context.va_credits:
            compute_va_core_scores_phase(context)

    # Phase 6: Supplementary Metrics
    if resume_from_phase < 6:
        if ws_broadcaster:
            ws_broadcaster.update_phase(6, "Supplementary Metrics", "running")

        phase_start = time.monotonic()
        compute_supplementary_metrics_phase(context)

        if ws_broadcaster:
            ws_broadcaster.complete_phase(
                6, "Supplementary Metrics", (time.monotonic() - phase_start) * 1000
            )

        # Phase 6B: VA Supplementary Metrics
        if context.va_credits:
            compute_va_supplementary_metrics_phase(context)

    # Phase 7: Result Assembly (build comprehensive result dicts)
    if resume_from_phase < 7:
        if ws_broadcaster:
            ws_broadcaster.update_phase(7, "Result Assembly", "running")

        phase_start = time.monotonic()
        assemble_result_entries(context, conn)
        conn.commit()
        conn.close()

        # Phase 7B: VA Result Assembly
        if context.va_credits:
            assemble_va_results(context)

        if ws_broadcaster:
            ws_broadcaster.complete_phase(
                7, "Result Assembly", (time.monotonic() - phase_start) * 1000
            )

        checkpoint_mgr.save(7, context)
    else:
        conn.close()

    # Phase 8: Post-Processing (percentiles, confidence, stability)
    if resume_from_phase < 8:
        if ws_broadcaster:
            ws_broadcaster.update_phase(8, "Post-Processing", "running")

        phase_start = time.monotonic()
        post_process_results(context)

        if ws_broadcaster:
            ws_broadcaster.complete_phase(
                8, "Post-Processing", (time.monotonic() - phase_start) * 1000
            )

    # Phase 9: Analysis Modules (18+ independent analyses - PARALLELIZABLE)
    if resume_from_phase < 9:
        if ws_broadcaster:
            ws_broadcaster.update_phase(9, "Analysis Modules", "running")

        phase_start = time.monotonic()
        run_analysis_modules_phase(context)
        person_params = context.analysis_results.get("person_parameters")
        if person_params:
            from src.analysis.person_parameters import (
                populate_meta_common_person_parameters,
            )

            with db_connection() as meta_conn:
                populate_meta_common_person_parameters(meta_conn, person_params)

        if ws_broadcaster:
            ws_broadcaster.complete_phase(
                9, "Analysis Modules", (time.monotonic() - phase_start) * 1000
            )

        checkpoint_mgr.save(9, context)

    # Phase 10: Export & Visualization
    if ws_broadcaster:
        ws_broadcaster.update_phase(10, "Export & Visualization", "running")

    phase_start = time.monotonic()
    elapsed = time.monotonic() - t_start
    export_and_visualize_phase(context, elapsed)

    if ws_broadcaster:
        ws_broadcaster.complete_phase(
            10, "Export & Visualization", (time.monotonic() - phase_start) * 1000
        )

    # Record pipeline completion and clean up checkpoint
    checkpoint_mgr.delete()
    with db_connection() as conn:
        record_pipeline_run(
            conn,
            credit_count=len(context.credits),
            person_count=len(context.results),
            elapsed=elapsed,
            mode="resume" if resume else ("incremental" if incremental else "full"),
        )
        # Phase 4-6 / V-3: persist per-run quality snapshots for drift checks.
        try:
            from scripts.monitoring.compute_quality_snapshot import compute_and_write

            compute_and_write(conn)
        except ImportError:
            logger.exception("quality_snapshot_import_failed")
        except Exception:
            logger.exception("quality_snapshot_write_failed")

    # Log performance summary and export report
    context.monitor.record_memory("pipeline_end")
    context.monitor.log_summary()

    # Export performance report to JSON (keep only the last 10)
    perf_report_path = JSON_DIR / f"performance_{time.strftime('%Y%m%d_%H%M%S')}.json"
    context.monitor.export_report(perf_report_path)
    logger.info("performance_report_saved", path=str(perf_report_path))
    old_perf = sorted(JSON_DIR.glob("performance_*.json"))[:-10]
    for p in old_perf:
        p.unlink(missing_ok=True)

    logger.info(
        "pipeline_complete", elapsed=round(elapsed, 2), persons=len(context.results)
    )

    # Broadcast pipeline completion
    if ws_broadcaster:
        ws_broadcaster.complete_pipeline(len(context.results), elapsed)

    return context.results


def main() -> None:
    """Entry point."""
    import argparse

    from src.log import setup_logging

    setup_logging()

    parser = argparse.ArgumentParser(description="Animetor Eval パイプライン")
    parser.add_argument("--visualize", action="store_true", help="可視化を生成")
    parser.add_argument(
        "--dry-run", action="store_true", help="データ検証のみ（スコア計算なし）"
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="増分モード: データ変化がなければキャッシュ結果を返す",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="クラッシュ再開: 前回中断した地点から再開する",
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
