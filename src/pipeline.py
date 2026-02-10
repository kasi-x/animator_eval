"""統合パイプライン — データ収集→名寄せ→グラフ構築→スコアリング→出力.

全フェーズを順次実行するオーケストレーター。

This refactored version decomposes the monolithic pipeline into 10 modular
phases in src/pipeline_phases/, reducing this file from 930→150 lines while
maintaining identical functionality.
"""
import time

import structlog

from src.database import db_connection, get_connection, init_db, record_pipeline_run
from src.utils.config import JSON_DIR  # noqa: F401 - Imported for test monkeypatch compatibility
from src.pipeline_phases import (
    PipelineContext,
    assemble_result_entries,
    build_graphs_phase,
    compute_core_scores_phase,
    compute_supplementary_metrics_phase,
    export_and_visualize_phase,
    load_pipeline_data,
    post_process_results,
    run_analysis_modules_phase,
    run_entity_resolution,
    run_validation_phase,
)
from src.utils.performance import reset_monitor

logger = structlog.get_logger()


def run_scoring_pipeline(visualize: bool = False, dry_run: bool = False, enable_websocket: bool = True) -> list[dict]:
    """スコアリングパイプラインを実行する.

    前提: DBにクレジットデータが既に存在すること。

    Args:
        visualize: 可視化を生成するか
        dry_run: True の場合、データ検証のみ行いスコア計算は行わない
        enable_websocket: WebSocket進捗配信を有効にするか (default: True)

    Returns:
        list[dict]: スコア計算結果 (person_id, composite, authority, trust, skill, etc.)

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

    # Database connection
    conn = get_connection()
    init_db(conn)

    # Phase 1: Data Loading
    logger.info("step_start", step="data_loading")
    if ws_broadcaster:
        ws_broadcaster.update_phase(1, "Data Loading", "running")

    phase_start = time.monotonic()
    load_pipeline_data(context, conn)

    if ws_broadcaster:
        ws_broadcaster.complete_phase(1, "Data Loading", (time.monotonic() - phase_start) * 1000)

    if not context.credits:
        logger.warning("No credits in DB. Run scrapers first.")
        conn.close()
        if ws_broadcaster:
            ws_broadcaster.error_phase(1, "Data Loading", "No credits in database")
        return []

    # Phase 2: Validation
    logger.info("step_start", step="validation")
    if ws_broadcaster:
        ws_broadcaster.update_phase(2, "Validation", "running")

    phase_start = time.monotonic()
    validation = run_validation_phase(context, conn)

    if ws_broadcaster:
        ws_broadcaster.complete_phase(2, "Validation", (time.monotonic() - phase_start) * 1000)

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
    run_entity_resolution(context)

    if ws_broadcaster:
        ws_broadcaster.complete_phase(3, "Entity Resolution", (time.monotonic() - phase_start) * 1000)

    # Phase 4: Graph Construction
    logger.info("step_start", step="graph_construction")
    if ws_broadcaster:
        ws_broadcaster.update_phase(4, "Graph Construction", "running")

    phase_start = time.monotonic()
    build_graphs_phase(context)

    if ws_broadcaster:
        ws_broadcaster.complete_phase(4, "Graph Construction", (time.monotonic() - phase_start) * 1000)

    # Phase 5: Core Scoring (Authority, Trust, Skill + Normalize)
    if ws_broadcaster:
        ws_broadcaster.update_phase(5, "Core Scoring", "running")

    phase_start = time.monotonic()
    compute_core_scores_phase(context)

    if ws_broadcaster:
        ws_broadcaster.complete_phase(5, "Core Scoring", (time.monotonic() - phase_start) * 1000)

    # Phase 6: Supplementary Metrics
    if ws_broadcaster:
        ws_broadcaster.update_phase(6, "Supplementary Metrics", "running")

    phase_start = time.monotonic()
    compute_supplementary_metrics_phase(context)

    if ws_broadcaster:
        ws_broadcaster.complete_phase(6, "Supplementary Metrics", (time.monotonic() - phase_start) * 1000)

    # Phase 7: Result Assembly (build comprehensive result dicts)
    if ws_broadcaster:
        ws_broadcaster.update_phase(7, "Result Assembly", "running")

    phase_start = time.monotonic()
    assemble_result_entries(context, conn)
    conn.commit()
    conn.close()

    if ws_broadcaster:
        ws_broadcaster.complete_phase(7, "Result Assembly", (time.monotonic() - phase_start) * 1000)

    # Phase 8: Post-Processing (percentiles, confidence, stability)
    if ws_broadcaster:
        ws_broadcaster.update_phase(8, "Post-Processing", "running")

    phase_start = time.monotonic()
    post_process_results(context)

    if ws_broadcaster:
        ws_broadcaster.complete_phase(8, "Post-Processing", (time.monotonic() - phase_start) * 1000)

    # Phase 9: Analysis Modules (18+ independent analyses - PARALLELIZABLE)
    if ws_broadcaster:
        ws_broadcaster.update_phase(9, "Analysis Modules", "running")

    phase_start = time.monotonic()
    run_analysis_modules_phase(context)

    if ws_broadcaster:
        ws_broadcaster.complete_phase(9, "Analysis Modules", (time.monotonic() - phase_start) * 1000)

    # Phase 10: Export & Visualization
    if ws_broadcaster:
        ws_broadcaster.update_phase(10, "Export & Visualization", "running")

    phase_start = time.monotonic()
    elapsed = time.monotonic() - t_start
    export_and_visualize_phase(context, elapsed)

    if ws_broadcaster:
        ws_broadcaster.complete_phase(10, "Export & Visualization", (time.monotonic() - phase_start) * 1000)

    # Record pipeline completion
    with db_connection() as conn:
        record_pipeline_run(
            conn,
            credit_count=len(context.credits),
            person_count=len(context.results),
            elapsed=elapsed,
        )

    # Log performance summary and export report
    context.monitor.record_memory("pipeline_end")
    context.monitor.log_summary()

    # Export performance report to JSON
    from src.utils.config import JSON_DIR
    perf_report_path = JSON_DIR / f"performance_{time.strftime('%Y%m%d_%H%M%S')}.json"
    context.monitor.export_report(perf_report_path)
    logger.info("performance_report_saved", path=str(perf_report_path))

    logger.info("pipeline_complete", elapsed=round(elapsed, 2), persons=len(context.results))

    # Broadcast pipeline completion
    if ws_broadcaster:
        ws_broadcaster.complete_pipeline(len(context.results), elapsed)

    return context.results


def main() -> None:
    """エントリーポイント."""
    import argparse

    from src.log import setup_logging

    setup_logging()

    parser = argparse.ArgumentParser(description="Animetor Eval パイプライン")
    parser.add_argument("--visualize", action="store_true", help="可視化を生成")
    parser.add_argument("--dry-run", action="store_true", help="データ検証のみ（スコア計算なし）")
    args = parser.parse_args()

    run_scoring_pipeline(visualize=args.visualize, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
