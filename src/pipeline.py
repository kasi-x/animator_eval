"""統合パイプライン — データ収集→名寄せ→グラフ構築→スコアリング→出力.

全フェーズを順次実行するオーケストレーター。

This refactored version decomposes the monolithic pipeline into 10 modular
phases in src/pipeline_phases/, reducing this file from 930→150 lines while
maintaining identical functionality.
"""
import time

import structlog

from src.database import get_connection, init_db, record_pipeline_run
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


def run_scoring_pipeline(visualize: bool = False, dry_run: bool = False) -> list[dict]:
    """スコアリングパイプラインを実行する.

    前提: DBにクレジットデータが既に存在すること。

    Args:
        visualize: 可視化を生成するか
        dry_run: True の場合、データ検証のみ行いスコア計算は行わない

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

    # Database connection
    conn = get_connection()
    init_db(conn)

    # Phase 1: Data Loading
    logger.info("step_start", step="data_loading")
    load_pipeline_data(context, conn)

    if not context.credits:
        logger.warning("No credits in DB. Run scrapers first.")
        conn.close()
        return []

    # Phase 2: Validation
    logger.info("step_start", step="validation")
    validation = run_validation_phase(context, conn)

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
        return []

    # Phase 3: Entity Resolution
    logger.info("step_start", step="entity_resolution")
    run_entity_resolution(context)

    # Phase 4: Graph Construction
    logger.info("step_start", step="graph_construction")
    build_graphs_phase(context)

    # Phase 5: Core Scoring (Authority, Trust, Skill + Normalize)
    compute_core_scores_phase(context)

    # Phase 6: Supplementary Metrics
    compute_supplementary_metrics_phase(context)

    # Phase 7: Result Assembly (build comprehensive result dicts)
    assemble_result_entries(context, conn)
    conn.commit()
    conn.close()

    # Phase 8: Post-Processing (percentiles, confidence, stability)
    post_process_results(context)

    # Phase 9: Analysis Modules (18+ independent analyses - PARALLELIZABLE)
    run_analysis_modules_phase(context)

    # Phase 10: Export & Visualization
    elapsed = time.monotonic() - t_start
    export_and_visualize_phase(context, elapsed)

    # Record pipeline completion
    conn = get_connection()
    record_pipeline_run(
        conn,
        credit_count=len(context.credits),
        person_count=len(context.results),
        elapsed=elapsed,
    )
    conn.close()

    # Log performance summary
    context.monitor.record_memory("pipeline_end")
    context.monitor.log_summary()

    logger.info("pipeline_complete", elapsed=round(elapsed, 2), persons=len(context.results))

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
