"""Benchmark: Hamilton DAG vs ThreadPoolExecutor for Phase 9 analysis modules.

Compares run_analysis_modules_hamilton() against run_analysis_modules_phase()
on identical synthetic data. Used by H-1 acceptance gate (overhead must be <20%).

Usage:
    python benchmarks/bench_hamilton_h1.py
"""

from __future__ import annotations

import json
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import structlog

from src.log import setup_logging

setup_logging()
logger = structlog.get_logger()

BENCH_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BENCH_DIR / "results"
HAMILTON_RESULT_PATH = RESULTS_DIR / "hamilton_h1.json"

SYNTH_DIRECTORS = 10
SYNTH_ANIMATORS = 100
SYNTH_ANIME = 50
SYNTH_SEED = 42

OVERHEAD_THRESHOLD = 0.20  # H-1 acceptance gate


def _populate_temp_db(db_path: Path) -> int:
    from src.db import (
        get_connection,
        init_db,
        insert_credit,
        upsert_anime,
        upsert_person,
    )
    from src.synthetic import generate_synthetic_data

    persons, anime_list, credits = generate_synthetic_data(
        n_directors=SYNTH_DIRECTORS,
        n_animators=SYNTH_ANIMATORS,
        n_anime=SYNTH_ANIME,
        seed=SYNTH_SEED,
    )

    conn = get_connection(db_path)
    init_db(conn)
    for p in persons:
        upsert_person(conn, p)
    for a in anime_list:
        upsert_anime(conn, a)
    for c in credits:
        insert_credit(conn, c)
    conn.commit()
    conn.close()
    return len(credits)


def _build_context_through_phase8():
    """Run phases 1-8 to produce a fully-populated PipelineContext for Phase 9."""
    from src.db import get_connection, init_db
    from src.pipeline_phases import (
        PipelineContext,
        assemble_result_entries,
        build_graphs_phase,
        compute_core_scores_phase,
        compute_supplementary_metrics_phase,
        load_pipeline_data,
        post_process_results,
        run_entity_resolution,
        run_validation_phase,
    )
    from src.utils.performance import reset_monitor

    reset_monitor()
    context = PipelineContext(visualize=False, dry_run=False)
    conn = get_connection()
    init_db(conn)

    load_pipeline_data(context, conn)
    run_validation_phase(context, conn)
    run_entity_resolution(context)
    build_graphs_phase(context)
    compute_core_scores_phase(context)
    compute_supplementary_metrics_phase(context)
    assemble_result_entries(context, conn)
    conn.commit()
    conn.close()
    post_process_results(context)

    return context


def _time_threadpool(context) -> float:
    from src.pipeline_phases.analysis_modules import run_analysis_modules_phase

    t0 = time.monotonic()
    run_analysis_modules_phase(context)
    return time.monotonic() - t0


def _time_hamilton(context) -> float:
    from src.pipeline_phases.analysis_modules import run_analysis_modules_hamilton

    t0 = time.monotonic()
    run_analysis_modules_hamilton(context)
    return time.monotonic() - t0


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="animetor_bench_h1_") as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "bench.db"
        json_dir = tmp_path / "json"
        json_dir.mkdir()

        import src.analysis.visualize
        import src.database
        import src.pipeline
        import src.utils.config

        original_db_path = src.database.DEFAULT_DB_PATH
        original_pipeline_json = src.pipeline.JSON_DIR
        original_config_json = src.utils.config.JSON_DIR
        original_viz_json = src.analysis.visualize.JSON_DIR

        src.database.DEFAULT_DB_PATH = db_path
        src.pipeline.JSON_DIR = json_dir
        src.utils.config.JSON_DIR = json_dir
        src.analysis.visualize.JSON_DIR = json_dir

        try:
            credit_count = _populate_temp_db(db_path)
            logger.info("h1_bench_db_populated", credits=credit_count)

            # Build a fresh context for each path to avoid mutation cross-talk.
            # PipelineContext contains threading locks and graph objects that
            # cannot be deepcopied; rebuilding through phases 1-8 is the
            # cleanest way to get a fully-populated, untouched context.
            ctx_tp = _build_context_through_phase8()
            tp_seconds = _time_threadpool(ctx_tp)
            logger.info("h1_bench_threadpool_done", seconds=round(tp_seconds, 3))
            del ctx_tp

            ctx_ham = _build_context_through_phase8()
            ham_seconds = _time_hamilton(ctx_ham)
            logger.info("h1_bench_hamilton_done", seconds=round(ham_seconds, 3))
            del ctx_ham

        finally:
            src.database.DEFAULT_DB_PATH = original_db_path
            src.pipeline.JSON_DIR = original_pipeline_json
            src.utils.config.JSON_DIR = original_config_json
            src.analysis.visualize.JSON_DIR = original_viz_json

    overhead = (ham_seconds - tp_seconds) / tp_seconds if tp_seconds > 0 else 0.0

    result = {
        "threadpool_seconds": round(tp_seconds, 4),
        "hamilton_seconds": round(ham_seconds, 4),
        "overhead_fraction": round(overhead, 4),
        "overhead_threshold": OVERHEAD_THRESHOLD,
        "passed": overhead < OVERHEAD_THRESHOLD,
        "credit_count": credit_count,
        "synthetic_params": {
            "n_directors": SYNTH_DIRECTORS,
            "n_animators": SYNTH_ANIMATORS,
            "n_anime": SYNTH_ANIME,
            "seed": SYNTH_SEED,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    print(json.dumps(result, indent=2))

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    HAMILTON_RESULT_PATH.write_text(json.dumps(result, indent=2), encoding="utf-8")
    logger.info("h1_bench_saved", path=str(HAMILTON_RESULT_PATH))

    if not result["passed"]:
        print(
            f"\nFAIL: Hamilton overhead {overhead * 100:.1f}% exceeds threshold "
            f"{OVERHEAD_THRESHOLD * 100:.0f}%",
            file=sys.stderr,
        )
        return 1

    print(
        f"\nPASS: Hamilton overhead {overhead * 100:.1f}% (threshold "
        f"{OVERHEAD_THRESHOLD * 100:.0f}%)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
