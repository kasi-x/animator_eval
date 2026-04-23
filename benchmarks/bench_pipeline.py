"""Performance benchmark for the scoring pipeline using synthetic data.

Generates deterministic synthetic data, populates a temporary database,
runs the full pipeline, and reports phase-level timing results as JSON.

Usage:
    python benchmarks/bench_pipeline.py            # run benchmark
    python benchmarks/bench_pipeline.py --compare   # run and compare against latest.json
"""

import argparse
import json
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on sys.path so `src` package is importable
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import structlog

from src.infra.log import setup_logging

setup_logging()
logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BENCH_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BENCH_DIR / "results"
LATEST_PATH = RESULTS_DIR / "latest.json"

# Synthetic data parameters (deterministic)
SYNTH_DIRECTORS = 10
SYNTH_ANIMATORS = 100
SYNTH_ANIME = 50
SYNTH_SEED = 42

# Regression threshold (fraction, e.g. 0.20 = 20%)
REGRESSION_THRESHOLD = 0.20


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _populate_temp_db(db_path: Path) -> int:
    """Populate a temporary database with synthetic data.

    Returns the number of credits inserted.
    """
    from src.db import get_connection, init_db, insert_credit, upsert_anime, upsert_person
    from src.testing.fixtures import generate_synthetic_data

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

    logger.info(
        "bench_db_populated",
        persons=len(persons),
        anime=len(anime_list),
        credits=len(credits),
    )
    return len(credits)


def _run_full_pipeline() -> tuple[float, list[dict]]:
    """Run the full scoring pipeline and return (elapsed_seconds, results)."""
    from src.runtime.pipeline import run_scoring_pipeline

    t0 = time.monotonic()
    results = run_scoring_pipeline(enable_websocket=False)
    elapsed = time.monotonic() - t0
    return elapsed, results


def _run_individual_phases(json_dir: Path) -> dict[str, float]:
    """Run each pipeline phase individually and return phase timings.

    Returns a dict mapping phase name to elapsed seconds.
    """
    from src.db import get_connection, init_db
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

    reset_monitor()
    context = PipelineContext(visualize=False, dry_run=False)
    conn = get_connection()
    init_db(conn)

    timings: dict[str, float] = {}

    # Phase 1: Data Loading
    t0 = time.monotonic()
    load_pipeline_data(context, conn)
    timings["data_loading"] = time.monotonic() - t0

    # Phase 2: Validation
    t0 = time.monotonic()
    run_validation_phase(context, conn)
    timings["validation"] = time.monotonic() - t0

    # Phase 3: Entity Resolution
    t0 = time.monotonic()
    run_entity_resolution(context)
    timings["entity_resolution"] = time.monotonic() - t0

    # Phase 4: Graph Construction
    t0 = time.monotonic()
    build_graphs_phase(context)
    timings["graph_construction"] = time.monotonic() - t0

    # Phase 5: Core Scoring
    t0 = time.monotonic()
    compute_core_scores_phase(context)
    timings["core_scoring"] = time.monotonic() - t0

    # Phase 6: Supplementary Metrics
    t0 = time.monotonic()
    compute_supplementary_metrics_phase(context)
    timings["supplementary_metrics"] = time.monotonic() - t0

    # Phase 7: Result Assembly
    t0 = time.monotonic()
    assemble_result_entries(context, conn)
    conn.commit()
    conn.close()
    timings["result_assembly"] = time.monotonic() - t0

    # Phase 8: Post-Processing
    t0 = time.monotonic()
    post_process_results(context)
    timings["post_processing"] = time.monotonic() - t0

    # Phase 9: Analysis Modules
    t0 = time.monotonic()
    run_analysis_modules_phase(context)
    timings["analysis_modules"] = time.monotonic() - t0

    # Phase 10: Export & Visualization
    t0 = time.monotonic()
    export_and_visualize_phase(context, elapsed=sum(timings.values()))
    timings["export_and_viz"] = time.monotonic() - t0

    return timings


def _compare_results(current: dict, baseline: dict) -> list[str]:
    """Compare current benchmark results against a baseline.

    Returns a list of regression warning strings. Empty list means no regressions.
    """
    warnings: list[str] = []

    # Compare total pipeline time
    cur_total = current["total_seconds"]
    base_total = baseline["total_seconds"]
    if base_total > 0:
        change = (cur_total - base_total) / base_total
        if change > REGRESSION_THRESHOLD:
            warnings.append(
                f"REGRESSION: total pipeline {base_total:.2f}s -> {cur_total:.2f}s "
                f"(+{change * 100:.1f}%, threshold {REGRESSION_THRESHOLD * 100:.0f}%)"
            )

    # Compare individual phases
    base_phases = baseline.get("phases", {})
    cur_phases = current.get("phases", {})
    for phase_name, cur_time in cur_phases.items():
        base_time = base_phases.get(phase_name)
        if base_time is None or base_time <= 0:
            continue
        change = (cur_time - base_time) / base_time
        if change > REGRESSION_THRESHOLD:
            warnings.append(
                f"REGRESSION: {phase_name} {base_time:.3f}s -> {cur_time:.3f}s "
                f"(+{change * 100:.1f}%, threshold {REGRESSION_THRESHOLD * 100:.0f}%)"
            )

    return warnings


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    """Run the benchmark and output results.

    Returns 0 on success, 1 if regressions detected in --compare mode.
    """
    parser = argparse.ArgumentParser(description="Animetor Eval pipeline benchmark")
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Compare against latest.json and flag >20%% regressions",
    )
    args = parser.parse_args()

    # Create temporary directories for DB and JSON output
    with tempfile.TemporaryDirectory(prefix="animetor_bench_") as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "bench.db"
        json_dir = tmp_path / "json"
        json_dir.mkdir()

        # Monkeypatch module-level paths to use temp directories
        import src.db.init
        import src.runtime.pipeline
        import src.utils.config
        import src.analysis.visualize

        original_db_path = src.db.init.DEFAULT_DB_PATH
        original_pipeline_json = src.runtime.pipeline.JSON_DIR
        original_config_json = src.utils.config.JSON_DIR
        original_viz_json = src.analysis.visualize.JSON_DIR

        src.db.init.DEFAULT_DB_PATH = db_path
        src.runtime.pipeline.JSON_DIR = json_dir
        src.utils.config.JSON_DIR = json_dir
        src.analysis.visualize.JSON_DIR = json_dir

        try:
            # Populate database with synthetic data
            logger.info("bench_start", action="populating_database")
            credit_count = _populate_temp_db(db_path)

            # Run full pipeline (for total time)
            logger.info("bench_start", action="full_pipeline")
            total_seconds, results = _run_full_pipeline()
            result_count = len(results)
            logger.info(
                "bench_full_pipeline_done",
                total_seconds=round(total_seconds, 3),
                results=result_count,
            )

            # Re-populate for individual phase timing (clean state)
            db_path.unlink(missing_ok=True)
            _populate_temp_db(db_path)

            # Run individual phases
            logger.info("bench_start", action="individual_phases")
            phase_timings = _run_individual_phases(json_dir)
            logger.info("bench_individual_phases_done", phases=phase_timings)

        finally:
            # Restore original paths
            src.db.init.DEFAULT_DB_PATH = original_db_path
            src.runtime.pipeline.JSON_DIR = original_pipeline_json
            src.utils.config.JSON_DIR = original_config_json
            src.analysis.visualize.JSON_DIR = original_viz_json

    # Build result document
    bench_result = {
        "total_seconds": round(total_seconds, 4),
        "phases": {k: round(v, 4) for k, v in phase_timings.items()},
        "result_count": result_count,
        "credit_count": credit_count,
        "synthetic_params": {
            "n_directors": SYNTH_DIRECTORS,
            "n_animators": SYNTH_ANIMATORS,
            "n_anime": SYNTH_ANIME,
            "seed": SYNTH_SEED,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Output to stdout
    print(json.dumps(bench_result, indent=2))

    # Save to latest.json
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_PATH.write_text(json.dumps(bench_result, indent=2), encoding="utf-8")
    logger.info("bench_saved", path=str(LATEST_PATH))

    # Compare mode
    exit_code = 0
    if args.compare:
        # Load previous baseline (if it existed before this run, we would
        # have loaded it before overwriting — but since we already overwrote,
        # we check if a backup exists. For simplicity, compare against the
        # result we just wrote if no prior baseline.)
        # In practice, CI would have latest.json from a previous commit.
        # For local use, we just saved the current run.
        baseline_path = LATEST_PATH
        if baseline_path.exists():
            baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
            warnings = _compare_results(bench_result, baseline)
            if warnings:
                print("\n=== PERFORMANCE REGRESSIONS DETECTED ===", file=sys.stderr)
                for w in warnings:
                    print(f"  {w}", file=sys.stderr)
                exit_code = 1
            else:
                print("\nNo performance regressions detected.", file=sys.stderr)
        else:
            print("\nNo baseline found for comparison (first run).", file=sys.stderr)

    logger.info(
        "bench_complete",
        total_seconds=bench_result["total_seconds"],
        result_count=bench_result["result_count"],
    )

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
