"""Phase 7B: VA Result Assembly — build VA result dictionaries."""

import structlog

from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def assemble_va_results(context: PipelineContext) -> None:
    """Build comprehensive VA result dictionaries into context.va_results."""
    if not context.va_person_ids:
        logger.debug("va_result_assembly_skipped", reason="no_va_persons")
        return

    pid_to_name = _build_pid_to_name_map(context.persons)
    results = []
    for pid in sorted(context.va_person_ids):
        record = _build_va_base_record(pid, context, pid_to_name)
        _enrich_with_diversity(record, context.va_character_diversity.get(pid))
        results.append(record)
    context.va_results = _sort_results_by_iv(results)
    logger.info("va_results_assembled", persons=len(results))


def _build_pid_to_name_map(persons) -> dict[str, str]:
    """Build person ID to name mapping for VA results."""
    return {p.id: p.name_ja or p.name_en or p.id for p in persons}


def _build_va_base_record(
    pid: str, context: PipelineContext, pid_to_name: dict[str, str]
) -> dict:
    """Build base record with 10 scoring fields for a VA person."""
    return {
        "person_id": pid,
        "name": pid_to_name.get(pid, pid),
        "person_fe": context.va_person_fe.get(pid, 0.0),
        "birank": context.va_birank_scores.get(pid, 0.0),
        "trust": context.va_trust_scores.get(pid, 0.0),
        "patronage": context.va_patronage_scores.get(pid, 0.0),
        "dormancy": context.va_dormancy_scores.get(pid, 1.0),
        "awcc": context.va_awcc_scores.get(pid, 0.0),
        "va_iv_score": context.va_iv_scores.get(pid, 0.0),
        "replacement_difficulty": context.va_replacement_difficulty.get(pid, 0.0),
    }


def _enrich_with_diversity(record: dict, diversity_metrics) -> None:
    """In-place add 7 diversity fields to record if diversity_metrics is truthy."""
    if diversity_metrics:
        record.update(
            {
                "character_diversity_index": diversity_metrics.cdi,
                "casting_tier": diversity_metrics.casting_tier,
                "main_count": diversity_metrics.main_count,
                "supporting_count": diversity_metrics.supporting_count,
                "background_count": diversity_metrics.background_count,
                "unique_characters": diversity_metrics.unique_characters,
                "genre_entropy": diversity_metrics.genre_entropy,
            }
        )


def _sort_results_by_iv(results: list[dict]) -> list[dict]:
    """Sort results by va_iv_score descending."""
    results.sort(key=lambda r: r.get("va_iv_score", 0.0), reverse=True)
    return results
