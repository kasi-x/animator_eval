"""Phase 7B: VA Result Assembly — build VA result dictionaries."""

import structlog

from src.pipeline_phases.pipeline_types import VAScoresResult

logger = structlog.get_logger()


def assemble_va_results(
    va_person_ids: set,
    persons: list,
    va_person_fe: dict,
    va_birank_scores: dict,
    va_trust_scores: dict,
    va_patronage_scores: dict,
    va_dormancy_scores: dict,
    va_awcc_scores: dict,
    va_iv_scores: dict,
    va_character_diversity: dict,
    va_replacement_difficulty: dict,
) -> VAScoresResult:
    """Build comprehensive VA result dictionaries.

    Args:
        va_person_ids: Set of VA person IDs
        persons: Person records
        va_person_fe: Person fixed effects
        va_birank_scores: BiRank scores
        va_trust_scores: Trust scores
        va_patronage_scores: Patronage scores
        va_dormancy_scores: Dormancy scores
        va_awcc_scores: AWCC scores
        va_iv_scores: Integrated Value scores
        va_character_diversity: Character diversity metrics
        va_replacement_difficulty: Replacement difficulty scores

    Returns:
        VAScoresResult with assembled VA results.
    """
    result = VAScoresResult()

    if not va_person_ids:
        logger.debug("va_result_assembly_skipped", reason="no_va_persons")
        return result

    pid_to_name = _build_pid_to_name_map(persons)
    results = []
    for pid in sorted(va_person_ids):
        record = _build_va_base_record(
            pid,
            pid_to_name,
            va_person_fe,
            va_birank_scores,
            va_trust_scores,
            va_patronage_scores,
            va_dormancy_scores,
            va_awcc_scores,
            va_iv_scores,
            va_replacement_difficulty,
        )
        _enrich_with_diversity(record, va_character_diversity.get(pid))
        results.append(record)
    result.va_results = _sort_results_by_iv(results)
    logger.info("va_results_assembled", persons=len(results))
    return result


def _build_pid_to_name_map(persons) -> dict[str, str]:
    """Build person ID to name mapping for VA results."""
    return {p.id: p.name_ja or p.name_en or p.id for p in persons}


def _build_va_base_record(
    pid: str,
    pid_to_name: dict[str, str],
    va_person_fe: dict,
    va_birank_scores: dict,
    va_trust_scores: dict,
    va_patronage_scores: dict,
    va_dormancy_scores: dict,
    va_awcc_scores: dict,
    va_iv_scores: dict,
    va_replacement_difficulty: dict,
) -> dict:
    """Build base record with 10 scoring fields for a VA person."""
    return {
        "person_id": pid,
        "name": pid_to_name.get(pid, pid),
        "person_fe": va_person_fe.get(pid, 0.0),
        "birank": va_birank_scores.get(pid, 0.0),
        "trust": va_trust_scores.get(pid, 0.0),
        "patronage": va_patronage_scores.get(pid, 0.0),
        "dormancy": va_dormancy_scores.get(pid, 1.0),
        "awcc": va_awcc_scores.get(pid, 0.0),
        "va_iv_score": va_iv_scores.get(pid, 0.0),
        "replacement_difficulty": va_replacement_difficulty.get(pid, 0.0),
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
