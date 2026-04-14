"""Phase 7B: VA Result Assembly — build VA result dictionaries."""

import structlog

from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def assemble_va_results(context: PipelineContext) -> None:
    """Build comprehensive VA result dictionaries.

    Args:
        context: Pipeline context (must have VA scores computed)

    Updates context fields:
        - va_results: list of VA score dicts
    """
    if not context.va_person_ids:
        logger.debug("va_result_assembly_skipped", reason="no_va_persons")
        return

    # Build person_id -> name mapping
    pid_to_name = {p.id: p.name_ja or p.name_en or p.id for p in context.persons}

    results = []
    for pid in sorted(context.va_person_ids):
        iv = context.va_iv_scores.get(pid, 0.0)
        diversity_metrics = context.va_character_diversity.get(pid)

        result = {
            "person_id": pid,
            "name": pid_to_name.get(pid, pid),
            "person_fe": context.va_person_fe.get(pid, 0.0),
            "birank": context.va_birank_scores.get(pid, 0.0),
            "trust": context.va_trust_scores.get(pid, 0.0),
            "patronage": context.va_patronage_scores.get(pid, 0.0),
            "dormancy": context.va_dormancy_scores.get(pid, 1.0),
            "awcc": context.va_awcc_scores.get(pid, 0.0),
            "va_iv_score": iv,
            "replacement_difficulty": context.va_replacement_difficulty.get(pid, 0.0),
        }

        # Add diversity metrics if available
        if diversity_metrics:
            result.update(
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

        results.append(result)

    # Sort by VA IV score descending
    results.sort(key=lambda r: r.get("va_iv_score", 0.0), reverse=True)
    context.va_results = results

    logger.info("va_results_assembled", persons=len(results))
