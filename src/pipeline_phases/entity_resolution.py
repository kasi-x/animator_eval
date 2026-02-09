"""Phase 3: Entity Resolution — deduplicate person identities."""
import structlog

from src.analysis.entity_resolution import resolve_all
from src.models import Credit
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def run_entity_resolution(context: PipelineContext) -> None:
    """Perform entity resolution and update credits with canonical person IDs.

    Args:
        context: Pipeline context

    Updates context fields:
        - canonical_map: Dict mapping duplicate person_id to canonical person_id
        - credits: List of credits with resolved person_ids
    """
    with context.monitor.measure("entity_resolution"):
        context.canonical_map = resolve_all(context.persons)

        # Replace person_id in credits with canonical IDs
        if context.canonical_map:
            resolved_credits = []
            for c in context.credits:
                new_pid = context.canonical_map.get(c.person_id, c.person_id)
                resolved_credits.append(
                    Credit(
                        person_id=new_pid,
                        anime_id=c.anime_id,
                        role=c.role,
                        episode=c.episode,
                        source=c.source,
                    )
                )
            context.credits = resolved_credits
            logger.info("person_ids_resolved", count=len(context.canonical_map))
            context.monitor.increment_counter("persons_resolved", len(context.canonical_map))
