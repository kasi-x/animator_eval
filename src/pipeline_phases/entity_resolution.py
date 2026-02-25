"""Phase 3: Entity Resolution — deduplicate person identities and merge credits."""

from collections import defaultdict

import structlog

from src.analysis.entity_resolution import resolve_all
from src.models import Credit
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def _merge_duplicate_credits(credits: list[Credit]) -> list[Credit]:
    """Merge duplicate credits after entity resolution.

    After person ID and anime ID resolution, the same (person, anime, role)
    may appear multiple times from different sources (e.g. AniList + MADB).
    This function merges them while preserving episode info and source provenance.

    Rules:
    - Key: (person_id, anime_id, role)
    - episode=-1 (unknown) is replaced by specific episode numbers if available
    - Multiple specific episodes → kept as separate credits
    - source: concatenated from all sources (provenance tracking)
    - raw_role: prefer AniList (English) over MADB (Japanese)

    Args:
        credits: List of credits (already resolved person_ids)

    Returns:
        Merged list of credits
    """
    # Group by (person_id, anime_id, role)
    groups: dict[tuple[str, str, str], list[Credit]] = defaultdict(list)
    for c in credits:
        key = (c.person_id, c.anime_id, c.role.value)
        groups[key].append(c)

    merged: list[Credit] = []
    total_merged = 0
    episodes_preserved = 0

    for (person_id, anime_id, _role_val), group in groups.items():
        if len(group) == 1:
            merged.append(group[0])
            continue

        # Multiple credits for same (person, anime, role)
        total_merged += len(group) - 1

        # Collect all sources
        sources = sorted({c.source for c in group if c.source})
        merged_source = ",".join(sources) if sources else ""

        # Prefer non-MADB raw_role (English from AniList)
        raw_role = None
        for c in group:
            if c.raw_role and not c.source.startswith("madb"):
                raw_role = c.raw_role
                break
        if raw_role is None:
            # Fallback to any available raw_role
            for c in group:
                if c.raw_role:
                    raw_role = c.raw_role
                    break

        # Collect episode info
        specific_episodes: set[int] = set()
        for c in group:
            if c.episode is not None and c.episode >= 0:
                specific_episodes.add(c.episode)

        if specific_episodes:
            # Emit one credit per specific episode
            episodes_preserved += len(specific_episodes)
            for ep in sorted(specific_episodes):
                merged.append(
                    Credit(
                        person_id=person_id,
                        anime_id=anime_id,
                        role=group[0].role,
                        raw_role=raw_role,
                        episode=ep,
                        source=merged_source,
                    )
                )
        else:
            # No specific episodes → keep one credit with episode as-is
            merged.append(
                Credit(
                    person_id=person_id,
                    anime_id=anime_id,
                    role=group[0].role,
                    raw_role=raw_role,
                    episode=group[0].episode,
                    source=merged_source,
                )
            )

    if total_merged > 0:
        logger.info(
            "credits_merged",
            duplicates_removed=total_merged,
            episodes_preserved=episodes_preserved,
            credits_before=len(credits),
            credits_after=len(merged),
        )

    return merged


def run_entity_resolution(context: PipelineContext) -> None:
    """Perform entity resolution and update credits with canonical person IDs.

    Args:
        context: Pipeline context

    Updates context fields:
        - canonical_map: Dict mapping duplicate person_id to canonical person_id
        - credits: List of credits with resolved person_ids (deduplicated)
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
            context.monitor.increment_counter(
                "persons_resolved", len(context.canonical_map)
            )

        # Merge duplicate credits (same person+anime+role from different sources)
        context.credits = _merge_duplicate_credits(context.credits)
